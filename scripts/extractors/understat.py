"""
Extractor Understat (xG y metricas de estilo) para laliga-predictor-engine.

Fuente: Understat (https://understat.com), modelo de xG propio con historico de
LaLiga desde la temporada 2014/15. Aqui arrancamos en 2016/17 para casar con el
historico de Sofascore.

Que aporta Understat que NO esta en Sofascore
---------------------------------------------
De todo lo que publica Understat, este modulo guarda SOLO lo que Sofascore no
cubre (para no duplicar tiros, posesion, pases, duelos, ratings, ...):
    - xg    : goles esperados (modelo Understat). Rellena el hueco 2016-2022 que
              Sofascore deja vacio y sirve de 2a fuente donde ambos coexisten.
    - npxg  : xG SIN penaltis (senal de ataque mas limpia; Sofascore solo da xG total).
    - ppda  : presion (pases del rival por accion defensiva). Cuanto MENOR, mas
              intensa la presion. No existe en Sofascore.
    - deep  : jugadas completadas cerca del area rival (dominio territorial).
    - xpts  : puntos esperados del partido (capta forma real / regresion a la media).

Nota de diseno (por que 5 metricas por lado y no 10):
    En un dato POR PARTIDO con columnas home/away, las variantes "en contra"
    (xGA, deep_allowed, ppda_allowed, ...) son REDUNDANTES: el `deep` en contra
    del local es exactamente el `deep` del visitante, etc. Por eso guardamos solo
    las 5 metricas independientes por lado; nada se pierde.

Como se obtienen los datos (IMPORTANTE: cambio de Understat, ~dic 2025)
-----------------------------------------------------------------------
Hasta finales de 2025 Understat incrustaba los datos en el HTML de la pagina de
liga como `var datesData = JSON.parse('...')` / `var teamsData = ...`. En
diciembre de 2025 rehicieron la web y ESOS BLOQUES YA NO EXISTEN: ahora la
pagina carga los datos por AJAX desde un endpoint JSON:

    GET https://understat.com/getLeagueData/<slug>/<year>
        -> {"dates": [partidos...], "teams": {id: {..., "history": [...]}, ...},
            "players": [...]}

(los objetos internos "dates"/"teams" mantienen el MISMO esquema que los
antiguos datesData/teamsData). Este modulo usa ese endpoint como via principal
y conserva el parseo del HTML antiguo como fallback por si Understat revirtiera
el cambio. Referencia del cambio: issue #71 de ScraperFC (dic 2025), donde se
valido este mismo enfoque.

Estructura del modulo (identica a los demas extractores)
--------------------------------------------------------
A) CAPA DE SCRAPING (corre en LOCAL, red abierta):
     backfill(seasons) -> descarga todo el historico al CSV maestro
                          `understat_laliga_match_stats.csv`.
     update()          -> EJECUTABLE SEMANALMENTE: anade solo los partidos
                          finalizados nuevos de la(s) temporada(s) en curso.
   Una sola peticion por temporada (el endpoint trae toda la temporada de
   golpe). El guardado esta pensado para ejecutarse una vez sin intervencion:
   checkpoint por temporada, escritura atomica y reanudacion automatica (ver la
   seccion "Guardado a prueba de fallos"). Idempotente por `match_id` de Understat.

B) CAPA DE ADAPTACION / NORMALIZACION (corre en cualquier sitio, solo pandas):
     load_processed(master_csv) -> lee el master (ya normalizado), fija tipos y
                          guarda un parquet limpio `understat_match.parquet` con
                          `match_key` para el cruce con las demas fuentes.

IMPORTANTE: la capa A exige salida a understat.com, por lo que se ejecuta siempre
en la maquina local. Requiere: pandas y, opcionalmente, curl_cffi (si no esta, cae
a urllib de la libreria estandar).

Uso tipico (en local, cada semana)
-----------------------------------
    python scripts/extractors/understat.py weekly      # update + parquet
    python scripts/extractors/understat.py backfill    # historico 2016/17 -> hoy
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

try:  # ejecucion como paquete (from scripts.extractors import understat)
    from . import _common
    from . import teamnames
except ImportError:  # ejecucion como script suelto
    import _common  # type: ignore
    import teamnames  # type: ignore


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
LEAGUE_SLUG = "La_liga"          # slug de LaLiga en understat.com/league/<slug>/<year>
FIRST_SEASON = 2016              # 2016 = temporada 2016/17 (casa con Sofascore)
_BASE_URL = "https://understat.com/league"
# Endpoint JSON que usa la web nueva de Understat (~dic 2025). Devuelve
# {"dates": [...], "teams": {...}, "players": [...]} para toda la temporada.
_API_URL = "https://understat.com/getLeagueData"

# raiz del repo: .../scripts/extractors/understat.py -> subir 3 -> repo/
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "understat"
# CSV maestro (crudo, ya normalizado a una fila por partido con las metricas
# Understat que NO estan en Sofascore).
_MATCH_STATS_CSV = _RAW_DIR / "understat_laliga_match_stats.csv"
# Parquet limpio (tipado) listo para cruzar con las demas fuentes.
_OUT_PARQUET = _REPO / "data" / "02_processed" / "understat_match.parquet"

# Metricas independientes por lado que guardamos (prefijo us_ = Understat).
# El resto de Understat o esta en Sofascore o es la variante "en contra" (=
# el mismo valor del rival), asi que no se duplica.
_METRICS = ["xg", "npxg", "ppda", "deep", "xpts"]

# Cabeceras base de navegador para las peticiones.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


# =========================================================================== #
# A) CAPA DE SCRAPING
# =========================================================================== #

class _NotFound(Exception):
    """La temporada aun no existe en Understat (p.ej. la que no ha empezado)."""


# --- transporte (delega en _common; curl_cffi si esta, si no urllib) -------- #
def _http_get(url: str, *, headers: dict, timeout: int = 30,
              retries: int = 2, wait: float = 2.0) -> tuple[int, str]:
    """GET -> (status, cuerpo). NO lanza por codigos HTTP; reintenta con espera
    creciente en red, 429 y 5xx. Compartido en `_common.http_get`.

    El 403 vuelve en el primer intento a proposito: es la senal con la que
    `_fetch_season_payload` escala del endpoint JSON al HTML con `datesData`."""
    return _common.http_get(url, headers=headers, timeout=timeout,
                            retries=retries, wait=wait)


# --- parseo (funciones puras, testeables sin red) -------------------------- #
def _json_from_var(html: str, var_name: str) -> dict | list:
    """Extrae el JSON embebido `var <var_name> = JSON.parse('...')` del HTML.

    FORMATO ANTIGUO de Understat (hasta ~dic 2025); se conserva como fallback.
    Tolera var/let/const, comillas simples o dobles y espaciado variable.
    Understat inyectaba los datos como una cadena con escapes \\xHH; se
    decodifican con 'unicode_escape' + ida/vuelta latin-1 para los acentos.
    """
    m = re.search(
        r"(?:var|let|const)?\s*" + re.escape(var_name)
        + r"\s*=\s*JSON\.parse\(\s*(['\"])(.*?)\1\s*\)",
        html, re.DOTALL,
    )
    if not m:
        raise ValueError(
            f"No encontre '{var_name}' embebido en el HTML de Understat "
            "(formato antiguo, retirado hacia dic 2025)."
        )
    raw = m.group(2)
    # decodifica \xHH -> caracteres; robustez extra para acentos si los hubiera
    try:
        decoded = raw.encode("utf-8").decode("unicode_escape").encode(
            "latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        decoded = raw.encode("utf-8").decode("unicode_escape")
    return json.loads(decoded)


def _norm_dates(x) -> list[dict]:
    """Normaliza 'dates' a lista de partidos (la API puede dar lista o dict)."""
    if x is None:
        return []
    if isinstance(x, dict):
        return [v for v in x.values() if isinstance(v, dict)]
    return [v for v in x if isinstance(v, dict)]


def _norm_teams(x) -> dict[str, dict]:
    """Normaliza 'teams' a {team_id: info} (la API puede dar dict o lista)."""
    if x is None:
        return {}
    if isinstance(x, list):
        return {str(t.get("id", i)): t for i, t in enumerate(x)
                if isinstance(t, dict)}
    return {str(k): v for k, v in x.items() if isinstance(v, dict)}


def _as_bool(v) -> bool:
    """isResult puede venir como bool, 'true'/'false' o 0/1 segun la fuente."""
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _to_float(v):
    """Convierte a float con tolerancia; None si no aplica."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ppda_ratio(ppda: dict) -> float | None:
    """PPDA = pases del rival / acciones defensivas (att/def). None si no calc."""
    if not isinstance(ppda, dict):
        return None
    att, dfn = _to_float(ppda.get("att")), _to_float(ppda.get("def"))
    if att is None or not dfn:  # def 0 o ausente -> no definible
        return None
    return att / dfn


def _metrics_from_hist(entry: dict) -> dict:
    """Extrae las 5 metricas independientes de una entrada 'history' de Understat."""
    return {
        "xg": _to_float(entry.get("xG")),
        "npxg": _to_float(entry.get("npxG")),
        "ppda": _ppda_ratio(entry.get("ppda")),
        "deep": _to_float(entry.get("deep")),
        "xpts": _to_float(entry.get("xpts")),
    }


def parse_payload(dates: list[dict], teams: dict[str, dict],
                  season_year: int, *, skipped: list | None = None) -> list[dict]:
    """Convierte (dates, teams) de una temporada en filas-partido.

    Nucleo puro y testeable: cruza los partidos ('dates': id, equipos, fecha,
    si esta jugado) con el historial por equipo ('teams' -> 'history', que trae
    las metricas de estilo). Devuelve una lista de dicts, una por partido
    FINALIZADO, con las metricas us_home_*/us_away_*.

    `skipped`: lista opcional donde se ANOTAN los partidos finalizados que se
    saltan por no tener historial cruzable. Descartarlos en silencio seria lo
    comodo, pero en una temporada cerrada cualquier saltado es un hueco del que
    hay que enterarse.
    """
    # indice: {team_id: {fecha 'YYYY-MM-DD': entrada_history}}
    hist_by_team: dict[str, dict[str, dict]] = {}
    for tid, tinfo in teams.items():
        by_date: dict[str, dict] = {}
        for h in tinfo.get("history", []):
            d = str(h.get("date", ""))[:10]
            if d:
                by_date[d] = h
        hist_by_team[str(tid)] = by_date

    season_label = f"{season_year}/{str(season_year + 1)[2:]}"  # 2016 -> "2016/17"
    rows: list[dict] = []
    for m in dates:
        if not _as_bool(m.get("isResult")):  # partido aun no jugado -> se ignora
            continue
        h_id, a_id = str(m["h"]["id"]), str(m["a"]["id"])
        home_name, away_name = m["h"]["title"], m["a"]["title"]
        dt = str(m.get("datetime", ""))
        date = dt[:10]

        home_hist = hist_by_team.get(h_id, {}).get(date)
        away_hist = hist_by_team.get(a_id, {}).get(date)
        if home_hist is None or away_hist is None:
            # sin historial cruzable (raro): se salta, pero se ANOTA para que el
            # llamante lo haga visible (un hueco silencioso aqui = partido que
            # desaparece del xG sin dejar rastro)
            if skipped is not None:
                skipped.append({"match_id": m.get("id"), "date": date,
                                "home": home_name, "away": away_name,
                                "missing_side": ("home" if home_hist is None
                                                 else "away")})
            continue

        row = {
            "match_id": int(m["id"]),
            "season": season_label,
            "season_year": season_year,
            "datetime": dt,
            "date": date,
            "home_team": home_name,
            "away_team": away_name,
            "home_key": teamnames.to_key(home_name),
            "away_key": teamnames.to_key(away_name),
            "match_key": None,  # se fija abajo (vectorizado en _finalize)
        }
        hm = _metrics_from_hist(home_hist)
        am = _metrics_from_hist(away_hist)
        for k in _METRICS:
            row[f"us_home_{k}"] = hm[k]
            row[f"us_away_{k}"] = am[k]
        rows.append(row)
    return rows


def parse_season(html: str, season_year: int) -> list[dict]:
    """[LEGADO] Parsea el HTML antiguo de understat.com/league/<slug>/<year>.

    Se mantiene por compatibilidad y como fallback: extrae datesData/teamsData
    embebidos y delega en `parse_payload`. La via principal ahora es el
    endpoint JSON (ver `_fetch_season_payload`).
    """
    dates = _norm_dates(_json_from_var(html, "datesData"))
    teams = _norm_teams(_json_from_var(html, "teamsData"))
    return parse_payload(dates, teams, season_year)


# --- descarga de una temporada (API nueva con fallback al HTML) ------------- #
def _fetch_season_payload(year: int) -> tuple[list[dict], dict[str, dict]]:
    """Descarga (dates, teams) de una temporada.

    1) Via principal: GET {_API_URL}/{slug}/{year} (JSON; web nueva ~dic 2025).
    2) Fallback: pagina HTML clasica con datesData/teamsData embebidos.
    Lanza `_NotFound` SOLO si ambas rutas devuelven 404 (temporada inexistente);
    cualquier otro fallo doble lanza RuntimeError con diagnostico completo, para
    no confundir "Understat volvio a cambiar" con "temporada sin empezar".
    """
    api_url = f"{_API_URL}/{LEAGUE_SLUG}/{year}"
    page_url = f"{_BASE_URL}/{LEAGUE_SLUG}/{year}"
    problems: list[str] = []
    not_found = 0

    # 1) endpoint JSON de la web nueva
    try:
        headers = {
            **_HEADERS,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",  # asi lo pide el front nuevo
            "Referer": f"{page_url}",
        }
        status, body = _http_get(api_url, headers=headers)
        if status == 404:
            not_found += 1
            problems.append(f"HTTP 404 en {api_url}")
        elif status != 200:
            problems.append(f"HTTP {status} en {api_url}: {body[:120]!r}")
        else:
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                problems.append(
                    f"respuesta no-JSON de {api_url}: {body[:120]!r}")
            else:
                if isinstance(data, dict):
                    dates = data.get("dates", data.get("datesData"))
                    teams = data.get("teams", data.get("teamsData"))
                    if dates is not None and teams is not None:
                        return _norm_dates(dates), _norm_teams(teams)
                    problems.append(
                        f"JSON de {api_url} sin 'dates'/'teams' "
                        f"(claves: {sorted(data)[:8]})")
                else:
                    problems.append(
                        f"JSON de {api_url} con forma inesperada "
                        f"({type(data).__name__})")
    except RuntimeError as e:  # error de red ya reintentado en _http_get
        problems.append(str(e))

    # 2) fallback: HTML clasico con los datos embebidos
    try:
        status, body = _http_get(page_url, headers=_HEADERS)
        if status == 404:
            not_found += 1
            problems.append(f"HTTP 404 en {page_url}")
        elif status != 200:
            problems.append(f"HTTP {status} en {page_url}: {body[:120]!r}")
        else:
            try:
                dates = _norm_dates(_json_from_var(body, "datesData"))
                teams = _norm_teams(_json_from_var(body, "teamsData"))
                return dates, teams
            except ValueError as e:
                problems.append(f"{e} (inicio del HTML: {body[:120]!r})")
    except RuntimeError as e:
        problems.append(str(e))

    if not_found == 2:  # ninguna de las dos rutas conoce la temporada
        raise _NotFound(page_url)
    raise RuntimeError(
        "No pude obtener la temporada de Understat por ninguna via.\n  - "
        + "\n  - ".join(problems)
        + "\n  Contexto: Understat rehizo su web hacia dic 2025 (los datos ya "
        "no van embebidos en el HTML sino via getLeagueData). Si este error "
        "persiste, probablemente hayan vuelto a cambiar el endpoint: abre "
        f"{page_url} en el navegador y mira en las DevTools (pestana Red/XHR) "
        "de donde carga ahora los datos la pagina."
    )


# --- orquestacion backfill / update ---------------------------------------- #
def _current_season_year(now: datetime | None = None) -> int:
    """Ano de inicio de la temporada vigente (LaLiga arranca en agosto)."""
    now = now or datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def _fetch_and_parse_season(year: int, *, verbose: bool = True) -> list[dict]:
    """Descarga y parsea una temporada. [] si aun no existe (404) o va vacia."""
    try:
        dates, teams = _fetch_season_payload(year)
    except _NotFound:
        if verbose:
            print(f"  [{year}] aun no existe en Understat (se omite).")
        return []
    skipped: list[dict] = []
    rows = parse_payload(dates, teams, year, skipped=skipped)
    if verbose:
        print(f"  [{year}] partidos finalizados: {len(rows)}")
    if skipped:
        head = ", ".join(f"{s['date']} {s['home']}-{s['away']}"
                         for s in skipped[:5])
        print(f"  [{year}] AVISO: {len(skipped)} partido(s) finalizados SIN "
              f"historial cruzable (NO entran al master): {head}"
              f"{' ...' if len(skipped) > 5 else ''}. Si la temporada esta "
              "cerrada, es un hueco real: revisa el payload de Understat.")
    return rows


def backfill(
    seasons=None,
    *,
    master_csv: Path | str = _MATCH_STATS_CSV,
    pause: float = 1.0,
) -> pd.DataFrame:
    """Descarga TODO el historico (por defecto 2016/17 -> temporada vigente).

    `seasons`: iterable de anos de inicio (2016 = 16/17). Idempotente: salta los
    `match_id` que ya esten en el master. Checkpoint por temporada -> reanudable.
    """
    if seasons is None:
        seasons = range(FIRST_SEASON, _current_season_year() + 1)
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master, existing = _load_master(master_csv)
    df_ckpt, ckpt_ids = _load_checkpoint(master_csv)
    new_rows: list = df_ckpt.to_dict("records") if not df_ckpt.empty else []
    existing = existing | ckpt_ids

    for year in sorted(set(int(y) for y in seasons)):
        print(f"\n=== Temporada {year}/{str(year + 1)[2:]} ===")
        rows = _fetch_and_parse_season(year)
        fresh = [r for r in rows if r["match_id"] not in existing]
        print(f"  nuevos (no presentes): {len(fresh)}")
        if fresh:
            new_rows.extend(fresh)
            existing |= {r["match_id"] for r in fresh}
            _save_checkpoint(new_rows, master_csv)  # persiste tras cada temporada
        time.sleep(pause)

    return _finalize_master(df_master, new_rows, master_csv)


def update(
    *,
    master_csv: Path | str = _MATCH_STATS_CSV,
    recent_seasons: int = 2,
    pause: float = 1.0,
) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE. Anade los partidos finalizados nuevos.

    Escanea las `recent_seasons` temporadas mas recientes (por defecto 2 =
    vigente + anterior), para cubrir el cambio de temporada sin dejar huecos.
    Idempotente por `match_id`.
    """
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master, existing = _load_master(master_csv)
    df_ckpt, ckpt_ids = _load_checkpoint(master_csv)
    new_rows: list = df_ckpt.to_dict("records") if not df_ckpt.empty else []
    existing = existing | ckpt_ids

    cur = _current_season_year()
    years = sorted({cur - i for i in range(max(1, recent_seasons))
                    if cur - i >= FIRST_SEASON}, reverse=True)
    print(f"[INFO] Temporadas a escanear: {years}")

    for year in years:
        rows = _fetch_and_parse_season(year)
        fresh = [r for r in rows if r["match_id"] not in existing]
        print(f"  [{year}] nuevos finalizados: {len(fresh)}")
        if fresh:
            new_rows.extend(fresh)
            existing |= {r["match_id"] for r in fresh}
            _save_checkpoint(new_rows, master_csv)
        time.sleep(pause)

    return _finalize_master(df_master, new_rows, master_csv)


def _load_master(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el CSV maestro. Devuelve (dataframe, match_id ya presentes)."""
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        ids = set(df["match_id"].unique()) if "match_id" in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} partidos.")
        return df, ids
    print("[INFO] sin datos previos; se empieza de cero.")
    return pd.DataFrame(), set()


# --------------------------------------------------------------------------- #
# Guardado a prueba de fallos (disenado para "ejecutar una vez y olvidarse")
# --------------------------------------------------------------------------- #
# Mismo patron robusto que el resto de extractores:
#   * checkpoint por temporada: cada temporada parseada se vuelca a
#     `_ckpt_<nombre>.csv`, de modo que un corte no pierde trabajo.
#   * escritura atomica: el CSV maestro se escribe a un temporal y se publica con
#     `os.replace` (atomico), dejando una copia `.bak`.
#   * reanudacion automatica: al re-ejecutar, los match_id del checkpoint se
#     tratan como ya presentes -> no se vuelven a descargar y se funden.
#   * escritura garantizada en Windows: antes de guardar se retira el atributo
#     "solo lectura" (que OneDrive suele poner) y se reintenta si hay candado.
def _ckpt_path(master_csv: Path) -> Path:
    return _common.ckpt_path(master_csv)


def _load_checkpoint(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el checkpoint pendiente (temporadas parseadas sin volcar al master)."""
    return _common.load_checkpoint(master_csv, key_col="match_id")


def _save_checkpoint(new_rows: list, master_csv: Path) -> None:
    """Reescribe el checkpoint con todas las filas descargadas hasta ahora."""
    _common.save_checkpoint(new_rows, master_csv)


_clear_readonly = _common.clear_readonly
_write_df_safe = _common.write_df_safe


def _finalize_master(df_master: pd.DataFrame, new_rows: list,
                     master_csv: Path) -> pd.DataFrame:
    """Funde los partidos nuevos en el maestro, lo guarda y limpia el checkpoint."""
    if not new_rows:
        print("[INFO] no hay partidos nuevos: el maestro ya esta al dia.")
        return df_master
    df_new = pd.DataFrame(new_rows)
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty],
                          ignore_index=True)
    if "match_id" in df_master.columns:
        # El maestro (primero en el concat) manda: las metricas de un partido
        # finalizado no cambian en Understat. La excepcion es la fila capturada
        # incompleta, que un "gana el primero" estricto congelaria; el desempate
        # por completitud permite que una descarga posterior la rellene.
        df_master = _common.dedupe_preferring_complete(df_master, "match_id")
    # match_key estable para el cruce con las demas fuentes
    df_master["match_key"] = teamnames.match_key(
        df_master["date"], df_master["home_team"], df_master["away_team"])
    if "datetime" in df_master.columns:
        df_master = df_master.sort_values("datetime").reset_index(drop=True)

    if _write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] estadisticas Understat guardadas -> {master_csv}  "
              f"({len(df_master)} partidos)")
        ckpt = _ckpt_path(master_csv)
        if ckpt.exists():
            try:
                ckpt.unlink()
            except Exception:  # noqa: BLE001
                pass
    return df_master


# =========================================================================== #
# B) CAPA DE ADAPTACION / NORMALIZACION  (solo pandas, testeable offline)
# =========================================================================== #
def load_processed(
    master_csv: Path | str = _MATCH_STATS_CSV,
    *,
    out_parquet: Path | str | None = _OUT_PARQUET,
    verbose: bool = True,
    strict: bool = True,
) -> pd.DataFrame:
    """Lee el CSV maestro, fija tipos/match_key y guarda el parquet limpio.

    Salida: una fila por partido con match_key, home_key, away_key, date, season
    y las columnas us_home_*/us_away_* (xg, npxg, ppda, deep, xpts).

    `strict=True`: falla ruidosamente ante columnas ausentes, match_key nulos,
    duplicados o equipos sin clave canonica (ver teamnames.assert_known).
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        raise _common.RegressionError(
            f"[understat.load_processed] no existe el maestro '{master_csv}'. "
            "Publicar un parquet vacio destruiria el vigente (las aserciones de "
            "esquema no detectan un frame sin filas). Ejecuta antes el backfill.")
    df = pd.read_csv(master_csv)
    _common.assert_columns(
        df, ["match_id", "date", "home_team", "away_team"]
            + [f"us_{side}_{k}" for side in ("home", "away") for k in _METRICS],
        source="understat.load_processed")

    # re-normaliza claves y match_key por si el CSV se edito a mano
    df["home_key"] = df["home_team"].map(teamnames.to_key)
    df["away_key"] = df["away_team"].map(teamnames.to_key)
    df["match_key"] = teamnames.match_key(df["date"], df["home_team"], df["away_team"])

    # tipa las metricas como float
    for c in [f"us_{side}_{k}" for side in ("home", "away") for k in _METRICS]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # validaciones ruidosas (antes de publicar nada)
    if strict:
        _common.assert_non_null(df, ["match_key", "home_key", "away_key"],
                                source="understat.load_processed")
        _common.assert_unique(df, "match_key", source="understat.load_processed")
        bad = set()
        for side in ("home", "away"):
            m = ~df[f"{side}_key"].isin(teamnames.CANONICAL)
            bad |= set(df.loc[m, f"{side}_team"].dropna().unique())
        if bad:
            teamnames.assert_known(bad, source="understat.load_processed")

    if verbose:
        _report(df)

    if out_parquet is not None:
        _common.publish_df(df, out_parquet, source="understat.load_processed",
                           fmt="parquet", verbose=verbose)
    return df


def _report(df: pd.DataFrame) -> None:
    print("\n===== INFORME COBERTURA Understat =====")
    print(f"partidos              : {len(df)}")
    if "match_key" in df:
        print(f"match_key unicos      : {df['match_key'].nunique()}")
    if "season" in df:
        seasons = sorted(df["season"].dropna().unique().tolist())
        print(f"temporadas            : {seasons}")
    if "date" in df:
        d = pd.to_datetime(df["date"], errors="coerce")
        print(f"rango fechas          : {d.min()} -> {d.max()}")
    for c in ("us_home_xg", "us_home_npxg", "us_home_ppda", "us_home_deep",
              "us_home_xpts"):
        if c in df:
            print(f"{c:16s}: {df[c].notna().mean() * 100:.1f}% no nulo")
    # equipos no reconocidos por la capa de normalizacion
    bad = set()
    for side in ("home", "away"):
        keycol, namecol = f"{side}_key", f"{side}_team"
        if keycol in df and namecol in df:
            m = ~df[keycol].isin(teamnames.CANONICAL)
            bad |= set(df.loc[m, namecol].dropna().unique())
    print(f"equipos NO reconocidos: {sorted(bad) if bad else '[] (ok)'}")
    print("=======================================\n")


# =========================================================================== #
# CLI
# =========================================================================== #
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Extractor Understat LaLiga (xG + estilo).")
    p.add_argument("cmd", choices=["backfill", "update", "process", "weekly"],
                   help="backfill=historico | update=nuevos | process=normaliza | "
                        "weekly=update+process")
    p.add_argument("--seasons", nargs="+", type=int, default=None,
                   help="anos de inicio (2016 2017 ...). Def.: 2016 -> vigente.")
    p.add_argument("--master", default=str(_MATCH_STATS_CSV))
    p.add_argument("--recent", type=int, default=2,
                   help="update/weekly: n temporadas mas recientes a escanear (def. 2)")
    args = p.parse_args()

    if args.cmd == "backfill":
        backfill(seasons=args.seasons, master_csv=args.master)
    elif args.cmd == "update":
        update(master_csv=args.master, recent_seasons=args.recent)
    elif args.cmd == "process":
        load_processed(master_csv=args.master)
    elif args.cmd == "weekly":
        update(master_csv=args.master, recent_seasons=args.recent)
        if _ckpt_path(Path(args.master)).exists():
            print("[WARN] el master no se guardo (checkpoint pendiente); "
                  "NO regenero el parquet para no dejarlo desactualizado. "
                  "Resuelve el bloqueo y re-ejecuta 'weekly'.")
        else:
            load_processed(master_csv=args.master)
