"""
Extractor FBref para laliga-predictor-engine.

CONTEXTO IMPORTANTE (enero 2026): Opta/StatsPerform corto el feed de datos
avanzados a Sports Reference y exigio su borrado inmediato. FBref ELIMINO
todo el dato avanzado del sitio (xG, xAG, PSxG, pases/acarreos progresivos,
SCA/GCA, defense/possession detallados...) y hoy solo publica dato BASICO
historico. En consecuencia:
    * `soccerdata` >= 1.9.0 solo expone estos stat_type a nivel
      equipo-partido en `read_team_match_stats`:
          'schedule', 'shooting', 'keeper', 'misc'
      ('passing', 'passing_types', 'possession', 'defense' y
      'goal_shot_creation' YA NO EXISTEN en la web; 'standard' nunca existio).
    * Ademas FBref endurecio el anti-bot (Cloudflare): `requests` recibe 403.
      soccerdata >= 1.9.0 lo resuelve usando SeleniumBase en modo UC
      (Chrome real "undetected"), con rate limit interno de ~7 s/peticion
      (respetuoso con la politica de crawling de Sports Reference).

Que aporta FBref HOY que no tenemos ya en las demas fuentes
-----------------------------------------------------------
Sofascore, Understat, ClubElo y football-data ya cubren: tiros/posesion/pases/
duelos/ratings (Sofascore), xg/npxg/ppda/deep/xpts (Understat), Elo (ClubElo),
resultado + cuotas + tiros/faltas/corners/tarjetas crudas (football-data).
Con solo el dato basico, el valor residual de FBref queda en:
    - schedule : asistencia, capitan, FORMACION y ARBITRO por partido
                 (features de contexto que no da ninguna otra fuente).
    - keeper   : paradas, GA, %paradas por partido (PSxG YA NO existe).
    - shooting : Sh, SoT, G/Sh, G/SoT (eficiencia basica de tiro).
    - misc     : faltas cometidas/recibidas, tarjetas, fueras de juego, corners
                 y, si siguen publicados, Int/TklW basicos.
El extractor es "espejo fiel": vuelca las columnas que FBref sirva en cada
momento, asi que si Sports Reference restaura dato avanzado en el futuro,
las columnas nuevas entran solas (mientras el stat_type exista en soccerdata).

Requisitos (maquina local)
--------------------------
    pip install -U soccerdata          # >= 1.9.0 (trae seleniumbase)
    + Google Chrome instalado (SeleniumBase lo controla en modo UC; el
      chromedriver se descarga solo la primera vez).
Recomendado antes del primer backfill: vaciar la cache antigua
`C:\\Users\\<usuario>\\soccerdata\\data\\FBref` (paginas de la era pre-1.9
tienen otro layout y otros nombres de fichero).

Diseno (identico al resto de extractores: understat / clubelo / footballdata)
-----------------------------------------------------------------------------
A) CAPA DE SCRAPING (corre en LOCAL; FBref necesita navegador + red abierta):
     backfill(seasons) -> baja TODO el historico al CSV maestro CRUDO
                          `fbref_laliga_team_match.csv` (una fila por
                          equipo-partido, ESPEJO FIEL de los 4 bloques FBref
                          consolidados; nada se descarta aqui).
     update()          -> EJECUTABLE SEMANALMENTE: anade solo los partidos
                          JUGADOS nuevos de la(s) temporada(s) en curso.
   Una lectura por bloque y temporada. Guardado a prueba de fallos: checkpoint
   por temporada, escritura atomica y reanudacion automatica. Idempotente por
   `row_id` = "<game>|<team_key>" (estable, unico por equipo-partido; FBref no
   da match_id numerico).

B) CAPA DE ADAPTACION / NORMALIZACION (corre en cualquier sitio, solo pandas):
     load_processed(master_csv) -> re-normaliza claves, tipa numericos y guarda
                          el parquet `fbref_team_match.parquet` con `match_key`
                          para el cruce con las demas fuentes.

La decision de QUE columnas sobreviven cuando se solapan con otra fuente se toma
LUEGO, en la consolidacion (matches_master). Aqui, en extraccion, el crudo es un
espejo fiel; `_OVERLAP_COLS` deja DOCUMENTADO (no aplicado) que columnas de FBref
tienen equivalente en otra fuente, como referencia para esa fase.

IMPORTANTE: la capa A exige navegador y salida a internet, por lo que se ejecuta
siempre en la maquina local. Requiere: pandas y soccerdata.

Uso tipico (en local, cada semana)
-----------------------------------
    python scripts/extractors/fbref.py weekly       # update + parquet
    python scripts/extractors/fbref.py backfill     # historico 2016/17 -> hoy
    python scripts/extractors/fbref.py schedule     # calendario (jornada wk)
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

try:  # ejecucion como paquete (from scripts.extractors import fbref)
    from . import _common
    from . import teamnames
except ImportError:  # ejecucion como script suelto
    import _common  # type: ignore
    import teamnames  # type: ignore


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
LEAGUE = "ESP-La Liga"
FIRST_SEASON = 2016              # 2016 = temporada 2016/17 (casa con las demas)

# stat_type validos HOY en read_team_match_stats (soccerdata >= 1.9.0), tras
# el borrado del dato avanzado de Opta (enero 2026). El orden = orden de merge.
# 'schedule' va PRIMERO porque es el bloque que trae la meta completa
# (date/venue/result/gf/ga/opponent + attendance/captain/formation/referee).
STAT_TYPES = [
    "schedule", "shooting", "keeper", "misc",
]

# version minima de soccerdata: 1.9.0 cambio FBref a SeleniumBase UC (unica
# via que pasa el Cloudflare actual) y ajusto los stat_type a los que existen.
_MIN_SOCCERDATA = (1, 9)

# clave de idempotencia: una fila por (partido, equipo).
_KEY_COL = "row_id"

# columnas "meta" (del calendario) que NO deben llevar prefijo de stat_type;
# se conservan una sola vez (del primer bloque).
_META = {
    "date", "round", "day", "venue", "result", "gf", "ga", "opponent",
    "poss", "xg", "xga", "attendance", "captain", "formation", "referee",
    "match_report", "notes", "time", "comp",
}

# raiz del repo: .../scripts/extractors/fbref.py -> subir 3 -> repo/
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "fbref"
# CSV maestro CRUDO (espejo fiel: una fila por equipo-partido, todos los bloques).
_MASTER_CSV = _RAW_DIR / "fbref_laliga_team_match.csv"
# Parquet limpio (tipado) listo para cruzar con las demas fuentes.
_OUT_PARQUET = _REPO / "data" / "02_processed" / "fbref_team_match.parquet"

# DOCUMENTACION (no se aplica): columnas de FBref cuyo contenido tambien esta en
# otra fuente ya extraida. Sirve de referencia para decidir en CONSOLIDACION que
# fuente manda; NO se dropea nada aqui (estamos en extraccion, espejo fiel).
_OVERLAP_COLS = {
    # (nota: xg/xga/npxg de FBref desaparecieron en 2026-01 con el corte de
    # Opta; el xG del pipeline sale de Understat.)
    # tiros / a puerta / posesion ya en Sofascore y football-data
    "shooting_sh", "shooting_sot", "poss",
    # faltas / tarjetas / fueras de juego crudos ya en football-data
    "misc_fls", "misc_crdy", "misc_crdr", "misc_off",
    # paradas del portero ya en Sofascore
    "keeper_saves",
    # goles/GA/resultado ya en football-data (target) y Sofascore
    "gf", "ga", "result",
}


# --------------------------------------------------------------------------- #
# Normalizacion de columnas
# --------------------------------------------------------------------------- #
def _snake(s: str) -> str:
    """Nombre de columna a snake_case estable."""
    s = str(s).strip().lower()
    s = s.replace("%", "pct").replace("+/-", "_plusminus")
    s = s.replace("+", "_plus_").replace("/", "_per_").replace("-", "_")
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    """Aplana columnas MultiIndex de FBref a snake_case plano."""
    flat = []
    for c in df.columns:
        if isinstance(c, tuple):
            parts = [
                str(p) for p in c
                if p is not None and str(p) != "" and not str(p).lower().startswith("unnamed")
            ]
            name = "_".join(parts)
        else:
            name = str(c)
        flat.append(_snake(name))
    out = df.copy()
    out.columns = flat
    return out


def _prep_block(raw: pd.DataFrame, stat_type: str, keep_meta: bool) -> pd.DataFrame:
    """Aplana + prefija un bloque de stats; conserva/descarta las columnas meta.

    Mantiene el indice (league, season, team, game) intacto para el concat.
    """
    d = _flatten(raw)
    rename, drop = {}, []
    seen: set[str] = set()
    for c in d.columns:
        if c in _META:
            if not keep_meta:
                drop.append(c)
            continue
        new = c if c.startswith(stat_type + "_") else f"{stat_type}_{c}"
        # evita colisiones dentro del bloque
        base, k = new, 1
        while new in seen:
            k += 1
            new = f"{base}_{k}"
        seen.add(new)
        if new != c:
            rename[c] = new
    d = d.rename(columns=rename)
    if drop:
        d = d.drop(columns=drop)
    return d


# --------------------------------------------------------------------------- #
# Consolidacion / normalizacion de equipos
# --------------------------------------------------------------------------- #
def _consolidate(blocks: list[pd.DataFrame]) -> pd.DataFrame:
    """Une los bloques por indice, resetea y normaliza equipos + match_key + row_id."""
    merged = pd.concat(blocks, axis=1)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    merged = merged.reset_index()

    # normalizacion de nombres
    merged["team_key"] = merged["team"].map(teamnames.to_key)
    if "opponent" in merged.columns:
        merged["opp_key"] = merged["opponent"].map(teamnames.to_key)
    else:
        merged["opp_key"] = pd.NA

    # orientacion local/visitante -> match_key compartido por ambas filas
    venue = merged.get("venue", pd.Series("", index=merged.index)).astype(str).str.lower()
    is_home = venue.str.startswith("home")
    home = merged["team"].where(is_home, merged.get("opponent"))
    away = merged.get("opponent").where(is_home, merged["team"])

    if "date" not in merged.columns:
        # fallback: extrae la fecha del id de partido "YYYY-MM-DD Home-Away"
        merged["date"] = merged["game"].astype(str).str.slice(0, 10)

    merged["match_key"] = teamnames.match_key(merged["date"], home, away)
    merged["is_home"] = is_home

    # row_id: clave estable, unica por (partido, equipo). FBref no da match_id
    # numerico; 'game' identifica el partido y team_key distingue las dos filas.
    merged["row_id"] = (
        merged["game"].astype(str).str.strip() + "|" + merged["team_key"].astype(str)
    )

    # ordena las columnas: claves delante, stats detras
    front = [c for c in ("row_id", "match_key", "season", "date", "team", "opponent",
                         "team_key", "opp_key", "is_home", "venue", "result", "gf", "ga")
             if c in merged.columns]
    rest = [c for c in merged.columns if c not in front]
    return merged[front + rest]


def _played_only(df: pd.DataFrame) -> pd.DataFrame:
    """Deja solo partidos JUGADOS (descarta futuros sin stats/resultado)."""
    if df.empty:
        return df
    if "result" in df.columns:
        mask = df["result"].notna() & (df["result"].astype(str).str.strip() != "")
    elif "gf" in df.columns:
        mask = df["gf"].notna()
    else:
        return df
    return df[mask].copy()


# --------------------------------------------------------------------------- #
# Descarga de una temporada (soccerdata >= 1.9.0, SeleniumBase UC)
# --------------------------------------------------------------------------- #
class FBrefDownloadBlocked(RuntimeError):
    """Todos los bloques de una temporada fallaron por descarga (no por datos).

    Suele significar Cloudflare bloqueando, Chrome ausente/roto o red caida;
    NO significa que la temporada no tenga datos.
    """


def _require_soccerdata():
    """Importa soccerdata y verifica version >= 1.9.0 con mensaje accionable."""
    import soccerdata as sd

    ver = tuple(int(x) for x in re.findall(r"\d+", sd.__version__)[:2])
    if ver < _MIN_SOCCERDATA:
        raise SystemExit(
            f"[ERROR] soccerdata {sd.__version__} detectado, pero este extractor "
            f"necesita >= {'.'.join(map(str, _MIN_SOCCERDATA))}.0.\n"
            "  Las versiones antiguas usan `requests` (FBref devuelve 403 por "
            "Cloudflare) y stat_type que ya no existen en la web.\n"
            "  Solucion:  pip install -U soccerdata\n"
            "  (instala tambien seleniumbase; ademas hace falta Google Chrome)."
        )
    return sd


def _season_str(year: int) -> str:
    """Codigo de temporada SIN ambiguedad para soccerdata.

    Pasar `str(year)` es peligroso: soccerdata interpreta "2021" como la
    temporada 20/21 (aviso 'Season id is ambiguous'), no como 2021/22. El
    formato "2021-2022" es inequivoco para cualquier ano.
    """
    return f"{year}-{year + 1}"


def _season_window(df: pd.DataFrame, year: int, verbose: bool) -> pd.DataFrame:
    """Filtro de cordura: solo filas con fecha dentro de la temporada pedida.

    Protege contra dos fallos reales de FBref/soccerdata: (a) el codigo de
    temporada ambiguo y (b) la URL "temporada en curso" sin ano, que en verano
    puede servir la temporada anterior con la etiqueta de la nueva.
    """
    if df.empty or "date" not in df.columns:
        return df
    d = pd.to_datetime(df["date"], errors="coerce")
    mask = (d >= f"{year}-07-01") & (d <= f"{year + 1}-06-30")
    dropped = int((~mask).sum())
    if dropped and verbose:
        print(f"  [{year}] AVISO: {dropped} filas con fecha fuera de la "
              f"temporada {year}/{str(year + 1)[2:]} descartadas (pagina de "
              "otra temporada o codigo ambiguo).")
    return df[mask].copy()


def _fetch_season(
    year: int,
    *,
    leagues: str = LEAGUE,
    proxy=None,
    data_dir: Path | str | None = None,
    stat_types: list[str] | None = None,
    headless: bool = True,
    browser: str | None = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """Baja y consolida UNA temporada de FBref (solo partidos jugados).

    Devuelve un DataFrame equipo-partido con row_id/match_key, o vacio si la
    temporada aun no existe / no tiene datos (p.ej. la que no ha empezado).
    Lanza FBrefDownloadBlocked si TODOS los bloques fallan por descarga.
    """
    sd = _require_soccerdata()

    sts = stat_types or STAT_TYPES
    kwargs = {"leagues": leagues, "seasons": _season_str(year),
              "headless": headless}
    if proxy is not None:
        kwargs["proxy"] = proxy
    if data_dir is not None:
        kwargs["data_dir"] = Path(data_dir)
    if browser is not None:
        kwargs["path_to_browser"] = Path(browser)

    try:
        fb = sd.FBref(**kwargs)
    except Exception as e:  # noqa: BLE001  fallo de red / temporada no empezada
        if verbose:
            print(f"  [{year}] no se pudo iniciar FBref (se omite): {e}")
        return pd.DataFrame()

    # soccerdata 1.9 abre un Chrome (SeleniumBase UC). Si no pudo arrancarlo,
    # NINGUNA descarga va a funcionar: mejor cortar ya con un mensaje claro
    # que quemar horas de reintentos.
    if not hasattr(fb, "_driver"):
        raise SystemExit(
            "[ERROR] soccerdata no pudo arrancar Chrome (SeleniumBase UC), y "
            "sin navegador FBref devuelve 403.\n"
            "  1) Instala Google Chrome.\n"
            "  2) pip install -U soccerdata seleniumbase\n"
            "  3) Si usas un Chrome portable: --browser <ruta_al_chrome.exe>\n"
            "El checkpoint queda intacto; re-ejecuta el mismo comando despues."
        )

    # Cada bloque en su propio try: que falte uno NO debe tumbar la temporada
    # entera. La META (fechas/rival/resultado/gf/ga) la lleva el PRIMER bloque
    # que baje bien (todos los stat_type la traen).
    blocks: list = []
    meta_taken = False
    download_failed = 0
    try:
        for st in sts:
            if verbose:
                print(f"  [{year}] leyendo stat_type='{st}' ...")
            try:
                raw = fb.read_team_match_stats(stat_type=st)
            except Exception as e:  # noqa: BLE001  bloque no disponible
                if "Could not download" in str(e) or isinstance(e, ConnectionError):
                    download_failed += 1
                if verbose:
                    print(f"  [{year}] bloque '{st}' no disponible (se omite): {e}")
                continue
            if raw is None or len(raw) == 0:
                if verbose:
                    print(f"  [{year}] bloque '{st}' vacio (se omite).")
                continue
            blocks.append(_prep_block(raw, st, keep_meta=not meta_taken))
            meta_taken = True
    finally:
        # cierra el Chrome de esta temporada (una instancia por temporada
        # mantiene el checkpoint simple; la cache hace baratas las repeticiones)
        try:
            fb._driver.quit()  # noqa: SLF001
        except Exception:  # noqa: BLE001
            pass

    if not blocks:
        if download_failed == len(sts) and sts:
            raise FBrefDownloadBlocked(
                f"temporada {year}/{str(year + 1)[2:]}: los {len(sts)} bloques "
                "fallaron por DESCARGA (Cloudflare/red), no por falta de datos."
            )
        if verbose:
            print(f"  [{year}] sin datos utilizables (se omite).")
        return pd.DataFrame()

    df = _played_only(_season_window(_consolidate(blocks), year, verbose))
    if verbose:
        print(f"  [{year}] filas equipo-partido (jugados): {len(df)}")
    return df


def _current_season_year(now: datetime | None = None) -> int:
    """Ano de inicio de la temporada vigente (LaLiga arranca en agosto)."""
    now = now or datetime.now()
    return now.year if now.month >= 7 else now.year - 1


# --------------------------------------------------------------------------- #
# Orquestacion backfill / update
# --------------------------------------------------------------------------- #
def backfill(
    seasons=None,
    *,
    master_csv: Path | str = _MASTER_CSV,
    leagues: str = LEAGUE,
    proxy=None,
    data_dir: Path | str | None = None,
    stat_types: list[str] | None = None,
    headless: bool = True,
    browser: str | None = None,
    pause: float = 2.0,
) -> pd.DataFrame:
    """Descarga TODO el historico (por defecto 2016/17 -> temporada vigente).

    `seasons`: iterable de anos de inicio (2016 = 16/17). Idempotente: salta los
    `row_id` que ya esten en el master. Checkpoint por temporada -> reanudable.

    DURACION: FBref limita el ritmo (~7 s/pagina, lo respeta soccerdata) y cada
    temporada son ~1 + 20 equipos x 4 bloques = ~81 paginas -> ~12 min por
    temporada, ~2 h el historico completo la primera vez. Las temporadas
    cerradas quedan en cache: re-ejecutar es casi instantaneo.
    """
    if seasons is None:
        seasons = range(FIRST_SEASON, _current_season_year() + 1)
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master, existing = _load_master(master_csv)
    df_ckpt, ckpt_ids = _load_checkpoint(master_csv)
    new_rows: list = df_ckpt.to_dict("records") if not df_ckpt.empty else []
    existing = existing | ckpt_ids

    blocked_streak = 0
    for year in sorted(set(int(y) for y in seasons)):
        print(f"\n=== Temporada {year}/{str(year + 1)[2:]} ===")
        try:
            df_year = _fetch_season(year, leagues=leagues, proxy=proxy,
                                    data_dir=data_dir, stat_types=stat_types,
                                    headless=headless, browser=browser)
        except FBrefDownloadBlocked as e:
            blocked_streak += 1
            print(f"  [WARN] {e}")
            if blocked_streak >= 2:
                _finalize_master(df_master, new_rows, master_csv)
                raise SystemExit(
                    "\n[ERROR] 2 temporadas seguidas bloqueadas por descarga: "
                    "FBref esta rechazando la conexion. Lo descargado esta a "
                    "salvo (master/checkpoint). Prueba, por este orden: "
                    "1) esperar 30-60 min; 2) ejecutar con --visible (Chrome "
                    "visible evita algunos retos de Cloudflare); 3) cambiar de "
                    "red o usar --proxy. Al re-ejecutar se reanuda solo."
                )
            time.sleep(pause)
            continue
        blocked_streak = 0
        if df_year.empty:
            time.sleep(pause)
            continue
        fresh = df_year[~df_year[_KEY_COL].isin(existing)]
        print(f"  nuevos (no presentes): {len(fresh)}")
        if not fresh.empty:
            new_rows.extend(fresh.to_dict("records"))
            existing |= set(fresh[_KEY_COL])
            _save_checkpoint(new_rows, master_csv)  # persiste tras cada temporada
        time.sleep(pause)

    return _finalize_master(df_master, new_rows, master_csv)


def update(
    *,
    master_csv: Path | str = _MASTER_CSV,
    recent_seasons: int = 2,
    leagues: str = LEAGUE,
    proxy=None,
    data_dir: Path | str | None = None,
    stat_types: list[str] | None = None,
    headless: bool = True,
    browser: str | None = None,
    pause: float = 2.0,
) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE. Anade los partidos jugados nuevos.

    Escanea las `recent_seasons` temporadas mas recientes (por defecto 2 =
    vigente + anterior), para cubrir el cambio de temporada sin dejar huecos.
    Idempotente por `row_id`. La temporada en curso se re-descarga siempre
    (soccerdata no cachea la temporada vigente): ~10-12 min por ejecucion.
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

    blocked = False
    for year in years:
        try:
            df_year = _fetch_season(year, leagues=leagues, proxy=proxy,
                                    data_dir=data_dir, stat_types=stat_types,
                                    headless=headless, browser=browser)
        except FBrefDownloadBlocked as e:
            print(f"  [WARN] {e}")
            blocked = True
            time.sleep(pause)
            continue
        if df_year.empty:
            time.sleep(pause)
            continue
        fresh = df_year[~df_year[_KEY_COL].isin(existing)]
        print(f"  [{year}] nuevos jugados: {len(fresh)}")
        if not fresh.empty:
            new_rows.extend(fresh.to_dict("records"))
            existing |= set(fresh[_KEY_COL])
            _save_checkpoint(new_rows, master_csv)
        time.sleep(pause)

    if blocked:
        print("[WARN] hubo temporadas bloqueadas por descarga; el proximo "
              "'update' las recupera (idempotente). Si persiste: --visible, "
              "otra red o --proxy.")
    return _finalize_master(df_master, new_rows, master_csv)


def _load_master(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el CSV maestro. Devuelve (dataframe, row_id ya presentes)."""
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        ids = set(df[_KEY_COL].unique()) if _KEY_COL in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} filas equipo-partido.")
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
#   * reanudacion automatica: al re-ejecutar, los row_id del checkpoint se tratan
#     como ya presentes -> no se vuelven a descargar y se funden.
#   * escritura garantizada en Windows: antes de guardar se retira el atributo
#     "solo lectura" (que OneDrive suele poner) y se reintenta si hay candado.
def _ckpt_path(master_csv: Path) -> Path:
    return _common.ckpt_path(master_csv)


def _load_checkpoint(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el checkpoint pendiente. Devuelve (df, {row_id ya descargados})."""
    return _common.load_checkpoint(master_csv, key_col=_KEY_COL)


def _save_checkpoint(new_rows: list, master_csv: Path) -> None:
    """Reescribe el checkpoint con todo lo descargado hasta ahora."""
    _common.save_checkpoint(new_rows, master_csv)


# alias finos: implementacion compartida en _common (misma para todas las fuentes)
_clear_readonly = _common.clear_readonly
_write_df_safe = _common.write_df_safe


def _finalize_master(df_master: pd.DataFrame, new_rows: list,
                     master_csv: Path) -> pd.DataFrame:
    """Funde las filas nuevas en el maestro, lo guarda y limpia el checkpoint."""
    if not new_rows:
        print("[INFO] no hay filas nuevas: el maestro ya esta al dia.")
        return df_master
    df_new = pd.DataFrame(new_rows)
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty],
                          ignore_index=True)
    if _KEY_COL in df_master.columns:
        df_master = df_master.drop_duplicates(_KEY_COL, keep="last")
    if "date" in df_master.columns:
        df_master = df_master.sort_values("date").reset_index(drop=True)

    if _write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] stats FBref guardadas -> {master_csv}  "
              f"({len(df_master)} filas equipo-partido)")
        ckpt = _ckpt_path(master_csv)
        if ckpt.exists():
            try:
                ckpt.unlink()
            except Exception:  # noqa: BLE001
                pass
    return df_master


# --------------------------------------------------------------------------- #
# Calendario (jornada wk) — utilidad
# --------------------------------------------------------------------------- #
def run_schedule(
    seasons,
    *,
    leagues: str = LEAGUE,
    proxy=None,
    data_dir: Path | str | None = None,
    headless: bool = True,
    browser: str | None = None,
) -> pd.DataFrame:
    """Calendario FBref normalizado (util para derivar la jornada `wk`).

    `seasons`: iterable de ANOS de inicio (2016 = 16/17); se convierten a
    codigos inequivocos internamente. Devuelve una fila por partido con
    home_key/away_key/match_key/wk.
    """
    sd = _require_soccerdata()

    season_codes = [_season_str(int(y)) for y in seasons]
    kwargs = {"leagues": leagues, "seasons": season_codes, "headless": headless}
    if proxy is not None:
        kwargs["proxy"] = proxy
    if data_dir is not None:
        kwargs["data_dir"] = Path(data_dir)
    if browser is not None:
        kwargs["path_to_browser"] = Path(browser)

    fb = sd.FBref(**kwargs)
    try:
        sch = _flatten(fb.read_schedule().reset_index())
    finally:
        try:
            fb._driver.quit()  # noqa: SLF001
        except Exception:  # noqa: BLE001
            pass

    home_col = _first_present(sch, ["home_team", "home"])
    away_col = _first_present(sch, ["away_team", "away"])
    date_col = _first_present(sch, ["date"])
    wk_col = _first_present(sch, ["week", "wk", "round"])

    out = teamnames.add_keys(sch, home_col=home_col, away_col=away_col, date_col=date_col)
    if wk_col:
        out = out.rename(columns={wk_col: "wk"})
    return out


# =========================================================================== #
# B) CAPA DE ADAPTACION / NORMALIZACION  (solo pandas, testeable offline)
# =========================================================================== #
def load_processed(
    master_csv: Path | str = _MASTER_CSV,
    *,
    out_parquet: Path | str | None = _OUT_PARQUET,
    verbose: bool = True,
    strict: bool = True,
) -> pd.DataFrame:
    """Lee el CSV maestro, re-normaliza claves/tipos y guarda el parquet limpio.

    Salida: una fila por (equipo, partido) con row_id, match_key, team_key,
    opp_key, is_home, date, season y todas las columnas de stats FBref tipadas.

    `strict=True`: falla ruidosamente ante columnas ausentes, claves nulas,
    row_id duplicados o equipos sin clave canonica. A diferencia de fatigue,
    aqui SI se valida `opponent`: en FBref todas las filas son de LaLiga, asi
    que el rival tambien debe ser canonico.
    """
    master_csv = Path(master_csv)
    df = pd.read_csv(master_csv)
    _common.assert_columns(df, [_KEY_COL, "season", "date", "team", "opponent"],
                           source="fbref.load_processed")

    # re-normaliza claves por si el CSV se edito a mano
    if "team" in df.columns:
        df["team_key"] = df["team"].map(teamnames.to_key)
    if "opponent" in df.columns:
        df["opp_key"] = df["opponent"].map(teamnames.to_key)

    # re-deriva orientacion y match_key de forma robusta
    if "venue" in df.columns and "opponent" in df.columns:
        is_home = df["venue"].astype(str).str.lower().str.startswith("home")
        home = df["team"].where(is_home, df["opponent"])
        away = df["opponent"].where(is_home, df["team"])
        df["match_key"] = teamnames.match_key(df["date"], home, away)
        df["is_home"] = is_home

    # tipa como numerico todas las columnas de stats (todo lo que no es meta/clave)
    non_numeric = {
        "row_id", "match_key", "season", "date", "team", "opponent",
        "team_key", "opp_key", "is_home", "venue", "result", "game",
        "league", "day", "captain", "formation", "referee", "match_report",
        "notes", "time", "comp", "round",
    }
    for c in df.columns:
        if c not in non_numeric:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # validaciones ruidosas (antes de publicar nada)
    if strict:
        _common.assert_non_null(df, ["match_key", "team_key", "opp_key", "date"],
                                source="fbref.load_processed")
        _common.assert_unique(df, _KEY_COL, source="fbref.load_processed")
        names = pd.concat([df.get("team", pd.Series(dtype=object)),
                           df.get("opponent", pd.Series(dtype=object))])
        bad = set(names.dropna().unique()) - {
            n for n in names.dropna().unique()
            if teamnames.to_key(n) in teamnames.CANONICAL}
        if bad:
            teamnames.assert_known(bad, source="fbref.load_processed")

    if verbose:
        _report(df)

    if out_parquet is not None:
        out_parquet = Path(out_parquet)
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        if _write_df_safe(df, out_parquet, fmt="parquet", backup=False) and verbose:
            print(f"[OK] parquet normalizado -> {out_parquet}  "
                  f"({len(df)} filas, {df.shape[1]} cols)")
    return df


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def _first_present(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _report(df: pd.DataFrame) -> None:
    """Imprime un informe rapido de cobertura."""
    print("\n===== INFORME COBERTURA FBref =====")
    print(f"filas (equipo-partido): {len(df)}")
    if "match_key" in df:
        print(f"partidos unicos       : {df['match_key'].nunique()}")
    if "season" in df:
        seasons = sorted(str(s) for s in df["season"].dropna().unique().tolist())
        print(f"temporadas            : {seasons}")
    if "date" in df:
        d = pd.to_datetime(df["date"], errors="coerce")
        print(f"rango fechas          : {d.min()} -> {d.max()}")
    # cobertura de las columnas-hueco clave (las que justifican extraer FBref).
    # Coincidencia por SUBCADENA: robusta al nombre exacto que aplana FBref
    # (p.ej. 'keeper_expected_psxg', 'shooting_standard_dist', ...).
    for label, kw in (("psxg (portero)", "psxg"), ("paradas", "save"),
                      ("dist. tiro", "dist"), ("pases prog.", "prgp"),
                      ("acarreos prog.", "prgc"), ("sca (crea tiro)", "sca"),
                      ("entradas", "tkl"), ("intercepciones", "int"),
                      ("recuperaciones", "recov"), ("aereos ganados", "won")):
        cols = [c for c in df.columns if kw in str(c).lower()]
        if cols:
            print(f"{label:16s}: {cols[0]} -> {df[cols[0]].notna().mean() * 100:.1f}% no nulo")
    # equipos no reconocidos por la capa de normalizacion
    names = pd.concat(
        [df.get("team", pd.Series(dtype=object)), df.get("opponent", pd.Series(dtype=object))]
    ).dropna().unique()
    unknown = teamnames.unknown_names(names)
    print(f"equipos NO reconocidos: {unknown if unknown else '[] (ok)'}")
    print("===================================\n")


# =========================================================================== #
# CLI
# =========================================================================== #
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Extractor FBref equipo-partido (La Liga).")
    p.add_argument("cmd",
                   choices=["backfill", "update", "process", "weekly", "schedule"],
                   help="backfill=historico | update=nuevos | process=normaliza | "
                        "weekly=update+process | schedule=calendario (wk)")
    p.add_argument("--seasons", nargs="+", type=int, default=None,
                   help="anos de inicio (2016 2017 ...). Def.: 2016 -> vigente.")
    p.add_argument("--master", default=str(_MASTER_CSV))
    p.add_argument("--out", default=str(_OUT_PARQUET))
    p.add_argument("--recent", type=int, default=2,
                   help="update/weekly: n temporadas mas recientes a escanear (def. 2)")
    p.add_argument("--proxy", default=None)
    p.add_argument("--data-dir", default=None, help="cache local de soccerdata")
    p.add_argument("--stats", nargs="+", default=None,
                   help="stat_type a bajar (def.: schedule shooting keeper misc)")
    p.add_argument("--visible", action="store_true",
                   help="Chrome VISIBLE (no headless); ayuda si Cloudflare bloquea")
    p.add_argument("--browser", default=None,
                   help="ruta al ejecutable de Chrome (opcional)")
    args = p.parse_args()
    headless = not args.visible

    if args.cmd == "backfill":
        backfill(seasons=args.seasons, master_csv=args.master,
                 proxy=args.proxy, data_dir=args.data_dir,
                 stat_types=args.stats, headless=headless, browser=args.browser)
    elif args.cmd == "update":
        update(master_csv=args.master, recent_seasons=args.recent,
               proxy=args.proxy, data_dir=args.data_dir,
               stat_types=args.stats, headless=headless, browser=args.browser)
    elif args.cmd == "process":
        load_processed(master_csv=args.master, out_parquet=args.out)
    elif args.cmd == "weekly":
        update(master_csv=args.master, recent_seasons=args.recent,
               proxy=args.proxy, data_dir=args.data_dir,
               stat_types=args.stats, headless=headless, browser=args.browser)
        if _ckpt_path(Path(args.master)).exists():
            print("[WARN] el master no se guardo (checkpoint pendiente); "
                  "NO regenero el parquet para no dejarlo desactualizado. "
                  "Resuelve el bloqueo y re-ejecuta 'weekly'.")
        else:
            load_processed(master_csv=args.master, out_parquet=args.out)
    elif args.cmd == "schedule":
        seasons = args.seasons or list(range(FIRST_SEASON, _current_season_year() + 1))
        sch = run_schedule(seasons=seasons, proxy=args.proxy,
                           data_dir=args.data_dir, headless=headless,
                           browser=args.browser)
        print(sch.head().to_string())
        print(f"\n[OK] calendario: {len(sch)} partidos.")
