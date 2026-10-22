"""
Extractor football-data.co.uk (resultado + cuotas) para laliga-predictor-engine.

Fuente: football-data.co.uk, CSV por temporada de LaLiga (division "SP1"):
    https://www.football-data.co.uk/mmz4281/<CODIGO>/SP1.csv
donde <CODIGO> = 2 digitos del ano de inicio + 2 del de fin (2016/17 -> "1617").

Por que es la fuente ESPINA del modelo
--------------------------------------
Todo lo demas (Elo, xG, estilo, valor de plantilla) son FEATURES que se enganchan
a un partido. football-data aporta las dos cosas que faltaban y que son el centro
de un modelo 1X2:

    1) RESULTADO = la variable objetivo (1/X/2). Marcador final y al descanso.
    2) CUOTAS de multiples casas = a la vez el BENCHMARK (batir a la casa), la base
       del ROI, y la senal individual mas fuerte que existe (consenso de mercado).

Que se guarda
-------------
En la EXTRACCION el maestro crudo es un ESPEJO FIEL de la fuente: se conservan
TODAS las columnas originales de football-data verbatim (resultado, cuotas de
todas las casas Y las stats de tiros/faltas/corners/tarjetas), mas metadatos de
temporada y claves de cruce. football-data trae stats que se solapan con
Sofascore/FBref, pero la decision de cuales quedarse y de que fuente se toma MAS
TARDE, en la fase de consolidacion; aqui no se descarta nada.

La capa PROCESADA (parquet) es la vista curada y lista para el modelo: la espina
de resultado + un set de cuotas NORMALIZADO y consistente entre temporadas
(resuelve el cambio de nomenclatura Bb*->Max/Avg y la aparicion de las cuotas de
cierre "C"), mas las probabilidades implicitas del mercado.

Estructura del modulo (identica a los demas extractores)
--------------------------------------------------------
A) CAPA DE SCRAPING (corre en LOCAL, red abierta):
     backfill(seasons) -> baja el CSV de cada temporada al maestro
                          `footballdata_laliga.csv`.
     update(recent_seasons) -> EJECUTABLE SEMANALMENTE: re-baja la(s) temporada(s)
                          en curso (football-data va anadiendo partidos y sus
                          resultados) y hace upsert. Idempotente por fixture.

B) CAPA DE ADAPTACION / NORMALIZACION (solo pandas, testeable offline):
     load_processed(master_csv) -> lee el maestro, construye las cuotas
                          normalizadas + probabilidades implicitas y guarda el
                          parquet `footballdata_match.parquet` con `match_key`.

Idempotencia: NO hay id de partido en football-data. Se usa la IDENTIDAD DE
FIXTURE (season, home_key, away_key) -> cada emparejamiento ocurre una vez por
temporada, sea cual sea la fecha (robusto ante aplazamientos). `match_key`
(fecha|home|away) se conserva como clave de cruce con las demas fuentes.

IMPORTANTE: la capa A exige salida a internet, por lo que se ejecuta siempre en la
maquina local. Requiere pandas y, opcionalmente, curl_cffi.

Uso tipico (en local)
---------------------
    python scripts/extractors/footballdata.py weekly       # update + parquet
    python scripts/extractors/footballdata.py backfill     # 2016/17 -> hoy
"""

from __future__ import annotations

import io
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

try:  # ejecucion como paquete (from scripts.extractors import footballdata)
    from . import _common
    from . import teamnames
except ImportError:  # ejecucion como script suelto
    import _common  # type: ignore
    import teamnames  # type: ignore


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
BASE_URL = "https://www.football-data.co.uk/mmz4281"
LEAGUE_CODE = "SP1"                 # Primera Division en football-data
FIRST_SEASON = 2016                 # 2016 = temporada 2016/17 (casa con las demas fuentes)

# raiz del repo: .../scripts/extractors/footballdata.py -> subir 3 -> repo/
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "footballdata"
# CSV maestro (crudo: resultado + TODAS las cuotas, sin las stats que solapan).
_MASTER_CSV = _RAW_DIR / "footballdata_laliga.csv"
# Parquet limpio (tipado, cuotas normalizadas) listo para el cruce.
_OUT_PARQUET = _REPO / "data" / "02_processed" / "footballdata_match.parquet"

# --- PROXIMOS PARTIDOS (cuotas pre-partido, semanal) ----------------------- #
# football-data publica un unico fixtures.csv con la ventana de ~7 dias de TODAS
# las ligas y SOLO cuotas de APERTURA (sin resultado ni cierre). Es la fuente
# semanal de cuotas pre-partido: se baja el viernes/sabado, se filtra SP1 y se
# predice antes de que se juegue la jornada.
FIXTURES_URL = "https://www.football-data.co.uk/fixtures.csv"
# Crudo acumulado de proximos partidos SP1 (espejo fiel de la parte SP1 del csv).
_UPCOMING_CSV = _RAW_DIR / "footballdata_upcoming.csv"
# Parquet normalizado (solo cuotas de apertura + prob. implicitas; sin resultado).
_UPCOMING_PARQUET = _REPO / "data" / "02_processed" / "footballdata_upcoming.parquet"

# --- CUOTAS TECLEADAS A MANO (registro historico) -------------------------- #
# Vive en el crudo y NO se deriva de nada: es la unica entrada del proyecto que
# no procede de una fuente. Lo escribe `build_scoring` al promover lo que se
# teclea para emitir una jornada, y lo relee `load_processed` cada semana.
#
# Por que un artefacto propio y no una edicion del CSV maestro: `_merge_upsert`
# funde por (season_year, home_key, away_key) con keep='last', de modo que la
# siguiente descarga de football-data PISA cualquier valor escrito a mano. El
# comentario "re-normaliza claves por si el maestro se edito a mano" anticipa la
# edicion manual pero no la protege. Separandolo, el maestro sigue siendo copia
# fiel de la fuente y el override se aplica despues, en la capa de adaptacion.
MANUAL_ODDS_HIST = _RAW_DIR / "footballdata_odds_manual.csv"
MANUAL_ODDS_COLS = ["match_key", "date", "home_team", "away_team",
                    "odds_h", "odds_d", "odds_a", "entered_ts"]

# Clave de idempotencia = identidad de fixture (robusta ante aplazamientos).
_KEY_COLS = ["season_year", "home_key", "away_key"]

# Referencia (NO se aplica en extraccion): estadisticas de football-data que se
# solapan con Sofascore/FBref. Se documentan aqui para la fase de consolidacion,
# donde se decidira que columnas quedarse y de que fuente. El maestro CRUDO las
# conserva todas (espejo fiel de la fuente).
_OVERLAP_STATS = {
    "HS", "AS", "HST", "AST", "HF", "AF", "HC", "AC", "HY", "AY", "HR", "AR",
    "HHW", "AHW", "HO", "AO", "HBP", "ABP", "HFKC", "AFKC", "Attendance",
}

# Cabeceras base de navegador.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


# =========================================================================== #
# A) CAPA DE SCRAPING
# =========================================================================== #

class _NotFound(Exception):
    """El CSV de la temporada aun no existe (404)."""


def _season_code(year: int) -> str:
    """2016 -> '1617' (formato de la ruta de football-data)."""
    return f"{str(year)[2:]}{str(year + 1)[2:]}"


def _season_label(year: int) -> str:
    """2016 -> '2016/17'."""
    return f"{year}/{str(year + 1)[2:]}"


def _current_season_year(now: datetime | None = None) -> int:
    """Ano de inicio de la temporada vigente (LaLiga arranca en agosto)."""
    now = now or datetime.now()
    return now.year if now.month >= 7 else now.year - 1


# --- transporte (delega en _common; curl_cffi si esta, si no urllib) -------- #
def _http_get_bytes(url: str, *, headers: dict, timeout: int = 30,
                    retries: int = 3, wait: float = 2.0) -> tuple[int, bytes]:
    """GET -> (status, cuerpo en BYTES: los CSV de football-data suelen venir en
    latin-1 y decodifica el parser). Implementacion en `_common.http_get_bytes`."""
    return _common.http_get_bytes(url, headers=headers, timeout=timeout,
                                  retries=retries, wait=wait)


# --- parseo (funciones puras, testeables sin red) -------------------------- #
def _read_csv_bytes(raw: bytes) -> pd.DataFrame:
    """Lee un CSV de football-data desde bytes, tolerante a codificacion y basura.

    Prueba utf-8 y luego latin-1; salta lineas corruptas; descarta columnas
    'Unnamed' (football-data suele dejar columnas vacias al final).
    """
    last_exc: Exception | None = None
    for enc in ("utf-8-sig", "latin-1"):
        try:
            df = pd.read_csv(io.BytesIO(raw), encoding=enc, on_bad_lines="skip")
            break
        except Exception as e:  # noqa: BLE001
            last_exc = e
            df = None
    if df is None:
        raise RuntimeError(f"no pude parsear el CSV: {last_exc}")
    # limpia columnas basura y espacios
    df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed")]]
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _clean_season_frame(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Anade metadatos de temporada y claves de equipo/partido, SIN quitar columnas.

    En la capa de extraccion el maestro es un ESPEJO FIEL de la fuente: se
    conservan TODAS las columnas originales de football-data (resultado, cuotas de
    todas las casas Y las stats de tiros/faltas/corners/tarjetas). Aunque algunas
    stats se solapan con Sofascore/FBref, la decision de cuales quedarse y de que
    fuente se toma MAS TARDE, en la fase de consolidacion; aqui no se descarta nada.
    """
    # filas validas: con ambos equipos
    df = df[df.get("HomeTeam").notna() & df.get("AwayTeam").notna()].copy()
    if df.empty:
        return df
    # reindexar 0..n-1 ANTES de anadir claves: match_key() devuelve una Series con
    # indice reseteado; si el df conservara el indice original (con huecos por el
    # filtrado) la asignacion se alinearia mal y meteria NaN.
    df = df.reset_index(drop=True)

    # tipa goles
    for c in ("FTHG", "FTAG", "HTHG", "HTAG"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # metadatos de temporada + claves de equipo/partido
    df.insert(0, "season", _season_label(year))
    df.insert(1, "season_year", year)
    df["home_key"] = df["HomeTeam"].map(teamnames.to_key)
    df["away_key"] = df["AwayTeam"].map(teamnames.to_key)
    df["match_key"] = teamnames.match_key(df["Date"], df["HomeTeam"], df["AwayTeam"])
    # date ISO normalizada (football-data usa dd/mm/yy o dd/mm/yyyy)
    df["date"] = df["match_key"].str.slice(0, 10)
    return df.reset_index(drop=True)


def parse_season_csv(raw: bytes, year: int) -> pd.DataFrame:
    """Nucleo puro: bytes del CSV de una temporada -> DataFrame limpio con claves."""
    return _clean_season_frame(_read_csv_bytes(raw), year)


# --- descarga de una temporada -------------------------------------------- #
def _fetch_season(year: int, *, verbose: bool = True) -> pd.DataFrame:
    """Descarga y limpia una temporada. DataFrame vacio si aun no existe (404)."""
    url = f"{BASE_URL}/{_season_code(year)}/{LEAGUE_CODE}.csv"
    status, body = _http_get_bytes(url, headers=_HEADERS)
    if status == 404:
        if verbose:
            print(f"  [{_season_label(year)}] aun no existe (404); se omite.")
        return pd.DataFrame()
    if status != 200:
        raise RuntimeError(f"HTTP {status} en {url}")
    df = parse_season_csv(body, year)
    if verbose:
        played = int(df["FTR"].notna().sum()) if "FTR" in df.columns else len(df)
        print(f"  [{_season_label(year)}] partidos: {len(df)} (con resultado: {played})")
    return df


# --- orquestacion backfill / update ---------------------------------------- #
def backfill(
    seasons=None,
    *,
    master_csv: Path | str = _MASTER_CSV,
    pause: float = 1.0,
) -> pd.DataFrame:
    """Descarga TODO el historico (por defecto 2016/17 -> vigente).

    Idempotente por (season_year, home_key, away_key). Checkpoint por temporada,
    reanudable. Upsert keep='last' -> re-bajar una temporada actualiza resultados
    y cuotas que se hayan completado.
    """
    if seasons is None:
        seasons = range(FIRST_SEASON, _current_season_year() + 1)
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master = _load_master(master_csv)
    df_ckpt, done_years = _load_checkpoint(master_csv)
    frames: list[pd.DataFrame] = [df_ckpt] if not df_ckpt.empty else []

    for year in sorted({int(y) for y in seasons}):
        if year in done_years:
            print(f"=== {_season_label(year)}: ya en checkpoint; se omite. ===")
            continue
        print(f"\n=== Temporada {_season_label(year)} ===")
        df_season = _fetch_season(year)
        if not df_season.empty:
            frames.append(df_season)
            _save_checkpoint(frames, master_csv)  # persiste tras cada temporada
        time.sleep(pause)

    return _finalize_master(df_master, frames, master_csv)


def update(
    *,
    master_csv: Path | str = _MASTER_CSV,
    recent_seasons: int = 2,
    pause: float = 1.0,
) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE. Re-baja las temporadas recientes y hace upsert.

    Escanea las `recent_seasons` mas recientes (por defecto 2 = vigente +
    anterior). Idempotente: los fixtures existentes se sobrescriben con el dato
    fresco (resultado ya jugado, cuotas de cierre completadas) y se anaden nuevos.
    """
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master = _load_master(master_csv)
    # A diferencia de backfill, aqui NO se salta por checkpoint: la semantica de
    # `update` es re-bajar SIEMPRE las temporadas recientes (football-data
    # completa resultados y cuotas de cierre a posteriori). Un checkpoint
    # rancio de una ejecucion fallida anterior haria pasar datos de hace dias
    # por frescos. El ckpt pendiente se funde igualmente (el dato fresco gana:
    # va despues en el concat y el upsert es keep='last').
    df_ckpt, _ = _load_checkpoint(master_csv)
    frames: list[pd.DataFrame] = [df_ckpt] if not df_ckpt.empty else []

    cur = _current_season_year()
    years = sorted({cur - i for i in range(max(1, recent_seasons))
                    if cur - i >= FIRST_SEASON}, reverse=True)
    print(f"[INFO] Temporadas a escanear: {years}")

    for year in years:
        df_season = _fetch_season(year)
        if not df_season.empty:
            frames.append(df_season)
            _save_checkpoint(frames, master_csv)
        time.sleep(pause)

    return _finalize_master(df_master, frames, master_csv)


# --- proximos partidos (fixtures.csv) -------------------------------------- #
def _clean_fixtures_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra la liga espanola (SP1) del fixtures.csv y anade claves, SIN quitar
    columnas (espejo fiel de la parte SP1 de la fuente).

    fixtures.csv trae la ventana de proximos ~7 dias de TODAS las ligas con solo
    cuotas de APERTURA: no hay resultado (FTHG/FTAG/FTR) ni cuotas de cierre. La
    `season_year` se DERIVA de la fecha de cada partido (LaLiga arranca en agosto),
    porque el csv no la trae.
    """
    if "Div" not in df.columns:
        # Sin 'Div' NO se puede filtrar SP1: seguir adelante meteria los proximos
        # partidos de TODAS las ligas en el master de upcoming, en silencio.
        raise RuntimeError(
            "[footballdata] fixtures.csv ya NO trae la columna 'Div': "
            "football-data ha cambiado el esquema del fichero de proximos "
            f"partidos. Columnas recibidas: {list(df.columns)[:20]}. "
            "Revisa el CSV y adapta _clean_fixtures_frame antes de re-ejecutar."
        )
    div = df["Div"].astype("string").str.strip()
    df = df[div == LEAGUE_CODE].copy()
    df = df[df.get("HomeTeam").notna() & df.get("AwayTeam").notna()].copy()
    if df.empty:
        return df
    # reindexar antes de anadir claves (match_key devuelve Series reseteada)
    df = df.reset_index(drop=True)

    df["home_key"] = df["HomeTeam"].map(teamnames.to_key)
    df["away_key"] = df["AwayTeam"].map(teamnames.to_key)
    df["match_key"] = teamnames.match_key(df["Date"], df["HomeTeam"], df["AwayTeam"])
    df["date"] = df["match_key"].str.slice(0, 10)

    # season_year a partir de la fecha ISO (mes >= 7 -> temporada que empieza ese ano)
    dt = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    yr = dt.dt.year
    season_year = yr.where(dt.dt.month >= 7, yr - 1).astype("Int64")
    df.insert(0, "season_year", season_year)
    df.insert(0, "season", season_year.map(
        lambda y: _season_label(int(y)) if pd.notna(y) else pd.NA))
    return df.reset_index(drop=True)


def parse_fixtures_csv(raw: bytes) -> pd.DataFrame:
    """Nucleo puro: bytes de fixtures.csv -> DataFrame SP1 limpio con claves."""
    return _clean_fixtures_frame(_read_csv_bytes(raw))


def upcoming(
    *,
    master_csv: Path | str = _UPCOMING_CSV,
    out_parquet: Path | str | None = _UPCOMING_PARQUET,
    verbose: bool = True,
) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE (viernes/sabado, antes de la jornada).

    Baja fixtures.csv, filtra la liga espanola (SP1) y hace UPSERT por identidad de
    fixture sobre un crudo acumulado. Asi se va construyendo un historico de las
    cuotas de APERTURA tal y como se veian ANTES de cada partido -> justo lo que el
    modelo puede usar como feature en el momento de predecir (usar cuotas de cierre
    como feature seria casi fuga de informacion).

    fixtures.csv es una ventana movil de ~7 dias; al acumular por upsert no perdemos
    los partidos que ya salieron de la ventana. Idempotente: re-ejecutar el mismo
    dia refresca las cuotas (football-data actualiza el csv viernes y martes).
    """
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    status, body = _http_get_bytes(FIXTURES_URL, headers=_HEADERS)
    if status != 200:
        raise RuntimeError(f"HTTP {status} en {FIXTURES_URL}")

    df_new = parse_fixtures_csv(body)
    if verbose:
        print(f"[INFO] fixtures.csv -> {len(df_new)} proximos partidos de SP1.")
    if df_new.empty:
        print("[INFO] ahora mismo no hay proximos partidos de SP1 en fixtures.csv "
              "(ventana de ~7 dias vacia: paron, fin de temporada, etc.).")
        return _load_master(master_csv)

    df_master = _load_master(master_csv)
    df_out = _merge_upsert(df_master, df_new)

    if _write_df_safe(df_out, master_csv, fmt="csv"):
        print(f"[OK] proximos SP1 guardados -> {master_csv}  ({len(df_out)} fixtures)")

    if out_parquet is not None:
        load_upcoming_processed(master_csv, out_parquet=out_parquet, verbose=verbose)
    return df_out


def _load_master(master_csv: Path) -> pd.DataFrame:
    """Carga el CSV maestro. DataFrame vacio si no existe."""
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        print(f"[INFO] master cargado: {len(df)} partidos.")
        return df
    print("[INFO] sin datos previos; se empieza de cero.")
    return pd.DataFrame()


# --------------------------------------------------------------------------- #
# Guardado a prueba de fallos (mismo patron que el resto de extractores)
# --------------------------------------------------------------------------- #
def _ckpt_path(master_csv: Path) -> Path:
    return _common.ckpt_path(master_csv)


def _load_checkpoint(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el checkpoint pendiente. Devuelve (df, {season_year ya bajados})."""
    df, years = _common.load_checkpoint(master_csv, key_col="season_year")
    return df, {int(y) for y in years}


def _save_checkpoint(frames: list, master_csv: Path) -> None:
    """Reescribe el checkpoint con todo lo bajado hasta ahora."""
    _common.save_checkpoint(frames, master_csv)


_clear_readonly = _common.clear_readonly
_write_df_safe = _common.write_df_safe


def _merge_upsert(df_master: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """Funde master + nuevo por (season_year, home_key, away_key), keep='last'.

    El fixture recien bajado sobrescribe al antiguo (resultado ya jugado, cuotas
    de cierre completadas) y se anaden los que no existian. Alinea columnas de
    distintas eras (union; las ausentes quedan NaN)."""
    both = [d for d in (df_master, df_new)
            if isinstance(d, pd.DataFrame) and not d.empty]
    if not both:
        return pd.DataFrame()
    out = pd.concat(both, ignore_index=True)
    # re-normaliza claves por si el master se edito a mano
    if "HomeTeam" in out.columns:
        out["home_key"] = out["HomeTeam"].map(teamnames.to_key)
        out["away_key"] = out["AwayTeam"].map(teamnames.to_key)
    out = out.drop_duplicates(subset=_KEY_COLS, keep="last")
    sort_cols = [c for c in ("season_year", "date") if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def _finalize_master(df_master: pd.DataFrame, frames: list[pd.DataFrame],
                     master_csv: Path) -> pd.DataFrame:
    """Funde lo bajado en el maestro, lo guarda y limpia el checkpoint."""
    good = [d for d in frames if isinstance(d, pd.DataFrame) and not d.empty]
    if not good:
        print("[INFO] no hay partidos nuevos: el maestro ya esta al dia.")
        return df_master
    df_new = pd.concat(good, ignore_index=True)
    df_out = _merge_upsert(df_master, df_new)

    if _write_df_safe(df_out, master_csv, fmt="csv"):
        print(f"[OK] football-data guardado -> {master_csv}  ({len(df_out)} partidos)")
        ckpt = _ckpt_path(master_csv)
        if ckpt.exists():
            try:
                ckpt.unlink()
            except Exception:  # noqa: BLE001
                pass
    return df_out


# =========================================================================== #
# B) CAPA DE ADAPTACION / NORMALIZACION  (solo pandas, testeable offline)
# =========================================================================== #
# Familias de cuotas con FALLBACK entre eras. football-data cambio la
# nomenclatura de los agregados de mercado (era antigua "Bb*" de BetBrain ->
# era nueva "Max*/Avg*") y anadio las cuotas de CIERRE ("*C*"). Cada destino
# normalizado toma la PRIMERA columna disponible de su lista de candidatos.
_ODDS_MAP: dict[str, list[str]] = {
    # --- 1X2 apertura ---
    "odds_b365_h": ["B365H"], "odds_b365_d": ["B365D"], "odds_b365_a": ["B365A"],
    "odds_ps_h": ["PSH", "PH"], "odds_ps_d": ["PSD", "PD"], "odds_ps_a": ["PSA", "PA"],
    "odds_max_h": ["MaxH", "BbMxH"], "odds_max_d": ["MaxD", "BbMxD"],
    "odds_max_a": ["MaxA", "BbMxA"],
    "odds_avg_h": ["AvgH", "BbAvH"], "odds_avg_d": ["AvgD", "BbAvD"],
    "odds_avg_a": ["AvgA", "BbAvA"],
    # --- 1X2 cierre (solo temporadas recientes; NaN en las viejas) ---
    "odds_close_avg_h": ["AvgCH"], "odds_close_avg_d": ["AvgCD"],
    "odds_close_avg_a": ["AvgCA"],
    "odds_close_max_h": ["MaxCH"], "odds_close_max_d": ["MaxCD"],
    "odds_close_max_a": ["MaxCA"],
    "odds_b365_close_h": ["B365CH"], "odds_b365_close_d": ["B365CD"],
    "odds_b365_close_a": ["B365CA"],
    # --- Over/Under 2.5 ---
    "odds_over25": ["Avg>2.5", "BbAv>2.5"], "odds_under25": ["Avg<2.5", "BbAv<2.5"],
    "odds_b365_over25": ["B365>2.5"], "odds_b365_under25": ["B365<2.5"],
    "odds_close_over25": ["AvgC>2.5"], "odds_close_under25": ["AvgC<2.5"],
    # --- Handicap asiatico (linea + cuotas medias) ---
    "ah_line": ["AHh", "BbAHh"],
    "odds_ah_home": ["AvgAHH", "BbAvAHH"], "odds_ah_away": ["AvgAHA", "BbAvAHA"],
    "ah_close_line": ["AHCh"],
    "odds_ah_close_home": ["AvgCAHH"], "odds_ah_close_away": ["AvgCAHA"],
}


def _first_col(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """Coalesce por FILA de la lista de candidatos (prioridad izquierda->derecha).

    Importante: el maestro mezcla eras (la vieja trae 'BbAvH', la nueva 'AvgH').
    Ambas columnas coexisten en el frame unido, pero cada FILA solo tiene una
    rellena. Por eso NO basta con "primera columna que exista" (devolveria la de
    la era nueva, dejando las filas viejas en NaN): hay que rellenar hueco a hueco
    tomando el primer candidato con valor en esa fila.
    """
    out = pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")
    for c in candidates:
        if c in df.columns:
            out = out.fillna(pd.to_numeric(df[c], errors="coerce").astype("Float64"))
    return out


# Rango plausible de la suma de inversas (overround) de un mercado 1X2 real: por
# debajo de 1 la casa perderia dinero seguro y por encima de ~1.6 el margen no
# corresponde a un precio publicado. Fuera de ese rango el trio se considera
# corrupto: se anula entero, porque la normalizacion acopla los tres resultados y
# con una cuota mala las otras dos probabilidades tampoco son interpretables.
_MIN_OVERROUND = 0.95
_MAX_OVERROUND = 1.60


def _implied_probs(h: pd.Series, d: pd.Series, a: pd.Series) -> pd.DataFrame:
    """Probabilidades implicitas 1X2 normalizadas (quita el margen de la casa).

    Una cuota decimal valida es > 1.0; un 0 o un negativo (basura de la fuente)
    produciria inf/probabilidades sin sentido, asi que se anula a NA y las tres
    probabilidades de esa fila quedan NA (mejor hueco visible que numero falso).
    La misma politica se aplica cuando el overround del trio sale del rango
    plausible: es la firma de un dato corrupto aunque cada cuota parezca valida.
    """
    h, d, a = (pd.to_numeric(x, errors="coerce").where(lambda v: v > 1.0)
               for x in (h, d, a))
    ih, idr, ia = 1.0 / h, 1.0 / d, 1.0 / a
    s = ih + idr + ia
    valido = s.notna() & (s >= _MIN_OVERROUND) & (s <= _MAX_OVERROUND)
    out = pd.DataFrame({
        "mkt_p_home": ih / s,
        "mkt_p_draw": idr / s,
        "mkt_p_away": ia / s,
    })
    return out.where(valido, other=pd.NA)


def read_manual_odds(path: Path | str = MANUAL_ODDS_HIST) -> pd.DataFrame:
    """Lee el registro de cuotas tecleadas. Devuelve el esquema vacio si no existe."""
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=MANUAL_ODDS_COLS)
    df = pd.read_csv(path, dtype={"match_key": str})
    for c in MANUAL_ODDS_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[MANUAL_ODDS_COLS]


def apply_manual_odds(out: pd.DataFrame, *, path: Path | str = MANUAL_ODDS_HIST,
                      verbose: bool = True) -> pd.DataFrame:
    """Hace PREVALECER en el historico la cuota que se teclo para emitir.

    El problema que resuelve es una discrepancia real y no cosmetica. Una jornada
    se predice con el precio que football-data publica ANTES del partido, y si no
    lo publica, con el que se teclea a mano. Una semana despues el partido se ha
    jugado, football-data lo publica en el CSV historico con SU precio, el maestro
    se reconstruye y toma ese: la prediccion se emitio a 2.10/3.40/3.30 y el
    historico la reentrena y la liquida a otra cosa. Nadie avisa, porque las dos
    cifras son legitimas.

    Alcance, y es una decision con criterio y no una omision
    -------------------------------------------------------
    El override toca EXCLUSIVAMENTE `odds_avg_h/d/a`, que es el canal de entrada
    del modelo: de ahi salen `mkt_p_home/draw/away`, las tres variables de precio
    de `SEMILLA_MERCADO`. No toca `odds_b365_*`, `odds_ps_*`, `odds_max_*` ni
    ninguna columna de cierre.

    El motivo es la regla 3 del proyecto: las columnas de Bet365 son el LISTON
    contra el que se publica toda cifra economica (`backtest.REFERENCIA = "b365"`).
    Si una cuota escrita a mano pudiera entrar ahi, la pregunta que motiva el TFM
    -- bate el modelo al mercado -- se contestaria contra un precio que no publico
    ninguna casa y que nadie puede auditar. Y hay un argumento mas simple todavia:
    cuando football-data publica el partido ya jugado trae la cuota de CIERRE, que
    es mejor dato que la de apertura que se vio al teclear. Pisarla seria degradar
    el historico, no corregirlo.

    De modo que lo que prevalece es con que precio PREDIJO el modelo, y lo que se
    conserva es a que precio se LIQUIDA. Son dos preguntas distintas y el override
    solo contesta la primera.

    El limite que lo hace defendible
    --------------------------------
    Solo se aplica si `entered_ts` es anterior al dia del partido. Es la regla 6
    escrita en codigo: una cuota teclada despues del pitido final es informacion
    del futuro, y sin esta comprobacion nada impediria "corregir" el precio de un
    partido de 2019 y mejorar el backtest de los bloques ya cerrados. Con ella, el
    override queda confinado por construccion a partidos que estaban por jugarse
    cuando se tecleo -- en la practica, la temporada en curso -- y los bloques de
    validacion (2017/18-2024/25) son intocables.

    Procedencia
    -----------
    Anade `odds_src` con "footballdata" o "manual". Sin esa columna una cuota
    teclada y una publicada se leerian igual, y la desviacion entre las dos -- que
    es medible y vale como resultado -- quedaria invisible.
    """
    man = read_manual_odds(path)
    if "odds_src" not in out.columns:
        out = out.copy()
        out["odds_src"] = "footballdata"
    if man.empty:
        return out

    man = man.dropna(subset=["match_key"])
    man = man[man[["odds_h", "odds_d", "odds_a"]].notna().all(axis=1)]
    man = man.drop_duplicates("match_key", keep="last")
    if man.empty:
        return out

    probs = _implied_probs(man["odds_h"], man["odds_d"], man["odds_a"])
    plausible = probs["mkt_p_home"].notna()
    dia = pd.to_datetime(man["match_key"].str.slice(0, 10), errors="coerce")
    tecleado = pd.to_datetime(man["entered_ts"], errors="coerce")
    a_tiempo = tecleado.notna() & (tecleado < dia)

    if verbose:
        for _, r in man.loc[~plausible].iterrows():
            print(f"[WARN] cuota manual descartada en {r['match_key']}: "
                  f"{r['odds_h']} / {r['odds_d']} / {r['odds_a']} no forman un "
                  "mercado 1X2 plausible.")
        for _, r in man.loc[plausible & ~a_tiempo].iterrows():
            print(f"[WARN] cuota manual descartada en {r['match_key']}: "
                  f"sello de entrada '{r['entered_ts']}' no es anterior al "
                  "partido, asi que seria informacion posterior al resultado.")

    man = man[plausible & a_tiempo]
    if man.empty:
        return out

    mmap = man.set_index("match_key")
    hit = out["match_key"].isin(mmap.index)
    if not hit.any():
        return out

    out = out.copy()
    idx = out.index[hit]
    claves = out.loc[idx, "match_key"]
    for dest, src in (("odds_avg_h", "odds_h"), ("odds_avg_d", "odds_d"),
                      ("odds_avg_a", "odds_a")):
        out.loc[idx, dest] = pd.to_numeric(
            claves.map(mmap[src]), errors="coerce").astype("Float64").values
    out.loc[idx, "odds_src"] = "manual"
    if verbose:
        print(f"[INFO] cuotas manuales aplicadas al consenso de entrada: "
              f"{int(hit.sum())} partido(s) (odds_src='manual'); las columnas de "
              "Bet365 y de cierre quedan intactas porque son el liston.")
    return out


def load_processed(
    master_csv: Path | str = _MASTER_CSV,
    *,
    out_parquet: Path | str | None = _OUT_PARQUET,
    verbose: bool = True,
    strict: bool = True,
) -> pd.DataFrame:
    """Lee el maestro y construye el parquet MODEL-READY.

    Salida: espina de resultado + claves + cuotas normalizadas (consistentes
    entre temporadas) + probabilidades implicitas del mercado (consenso Avg).

    `strict=True`: falla ruidosamente ante columnas ausentes, match_key nulos,
    fixtures duplicados o equipos sin clave canonica.
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        raise _common.RegressionError(
            f"[footballdata.load_processed] no existe el maestro '{master_csv}'. "
            "Publicar un parquet vacio destruiria el vigente (las aserciones de "
            "esquema no detectan un frame sin filas) y con el se iria el TARGET "
            "1X2 del proyecto. Ejecuta antes el backfill.")
    raw = pd.read_csv(master_csv)
    _common.assert_columns(raw, ["season", "season_year", "Date",
                                 "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"],
                           source="footballdata.load_processed")

    # re-normaliza claves por si el CSV se edito a mano
    if "HomeTeam" in raw.columns:
        raw["home_key"] = raw["HomeTeam"].map(teamnames.to_key)
        raw["away_key"] = raw["AwayTeam"].map(teamnames.to_key)
        raw["match_key"] = teamnames.match_key(raw["Date"], raw["HomeTeam"], raw["AwayTeam"])
        raw["date"] = raw["match_key"].str.slice(0, 10)

    base_cols = ["match_key", "season", "season_year", "date",
                 "HomeTeam", "AwayTeam", "home_key", "away_key",
                 "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR", "Referee"]
    out = pd.DataFrame(index=raw.index)
    for c in base_cols:
        if c in raw.columns:
            out[c] = raw[c]
    out = out.rename(columns={
        "HomeTeam": "home_team", "AwayTeam": "away_team",
        "FTHG": "fthg", "FTAG": "ftag", "FTR": "ftr",
        "HTHG": "hthg", "HTAG": "htag", "HTR": "htr", "Referee": "referee",
    })
    # dtype ESTABLE de los goles entre semanas: sin esto, read_csv da int64 con
    # la temporada completa y float64 si hay partidos pendientes, y el esquema
    # del parquet baila. Int64 (nullable) siempre.
    for c in ("fthg", "ftag", "hthg", "htag"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    # cuotas normalizadas
    for dest, cands in _ODDS_MAP.items():
        out[dest] = _first_col(raw, cands)

    # cuotas tecleadas a mano para emitir: PREVALECEN sobre el consenso de
    # football-data en el canal de entrada del modelo (ver `apply_manual_odds`).
    out = apply_manual_odds(out, verbose=verbose)

    # probabilidades implicitas del consenso (media de mercado)
    probs = _implied_probs(out["odds_avg_h"], out["odds_avg_d"], out["odds_avg_a"])
    out = pd.concat([out, probs], axis=1)

    if "date" in out.columns:
        out = out.sort_values(["season_year", "date"]).reset_index(drop=True)

    # validaciones ruidosas (antes de publicar nada)
    if strict:
        _common.assert_non_null(out, ["match_key", "home_key", "away_key"],
                                source="footballdata.load_processed")
        _common.assert_unique(out, ["season_year", "home_key", "away_key"],
                              source="footballdata.load_processed")
        bad = set()
        for side in ("home", "away"):
            m = ~out[f"{side}_key"].isin(teamnames.CANONICAL)
            bad |= set(out.loc[m, f"{side}_team"].dropna().unique())
        if bad:
            teamnames.assert_known(bad, source="footballdata.load_processed")

    if verbose:
        _report(out)

    if out_parquet is not None:
        _common.publish_df(out, out_parquet, source="footballdata.load_processed",
                           fmt="parquet", verbose=verbose)
    return out


def _report(df: pd.DataFrame) -> None:
    print("\n===== INFORME COBERTURA football-data =====")
    print(f"partidos              : {len(df)}")
    if "match_key" in df:
        print(f"match_key unicos      : {df['match_key'].nunique()}")
    if "season" in df:
        print(f"temporadas            : {sorted(df['season'].dropna().unique().tolist())}")
    if "ftr" in df:
        print(f"con resultado (FTR)   : {int(df['ftr'].notna().sum())}")
    for c in ("odds_avg_h", "odds_close_avg_h", "odds_over25", "ah_line"):
        if c in df:
            print(f"{c:18s}: {df[c].notna().mean() * 100:.1f}% no nulo")
    if {"mkt_p_home", "mkt_p_draw", "mkt_p_away"} <= set(df.columns):
        s = (df[["mkt_p_home", "mkt_p_draw", "mkt_p_away"]].sum(axis=1))
        print(f"probs implicitas suman ~1: media={s.mean():.3f}")
    # equipos no reconocidos por la capa de normalizacion
    bad = set()
    for side in ("home", "away"):
        namecol = f"{side}_team"
        if namecol in df:
            bad |= set(teamnames.unknown_names(df[namecol].dropna().unique()))
    print(f"equipos NO reconocidos: {sorted(bad) if bad else '[] (ok)'}")
    print("===========================================\n")


# Subconjunto de _ODDS_MAP con SOLO cuotas de APERTURA (sin las de cierre "*C*").
# Los proximos partidos aun no se han jugado: no existen cuotas de cierre.
_OPENING_ODDS_MAP: dict[str, list[str]] = {
    k: v for k, v in _ODDS_MAP.items() if "close" not in k
}


def load_upcoming_processed(
    master_csv: Path | str = _UPCOMING_CSV,
    *,
    out_parquet: Path | str | None = _UPCOMING_PARQUET,
    verbose: bool = True,
    strict: bool = True,
) -> pd.DataFrame:
    """Vista MODEL-READY de los proximos partidos: claves + cuotas de APERTURA
    normalizadas + probabilidades implicitas del mercado. SIN resultado ni cierre
    (no existen aun): es la foto de mercado disponible en el momento de predecir.
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        raise _common.RegressionError(
            f"[footballdata.load_upcoming_processed] no existe '{master_csv}'. "
            "Sin el fichero de proximos partidos no hay cuotas de apertura que "
            "publicar; ejecuta antes 'upcoming' para descargarlo.")
    raw = pd.read_csv(master_csv)

    # re-normaliza claves por si el CSV se edito a mano
    if "HomeTeam" in raw.columns:
        raw["home_key"] = raw["HomeTeam"].map(teamnames.to_key)
        raw["away_key"] = raw["AwayTeam"].map(teamnames.to_key)
        raw["match_key"] = teamnames.match_key(raw["Date"], raw["HomeTeam"], raw["AwayTeam"])
        raw["date"] = raw["match_key"].str.slice(0, 10)

    base_cols = ["match_key", "season", "season_year", "date", "Time",
                 "HomeTeam", "AwayTeam", "home_key", "away_key"]
    out = pd.DataFrame(index=raw.index)
    for c in base_cols:
        if c in raw.columns:
            out[c] = raw[c]
    out = out.rename(columns={
        "Time": "time", "HomeTeam": "home_team", "AwayTeam": "away_team",
    })

    # cuotas de apertura normalizadas (mismo fallback entre eras)
    for dest, cands in _OPENING_ODDS_MAP.items():
        out[dest] = _first_col(raw, cands)

    # probabilidades implicitas del consenso (media de mercado)
    probs = _implied_probs(out["odds_avg_h"], out["odds_avg_d"], out["odds_avg_a"])
    out = pd.concat([out, probs], axis=1)

    if "date" in out.columns:
        out = out.sort_values(["season_year", "date"]).reset_index(drop=True)

    # validaciones ruidosas (mismo contrato que el historico)
    if strict:
        _common.assert_non_null(out, ["match_key", "home_key", "away_key"],
                                source="footballdata.load_upcoming_processed")
        _common.assert_unique(out, ["season_year", "home_key", "away_key"],
                              source="footballdata.load_upcoming_processed")
        bad = set()
        for side in ("home", "away"):
            m = ~out[f"{side}_key"].isin(teamnames.CANONICAL)
            bad |= set(out.loc[m, f"{side}_team"].dropna().unique())
        if bad:
            teamnames.assert_known(bad, source="footballdata.load_upcoming_processed")

    if verbose:
        _report(out)

    if out_parquet is not None:
        # A diferencia del historico, este artefacto ES volatil por naturaleza: su
        # tamano es el de la proxima jornada y entre jornadas puede quedarse a
        # cero. Por eso se publica sin exigir no-regresion de filas (pero con
        # copia .bak, como el resto).
        _common.publish_df(out, out_parquet,
                           source="footballdata.load_upcoming_processed",
                           fmt="parquet", min_ratio=0.0, allow_empty=True,
                           verbose=verbose)
    return out


# =========================================================================== #
# CLI
# =========================================================================== #
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Extractor football-data.co.uk LaLiga "
                                            "(resultado + cuotas).")
    p.add_argument("cmd",
                   choices=["backfill", "update", "process", "weekly", "upcoming"],
                   help="backfill=historico | update=recientes | process=normaliza | "
                        "weekly=update+process | upcoming=proximos partidos (cuotas "
                        "de apertura, semanal)")
    p.add_argument("--seasons", nargs="+", type=int, default=None,
                   help="anos de inicio (2016 2017 ...). Def.: 2016 -> vigente.")
    p.add_argument("--master", default=str(_MASTER_CSV))
    p.add_argument("--recent", type=int, default=2,
                   help="update/weekly: n temporadas mas recientes a escanear (def. 2)")
    args = p.parse_args()

    if args.cmd == "backfill":
        backfill(seasons=args.seasons, master_csv=args.master)
    elif args.cmd == "update":
        update(master_csv=args.master, recent_seasons=args.recent)
    elif args.cmd == "process":
        load_processed(master_csv=args.master)
    elif args.cmd == "upcoming":
        # proximos partidos: master propio (_UPCOMING_CSV), no el historico SP1
        upcoming()
    elif args.cmd == "weekly":
        update(master_csv=args.master, recent_seasons=args.recent)
        if _ckpt_path(Path(args.master)).exists():
            print("[WARN] el master no se guardo (checkpoint pendiente); "
                  "NO regenero el parquet para no dejarlo desactualizado. "
                  "Resuelve el bloqueo y re-ejecuta 'weekly'.")
        else:
            load_processed(master_csv=args.master)
