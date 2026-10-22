"""
Extractor ClubElo (rating Elo de clubes) para laliga-predictor-engine.

Que aporta ClubElo
------------------
Un rating Elo por club que evoluciona con cada partido. A diferencia de las demas
fuentes (orientadas a partido), aqui el dato es una SERIE TEMPORAL POR CLUB: cada
tramo [from, to] tiene un Elo. En la consolidacion se cruza con cada partido por
`merge_asof` sobre (team_key, fecha) -> el Elo vigente del local y del visitante
justo antes del partido (senal de fuerza pre-partido, sin fuga de informacion).

Dos vias, y por que la viva es la WEB
-------------------------------------
VIA WEB (`clubelo.com`, la que se usa) -- `--via web`, el valor por omision.
    ClubElo rehizo el sitio y jubilo la API CSV: `api.clubelo.com` responde 502
    en todos sus endpoints, en el sitio nuevo no queda ni un `.csv`, ni enlace de
    descarga, ni mencion de la API, y cualquier ruta desconocida devuelve la
    portada con 200. No es una caida pasajera; es una jubilacion.

    Lo que SI publica la web, y de donde sale el dato:
      1) PAGINA DE FEDERACION  GET /ESP  -> censo vigente (20 clubes de Primera
         + 5 de Segunda) con su `Level`, y la tabla del Ranking con el slug de
         URL de cada uno. Los slugs son irregulares (`atletico`, `rc-celta`,
         `realmadrid`, `athletic-club`, `Sociedad`), asi que hay que LEERLOS, no
         deducirlos.
      2) PAGINA DE CLUB  GET /<slug>  -> la serie Elo completa del club, un punto
         por partido, incrustada en el especificador Vega-Lite de la grafica.

    Dos trampas que el modulo resuelve:
      * EL DATO ES POSTERIOR AL PARTIDO. El Elo que la web fecha en D es el de
        DESPUES de jugar el partido de D: la tabla de partidos lo publica como
        'New Elo' y la grafica repite ese mismo numero. Pegarlo tal cual con
        `merge_asof` le daria a cada partido su propio resultado. Por eso el
        tramo que abre un punto empieza en D+1 (misma convencion que usaba el
        CSV de la API) y se cierra en la fecha del punto siguiente.
      * UN SLUG INEXISTENTE DEVUELVE 200. La portada se sirve con exito ante
        cualquier ruta, asi que no hay 404 que delate el error. El guardian de
        identidad exige que la pagina traiga titulo, puntos con fecha y que ese
        titulo resuelva a la clave canonica que se pidio.

    Limite conocido: la serie de la web arranca en 2022-10 y no hay forma de
    pedir antes (las rutas con fecha, `/Results`, `/History` y `?from=` estan
    muertas), asi que por si sola no cubre la ventana del proyecto.

VIA API (`api.clubelo.com`, jubilada) -- `--via api`. Ya no responde, pero el CSV
    maestro que dejo sigue en disco y es el unico Elo que existe para las
    temporadas 16/17 a 22/23. Se conserva por eso, no por nostalgia.

EL EMPALME. Las dos vias publican el Elo en ESCALAS DISTINTAS (la web va ~85
    puntos por encima y comprime el rango un 11%), de modo que concatenarlas en
    nivel meteria un escalon de fuerza falso para todos los clubes a la vez. Cada
    via mantiene su propio CSV maestro, y la serie que se PUBLICA es una sola,
    empalmada en `ELO_CORTE` por `serie_empalmada`: el archivo hasta esa fecha, la
    web desde ella, con una columna `fuente` que dice de donde sale cada tramo. Lo
    que hace legitimo el empalme es que el modelo no consume el Elo en nivel sino
    tipificado dentro de la jornada, y la tipificacion es invariante al cambio de
    escala; quien tipifica es `consolidate.spine.add_clubelo`.

Estructura del modulo (identica a los demas extractores)
--------------------------------------------------------
A) CAPA DE SCRAPING (corre en LOCAL, red abierta):
     backfill_web()  -> descubre el censo + resuelve slugs y baja la serie Elo
                        completa de cada club al CSV maestro de la via web.
     update_web()    -> EJECUTABLE SEMANALMENTE: re-baja la serie de los clubes
                        del censo vigente (extiende el tramo abierto + anade la
                        semana nueva). Idempotente por (team_key, from).
     backfill()/update() -> las mismas ordenes por la via API jubilada.
   Guardado a prueba de fallos: checkpoint POR CLUB, escritura atomica y
   reanudacion automatica (misma maquinaria que sofascore/understat).

B) CAPA DE ADAPTACION / NORMALIZACION (corre en cualquier sitio, solo pandas):
     load_processed(master_csv) -> lee UN master, fija tipos y valida. Sin
                          `out_parquet` no escribe nada: es la pieza por via.
     process_empalmado()  -> normaliza las dos vias, las empalma en `ELO_CORTE` y
                          publica `clubelo_elo.parquet`, que es el artefacto que
                          consume el merge_asof del maestro. Es lo que corre
                          `process` y el final de `weekly`.

IMPORTANTE: la capa A exige salida a clubelo.com, por lo que se ejecuta siempre
en la maquina local. Requiere pandas y, opcionalmente, curl_cffi (si no esta, cae a
urllib de la libreria estandar).

Uso tipico (en local)
---------------------
    python scripts/extractors/clubelo.py weekly       # update + parquet (via web)
    python scripts/extractors/clubelo.py backfill     # serie completa de la web
    python scripts/extractors/clubelo.py weekly --via api   # via jubilada
"""

from __future__ import annotations

import io
import json
import re
import time
import unicodedata
from datetime import date, datetime
from pathlib import Path

import pandas as pd

try:  # ejecucion como paquete (from scripts.extractors import clubelo)
    from . import _common
    from . import teamnames
except ImportError:  # ejecucion como script suelto
    import _common  # type: ignore
    import teamnames  # type: ignore


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
CLUB_ELO_API = "http://api.clubelo.com"   # API CSV (http, sin TLS obligatorio)
COUNTRY = "ESP"                            # codigo de pais de ClubElo para Espana
TOP_LEVEL = 1                              # 1 = Primera; 2 = Segunda; ...
FIRST_SEASON = 2016                        # 2016 = temporada 2016/17 (casa con Sofascore)

# --- cobertura de la temporada vigente ------------------------------------- #
# LaLiga arranca en la segunda quincena de agosto: '08-01' marca el limite a
# partir del cual la temporada se considera vigente para el sondeo de clubes.
_SEASON_START_MD = "08-01"
# A partir del 1 de septiembre la competicion lleva varias jornadas y ClubElo ya
# ha reclasificado a los ascendidos como nivel 1: antes de esa fecha la falta de
# cobertura puede ser legitima (foto todavia de Segunda), asi que solo se avisa.
_STRICT_COVERAGE_MD = "09-01"
# La liga termina a finales de mayo. Fuera de la ventana [08-01, 06-01) no hay
# jornada que predecir y la clasificacion por nivel de ClubElo esta en transito
# (descensos/ascensos), asi que no se exige cobertura.
_SEASON_END_MD = "06-01"
# Primera tiene 20 equipos; se exige casi el censo completo y se deja margen para
# una grafia nueva o una reclasificacion tardia de un ascendido.
_MIN_CURRENT_CLUBS = 18

# raiz del repo: .../scripts/extractors/clubelo.py -> subir 3 -> repo/
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "clubelo"
# CSV maestro (serie temporal Elo por club, ya normalizada con team_key).
_MASTER_CSV = _RAW_DIR / "clubelo_laliga_elo.csv"
# Parquet limpio (tipado) listo para el merge_asof con las demas fuentes.
_OUT_PARQUET = _REPO / "data" / "02_processed" / "clubelo_elo.parquet"

# Columnas de identidad del tramo (clave de idempotencia = team_key + from).
_KEY_COLS = ["team_key", "from"]

# Cabeceras base de navegador para las peticiones.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


# --------------------------------------------------------------------------- #
# Configuracion de la VIA WEB (la viva)
# --------------------------------------------------------------------------- #
CLUB_ELO_WEB = "https://clubelo.com"
# Pagina de federacion: censo vigente por nivel + tabla de slugs del Ranking.
WEB_FED = "ESP"
# Las dos series no comparten escala, asi que tampoco comparten maestro.
_MASTER_CSV_WEB = _RAW_DIR / "clubelo_laliga_elo_web.csv"
# Slug de URL por clave canonica. Se cachea porque resolverlo cuesta peticiones
# y porque un club que cae a Tercera desaparece del censo pero conserva pagina.
_SLUGS_JSON = _RAW_DIR / "clubelo_slugs_web.json"
# Suelo de la serie publicada por la web. No es una eleccion: la grafica de club
# es una VENTANA MOVIL de cuatro anos (medido en Barcelona: 217 puntos, del
# 2022-10-09 al 2026-09-19) y las rutas de club fechadas no existen —
# /2018-05-20/Barcelona responde 200 sirviendo la portada—, asi que la via web no
# se puede caminar hacia atras para conseguir mas historia.
WEB_PRIMER_PUNTO = "2022-10-01"
# El censo de /ESP trae los 20 de Primera y los 5 de Segunda que ya han jugado.
# Por debajo de 20 en Primera la pagina ha cambiado de forma y hay que mirarla.
_WEB_MIN_CENSO = 20


# --------------------------------------------------------------------------- #
# Empalme de las dos vias
# --------------------------------------------------------------------------- #
# Fecha en la que la serie publicada deja de salir del archivo de la API jubilada
# y pasa a salir de la web. No es una preferencia entre fuentes: es la PRIMERA
# fecha en la que la ventana movil de la web cubre a los veinte clubes de Primera
# de 2022/23 (medido: el 2022-10-09 cubre 8, el 10 cubre 16, el 17 cubre 19, el 18
# los 20). Antes de ella no hay web; desde ella, la web es la unica via viva.
#
# El empalme es legitimo porque lo que viaja al modelo no es el Elo en nivel sino
# su tipificacion DENTRO DE LA JORNADA, y la tipificacion es invariante a la
# transformacion afin que separa las dos escalas (la quita entera: resta la media
# y divide por la desviacion, que es exactamente lo que el rebase cambio). Medido
# sobre los 20 clubes que cruzan el corte dentro de 2022/23, el salto de fuerza
# tipificada entre el ultimo partido anterior y el primero posterior al empalme
# tiene mediana 0.073 desviaciones, p90 0.170 y maximo 0.194, mientras una jornada
# normal de futbol mueve esa misma cifra 0.048 de mediana, 0.180 en el p99 y hasta
# 0.343 en el extremo: el empalme es indistinguible de una semana corriente de
# competicion. En nivel crudo seria un escalon de ~85 puntos de golpe para los
# veinte a la vez.
# Quien tipifica es `consolidate/spine.add_clubelo`, la unica capa que sabe que
# clubes componen Primera en cada temporada.
ELO_CORTE = "2022-10-18"

_WEB_HEADERS = {
    "User-Agent": _HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": _HEADERS["Accept-Language"],
}

# Ruido de las denominaciones oficiales que ClubElo no pone en la URL. Se usa
# para GENERAR candidatos de slug; quien decide es el guardian de identidad.
_SLUG_SIGLAS = {"cf", "fc", "ud", "sd", "cd", "rcd", "rc", "ca", "sad", "cp", "ad"}
_SLUG_NEXOS = {"de", "del", "la", "las", "los", "el"}
_SLUG_PREFIJOS = {"real", "deportivo", "racing", "sporting", "club", "union",
                  "gimnastic", "recreativo"}


# =========================================================================== #
# A) CAPA DE SCRAPING
# =========================================================================== #

class _NotFound(Exception):
    """El recurso pedido no existe (404): temporada/club sin datos."""


# --- transporte (delega en _common; curl_cffi si esta, si no urllib) -------- #
def _http_get(url: str, *, headers: dict, timeout: int = 30,
              retries: int = 3, wait: float = 2.0) -> tuple[int, str]:
    """GET -> (status, cuerpo). NO lanza por codigos HTTP; reintenta con espera
    creciente en red, 429 y 5xx. Compartido en `_common.http_get`."""
    return _common.http_get(url, headers=headers, timeout=timeout,
                            retries=retries, wait=wait)


# --- parseo del CSV de ClubElo (puro y testeable sin red) ------------------ #
def _parse_clubelo_csv(text: str) -> pd.DataFrame:
    """Convierte el CSV de ClubElo en un DataFrame de columnas normalizadas.

    Cabecera de ClubElo: Rank,Club,Country,Level,Elo,From,To. Devuelve columnas
    en minusculas: rank, club, country, level, elo, from, to. Un CSV con solo la
    cabecera (club inexistente) -> DataFrame vacio con esas columnas.
    """
    cols = ["rank", "club", "country", "level", "elo", "from", "to"]
    if not text or not text.strip():
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(io.StringIO(text))
    df.columns = [str(c).strip().lower() for c in df.columns]
    if df.empty:
        # asegura las columnas esperadas aunque venga solo la cabecera
        for c in cols:
            if c not in df.columns:
                df[c] = pd.Series(dtype="object")
        return df[cols]
    # 'None' textual -> NaN; tipos
    df = df.replace("None", pd.NA)
    if "elo" in df.columns:
        df["elo"] = pd.to_numeric(df["elo"], errors="coerce")
    if "level" in df.columns:
        df["level"] = pd.to_numeric(df["level"], errors="coerce").astype("Int64")
    if "rank" in df.columns:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    for c in ("from", "to"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
    # garantiza el orden/existencia de columnas
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols]


def _laliga_clubs_from_snapshot(text: str) -> list[str]:
    """De una foto por fecha, devuelve los clubes de Primera espanola (nombres
    EXACTOS de ClubElo, listos para construir la URL de historia)."""
    df = _parse_clubelo_csv(text)
    if df.empty:
        return []
    mask = (df["country"] == COUNTRY) & (df["level"] == TOP_LEVEL)
    clubs = df.loc[mask, "club"].dropna().astype(str).str.strip()
    return sorted({c for c in clubs if c})


def _url_name(club: str) -> str:
    """Nombre de club -> segmento de URL de ClubElo (sin espacios ni apostrofes,
    sin acentos). Ej.: 'Real Madrid' -> 'RealMadrid'; 'Rayo Vallecano' ->
    'RayoVallecano'. Usa unidecode si esta; si no, deacentua con unicodedata."""
    try:
        from unidecode import unidecode
        s = unidecode(str(club))
    except ImportError:
        import unicodedata
        s = "".join(
            c for c in unicodedata.normalize("NFKD", str(club))
            if not unicodedata.combining(c)
        )
    return s.replace(" ", "").replace("'", "")


# --- descarga: descubrimiento de clubes y foto por fecha ------------------- #
def _season_probe_date(season_year: int, today: str | None = None) -> str | None:
    """Fecha de sondeo del descubrimiento de clubes para una temporada.

    Criterio (dos regimenes, por que el sondeo persigue objetivos distintos):
      * TEMPORADA CERRADA -> 1 de octubre. La liga esta en marcha y los 20 equipos
        ya estan definidos, asi que la foto de ese dia es el censo completo.
      * TEMPORADA VIGENTE -> HOY. El 1 de octubre puede estar todavia en el futuro,
        y con el criterio anterior la temporada en curso se omitia por completo:
        los recien ascendidos no se descubrian y sus partidos de agosto y
        septiembre entraban al modelo SIN Elo (NaN en el merge_asof). Sondear hoy
        los incorpora desde la primera jornada.
      * TEMPORADA NO ARRANCADA -> None. No hay nada que sondear (la foto de hoy
        pertenece a la temporada anterior y ya se sondea por su propio ano).

    2016 -> '2016-10-01'. Con today='2026-08-20': 2026 -> '2026-08-20'.
    """
    today = today or date.today().isoformat()
    probe = f"{season_year}-10-01"
    if probe <= today:
        return probe
    return today if today >= f"{season_year}-{_SEASON_START_MD}" else None


def _fetch_snapshot(date_str: str, *, verbose: bool = True) -> list[str]:
    """Foto por fecha -> lista de clubes de Primera espanola ese dia. [] si 404."""
    url = f"{CLUB_ELO_API}/{date_str}"
    status, body = _http_get(url, headers=_HEADERS)
    if status == 404:
        return []
    if status != 200:
        raise RuntimeError(f"HTTP {status} en {url}: {body[:120]!r}")
    return _laliga_clubs_from_snapshot(body)


def discover_clubs(seasons, *, extra: set[str] | None = None,
                   pause: float = 0.5, verbose: bool = True) -> list[str]:
    """Descubre el conjunto de clubes de Primera a seguir uniendo la foto de cada
    temporada. Salta las fechas futuras. `extra` fuerza la inclusion de clubes ya
    conocidos (p.ej. los que ya estan en el master)."""
    today = date.today().isoformat()
    found: set[str] = set(extra or set())
    for y in sorted({int(s) for s in seasons}):
        probe = _season_probe_date(y, today=today)
        if probe is None:  # temporada aun no arrancada: nada que sondear
            if verbose:
                print(f"  [{y}/{str(y + 1)[2:]}] temporada sin empezar; se omite.")
            continue
        try:
            clubs = _fetch_snapshot(probe, verbose=verbose)
        except RuntimeError as e:
            print(f"  [WARN] no pude sondear {probe}: {e}")
            continue
        if verbose:
            print(f"  [{y}/{str(y + 1)[2:]}] {probe}: {len(clubs)} clubes de Primera.")
        found |= set(clubs)
        time.sleep(pause)
    return sorted(found)


# --- descarga: historia Elo por club --------------------------------------- #
def fetch_club_history(club: str, *, verbose: bool = True) -> pd.DataFrame:
    """Historia Elo completa de un club -> DataFrame de tramos con team/team_key.

    Columnas: team, team_key, country, level, rank, elo, from, to.
    Lanza `_NotFound` si el club no existe en ClubElo (404 o CSV vacio).
    """
    url = f"{CLUB_ELO_API}/{_url_name(club)}"
    status, body = _http_get(url, headers=_HEADERS)
    if status == 404:
        raise _NotFound(url)
    if status != 200:
        raise RuntimeError(f"HTTP {status} en {url}: {body[:120]!r}")

    df = _parse_clubelo_csv(body)
    if df.empty:
        raise _NotFound(url)  # ClubElo devuelve solo cabecera para club inexistente

    df = df.rename(columns={"club": "team"})
    # el nombre de la foto (parametro `club`) es la etiqueta canonica; si algun
    # tramo antiguo trae otra grafia, se respeta la del propio CSV.
    df["team"] = df["team"].fillna(club).astype(str)
    df["team_key"] = df["team"].map(teamnames.to_key)
    df = df[["team", "team_key", "country", "level", "rank", "elo", "from", "to"]]
    df = df.dropna(subset=["from", "elo"]).reset_index(drop=True)
    if verbose:
        rng = f"{df['from'].min()} -> {df['from'].max()}" if len(df) else "-"
        print(f"    {club:16s}: {len(df):5d} tramos ({rng})")
    return df


# --- orquestacion backfill / update ---------------------------------------- #
def _current_season_year(now: datetime | None = None) -> int:
    """Ano de inicio de la temporada vigente (LaLiga arranca en agosto)."""
    now = now or datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def _sync(clubs: list[str], master_csv: Path, *, pause: float = 0.8,
          verbose: bool = True) -> pd.DataFrame:
    """Baja la historia de cada club y la funde de forma idempotente en el master.

    Reanudable: los clubes ya presentes en el checkpoint de esta ejecucion se
    saltan. La fusion final sobrescribe por (team_key, from) con el dato recien
    bajado (keep='last'), lo que actualiza el tramo abierto que se mueve.
    """
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master = _load_master(master_csv)
    df_ckpt, done_keys = _load_checkpoint(master_csv)
    new_rows: list[pd.DataFrame] = [df_ckpt] if not df_ckpt.empty else []

    attempted = failed = 0
    for club in clubs:
        if teamnames.to_key(club) in done_keys:
            if verbose:
                print(f"    {club:16s}: ya en checkpoint; se omite.")
            continue
        attempted += 1
        try:
            hist = fetch_club_history(club, verbose=verbose)
        except _NotFound:
            print(f"    [WARN] {club}: sin datos en ClubElo; se omite.")
            continue
        except RuntimeError as e:
            failed += 1
            print(f"    [WARN] {club}: {e}; se omite.")
            continue
        new_rows.append(hist)
        done_keys.add(teamnames.to_key(club))
        _save_checkpoint(new_rows, master_csv)  # persiste tras cada club
        time.sleep(pause)

    # si TODOS los clubes intentados fallaron por transporte, la ejecucion no
    # esta "al dia": esta rota. Cortar ruidosamente en vez de terminar en verde.
    if attempted and failed == attempted:
        _finalize_master(df_master, new_rows, master_csv)
        raise RuntimeError(
            f"[clubelo] los {failed} clubes intentados fallaron por descarga: "
            "la API de ClubElo esta caida o bloqueando. Nada se ha perdido; "
            "re-ejecuta el mismo comando mas tarde (reanuda solo)."
        )
    return _finalize_master(df_master, new_rows, master_csv)


def backfill(
    seasons=None,
    *,
    master_csv: Path | str = _MASTER_CSV,
    pause: float = 0.8,
) -> pd.DataFrame:
    """Descarga TODO el historico Elo (por defecto clubes de 2016/17 -> vigente).

    Descubre los clubes por temporada (foto por fecha) y baja su historia Elo
    completa. Idempotente y reanudable (checkpoint por club).
    """
    if seasons is None:
        seasons = range(FIRST_SEASON, _current_season_year() + 1)
    master_csv = Path(master_csv)

    print("=== Descubrimiento de clubes (foto por temporada) ===")
    known = _known_teams(master_csv)  # clubes ya en el master, para no perderlos
    clubs = discover_clubs(seasons, extra=known)
    print(f"[INFO] clubes a seguir: {len(clubs)} -> {clubs}")
    if not clubs:
        raise RuntimeError(
            "[clubelo] descubrimiento VACIO: ninguna foto por temporada devolvio "
            "clubes y el master no aporta ninguno. O la API de ClubElo esta "
            "caida o su formato de CSV ha cambiado; nada que sincronizar."
        )

    print("\n=== Descarga de historia Elo por club ===")
    return _sync(clubs, master_csv, pause=pause)


def update(
    *,
    master_csv: Path | str = _MASTER_CSV,
    recent_seasons: int = 2,
    pause: float = 0.8,
) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE. Actualiza la historia Elo de los clubes recientes.

    Descubre los clubes de las `recent_seasons` temporadas mas recientes (por
    defecto 2 = vigente + anterior) y re-baja su historia: extiende el tramo
    abierto y anade la semana nueva. Idempotente por (team_key, from).
    """
    master_csv = Path(master_csv)
    cur = _current_season_year()
    years = [cur - i for i in range(max(1, recent_seasons)) if cur - i >= FIRST_SEASON]
    print(f"[INFO] Temporadas a escanear: {sorted(years)}")

    clubs = discover_clubs(years)
    if not clubs:  # entre temporadas la foto reciente puede no dar equipos aun
        print("[INFO] sin clubes nuevos por foto; refresco los ya conocidos.")
        clubs = _known_teams_names(master_csv)
    print(f"[INFO] clubes a actualizar: {len(clubs)} -> {clubs}")
    if not clubs:
        raise RuntimeError(
            "[clubelo] sin clubes que actualizar: la foto por fecha no devolvio "
            "nada y el master esta vacio. Ejecuta primero 'backfill' o revisa "
            "la API de ClubElo."
        )

    return _sync(clubs, master_csv, pause=pause)


# =========================================================================== #
# A') VIA WEB -- parseo puro (sin red, testeable offline con HTML guardado)
# =========================================================================== #

def _bloque_json(texto: str, ancla: str) -> str | None:
    """Primer objeto/array con llaves BALANCEADAS que sigue a `ancla`.

    El especificador Vega-Lite y el literal del Ranking van incrustados en el
    HTML seguidos de mas JSON, asi que una expresion regular no sabe donde
    termina el bloque (`json.loads` protesta con 'Extra data'). Hay que contar
    llaves, respetando las que viajan dentro de una cadena o escapadas.
    """
    if not texto or not ancla:
        return None
    i = texto.find(ancla)
    if i < 0:
        return None
    i += len(ancla)
    # el separador puede ser ':' (JSON) o '=' (literal de JavaScript)
    while i < len(texto) and texto[i] not in "[{":
        i += 1
    if i >= len(texto):
        return None
    abre = texto[i]
    cierra = "]" if abre == "[" else "}"
    prof, j, en_cadena, escapado = 0, i, False, False
    while j < len(texto):
        c = texto[j]
        if escapado:
            escapado = False
        elif c == "\\":
            escapado = True
        elif c == '"':
            en_cadena = not en_cadena
        elif not en_cadena:
            if c == abre:
                prof += 1
            elif c == cierra:
                prof -= 1
                if prof == 0:
                    return texto[i:j + 1]
        j += 1
    return None


def _web_datasets(html: str) -> dict:
    """Bloque `datasets` del especificador Vega-Lite. {} si no esta o no parsea.

    La grafica trae los datos INCRUSTADOS (el especificador no referencia ni una
    URL), asi que este bloque es la unica via de acceso a la serie.
    """
    raw = _bloque_json(html or "", '"datasets"')
    if not raw:
        return {}
    try:
        ds = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return ds if isinstance(ds, dict) else {}


def _web_titulo(html: str) -> str:
    """Texto del <h1>, sin etiquetas. '' si no hay (portada y rutas invalidas)."""
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html or "", re.S)
    if not m:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", m.group(1))).strip()


def _web_fecha_sitio(html: str) -> str:
    """Fecha de corte que el SITIO se atribuye, leida del enlace del <h1>.

    El titulo de cada pagina enlaza a su propia foto del dia
    (`<h1><a href="/2026-10-08/Barcelona">`), asi que ese enlace es la fecha
    hasta la que el sitio afirma tener el Elo calculado. Es la unica medida
    honesta de frescura de la fuente: la fecha del ultimo partido de un club NO
    lo es, porque tres semanas sin punto pueden ser un paron de calendario
    (medido: en octubre de 2026 toda Primera para entre el 20-sep y el 10-oct
    mientras Segunda sigue jugando). Lo que un dato viejo pegado a un partido
    nuevo deberia disparar es la invariante 6 de `qa_master`, no esto.

    Se exige que tras la fecha venga el nombre de la pagina
    (`/2026-10-08/Barcelona`) y no solo la barra: la PORTADA tambien trae enlace
    fechado, `<h1><a href="/2026-10-09/"></a></h1>`, con el texto vacio y sin
    club. Pedir el segmento final es lo que distingue "la fecha de ESTA pagina"
    de "la fecha del ranking mundial", y deja la funcion devolviendo '' ante la
    portada y ante cualquier ruta que la sirva. No es un guardian de identidad
    -de eso responden `es_pagina_de_club` y `parse_censo_web`-, pero una medida
    de frescura que acepta la portada no mide la pagina que se cree medir.

    Sirve igual para la pagina de un club y para la de una federacion: el censo
    de /ESP se titula `<h1><a href="/2026-10-08/ESP">Spain</a></h1>` y declara su
    fecha con el mismo patron, de modo que la frescura del censo se mide sin
    inventar una regla aparte para el.
    """
    m = re.search(r'<h1[^>]*>\s*<a[^>]+href="/(\d{4}-\d{2}-\d{2})/[^"/]+"',
                  html or "")
    return m.group(1) if m else ""


def _web_puntos(html: str) -> list[dict]:
    """Puntos (Date, Elo) de la grafica de un club, en el orden en que vienen."""
    return [r for v in _web_datasets(html).values() if isinstance(v, list)
            for r in v if isinstance(r, dict) and "Date" in r]


def es_pagina_de_club(html: str, *, clave: str | None = None) -> bool:
    """Guardian de identidad: el HTML es de verdad la pagina del club pedido?

    Hace falta porque la web NO devuelve 404: ante cualquier ruta desconocida
    sirve la portada con 200, que trae datasets con forma de ranking (sin fecha)
    y el <h1> vacio. Sin este guardian, un slug mal resuelto pasaria por una
    serie valida y entraria al maestro como el club equivocado.

    Con `clave`, exige ademas que el titulo resuelva a esa clave canonica: es lo
    que permite PROBAR candidatos de slug a ciegas sin arriesgar una confusion
    (p.ej. `/Madrid` cuando se buscaba el Atletico).
    """
    titulo = _web_titulo(html)
    if not titulo or not _web_puntos(html):
        return False
    if clave is not None and teamnames.to_key(titulo) != clave:
        return False
    return True


def _web_tabla_slugs(html: str) -> dict[str, str]:
    """Nombre de club -> slug de URL, leido del literal del Ranking de /ESP.

    Los slugs de ClubElo no se deducen del nombre (`atletico`, `rc-celta`,
    `realmadrid`, `athletic-club`, `Sociedad`), asi que hay que leerlos. Se
    toman del literal `const eloData` porque ahi el enlace tiene SIEMPRE la
    misma forma; en las otras tablas de la pagina alterna con <span> de
    responsive y la cosecha sale incompleta.
    """
    raw = _bloque_json(html or "", "const eloData") or ""
    tabla: dict[str, str] = {}
    for slug, nombre in re.findall(r'<a href="/([^"/]+)">([^<]+)<', raw):
        nombre = nombre.strip()
        if nombre and slug:
            tabla.setdefault(nombre, slug)
    return tabla


def parse_censo_web(html: str, *, strict: bool = True) -> pd.DataFrame:
    """Pagina de federacion -> censo vigente de clubes con nivel, slug y Elo.

    Columnas: team, team_key, slug, level, tlc, elo. `level` es la
    clasificacion VIGENTE que publica ClubElo (1 = Primera), que es justo la que
    necesita `current_season_clubs` para comprobar la cobertura de la jornada
    que se va a predecir.

    Dos guardianes de identidad, por la misma razon que en `es_pagina_de_club`:
    la web sirve la PORTADA con 200 ante cualquier ruta desconocida, y la
    portada trae 50 filas con 'Name' y 'Level' (el ranking mundial) que pasarian
    el minimo de censo sin inmutarse (medido: 50 de nivel 1). Asi que se exige
    <h1> no vacio (la portada lo trae vacio; /ESP dice 'Spain') y se filtra por
    `FedURL`, que en /ESP vale ESP en las 25 filas y en la portada reparte entre
    ENG, ITA, GER, ESP, FRA y POR.
    """
    cols = ["team", "team_key", "slug", "level", "tlc", "elo"]
    titulo = _web_titulo(html)
    filas = [r for v in _web_datasets(html).values() if isinstance(v, list)
             for r in v if isinstance(r, dict) and "Name" in r and "Level" in r]
    if filas and any("FedURL" in r for r in filas):
        filas = [r for r in filas if str(r.get("FedURL", "")).strip() == WEB_FED]
    if not titulo:
        if strict:
            raise _common.SchemaError(
                f"[clubelo.web] la ruta /{WEB_FED} no ha devuelto una pagina de "
                "federacion: el <h1> viene vacio, que es la firma de la PORTADA. "
                "Esta web responde 200 a cualquier ruta invalida, asi que un 200 "
                "no prueba nada. Abre https://clubelo.com/" + WEB_FED +
                " en el navegador: o ha cambiado la ruta de las federaciones o "
                "el sitio esta sirviendo otra cosa.")
        return pd.DataFrame(columns=cols)
    if not filas:
        if strict:
            raise _common.SchemaError(
                f"[clubelo.web] la pagina /{WEB_FED} no trae censo: ninguna fila "
                f"del bloque `datasets` lleva 'Name', 'Level' y FedURL={WEB_FED}. "
                "La pagina ha cambiado de forma (o de federacion). Mirala en el "
                "navegador y ajusta el parseo.")
        return pd.DataFrame(columns=cols)

    tabla = _web_tabla_slugs(html)
    df = pd.DataFrame({
        "team": [str(r.get("Name", "")).strip() for r in filas],
        "level": [r.get("Level") for r in filas],
        "tlc": [r.get("TLC") for r in filas],
        "elo": [r.get("Elo") for r in filas],
    })
    df["slug"] = df["team"].map(tabla)
    df["team_key"] = df["team"].map(teamnames.to_key)
    df["level"] = pd.to_numeric(df["level"], errors="coerce").astype("Int64")
    df["elo"] = pd.to_numeric(df["elo"], errors="coerce")
    df = df.dropna(subset=["team"]).drop_duplicates(subset=["team_key"])
    df = df.sort_values(["level", "team"]).reset_index(drop=True)[cols]

    n_top = int((df["level"] == TOP_LEVEL).sum())
    if strict and n_top < _WEB_MIN_CENSO:
        raise _common.SchemaError(
            f"[clubelo.web] el censo de /{WEB_FED} da {n_top} clubes de nivel "
            f"{TOP_LEVEL} (se esperan >= {_WEB_MIN_CENSO}). Primera tiene 20; "
            "con menos, el parseo del censo ha dejado de casar con la pagina y "
            "la cobertura de la temporada vigente saldria falsamente corta.")
    sin_slug = df.loc[df["slug"].isna() & (df["level"] == TOP_LEVEL), "team"].tolist()
    if sin_slug:
        print(f"[WARN] sin slug en la tabla del Ranking: {sin_slug}; "
              "se intentara resolver por candidatos.")
    return df


def parse_historia_web(html: str, *, team: str, level=pd.NA,
                       country: str = COUNTRY) -> pd.DataFrame:
    """Pagina de club -> tramos [from, to] de Elo, SIN fuga de informacion.

    El valor que la web fecha en D es el POSTERIOR al partido de D (la tabla de
    partidos lo publica como 'New Elo' y la grafica repite ese numero; medido
    tambien por correlacion: el salto en D explica la diferencia de goles de D
    con r ~ 0.73-0.83, y la del partido anterior con r ~ 0.03). Por eso el tramo
    que abre empieza en D+1: un partido jugado en D2 se cruza con el Elo que
    dejo el partido ANTERIOR del club, nunca con el suyo. El tramo se cierra en
    la fecha del punto siguiente, que es el partido jugado con ese valor, y el
    ultimo queda abierto (`to` nulo) porque es el que se mueve cada semana.

    `level` y `country` son del club, no del tramo: la web solo publica la
    clasificacion VIGENTE y la historica no es recuperable. Nada aguas abajo lee
    el nivel por tramo (`spine.add_clubelo` solo pide team_key, elo y date); lo
    consume `current_season_clubs`, que mira el tramo abierto.
    """
    cols = ["team", "team_key", "country", "level", "rank", "elo", "from", "to",
            "fuente"]
    pts = _web_puntos(html)
    if not pts:
        return pd.DataFrame(columns=cols)

    d = pd.DataFrame({
        "_d": pd.to_datetime(
            pd.Series([r.get("Date") for r in pts]).astype("string").str.slice(0, 10),
            errors="coerce"),
        "elo": pd.to_numeric(pd.Series([r.get("Elo") for r in pts]), errors="coerce"),
    })
    d = d.dropna(subset=["_d", "elo"]).sort_values("_d")
    # dos puntos en el mismo dia (partido doble): manda el ultimo, que es el Elo
    # con el que el club afronta el siguiente partido.
    d = d.drop_duplicates(subset=["_d"], keep="last").reset_index(drop=True)
    if d.empty:
        return pd.DataFrame(columns=cols)

    desde = (d["_d"] + pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")
    hasta = d["_d"].shift(-1).dt.strftime("%Y-%m-%d")
    out = pd.DataFrame({
        "team": str(team),
        "team_key": teamnames.to_key(team),
        "country": country,
        "level": pd.NA,
        "rank": pd.NA,
        "elo": d["elo"].to_numpy(),
        "from": desde.to_numpy(),
        "to": hasta.to_numpy(),
        "fuente": "web",
    })
    out["level"] = pd.to_numeric(
        pd.Series([level] * len(out)), errors="coerce").astype("Int64").to_numpy()
    return out[cols].reset_index(drop=True)


def _slug_candidatos(nombre: str) -> list[str]:
    """Candidatos de slug para un nombre oficial, del mas probable al menos.

    Solo hacen falta para los clubes que NO estan en el censo vigente: los que
    han caido a Segunda B o mas abajo desaparecen de /ESP pero conservan su
    pagina (medido: Cadiz, Valladolid, LasPalmas, Granada, Leganes, Oviedo y
    Huesca responden con la serie entera). Aqui no se acierta, se PROPONE: quien
    acepta o descarta es el guardian de identidad.
    """
    txt = "".join(c for c in unicodedata.normalize("NFKD", str(nombre))
                  if not unicodedata.combining(c)).replace("'", "")
    toks = [t for t in txt.split() if t]
    sin_siglas = [t for t in toks if t.lower() not in _SLUG_SIGLAS]
    base = [t for t in sin_siglas if t.lower() not in _SLUG_NEXOS]
    out: list[str] = []

    def add(x: str) -> None:
        if x and x not in out:
            out.append(x)

    add("".join(base))                      # Cadiz, RealValladolid, AthleticClub
    add("".join(sin_siglas))                # LasPalmas (el nexo SI va en la URL)
    if len(base) > 1 and base[0].lower() in _SLUG_PREFIJOS:
        add("".join(base[1:]))              # Valladolid, Oviedo, Gijon, Santander
    add("".join(base).lower())              # realmadrid
    add("-".join(t.lower() for t in base))  # athletic-club
    return out


# =========================================================================== #
# A'') VIA WEB -- descarga
# =========================================================================== #

def _fetch_web(ruta: str, *, timeout: int = 90, retries: int = 3) -> str:
    """GET de una ruta de clubelo.com -> HTML. Lanza si no responde 200.

    No se comprueba el 404 porque la web NO lo usa: valida la ruta el guardian
    de identidad, no el codigo HTTP.
    """
    url = f"{CLUB_ELO_WEB}/{str(ruta).lstrip('/')}"
    status, body = _http_get(url, headers=_WEB_HEADERS, timeout=timeout,
                             retries=retries)
    if status != 200:
        raise RuntimeError(f"HTTP {status} en {url}: {body[:120]!r}")
    return body


def fetch_censo_web(*, verbose: bool = True) -> pd.DataFrame:
    """Censo vigente de clubes espanoles con nivel y slug (pagina /ESP)."""
    censo = parse_censo_web(_fetch_web(WEB_FED))
    if verbose:
        n1 = int((censo["level"] == TOP_LEVEL).sum())
        print(f"[INFO] censo /{WEB_FED}: {len(censo)} clubes "
              f"({n1} de nivel {TOP_LEVEL}).")
    return censo


def fetch_club_history_web(slug: str, *, team: str, clave: str | None = None,
                           level=pd.NA, verbose: bool = True) -> pd.DataFrame:
    """Serie Elo de un club por su slug. Lanza `_NotFound` si el slug no es suyo."""
    html = _fetch_web(slug)
    if not es_pagina_de_club(html, clave=clave):
        raise _NotFound(f"{CLUB_ELO_WEB}/{slug}")
    df = parse_historia_web(html, team=_web_titulo(html) or team, level=level)
    if df.empty:
        raise _NotFound(f"{CLUB_ELO_WEB}/{slug}")
    if verbose:
        rng = f"{df['from'].min()} -> {df['from'].max()}"
        print(f"    {team:16s} [{slug}]: {len(df):5d} tramos ({rng})")
    return df


def _cargar_slugs() -> dict[str, str]:
    """Cache clave canonica -> slug. {} si no existe o esta corrupta."""
    try:
        d = json.loads(Path(_SLUGS_JSON).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {str(k): str(v) for k, v in d.items()} if isinstance(d, dict) else {}


def _guardar_slugs(cache: dict[str, str]) -> None:
    p = Path(_SLUGS_JSON)
    p.parent.mkdir(parents=True, exist_ok=True)
    _common.clear_readonly(p)
    try:
        p.write_text(json.dumps(cache, indent=1, sort_keys=True), encoding="utf-8")
    except OSError as e:  # el cache es una comodidad, no un requisito
        print(f"[WARN] no pude guardar {p.name}: {e}")


def resolve_slug(clave: str, nombre: str, *, tabla: dict[str, str],
                 cache: dict[str, str], pause: float = 0.4,
                 verbose: bool = True) -> str | None:
    """Slug de URL de un club. Cache -> tabla del Ranking -> candidatos a ciegas.

    Los candidatos se prueban contra el guardian de identidad con la clave
    canonica esperada, asi que un acierto es un acierto comprobado y un fallo no
    contamina nada. Lo resuelto se cachea: resolverlo cuesta peticiones y un
    club que cae de categoria desaparece del censo pero conserva pagina.

    El cache guarda tambien los NEGATIVOS (cadena vacia). Importa: en clubelo.com
    cualquier ruta devuelve 200 con la portada entera, asi que cada candidato
    descartado son ~750 KB; sin memoria de lo que no existe, los clubes sin
    pagina se re-sondearian en cada backfill.
    """
    if clave in cache:
        return cache[clave] or None
    if nombre in tabla:
        cache[clave] = tabla[nombre]
        return cache[clave]
    for cand in _slug_candidatos(nombre):
        try:
            html = _fetch_web(cand, retries=2)
        except RuntimeError as e:
            print(f"    [WARN] {nombre}: candidato '{cand}' no responde ({e}).")
            continue
        finally:
            time.sleep(pause)
        if es_pagina_de_club(html, clave=clave):
            if verbose:
                print(f"    {nombre:16s}: slug resuelto -> '{cand}'")
            cache[clave] = cand
            return cand
    cache[clave] = ""
    return None


def discover_clubs_web(*, extra_keys=None, pause: float = 0.4,
                       verbose: bool = True) -> pd.DataFrame:
    """Clubes a seguir por la via web, con su slug y su nivel vigente.

    Dos aportaciones, porque el censo de /ESP NO es el censo historico:
      * el propio censo (nivel 1 y 2), que se auto-completa con los ascensos sin
        tocar codigo y es quien da el nivel vigente;
      * los clubes de `extra_keys` -- los que ya estan en el maestro y los que la
        capa de nombres reconoce -- que pueden haber caido de categoria y ya no
        salir en la foto, pero cuya pagina sigue en pie.
    """
    censo = fetch_censo_web(verbose=verbose)
    # la tabla del Ranking ya viaja en el censo; es la que resuelve los slugs
    # irregulares (atletico, rc-celta, realmadrid, athletic-club, Sociedad).
    tabla = {r.team: r.slug for r in censo.itertuples(index=False)
             if isinstance(r.slug, str) and r.slug}

    cache = _cargar_slugs()
    objetivos: dict[str, dict] = {}
    ajenos: list[str] = []
    for r in censo.itertuples(index=False):
        if not (isinstance(r.slug, str) and r.slug):
            continue  # sin slug no hay pagina que pedir; ya se ha avisado
        if r.team_key not in teamnames.CANONICAL:
            # un club que la capa de nombres no conoce no se puede cruzar con el
            # maestro de partidos: su Elo no llegaria a ninguna fila. En Primera
            # eso es un fallo que hay que arreglar; en Segunda, ruido esperable.
            if r.level == TOP_LEVEL:
                teamnames.assert_known([r.team], source="clubelo.discover_web")
            ajenos.append(r.team)
            continue
        cache[r.team_key] = r.slug
        objetivos[r.team_key] = {"team": r.team, "slug": r.slug, "level": r.level}
    if ajenos and verbose:
        print(f"[INFO] censo: {len(ajenos)} clubes fuera de la capa de nombres "
              f"(no son de Primera): {ajenos}")

    pendientes = [k for k in sorted(set(extra_keys or ())) if k not in objetivos]
    if pendientes and verbose:
        print(f"[INFO] {len(pendientes)} clubes fuera del censo vigente; "
              "resuelvo su slug (pagina en pie aunque hayan bajado).")
    for clave in pendientes:
        nombre = teamnames.CANONICAL.get(clave, clave)
        slug = resolve_slug(clave, nombre, tabla=tabla, cache=cache,
                            pause=pause, verbose=verbose)
        if slug is None:
            print(f"    [WARN] {nombre}: sin pagina en clubelo.com; se omite.")
            continue
        # fuera del censo -> sin nivel vigente: no cuenta para la cobertura de
        # Primera, que es exactamente lo correcto.
        objetivos[clave] = {"team": nombre, "slug": slug, "level": pd.NA}

    _guardar_slugs(cache)
    out = pd.DataFrame([{"team_key": k, **v} for k, v in sorted(objetivos.items())])
    if out.empty:
        return pd.DataFrame(columns=["team_key", "team", "slug", "level"])
    out = out.dropna(subset=["slug"]).reset_index(drop=True)
    if verbose:
        print(f"[INFO] clubes a seguir: {len(out)}")
    return out[["team_key", "team", "slug", "level"]]


def _sync_web(objetivos: pd.DataFrame, master_csv: Path, *, pause: float = 0.6,
              verbose: bool = True) -> pd.DataFrame:
    """Baja la serie de cada club y la funde de forma idempotente en el maestro.

    Reanudable por checkpoint y con la misma regla de corte ruidoso que la via
    API: si TODOS los clubes intentados fallan por descarga, la ejecucion no
    esta al dia, esta rota, y terminar en verde seria mentir.
    """
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master = _load_master(master_csv)
    df_ckpt, done_keys = _load_checkpoint(master_csv)
    new_rows: list[pd.DataFrame] = [df_ckpt] if not df_ckpt.empty else []

    attempted = failed = 0
    for r in objetivos.itertuples(index=False):
        if r.team_key in done_keys:
            if verbose:
                print(f"    {r.team:16s}: ya en checkpoint; se omite.")
            continue
        attempted += 1
        try:
            hist = fetch_club_history_web(r.slug, team=r.team, clave=r.team_key,
                                          level=r.level, verbose=verbose)
        except _NotFound:
            print(f"    [WARN] {r.team}: '/{r.slug}' no es su pagina; se omite.")
            continue
        except RuntimeError as e:
            failed += 1
            print(f"    [WARN] {r.team}: {e}; se omite.")
            continue
        new_rows.append(hist)
        done_keys.add(r.team_key)
        _save_checkpoint(new_rows, master_csv)
        time.sleep(pause)

    if attempted and failed == attempted:
        _finalize_master(df_master, new_rows, master_csv)
        raise RuntimeError(
            f"[clubelo.web] los {failed} clubes intentados fallaron por descarga: "
            "clubelo.com esta caida o bloqueando. Nada se ha perdido; re-ejecuta "
            "el mismo comando mas tarde (reanuda solo).")
    return _finalize_master(df_master, new_rows, master_csv)


def backfill_web(*, master_csv: Path | str = _MASTER_CSV_WEB,
                 pause: float = 0.6) -> pd.DataFrame:
    """Serie Elo completa de la web para todos los clubes alcanzables.

    Arranca donde la web arranca (2022-10) y no antes: las rutas con fecha,
    /Results, /History y ?from= estan muertas. Idempotente y reanudable.
    """
    master_csv = Path(master_csv)
    print("=== Descubrimiento de clubes (censo /ESP + clubes conocidos) ===")
    # la capa de nombres es el censo HISTORICO del proyecto: incluye a los que
    # ya no estan en Primera y que, por tanto, no salen en la foto de hoy.
    extra = set(teamnames.CANONICAL) | _known_keys(master_csv)
    objetivos = discover_clubs_web(extra_keys=extra, pause=pause)
    if objetivos.empty:
        raise RuntimeError(
            "[clubelo.web] descubrimiento VACIO: el censo de /ESP no devolvio "
            "clubes y no hay slug cacheado. O clubelo.com esta caida o ha "
            "cambiado de forma; nada que sincronizar.")

    print("\n=== Descarga de la serie Elo por club ===")
    return _sync_web(objetivos, master_csv, pause=pause)


def update_web(*, master_csv: Path | str = _MASTER_CSV_WEB,
               pause: float = 0.6) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE. Refresca la serie de los clubes ya conocidos.

    Baja el censo vigente (que incorpora los ascendidos sin tocar codigo) mas
    los clubes que ya estan en el maestro, y re-baja su serie completa: la
    pagina no permite pedir un tramo, asi que la idempotencia por
    (team_key, from) es la que evita duplicar.
    """
    master_csv = Path(master_csv)
    conocidos = _known_keys(master_csv)
    objetivos = discover_clubs_web(extra_keys=conocidos, pause=pause)
    if objetivos.empty:
        raise RuntimeError(
            "[clubelo.web] sin clubes que actualizar: el censo de /ESP no "
            "devolvio nada y el maestro esta vacio. Ejecuta antes 'backfill'.")
    print(f"[INFO] clubes a actualizar: {len(objetivos)}")
    return _sync_web(objetivos, master_csv, pause=pause)


# --------------------------------------------------------------------------- #
# Carga del master y de clubes conocidos
# --------------------------------------------------------------------------- #
def _load_master(master_csv: Path) -> pd.DataFrame:
    """Carga el CSV maestro (serie Elo por club). DataFrame vacio si no existe."""
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        print(f"[INFO] master cargado: {len(df)} tramos, "
              f"{df['team_key'].nunique() if 'team_key' in df else 0} clubes.")
        return df
    print("[INFO] sin datos previos; se empieza de cero.")
    return pd.DataFrame()


def _known_teams(master_csv: Path) -> set[str]:
    """team_key ya presentes en el master (para no perderlos en el descubrimiento)."""
    df = _load_master_quiet(master_csv)
    if df.empty or "team" not in df.columns:
        return set()
    return set(df["team"].dropna().astype(str).unique())


def _known_teams_names(master_csv: Path) -> list[str]:
    """Nombres de club (grafia ClubElo) ya presentes en el master."""
    return sorted(_known_teams(master_csv))


def _known_keys(master_csv: Path) -> set[str]:
    """Claves canonicas ya presentes en el master (via web: a quien refrescar)."""
    df = _load_master_quiet(master_csv)
    if df.empty or "team_key" not in df.columns:
        return set()
    return set(df["team_key"].dropna().astype(str).unique())


def _load_master_quiet(master_csv: Path) -> pd.DataFrame:
    master_csv = Path(master_csv)
    if master_csv.exists():
        try:
            return pd.read_csv(master_csv)
        except Exception:  # noqa: BLE001
            return pd.DataFrame()
    return pd.DataFrame()


# --------------------------------------------------------------------------- #
# Guardado a prueba de fallos (disenado para "ejecutar una vez y olvidarse")
# --------------------------------------------------------------------------- #
# Mismo patron robusto que el resto de extractores:
#   * checkpoint por club: cada club bajado se vuelca a `_ckpt_<nombre>.csv`, de
#     modo que un corte no pierde trabajo (se reanuda saltando los ya bajados).
#   * escritura atomica: el CSV maestro se escribe a un temporal y se publica con
#     `os.replace` (atomico), dejando una copia `.bak`.
#   * escritura garantizada en Windows: antes de guardar se retira el atributo
#     "solo lectura" (que OneDrive suele poner) y se reintenta si hay candado.
def _ckpt_path(master_csv: Path) -> Path:
    return _common.ckpt_path(master_csv)


def _load_checkpoint(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el checkpoint pendiente. Devuelve (df, {team_key ya bajados})."""
    return _common.load_checkpoint(master_csv, key_col="team_key")


def _save_checkpoint(new_rows: list, master_csv: Path) -> None:
    """Reescribe el checkpoint con todos los tramos bajados hasta ahora."""
    _common.save_checkpoint(new_rows, master_csv)


_clear_readonly = _common.clear_readonly
_write_df_safe = _common.write_df_safe


def _merge_upsert(df_master: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """Funde master + nuevo por (team_key, from), quedandose con el dato NUEVO.

    keep='last' sobre [master, nuevo] -> el tramo recien bajado sobrescribe al
    antiguo (asi se actualiza el `to`/`elo` del tramo abierto que se mueve cada
    semana) y se anaden los tramos que no existian."""
    both = [d for d in (df_master, df_new) if isinstance(d, pd.DataFrame) and not d.empty]
    if not both:
        return pd.DataFrame()
    out = pd.concat(both, ignore_index=True)
    # re-normaliza team_key por si el master se edito a mano o cambio el alias
    if "team" in out.columns:
        out["team_key"] = out["team"].map(teamnames.to_key)
    out = out.drop_duplicates(subset=_KEY_COLS, keep="last")
    out = out.sort_values(["team_key", "from"]).reset_index(drop=True)
    return out


def _finalize_master(df_master: pd.DataFrame, new_rows: list[pd.DataFrame],
                     master_csv: Path) -> pd.DataFrame:
    """Funde los tramos nuevos en el maestro, lo guarda y limpia el checkpoint."""
    frames = [d for d in new_rows if isinstance(d, pd.DataFrame) and not d.empty]
    if not frames:
        print("[INFO] no hay tramos nuevos: el maestro ya esta al dia.")
        return df_master
    df_new = pd.concat(frames, ignore_index=True)
    df_out = _merge_upsert(df_master, df_new)

    if _write_df_safe(df_out, master_csv, fmt="csv"):
        n_clubs = df_out["team_key"].nunique() if "team_key" in df_out else 0
        print(f"[OK] Elo ClubElo guardado -> {master_csv}  "
              f"({len(df_out)} tramos, {n_clubs} clubes)")
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
def assert_una_escala(df: pd.DataFrame) -> str:
    """Una sola escala de Elo vigente en cada fecha. Devuelve la via.

    Devuelve `'api'`, `'web'` o `'empalme:<fecha>'`. Lo tercero es la serie que
    publica el proyecto; lo admite solo si el empalme esta LIMPIO, es decir si las
    dos vias ocupan tramos de fecha disjuntos con una unica frontera. Mezclarlas
    de cualquier otra forma es un error, y este es el sitio donde se ve.

    Por que importa la frontera unica
    ---------------------------------
    La web publica el Elo en otra escala que el CSV de la API jubilada. Medido
    sobre las 204 semanas de solape (2022-10 a 2026-08), la web va por encima
    +85.1 de media, y el desplazamiento depende del club: +38 en el Real Madrid,
    +135 en el Deportivo, con desviacion 23.6 entre clubes. Tampoco es estable en
    el tiempo: la mediana por trimestre de los 29 clubes con cobertura continua
    oscila entre +60 y +100 sin tendencia que converja.

    No es un error de lectura de la grafica. El censo vigente de /ESP es una
    segunda via de parseo, independiente de la serie por club, y concuerda con
    ella en +-1.55 puntos (media +0.17) sobre los mismos clubes y el mismo dia,
    mientras el archivo de la API queda a -78.5 de media (|max| 144.8). Lo que
    cambio es la escala del sitio, no el parser.

    En NIVEL las dos series no se pueden reconciliar. El mejor ajuste afin sobre
    el solape (`web = 403.6 + 0.8053 * api`, R2 0.965) deja un residuo con
    desviacion 18.7 puntos y p95 de 31.7 — del orden de lo que separa a dos
    equipos de mitad de tabla, es decir, del efecto que el rating tiene que medir.
    Reajustar el afin SEMANA A SEMANA, que es el mejor remedio concebible, tampoco
    lo arregla: sobre 202 semanas con ~30 clubes comunes cada una, el residuo
    mediano sigue en 14.5 puntos de desviacion (maximo 29), y los coeficientes
    derivan con el tiempo en lugar de asentarse (pendiente mediana 0.830 en 2022
    frente a 0.785 en 2026; constante 363 frente a 441).

    Lo que si se reconcilia es la POSICION RELATIVA. El rebase comprime el rango:
    la desviacion entre los 20 clubes de Primera es 105.1 en el archivo y 93.4 en
    la web, un 11% menos, y de ahi sale el sesgo de -32 del Real Madrid una vez
    centrado. Tipificar dentro de la jornada —restar la media de los 20 y dividir
    por su desviacion— quita el desplazamiento y la compresion a la vez, porque es
    exactamente la transformacion inversa. Medido sobre el solape y sobre la base
    correcta (los 20 de Primera, no la mezcla con Segunda), la fuerza tipificada
    de un club discrepa entre las dos escalas 0.069 desviaciones de mediana, 0.179
    en el p90 y 0.622 en el peor caso.

    De ahi el reparto: aqui se exige que en cada fecha mande una sola escala, y la
    comparabilidad entre los dos tramos la consigue la tipificacion que aplica
    `consolidate.spine.add_clubelo`. Lo que esta asercion impide es el fallo que
    ninguna comprobacion de esquema ve: concatenar las dos series solapadas, con
    columnas que casan, fechas que casan y un Elo que sigue siendo un numero
    plausible, mientras el mismo club aparece dos veces el mismo dia con 85
    puntos de diferencia.
    """
    if "fuente" not in df.columns:
        return "api"  # el maestro de la via API es anterior a esta columna
    vias = set(df["fuente"].dropna().astype(str).unique()) or {"api"}
    vias = {v if v else "api" for v in vias}
    if len(vias) == 1:
        return vias.pop()

    base = ("[clubelo.assert_una_escala] la web y la API jubilada publican el Elo "
            "en ESCALAS DISTINTAS (+85 puntos de media en el solape, de +38 a +135 "
            "segun el club y oscilando con la fecha). ")
    if vias != {"api", "web"}:
        raise _common.RegressionError(
            base + f"Vias encontradas: {sorted(vias)}; solo 'api' y 'web' estan "
            "definidas.")

    es_api = df["fuente"].fillna("api").astype(str).replace("", "api") == "api"
    desde = pd.to_datetime(df["from"], errors="coerce")
    hasta = pd.to_datetime(df.get("to"), errors="coerce")
    corte = desde[~es_api].min()
    fin_api = desde[es_api].max()
    cierre_api = hasta[es_api].max()
    if pd.isna(corte) or pd.isna(fin_api):
        raise _common.RegressionError(base + "una de las dos vias no tiene fechas.")
    if fin_api >= corte or (pd.notna(cierre_api) and cierre_api >= corte) \
            or hasta[es_api].isna().any():
        raise _common.RegressionError(
            base + "Se admite empalmarlas con UNA frontera (el archivo hasta la "
            "fecha de corte, la web desde ella), porque la tipificacion dentro de "
            "la jornada que aplica `spine` es invariante al cambio de escala. Pero "
            f"aqui los tramos se solapan: la web empieza el {corte.date()} y el "
            f"archivo llega al {fin_api.date()} (cierre "
            f"{cierre_api.date() if pd.notna(cierre_api) else 'abierto'}). Usa "
            "`serie_empalmada`, no `pd.concat`.")
    return f"empalme:{corte.date()}"


def serie_empalmada(api: pd.DataFrame, web: pd.DataFrame, *,
                    corte: str = ELO_CORTE, strict: bool = True) -> pd.DataFrame:
    """Una sola serie larga: el archivo de la API hasta `corte`, la web desde `corte`.

    Es el unico sitio del proyecto autorizado a juntar las dos vias, y lo hace
    dejando una frontera unica: ningun club tiene las dos escalas vigentes el
    mismo dia. Para conseguirlo se recorta por los dos lados —los tramos del
    archivo se cierran el dia anterior al corte y los de la web se levantan hasta
    el corte—, de modo que la via queda determinada por la fecha y nada mas. Esa
    propiedad es la que luego permite tipificar sin logica de via: en una fecha
    dada, los veinte clubes de Primera vienen todos del mismo lado.

    Los tramos del archivo posteriores al corte se tiran, no se promedian con la
    web: en el solape las dos series miden lo mismo en escalas distintas y la
    buena es la viva, la que seguira publicandose la semana que viene.

    Limite conocido
    ---------------
    `spine.add_clubelo` cruza por `date` (= `from`) con `merge_asof` y NO lee `to`,
    asi que cerrar un tramo del archivo no impide por si solo que un club sin
    cobertura web herede su ultimo Elo de 2022 para siempre. El cierre deja la
    serie coherente como serie; el guardian duro de que toda fecha a partir del
    corte resuelva por la web vive en `spine`, que es quien sabe que clubes juegan.
    Aqui se cubre el caso detectable sin partidos: un club que tiene pagina en la
    web pero cuya serie no alcanza el corte, que seria un agujero real.
    """
    cols = ["team", "team_key", "country", "level", "rank", "elo", "from", "to",
            "fuente"]
    corte_ts = pd.Timestamp(corte)
    previo = corte_ts - pd.Timedelta(days=1)
    fmt = "%Y-%m-%d"

    def _prep(df: pd.DataFrame, via: str) -> pd.DataFrame:
        out = df.copy()
        for c in cols:
            if c not in out.columns:
                out[c] = pd.NA
        out["fuente"] = via
        out["_from"] = pd.to_datetime(out["from"], errors="coerce")
        out["_to"] = pd.to_datetime(out["to"], errors="coerce")
        return out.dropna(subset=["_from"])

    a = _prep(api, "api")
    a = a[a["_from"] < corte_ts].copy()
    # cierre en el dia anterior al corte, tambien para el tramo abierto: desde el
    # corte la escala vigente es la de la web y este tramo ya no rige.
    a["_to"] = a["_to"].where(a["_to"].notna() & (a["_to"] < previo), previo)

    w = _prep(web, "web")
    w = w[w["_to"].isna() | (w["_to"] >= corte_ts)].copy()
    # el tramo que cruza el corte se levanta hasta el: pierde la parte que ya
    # cubre el archivo y conserva el valor, que es el que rige el dia del corte.
    w["_from"] = w["_from"].where(w["_from"] >= corte_ts, corte_ts)

    if strict:
        if a.empty or w.empty:
            raise _common.RegressionError(
                f"[clubelo.serie_empalmada] con corte {corte} un lado queda vacio "
                f"(archivo={len(a)} tramos, web={len(w)}). Revisa que los dos "
                "maestros esten poblados antes de publicar la serie.")
        # agujero real: el club existe en la web pero su serie arranca despues del
        # corte, asi que entre el corte y su primer punto no tiene escala vigente.
        inicio_web = w.groupby("team_key")["_from"].min()
        tarde = sorted(inicio_web[inicio_web > corte_ts].index.astype(str))
        vivos = set(a.loc[a["_to"] >= previo, "team_key"].dropna().astype(str))
        huecos = [k for k in tarde if k in vivos]
        if huecos:
            detalle = ", ".join(f"{k} desde {inicio_web[k].date()}" for k in huecos)
            raise _common.RegressionError(
                f"[clubelo.serie_empalmada] corte {corte} demasiado temprano: "
                f"{len(huecos)} club(es) con pagina en la web no la cubren todavia "
                f"en esa fecha ({detalle}). Sus partidos entre el corte y su primer "
                "punto arrastrarian el Elo del archivo en la escala antigua. Mueve "
                "ELO_CORTE a la primera fecha con el censo completo.")

    out = pd.concat([a, w], ignore_index=True)
    out["from"] = out["_from"].dt.strftime(fmt)
    out["to"] = out["_to"].dt.strftime(fmt)
    out = out.drop(columns=["_from", "_to"])
    out = out.drop_duplicates(subset=_KEY_COLS, keep="last")
    out = out.sort_values(["team_key", "from"]).reset_index(drop=True)
    out["date"] = out["from"]
    if strict:
        assert_una_escala(out)      # frontera unica, verificada sobre el resultado
    return out[cols + ["date"]]


def current_season_clubs(df: pd.DataFrame, *, today: str | None = None) -> list[str]:
    """`team_key` con tramo de Elo de Primera espanola VIGENTE a fecha `today`.

    Un tramo esta vigente si `from <= today` y su cierre (`to`) es posterior o
    esta abierto (NaN, el tramo que se mueve cada semana). El filtro por
    pais/nivel usa las columnas de la propia fuente: si un club ha ascendido,
    ClubElo lo reclasifica a nivel 1 en cuanto la temporada arranca.
    """
    today = today or date.today().isoformat()
    if df is None or df.empty or "team_key" not in df.columns:
        return []
    d = df
    desde = pd.to_datetime(d.get("from"), errors="coerce")
    hasta = pd.to_datetime(d.get("to"), errors="coerce")
    hoy = pd.Timestamp(today)
    vigente = (desde <= hoy) & (hasta.isna() | (hasta >= hoy))
    if "country" in d.columns:
        vigente &= d["country"].astype(str) == COUNTRY
    if "level" in d.columns:
        vigente &= pd.to_numeric(d["level"], errors="coerce") == TOP_LEVEL
    return sorted(d.loc[vigente, "team_key"].dropna().astype(str).unique())


def assert_current_season_coverage(df: pd.DataFrame, *, strict: bool = True,
                                   today: str | None = None,
                                   min_clubs: int = _MIN_CURRENT_CLUBS) -> list[str]:
    """Comprueba que los clubes de Primera de la temporada VIGENTE tienen Elo.

    Motivo: el descubrimiento por foto anual solo garantizaba las temporadas
    cerradas. Si un recien ascendido no aparece, sus partidos entran al modelo con
    Elo nulo y la senal de fuerza pre-partido se degrada en silencio justo en las
    jornadas que se van a predecir. Esta asercion convierte ese fallo silencioso
    en un error visible antes de publicar el parquet.

    Ventana de exigencia = temporada en juego, [1 de agosto, 1 de junio): fuera de
    ella no hay jornada que predecir y la clasificacion por nivel de ClubElo esta
    en transito. Dentro, hasta el 1 de septiembre solo se avisa (ClubElo puede no
    haber reclasificado todavia a los ascendidos). Devuelve los clubes vigentes.
    """
    today = today or date.today().isoformat()
    cur = _current_season_year(datetime.fromisoformat(today))
    en_juego = f"{cur}-{_SEASON_START_MD}" <= today < f"{cur + 1}-{_SEASON_END_MD}"
    if not en_juego:
        return current_season_clubs(df, today=today)

    clubs = current_season_clubs(df, today=today)
    if len(clubs) >= min_clubs:
        return clubs

    msg = (f"[clubelo] cobertura insuficiente de la temporada {cur}/{str(cur + 1)[2:]}: "
           f"{len(clubs)} clubes de Primera con Elo vigente a {today} "
           f"(se esperan >= {min_clubs}). Los ausentes puntuarian con Elo nulo. "
           "Ejecuta 'update' (o 'backfill' si el master es antiguo) para que el "
           "descubrimiento sondee la temporada en curso.")
    if strict and today >= f"{cur}-{_STRICT_COVERAGE_MD}":
        raise _common.SchemaError(msg)
    print("[WARN] " + msg)
    return clubs


def load_processed(
    master_csv: Path | str = _MASTER_CSV,
    *,
    out_parquet: Path | str | None = None,
    verbose: bool = True,
    strict: bool = True,
    cobertura: bool = True,
) -> pd.DataFrame:
    """Normaliza UN CSV maestro (una via): lee, fija tipos y valida.

    Salida: una fila por (club, tramo) con team, team_key, elo, rank, country,
    level, from, to y `date` (= from, para el merge_asof por fecha del partido).

    No publica por omision, porque el artefacto del proyecto no es la serie de una
    via sino la empalmada: quien escribe `clubelo_elo.parquet` es
    `process_empalmado`. Pasar `out_parquet` aqui sirve para inspeccionar una via
    suelta a un fichero aparte.

    `strict=True`: falla ruidosamente ante columnas ausentes, claves nulas o
    clubes sin clave canonica (ver teamnames.assert_known).
    `cobertura=False`: no exige el censo de Primera vigente. Es lo que necesita la
    via del archivo, cuya serie termina en 2022 por definicion; la cobertura se
    comprueba una vez sobre la serie empalmada, que es la que se publica.
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        raise _common.RegressionError(
            f"[clubelo.load_processed] no existe el maestro '{master_csv}'. "
            "Publicar un parquet vacio destruiria el vigente (las aserciones de "
            "esquema no detectan un frame sin filas). Ejecuta antes el backfill.")
    df = pd.read_csv(master_csv)
    _common.assert_columns(df, ["team", "team_key", "elo", "from", "to"],
                           source="clubelo.load_processed")
    assert_una_escala(df)

    # re-normaliza team_key por si el CSV se edito a mano
    if "team" in df.columns:
        df["team_key"] = df["team"].map(teamnames.to_key)
    df["elo"] = pd.to_numeric(df["elo"], errors="coerce")
    if "rank" in df.columns:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    if "level" in df.columns:
        df["level"] = pd.to_numeric(df["level"], errors="coerce").astype("Int64")
    for c in ("from", "to"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
    # `date` = inicio del tramo -> clave del merge_asof (Elo vigente <= fecha part.)
    df["date"] = df["from"]
    df = df.dropna(subset=["from", "elo"])
    df = df.sort_values(["team_key", "from"]).reset_index(drop=True)

    # ventana del proyecto: solo tramos de Elo vigentes en/despues del inicio de
    # la temporada FIRST_SEASON/17. Se conserva el tramo activo al arranque aunque
    # su `from` sea anterior (necesario para el merge_asof por fecha del partido).
    _cut = f"{FIRST_SEASON}-07-01"
    _to = pd.to_datetime(df["to"], errors="coerce")
    df = df[_to.isna() | (_to >= _cut)].reset_index(drop=True)

    # validaciones ruidosas (antes de publicar nada)
    if strict:
        _common.assert_non_null(df, ["team_key", "date"],
                                source="clubelo.load_processed")
        _common.assert_unique(df, ["team_key", "from"],
                              source="clubelo.load_processed")
        m = ~df["team_key"].isin(teamnames.CANONICAL)
        if m.any():
            teamnames.assert_known(df.loc[m, "team"].dropna().unique(),
                                   source="clubelo.load_processed")

    # la cobertura de la temporada vigente se comprueba siempre (en modo aviso si
    # strict=False): es el fallo que degrada justo las jornadas a predecir.
    if cobertura:
        assert_current_season_coverage(df, strict=strict)

    if verbose:
        _report(df)

    if out_parquet is not None:
        _common.publish_df(df, out_parquet, source="clubelo.load_processed",
                           fmt="parquet", verbose=verbose)
    return df


def process_empalmado(
    *,
    api_csv: Path | str = _MASTER_CSV,
    web_csv: Path | str = _MASTER_CSV_WEB,
    out_parquet: Path | str | None = _OUT_PARQUET,
    corte: str = ELO_CORTE,
    verbose: bool = True,
    strict: bool = True,
) -> pd.DataFrame:
    """Publica `clubelo_elo.parquet`: la serie unica de las dos vias empalmadas.

    Normaliza cada maestro por separado —ahi cada uno tiene una sola escala y las
    aserciones de via valen— y los empalma en `corte`. La cobertura de la
    temporada vigente se exige una sola vez, sobre el resultado: es la unica serie
    de la que se puede exigir, porque el archivo termina en 2022 y la web no llega
    a 2016.
    """
    api = load_processed(api_csv, out_parquet=None, verbose=False,
                         strict=strict, cobertura=False)
    web = load_processed(web_csv, out_parquet=None, verbose=False,
                         strict=strict, cobertura=False)
    df = serie_empalmada(api, web, corte=corte, strict=strict)
    assert_current_season_coverage(df, strict=strict)

    if verbose:
        _report(df)
        n_api = int((df["fuente"] == "api").sum())
        n_web = int((df["fuente"] == "web").sum())
        print(f"empalme en {corte}: {n_api} tramos del archivo + {n_web} de la web "
              f"({df['team_key'].nunique()} clubes)")

    if out_parquet is not None:
        _common.publish_df(df, out_parquet, source="clubelo.process_empalmado",
                           fmt="parquet", verbose=verbose)
    return df


def _report(df: pd.DataFrame) -> None:
    print("\n===== INFORME COBERTURA ClubElo =====")
    print(f"tramos (club-periodo) : {len(df)}")
    if "team_key" in df:
        print(f"clubes                : {df['team_key'].nunique()}")
    if "from" in df:
        d = pd.to_datetime(df["from"], errors="coerce")
        print(f"rango fechas          : {d.min()} -> {d.max()}")
    if "elo" in df:
        print(f"elo                   : {df['elo'].min():.0f} -> {df['elo'].max():.0f}")
    # cobertura de la temporada que se va a predecir (incluye ascendidos)
    vig = current_season_clubs(df)
    print(f"clubes Primera vigentes: {len(vig)} (esperados ~20, minimo "
          f"{_MIN_CURRENT_CLUBS})")
    # clubes no reconocidos por la capa de normalizacion
    if "team" in df:
        bad = teamnames.unknown_names(df["team"].dropna().unique())
        print(f"clubes NO reconocidos : {bad if bad else '[] (ok)'}")
    print("=====================================\n")


# =========================================================================== #
# CLI
# =========================================================================== #
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Extractor ClubElo LaLiga (rating Elo).")
    p.add_argument("cmd", choices=["backfill", "update", "process", "weekly"],
                   help="backfill=historico | update=refresca recientes | "
                        "process=normaliza | weekly=update+process")
    p.add_argument("--via", choices=["web", "api"], default="web",
                   help="de donde se EXTRAE: web=clubelo.com (viva, por omision) "
                        "| api=api.clubelo.com (jubilada, responde 502). No "
                        "afecta a 'process', que siempre publica la serie "
                        "empalmada de las dos.")
    p.add_argument("--seasons", nargs="+", type=int, default=None,
                   help="solo --via api: anos de inicio (2016 2017 ...).")
    p.add_argument("--master", default=None,
                   help="CSV maestro del que se extrae. Def.: el de la via "
                        "elegida (cada via tiene el suyo porque sus escalas de "
                        "Elo no son compatibles).")
    p.add_argument("--recent", type=int, default=2,
                   help="solo --via api: n temporadas mas recientes a escanear.")
    args = p.parse_args()

    web = args.via == "web"
    master = Path(args.master) if args.master else (
        _MASTER_CSV_WEB if web else _MASTER_CSV)
    print(f"[INFO] via={args.via}  maestro={master}")

    if args.cmd == "backfill":
        backfill_web(master_csv=master) if web else \
            backfill(seasons=args.seasons, master_csv=master)
    elif args.cmd == "update":
        update_web(master_csv=master) if web else \
            update(master_csv=master, recent_seasons=args.recent)
    elif args.cmd == "process":
        # el artefacto publicado es siempre el empalme de las dos vias: el archivo
        # aporta 16/17-22/23 y la web, de ahi en adelante.
        process_empalmado()
    elif args.cmd == "weekly":
        # la web no permite pedir un tramo, asi que su 'update' re-baja la serie
        # completa de los clubes conocidos: es la misma orden que el backfill
        # salvo que no sale a buscar clubes fuera del censo.
        update_web(master_csv=master) if web else \
            update(master_csv=master, recent_seasons=args.recent)
        if _ckpt_path(master).exists():
            print("[WARN] el master no se guardo (checkpoint pendiente); "
                  "NO regenero el parquet para no dejarlo desactualizado. "
                  "Resuelve el bloqueo y re-ejecuta 'weekly'.")
        else:
            process_empalmado()
