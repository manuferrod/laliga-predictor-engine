"""
Extractor de TIROS de Understat (calidad de ocasion) para laliga-predictor-engine.

Fuente: Understat publica, ademas del agregado por partido que recoge
`understat.py`, el detalle de CADA disparo del encuentro: el instante, el
resultado, la coordenada desde la que se remato, la jugada que lo origino, la
superficie de golpeo y el valor de gol esperado que su modelo le asigna. Este
modulo baja ese detalle y lo convierte en una descripcion de la CALIDAD DE LA
OCASION de cada equipo en cada partido.

Que anade el detalle de tiro sobre lo que ya hay
------------------------------------------------
El proyecto ya conoce cuantos tiros hizo un equipo (SofaScore) y cuanto xG
acumulo (Understat). Lo que no conoce es como se reparte ese xG entre los
disparos, y esa forma de la distribucion es informacion distinta: dos equipos
con quince tiros y un gol esperado y medio pueden haber llegado ahi por caminos
opuestos —quince remates lejanos de valor bajo, o cinco ocasiones nitidas y diez
disparos sin peligro—, y el pronostico de lo que haran la proxima jornada no es
el mismo en los dos casos. El agregado los confunde; el detalle los separa.

Las cuatro magnitudes que se publican
-------------------------------------
Por equipo y partido, calculadas SOLO sobre tiros en juego (ver mas abajo por
que se excluyen penaltis y goles en propia):

    shot_xg_mean         gol esperado del disparo tipico. Es la definicion
                         directa de calidad de ocasion: separa al equipo que
                         genera poco y bueno del que dispara mucho desde fuera.
    shot_dist_mean       distancia media al centro de la porteria, en metros.
                         Ninguna otra fuente del proyecto publica la posicion
                         del remate, de modo que esta es la unica lectura
                         territorial del ataque que no depende del modelo de xG.
    shot_setpiece_xg_pct cuota del gol esperado que nace de balon parado
                         (cornera, falta directa y jugada de falta). Distingue
                         al equipo que fabrica sus ocasiones jugando del que
                         depende de la estrategia, que es un rasgo persistente y
                         poco correlacionado con el volumen.
    shot_xg_max          gol esperado de la MEJOR ocasion del partido. Acompana
                         a la media porque las dos juntas describen la forma de
                         la distribucion: la media dice cuanto vale el disparo
                         corriente y el maximo si el equipo llego a tener una
                         clara. Un mismo promedio es compatible con ambas cosas.

Por que cuatro y no mas (mismo criterio que `understat.py`)
-----------------------------------------------------------
1. No se publican RECUENTOS. El volumen de tiros y de tiros a puerta ya viene de
   SofaScore, y repetirlo aqui solo anadiria una segunda medida del mismo hecho
   con una definicion ligeramente distinta, que es la peor clase de duplicado:
   la que parece informacion nueva. Este modulo se limita a lo que el recuento
   no puede decir, que es como son los tiros y no cuantos son.
2. No se publican las variantes "en contra". En un dato por partido con columnas
   local/visitante, la calidad de ocasion CONCEDIDA por el local es exactamente
   la calidad de ocasion GENERADA por el visitante: la columna existiria ya, con
   otro nombre, en la fila del rival. La forma defensiva se obtiene igual aguas
   abajo, porque `teamlong.py` reparte el partido en dos filas y cada equipo ve
   la del contrario.
3. No se publica la proporcion de tiros dentro del area, pese a ser la variable
   que primero viene a la cabeza. Mide lo mismo que la distancia media pero
   peor: binariza una magnitud continua en un limite —la linea del area— que no
   tiene ningun significado especial para el modelo de xG, y al hacerlo pierde
   la diferencia entre disparar desde veinte metros y hacerlo desde treinta y
   cinco. La distancia media conserva esa diferencia y no necesita elegir un
   corte. El indicador `in_box` si se calcula y se conserva en la tabla de
   tiros, donde su papel es descriptivo y no de modelado.
4. No se ajusta un modelo de xG propio. Seria el uso mas ambicioso de este dato
   y tambien el que peor relacion tiene entre coste y aportacion dentro de este
   trabajo: exigiria validar un modelo auxiliar entero para producir una
   variable que Understat ya publica y cuyo valor anadido sobre la suya esta sin
   demostrar. El detalle de tiro se aprovecha aqui por su distribucion, que es
   informacion que ninguna fuente del proyecto ofrece, y no por reestimar lo que
   ya esta estimado.

Que se excluye del calculo y por que
------------------------------------
* PENALTIS (`situation == "Penalty"`). Un penalti tiene siempre la misma
  distancia y un gol esperado proximo a 0,76, de modo que su unica aportacion a
  estas cuatro magnitudes es desplazarlas segun cuantos penaltis hubo, que es un
  hecho puntual y no un rasgo de como ataca el equipo. El proyecto ya trata los
  penaltis por separado en el agregado (`npxg` frente a `xg`), y esta exclusion
  es la misma decision aplicada al detalle.
* GOLES EN PROPIA (`result == "OwnGoal"`). Understat los inscribe en la lista
  del equipo cuyo jugador remato, es decir, del que los sufre. Contarlos como
  ocasion generada por ese equipo invertiria el signo de lo que se quiere medir.
* Un equipo puede terminar un partido sin ningun tiro admisible. Las cuatro
  magnitudes quedan entonces NULAS, no a cero: cero seria la afirmacion de que
  disparo desde la propia porteria con valor nulo, y lo cierto es que no hay
  ninguna distribucion de la que hablar. El vacio se propaga como tal hasta las
  ventanas de forma, que lo ignoran en lugar de promediarlo.

Como se obtienen los datos
--------------------------
Understat rehizo su web hacia diciembre de 2025 y los datos dejaron de ir
embebidos en el HTML para servirse por AJAX (el cambio esta documentado en
`understat.py`). La pagina de partido sigue el mismo patron, de modo que aqui se
prueban en orden varios puntos de entrada JSON plausibles y, si ninguno
responde, se cae al parseo del HTML clasico con `var shotsData = JSON.parse(...)`,
reutilizando el mismo lector que ya usa el extractor de liga. Si fallan todas
las vias el error explica que mirar en las herramientas de desarrollo, para no
confundir un cambio de la fuente con un partido inexistente.

DEPENDENCIA: la lista de partidos que hay que pedir sale del maestro de
`understat.py` (una peticion por partido, frente a una por temporada). Hay que
ejecutar antes su `backfill`. A cambio, el volcado es reanudable: el checkpoint
guarda por partido y una interrupcion no cuesta mas que el ultimo encuentro.

Estructura del modulo (identica a los demas extractores)
--------------------------------------------------------
A) CAPA DE SCRAPING (corre en LOCAL, red abierta):
     backfill(match_ids) -> descarga el detalle de todos los partidos conocidos
                            al CSV maestro `understat_laliga_shots.csv`.
     update()            -> EJECUTABLE SEMANALMENTE: solo los partidos del
                            maestro de liga que aun no tienen tiros.
B) CAPA DE ADAPTACION / NORMALIZACION (corre en cualquier sitio, solo pandas):
     load_processed()    -> publica DOS artefactos: `understat_shots.parquet`
                            (un tiro por fila, para el analisis descriptivo) y
                            `team_shot_quality.parquet` (un equipo-partido por
                            fila, que es lo que consume la consolidacion).

Uso tipico (en local, cada semana)
-----------------------------------
    python scripts/extractors/understat_shots.py weekly     # update + parquets
    python scripts/extractors/understat_shots.py backfill   # historico completo
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

try:  # ejecucion como paquete (from scripts.extractors import understat_shots)
    from . import _common
    from . import teamnames
    from . import understat as us
except ImportError:  # ejecucion como script suelto
    import _common  # type: ignore
    import teamnames  # type: ignore
    import understat as us  # type: ignore


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "understat"
# CSV maestro (crudo): un tiro por fila, todas las temporadas.
_SHOTS_CSV = _RAW_DIR / "understat_laliga_shots.csv"
# Parquet de tiros (tipado): el artefacto descriptivo, un tiro por fila.
_OUT_SHOTS = _REPO / "data" / "02_processed" / "understat_shots.parquet"
# Parquet equipo-partido: las cuatro magnitudes de calidad de ocasion.
_OUT_TEAM = _REPO / "data" / "02_processed" / "team_shot_quality.parquet"
# Maestro de liga del que sale la lista de partidos a pedir.
_MATCH_CSV = us._MATCH_STATS_CSV

_BASE_MATCH_URL = "https://understat.com/match"
# Puntos de entrada JSON candidatos para la pagina de partido. Understat no
# documenta su API y ya cambio una vez; se prueban en orden y el primero que
# devuelva un payload con tiros gana. Mantener varios no es indecision sino la
# unica forma de que un renombrado del endpoint no obligue a tocar codigo: si
# ninguno responde, queda el HTML clasico, y si tampoco, el error dice donde
# mirar.
_API_CANDIDATES = (
    "https://understat.com/getMatchData",
    "https://understat.com/getMatchShots",
)

# Magnitudes de calidad de ocasion por equipo-partido (prefijo shot_).
METRICS = ["shot_xg_mean", "shot_dist_mean", "shot_setpiece_xg_pct",
           "shot_xg_max"]

# --- geometria del campo ---------------------------------------------------- #
# Understat entrega las coordenadas normalizadas al intervalo [0, 1], con X = 1
# sobre la linea de gol atacada e Y = 0,5 en el eje longitudinal. Se convierten a
# metros con las medidas reglamentarias para que la distancia tenga unidades
# interpretables y comparables con cualquier otra fuente.
PITCH_X, PITCH_Y = 105.0, 68.0
_BOX_DEPTH, _BOX_WIDTH = 16.5, 40.32     # area grande reglamentaria
_BOX_X_MIN = 1.0 - _BOX_DEPTH / PITCH_X
_BOX_Y_HALF = (_BOX_WIDTH / 2.0) / PITCH_Y

# Jugadas que el proyecto considera balon parado. `SetPiece` es, en la
# nomenclatura de Understat, el remate que nace de una falta puesta al area, y
# `DirectFreekick` el lanzamiento directo a porteria; los dos son estrategia y se
# agrupan con el cornera. `Penalty` NO entra aqui: se excluye del calculo entero.
SETPIECE_SITUATIONS = frozenset({"FromCorner", "SetPiece", "DirectFreekick"})
PENALTY_SITUATION = "Penalty"
OWNGOAL_RESULT = "OwnGoal"


# =========================================================================== #
# A) CAPA DE SCRAPING
# =========================================================================== #
def _http_get(url: str, *, headers: dict, timeout: int = 30,
              retries: int = 2, wait: float = 2.0) -> tuple[int, str]:
    """GET -> (status, cuerpo). Delega en la maquinaria compartida."""
    return _common.http_get(url, headers=headers, timeout=timeout,
                            retries=retries, wait=wait)


# --- parseo (funciones puras, testeables sin red) -------------------------- #
def _norm_payload(data) -> dict[str, list]:
    """Normaliza la respuesta a {'h': [...], 'a': [...]}.

    Understat devuelve los tiros separados por lado, pero segun la via —endpoint
    JSON, HTML antiguo o una envoltura con el resto del partido— pueden llegar
    dentro de una clave 'shots' o como lista plana. Se aceptan las tres formas y
    se reduce a la unica que entiende `parse_shots`; una lista plana se reparte
    por el campo `h_a` de cada tiro, que siempre viaja con el.
    """
    if isinstance(data, dict) and "shots" in data:
        data = data["shots"]
    if isinstance(data, dict):
        return {"h": list(data.get("h", []) or []),
                "a": list(data.get("a", []) or [])}
    if isinstance(data, list):
        out: dict[str, list] = {"h": [], "a": []}
        for s in data:
            if isinstance(s, dict):
                out["h" if str(s.get("h_a", "h")) == "h" else "a"].append(s)
        return out
    return {"h": [], "a": []}


def _shot_distance(x, y) -> float | None:
    """Distancia en metros del punto de remate al CENTRO de la porteria.

    Se mide al centro y no al poste mas cercano porque lo que se quiere resumir
    es cuan lejos se dispara, no el angulo; el angulo esta ya incorporado al gol
    esperado que publica la fuente, de modo que anadirlo aparte solo repetiria
    una parte del mismo calculo.
    """
    xf, yf = us._to_float(x), us._to_float(y)
    if xf is None or yf is None:
        return None
    dx = (1.0 - xf) * PITCH_X
    dy = (yf - 0.5) * PITCH_Y
    return float(np.hypot(dx, dy))


def _in_box(x, y) -> bool | None:
    """Si el remate se produjo dentro del area grande. Descriptivo (ver cabecera)."""
    xf, yf = us._to_float(x), us._to_float(y)
    if xf is None or yf is None:
        return None
    return bool(xf >= _BOX_X_MIN and abs(yf - 0.5) <= _BOX_Y_HALF)


def _shot_row(s: dict, match_id: int, side: str) -> dict:
    """Un disparo del payload -> fila normalizada del maestro."""
    dt = str(s.get("date", ""))
    situation = str(s.get("situation", "") or "")
    result = str(s.get("result", "") or "")
    return {
        "shot_id": s.get("id"),
        "match_id": int(match_id),
        "side": "home" if side == "h" else "away",
        "datetime": dt,
        "date": dt[:10],
        "home_team": s.get("h_team"),
        "away_team": s.get("a_team"),
        "minute": us._to_float(s.get("minute")),
        "player": s.get("player"),
        "player_id": s.get("player_id"),
        "result": result,
        "situation": situation,
        "shot_type": s.get("shotType"),
        "last_action": s.get("lastAction"),
        "x": us._to_float(s.get("X")),
        "y": us._to_float(s.get("Y")),
        "xg": us._to_float(s.get("xG")),
        "dist_m": _shot_distance(s.get("X"), s.get("Y")),
        "in_box": _in_box(s.get("X"), s.get("Y")),
        "is_penalty": situation == PENALTY_SITUATION,
        "is_setpiece": situation in SETPIECE_SITUATIONS,
        "is_own_goal": result == OWNGOAL_RESULT,
    }


def parse_shots(payload, match_id: int) -> list[dict]:
    """Payload de un partido -> lista de filas-tiro (nucleo puro y testeable)."""
    data = _norm_payload(payload)
    rows: list[dict] = []
    for side in ("h", "a"):
        for s in data.get(side, []):
            if isinstance(s, dict):
                rows.append(_shot_row(s, match_id, side))
    return rows


# --- descarga de un partido (API nueva con fallback al HTML) ---------------- #
def _fetch_match_shots(match_id: int):
    """Descarga el payload de tiros de un partido.

    Prueba los endpoints JSON candidatos y, si ninguno sirve, el HTML clasico
    con `shotsData` embebido. Lanza `understat._NotFound` solo si TODAS las vias
    coinciden en que el partido no existe (404); cualquier otro fallo conjunto
    lanza RuntimeError con el diagnostico completo, para no confundir "Understat
    volvio a cambiar" con "ese partido no esta".
    """
    page_url = f"{_BASE_MATCH_URL}/{match_id}"
    problems: list[str] = []
    intentos, not_found = 0, 0

    headers_json = {
        **us._HEADERS,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": page_url,
    }
    for base in _API_CANDIDATES:
        url = f"{base}/{match_id}"
        intentos += 1
        try:
            status, body = _http_get(url, headers=headers_json)
        except RuntimeError as e:  # error de red ya reintentado
            problems.append(str(e))
            continue
        if status == 404:
            not_found += 1
            problems.append(f"HTTP 404 en {url}")
            continue
        if status != 200:
            problems.append(f"HTTP {status} en {url}: {body[:120]!r}")
            continue
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            problems.append(f"respuesta no-JSON de {url}: {body[:120]!r}")
            continue
        norm = _norm_payload(data)
        if norm["h"] or norm["a"]:
            return norm
        problems.append(f"JSON de {url} sin tiros reconocibles")

    # fallback: HTML clasico con los datos embebidos
    intentos += 1
    try:
        status, body = _http_get(page_url, headers=us._HEADERS)
        if status == 404:
            not_found += 1
            problems.append(f"HTTP 404 en {page_url}")
        elif status != 200:
            problems.append(f"HTTP {status} en {page_url}: {body[:120]!r}")
        else:
            try:
                return _norm_payload(us._json_from_var(body, "shotsData"))
            except ValueError as e:
                problems.append(f"{e} (inicio del HTML: {body[:120]!r})")
    except RuntimeError as e:
        problems.append(str(e))

    if not_found == intentos:  # ninguna via conoce el partido
        raise us._NotFound(page_url)
    raise RuntimeError(
        f"No pude obtener los tiros del partido {match_id} por ninguna via.\n  - "
        + "\n  - ".join(problems)
        + "\n  Contexto: Understat rehizo su web hacia dic 2025 y sirve los datos "
        "por AJAX. Si este error persiste, probablemente el endpoint de partido "
        f"tenga otro nombre: abre {page_url} en el navegador y mira en las "
        "DevTools (pestana Red/XHR) de donde carga ahora los tiros la pagina; "
        "basta anadir esa URL a _API_CANDIDATES."
    )


def _fetch_and_parse_match(match_id: int, *, verbose: bool = False) -> list[dict]:
    """Descarga y parsea un partido. [] si Understat no lo conoce."""
    try:
        payload = _fetch_match_shots(match_id)
    except us._NotFound:
        if verbose:
            print(f"  [{match_id}] no existe en Understat (se omite).")
        return []
    rows = parse_shots(payload, match_id)
    if verbose:
        print(f"  [{match_id}] tiros: {len(rows)}")
    return rows


# --- orquestacion backfill / update ---------------------------------------- #
def match_ids_from_league(match_csv: Path | str = _MATCH_CSV) -> list[int]:
    """Identificadores de partido conocidos por el extractor de liga.

    Es la lista de trabajo de este modulo. Se lee del maestro de `understat.py`
    en lugar de re-descubrirla porque los dos extractores hablan del mismo
    universo de partidos y duplicar el descubrimiento abriria la puerta a que
    discrepen.
    """
    match_csv = Path(match_csv)
    if not match_csv.exists():
        raise FileNotFoundError(
            f"no existe el maestro de partidos '{match_csv}'. El detalle de tiro "
            "se pide partido a partido, asi que hay que ejecutar antes "
            "'python scripts/extractors/understat.py backfill'."
        )
    df = pd.read_csv(match_csv, usecols=["match_id"])
    return sorted(int(v) for v in df["match_id"].dropna().unique())


def _load_master(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el CSV maestro de tiros. Devuelve (dataframe, match_id presentes)."""
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        ids = set(df["match_id"].unique()) if "match_id" in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} tiros de {len(ids)} partidos.")
        return df, ids
    print("[INFO] sin datos previos; se empieza de cero.")
    return pd.DataFrame(), set()


def _harvest(pendientes: list[int], master_csv: Path, df_master: pd.DataFrame,
             new_rows: list, *, pause: float, every: int) -> pd.DataFrame:
    """Bucle comun de backfill y update: pide partido a partido y va guardando.

    El checkpoint se reescribe cada `every` partidos y no en cada uno: escribir
    un CSV de decenas de miles de filas por cada peticion cuesta mas que
    re-descargar los pocos partidos que separan dos volcados.
    """
    total = len(pendientes)
    if not total:
        print("[INFO] no hay partidos pendientes: el maestro ya esta al dia.")
        return _finalize_master(df_master, new_rows, master_csv)

    print(f"[INFO] partidos pendientes: {total}")
    for i, mid in enumerate(pendientes, start=1):
        rows = _fetch_and_parse_match(mid)
        if rows:
            new_rows.extend(rows)
        else:
            print(f"  [{mid}] sin tiros (partido no cubierto o sin datos).")
        if i % every == 0 or i == total:
            _common.save_checkpoint(new_rows, master_csv)
            print(f"  progreso {i}/{total} ({i / total:.0%}) — "
                  f"{len(new_rows)} tiros acumulados")
        time.sleep(pause)

    return _finalize_master(df_master, new_rows, master_csv)


def backfill(
    match_ids=None,
    *,
    master_csv: Path | str = _SHOTS_CSV,
    match_csv: Path | str = _MATCH_CSV,
    pause: float = 1.0,
    checkpoint_every: int = 25,
) -> pd.DataFrame:
    """Descarga el detalle de tiro de TODOS los partidos conocidos.

    Es un volcado largo —una peticion por partido— pero se hace una sola vez y es
    reanudable: los `match_id` ya presentes en el maestro o en el checkpoint no
    se vuelven a pedir, de modo que re-ejecutar tras un corte continua donde lo
    dejo.
    """
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)

    df_master, existing = _load_master(master_csv)
    df_ckpt, ckpt_ids = _common.load_checkpoint(master_csv, key_col="match_id")
    new_rows: list = df_ckpt.to_dict("records") if not df_ckpt.empty else []
    existing = existing | ckpt_ids

    todos = ([int(m) for m in match_ids] if match_ids is not None
             else match_ids_from_league(match_csv))
    pendientes = [m for m in todos if m not in existing]
    return _harvest(pendientes, master_csv, df_master, new_rows,
                    pause=pause, every=checkpoint_every)


def update(
    *,
    master_csv: Path | str = _SHOTS_CSV,
    match_csv: Path | str = _MATCH_CSV,
    pause: float = 1.0,
    checkpoint_every: int = 25,
) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE. Anade el detalle de los partidos nuevos.

    No escanea temporadas sino la diferencia entre el maestro de liga y el de
    tiros: todo partido que `understat.py` ya conoce y del que aqui no hay ningun
    disparo. Asi la puesta al dia semanal cuesta exactamente los partidos de la
    jornada, y una temporada recuperada a posteriori se completa sola.
    """
    return backfill(match_ids=None, master_csv=master_csv, match_csv=match_csv,
                    pause=pause, checkpoint_every=checkpoint_every)


def _finalize_master(df_master: pd.DataFrame, new_rows: list,
                     master_csv: Path) -> pd.DataFrame:
    """Funde los tiros nuevos en el maestro, lo guarda y limpia el checkpoint."""
    if not new_rows:
        print("[INFO] no hay tiros nuevos: el maestro ya esta al dia.")
        return df_master
    df_new = pd.DataFrame(new_rows)
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty],
                          ignore_index=True)
    # La clave de un disparo es su propio identificador; el desempate por
    # completitud protege del caso en que una captura anterior se hiciera con el
    # partido en juego y algun campo aun vacio.
    if "shot_id" in df_master.columns:
        df_master = _common.dedupe_preferring_complete(df_master, "shot_id")
    df_master["match_key"] = teamnames.match_key(
        df_master["date"], df_master["home_team"], df_master["away_team"])
    if "datetime" in df_master.columns:
        df_master = df_master.sort_values(
            ["datetime", "match_id", "minute"]).reset_index(drop=True)

    if _common.write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] tiros Understat guardados -> {master_csv}  "
              f"({len(df_master)} tiros, "
              f"{df_master['match_id'].nunique()} partidos)")
        ckpt = _common.ckpt_path(master_csv)
        if ckpt.exists():
            try:
                ckpt.unlink()
            except Exception:  # noqa: BLE001
                pass
    return df_master


# =========================================================================== #
# B) CAPA DE ADAPTACION / NORMALIZACION  (solo pandas, testeable offline)
# =========================================================================== #
_SHOT_NEED = ["shot_id", "match_id", "side", "date", "home_team", "away_team",
              "xg", "situation", "result"]


def _prepare_shots(df: pd.DataFrame) -> pd.DataFrame:
    """Tipa el maestro crudo y recalcula lo derivado de las coordenadas.

    Lo derivado se RECALCULA en lugar de leerse del CSV a proposito: la geometria
    del campo y el criterio de balon parado son decisiones de este proyecto, no
    dato de la fuente, y deben poder cambiarse sin volver a descargar nada.
    """
    df = df.copy()
    for c in ("x", "y", "xg", "minute"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("situation", "result", "shot_type", "last_action", "side"):
        if c in df.columns:
            df[c] = df[c].astype("string")

    # `in_box` se tipa como booleano NULLABLE y no como booleano corriente: un
    # remate sin coordenadas no esta ni dentro ni fuera del area, y el tipo de
    # numpy no sabe expresar esa tercera posibilidad —obligaria a elegir entre
    # inventar un False o degradar la columna entera a objeto—. La distancia, al
    # ser continua, admite el nulo de siempre.
    sin_coord = df["x"].isna() | df["y"].isna()
    df["dist_m"] = np.hypot((1.0 - df["x"]) * PITCH_X, (df["y"] - 0.5) * PITCH_Y)
    df["in_box"] = ((df["x"] >= _BOX_X_MIN)
                    & ((df["y"] - 0.5).abs() <= _BOX_Y_HALF)).astype("boolean")
    df.loc[sin_coord, "dist_m"] = np.nan
    df.loc[sin_coord, "in_box"] = pd.NA
    df["is_penalty"] = df["situation"].eq(PENALTY_SITUATION).fillna(False)
    df["is_setpiece"] = df["situation"].isin(SETPIECE_SITUATIONS).fillna(False)
    df["is_own_goal"] = df["result"].eq(OWNGOAL_RESULT).fillna(False)

    df["home_key"] = df["home_team"].map(teamnames.to_key)
    df["away_key"] = df["away_team"].map(teamnames.to_key)
    df["match_key"] = teamnames.match_key(df["date"], df["home_team"],
                                          df["away_team"])
    return df


def team_shot_quality(shots: pd.DataFrame) -> pd.DataFrame:
    """Tiros -> una fila por equipo-partido con las cuatro magnitudes.

    Se agrega sobre los disparos ADMISIBLES —en juego, del propio equipo— segun
    el criterio razonado en la cabecera del modulo. Un equipo-partido sin ningun
    disparo admisible no genera fila: la ausencia viaja hasta el master como
    hueco y se lee como "no hubo distribucion que resumir", que es distinto de un
    cero.

    La cuota de balon parado se calcula sobre el gol esperado y no sobre el
    recuento de disparos, porque lo que interesa no es cuantas veces se remato de
    estrategia sino cuanto peligro salio de ella: un equipo puede rematar mucho
    en cornera sin generar nada y otro sacar de ahi su unica ocasion clara.
    """
    _common.assert_columns(shots, _SHOT_NEED, "understat_shots.team_quality")
    ok = shots.loc[~(shots["is_penalty"].astype(bool)
                     | shots["is_own_goal"].astype(bool))].copy()
    if ok.empty:
        return pd.DataFrame(columns=["match_key", "side", "date", "season_year"]
                                    + METRICS)

    ok["xg_sp"] = ok["xg"].where(ok["is_setpiece"].astype(bool), 0.0)
    g = ok.groupby(["match_key", "side"], dropna=True, sort=False)
    out = g.agg(
        date=("date", "first"),
        n_shots=("shot_id", "size"),
        xg_sum=("xg", "sum"),
        xg_sp_sum=("xg_sp", "sum"),
        shot_xg_mean=("xg", "mean"),
        shot_xg_max=("xg", "max"),
        shot_dist_mean=("dist_m", "mean"),
    ).reset_index()

    # Cuota de balon parado: indefinida —no cero— cuando el equipo no genero nada.
    # Un partido con cero gol esperado no reparte nada entre jugada y estrategia,
    # y escribir 0 afirmaria que todo salio de jugada abierta.
    denom = out["xg_sum"].where(out["xg_sum"] > 0)
    out["shot_setpiece_xg_pct"] = out["xg_sp_sum"] / denom

    d = pd.to_datetime(out["date"], errors="coerce")
    # Ano de inicio de temporada: LaLiga arranca en agosto, de modo que enero a
    # junio pertenecen a la temporada que empezo el ano anterior.
    out["season_year"] = np.where(d.dt.month >= 7, d.dt.year, d.dt.year - 1)
    out["season_year"] = out["season_year"].astype("Int64")

    cols = ["match_key", "side", "date", "season_year", "n_shots"] + METRICS
    out = out.loc[:, cols].sort_values(["date", "match_key", "side"])
    return out.reset_index(drop=True)


def load_processed(
    master_csv: Path | str = _SHOTS_CSV,
    *,
    out_shots: Path | str | None = _OUT_SHOTS,
    out_team: Path | str | None = _OUT_TEAM,
    verbose: bool = True,
    strict: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Lee el maestro de tiros y publica los dos artefactos procesados.

    Devuelve (tiros, calidad por equipo-partido). El primero es el material del
    analisis descriptivo —posicion, jugada, superficie de golpeo— y el segundo el
    que consume la consolidacion.
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        raise _common.RegressionError(
            f"[understat_shots.load_processed] no existe el maestro "
            f"'{master_csv}'. Publicar artefactos vacios destruiria los vigentes "
            "(las aserciones de esquema no detectan un frame sin filas). "
            "Ejecuta antes el backfill.")
    raw = pd.read_csv(master_csv)
    _common.assert_columns(raw, _SHOT_NEED, "understat_shots.load_processed")
    shots = _prepare_shots(raw)

    if strict:
        _common.assert_non_null(shots, ["match_key", "home_key", "away_key",
                                        "side"],
                                source="understat_shots.load_processed")
        _common.assert_unique(shots, "shot_id",
                              source="understat_shots.load_processed")
        if not set(shots["side"].dropna().unique()) <= {"home", "away"}:
            raise _common.SchemaError(
                "[understat_shots] 'side' inesperado: "
                f"{sorted(shots['side'].dropna().unique())}")
        bad = set()
        for lado in ("home", "away"):
            m = ~shots[f"{lado}_key"].isin(teamnames.CANONICAL)
            bad |= set(shots.loc[m, f"{lado}_team"].dropna().unique())
        if bad:
            teamnames.assert_known(bad, source="understat_shots.load_processed")

    team = team_shot_quality(shots)
    if strict:
        _common.assert_unique(team, ["match_key", "side"],
                              source="understat_shots.team_quality")

    if verbose:
        _report(shots, team)

    if out_shots is not None:
        _common.publish_df(shots, out_shots,
                           source="understat_shots.load_processed",
                           fmt="parquet", verbose=verbose)
    if out_team is not None:
        _common.publish_df(team, out_team,
                           source="understat_shots.team_quality",
                           fmt="parquet", verbose=verbose)
    return shots, team


def _report(shots: pd.DataFrame, team: pd.DataFrame) -> None:
    print("\n===== INFORME COBERTURA tiros Understat =====")
    print(f"tiros                 : {len(shots)}")
    print(f"partidos              : {shots['match_id'].nunique()}")
    print(f"equipo-partido        : {len(team)}")
    if "date" in shots:
        d = pd.to_datetime(shots["date"], errors="coerce")
        print(f"rango fechas          : {d.min()} -> {d.max()}")
    print(f"tiros por partido     : "
          f"{len(shots) / max(shots['match_id'].nunique(), 1):.1f} de media")
    for etiqueta, mask in (("penaltis", shots["is_penalty"]),
                           ("balon parado", shots["is_setpiece"]),
                           ("en propia", shots["is_own_goal"]),
                           ("dentro del area", shots["in_box"] == True)):  # noqa: E712
        print(f"{etiqueta:22s}: {mask.mean() * 100:5.1f}% de los tiros")
    print("--- magnitudes por equipo-partido ---")
    for c in METRICS:
        if c in team:
            s = pd.to_numeric(team[c], errors="coerce")
            print(f"{c:22s}: {s.notna().mean() * 100:5.1f}% no nulo  "
                  f"media={s.mean():.3f}  p5={s.quantile(.05):.3f}  "
                  f"p95={s.quantile(.95):.3f}")
    bad = set()
    for lado in ("home", "away"):
        if f"{lado}_key" in shots:
            m = ~shots[f"{lado}_key"].isin(teamnames.CANONICAL)
            bad |= set(shots.loc[m, f"{lado}_team"].dropna().unique())
    print(f"equipos NO reconocidos: {sorted(bad) if bad else '[] (ok)'}")
    print("=============================================\n")


# =========================================================================== #
# CLI
# =========================================================================== #
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Extractor de tiros de Understat (calidad de ocasion).")
    p.add_argument("cmd", choices=["backfill", "update", "process", "weekly"],
                   help="backfill=historico | update=partidos nuevos | "
                        "process=normaliza | weekly=update+process")
    p.add_argument("--matches", nargs="+", type=int, default=None,
                   help="backfill: match_id concretos (por defecto, todos los "
                        "del maestro de liga).")
    p.add_argument("--master", default=str(_SHOTS_CSV))
    p.add_argument("--match-csv", default=str(_MATCH_CSV))
    p.add_argument("--pause", type=float, default=1.0,
                   help="segundos entre peticiones (def. 1.0)")
    args = p.parse_args()

    if args.cmd == "backfill":
        backfill(match_ids=args.matches, master_csv=args.master,
                 match_csv=args.match_csv, pause=args.pause)
    elif args.cmd == "update":
        update(master_csv=args.master, match_csv=args.match_csv,
               pause=args.pause)
    elif args.cmd == "process":
        load_processed(master_csv=args.master)
    elif args.cmd == "weekly":
        update(master_csv=args.master, match_csv=args.match_csv,
               pause=args.pause)
        if _common.ckpt_path(Path(args.master)).exists():
            print("[WARN] el master no se guardo (checkpoint pendiente); "
                  "NO regenero los parquet para no dejarlos desactualizados. "
                  "Resuelve el bloqueo y re-ejecuta 'weekly'.")
        else:
            load_processed(master_csv=args.master)
