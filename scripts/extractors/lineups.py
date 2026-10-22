"""Extractor de ALINEACIONES Y MINUTOS (Sofascore, `/event/{id}/lineups`).

Del mismo endpoint del que salen las bajas (`availability.py`) cuelga la otra
mitad del parte: quien SI jugo y cuanto. Cada entrada de `players` trae el
jugador, si salio de inicio y los minutos que disputo. Sobre ese dato se
construyen las tres magnitudes que ninguna otra fuente del proyecto ofrece:

  * CONTINUIDAD DEL ONCE -- cuantos titulares repite un equipo de un partido al
    siguiente. Es una medida directa de rotacion y, por tanto, de la prioridad
    que el entrenador concede a cada encuentro. La fatiga de calendario dice que
    un equipo ha jugado tres partidos en ocho dias; la continuidad dice si los
    ha jugado con el mismo once o repartiendo la carga.
  * CARGA REAL POR JUGADOR -- minutos acumulados en una ventana movil. El bloque
    de fatiga cuenta PARTIDOS, que es un denominador grueso: un suplente que
    entra diez minutos y un titular que juega los noventa suman lo mismo. Los
    minutos son el denominador que faltaba.
  * CUOTA DE MINUTOS POR JUGADOR -- que fraccion de los minutos posibles ha
    disputado cada futbolista en lo que va de temporada. Es lo que permite pesar
    una baja por el papel que el jugador tiene EN SU EQUIPO y no solo por su
    valor de mercado (`consolidate/absences.py`).

QUE ES DATO DE PARTIDO Y QUE ES DATO PRE-PARTIDO
------------------------------------------------
Esta es la distincion que gobierna todo el modulo y conviene no darla por obvia.
La alineacion de un encuentro y los minutos que cada jugador disputa en el son
hechos DEL PARTIDO: describen lo que ocurrio, exactamente igual que los goles o
el xG, y usarlos crudos seria fuga de informacion aunque el once se publique una
hora antes del saque. El proyecto emite su prediccion con dias de antelacion, de
modo que ni siquiera esa hora de margen esta disponible.

En consecuencia, el crudo de este extractor tiene el mismo estatuto que la
estadistica de partido de Sofascore: es MATERIA PRIMA, nunca feature. Todo lo
que se publica se calcula mirando exclusivamente hacia atras, sobre partidos
ANTERIORES del mismo equipo, igual que hace `features/form.py` con el marcador.
Asi, `xi_continuity` no mide cuanto rota el equipo en el partido que se va a
predecir --eso no se sabe-- sino cuanto roto en el ultimo que jugo.

De esa asimetria se sigue una propiedad util: a diferencia de las bajas, que
dependen de que la fuente publique el parte medico y por eso pueden faltar en la
jornada a predecir, este bloque es conocible con dias de antelacion para
cualquier partido futuro, porque solo necesita el pasado. Por eso el universo a
DESCARGAR son los partidos ya jugados, mientras que el universo a PUBLICAR son
todos los partidos de LaLiga del calendario, jugados o no.

UNIVERSO MULTICOMPETICION
-------------------------
Se descargan las alineaciones de TODOS los partidos competitivos del calendario
de los clubes de LaLiga, no solo los de Liga. La razon es la misma por la que la
fatiga se calcula multicompeticion: los minutos de un miercoles europeo son
carga real, y contarlos solo en Liga subestimaria de forma sistematica el
desgaste de los cuatro o cinco clubes que juegan en Europa, que son ademas los
mas fuertes. Un sesgo correlacionado con la fuerza del equipo no es ruido: es
justo el tipo de error que el modelo aprenderia como senal.

El coste es un backfill largo (una peticion por partido y temporada de todas las
competiciones desde 2016/17). Es un gasto de una sola vez: el sentinela de
partidos consultados hace que las pasadas semanales solo pidan lo nuevo.

CONTRATO
--------
    backfill  -> alineaciones de todos los partidos competitivos ya jugados.
    update    -> solo los partidos recientes (runner semanal).
    process   -> reduce a jugador-partido y a equipo-partido, y publica.

Publica DOS artefactos, porque son dos granularidades distintas y cada una tiene
un consumidor distinto:

    data/02_processed/player_minutes.parquet  (jugador-partido)
        minutos y titularidad por jugador. Lo consume `consolidate/absences.py`
        para pesar las bajas por cuota de minutos.
    data/02_processed/team_lineups.parquet    (equipo-partido de LaLiga)
        las magnitudes estrictamente pasadas de rotacion y carga. Lo consume
        `consolidate/reducers.reduce_lineups`.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from . import _common
    from . import sofascore as sofa
    from . import teamnames as tn
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import _common  # type: ignore
    import sofascore as sofa  # type: ignore
    import teamnames as tn  # type: ignore


_KEY_COL = "row_id"
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "sofascore"
_MASTER_CSV = _RAW_DIR / "sofascore_lineups.csv"
_FIXTURES_CSV = _RAW_DIR / "sofascore_team_fixtures.csv"
_OUT_PLAYERS = _REPO / "data" / "02_processed" / "player_minutes.parquet"
_OUT_TEAMS = _REPO / "data" / "02_processed" / "team_lineups.parquet"

# Primera temporada del proyecto (ano de inicio). Coincide con `fatigue.FIRST_SEASON`
# porque ambos modulos recorren el mismo calendario.
FIRST_SEASON = 2016
_SINCE_TS = int(datetime(FIRST_SEASON, 7, 1, tzinfo=timezone.utc).timestamp())

# Un partido se considera jugado --y su alineacion, definitiva-- pasadas estas
# horas desde el saque. El margen evita sellar como "sin alineacion" un encuentro
# en curso, cuya lista de participantes aun crece con cada cambio.
_PLAYED_BUFFER_S = 4 * 3600

_SIDES = ("home", "away")
_MAX_FAIL_STREAK = 8       # fallos de transporte seguidos antes de abortar

# Un partido reglamentario reparte once titulares durante noventa minutos. Ambas
# constantes son el denominador de las cuotas y viven aqui, con nombre, para que
# el 11 y el 90 no aparezcan sueltos en medio de un calculo.
_XI = 11
_MINUTOS_REGLAMENTARIOS = 90

# Ventanas de carga (dias). Siete dias cubren la semana entre jornadas; catorce,
# el ciclo completo de un equipo con competicion europea de por medio.
_VENTANAS_CARGA = (7, 14)

# Numero de transiciones sobre las que se promedia la continuidad. Tres es el
# minimo que suaviza el ruido de un partido atipico --una eliminatoria de Copa
# con el filial-- sin diluir un cambio real de politica de rotacion.
_VENTANA_CONTINUIDAD = 3

# Magnitudes que el modulo publica por equipo-partido. El orden es el de lectura:
# primero rotacion, luego carga, luego profundidad.
STATS = ["xi_continuity", "xi_continuity_mean3",
         "starters_minutes_7", "starters_minutes_14",
         "minutes_hhi", "n_players_used"]


# --------------------------------------------------------------------------- #
#  A) Parseo del payload                                                       #
# --------------------------------------------------------------------------- #
def _minutos(item: dict) -> float | None:
    """Minutos disputados por un jugador, o None si la fuente no los publica.

    Sofascore anida las estadisticas individuales bajo `statistics`. Un suplente
    que no llega a entrar aparece en la lista SIN bloque de estadisticas: eso son
    cero minutos, no un dato ausente, y distinguirlo importa porque el cero es
    justamente lo que convierte la lista en una medida de reparto de carga.
    """
    st = item.get("statistics")
    if not isinstance(st, dict):
        # sin bloque de estadisticas: el jugador figuro en la convocatoria y no
        # jugo. Solo se puede afirmar para un suplente; de un titular sin
        # estadisticas no se sabe nada y se deja en None.
        return 0.0 if item.get("substitute") else None
    v = st.get("minutesPlayed")
    if v is None:
        return 0.0 if item.get("substitute") else None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _player_rows(event_id, lineups: dict) -> list[dict]:
    """Filas jugador-partido a partir de la respuesta de `/event/{id}/lineups`."""
    rows: list[dict] = []
    if not isinstance(lineups, dict):
        return rows
    for side in _SIDES:
        bloque = lineups.get(side) or {}
        for it in bloque.get("players") or []:
            p = it.get("player") or {}
            slug = p.get("slug") or tn.to_key(p.get("name"))
            if not slug:
                # sin identificador no hay forma de seguir al jugador entre
                # partidos, que es todo lo que este extractor hace con el.
                continue
            rows.append({
                "row_id": f"{event_id}|{side}|{slug}",
                "event_id": event_id,
                "side": side,
                "player": p.get("name"),
                "player_slug": slug,
                "position": it.get("position") or p.get("position"),
                "is_starter": not bool(it.get("substitute")),
                "minutes_played": _minutos(it),
            })
    return rows


def scrape_lineup(session, event_id) -> list[dict] | None:
    """Alineaciones de un partido.

    Devuelve:
      * list (posiblemente vacia) -> consulta correcta; [] significa que la
        fuente no publica alineacion de ese encuentro (habitual en competiciones
        menores y temporadas antiguas), lo que es un hecho definitivo.
      * None -> la peticion FALLO. El llamante lo distingue del caso anterior:
        un fallo de transporte se reintenta, un vacio definitivo se sella.
    """
    res = sofa._fetch_json(session, f"/api/v1/event/{event_id}/lineups")
    if not res.get("ok"):
        if res.get("err") == "404":     # no hay alineaciones publicadas
            return []
        return None
    return _player_rows(event_id, res.get("data"))


# --------------------------------------------------------------------------- #
#  B) De crudo a jugador-partido                                               #
# --------------------------------------------------------------------------- #
def _season_year(ts) -> float:
    """Ano de inicio de la temporada a la que pertenece un sello de saque.

    LaLiga arranca en agosto; el corte se pone en julio para que la pretemporada
    y los primeros amistosos de agosto caigan ya en la temporada nueva.
    """
    t = pd.to_numeric(ts, errors="coerce")
    if pd.isna(t):
        return np.nan
    d = datetime.fromtimestamp(float(t), tz=timezone.utc)
    return float(d.year if d.month >= 7 else d.year - 1)


def _calendario(fixtures_csv: Path = _FIXTURES_CSV) -> pd.DataFrame:
    """Calendario de equipo-partido, filtrado a lo competitivo y con temporada.

    Es el puente entre el mundo `event_id|side` del payload de alineaciones y el
    mundo `equipo` en el que hay que ordenar los partidos para mirar hacia atras.
    Solo contiene clubes de LaLiga --son los unicos cuyo calendario se rastrea--,
    de modo que de un partido europeo se recupera el lado espanol y se descarta
    el rival, que es justo lo que interesa.
    """
    fx = pd.read_csv(fixtures_csv)
    _common.assert_columns(
        fx, ["event_id", "team_id", "team", "is_home", "start_ts", "competition"],
        "lineups.calendario")
    fx = fx.copy()
    fx["event_id"] = pd.to_numeric(fx["event_id"], errors="coerce")
    fx["start_ts"] = pd.to_numeric(fx["start_ts"], errors="coerce")
    fx = fx.dropna(subset=["event_id", "start_ts"])
    fx["event_id"] = fx["event_id"].astype("int64")
    fx["is_home"] = fx["is_home"].astype(str).str.strip().str.lower().isin(("true", "1"))
    fx["side"] = np.where(fx["is_home"], "home", "away")
    # `is_competitive` se re-deriva del estado, igual que en `fatigue.py`: asi un
    # cambio de politica sobre que partidos cuentan como carga se aplica al
    # reprocesar y no obliga a volver a descargar el calendario.
    if "status" in fx.columns:
        st = fx["status"].astype(str).str.strip().str.lower()
        comp = fx["competition"].fillna("").astype(str)
        vivo = ~comp.str.contains(r"friendly|friendlies|amistoso", case=False, regex=True)
        fx["is_competitive"] = vivo & (~st.isin(
            {"canceled", "cancelled", "postponed", "suspended", "abandoned",
             "notstarted", "willstart"}))
    else:
        fx["is_competitive"] = fx.get("is_competitive", True)
    fx["team_key"] = fx["team"].map(tn.to_key)
    fx["season"] = fx["start_ts"].map(_season_year)
    fx["comp_id"] = pd.to_numeric(fx.get("comp_id"), errors="coerce")
    cols = ["event_id", "side", "team_id", "team", "team_key", "season",
            "start_ts", "competition", "comp_id", "is_competitive", "date"]
    return fx.loc[:, [c for c in cols if c in fx.columns]].drop_duplicates(
        ["event_id", "side"])


def to_player_level(raw: pd.DataFrame, cal: pd.DataFrame) -> pd.DataFrame:
    """Tabla jugador-partido: quien jugo, cuanto y con que equipo.

    Al cruzar con el calendario cada fila hereda equipo, temporada y sello de
    saque, que son las tres cosas que hacen falta para ordenar la carrera de un
    jugador en el tiempo. Las filas que no cruzan corresponden al rival extranjero
    de un partido europeo y se descartan sin ruido: su calendario nunca se
    rastreo y no forman parte del universo del proyecto.
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["event_id", "side", "team_key", "season",
                                     "start_ts", "player_key", "is_starter",
                                     "minutes_played"])
    d = raw.copy()
    d["event_id"] = pd.to_numeric(d["event_id"], errors="coerce")
    d = d.dropna(subset=["event_id"])
    d["event_id"] = d["event_id"].astype("int64")
    d["is_starter"] = d["is_starter"].astype(str).str.lower().isin(("true", "1"))
    d["minutes_played"] = pd.to_numeric(d["minutes_played"], errors="coerce")
    d["player_key"] = d["player_slug"].astype(str)
    d = d.merge(cal, on=["event_id", "side"], how="inner")
    d = d[d["is_competitive"].astype(bool)]
    cols = ["event_id", "side", "team_key", "team", "season", "start_ts",
            "competition", "comp_id", "player_key", "player", "position",
            "is_starter", "minutes_played"]
    out = d.loc[:, [c for c in cols if c in d.columns]]
    return out.sort_values(["start_ts", "event_id", "side"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
#  C) De jugador-partido a equipo-partido ESTRICTAMENTE PASADO                 #
# --------------------------------------------------------------------------- #
def _once(g: pd.DataFrame) -> set:
    """Titulares de un partido concreto, como conjunto de claves de jugador."""
    return set(g.loc[g["is_starter"], "player_key"])


def compute_team_features(players: pd.DataFrame, cal: pd.DataFrame,
                          *, tournament_id: int | None = None) -> pd.DataFrame:
    """Magnitudes de rotacion y carga por equipo-partido, mirando solo hacia atras.

    Para cada partido de LaLiga del calendario --jugado o por jugar-- se calcula,
    usando EXCLUSIVAMENTE los partidos anteriores del mismo equipo dentro de la
    misma temporada:

      xi_continuity        fraccion del once del ultimo partido que ya era
                           titular en el anterior. Mide la rotacion OBSERVADA,
                           no la que se producira en el partido a predecir.
      xi_continuity_mean3  la misma magnitud promediada sobre las tres ultimas
                           transiciones disponibles, para separar una politica de
                           rotacion de un partido atipico.
      starters_minutes_7   minutos medios acumulados en los siete dias previos
      starters_minutes_14  por los once que salieron de inicio en el ultimo
                           partido. Es la carga de quienes previsiblemente
                           vuelvan a jugar, y no la del vestuario entero.
      minutes_hhi          concentracion de Herfindahl de los minutos repartidos
                           en la temporada hasta la fecha. Alto = pocos jugadores
                           sostienen al equipo; bajo = plantilla larga en uso.
      n_players_used       futbolistas distintos con al menos un minuto en la
                           temporada hasta la fecha.

    Por que TODO se reinicia con la temporada: entre dos cursos hay un mercado de
    fichajes. Comparar el once del ultimo partido de mayo con el del primero de
    agosto no mide rotacion, mide plantilla nueva; y arrastrar los minutos de la
    temporada anterior falsearia la concentracion con jugadores que ya no estan.
    El precio es un arranque en frio de una o dos jornadas por temporada, que
    queda en NaN --hueco honesto-- y que el preprocesado marca con su indicador.
    """
    tid = tournament_id if tournament_id is not None else sofa.LALIGA_TOURNAMENT_ID
    vacio = pd.DataFrame(columns=["event_id", "side"] + STATS)
    if cal is None or cal.empty:
        return vacio

    # universo de SALIDA: partidos de LaLiga del calendario, jugados o no. El
    # bloque es publicable para un partido futuro justamente porque solo mira
    # hacia atras (ver el docstring del modulo).
    salida = cal[cal["comp_id"] == tid].copy() if "comp_id" in cal.columns else cal.copy()
    if salida.empty:
        return vacio

    # historial: un partido por equipo con su once y los minutos repartidos. Se
    # construye una sola vez y se recorre por equipo y temporada.
    if players is None or players.empty:
        hist = pd.DataFrame(columns=["team_key", "season", "event_id", "start_ts"])
    else:
        hist = players.copy()

    filas: list[dict] = []
    if not hist.empty:
        onces: dict[tuple, set] = {}
        minutos: dict[tuple, pd.DataFrame] = {}
        for (tk, ss, eid), g in hist.groupby(["team_key", "season", "event_id"],
                                             sort=False):
            onces[(tk, ss, eid)] = _once(g)
            minutos[(tk, ss, eid)] = g[["player_key", "minutes_played"]]
        orden = (hist[["team_key", "season", "event_id", "start_ts"]]
                 .drop_duplicates().sort_values(["team_key", "season", "start_ts"]))
    else:
        onces, minutos, orden = {}, {}, pd.DataFrame(
            columns=["team_key", "season", "event_id", "start_ts"])

    # indice de partidos anteriores por (equipo, temporada), ya ordenado
    previos: dict[tuple, list[tuple[float, int]]] = {}
    for (tk, ss), g in orden.groupby(["team_key", "season"], sort=False):
        previos[(tk, ss)] = list(zip(g["start_ts"].astype(float),
                                     g["event_id"].astype("int64")))

    for _, r in salida.iterrows():
        clave = (r["team_key"], r["season"])
        t = float(r["start_ts"])
        pasados = [(ts, eid) for ts, eid in previos.get(clave, []) if ts < t]
        fila = {"event_id": int(r["event_id"]), "side": r["side"],
                **{c: np.nan for c in STATS}}

        # --- rotacion: continuidad entre onces consecutivos ya observados ------
        conts: list[float] = []
        for i in range(len(pasados) - 1, 0, -1):
            a = onces.get((clave[0], clave[1], pasados[i][1]), set())
            b = onces.get((clave[0], clave[1], pasados[i - 1][1]), set())
            if len(a) < _XI or len(b) < _XI:
                # un once incompleto no es una rotacion: es una alineacion que la
                # fuente no publico entera, y promediarla inventaria movimiento.
                continue
            conts.append(len(a & b) / _XI)
            if len(conts) >= _VENTANA_CONTINUIDAD:
                break
        if conts:
            fila["xi_continuity"] = conts[0]
            fila["xi_continuity_mean3"] = float(np.mean(conts))

        if not pasados:
            filas.append(fila)
            continue

        # --- carga: minutos de los once que jugaron el ultimo partido ---------
        ultimo = pasados[-1][1]
        once = onces.get((clave[0], clave[1], ultimo), set())
        acumulados = pd.concat(
            [minutos[(clave[0], clave[1], eid)].assign(_ts=ts)
             for ts, eid in pasados if (clave[0], clave[1], eid) in minutos],
            ignore_index=True) if pasados else pd.DataFrame()
        if once and not acumulados.empty:
            for dias in _VENTANAS_CARGA:
                ventana = acumulados[acumulados["_ts"] >= t - dias * 86400]
                suma = (ventana[ventana["player_key"].isin(once)]
                        .groupby("player_key")["minutes_played"].sum())
                # los titulares sin minutos en la ventana cuentan como cero: es
                # informacion (han descansado), no ausencia de dato.
                fila[f"starters_minutes_{dias}"] = float(
                    suma.reindex(list(once)).fillna(0.0).mean())

        # --- profundidad: reparto de minutos en la temporada hasta la fecha ----
        if not acumulados.empty:
            por_jugador = acumulados.groupby("player_key")["minutes_played"].sum()
            por_jugador = por_jugador[por_jugador > 0]
            total = float(por_jugador.sum())
            if total > 0:
                cuotas = (por_jugador / total).to_numpy(dtype=float)
                fila["minutes_hhi"] = float(np.sum(cuotas ** 2))
                fila["n_players_used"] = float(len(por_jugador))
        filas.append(fila)

    out = pd.DataFrame(filas, columns=["event_id", "side"] + STATS)
    return out.sort_values(["event_id", "side"]).reset_index(drop=True)


def minutes_share_at(players: pd.DataFrame, queries: pd.DataFrame) -> pd.DataFrame:
    """Cuota de minutos de un jugador en su equipo ANTES de un instante dado.

    Es la definicion primitiva del modulo y la unica que se escribe una vez:

        cuota(jugador, equipo, temporada, t) =
            minutos que el jugador ha disputado con ese equipo en esa temporada
            en partidos anteriores a `t`
            -------------------------------------------------------------------
            90 x numero de partidos que el equipo ha jugado antes de `t`

    Un titular indiscutible ronda 1; un suplente habitual, 0,2. El denominador
    cuenta partidos del EQUIPO y no del jugador, de modo que un fichaje de
    invierno no aparece como fijo por haber jugado entero lo poco que lleva: su
    cuota crece a medida que acumula partidos.

    `queries` es una tabla con `team_key`, `season`, `player_key` y `start_ts`
    (el instante de corte). Devuelve, alineadas a su indice, `minutes_share` y
    `team_matches_before`.

    Que la consulta admita un instante ARBITRARIO --y no solo los partidos en los
    que el jugador aparecio-- es lo que la hace util para pesar una baja
    (`consolidate/absences.py`): un ausente, por definicion, no figura en la
    alineacion del partido que se quiere valorar. Y tiene una consecuencia
    deseable: mientras dura la ausencia el numerador se congela y el denominador
    sigue creciendo, asi que el peso de una baja de larga duracion decae solo. Es
    el comportamiento correcto, porque un equipo que lleva dos meses sin un
    futbolista ya no le echa de menos igual que el primer dia.

    Sin partidos previos del equipo la cuota es NaN --hueco honesto-- y no cero,
    que significaria "no juega". Un jugador del que no consta ningun minuto en la
    temporada, en cambio, si recibe cero: es lo que se sabe de el.
    """
    idx = queries.index if queries is not None else pd.RangeIndex(0)
    vacio = pd.DataFrame({"minutes_share": pd.Series(dtype=float),
                          "team_matches_before": pd.Series(dtype=float)}, index=idx)
    if players is None or players.empty or queries is None or queries.empty:
        return pd.DataFrame({"minutes_share": np.nan, "team_matches_before": np.nan},
                            index=idx) if len(idx) else vacio

    p = players.loc[:, ["team_key", "season", "player_key", "start_ts",
                        "minutes_played"]].copy()
    p["minutes_played"] = pd.to_numeric(p["minutes_played"], errors="coerce").fillna(0.0)
    p["start_ts"] = pd.to_numeric(p["start_ts"], errors="coerce")
    p["season"] = pd.to_numeric(p["season"], errors="coerce")
    p = p.dropna(subset=["start_ts", "season"]).sort_values("start_ts")

    # calendario del equipo: instantes de saque de sus partidos en la temporada
    cal = {k: np.sort(g["start_ts"].unique())
           for k, g in p.groupby(["team_key", "season"], sort=False)}
    # carrera del jugador: instantes y minutos acumulados hasta cada uno de ellos
    carrera: dict[tuple, tuple] = {}
    for k, g in p.groupby(["team_key", "season", "player_key"], sort=False):
        g = g.sort_values("start_ts")
        carrera[k] = (g["start_ts"].to_numpy(dtype=float),
                      np.cumsum(g["minutes_played"].to_numpy(dtype=float)))

    q_ss = pd.to_numeric(queries["season"], errors="coerce")
    q_ts = pd.to_numeric(queries["start_ts"], errors="coerce")
    share = np.full(len(queries), np.nan)
    antes = np.full(len(queries), np.nan)
    for i, (tk, ss, pk, t) in enumerate(zip(queries["team_key"], q_ss,
                                            queries["player_key"], q_ts)):
        if pd.isna(t) or pd.isna(ss):
            continue
        ts_eq = cal.get((tk, ss))
        if ts_eq is None:
            continue
        n = int(np.searchsorted(ts_eq, float(t), side="left"))
        antes[i] = float(n)
        if n == 0:
            continue                      # el equipo aun no ha jugado: sin base
        ts_p, acum = carrera.get((tk, ss, pk), (None, None))
        minutos = 0.0
        if ts_p is not None:
            j = int(np.searchsorted(ts_p, float(t), side="left"))
            minutos = float(acum[j - 1]) if j > 0 else 0.0
        share[i] = min(minutos / (n * _MINUTOS_REGLAMENTARIOS), 1.0)

    return pd.DataFrame({"minutes_share": share, "team_matches_before": antes},
                        index=idx)


def season_minutes_share(players: pd.DataFrame) -> pd.DataFrame:
    """Cuota de minutos que acompana a cada fila jugador-partido.

    Es `minutes_share_at` evaluada en el propio partido de cada fila, de modo que
    la cuota publicada junto a un partido NO incluye los minutos de ese partido.
    Ese desplazamiento es lo que permite leerla en inferencia, cuando el partido
    todavia no se ha jugado.
    """
    cols = ["event_id", "side", "team_key", "season", "player_key",
            "minutes_share", "team_matches_before"]
    if players is None or players.empty:
        return pd.DataFrame(columns=cols)

    d = players.loc[:, ["event_id", "side", "team_key", "season", "player_key",
                        "start_ts", "minutes_played"]].copy()
    d = d.join(minutes_share_at(players, d))
    return d.loc[:, cols].reset_index(drop=True)


# --------------------------------------------------------------------------- #
#  D) Persistencia, sentinela y orquestacion                                   #
# --------------------------------------------------------------------------- #
def _load_master(master_csv: Path) -> tuple[pd.DataFrame, set]:
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        hechos = set(df["event_id"].astype(str)) if "event_id" in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} filas, {len(hechos)} partidos.")
        return df, hechos
    print("[INFO] sin datos previos.")
    return pd.DataFrame(), set()


def _finalize_master(df_master, new_rows, master_csv: Path):
    if not new_rows:
        return df_master
    df_new = pd.DataFrame(new_rows)
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty],
                          ignore_index=True)
    if _KEY_COL in df_master.columns:
        df_master = df_master.drop_duplicates(_KEY_COL, keep="last")
    if sofa._write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] -> {master_csv} ({len(df_master)} filas)")
    return df_master


def _checked_path(master_csv: Path) -> Path:
    """Sentinela de partidos consultados SIN alineacion publicada.

    Sin el, un partido de Copa de 2016 del que Sofascore no guarda alineaciones
    devuelve una lista vacia, no deja rastro en el maestro y se vuelve a pedir en
    cada pasada, para siempre. Con nueve temporadas y todas las competiciones,
    esos partidos son cientos.
    """
    master_csv = Path(master_csv)
    return master_csv.parent / f"_checked_{master_csv.stem}.csv"


def _load_checked(master_csv: Path) -> tuple[pd.DataFrame, set]:
    p = _checked_path(master_csv)
    if not p.exists():
        return pd.DataFrame(columns=["event_id", "checked_at"]), set()
    try:
        df = pd.read_csv(p)
        return df, set(df["event_id"].astype(str))
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] sentinela '{p.name}' ilegible ({e}); se re-consultaran "
              "los partidos sin alineacion.")
        return pd.DataFrame(columns=["event_id", "checked_at"]), set()


def _save_checked(df_checked: pd.DataFrame, new_ids: list,
                  master_csv: Path) -> pd.DataFrame:
    if not new_ids:
        return df_checked
    add = pd.DataFrame({"event_id": [str(x) for x in new_ids],
                        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S")})
    df_checked = pd.concat([df_checked, add], ignore_index=True)
    df_checked = df_checked.drop_duplicates("event_id", keep="last")
    sofa._write_df_safe(df_checked, _checked_path(master_csv), fmt="csv", backup=False)
    return df_checked


def _played_ids(cal: pd.DataFrame) -> list[int]:
    """Partidos competitivos YA JUGADOS cuyo once tiene sentido pedir.

    Solo los jugados: la alineacion de un partido futuro no existe todavia y, lo
    que importa mas, este bloque no la necesita --se calcula mirando hacia atras--.

    El universo es el historico entero y no una ventana de dias, tambien en la
    pasada semanal. Lo que decide que se descarga es el atraso real --maestro mas
    sentinela, en `_run`--, no el calendario: una ventana fija cubre la semana
    solo si la pasada anterior se ejecuto, y si el proyecto estuvo parado deja un
    hueco permanente sin dar error, porque el extractor termina diciendo que todo
    fue bien. En una semana corriente el atraso son los diez partidos de la
    jornada; tras un paron del proyecto, los que falten.
    """
    if cal is None or cal.empty:
        return []
    corte = time.time() - _PLAYED_BUFFER_S
    d = cal[cal["is_competitive"].astype(bool)]
    d = d[(d["start_ts"] >= _SINCE_TS) & (d["start_ts"] < corte)]
    return sorted(set(d["event_id"].astype("int64").tolist()))


def _run(event_ids, master_csv: Path):
    """Descarga las alineaciones que faltan, con reanudacion e idempotencia.

    Un partido esta "hecho" si dejo filas en el maestro o figura en el sentinela.
    Los fallos de transporte no marcan nada y se reintentan en la pasada
    siguiente; `_MAX_FAIL_STREAK` fallos seguidos abortan guardando lo obtenido,
    porque a esas alturas lo que ocurre no es que falten alineaciones sino que la
    fuente esta caida, y seguir pidiendo solo produce ruido.
    """
    session = sofa._new_session()
    df_master, hechos_master = _load_master(master_csv)
    df_checked, hechos_checked = _load_checked(master_csv)
    hechos = hechos_master | hechos_checked
    new_rows: list = []
    new_checked: list = []
    vacios = fallos = racha = 0
    try:
        pend = [e for e in event_ids if str(e) not in hechos]
        print(f"[INFO] partidos a consultar: {len(pend)}")
        for i, eid in enumerate(pend, 1):
            rows = scrape_lineup(session, eid)
            if rows is None:
                fallos += 1
                racha += 1
                if racha >= _MAX_FAIL_STREAK:
                    df_master = _finalize_master(df_master, new_rows, master_csv)
                    _save_checked(df_checked, new_checked, master_csv)
                    raise SystemExit(
                        f"\n[ERROR] {racha} partidos seguidos con fallo de "
                        "descarga: el transporte a Sofascore esta caido. Lo "
                        "obtenido esta guardado; re-ejecuta para reanudar."
                    )
                continue
            racha = 0
            if rows:
                new_rows.extend(rows)
            else:
                vacios += 1
                new_checked.append(eid)
            if i % 25 == 0:
                print(f"  [{i}/{len(pend)}] filas acumuladas: {len(new_rows)}")
                df_master = _finalize_master(df_master, new_rows, master_csv)
                df_checked = _save_checked(df_checked, new_checked, master_csv)
                new_rows, new_checked = [], []
            time.sleep(0.2)
        df_master = _finalize_master(df_master, new_rows, master_csv)
        _save_checked(df_checked, new_checked, master_csv)
    finally:
        session.close()
    print(f"[INFO] partidos sin alineacion publicada: {vacios}; "
          f"fallos de transporte (se reintentaran): {fallos}")
    return df_master


def backfill(master_csv: Path = _MASTER_CSV, *, fixtures_csv: Path = _FIXTURES_CSV):
    Path(master_csv).parent.mkdir(parents=True, exist_ok=True)
    cal = _calendario(fixtures_csv)
    _run(_played_ids(cal), master_csv)
    process(master_csv, fixtures_csv=fixtures_csv)


def update(master_csv: Path = _MASTER_CSV, *, fixtures_csv: Path = _FIXTURES_CSV):
    """Pasada semanal: descarga las alineaciones que faltan, sean de esta jornada
    o de cualquier otra anterior que se quedara sin cubrir."""
    cal = _calendario(fixtures_csv)
    _run(_played_ids(cal), master_csv)
    process(master_csv, fixtures_csv=fixtures_csv)


def process(master_csv: Path = _MASTER_CSV, *,
            fixtures_csv: Path = _FIXTURES_CSV,
            out_players: Path = _OUT_PLAYERS,
            out_teams: Path = _OUT_TEAMS,
            verbose: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reduce el crudo y publica los dos artefactos.

    Publicar dos tablas desde un solo `process` no es una excepcion sino la
    consecuencia de que el crudo tiene dos lecturas legitimas y distintas: una
    por jugador, que pesa las bajas, y otra por equipo-partido, que describe la
    rotacion. Separarlas en dos extractores obligaria a descargar dos veces el
    mismo endpoint.
    """
    master_csv = Path(master_csv)
    sentinela = _checked_path(master_csv)
    if not master_csv.exists() and not sentinela.exists():
        raise _common.RegressionError(
            f"[lineups.process] no existe ni el maestro '{master_csv.name}' ni el "
            f"sentinela '{sentinela.name}': no hay nada consultado que publicar y "
            "un parquet vacio destruiria el vigente. Ejecuta antes el backfill.")
    raw = pd.read_csv(master_csv) if master_csv.exists() else pd.DataFrame()
    cal = _calendario(fixtures_csv)

    jugadores = to_player_level(raw, cal)
    cuotas = season_minutes_share(jugadores)
    # la cuota viaja junto a los minutos: es la lectura util del jugador-partido
    # y calcularla dos veces en dos modulos distintos seria pedir que se separen.
    jugadores = jugadores.merge(
        cuotas[["event_id", "side", "player_key", "minutes_share",
                "team_matches_before"]],
        on=["event_id", "side", "player_key"], how="left")
    equipos = compute_team_features(jugadores, cal)

    if verbose:
        _report(jugadores, equipos)
    _common.publish_df(jugadores, out_players, source="lineups.process",
                       fmt="parquet", verbose=verbose)
    _common.publish_df(equipos, out_teams, source="lineups.process",
                       fmt="parquet", verbose=verbose)
    return jugadores, equipos


def _report(jug: pd.DataFrame, eq: pd.DataFrame) -> None:
    print("\n===== INFORME ALINEACIONES Y MINUTOS =====")
    print(f"filas jugador-partido : {len(jug)}")
    if len(jug):
        print(f"partidos con alineacion: {jug['event_id'].nunique()}")
        print(f"jugadores distintos    : {jug['player_key'].nunique()}")
        print(f"temporadas             : "
              f"{sorted(jug['season'].dropna().unique().astype(int).tolist())}")
        titulares = jug.groupby(["event_id", "side"])["is_starter"].sum()
        print(f"titulares por equipo-partido: media {titulares.mean():.2f} "
              f"(deberia ser 11), fuera de 11: {(titulares != _XI).sum()}")
        print(f"minutos medios por jugador: {jug['minutes_played'].mean():.1f}")
    print(f"\nfilas equipo-partido (LaLiga): {len(eq)}")
    if len(eq):
        cob = eq[STATS].notna().mean()
        print("cobertura de cada magnitud (el hueco es el arranque de temporada):")
        for c in STATS:
            print(f"   {cob[c]:6.1%}  {c}")
        print(f"continuidad media del once: {eq['xi_continuity'].mean():.3f}")
        print(f"concentracion media de minutos (HHI): {eq['minutes_hhi'].mean():.4f}")
    print("==========================================\n")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "process"
    if cmd == "backfill":
        backfill()
    elif cmd in ("update", "weekly", "schedule"):
        # `update` ya publica los dos parquets al terminar, de modo que es en si
        # misma la rutina semanal: `weekly` es su nombre en el manual de operacion
        # y los extractores responden todos al mismo verbo.
        update()
    elif cmd == "process":
        process()
    else:
        print("uso: python lineups.py [backfill|update|weekly|process|schedule]")
