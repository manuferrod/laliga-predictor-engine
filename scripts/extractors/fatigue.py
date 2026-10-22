"""Extractor de FATIGA / CONGESTION (Sofascore, calendario multi-competicion).

Idea: los dias de descanso reales de un equipo dependen de TODAS sus
competiciones (Liga + Copa + Europa + Supercopa + Mundial de Clubes), no solo
de LaLiga. Este modulo cosecha el calendario completo de cada equipo de LaLiga
y deriva features de fatiga por partido, que luego se cruzan (local vs
visitante -> rest_diff) en la consolidacion.

Patron estandar del proyecto:
    backfill  -> historia completa desde FIRST_SEASON (una vez).
    update    -> refresca la ventana reciente + proximos partidos (semanal).
    process   -> re-deriva features y escribe el parquet.
    schedule  -> update + process (para el runner).

Reutiliza el TRANSPORTE de sofascore.py (curl_cffi en cascada) y teamnames, por lo
que la capa de scraping se ejecuta en la maquina local, igual que aquel.

DOS CAPAS:
    raw   data/01_raw/sofascore/sofascore_team_fixtures.csv   (1 fila = equipo-partido, todas las comps)
    proc  data/02_processed/team_fatigue.parquet              (solo comps competitivas + features)

DECISIONES:
  * Los AMISTOSOS se excluyen del computo de descanso/congestion (no son carga real).
  * Se excluyen tambien los partidos NO DISPUTADOS (aplazados, cancelados,
    suspendidos): figuran en el calendario con su fecha original y, de contarlos,
    inflarian la congestion con una carga que nunca existio. Ver `_NOT_PLAYED`.
  * Los partidos AUN NO JUGADOS (`notstarted`) SI cuentan, por paridad con el
    entrenamiento: al predecir la jornada N ya se habran disputado. Ver `_NOT_PLAYED`.
  * El PARON DE SELECCIONES se aproxima por ventana FIFA (hueco grande en meses de
    paron), sin rastrear cada jugador (barato, ~80% del efecto).
"""
from __future__ import annotations

import re
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
except ImportError:  # ejecucion como script suelto
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import _common  # type: ignore
    import sofascore as sofa  # type: ignore
    import teamnames as tn  # type: ignore


FIRST_SEASON = 2016
_SINCE_TS = int(datetime(FIRST_SEASON, 7, 1, tzinfo=timezone.utc).timestamp())

# HUSO HORARIO DE REFERENCIA. Sofascore entrega marcas de tiempo UTC, pero el
# calendario de LaLiga que consolida `sofascore.py` esta expresado en hora local
# peninsular (`tz_convert("Europe/Madrid")`). Derivar aqui la fecha en UTC
# introduce dos discrepancias con esa referencia: un saque nocturno (22:00 CEST
# del domingo = 20:00 UTC del domingo, pero 00:00 CEST del lunes en invierno con
# horarios tardios de competiciones al este) puede caer en el dia anterior, lo que
# desplaza `match_key` y desconecta la fila del maestro; y el dia de la semana que
# alimenta `is_midweek` puede corresponder a otra jornada. Se usa por tanto el
# mismo huso que la fuente consolidada. La conversion se hace con pandas, que
# incorpora su propia base de husos y no depende de que el sistema tenga `tzdata`.
_TZ_LOCAL = "Europe/Madrid"


def _fecha_local(ts) -> str | None:
    """'YYYY-MM-DD' del saque en hora local peninsular (None si no hay marca)."""
    if ts is None or (isinstance(ts, float) and np.isnan(ts)):
        return None
    try:
        return (pd.Timestamp(int(ts), unit="s", tz="UTC")
                  .tz_convert(_TZ_LOCAL).strftime("%Y-%m-%d"))
    except (ValueError, OverflowError, TypeError):
        return None


def _local_dt(ts) -> pd.Timestamp:
    """Marca de tiempo del saque en hora local peninsular."""
    return pd.Timestamp(int(ts), unit="s", tz="UTC").tz_convert(_TZ_LOCAL)


# amistosos: fuera del computo de carga
_FRIENDLY_RE = re.compile(r"friendly|friendlies|amistoso", re.I)

# Estados de partido que NO representan carga real: el encuentro no se disputo en
# la fecha en la que aparece listado. Un aplazado es el caso critico: Sofascore lo
# mantiene con su fecha original hasta que se reprograma, asi que sin esta
# exclusion inflaria `games_last_7/14` y acortaria `rest_days` de los partidos
# siguientes con una carga que nunca existio.
#
# `notstarted` NO esta en la lista, y es deliberado: al puntuar una jornada futura,
# los partidos anteriores del equipo todavia no se han jugado pero SI se habran
# jugado cuando llegue el partido a predecir. Contarlos es la estimacion correcta de
# la carga y, sobre todo, es lo que mantiene la paridad con el entrenamiento: si se
# excluyeran, `games_last_7/14` seria sistematicamente menor en inferencia que en
# entrenamiento y el modelo veria una distribucion distinta a la que aprendio.
_NOT_PLAYED = {"canceled", "cancelled", "postponed", "suspended", "abandoned",
               "willcontinue", "deleted"}
# competiciones europeas de clubes (para detectar viaje europeo reciente)
_EURO_RE = re.compile(r"champions league|europa league|conference league", re.I)

# proxy ventana FIFA: un hueco >= 12 dias en un mes de paron = paron de selecciones
_INTL_MONTHS = {3, 6, 9, 10, 11}
_INTL_GAP_DAYS = 12
# congestion / midweek
_MIDWEEK_WD = {1, 2, 3}  # mar/mie/jue (Mon=0..Sun=6)
_EURO_TRIP_DAYS = 5      # ventana para "viajo a Europa hace poco"

# Umbral de tolerancia a fallos de cosecha: por encima de esta fraccion de
# equipos con calendario truncado la pasada se considera degradada y se aborta.
# Un 25% se ha fijado como el punto en que dejan de ser incidencias aisladas
# (bloqueo puntual del anti-bot en un equipo) y pasa a ser una caida del servicio.
_MAX_FAILED_FRAC = 0.25

_KEY_COL = "row_id"
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "sofascore"
_MASTER_CSV = _RAW_DIR / "sofascore_team_fixtures.csv"
_OUT_PARQUET = _REPO / "data" / "02_processed" / "team_fatigue.parquet"


# =========================================================================== #
#  A) SCRAPING (LOCAL): calendario multi-competicion por equipo                #
# =========================================================================== #
def _laliga_team_ids(session, first_season: int = FIRST_SEASON) -> dict:
    """Descubre {team_id: nombre} de todos los equipos que han jugado LaLiga
    desde first_season, a partir de los eventos de temporada de LaLiga."""
    teams: dict = {}
    for season in sofa._fetch_seasons(session):
        if sofa._season_year(season) < first_season:
            continue
        for e in sofa._fetch_season_events(session, season.get("id")):
            for side in ("homeTeam", "awayTeam"):
                t = e.get(side) or {}
                if t.get("id") is not None:
                    teams[t["id"]] = t.get("name")
    if not teams:
        raise RuntimeError(
            "[fatigue] descubrimiento de equipos VACIO: no se obtuvo ningun "
            "equipo de LaLiga desde Sofascore (transporte caido o cambio de "
            "API). No se cosecha nada; re-ejecuta mas tarde."
        )
    return teams


def _is_competitive(competition, status) -> bool:
    """True si el partido representa carga real (ni amistoso ni no disputado)."""
    comp = "" if competition is None else str(competition)
    st = "" if status is None else str(status).strip().lower()
    return (not _FRIENDLY_RE.search(comp)) and st not in _NOT_PLAYED


def _team_event_row(e: dict, team_id) -> dict:
    ht, at = e.get("homeTeam") or {}, e.get("awayTeam") or {}
    is_home = ht.get("id") == team_id
    me, opp = (ht, at) if is_home else (at, ht)
    ut = (e.get("tournament") or {}).get("uniqueTournament") or {}
    comp = ut.get("name") or (e.get("tournament") or {}).get("name") or ""
    ts = e.get("startTimestamp")
    date = _fecha_local(ts)
    friendly = bool(_FRIENDLY_RE.search(comp))
    status = (e.get("status") or {}).get("type")
    return {
        "row_id": f"{e.get('id')}|{team_id}",
        "event_id": e.get("id"),
        "team_id": team_id,
        "team": me.get("name"),
        "team_key": tn.to_key(me.get("name")),
        "opponent": opp.get("name"),
        "opp_key": tn.to_key(opp.get("name")),
        "is_home": is_home,
        "date": date,
        "start_ts": ts,
        "competition": comp,
        "comp_id": ut.get("id"),
        "is_friendly": friendly,
        "is_competitive": _is_competitive(comp, status),
        "status": status,
    }


def _fetch_team_fixtures(session, team_id, *, since_ts=_SINCE_TS,
                         max_last_pages=50, next_pages=2) -> list[dict]:
    """Todos los partidos de un equipo (pasados hasta since_ts + proximos).

    Igual que en la paginacion de temporada de Sofascore, se distingue el fin de
    datos (pagina vacia o `hasNextPage=False`) del fallo de transporte (`ok=False`).
    Aqui la confusion es especialmente daninna porque el calendario alimenta
    `rest_days` y la congestion: un tramo de calendario que se pierde por un 403 no
    deja hueco visible, sino un descanso ARTIFICIALMENTE LARGO -- un valor
    plausible y silencioso que el modelo se cree. Por eso el corte por transporte
    se propaga como HarvestError y `_crawl` lo contabiliza como equipo fallido en
    lugar de aceptar un calendario a medias.
    """
    rows, seen = [], set()

    def _push(events):
        oldest = None
        for e in events or []:
            eid = e.get("id")
            if eid in seen:
                continue
            seen.add(eid)
            rows.append(_team_event_row(e, team_id))
            ts = e.get("startTimestamp")
            if ts and (oldest is None or ts < oldest):
                oldest = ts
        return oldest

    def _fallo(tramo, page, res):
        if str(res.get("err")) == "404":
            return True                  # 404 = la pagina no existe -> fin limpio
        raise _common.HarvestError(
            f"[fatigue] equipo {team_id}: fallo de transporte en {tramo}/{page} "
            f"(err={res.get('err')!r}) con {len(rows)} partido(s) acumulados. "
            "No es fin de calendario; el equipo queda pendiente para la "
            "siguiente pasada."
        )

    for page in range(max_last_pages):
        res = sofa._fetch_json(session, f"/api/v1/team/{team_id}/events/last/{page}")
        if not res.get("ok") and _fallo("last", page, res):
            break
        data = res["data"] if isinstance(res.get("data"), dict) else {}
        events = data.get("events")
        if not events:
            break
        oldest = _push(events)
        if oldest is not None and oldest < since_ts:
            break
        if data.get("hasNextPage") is False:
            break
    for page in range(next_pages):
        res = sofa._fetch_json(session, f"/api/v1/team/{team_id}/events/next/{page}")
        if not res.get("ok") and _fallo("next", page, res):
            # 404 es normal aqui (equipo sin partidos futuros); un 403 o un
            # timeout no lo son y _fallo ya los ha propagado.
            break
        data = res["data"] if isinstance(res.get("data"), dict) else {}
        events = data.get("events")
        if not events:
            break
        _push(events)
        if data.get("hasNextPage") is False:
            break
    return rows


# =========================================================================== #
#  B) FEATURES (funciones puras, testeables sin red)                           #
# =========================================================================== #
def _parones_de_liga(ts_all: np.ndarray) -> list[tuple[float, float]]:
    """Intervalos en los que la competicion de clubes se detiene POR COMPLETO.

    Un hueco largo en el calendario de UN equipo no identifica una ventana FIFA:
    una lesion no existe a nivel de calendario, pero un aplazamiento, una
    eliminacion europea temprana o una jornada sin partido para ese club producen
    exactamente el mismo hueco. Lo que distingue al paron de selecciones es que
    detiene a TODOS los equipos a la vez. Por eso el criterio se corrobora contra
    el calendario agregado: solo cuentan los tramos de al menos `_INTL_GAP_DAYS`
    dias en los que no se disputa NINGUN partido en todo el conjunto de datos.

    Devuelve la lista de intervalos `(inicio, fin)` en segundos epoch.
    """
    if ts_all.size == 0:
        return []
    orden = np.sort(ts_all)
    huecos = np.diff(orden)
    umbral = _INTL_GAP_DAYS * 86400
    return [(float(orden[i]), float(orden[i + 1]))
            for i in np.flatnonzero(huecos >= umbral)]


def _tras_paron(t: float, prev: float, parones: list[tuple[float, float]]) -> bool:
    """`True` si el hueco previo del equipo coincide con un paron de la competicion."""
    return any(prev <= ini and fin <= t for ini, fin in parones)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Deriva features de fatiga sobre las filas COMPETITIVAS (amistosos fuera).

    Por cada equipo-partido competitivo:
      rest_days           dias desde el partido competitivo anterior
      games_last_7/14     partidos competitivos en la ventana previa
      is_midweek          saque entre semana (mar/mie/jue)
      euro_trip_recent    jugo fuera en Europa en los ultimos dias
      is_post_intl_break  vuelta de paron de selecciones (ventana FIFA): hueco
                          largo del equipo en un mes de paron QUE ADEMAS coincide
                          con una detencion de toda la competicion
    """
    if df.empty:
        return df.copy()
    work = df.copy()
    work["start_ts"] = pd.to_numeric(work["start_ts"], errors="coerce")
    work["competition"] = work["competition"].fillna("").astype(str)
    # Las claves se DERIVAN aqui desde el nombre (el raw solo guarda nombres):
    # asi cualquier mejora en teamnames se aplica al reprocesar, sin re-scrapear.
    if "team" in work.columns:
        work["team_key"] = work["team"].map(tn.to_key)
    if "opponent" in work.columns:
        work["opp_key"] = work["opponent"].map(tn.to_key)
    work["is_home"] = work["is_home"].astype(str).str.strip().str.lower().isin(("true", "1"))
    # `is_competitive` se RE-DERIVA aqui desde `competition` + `status`, igual que
    # las claves: asi un cambio en la politica de exclusion (que estados cuentan
    # como carga) se aplica al reprocesar, sin volver a scrapear el calendario. Si
    # el raw es antiguo y no trae `status`, se respeta el valor almacenado.
    if "status" in work.columns:
        st = work["status"].astype(str).str.strip().str.lower()
        work["is_competitive"] = (~work["competition"].str.contains(_FRIENDLY_RE)) \
            & (~st.isin(_NOT_PLAYED))
    else:
        work["is_competitive"] = (work["is_competitive"].astype(str).str.strip()
                                 .str.lower().isin(("true", "1")))
    comp = work[work["is_competitive"] & work["start_ts"].notna()].copy()
    comp = comp.sort_values(["team_id", "start_ts"]).reset_index(drop=True)

    feats = {k: [] for k in ("rest_days", "games_last_7", "games_last_14",
                             "is_midweek", "euro_trip_recent", "is_post_intl_break")}
    D7, D14, DE = 7 * 86400, 14 * 86400, _EURO_TRIP_DAYS * 86400
    # calendario agregado: sirve para corroborar que un hueco largo es un paron de
    # la competicion y no una incidencia de un solo equipo (ver `_parones_de_liga`)
    parones = _parones_de_liga(comp["start_ts"].to_numpy(dtype=float))
    for _, g in comp.groupby("team_id", sort=False):
        ts = g["start_ts"].to_numpy()
        euro_away = (g["competition"].str.contains(_EURO_RE) & (~g["is_home"])).to_numpy()
        for i in range(len(ts)):
            t = ts[i]
            prev = ts[i - 1] if i > 0 else np.nan
            rest = (t - prev) / 86400 if i > 0 else np.nan
            past = ts[:i]
            g7 = int(((past >= t - D7) & (past < t)).sum())
            g14 = int(((past >= t - D14) & (past < t)).sum())
            # dia de la semana y mes en hora local (ver `_TZ_LOCAL`): en UTC un
            # saque nocturno se contabilizaria en la jornada anterior
            _loc = _local_dt(t)
            wd, month = _loc.weekday(), _loc.month
            trip = bool(((past >= t - DE) & (past < t) & euro_away[:i]).any())
            post_intl = bool((not np.isnan(rest)) and rest >= _INTL_GAP_DAYS
                             and month in _INTL_MONTHS
                             and _tras_paron(t, prev, parones))
            feats["rest_days"].append(rest)
            feats["games_last_7"].append(g7)
            feats["games_last_14"].append(g14)
            feats["is_midweek"].append(wd in _MIDWEEK_WD)
            feats["euro_trip_recent"].append(trip)
            feats["is_post_intl_break"].append(post_intl)
    for k, v in feats.items():
        comp[k] = v
    return comp


def to_match_level(feat_df: pd.DataFrame, tournament_id: int | None = None) -> pd.DataFrame:
    """Pivota a nivel PARTIDO de LaLiga: una fila por event_id con las features
    de local y visitante + diferencias (rest_diff, congestion_diff)."""
    tid = tournament_id if tournament_id is not None else sofa.LALIGA_TOURNAMENT_ID
    lg = feat_df[feat_df["comp_id"] == tid].copy()
    cols = ["rest_days", "games_last_7", "games_last_14", "is_midweek",
            "euro_trip_recent", "is_post_intl_break"]
    rows = []
    for eid, g in lg.groupby("event_id", sort=False):
        h = g[g["is_home"]]
        a = g[~g["is_home"]]
        if h.empty or a.empty:
            continue
        h, a = h.iloc[0], a.iloc[0]
        row = {"event_id": eid, "date": h["date"],
               "home_key": h["team_key"], "away_key": a["team_key"],
               "match_key": tn.match_key(pd.Series([h["date"]]),
                                         pd.Series([h["team"]]),
                                         pd.Series([a["team"]]))[0]}
        for c in cols:
            row[f"home_{c}"] = h[c]
            row[f"away_{c}"] = a[c]
        row["rest_diff"] = h["rest_days"] - a["rest_days"]
        row["congestion_diff"] = h["games_last_14"] - a["games_last_14"]
        rows.append(row)
    return pd.DataFrame(rows)


# =========================================================================== #
#  C) GUARDADO ENDURECIDO (reutiliza el writer atomico de sofascore.py)        #
# =========================================================================== #
def _load_master(master_csv: Path):
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        ids = set(df[_KEY_COL].astype(str)) if _KEY_COL in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} filas.")
        return df, ids
    print("[INFO] sin datos previos.")
    return pd.DataFrame(), set()


def _finalize_master(df_master: pd.DataFrame, new_rows: list, master_csv: Path):
    if not new_rows:
        print("[INFO] no hay filas nuevas.")
        return df_master
    df_new = pd.DataFrame(new_rows)
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty],
                          ignore_index=True)
    if _KEY_COL in df_master.columns:
        df_master = df_master.drop_duplicates(_KEY_COL, keep="last")
    if "start_ts" in df_master.columns:
        df_master = df_master.sort_values("start_ts").reset_index(drop=True)
    if sofa._write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] -> {master_csv} ({len(df_master)} filas)")
    return df_master


# =========================================================================== #
#  D) ORQUESTACION: backfill / update / process / schedule                     #
# =========================================================================== #
def _crawl(session, team_ids: dict, known: set, *, since_ts=_SINCE_TS,
           flush=None, flush_every: int = 5,
           max_failed_frac: float = _MAX_FAILED_FRAC) -> list:
    """Cosecha calendarios de todos los equipos.

    `known`: row_ids que NO hay que volver a traer (en update = solo partidos
    FINALIZADOS; ver update()). Un partido cuyo row_id no esta en `known` se
    re-baja y su fila nueva pisa a la vieja (keep='last' en _finalize_master),
    de modo que un aplazado cambia de fecha/estado correctamente.

    `flush(rows)`: callback opcional que persiste lo acumulado cada `flush_every`
    equipos (para un backfill de ~40 equipos x ~50 paginas que no se pierda si
    se corta a la mitad). Debe devolver el nuevo df_master; las filas volcadas
    se vacian de la lista.

    Contabilidad de fallos: `_fetch_team_fixtures` lanza `HarvestError` cuando una
    pagina cae por transporte, de modo que un calendario truncado se registra como
    equipo FALLIDO y no como equipo sin novedades. Si la proporcion de fallidos
    supera `max_failed_frac` se aborta con el detalle de que equipos han caido: por
    debajo de ese umbral se avisa y se continua, porque un 403 aislado no justifica
    tirar una cosecha de 40 equipos, pero una caida generalizada si. Lo persistido
    en lotes previos queda a salvo en ambos casos.
    """
    new_rows: list = []
    n = len(team_ids)
    fallidos: list[tuple[str, str]] = []
    for i, (tid, name) in enumerate(team_ids.items(), 1):
        print(f"  [{i}/{n}] equipo {name} (id={tid})")
        try:
            for row in _fetch_team_fixtures(session, tid, since_ts=since_ts):
                if row["row_id"] not in known:
                    new_rows.append(row)
                    known.add(row["row_id"])
        except Exception as e:  # noqa: BLE001
            fallidos.append((str(name), str(e)))
            print(f"     [WARN] {name}: {e}")
        if flush is not None and i % flush_every == 0 and new_rows:
            flush(new_rows)          # persiste el lote
            new_rows = []            # y lo vacia (ya esta en el master)
        time.sleep(0.2)
    if fallidos and n:
        detalle = "; ".join(f"{nm}: {err[:120]}" for nm, err in fallidos[:8])
        resumen = (f"[fatigue] {len(fallidos)}/{n} equipos con calendario "
                   f"incompleto -> {detalle}"
                   + (" ..." if len(fallidos) > 8 else ""))
        if len(fallidos) / n > max_failed_frac:
            if flush is not None and new_rows:
                flush(new_rows)      # no se pierde lo cosechado antes del corte
            raise _common.HarvestError(
                resumen + f". Supera el umbral admisible ({max_failed_frac:.0%}): "
                "el transporte a Sofascore esta degradado y la cosecha seria "
                "parcial. Re-ejecuta el mismo comando para reanudar."
            )
        print("[WARN] " + resumen + ". Por debajo del umbral: se continua, pero "
              "esos equipos quedan con calendario viejo hasta la proxima pasada.")
    return new_rows


def backfill(master_csv: Path = _MASTER_CSV, *, first_season: int = FIRST_SEASON):
    Path(master_csv).parent.mkdir(parents=True, exist_ok=True)
    session = sofa._new_session()
    try:
        df_master, known = _load_master(master_csv)
        print("[INFO] descubriendo equipos de LaLiga...")
        team_ids = _laliga_team_ids(session, first_season)
        print(f"[INFO] {len(team_ids)} equipos. Cosechando calendarios...")

        # el backfill baja ~40 equipos x muchas paginas: se persiste por lotes
        # para que una interrupcion no obligue a empezar de cero. `state` mantiene
        # el df_master mutable entre flushes.
        state = {"df": df_master}

        def _flush(rows):
            state["df"] = _finalize_master(state["df"], rows, master_csv)
            return state["df"]

        tail = _crawl(session, team_ids, known,
                      since_ts=int(datetime(first_season, 7, 1,
                                            tzinfo=timezone.utc).timestamp()),
                      flush=_flush)
        df_master = _finalize_master(state["df"], tail, master_csv)
    finally:
        session.close()
    process(master_csv)
    return df_master


def update(master_csv: Path = _MASTER_CSV, *, recent_days: int = 45):
    """Refresca solo la ventana reciente + proximos (para el runner semanal).

    C3: `known` se restringe a los partidos ya FINALIZADOS. Un partido guardado
    como proximo o aplazado (status != finished) NO se marca conocido, de modo
    que se re-baja y su fila nueva (con la fecha/estado correctos) pisa a la
    vieja via keep='last'. Antes, `known` = todo el master, y un aplazamiento
    dejaba start_ts/status congelados para siempre -> rest_days corruptos.
    """
    session = sofa._new_session()
    try:
        df_master, _ = _load_master(master_csv)
        # solo los FINALIZADOS son inmutables; el resto se vuelve a consultar
        if not df_master.empty and {"row_id", "status"}.issubset(df_master.columns):
            known = set(df_master.loc[df_master["status"] == "finished",
                                      "row_id"].astype(str))
        else:
            known = set()
        team_ids = _laliga_team_ids(session, _current_season_year())
        since = int((datetime.now(timezone.utc).timestamp()) - recent_days * 86400)
        new_rows = _crawl(session, team_ids, known, since_ts=since)
        df_master = _finalize_master(df_master, new_rows, master_csv)
    finally:
        session.close()
    process(master_csv)
    return df_master


def _current_season_year(now=None) -> int:
    """Ano de inicio de la temporada vigente (logica compartida en _common)."""
    return _common.current_season_year(now)


def process(master_csv: Path = _MASTER_CSV, *, out_parquet: Path = _OUT_PARQUET,
            verbose: bool = True, strict: bool = True) -> pd.DataFrame:
    """Calcula features de fatiga y escribe el parquet.

    `strict=True`: valida que los EQUIPOS de LaLiga (columna `team`) tengan clave
    canonica. NO valida `opponent`: por diseno incluye rivales de Champions/Europa
    League y amistosos que no son de LaLiga y no tienen (ni deben tener) clave
    canonica; validarlos daria falsos positivos constantes.
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        raise _common.RegressionError(
            f"[fatigue.process] no existe el maestro '{master_csv}'. Publicar un "
            "parquet vacio destruiria el vigente (las aserciones de esquema no "
            "detectan un frame sin filas). Ejecuta antes el backfill.")
    df = pd.read_csv(master_csv)
    feat = compute_features(df)
    # ventana del proyecto: temporada FIRST_SEASON/17 en adelante. compute_features
    # usa el historico completo para el look-back (rest_days), pero solo se publica
    # el tramo dentro de la ventana.
    if "date" in feat.columns and not feat.empty:
        _cut = f"{FIRST_SEASON}-07-01"
        feat = feat[pd.to_datetime(feat["date"], errors="coerce") >= _cut].reset_index(drop=True)
    if strict and not feat.empty and "team" in feat.columns:
        m = ~feat["team_key"].isin(tn.CANONICAL) if "team_key" in feat.columns \
            else pd.Series(True, index=feat.index)
        bad = set(feat.loc[m, "team"].dropna().unique())
        if bad:
            tn.assert_known(bad, source="fatigue.process(team)")
    if verbose:
        _report(feat)
    _common.publish_df(feat, out_parquet, source="fatigue.process",
                       fmt="parquet", verbose=verbose)
    return feat


def _report(df: pd.DataFrame) -> None:
    print("\n===== INFORME FATIGA =====")
    print(f"filas competitivas: {len(df)}")
    if "status" in df:
        st = df["status"].astype(str).str.strip().str.lower().value_counts()
        print("estados (tras excluir no disputados y amistosos):")
        for k, v in st.head(6).items():
            print(f"   {v:5d}  {k}")
        colados = [k for k in st.index if k in _NOT_PLAYED]
        if colados:
            print(f"   [WARN] estados no disputados presentes: {colados}")
    if "competition" in df:
        print("competiciones     :")
        for k, v in df["competition"].value_counts().head(10).items():
            print(f"   {v:5d}  {k}")
    for c in ("rest_days", "games_last_14"):
        if c in df:
            print(f"{c:14s}: media={df[c].mean():.2f}  no-nulo={df[c].notna().mean()*100:.0f}%")
    for c in ("is_midweek", "euro_trip_recent", "is_post_intl_break"):
        if c in df:
            print(f"{c:18s}: {df[c].mean()*100:.1f}% True")
    names = df.get("team", pd.Series(dtype=object)).dropna().unique()
    print(f"equipos NO reconocidos: {tn.unknown_names(names) or '[] (ok)'}")
    print("==========================\n")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "process"
    if cmd == "backfill":
        backfill()
    elif cmd in ("update", "weekly", "schedule"):
        # `update` ya publica el parquet al terminar, de modo que es en si misma
        # la rutina semanal: `weekly` es su nombre en el manual de operacion y los
        # extractores responden todos al mismo verbo.
        update()
    elif cmd == "process":
        process()
    else:
        print("uso: python fatigue.py [backfill|update|weekly|process|schedule]")
