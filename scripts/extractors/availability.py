"""Extractor de DISPONIBILIDAD / BAJAS (Sofascore, missingPlayers por partido).

Saca de '/event/{id}/lineups' la lista de jugadores que se pierden el partido,
con su motivo (lesion / sancion / duda) y la fecha estimada de vuelta. Se agrega
a nivel equipo-partido: numero de bajas y desglose por tipo. Aguas abajo,
`consolidate/absences.py` cruza esa lista con el valor de mercado de Transfermarkt
para ponderar el "peso" de las bajas (no es lo mismo perder a un suplente que al
'9' titular).

Patron estandar:
    backfill  -> todos los partidos de LaLiga conocidos (jugados + calendario).
    update    -> los partidos recientes / proximos (para el runner semanal).
    process   -> agrega a equipo-partido y escribe el parquet.

Reutiliza el TRANSPORTE de sofascore.py y dos indices de ids complementarios.

UNIVERSO DE PARTIDOS A CONSULTAR
--------------------------------
El master de estadisticas de Sofascore (`sofascore_match_stats.csv`) solo contiene
partidos YA JUGADOS: si fuera la unica fuente de ids, las bajas de la jornada que
se quiere predecir nunca se descargarian y el bloque de disponibilidad entraria
vacio precisamente en inferencia. Por eso el universo es la union de:

  * master de partidos jugados  -> historico (backfill).
  * calendario crudo `sofascore_team_fixtures.csv` -> incluye los fixtures con
      estado `notstarted`, es decir los partidos que se van a puntuar. Se limita a
      LaLiga y a un horizonte de `_HORIZON_DAYS` dias, coherente con la ventana de
      `features/build_scoring.py`: todo partido que el scoring pueda puntuar ha
      sido consultado antes por este extractor.

SEMANTICA DE "SIN BAJAS" vs "SIN DATO"
--------------------------------------
Son dos estados distintos y el modelo los trata distinto (un 0 real es senal; un
"no lo sabemos" debe ir por la via del flag de ausencia de dato). La tabla
agregada lo hace explicito:

  * fila presente con recuentos 0 y `queried=True` -> se consulto y no hay bajas.
  * partido AUSENTE de la tabla                    -> nunca se consulto (o su
      resultado aun no es definitivo); al cruzar en la consolidacion queda NaN y
      `preprocess` lo marca con `flag_no_absences`.

Ademas se emiten SIEMPRE las dos filas (`home` y `away`) de cada partido
consultado: un partido con bajas solo en el local tiene tambien un dato de
visitante, y ese dato es cero.

Hay un tercer estado que el par anterior no cubre y que no se puede decidir
partido a partido: la TEMPORADA SIN FEED. Un encuentro consultado que no devuelve
ninguna baja se publica como cero, y eso es correcto cuando la fuente tiene el
dato y ese dia no habia ausentes. Pero si en una temporada entera no hay ni un
registro en los 380 partidos de la liga, lo que falta no son las bajas: es el
feed. Publicarla como ceros afirmaria que no se lesiono nadie en todo un ano
--una magnitud que el modelo usaria como senal-- en lugar de reconocer que no se
sabe. Esas temporadas se dejan fuera de la tabla, igual que un partido no
consultado, y la consolidacion las marca con `flag_no_absences`. El criterio lo
decide el propio dato y no una fecha escrita a mano: una temporada con el feed
vivo tiene registros en casi todos los partidos, y sus huecos sueltos si son
ceros de verdad.

CLASIFICACION robusta a codigos desconocidos: se usa primero el TEXTO
(description / externalType), que en Sofascore trae "Suspension", "Injury", etc.
Se conservan los codigos numericos type/reason por si hay que afinar el mapa tras
ver el vocabulario real (el informe de cobertura los resume).
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import pandas as pd

try:
    from . import _common            # publicacion no regresiva compartida
    from . import sofascore as sofa   # transporte + guardado atomico compartidos
    from . import teamnames as tn
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import _common  # type: ignore
    import sofascore as sofa  # type: ignore
    import teamnames as tn  # type: ignore


_KEY_COL = "row_id"
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "sofascore"
_MASTER_CSV = _RAW_DIR / "sofascore_missing_players.csv"
_FIXTURES_CSV = _RAW_DIR / "sofascore_team_fixtures.csv"
_OUT_PARQUET = _REPO / "data" / "02_processed" / "team_availability.parquet"

# Horizonte futuro (dias) del calendario que se consulta. Se alinea con
# `_HORIZON_DAYS` de features/build_scoring.py para garantizar la invariante
# "todo partido puntuable ha sido consultado".
_HORIZON_DAYS = 10
# Un partido se considera JUGADO (y por tanto su respuesta es definitiva y
# sellable) pasadas estas horas desde el pitido inicial. El margen evita sellar
# como "sin bajas" un encuentro que acaba de empezar y cuyas alineaciones aun
# podrian no estar publicadas.
_PLAYED_BUFFER_S = 3 * 3600

_SIDES = ("home", "away")
_STATS = ["n_missing", "n_out", "n_injury", "n_suspension", "n_doubtful"]

_SUSP_RE = re.compile(r"susp|ban|red card|sanc|expul|doping|accumulation", re.I)
_INJ_RE = re.compile(r"injur|knock|fitness|muscle|strain|surgery|lesion|hamstring|knee|ankle", re.I)


def _classify(item: dict) -> tuple[str, bool]:
    """(categoria, is_out) a partir de los campos ESTRUCTURADOS de Sofascore.

    Aprendido del backfill real (2026-07-10): `description` es NaN en ~89% de las
    filas, asi que clasificar por texto mandaba casi todo a "other". Los campos
    fiables son estructurados:
      * `type`   -> is_out:  'missing' = baja segura, 'doubtful' = duda.
      * `reason` -> codigo numerico (13 = sancion; 1 = lesion, mayoria).
    El sub-tipo es secundario (lo predictivo es el RECUENTO out/duda + valor del
    jugador), asi que basta con: duda -> doubtful; sancion (reason 13 o texto) ->
    suspension; el resto de bajas seguras -> injury (la gran mayoria lo son).
    """
    typ = str(item.get("type") or "").strip().lower()
    is_out = typ == "missing"
    text = " ".join(str(item.get(k) or "") for k in ("description", "externalType")).lower()
    try:
        reason = int(float(item.get("reason")))
    except (TypeError, ValueError):
        reason = None
    if typ == "doubtful":
        cat = "doubtful"
    elif reason == 13 or _SUSP_RE.search(text):
        cat = "suspension"
    elif _INJ_RE.search(text) or typ == "missing":
        cat = "injury"
    else:
        cat = "unknown"
    return cat, is_out


def _missing_rows(event_id, lineups: dict) -> list[dict]:
    rows = []
    if not isinstance(lineups, dict):
        return rows
    for side in ("home", "away"):
        for it in (lineups.get(side) or {}).get("missingPlayers") or []:
            p = it.get("player") or {}
            cat, is_out = _classify(it)
            slug = p.get("slug") or tn.to_key(p.get("name"))
            rows.append({
                "row_id": f"{event_id}|{side}|{slug}",
                "event_id": event_id,
                "side": side,
                "player": p.get("name"),
                "player_slug": slug,
                "position": p.get("position"),
                "category": cat,
                "is_out": is_out,
                "type": it.get("type"),
                "reason": it.get("reason"),
                "description": it.get("description"),
                "external_type": it.get("externalType"),
                "expected_end_date": it.get("expectedEndDate"),
                "confirmed": lineups.get("confirmed"),
            })
    return rows


def scrape_missing(session, event_id) -> list[dict] | None:
    """Bajas de un partido.

    Devuelve:
      * list (posiblemente vacia) -> consulta correcta: [] significa "sin bajas".
      * None -> la peticion FALLO (403/red). El llamante debe distinguir esto de
        "sin bajas": antes ambos casos devolvian [] y un fallo puntual se
        reintentaba eternamente (nunca se marcaba el partido como consultado).
    """
    res = sofa._fetch_json(session, f"/api/v1/event/{event_id}/lineups")
    if not res.get("ok"):
        if res.get("err") == "404":       # 404 = no hay lineups publicadas: sin bajas
            return []
        return None                        # fallo transitorio -> reintentar luego
    return _missing_rows(event_id, res.get("data"))


# --------------------------------------------------------------------------- #
#  Agregacion a equipo-partido + nivel partido                                #
# --------------------------------------------------------------------------- #
def _as_int_ids(ids) -> set:
    """Conjunto de event_id como int, descartando lo que no sea un id valido."""
    out: set = set()
    for x in ids or ():
        try:
            out.add(int(float(x)))
        except (TypeError, ValueError):
            continue
    return out


def aggregate(df: pd.DataFrame, queried_ids=None) -> pd.DataFrame:
    """Una fila por (event_id, side) para CADA partido consultado.

    `queried_ids` son los partidos con respuesta DEFINITIVA que no dejaron filas
    en el raw (consultados y sin bajas: el sentinela `_checked_*`). Se anaden al
    universo con recuentos 0 para que "cero bajas" no sea indistinguible de "no
    consultado" aguas abajo; ver la nota de semantica en el docstring del modulo.

    Se emiten siempre los dos lados de cada partido del universo: un encuentro con
    bajas solo en el local tiene tambien un dato de visitante, y ese dato es 0.
    """
    cols = ["event_id", "side", "queried"] + _STATS
    universe = _as_int_ids(queried_ids)
    # esqueleto vacio con los dtypes definitivos: si no hubiera ninguna baja en el
    # raw, el merge posterior seguiria cuadrando (int64 contra int64).
    counts = pd.DataFrame({"event_id": pd.Series(dtype="int64"),
                           "side": pd.Series(dtype=object),
                           **{c: pd.Series(dtype="int64") for c in _STATS}})
    if df is not None and not df.empty:
        d = df.copy()
        d["event_id"] = pd.to_numeric(d["event_id"], errors="coerce")
        d = d.dropna(subset=["event_id"])
        d["event_id"] = d["event_id"].astype("int64")
        d["is_out"] = d["is_out"].astype(str).str.lower().isin(("true", "1"))
        g = d.groupby(["event_id", "side"])
        # `n_missing` se cuenta con size (numero de filas del grupo) y no con el
        # count de una columna: un nombre de jugador ausente no debe descontar una
        # baja que si esta registrada.
        counts = g.size().rename("n_missing").reset_index().merge(
            g.agg(
                n_out=("is_out", "sum"),
                n_injury=("category", lambda s: (s == "injury").sum()),
                n_suspension=("category", lambda s: (s == "suspension").sum()),
                n_doubtful=("category", lambda s: (s == "doubtful").sum()),
            ).reset_index(), on=["event_id", "side"], how="outer")
        universe |= set(counts["event_id"].tolist())
    if not universe:
        return pd.DataFrame(columns=cols)

    full = pd.MultiIndex.from_product([sorted(universe), _SIDES],
                                      names=["event_id", "side"]).to_frame(index=False)
    out = full.merge(counts, on=["event_id", "side"], how="left")
    for c in _STATS:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype("int64")
    # todas las filas de la tabla corresponden a partidos consultados; la columna
    # deja el contrato explicito y permite aseverarlo en la consolidacion.
    out["queried"] = True
    return out.loc[:, cols].reset_index(drop=True)


def temporadas_sin_feed(df_raw: pd.DataFrame,
                        stats_csv: Path = sofa._MATCH_STATS_CSV) -> dict:
    """Temporadas en las que NINGUN partido de la liga tiene una sola baja.

    Devuelve {temporada: partidos_de_la_temporada}. Una temporada asi no es una
    temporada sin lesionados --eso no existe-- sino una temporada anterior al
    feed de bajas de la fuente, y sus partidos no pueden publicarse como ceros.

    No se recorta por fecha ni se exige que la temporada este terminada: si la
    que se esta jugando apareciese aqui, lo que hay es un fallo de extraccion, y
    dejarla fuera de la tabla es precisamente lo que hace que la cobertura de la
    cola del maestro caiga a cero y `qa_master` detenga la cadena. Publicarla
    como ceros seria lo que lo ocultaria.
    """
    try:
        meta = pd.read_csv(stats_csv, usecols=["match_id", "season_name"])
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] sin temporadas en '{Path(stats_csv).name}' ({e}); no se "
              "puede distinguir la temporada sin feed de la temporada sin bajas.")
        return {}
    meta = meta.dropna(subset=["match_id", "season_name"])
    meta["match_id"] = pd.to_numeric(meta["match_id"], errors="coerce")
    meta = meta.dropna(subset=["match_id"]).astype({"match_id": "int64"})
    con_baja: set = set()
    if df_raw is not None and not df_raw.empty and "event_id" in df_raw.columns:
        ids = pd.to_numeric(df_raw["event_id"], errors="coerce").dropna()
        con_baja = set(ids.astype("int64").tolist())
    meta["con_baja"] = meta["match_id"].isin(con_baja)
    g = meta.groupby("season_name")["con_baja"].agg(["size", "sum"])
    return {str(s): int(r["size"]) for s, r in g.iterrows() if r["sum"] == 0}


def _sin_temporadas_sin_feed(df_raw: pd.DataFrame, queried_ids,
                             *, stats_csv: Path = sofa._MATCH_STATS_CSV,
                             verbose: bool = True) -> set:
    """Quita del universo los partidos de las temporadas sin feed de bajas."""
    sin_feed = temporadas_sin_feed(df_raw, stats_csv)
    ids = _as_int_ids(queried_ids)
    if not sin_feed:
        return ids
    try:
        meta = pd.read_csv(stats_csv, usecols=["match_id", "season_name"])
    except Exception:  # noqa: BLE001
        return ids
    fuera = pd.to_numeric(
        meta.loc[meta["season_name"].astype(str).isin(sin_feed), "match_id"],
        errors="coerce").dropna().astype("int64")
    quitados = ids & set(fuera.tolist())
    if verbose and quitados:
        detalle = ", ".join(f"{s} ({n} partidos)" for s, n in sorted(sin_feed.items()))
        print(f"[INFO] temporada(s) SIN feed de bajas -> fuera de la tabla, "
              f"para que lleguen como 'sin dato' y no como cero: {detalle}.")
    return ids - quitados


def to_match_level(agg_df: pd.DataFrame, master_csv: Path = sofa._MATCH_STATS_CSV) -> pd.DataFrame:
    """Pivota a partido: home_* / away_* + diffs, con match_key desde el master."""
    if agg_df.empty:
        return pd.DataFrame()
    keep = ["event_id", "side"] + [c for c in _STATS if c in agg_df.columns]
    piv = agg_df.loc[:, keep].pivot(index="event_id", columns="side")
    piv.columns = [f"{side}_{stat}" for stat, side in piv.columns]
    piv = piv.reset_index().fillna(0)
    try:
        meta = pd.read_csv(master_csv, usecols=["match_id", "home", "away", "match_date"])
        meta = meta.rename(columns={"match_id": "event_id"})
        piv = piv.merge(meta, on="event_id", how="left")
        piv["home_key"] = piv["home"].map(tn.to_key)
        piv["away_key"] = piv["away"].map(tn.to_key)
        piv["match_key"] = tn.match_key(piv["match_date"], piv["home"], piv["away"])
        n_bad = int(piv["match_key"].isna().sum())
        if n_bad:
            # un match_key nulo NO cruza en la consolidacion: esas bajas se
            # perderian en silencio. Se avisa con ejemplos para poder corregir.
            ej = piv.loc[piv["match_key"].isna(), "event_id"].head(5).tolist()
            print(f"[WARN] {n_bad} partido(s) con match_key NULO (no cruzaran "
                  f"con las demas fuentes); event_id de ejemplo: {ej}. "
                  "Suele indicar que el partido no esta en el master de "
                  "sofascore o que su match_date es invalida.")
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] sin meta del master ({e}); solo recuentos por event_id.")
    for stat in ("n_missing", "n_out", "n_injury", "n_suspension"):
        h, a = f"home_{stat}", f"away_{stat}"
        if h in piv.columns and a in piv.columns:
            piv[f"{stat}_diff"] = piv[h] - piv[a]
    return piv


# --------------------------------------------------------------------------- #
#  Guardado + orquestacion                                                     #
# --------------------------------------------------------------------------- #
def _load_master(master_csv: Path):
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        done = set(df["event_id"].astype(str)) if "event_id" in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} filas, {len(done)} partidos.")
        return df, done
    print("[INFO] sin datos previos.")
    return pd.DataFrame(), set()


def _finalize_master(df_master, new_rows, master_csv: Path, refrescados=None):
    """Anade las filas nuevas y PISA por completo las de los partidos refrescados.

    Un partido por jugar se vuelve a consultar cada semana, y su parte de bajas
    cambia en los dos sentidos: aparecen lesionados y se recuperan. Si solo se
    anadiese, el jugador que ya esta disponible seguiria contando como baja para
    siempre, porque su fila vieja no la pisa nadie. De ahi que el refresco borre
    el partido entero antes de escribir su foto nueva.
    """
    refrescados = {str(e) for e in (refrescados or ())}
    if not new_rows and not refrescados:
        print("[INFO] no hay filas nuevas.")
        return df_master
    df_new = pd.DataFrame(new_rows)
    if refrescados and not df_master.empty and "event_id" in df_master.columns:
        df_master = df_master[~df_master["event_id"].astype(str).isin(refrescados)]
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty], ignore_index=True)
    if _KEY_COL in df_master.columns:
        df_master = df_master.drop_duplicates(_KEY_COL, keep="last")
    if sofa._write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] -> {master_csv} ({len(df_master)} filas)")
    return df_master


def _fixture_ids(fixtures_csv: Path = _FIXTURES_CSV, *,
                 horizon_days: int = _HORIZON_DAYS) -> list:
    """event_ids de LaLiga tomados del CALENDARIO (incluye partidos por jugar).

    Es la mitad que el master de estadisticas no puede dar: los fixtures con
    estado `notstarted`, que son exactamente los que se van a predecir. El
    calendario tiene una fila por equipo-partido, de ahi el de-duplicado.
    """
    try:
        fx = pd.read_csv(fixtures_csv, usecols=["event_id", "start_ts", "competition"])
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] calendario '{Path(fixtures_csv).name}' no disponible ({e}); "
              "solo se consultaran partidos ya jugados -> la jornada a predecir "
              "quedara SIN datos de bajas.")
        return []
    fx = fx[fx["competition"].astype(str) == "LaLiga"]
    ts = pd.to_numeric(fx["start_ts"], errors="coerce")
    fx = fx[ts <= time.time() + horizon_days * 86400]
    return fx["event_id"].dropna().astype(int).drop_duplicates().tolist()


def _match_ids(master_csv: Path = sofa._MATCH_STATS_CSV, *,
               fixtures_csv: Path = _FIXTURES_CSV,
               horizon_days: int = _HORIZON_DAYS) -> list:
    """Universo de partidos a consultar: jugados (master) + por jugar (calendario).

    El universo es el historico entero mas el horizonte de prediccion, tambien en
    la pasada semanal. No se recorta por dias: lo que decide que se descarga es el
    atraso real --maestro mas sentinela, en `_run`--, y una ventana fija solo
    cubre la semana si la pasada anterior se ejecuto. Si el proyecto estuvo
    parado, recortar deja un hueco permanente y silencioso, porque el extractor
    termina diciendo que todo fue bien.
    """
    jugados: list = []
    try:
        df = pd.read_csv(master_csv, usecols=["match_id", "timestamp"])
        jugados = df["match_id"].dropna().astype(int).tolist()
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] master de partidos ilegible ({e}); se usara solo el calendario.")
    futuros = _fixture_ids(fixtures_csv, horizon_days=horizon_days)
    vistos: set = set()
    universo: list = []
    for eid in jugados + futuros:
        if eid not in vistos:
            vistos.add(eid)
            universo.append(eid)
    print(f"[INFO] universo de partidos: {len(jugados)} jugados + "
          f"{len(futuros)} del calendario -> {len(universo)} unicos.")
    return universo


def _past_ids(master_csv: Path = sofa._MATCH_STATS_CSV,
              fixtures_csv: Path = _FIXTURES_CSV) -> set:
    """event_ids cuyo partido YA se jugo. Solo estos se pueden 'sellar' en el
    sentinela: para un partido futuro sin bajas AUN, las bajas apareceran segun se
    acerque, asi que hay que re-consultarlo.

    Se leen las dos fuentes de fecha porque un partido recien jugado puede estar
    ya en el calendario y todavia no en el master de estadisticas; sin el
    calendario, ese encuentro sin bajas se re-consultaria indefinidamente.
    """
    corte = time.time() - _PLAYED_BUFFER_S
    out: set = set()
    for path, id_col, ts_col in ((master_csv, "match_id", "timestamp"),
                                 (fixtures_csv, "event_id", "start_ts")):
        try:
            df = pd.read_csv(path, usecols=[id_col, ts_col])
        except Exception:  # noqa: BLE001
            continue
        ts = pd.to_numeric(df[ts_col], errors="coerce")
        jugados = df.loc[ts < corte, id_col].dropna().astype(int)
        out |= set(jugados.astype(str))
    return out


_MAX_FAIL_STREAK = 8  # fallos de fetch consecutivos antes de abortar la pasada


def _checked_path(master_csv: Path) -> Path:
    """Sentinela: partidos consultados que NO tenian bajas (para idempotencia).

    Sin esto, un partido sin bajas devuelve [] y, al no dejar ninguna fila en el
    master, se vuelve a consultar en CADA pasada para siempre. Aqui se registra
    (event_id, checked_at) para no repetir la peticion."""
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
              "los partidos sin bajas.")
        return pd.DataFrame(columns=["event_id", "checked_at"]), set()


def _save_checked(df_checked: pd.DataFrame, new_ids: list, master_csv: Path) -> pd.DataFrame:
    if not new_ids:
        return df_checked
    add = pd.DataFrame({"event_id": [str(x) for x in new_ids],
                        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S")})
    df_checked = pd.concat([df_checked, add], ignore_index=True)
    df_checked = df_checked.drop_duplicates("event_id", keep="last")
    sofa._write_df_safe(df_checked, _checked_path(master_csv), fmt="csv", backup=False)
    return df_checked


def _run(event_ids, master_csv):
    """Consulta bajas de los partidos que faltan. Carga el master UNA sola vez.

    Idempotencia real: un partido YA JUGADO esta "hecho" si tiene filas en el
    master o figura en el sentinela de consultados-sin-bajas, y no se vuelve a
    pedir. Un partido POR JUGAR nunca esta hecho: su parte de bajas es
    provisional hasta el saque y es justo la que alimenta la prediccion, asi que
    se refresca en cada pasada y su foto nueva pisa la anterior. Los fallos de
    fetch (None) no marcan ni borran nada -> se reintentan en la siguiente
    pasada. Si se acumulan `_MAX_FAIL_STREAK` fallos seguidos, se aborta
    guardando lo obtenido.
    """
    session = sofa._new_session()
    df_master, done_master = _load_master(master_csv)
    df_checked, done_checked = _load_checked(master_csv)
    done = done_master | done_checked
    past = _past_ids()                       # partidos ya jugados (sellables)
    new_rows: list = []
    new_checked: list = []
    refrescados: list = []
    empties = failed = fail_streak = 0
    try:
        pending = [e for e in event_ids
                   if str(e) not in done or str(e) not in past]
        futuros = sum(1 for e in pending if str(e) not in past)
        print(f"[INFO] partidos a consultar: {len(pending)} "
              f"({len(pending) - futuros} jugados sin datos + "
              f"{futuros} por jugar, que se refrescan siempre).")
        for i, eid in enumerate(pending, 1):
            rows = scrape_missing(session, eid)
            if rows is None:                 # fallo de fetch -> reintentar luego
                failed += 1
                fail_streak += 1
                if fail_streak >= _MAX_FAIL_STREAK:
                    df_master = _finalize_master(df_master, new_rows, master_csv,
                                                 refrescados)
                    _save_checked(df_checked, new_checked, master_csv)
                    raise SystemExit(
                        f"\n[ERROR] {fail_streak} partidos seguidos con fallo de "
                        "descarga: el transporte a Sofascore esta caido. Lo "
                        "obtenido esta guardado; re-ejecuta para reanudar "
                        "(los fallidos se reintentan)."
                    )
                continue
            fail_streak = 0
            if str(eid) not in past:          # por jugar -> su foto pisa la vieja
                refrescados.append(eid)
            if rows:                          # hay bajas
                new_rows.extend(rows)
            elif str(eid) in past:            # jugado y SIN bajas -> sellar
                empties += 1
                new_checked.append(eid)
            # else: partido futuro sin bajas aun -> NO se sella (se re-consulta)
            if i % 25 == 0:
                print(f"  [{i}/{len(pending)}] bajas acumuladas: {len(new_rows)}")
                df_master = _finalize_master(df_master, new_rows, master_csv,
                                             refrescados)
                df_checked = _save_checked(df_checked, new_checked, master_csv)
                new_rows, new_checked, refrescados = [], [], []
            time.sleep(0.2)
        df_master = _finalize_master(df_master, new_rows, master_csv, refrescados)
        _save_checked(df_checked, new_checked, master_csv)
    finally:
        session.close()
    print(f"[INFO] partidos sin bajas (registrados): {empties}; "
          f"fallos de fetch (se reintentaran): {failed}")
    return df_master


def backfill(master_csv: Path = _MASTER_CSV):
    Path(master_csv).parent.mkdir(parents=True, exist_ok=True)
    _run(_match_ids(), master_csv)
    process(master_csv)


def update(master_csv: Path = _MASTER_CSV):
    """Pasada semanal: completa lo que falte del historico y refresca las bajas
    de los partidos por jugar, que son las que entran en la prediccion."""
    _run(_match_ids(), master_csv)
    process(master_csv)


def _reclassify(df: pd.DataFrame) -> pd.DataFrame:
    """Recalcula category/is_out desde los campos crudos del CSV.

    El raw guarda type/reason/description/external_type; las columnas derivadas
    (category, is_out) se regeneran aqui, de modo que cualquier mejora de
    `_classify` se aplica al REPROCESAR, sin volver a scrapear.
    """
    if df.empty or "type" not in df.columns:
        return df
    df = df.copy()
    cats, outs = [], []
    for _, r in df.iterrows():
        c, o = _classify({"type": r.get("type"), "reason": r.get("reason"),
                          "description": r.get("description"),
                          "externalType": r.get("external_type")})
        cats.append(c)
        outs.append(o)
    df["category"] = cats
    df["is_out"] = outs
    return df


def process(master_csv: Path = _MASTER_CSV, *, out_parquet: Path = _OUT_PARQUET,
            verbose: bool = True) -> pd.DataFrame:
    master_csv = Path(master_csv)
    sentinela = _checked_path(master_csv)
    if not master_csv.exists() and not sentinela.exists():
        raise _common.RegressionError(
            f"[availability.process] no existe ni el maestro '{master_csv.name}' "
            f"ni el sentinela '{sentinela.name}': no hay nada consultado que "
            "publicar y un parquet vacio destruiria el vigente. Ejecuta antes el "
            "backfill.")
    df = pd.read_csv(master_csv) if master_csv.exists() else pd.DataFrame()
    df = _reclassify(df)
    # los partidos consultados SIN bajas viven solo en el sentinela: hay que
    # incorporarlos para que entren como ceros y no como "sin dato". Salvo los
    # de una temporada en la que la fuente no tiene feed de bajas: ahi el cero
    # seria una afirmacion falsa (ver la nota de semantica del modulo).
    _, sin_bajas = _load_checked(master_csv)
    sin_bajas = _sin_temporadas_sin_feed(df, sin_bajas, verbose=verbose)
    agg = aggregate(df, queried_ids=sin_bajas)
    if verbose:
        _report(df, agg)
    _common.publish_df(agg, out_parquet, source="availability.process",
                       fmt="parquet", verbose=verbose)
    return agg


def _report(df: pd.DataFrame, agg: pd.DataFrame) -> None:
    print("\n===== INFORME BAJAS =====")
    print(f"filas jugador-baja : {len(df)}")
    print(f"equipo-partido     : {len(agg)}")
    if len(agg):
        n_part = agg["event_id"].nunique()
        cero = int((agg.groupby("event_id")["n_missing"].sum() == 0).sum())
        print(f"partidos consultados: {n_part}  (de ellos SIN ninguna baja: {cero})")
        print("los partidos NO consultados quedan fuera de la tabla -> NaN en la "
              "consolidacion -> flag_no_absences (no se confunden con un 0 real).")
    if "category" in df and len(df):
        print("categorias (revisa que el texto clasifica bien):")
        for k, v in df["category"].value_counts().items():
            print(f"   {v:6d}  {k}")
        # vocabulario crudo para afinar el mapa si hiciera falta
        print("muestra description/externalType/reason:")
        for _, r in df.dropna(subset=["description"]).head(6).iterrows():
            print(f"   type={r.get('type')!r} reason={r.get('reason')!r} "
                  f"desc={r.get('description')!r} ext={r.get('external_type')!r}")
    if len(agg):
        print(f"media bajas/equipo : {agg['n_missing'].mean():.2f}")
    print("=========================\n")


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
        print("uso: python availability.py [backfill|update|weekly|process|schedule]")
