"""Reducers: llevan cada fuente SofaScore a granularidad "1 fila por partido".

Cada funcion recibe un DataFrame ya procesado por su extractor y devuelve una
tabla con clave unica `match_key` y columnas `home_*` / `away_*`, lista para un
left-merge sobre el spine del master.

Puentes de clave:
  - `match_key` = "YYYY-MM-DD|home_key|away_key", producido por teamnames.
  - `event_id` (fatigue, availability, lineups) == `match_id` (sofascore): las
    cuatro fuentes comparten el identificador de partido de SofaScore, y por eso
    el mapa `event_id -> match_key` se toma de su tabla canonica.
  - la calidad de ocasion (Understat) no pasa por SofaScore y trae ya la clave
    canonica construida con el mismo `teamnames`, de modo que su reducer no usa
    puente alguno.

Casos borde tolerados:
  - fatigue LaLiga trae event_ids futuros (fixtures a temporadas siguientes) que
    NO estan en sofascore: al mapear a match_key quedan NaN y se descartan, por
    no formar parte del universo del master.
  - algunos partidos traen un solo lado (home o away) en fatigue/availability:
    el lado ausente queda NaN; el left-merge posterior no rompe.
  - lineups deja en NaN las primeras jornadas de cada temporada. No es un fallo
    de cobertura sino la consecuencia de que sus magnitudes se calculan sobre el
    pasado del equipo dentro de la temporada, y en agosto ese pasado no existe.
  - la calidad de ocasion no tiene fila para el equipo que no genero ningun
    disparo admisible en un partido. Tampoco es un fallo: significa que no hay
    distribucion de tiros que resumir, que es distinto de resumirla en cero.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# --- reutilizar la maquinaria de los extractores (misma carpeta hermana) ---
_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

import teamnames as tn  # noqa: E402
from _common import SchemaError, assert_columns, assert_unique  # noqa: E402

# Rutas estandar del repo (mismas que usan los extractores)
_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
RAW = _ROOT / "data" / "01_raw"

# Primera temporada del master (16/17). Season = ano de inicio.
FIRST_SEASON = 2016


# --------------------------------------------------------------------------- #
# Puente event_id -> match_key (tabla canonica = sofascore)
# --------------------------------------------------------------------------- #
def event_to_match_key(sofa: pd.DataFrame) -> pd.Series:
    """Serie `match_id -> match_key` a partir de la tabla canonica de sofascore.

    Es el unico puente entre el mundo `event_id` (fatigue/availability) y el
    mundo `match_key` (spine FD + sofascore). Falla ruidosamente si sofascore no
    tiene match_key unico por partido.
    """
    assert_columns(sofa, ["match_id", "match_key"], "sofascore")
    assert_unique(sofa, ["match_key"], "sofascore")
    if sofa["match_id"].duplicated().any():
        raise SchemaError("sofascore: match_id duplicado; no puedo mapear event_id->match_key")
    return sofa.set_index("match_id")["match_key"]


# --------------------------------------------------------------------------- #
# 1) SofaScore  (ya es 1 fila / partido -> solo seleccionar y validar)
# --------------------------------------------------------------------------- #
def reduce_sofascore(sofa: pd.DataFrame) -> pd.DataFrame:
    """Tabla sofascore keyed por match_key con todo el payload `sofa_*` + ratings.

    No re-normaliza nombres: sofascore ya trae `match_key` canonico. Se limita a
    seleccionar las columnas de estadistica (una por partido, home/away en
    columnas), el sello de saque `kickoff_ts` y a garantizar unicidad por
    match_key.
    """
    assert_columns(sofa, ["match_key", "season", "round", "date"], "sofascore")
    assert_unique(sofa, ["match_key"], "sofascore")

    feat_cols = [c for c in sofa.columns if c.startswith("sofa_")]
    feat_cols += [c for c in ("home_avg_rating", "away_avg_rating") if c in sofa.columns]

    # `timestamp` (= startTimestamp de sofascore) viaja al master como METADATO
    # del partido, no como feature: es la unica fuente de la HORA de saque con
    # cobertura completa desde 16/17 —la columna `Time` de football-data solo
    # aparece a partir de 2019/20, y un hueco ligado a la epoca confundiria
    # cualquier efecto horario con el de las temporadas que lo tienen—. De ahi lo
    # derivan las features de calendario (`features/context.py`); en crudo no es
    # utilizable porque es un entero de segundos sin significado propio.
    keep = (["match_key", "match_id", "season", "round", "date"]
            + (["timestamp"] if "timestamp" in sofa.columns else []) + feat_cols)
    out = sofa.loc[:, [c for c in keep if c in sofa.columns]].copy()
    # prefijo homogeneo para los ratings (el resto ya va con sufijo _home/_away)
    out = out.rename(columns={"home_avg_rating": "home_sofa_rating",
                              "away_avg_rating": "away_sofa_rating",
                              "timestamp": "kickoff_ts"})
    assert_unique(out, ["match_key"], "sofascore.reduced")
    return out.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 2) Fatiga  (largo, 1 fila / equipo-partido, multi-competicion -> home/away)
# --------------------------------------------------------------------------- #
_FAT_STATS = [
    "rest_days", "games_last_7", "games_last_14",
    "is_midweek", "euro_trip_recent", "is_post_intl_break",
]


def reduce_fatigue(fat: pd.DataFrame, event2key: pd.Series) -> pd.DataFrame:
    """Pivota team_fatigue a `home_fat_*` / `away_fat_*` por partido de LaLiga.

    Solo las filas cuya competicion es 'LaLiga' definen partidos del master; los
    valores de fatiga que contienen (rest_days, games_last_7/14, ...) YA reflejan
    la congestion de TODAS las competiciones (esa es la gracia del extractor).
    El partido se identifica por event_id, que se traduce a match_key.
    """
    assert_columns(fat, ["event_id", "is_home", "competition"] + _FAT_STATS, "fatigue")

    laliga = fat[fat["competition"] == "LaLiga"].copy()
    laliga["match_key"] = laliga["event_id"].map(event2key)
    laliga = laliga.dropna(subset=["match_key"])  # descarta fixtures futuros / no casados

    home = (laliga[laliga["is_home"]]
            .drop_duplicates("match_key")
            .set_index("match_key")[_FAT_STATS]
            .add_prefix("home_fat_"))
    away = (laliga[~laliga["is_home"]]
            .drop_duplicates("match_key")
            .set_index("match_key")[_FAT_STATS]
            .add_prefix("away_fat_"))

    out = home.join(away, how="outer").reset_index()
    assert_unique(out, ["match_key"], "fatigue.reduced")
    return out


# --------------------------------------------------------------------------- #
# 3) Disponibilidad / bajas (1 fila / equipo-partido por side -> home/away)
# --------------------------------------------------------------------------- #
_AV_STATS = ["n_missing", "n_out", "n_injury", "n_suspension", "n_doubtful"]


def reduce_availability(av: pd.DataFrame, event2key: pd.Series) -> pd.DataFrame:
    """Pivota team_availability (event_id + side) a `home_*` / `away_*`.

    Solo CONTEOS (n_missing, n_out, ...). La ponderacion por valor economico de
    las bajas se construye aparte (raw missing_players x player_values), porque
    esta tabla no trae los nombres de los jugadores.

    CONTRATO de la fuente (`extractors/availability.py`): la tabla contiene los dos
    lados de TODO partido consultado, con recuentos 0 cuando no hubo bajas. Por
    tanto un 0 aqui es un cero REAL y un partido AUSENTE es "no consultado", que al
    hacer left-merge en el master queda NaN y lo recoge `flag_no_absences`. Los dos
    estados no deben confundirse: el primero es senal, el segundo es falta de dato.
    """
    assert_columns(av, ["event_id", "side"] + _AV_STATS, "availability")
    if not set(av["side"].unique()) <= {"home", "away"}:
        raise SchemaError(f"availability: 'side' inesperado: {av['side'].unique()}")

    piv = av.pivot_table(index="event_id", columns="side", values=_AV_STATS, aggfunc="first")
    piv.columns = [f"{side}_{stat}" for stat, side in piv.columns]  # aplanar MultiIndex
    piv = piv.reset_index()
    piv["match_key"] = piv["event_id"].map(event2key)
    piv = piv.dropna(subset=["match_key"]).drop(columns=["event_id"])

    assert_unique(piv, ["match_key"], "availability.reduced")
    # orden estable de columnas: match_key primero
    cols = ["match_key"] + [c for c in piv.columns if c != "match_key"]
    return piv.loc[:, cols].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 4) Rotacion y minutos (1 fila / equipo-partido por side -> home/away)
# --------------------------------------------------------------------------- #
_LIN_STATS = ["xi_continuity", "xi_continuity_mean3",
              "starters_minutes_7", "starters_minutes_14",
              "minutes_hhi", "n_players_used"]

# Magnitudes cuyo CONTRASTE entre los dos equipos es la cantidad interesante y
# que por eso viajan tambien como diferencia. El criterio no es de comodidad: la
# continuidad del once y los minutos acumulados describen el ESTADO con el que
# cada equipo llega a este partido, y lo que decide un encuentro es el
# desequilibrio entre ambos, no el nivel absoluto de uno. La concentracion de
# minutos y el numero de futbolistas utilizados, en cambio, describen una
# politica de temporada —cuanto rota el entrenador— que se interpreta por si
# sola; convertirla en diferencia mezclaria dos rasgos de plantilla distintos en
# un solo numero sin significado claro.
_LIN_DIFFS = ["xi_continuity", "starters_minutes_7", "starters_minutes_14"]


def reduce_lineups(lin: pd.DataFrame, event2key: pd.Series) -> pd.DataFrame:
    """Pivota team_lineups (event_id + side) a `home_*` / `away_*` + diferencias.

    La tabla de origen (`extractors/lineups.py`) ya viene calculada MIRANDO SOLO
    HACIA ATRAS: `home_xi_continuity` no es la rotacion de este partido —que en
    el momento de predecir se desconoce— sino la que el equipo mostro en el
    ultimo que jugo. Ese desplazamiento se hace en el extractor y no aqui, junto
    al dato crudo del que depende; este reducer se limita a cambiar la
    granularidad.

    CONTRATO de la fuente: contiene los dos lados de TODO partido de LaLiga del
    calendario, jugado o no, y deja en NaN las magnitudes que aun no tienen
    historial suficiente (arranque de temporada). Un NaN aqui significa, por
    tanto, "el equipo no ha jugado todavia bastante en esta temporada", no "no se
    consulto"; `flag_no_*` del preprocesado lo recoge igual, pero la lectura al
    interpretar el modelo es distinta.
    """
    assert_columns(lin, ["event_id", "side"] + _LIN_STATS, "lineups")
    if not set(lin["side"].unique()) <= {"home", "away"}:
        raise SchemaError(f"lineups: 'side' inesperado: {lin['side'].unique()}")

    piv = lin.pivot_table(index="event_id", columns="side", values=_LIN_STATS,
                          aggfunc="first", dropna=False)
    piv.columns = [f"{side}_{stat}" for stat, side in piv.columns]  # aplanar MultiIndex
    piv = piv.reset_index()
    piv["match_key"] = piv["event_id"].map(event2key)
    piv = piv.dropna(subset=["match_key"]).drop(columns=["event_id"])

    for stat in _LIN_DIFFS:
        h, a = f"home_{stat}", f"away_{stat}"
        piv[f"{stat}_diff"] = (piv[h] - piv[a]) if (h in piv and a in piv) else np.nan

    assert_unique(piv, ["match_key"], "lineups.reduced")
    cols = (["match_key"]
            + [f"{s}_{c}" for c in _LIN_STATS for s in ("home", "away")
               if f"{s}_{c}" in piv.columns]
            + [f"{c}_diff" for c in _LIN_DIFFS])
    return piv.loc[:, cols].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 5) Calidad de ocasión (1 fila / equipo-partido por side -> home/away)
# --------------------------------------------------------------------------- #
# Las cuatro magnitudes que publica `extractors/understat_shots.py`. La lista se
# escribe aquí y no se importa del extractor a propósito: los reducers son la
# frontera del máster y deben poder leerse sin abrir el módulo de scraping, del
# mismo modo que `_FAT_STATS` o `_LIN_STATS` tampoco se importan de los suyos.
_SHOT_STATS = ["shot_xg_mean", "shot_dist_mean", "shot_setpiece_xg_pct",
               "shot_xg_max"]


def reduce_shot_quality(shots: pd.DataFrame) -> pd.DataFrame:
    """Pivota team_shot_quality (match_key + side) a `home_shot_*` / `away_shot_*`.

    Es el único reducer que no necesita el puente `event_id -> match_key`: el
    extractor de tiros no pasa por SofaScore y construye la clave canónica él
    mismo a partir de la fecha y los dos nombres, con el mismo `teamnames` que
    usa el resto del proyecto. Que el puente sobre y que aun así la clave case
    con la del máster es, de hecho, la comprobación de que la normalización de
    nombres es consistente entre fuentes independientes.

    A diferencia de la rotación, aquí NO se calculan diferencias local–visitante.
    La razón es que estas columnas describen lo ocurrido en el propio partido y
    por tanto son materia prima y no variables: nunca entran al modelo tal cual,
    sino a través de las ventanas retrospectivas de `features/form.py`, que
    construyen ellas mismas el contraste entre los dos equipos. Un `_diff` aquí
    sería una diferencia entre dos hechos del futuro, inservible y peligrosa.

    Tampoco viaja el recuento de disparos: el volumen ya llega desde SofaScore y
    duplicarlo con una definición ligeramente distinta es justo lo que la
    cabecera del extractor descarta. `n_shots` se queda en el parquet de origen,
    donde cumple su papel de denominador para el análisis descriptivo.

    CONTRATO de la fuente: solo hay fila para el equipo-partido que produjo algún
    disparo admisible —en juego, ni penalti ni gol en propia—. Un partido ausente
    de esta tabla no es un fallo de cobertura sino la ausencia de distribución que
    resumir; el left-merge lo deja en NaN y las ventanas de forma lo ignoran en
    lugar de promediarlo como si fuera un cero.
    """
    assert_columns(shots, ["match_key", "side"] + _SHOT_STATS, "shot_quality")
    if not set(shots["side"].dropna().unique()) <= {"home", "away"}:
        raise SchemaError(
            f"shot_quality: 'side' inesperado: {shots['side'].unique()}")

    piv = shots.pivot_table(index="match_key", columns="side",
                            values=_SHOT_STATS, aggfunc="first", dropna=False)
    piv.columns = [f"{side}_{stat}" for stat, side in piv.columns]  # aplanar MultiIndex
    piv = piv.reset_index()

    assert_unique(piv, ["match_key"], "shot_quality.reduced")
    cols = (["match_key"]
            + [f"{s}_{c}" for c in _SHOT_STATS for s in ("home", "away")
               if f"{s}_{c}" in piv.columns])
    return piv.loc[:, cols].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Auto-chequeo offline: valida invariantes contra los parquets presentes.
#   python3 scripts/consolidate/reducers.py
# --------------------------------------------------------------------------- #
def _selfcheck() -> None:
    sofa = pd.read_parquet(PROC / "sofascore_team_match.parquet")
    fat = pd.read_parquet(PROC / "team_fatigue.parquet")
    av = pd.read_parquet(PROC / "team_availability.parquet")
    p_lin = PROC / "team_lineups.parquet"
    lin = pd.read_parquet(p_lin) if p_lin.exists() else None
    p_shot = PROC / "team_shot_quality.parquet"
    shot = pd.read_parquet(p_shot) if p_shot.exists() else None

    e2k = event_to_match_key(sofa)
    r_sofa = reduce_sofascore(sofa)
    r_fat = reduce_fatigue(fat, e2k)
    r_av = reduce_availability(av, e2k)
    r_lin = reduce_lineups(lin, e2k) if lin is not None else None
    r_shot = reduce_shot_quality(shot) if shot is not None else None

    def _report(name, df, key="match_key"):
        seasons = ""
        if "season" in df.columns:
            seasons = f" | temporadas={sorted(df['season'].dropna().unique())[:3]}...{sorted(df['season'].dropna().unique())[-1:]}"
        print(f"[{name}] filas={len(df):<5} cols={df.shape[1]:<4} "
              f"match_key unicos={df[key].nunique()}{seasons}")

    print("=" * 70)
    print("SELF-CHECK reducers (offline) —", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 70)
    _report("sofascore", r_sofa)
    _report("fatigue  ", r_fat)
    _report("availabty", r_av)
    if r_lin is not None:
        _report("lineups  ", r_lin)
    if r_shot is not None:
        _report("shotqual ", r_shot)

    # invariantes duras
    assert r_sofa["match_key"].is_unique, "sofascore: match_key no unico"
    assert r_fat["match_key"].is_unique, "fatigue: match_key no unico"
    assert r_av["match_key"].is_unique, "availability: match_key no unico"
    if r_lin is not None:
        assert r_lin["match_key"].is_unique, "lineups: match_key no unico"
    if r_shot is not None:
        assert r_shot["match_key"].is_unique, "shot_quality: match_key no unico"

    # cobertura relativa al universo sofascore
    universe = set(r_sofa["match_key"])
    cov_fat = len(set(r_fat["match_key"]) & universe) / len(universe)
    cov_av = len(set(r_av["match_key"]) & universe) / len(universe)
    print(f"\nCobertura sobre universo sofascore ({len(universe)} partidos):")
    print(f"  fatigue      : {cov_fat:6.1%}")
    print(f"  availability : {cov_av:6.1%}")
    if r_lin is not None:
        cov_lin = len(set(r_lin["match_key"]) & universe) / len(universe)
        print(f"  lineups      : {cov_lin:6.1%}")
        # el hueco esperado de este bloque no es de cobertura de partido sino de
        # historial: las primeras jornadas de cada temporada no tienen pasado.
        frio = r_lin["home_xi_continuity"].isna().mean()
        print(f"  (arranque en frio de la continuidad: {frio:6.1%} de los partidos)")
    else:
        print("  lineups      : parquet ausente (ejecuta lineups.py backfill)")
    if r_shot is not None:
        cov_shot = len(set(r_shot["match_key"]) & universe) / len(universe)
        print(f"  shot_quality : {cov_shot:6.1%}")
        # Aqui el hueco relevante no es de partido sino de LADO: el equipo que no
        # remato en juego no tiene fila, de modo que un partido puede estar
        # cubierto y traer una mitad vacia.
        medio = r_shot[["home_shot_xg_mean", "away_shot_xg_mean"]].isna().any(axis=1).mean()
        print(f"  (partidos con un lado sin tiros admisibles: {medio:6.1%})")
    else:
        print("  shot_quality : parquet ausente (ejecuta understat_shots.py backfill)")

    # cuantos lados quedan NaN (casos borde)
    fat_home_nan = r_fat[[c for c in r_fat if c.startswith("home_fat_")]].isna().all(axis=1).sum()
    fat_away_nan = r_fat[[c for c in r_fat if c.startswith("away_fat_")]].isna().all(axis=1).sum()
    print(f"\nCasos borde fatigue (lado ausente): home_all_NaN={fat_home_nan}  away_all_NaN={fat_away_nan}")
    print("\nMuestra fatigue reducida:")
    print(r_fat.head(3).to_string())
    print("\nMuestra availability reducida:")
    print(r_av.head(3).to_string())
    if r_lin is not None:
        print("\nMuestra lineups reducida:")
        print(r_lin.head(3).to_string())
    if r_shot is not None:
        print("\nMuestra calidad de ocasion reducida:")
        print(r_shot.head(3).to_string())
    n = 3 + (r_lin is not None) + (r_shot is not None)
    print(f"\nOK: los {n} reducers cumplen 1-fila-por-partido y clave unica.")


if __name__ == "__main__":
    _selfcheck()
