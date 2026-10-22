"""Features de FORMA leak-safe a partir del team-match long.

Regla de oro anti-fuga: cada estadística de un partido solo puede describir
partidos ANTERIORES del equipo. Se implementa con tres patrones:

    acumulado de temporada : g[attr].cumsum().shift(1)         -> excluye el actual
    forma últimos N        : g[attr].shift(1).rolling(N).mean()
    forma exponencial      : g[attr].shift(1).ewm(halflife=H).mean()

El .shift(1) garantiza que la fila del partido t NO ve su propio resultado. Se
agrupa por (season_year, team_key): la tabla y la "forma" se reinician cada
temporada. Excepción deliberada: el híbrido cross-season (gd_hybrid) y el
momentum de Elo miran a través del corte de temporada a propósito, para no dejar
NaN en las primeras jornadas (cold-start), ponderando la temporada previa.

Qué se construye y qué no
-------------------------
Este módulo no decide su propio alcance: lee de `catalogo` qué atributos calcular
(`FORMA_ATRIBUTOS`), con qué memoria (`FORMA_SEMIVIDA` y `FORMA_VENTANA`) y en qué
lados emitir cada resultado (`catalogo.lados`). Un atributo que se añada aquí sin
declararse allí se calcula en el long pero no llega a la matriz de partido, que
es la garantía de que el conjunto de variables sea una decisión declarada y no el
residuo de lo que este módulo tuvo a bien construir.

Dos parametrizaciones de la misma memoria
-----------------------------------------
De cada atributo salen dos lecturas porque responden a dos preguntas distintas.
La ponderación exponencial —semivida declarada en `catalogo.FORMA_SEMIVIDA`—
describe cómo ha ido el equipo, decayendo de forma continua sin que nada
desaparezca de golpe ni haya que elegir dónde cortar. La ventana corta
(`catalogo.FORMA_VENTANA`) describe cómo llega ahora mismo, y por ser corta capta
el cambio brusco —un cambio de entrenador, una lesión clave— que la exponencial
todavía está amortiguando. Una tercera ventana intermedia no añadiría una tercera
pregunta, solo una tercera respuesta a la misma, y por eso no se produce.

Dificultad del calendario
-------------------------
Doce puntos en cinco partidos no significan lo mismo contra los cinco primeros
que contra los cinco últimos. La media móvil de `opp_elo_z` viaja junto al resto
de la forma para que el modelo pueda leer cada racha en su contexto de dificultad
y descontarla por sí mismo, sin necesidad de un residuo precalculado que fije de
antemano cuánto vale un rival duro. La dureza se mide en desviaciones respecto a
la media de la jornada y no en puntos Elo porque es la única lectura comparable a
lo largo del histórico (ver `consolidate/spine.add_clubelo`).

Bloques que se producen (por equipo, luego al lado que el catálogo declare):
  * matches_prev, acumulados per-game (cum_*_pg)
  * forma exponencial (form_<attr>_ewm) y ventana corta (form_<attr>_<N>) sobre
    los atributos de `catalogo.FORMA_ATRIBUTOS`
  * forma específica de ESCENARIO (venue_form_*): rendimiento del equipo SOLO en
    su condición (local en casa, visitante fuera)
  * effectiveness: puntos acumulados / tiros a puerta acumulados (pre-partido)
  * playstyle: perfil derivado del híbrido de dif. de goles con memoria entre
    temporadas (el híbrido es insumo interno, no sale a la matriz)
  * elo_mom_5_z: variación de la fuerza tipificada en los últimos 5 partidos
    (momentum). Se calcula sobre `elo_z` y no sobre el Elo en nivel: una resta
    entre dos puntos de la serie en nivel no cancela el cambio de escala de
    ClubElo si caen a distinto lado del empalme de octubre de 2022.

Ninguna columna de este módulo lleva el token `close`: toda la forma se conoce
en cuanto termina la jornada anterior, de modo que el bloque completo pertenece
al régimen semanal y sobrevive al filtro de columns.weekly_unavailable().
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import catalogo
from teamlong import to_team_long

# Todo el alcance del módulo se lee del catálogo: aquí solo se le pone nombre
# local a lo que allí se declara, para que este fichero no tenga una segunda
# lista de atributos que pueda divergir de la primera.
_ROLL_ATTRS = list(catalogo.FORMA_ATRIBUTOS)
# atributos con acumulado de temporada (per-game)
_CUM_ATTRS = list(catalogo.FORMA_ACUMULADOS)
# ventana corta de forma reciente (la exponencial es la otra parametrización)
WINDOWS = (catalogo.FORMA_VENTANA,)
# semivida (en partidos) de la forma exponencial: a los 3 partidos de distancia un
# resultado pesa la mitad que el último, y a los 6 la cuarta parte. Se declara —no
# se ajusta— para que la feature siga siendo una descripción del pasado y no un
# hiperparámetro más que optimizar sobre el conjunto de evaluación.
EWM_HALFLIFE = catalogo.FORMA_SEMIVIDA
# forma por escenario (local en casa / visitante fuera)
_VENUE_ATTRS = list(catalogo.FORMA_ESCENARIO)
_VENUE_WINDOW = catalogo.FORMA_ESCENARIO_VENTANA

_GRP = ["season_year", "team_key"]

# --- híbrido cross-season (memoria entre temporadas) -------------------------
_HYB_WINDOW = 6        # nº máximo de partidos previos a considerar
_HYB_PREV_W = 0.7      # peso de partidos de la temporada PREVIA
_HYB_MINP = 3          # mínimo de previos (actual+previa) si NO es ascendido
_HYB_MINP_PROMOTED = 1  # mínimo de previos si ES temporada de debut
_HYB_THR_OFF = 0.75    # umbral estilo ofensivo
_HYB_THR_DEF = -0.75   # umbral estilo defensivo


def _hybrid_gd_series(g: pd.DataFrame) -> pd.Series:
    """Media ponderada de gd con memoria de la temporada previa (leak-safe).

    Para cada partido usa hasta _HYB_WINDOW partidos ANTERIORES del equipo
    (a través del corte de temporada); a los de la temporada previa les da peso
    _HYB_PREV_W. En temporada de debut usa solo los previos de esa temporada.
    """
    seasons = g["season_year"].to_numpy()
    gds = g["gd"].to_numpy(dtype="float64")
    promoted = g["is_promoted"].to_numpy()
    n = len(g)
    out = np.full(n, np.nan)
    for i in range(n):
        if i == 0:
            continue
        start = max(0, i - _HYB_WINDOW)
        ps, pg = seasons[start:i], gds[start:i]
        if promoted[i]:
            sel = pg[ps == seasons[i]]
            sel = sel[~np.isnan(sel)]
            if sel.size < _HYB_MINP_PROMOTED:
                continue
            out[i] = sel.mean()
        else:
            valid = ~np.isnan(pg)
            if valid.sum() < _HYB_MINP:
                continue
            w = np.where(ps == seasons[i], 1.0, _HYB_PREV_W)
            w = w[valid]
            out[i] = np.dot(pg[valid], w) / w.sum()
    return pd.Series(out, index=g.index)


def _classify_style(v: float) -> object:
    if pd.isna(v):
        return pd.NA
    if v >= _HYB_THR_OFF:
        return "ofensivo"
    if v <= _HYB_THR_DEF:
        return "defensivo"
    return "equilibrado"


def _add_team_features(long: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    """Añade columnas de forma. Devuelve (long, feats_numericas, feats_categoricas)."""
    long = long.sort_values(["team_key", "date", "match_key"]).reset_index(drop=True)
    num: list[str] = []
    cat: list[str] = []

    # nº de partidos ya jugados esa temporada (0 en la 1ª jornada)
    long["matches_prev"] = long.groupby(_GRP, sort=False).cumcount().astype("float64")
    num.append("matches_prev")

    # acumulados de temporada (excluyen el partido actual) -> per-game
    g = long.groupby(_GRP, sort=False)
    denom = long["matches_prev"].replace(0, np.nan)
    for attr in _CUM_ATTRS:
        long[f"cum_{attr}_pg"] = g[attr].transform(lambda s: s.cumsum().shift(1)) / denom
        num.append(f"cum_{attr}_pg")

    # forma reciente: media de la ventana corta (shift para no verse a sí mismo)
    g = long.groupby(_GRP, sort=False)
    for attr in _ROLL_ATTRS:
        for w in WINDOWS:
            col = f"form_{attr}_{w}"
            long[col] = g[attr].transform(
                lambda s, w=w: s.shift(1).rolling(w, min_periods=1).mean())
            num.append(col)

    # forma exponencial: mismo contenido, otro supuesto de olvido. `ignore_na=False`
    # (por defecto) hace que un partido sin dato consuma su turno en la ponderación
    # en lugar de comprimir el calendario, que es lo coherente con el hecho de que
    # el partido se jugó aunque la fuente no lo cubra.
    for attr in _ROLL_ATTRS:
        col = f"form_{attr}_ewm"
        long[col] = g[attr].transform(
            lambda s: s.shift(1).ewm(halflife=EWM_HALFLIFE, min_periods=1).mean())
        num.append(col)

    # forma por ESCENARIO: solo partidos en la misma condición (casa/fuera)
    gv = long.groupby(["season_year", "team_key", "is_home"], sort=False)
    for attr in _VENUE_ATTRS:
        col = f"venue_form_{attr}_{_VENUE_WINDOW}"
        long[col] = gv[attr].transform(
            lambda s: s.shift(1).rolling(_VENUE_WINDOW, min_periods=1).mean())
        num.append(col)

    # effectiveness: puntos acumulados / tiros a puerta acumulados (pre-partido)
    g = long.groupby(_GRP, sort=False)
    cum_pts = g["points"].transform(lambda s: s.cumsum().shift(1))
    cum_sot = g["sot"].transform(lambda s: s.cumsum().shift(1))
    long["effectiveness"] = cum_pts / cum_sot.replace(0, np.nan)
    num.append("effectiveness")

    # momentum de fuerza: variación en los últimos 5 partidos (cross-season)
    ge = long.groupby("team_key", sort=False)
    long["elo_mom_5_z"] = ge["elo_z"].transform(lambda s: s - s.shift(5))
    num.append("elo_mom_5_z")

    # híbrido cross-season de gd -> estilo de juego. El híbrido es INSUMO: se
    # calcula para clasificar el perfil del equipo, pero no sale a la matriz,
    # porque como número es una media de diferencia de goles más que ya viajan
    # en cum_points_pg y en la forma. Lo que aporta es el corte en tres perfiles.
    first_season = long.groupby("team_key")["season_year"].transform("min")
    long["is_promoted"] = long["season_year"] == first_season
    long["gd_hybrid"] = (long.sort_values(["team_key", "date", "match_key"])
                         .groupby("team_key", group_keys=False)
                         .apply(_hybrid_gd_series))
    long["playstyle"] = long["gd_hybrid"].map(_classify_style).astype("string")
    cat.append("playstyle")

    return long, num, cat


def _to_wide(long: pd.DataFrame, num: list[str], cat: list[str]) -> pd.DataFrame:
    """team-long con features -> 1 fila/partido, con los lados que declara el catálogo.

    De cada atributo de equipo caben tres columnas en la fila de partido —el
    valor del local, el del visitante y su resta—, y las tres son linealmente
    dependientes: emitirlas todas regala una columna redundante por atributo. El
    catálogo decide cuáles salen, y esta función se limita a obedecerle. El par
    intermedio se construye siempre porque la diferencia se calcula a partir de
    él; lo que no esté declarado se descarta al final.
    """
    feats = num + cat
    keep = ["match_key", "is_home"] + feats
    home = (long.loc[long["is_home"] == 1, keep]
                .drop(columns="is_home")
                .rename(columns={c: f"home_{c}" for c in feats}))
    away = (long.loc[long["is_home"] == 0, keep]
                .drop(columns="is_home")
                .rename(columns={c: f"away_{c}" for c in feats}))
    wide = home.merge(away, on="match_key", how="outer")

    # diferencias home-away SOLO para features numéricas (señal relativa)
    for c in num:
        if catalogo.emite(c, "diff"):
            wide[f"{c}_diff"] = wide[f"home_{c}"] - wide[f"away_{c}"]

    sobran = [f"{lado}_{c}" for c in feats for lado in ("home", "away")
              if not catalogo.emite(c, lado)]
    return wide.drop(columns=sobran)


def build_form_features(master: pd.DataFrame) -> pd.DataFrame:
    """Máster -> tabla de features de forma leak-safe, keyed por match_key."""
    long = to_team_long(master)
    long, num, cat = _add_team_features(long)
    wide = _to_wide(long, num, cat)
    return wide.reset_index(drop=True)


def form_feature_names() -> dict[str, list[str]]:
    """Atributos BASE de forma por tipo (para documentación / QA).

    Devuelve los atributos de equipo, no las columnas finales de la matriz: la
    correspondencia base -> lados vive en `catalogo.nombres`, y quien necesite
    los nombres de columna la consulta allí en vez de reconstruirla aquí.
    """
    num = ["matches_prev"] + [f"cum_{a}_pg" for a in _CUM_ATTRS]
    for a in _ROLL_ATTRS:
        for w in WINDOWS:
            num.append(f"form_{a}_{w}")
    num += [f"form_{a}_ewm" for a in _ROLL_ATTRS]
    num += [f"venue_form_{a}_{_VENUE_WINDOW}" for a in _VENUE_ATTRS]
    num += ["effectiveness", "elo_mom_5_z"]
    return {"numeric": num, "categorical": ["playstyle"]}
