"""Reshape del máster (1 fila/partido) a formato TEAM-MATCH LONG (2 filas/partido).

Para calcular "forma" de un equipo hay que verlo como una secuencia temporal de
SUS partidos, no como filas home/away de partidos. Este módulo explota cada
partido en dos filas —una por equipo, cada una desde su propia perspectiva— con
un esquema homogéneo. Es la primitiva sobre la que form.py calcula medias
móviles y acumulados con shift() sin mezclar equipos.

Perspectiva "propia" del equipo en cada fila:
    gf/ga       goles a favor/en contra de ESE equipo
    xg/xga      xG generado/concedido por ESE equipo
    npxg/npxga  ídem sin penaltis
    xpts        puntos esperados de ESE equipo
    ppda        presión de ESE equipo (menor = presiona más)
    deep        pases profundos de ESE equipo
    rating      rating SofaScore de ESE equipo en el partido
    shots/sot   tiros / tiros a puerta de ESE equipo (SofaScore)
    corners     córners de ESE equipo
    big_chances ocasiones claras de ESE equipo
    shot_xg_mean, shot_dist_mean, shot_setpiece_xg_pct, shot_xg_max
                forma de la distribución de remates de ESE equipo: gol esperado
                del disparo corriente, distancia media de tiro, parte del peligro
                que nace de balón parado y valor de la ocasión más clara
    shot_xg_mean_against, shot_dist_mean_against
                las dos mismas magnitudes vistas desde la portería propia: qué
                calidad de remate y a qué distancia concede ESE equipo
    points      3/1/0 según resultado
    result      "W"/"D"/"L"
    won         1 si ESE equipo ganó, 0 en otro caso (NaN si futuro)

Escalares PRE-PARTIDO que se arrastran para features derivadas de mercado/Elo
(NO son estadística del propio partido, se conocen antes del pitido):
    elo_z       fuerza del equipo antes del partido, TIPIFICADA dentro de la
                jornada: desviaciones respecto a la media de Primera ese día
    opp_elo_z   la misma lectura para el RIVAL (dificultad del encuentro)
    p_mkt_win   probabilidad implícita (des-margen) de que ESE equipo gane
    odds_win    cuota decimal (media de casas) de victoria de ESE equipo

Las dos columnas de Elo llegan tipificadas y no en nivel porque la serie
publicada de ClubElo empalma dos escalas (ver `consolidate/spine.add_clubelo`) y
solo la posición relativa dentro de la jornada es comparable a lo largo de todo
el histórico. Importa aquí y no solo en el máster porque `form.py` deriva de
`elo_z` una memoria a cinco partidos, y una resta entre dos momentos de la serie
en nivel no cancelaría la diferencia de escala si ambos cayesen a distinto lado
del empalme.

`opp_elo_z` es lo que convierte la secuencia de resultados en una secuencia
interpretable: doce puntos en cinco partidos no significan lo mismo contra los
cinco primeros que contra los cinco últimos. Al viajar en la fila del equipo, la
dificultad del calendario admite el mismo tratamiento de media móvil que
cualquier otro atributo, y permite contrastar cada resultado con lo que el Elo
esperaba de él ANTES de jugarse, sin usar información del propio partido.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import assert_columns, assert_unique  # noqa: E402

# columnas del máster imprescindibles (el resto son opcionales y se rellenan NaN)
_NEED = ["match_key", "season_year", "date", "round",
         "home_key", "away_key", "fthg", "ftag"]

# (nombre_propio, col_home, col_away) para atributos que existen en pares
# El lado "away" de un atributo defensivo toma el valor ofensivo del rival.
_PAIRED = [
    ("xg",          "home_xg",     "away_xg"),
    ("xga",         "away_xg",     "home_xg"),
    ("npxg",        "home_npxg",   "away_npxg"),
    ("npxga",       "away_npxg",   "home_npxg"),
    ("xpts",        "home_xpts",   "away_xpts"),
    ("ppda",        "home_ppda",   "away_ppda"),
    ("deep",        "home_deep",   "away_deep"),
    ("rating",      "home_sofa_rating", "away_sofa_rating"),
    ("shots",       "sofa_shots_total_shots_home",      "sofa_shots_total_shots_away"),
    ("sot",         "sofa_shots_shots_on_target_home",  "sofa_shots_shots_on_target_away"),
    ("corners",     "sofa_match_overview_corner_kicks_home", "sofa_match_overview_corner_kicks_away"),
    ("big_chances", "sofa_match_overview_big_chances_home",  "sofa_match_overview_big_chances_away"),
    # --- calidad de ocasión (`extractors/understat_shots.py`) ----------------
    # Cuatro magnitudes en versión ofensiva y dos en versión concedida. La
    # asimetría es deliberada. En ataque interesan las cuatro porque juntas
    # describen la forma de la distribución de tiros: cuánto vale el remate
    # corriente, desde dónde se dispara, cuánto peligro nace de estrategia y si
    # hubo alguna ocasión clara.
    ("shot_xg_mean",         "home_shot_xg_mean",         "away_shot_xg_mean"),
    ("shot_dist_mean",       "home_shot_dist_mean",       "away_shot_dist_mean"),
    ("shot_setpiece_xg_pct", "home_shot_setpiece_xg_pct", "away_shot_setpiece_xg_pct"),
    ("shot_xg_max",          "home_shot_xg_max",          "away_shot_xg_max"),
    # En defensa se conservan solo las dos que miden algo que ninguna otra
    # variable del proyecto mide. El gol esperado CONCEDIDO ya viaja en `xga`,
    # pero es un total y confunde al equipo que concede muchos remates inocuos
    # con el que concede pocos y claros: `shot_xg_mean_against` separa esos dos
    # perfiles. `shot_dist_mean_against` es la lectura territorial de lo mismo,
    # la distancia a la que el bloque obliga a rematar, que es el rasgo clásico
    # del equipo replegado y no se deduce de ningún recuento.
    #
    # Las otras dos no se cruzan: la cuota de estrategia concedida y el mejor
    # remate permitido son sucesos escasos por partido y, promediados sobre
    # ventanas de tres o cinco encuentros, aportan más ruido que señal —el mismo
    # criterio de parquedad con el que se eligieron las cuatro de origen—.
    ("shot_xg_mean_against",   "away_shot_xg_mean",   "home_shot_xg_mean"),
    ("shot_dist_mean_against", "away_shot_dist_mean", "home_shot_dist_mean"),
]

# Atributos on-pitch que alimentan la forma (además de gf/ga/gd/points).
#
# Los seis últimos describen la CALIDAD de la ocasión, no su recuento, y por eso
# se suman a una lista que ya contenía volumen (`shots`, `sot`) y peligro total
# (`xg`, `npxg`). Un equipo puede sostener un promedio alto de tiros y de xG
# acumulado con remates lejanos y forzados; el xG por remate y la distancia media
# distinguen ese perfil del que llega poco pero llega bien. Ninguna de las dos
# lecturas se deduce de la otra, y ambas son rasgos de estilo lo bastante
# estables como para que una ventana retrospectiva de tres o cinco encuentros
# tenga sentido.
FORM_ATTRS = ["gf", "ga", "gd", "points",
              "xg", "xga", "xgd", "npxg", "npxga", "xpts",
              "ppda", "deep", "rating",
              "shots", "sot", "corners", "big_chances",
              "shot_xg_mean", "shot_dist_mean", "shot_setpiece_xg_pct",
              "shot_xg_max",
              "shot_xg_mean_against", "shot_dist_mean_against"]


def _col(master: pd.DataFrame, name: str):
    """Devuelve la columna si existe, si no una Serie de NaN (futuro/fuente ausente)."""
    if name in master.columns:
        return master[name].astype("float64")
    return pd.Series(np.nan, index=master.index, dtype="float64")


def _points(gf, ga) -> "np.ndarray":
    """Puntos 3/1/0. NaN-safe: si no hay resultado (partido futuro), NaN.

    Importante para la ruta de inferencia: una fila de partido futuro tiene
    gf/ga NaN. Sin esta guarda, np.where la trataría como derrota (0 puntos) y
    ese 0 espurio contaminaría la ventana rolling de una jornada futura
    posterior. Con NaN, rolling().mean() la ignora (skipna).
    """
    gf = np.asarray(gf, dtype="float64")
    ga = np.asarray(ga, dtype="float64")
    pts = np.where(gf > ga, 3.0, np.where(gf == ga, 1.0, 0.0))
    pts[np.isnan(gf) | np.isnan(ga)] = np.nan
    return pts


def _side_frame(master: pd.DataFrame, side: str) -> pd.DataFrame:
    """Construye las filas de una perspectiva ('home' o 'away')."""
    is_home = side == "home"
    tk, ok = ("home_key", "away_key") if is_home else ("away_key", "home_key")
    gf_col, ga_col = ("fthg", "ftag") if is_home else ("ftag", "fthg")

    out = pd.DataFrame({
        "match_key": master["match_key"],
        "season_year": master["season_year"],
        "date": pd.to_datetime(master["date"]),
        "round": master["round"] if "round" in master else np.nan,
        "team_key": master[tk],
        "opp_key": master[ok],
        "is_home": 1 if is_home else 0,
        "gf": master[gf_col].astype("float64"),
        "ga": master[ga_col].astype("float64"),
    })
    for name, hcol, acol in _PAIRED:
        src = hcol if is_home else acol
        out[name] = _col(master, src)

    out["gd"] = out["gf"] - out["ga"]
    out["xgd"] = out["xg"] - out["xga"]
    out["points"] = _points(out["gf"].values, out["ga"].values).astype("float64")
    out["result"] = np.where(out["gf"] > out["ga"], "W",
                             np.where(out["gf"] == out["ga"], "D", "L"))
    out.loc[out["gf"].isna() | out["ga"].isna(), "result"] = pd.NA
    out["won"] = np.where(out["gf"] > out["ga"], 1.0, 0.0)
    out.loc[out["gf"].isna() | out["ga"].isna(), "won"] = np.nan

    # --- escalares PRE-PARTIDO (mercado / Elo), desde la perspectiva del equipo ---
    out["elo_z"] = _col(master, "home_elo_z" if is_home else "away_elo_z")
    out["opp_elo_z"] = _col(master, "away_elo_z" if is_home else "home_elo_z")
    out["p_mkt_win"] = _col(master, "mkt_p_home" if is_home else "mkt_p_away")
    # cuota decimal de victoria: media de casas, con fallback a Bet365
    odds = _col(master, "odds_avg_h" if is_home else "odds_avg_a")
    fb = _col(master, "odds_b365_h" if is_home else "odds_b365_a")
    out["odds_win"] = odds.where(odds.notna(), fb)
    return out


def to_team_long(master: pd.DataFrame) -> pd.DataFrame:
    """Máster (1 fila/partido) -> team-match long (2 filas/partido).

    Ordenado por (team_key, date) para que form.py pueda hacer rolling temporal.
    Clave única de la tabla: (match_key, team_key).
    """
    assert_columns(master, _NEED, "teamlong.master")
    long = pd.concat([_side_frame(master, "home"), _side_frame(master, "away")],
                     ignore_index=True)
    long = long.sort_values(["team_key", "date", "match_key"]).reset_index(drop=True)
    assert_unique(long, ["match_key", "team_key"], "teamlong")
    return long
