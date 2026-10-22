"""Validacion offline del reshape a TEAM-MATCH LONG (teamlong.py). Sin red.

Este modulo no produce ninguna feature: produce la PRIMITIVA sobre la que se
calculan la forma, el rendimiento relativo al mercado y el ajuste al rival. Por
eso un error suyo no se manifiesta donde se comete, sino repartido por tres
bloques distintos, y con la forma de un resultado plausible.

Los dos modos de fallo que este banco vigila son de esa clase. El primero es el
espejo defensivo: en la fila de un equipo, `xga` no es una columna del master
sino el `xg` de SU RIVAL, y lo mismo ocurre con `npxga` y con los goles en
contra. Si el espejo se invirtiera, cada equipo arrastraria su propio ataque como
si fuera su defensa y las medias moviles resultantes seguirian siendo numeros
razonables. El segundo es el tratamiento del partido por jugar: la fila de la
jornada futura llega sin marcador, y convertir esa ausencia en cero puntos —que
es lo que hace `np.where` sin guarda— introduciria una derrota inventada en la
ventana de las jornadas siguientes.

Se comprueba:

  1. FORMA        dos filas por partido, clave unica y esquema homogeneo.
  2. ESPEJO       lo defensivo de un lado es lo ofensivo del otro, sin excepcion.
  3. RESULTADO    puntos, etiqueta y victoria salen del marcador, y el partido
                  por jugar deja hueco en los tres en lugar de derrota.
  4. PRE-PARTIDO  fuerza tipificada propia y del rival, probabilidad implicita y
                  cuota viajan desde la perspectiva del equipo, con respaldo
                  cuando falta la media de casas.
  5. FUENTE       un master sin las columnas opcionales produce igualmente el
                  esquema completo, con hueco donde la fuente no llega.
  6. GUARDAS      falta de columna obligatoria y clave duplicada se detienen aqui.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
for _p in (_HERE, _EXTRACTORS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import teamlong as T  # noqa: E402
from _common import SchemaError  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []
_TOL = 1e-9


def check(cond: bool, msg: str) -> None:
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")


def _master(filas: list[dict]) -> pd.DataFrame:
    """Master minimo mas las columnas opcionales que se declaren en cada fila."""
    df = pd.DataFrame(filas)
    df["date"] = pd.to_datetime(df["date"])
    if "season_year" not in df:
        df["season_year"] = 2020
    if "round" not in df:
        df["round"] = np.nan
    df["match_key"] = (df["date"].dt.strftime("%Y-%m-%d") + "|"
                       + df["home_key"] + "|" + df["away_key"])
    return df


def _fila(long: pd.DataFrame, k: str, equipo: str) -> pd.Series:
    return long[(long["match_key"] == k) & (long["team_key"] == equipo)].iloc[0]


# --------------------------------------------------------------------------- #
# 1. FORMA DE LA TABLA
# --------------------------------------------------------------------------- #
def test_forma() -> None:
    print("\n[1] FORMA — dos filas por partido, clave unica, esquema homogeneo")
    m = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                  "fthg": 2.0, "ftag": 1.0},
                 {"date": "2020-08-22", "home_key": "ccc", "away_key": "aaa",
                  "fthg": 0.0, "ftag": 0.0}])
    long = T.to_team_long(m)

    check(len(long) == 2 * len(m), "cada partido genera exactamente dos filas")
    check(not long.duplicated(subset=["match_key", "team_key"]).any(),
          "la clave (match_key, team_key) es unica")
    check(set(long["is_home"]) == {0, 1} and long["is_home"].sum() == len(m),
          "un local y un visitante por partido")
    check(long.groupby("team_key")["date"].apply(lambda s: s.is_monotonic_increasing).all(),
          "las filas de cada equipo quedan ordenadas por fecha (base del shift)")
    check(all(a in long.columns for a in T.FORM_ATTRS),
          "el long publica todos los atributos declarados en FORM_ATTRS")
    check(long["opp_key"].tolist() != long["team_key"].tolist()
          and (long["opp_key"] != long["team_key"]).all(),
          "cada fila identifica al rival, que nunca es el propio equipo")


# --------------------------------------------------------------------------- #
# 2. ESPEJO DEFENSIVO
# --------------------------------------------------------------------------- #
def test_espejo() -> None:
    print("\n[2] ESPEJO — lo defensivo de un lado es lo ofensivo del otro")
    m = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                  "fthg": 3.0, "ftag": 1.0,
                  "home_xg": 2.4, "away_xg": 0.7,
                  "home_npxg": 1.6, "away_npxg": 0.7,
                  "home_deep": 12.0, "away_deep": 4.0,
                  "home_sofa_rating": 7.4, "away_sofa_rating": 6.1}])
    long = T.to_team_long(m)
    k = m["match_key"].iloc[0]
    h, a = _fila(long, k, "aaa"), _fila(long, k, "bbb")

    check(abs(h["gf"] - a["ga"]) < _TOL and abs(h["ga"] - a["gf"]) < _TOL,
          "los goles a favor de uno son los goles en contra del otro")
    check(abs(h["xga"] - a["xg"]) < _TOL and abs(a["xga"] - h["xg"]) < _TOL,
          "el xG concedido es el xG generado por el rival, no el propio")
    check(abs(h["npxga"] - a["npxg"]) < _TOL,
          "y lo mismo con el xG sin penaltis")
    check(abs(h["gd"] + a["gd"]) < _TOL and abs(h["xgd"] + a["xgd"]) < _TOL,
          "las dos diferencias del mismo partido son opuestas")
    check(abs(h["xgd"] - (2.4 - 0.7)) < _TOL,
          "xgd se calcula sobre la perspectiva ya espejada")
    check(abs(h["rating"] - 7.4) < _TOL and abs(a["rating"] - 6.1) < _TOL,
          "los atributos sin contrapartida defensiva no se cruzan")
    check(abs(h["deep"] - 12.0) < _TOL and abs(a["deep"] - 4.0) < _TOL,
          "cada equipo arrastra sus propios pases profundos")


# --------------------------------------------------------------------------- #
# 3. RESULTADO
# --------------------------------------------------------------------------- #
def test_resultado() -> None:
    print("\n[3] RESULTADO — puntos, etiqueta y victoria, con hueco si no se ha jugado")
    m = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                  "fthg": 2.0, "ftag": 0.0},
                 {"date": "2020-08-22", "home_key": "aaa", "away_key": "ccc",
                  "fthg": 1.0, "ftag": 1.0},
                 {"date": "2020-08-29", "home_key": "aaa", "away_key": "ddd",
                  "fthg": np.nan, "ftag": np.nan}])
    long = T.to_team_long(m)
    k0, k1, k2 = m["match_key"]

    v, p = _fila(long, k0, "aaa"), _fila(long, k0, "bbb")
    check(v["points"] == 3.0 and p["points"] == 0.0, "victoria y derrota valen 3 y 0")
    check(v["result"] == "W" and p["result"] == "L", "la etiqueta acompana al marcador")
    check(v["won"] == 1.0 and p["won"] == 0.0, "el indicador de victoria es binario")

    e = _fila(long, k1, "aaa")
    check(e["points"] == 1.0 and e["result"] == "D" and e["won"] == 0.0,
          "el empate vale un punto y no cuenta como victoria")

    f = _fila(long, k2, "aaa")
    check(pd.isna(f["points"]),
          "el partido por jugar deja los puntos en hueco, NO en cero: un cero "
          "entraria en la ventana movil como una derrota que no ha ocurrido")
    check(pd.isna(f["result"]) and pd.isna(f["won"]),
          "y deja igualmente en hueco la etiqueta y el indicador de victoria")
    check(pd.isna(f["gd"]), "sin marcador tampoco hay diferencia de goles")


# --------------------------------------------------------------------------- #
# 4. ESCALARES PRE-PARTIDO
# --------------------------------------------------------------------------- #
def test_prepartido() -> None:
    print("\n[4] PRE-PARTIDO — Elo, mercado y cuota desde la perspectiva del equipo")
    m = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                  "fthg": 1.0, "ftag": 0.0,
                  "home_elo_z": 1.8, "away_elo_z": -0.4,
                  "mkt_p_home": 0.72, "mkt_p_away": 0.12,
                  "odds_avg_h": 1.35, "odds_avg_a": 8.0}])
    long = T.to_team_long(m)
    k = m["match_key"].iloc[0]
    h, a = _fila(long, k, "aaa"), _fila(long, k, "bbb")

    # La fuerza llega TIPIFICADA dentro de la jornada, no en puntos de rating:
    # es la unica lectura comparable a traves del empalme de las dos escalas de
    # ClubElo (ver `consolidate/spine.add_clubelo`). Los valores del fixture son
    # desviaciones, de modo que un 1700 aqui seria una senal de que la capa de
    # consolidacion ha vuelto a publicar el nivel con el nombre tipificado.
    check(h["elo_z"] == 1.8 and h["opp_elo_z"] == -0.4,
          "el local lleva su fuerza y la del rival, sin cruzarlas")
    check(a["elo_z"] == -0.4 and a["opp_elo_z"] == 1.8,
          "y el visitante las lleva al reves")
    check(h["p_mkt_win"] == 0.72 and a["p_mkt_win"] == 0.12,
          "la probabilidad implicita es la de que gane ESE equipo")
    check(h["odds_win"] == 1.35 and a["odds_win"] == 8.0,
          "la cuota de victoria tambien se toma desde su perspectiva")

    # respaldo: sin media de casas, la cuota de Bet365 sostiene la columna en vez
    # de dejar sin senal a las temporadas donde la media no se publico.
    m2 = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                   "fthg": 1.0, "ftag": 0.0,
                   "odds_b365_h": 1.40, "odds_b365_a": 7.5}])
    long2 = T.to_team_long(m2)
    h2 = _fila(long2, m2["match_key"].iloc[0], "aaa")
    check(h2["odds_win"] == 1.40,
          "cuando falta la media de casas la cuota cae a Bet365")

    m3 = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                   "fthg": 1.0, "ftag": 0.0,
                   "odds_avg_h": 1.35, "odds_b365_h": 9.99}])
    h3 = _fila(T.to_team_long(m3), m3["match_key"].iloc[0], "aaa")
    check(h3["odds_win"] == 1.35,
          "y el respaldo no sustituye a la media cuando esta existe")


# --------------------------------------------------------------------------- #
# 5. FUENTE AUSENTE
# --------------------------------------------------------------------------- #
def test_fuente_ausente() -> None:
    print("\n[5] FUENTE — sin columnas opcionales, el esquema sigue completo")
    m = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                  "fthg": 2.0, "ftag": 1.0}])
    long = T.to_team_long(m)

    check(all(a in long.columns for a in T.FORM_ATTRS),
          "el esquema no depende de que la fuente haya llegado")
    # La calidad de ocasión se comprueba aquí junto al resto porque es la fuente
    # cuya ausencia es más plausible: nace de un volcado propio, de una petición
    # por partido, y el máster se publica igualmente sin ella.
    ausentes = ["xg", "npxg", "xpts", "ppda", "deep", "rating",
                "shots", "sot", "corners", "big_chances",
                "shot_xg_mean", "shot_dist_mean", "shot_setpiece_xg_pct",
                "shot_xg_max",
                "shot_xg_mean_against", "shot_dist_mean_against"]
    check(bool(long[ausentes].isna().all().all()),
          "lo que la fuente no cubre queda en hueco, no en cero")
    check(long[["gf", "ga", "gd", "points"]].notna().all().all(),
          "y lo que solo depende del marcador se calcula igualmente")
    check(str(long["xg"].dtype) == "float64",
          "la columna ausente conserva el tipo numerico (no se degrada a objeto)")


# --------------------------------------------------------------------------- #
# 6. GUARDAS
# --------------------------------------------------------------------------- #
def test_guardas() -> None:
    print("\n[6] GUARDAS — el reshape se detiene ante un master mal formado")
    m = _master([{"date": "2020-08-15", "home_key": "aaa", "away_key": "bbb",
                  "fthg": 1.0, "ftag": 0.0}])

    # Las dos guardas se expresan con la excepcion de esquema del proyecto
    # -`SchemaError`, la misma que emiten los extractores- y no con un `assert`
    # desnudo: `python -O` desactiva los `assert` y dejaria pasar en silencio un
    # master sin marcador o con partidos duplicados, que es justo lo que estas dos
    # comprobaciones existen para impedir. La prueba captura por tanto ese tipo.
    try:
        T.to_team_long(m.drop(columns=["fthg"]))
        check(False, "falta una columna obligatoria y el reshape se detiene")
    except SchemaError:
        check(True, "falta una columna obligatoria y el reshape se detiene")

    dup = pd.concat([m, m], ignore_index=True)
    try:
        T.to_team_long(dup)
        check(False, "un partido duplicado se detiene en vez de duplicar la forma")
    except SchemaError:
        check(True, "un partido duplicado se detiene en vez de duplicar la forma")


def _run() -> None:
    print("=" * 72)
    print("TEST — reshape a team-match long (primitiva de los bloques de forma)")
    print("=" * 72)
    test_forma()
    test_espejo()
    test_resultado()
    test_prepartido()
    test_fuente_ausente()
    test_guardas()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_teamlong() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
