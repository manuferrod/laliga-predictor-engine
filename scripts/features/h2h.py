"""Features HEAD-TO-HEAD (rivalidad histórica) leak-safe.

Mide cómo le ha ido históricamente a un equipo contra un rival CONCRETO, no
contra el campo. Se construye un long DIRIGIDO: por cada partido, la fila del
par (equipo, rival) desde la perspectiva del equipo. Se agrupa por ese par
ordenado por fecha y se calculan tasas con decaimiento exponencial (EWM, da más
peso a los enfrentamientos recientes) usando shift(1) para no ver el actual.

La memoria cruza temporadas a propósito: la historia de un clásico no se reinicia
cada agosto. Si no hay enfrentamientos previos, las tasas se rellenan neutras
(1/3, 1/3, 1/3) y la dif. de goles h2h queda NaN (se imputa en 04).

Qué se emite lo declaran `catalogo.H2H_ATRIBUTOS` y `catalogo.H2H_PARTIDO`, y son
tres columnas. Dos miden al EQUIPO y salen como diferencia entre los dos: la tasa
de victoria y la diferencia de goles. La tercera, la tasa de empate, mide al PAR
—un empate lo es para los dos a la vez, así que ambos lados llevan el mismo
número— y por eso sale una sola vez, sin rótulo de lado; restarla daría una
columna de ceros. La tasa de derrota no se construye porque es uno menos las
otras dos, y la ventana rectangular de ocho enfrentamientos tampoco: responde a
la misma pregunta que la exponencial con otro supuesto de olvido, y en un
historial donde la muestra es de uno o dos partidos por temporada ambas apenas se
distinguen. Las tasas se calculan igualmente en el long —son el insumo de las
tres— pero solo sale a la matriz lo declarado.
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

from _common import assert_unique  # noqa: E402

import catalogo  # noqa: E402

_HALFLIFE = catalogo.H2H_SEMIVIDA

_NEED = ["match_key", "date", "home_key", "away_key", "ftr", "fthg", "ftag"]


def _side(master: pd.DataFrame, is_home: bool) -> pd.DataFrame:
    tk, ok = ("home_key", "away_key") if is_home else ("away_key", "home_key")
    ftr = master["ftr"].astype("string")
    if is_home:
        win = (ftr == "H"); loss = (ftr == "A")
        gd = master["fthg"].astype("float64") - master["ftag"].astype("float64")
    else:
        win = (ftr == "A"); loss = (ftr == "H")
        gd = master["ftag"].astype("float64") - master["fthg"].astype("float64")
    draw = (ftr == "D")
    played = ftr.notna()

    def _ind(mask):
        v = mask.astype("float64")
        v[~played] = np.nan
        return v

    return pd.DataFrame({
        "match_key": master["match_key"],
        "date": pd.to_datetime(master["date"]),
        "is_home": 1 if is_home else 0,
        "team_key": master[tk],
        "opp_key": master[ok],
        "win": _ind(win), "draw": _ind(draw), "loss": _ind(loss),
        "gd": gd.where(played),
    })


def _add_h2h(long: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    long = long.sort_values(["team_key", "opp_key", "date", "match_key"]).reset_index(drop=True)
    g = long.groupby(["team_key", "opp_key"], sort=False)
    feats: list[str] = []

    for col in ("win", "draw"):
        c = f"{col}_rate_ewm"
        long[c] = g[col].transform(
            lambda s: s.shift(1).ewm(halflife=_HALFLIFE, adjust=False, min_periods=1).mean())
        feats.append(c)
    long["gd_ewm"] = g["gd"].transform(
        lambda s: s.shift(1).ewm(halflife=_HALFLIFE, adjust=False, min_periods=1).mean())
    feats.append("gd_ewm")

    # relleno neutral para las tasas sin historial previo (gd se deja NaN)
    for c in feats:
        if c.startswith(("win_rate", "draw_rate", "loss_rate")):
            long[c] = long[c].fillna(1 / 3)
    return long, feats


def _to_wide(long: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    """Long dirigido -> fila de partido, con los lados que declara el catálogo."""
    keep = ["match_key", "is_home"] + feats
    home = (long.loc[long["is_home"] == 1, keep].drop(columns="is_home")
                .rename(columns={c: f"home_h2h_{c}" for c in feats}))
    away = (long.loc[long["is_home"] == 0, keep].drop(columns="is_home")
                .rename(columns={c: f"away_h2h_{c}" for c in feats}))
    wide = home.merge(away, on="match_key", how="outer")
    for c in feats:
        if catalogo.emite(c, "diff"):
            wide[f"h2h_{c}_diff"] = wide[f"home_h2h_{c}"] - wide[f"away_h2h_{c}"]
        if catalogo.emite(c, "partido"):
            # magnitud del par: los dos lados llevan el mismo valor, así que se
            # toma el del local y se comprueba que efectivamente coinciden. Si
            # dejaran de hacerlo el agrupamiento estaría mezclando pares —una
            # clave de club sin canonizar— y la columna dejaría de describir el
            # enfrentamiento que dice describir.
            h, a = wide[f"home_h2h_{c}"], wide[f"away_h2h_{c}"]
            err = float((h - a).abs().max())
            if err > 1e-9:
                raise AssertionError(
                    f"h2h_{c} se declara como magnitud del enfrentamiento pero "
                    f"difiere entre lados hasta {err:.2e}: revise la "
                    f"canonización de claves de club, que es lo que define el par")
            wide[f"h2h_{c}"] = h
    sobran = [f"{lado}_h2h_{c}" for c in feats for lado in ("home", "away")
              if not catalogo.emite(c, lado)]
    return wide.drop(columns=sobran)


def h2h_history(master: pd.DataFrame) -> pd.DataFrame:
    """Long DIRIGIDO: dos filas por partido, la del par (equipo, rival) y su espejo.

    Se expone aparte porque la fila de partido ya no lo contiene: de las dos
    magnitudes que miden al equipo el catálogo emite solo la diferencia, y de una
    resta no se recupera el historial de ninguno de los dos. Este frame es donde
    la media exponencial se calcula y donde la identidad que da sentido al bloque
    —que las dos filas dirigidas describen el mismo historial visto desde lados
    opuestos— es siquiera formulable.
    """
    long = pd.concat([_side(master, True), _side(master, False)], ignore_index=True)
    long, _ = _add_h2h(long)
    return long


def build_h2h_features(master: pd.DataFrame) -> pd.DataFrame:
    """Máster -> features H2H, keyed por match_key."""
    long = pd.concat([_side(master, True), _side(master, False)], ignore_index=True)
    long, feats = _add_h2h(long)
    wide = _to_wide(long, feats).reset_index(drop=True)
    assert_unique(wide, ["match_key"], "h2h")
    return wide


def h2h_feature_names() -> list[str]:
    """Atributos base del h2h; los nombres de columna salen de `catalogo.nombres`.

    Van los dos grupos en una sola lista —los que se miden por equipo y el que se
    mide por enfrentamiento— porque quien la consume pregunta después al catálogo
    en qué lados se emite cada uno, y esa pregunta ya distingue los dos casos.
    """
    return list(catalogo.H2H_ATRIBUTOS) + list(catalogo.H2H_PARTIDO)
