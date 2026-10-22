"""Features de forma derivadas del MERCADO y de estados psicológicos.

Todas leak-safe (shift(1)) y calculadas sobre el team-match long, agrupando por
equipo y ordenando por fecha. A diferencia de form.py, aquí la memoria cruza el
corte de temporada a propósito (EWM con vida media), porque el rendimiento
relativo al mercado y los "arrastres" psicológicos no se reinician en agosto.

Features:
  * relative_perf : victorias reales (EWM) / victorias esperadas por cuota (EWM).
      >1 el equipo rinde por encima de lo que dicta el mercado; <1 por debajo.
      Señal de reversión a la media respecto a la "opinión agregada" de las casas.
      Sale como diferencia entre los dos equipos.
  * post_big_win  : 1 si el partido ANTERIOR fue una victoria inesperada
      (cuota de victoria > 4.0, ~<25% implícita). Hipótesis: bajón posterior.
      Sale como par, porque la resta de dos indicadores binarios confunde el
      caso en que ninguno viene de una sorpresa con aquel en que vienen los dos.

El compañero natural de `post_big_win` —la derrota del gran favorito, con la
hipótesis simétrica de resaca— no se construye: `catalogo` no lo declara porque
su información sobre el resultado es del orden del ruido de muestreo.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import catalogo
from teamlong import to_team_long

_HALFLIFE = 6
_MINP_EWM = 3
_BIG_WIN_ODDS = 4.0


def _add_market_features(long: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    long = long.sort_values(["team_key", "date", "match_key"]).reset_index(drop=True)
    feats: list[str] = []

    g = long.groupby("team_key", sort=False)
    real = g["won"].transform(
        lambda s: s.shift(1).ewm(halflife=_HALFLIFE, adjust=False, min_periods=_MINP_EWM).mean())
    exp = g["p_mkt_win"].transform(
        lambda s: s.shift(1).ewm(halflife=_HALFLIFE, adjust=False, min_periods=_MINP_EWM).mean())
    long["relative_perf"] = real / exp.replace(0, np.nan)
    feats.append("relative_perf")

    # indicador del propio partido (materia prima; se desplaza para no filtrar)
    big_win = ((long["won"] == 1.0) & (long["odds_win"] > _BIG_WIN_ODDS)).astype("float64")
    big_win[long["won"].isna()] = np.nan
    long["_big_win"] = big_win

    g = long.groupby("team_key", sort=False)
    long["post_big_win"] = g["_big_win"].shift(1)
    feats.append("post_big_win")

    return long, feats


def _to_wide(long: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    """Long por equipo -> fila de partido, con los lados que declara el catálogo."""
    keep = ["match_key", "is_home"] + feats
    home = (long.loc[long["is_home"] == 1, keep].drop(columns="is_home")
                .rename(columns={c: f"home_{c}" for c in feats}))
    away = (long.loc[long["is_home"] == 0, keep].drop(columns="is_home")
                .rename(columns={c: f"away_{c}" for c in feats}))
    wide = home.merge(away, on="match_key", how="outer")
    for c in feats:
        if catalogo.emite(c, "diff"):
            wide[f"{c}_diff"] = wide[f"home_{c}"] - wide[f"away_{c}"]
    sobran = [f"{lado}_{c}" for c in feats for lado in ("home", "away")
              if not catalogo.emite(c, lado)]
    return wide.drop(columns=sobran)


def market_form_history(master: pd.DataFrame) -> pd.DataFrame:
    """Trayectoria POR EQUIPO: dos filas por partido, con las señales calculadas.

    Se expone aparte porque la fila de partido ya no la contiene entera:
    `relative_perf` viaja solo como diferencia y de una resta no se recupera el
    rendimiento de ninguno de los dos equipos. Este frame es el sitio donde esa
    ratio realmente se calcula, y por tanto el único desde el que se puede
    comprobar que vale lo que dice valer.
    """
    long, _ = _add_market_features(to_team_long(master))
    return long


def build_market_form_features(master: pd.DataFrame) -> pd.DataFrame:
    """Máster -> features de mercado/psicológicas, keyed por match_key."""
    long = to_team_long(master)
    long, feats = _add_market_features(long)
    return _to_wide(long, feats).reset_index(drop=True)


def market_form_feature_names() -> list[str]:
    """Atributos base; los nombres de columna salen de `catalogo.nombres`."""
    return list(catalogo.PSICOLOGICAS_DIFERENCIA) + list(catalogo.PSICOLOGICAS_PAR)
