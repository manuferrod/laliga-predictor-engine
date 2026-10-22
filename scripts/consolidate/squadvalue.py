"""Valor de plantilla por partido (sustituye al estatico de 402 filas).

`team_squad_value.parquet` es club-temporada (`season, team_key, squad_value,
squad_size, value_avg_mio, avg_age`). Aqui se cruza contra los equipos de cada
partido del spine para producir `home_*` / `away_*` + diferencias, keyed por
match_key. Es el "prior de fuerza" economico previo al partido.

LAG t-1 (anti-fuga): el valor de plantilla de Transfermarkt de una temporada se
consolida DURANTE esa temporada (fichajes de invierno, revalorizaciones por
rendimiento), por lo que el valor de la temporada `s` NO se conoce integramente
antes del primer partido de `s`. Para garantizar que la feature es estrictamente
pre-partido, cada partido de la temporada `s` recibe el valor de la temporada
`s-1` (ya cerrado). Para que la primera temporada del universo de partidos
(2016/17) no quede sin prior, el extractor de Transfermarkt baja una temporada
extra (`_VALUE_FIRST_SEASON = FIRST_SEASON - 1`). El unico NaN estructural que
queda es el de los ascendidos, que no tienen valor de `s-1` en LaLiga
(cold-start); se resuelve aguas abajo en preprocess.py (imputacion +
flag_no_squadvalue), no aqui.
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

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"

_TV_STATS = ["squad_value", "squad_size", "value_avg_mio", "avg_age"]


def reduce_squad_value(match_meta: pd.DataFrame, tv: pd.DataFrame,
                       lag: int = 1) -> pd.DataFrame:
    """`home_*` / `away_*` de valor de plantilla + diferencias, por match_key.

    `match_meta`: columnas [match_key, season, home_key, away_key] (season int).
    `tv`        : team_squad_value (club-temporada).
    `lag`       : temporadas de desfase anti-fuga. Con `lag=1` (por defecto) un
                  partido de la temporada `s` toma el valor de plantilla de la
                  temporada `s-1`. `lag=0` recupera el comportamiento antiguo
                  (mismo-temporada, no recomendado por fuga temporal).
    """
    assert_columns(match_meta, ["match_key", "season", "home_key", "away_key"],
                   "squad_value.meta")
    assert_columns(tv, ["season", "team_key"] + _TV_STATS, "team_squad_value")
    assert_unique(tv, ["season", "team_key"], "team_squad_value")

    val = tv[["season", "team_key"] + _TV_STATS].copy()
    # desfase: el valor de la temporada `s-lag` se alinea con los partidos de `s`
    val["season"] = val["season"] + lag
    out = match_meta[["match_key", "season", "home_key", "away_key"]].copy()

    home = val.add_prefix("home_").rename(columns={"home_season": "season",
                                                   "home_team_key": "home_key"})
    away = val.add_prefix("away_").rename(columns={"away_season": "season",
                                                   "away_team_key": "away_key"})
    out = out.merge(home, on=["season", "home_key"], how="left")
    out = out.merge(away, on=["season", "away_key"], how="left")

    out["squad_value_diff"] = out["home_squad_value"] - out["away_squad_value"]
    # ratio log-simetrico (robusto): >0 favorece al local
    hv = out["home_squad_value"].clip(lower=0.1)
    av = out["away_squad_value"].clip(lower=0.1)
    out["squad_value_logratio"] = np.log(hv / av)
    out["age_diff"] = out["home_avg_age"] - out["away_avg_age"]

    keep = ["match_key",
            "home_squad_value", "away_squad_value", "squad_value_diff",
            "squad_value_logratio",
            "home_avg_age", "away_avg_age", "age_diff",
            "home_squad_size", "away_squad_size"]
    out = out.loc[:, keep]
    assert_unique(out, ["match_key"], "squad_value.reduced")
    return out.reset_index(drop=True)


def _selfcheck() -> None:
    import spine as sp  # noqa
    fd = pd.read_parquet(PROC / "footballdata_match.parquet")
    us = pd.read_parquet(PROC / "understat_match.parquet")
    elo = pd.read_parquet(PROC / "clubelo_elo.parquet")
    spine = sp.build_spine(fd, us, elo)
    meta = spine[["match_key", "season_year", "home_key", "away_key"]].rename(
        columns={"season_year": "season"})

    tv = pd.read_parquet(PROC / "team_squad_value.parquet")

    r = reduce_squad_value(meta, tv)
    print("=" * 70)
    print("SELF-CHECK squad_value (offline, lag t-1)")
    print("=" * 70)
    print(f"filas={len(r)}  match_key unico={r['match_key'].is_unique}")
    cov = 1 - r["home_squad_value"].isna().mean()
    covA = 1 - r["away_squad_value"].isna().mean()
    print(f"cobertura home_squad_value={cov:.1%}  away_squad_value={covA:.1%}")

    # cobertura por temporada: con la temporada extra de Transfermarkt, el unico
    # NaN esperado es el de los ascendidos (sin valor de s-1 en LaLiga).
    m2 = meta.merge(r[["match_key", "home_squad_value", "away_squad_value"]],
                    on="match_key", how="left")
    print("\ncobertura por temporada (home / away):")
    for s, g in m2.groupby("season"):
        ch = 1 - g["home_squad_value"].isna().mean()
        ca = 1 - g["away_squad_value"].isna().mean()
        print(f"  {s}: {ch:5.1%} / {ca:5.1%}  (n={len(g)})")
    print(r.head(4).to_string())


if __name__ == "__main__":
    _selfcheck()
