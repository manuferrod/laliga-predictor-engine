"""Spine del master: football-data (resultado + cuotas) + xG + Elo.

El spine ancla el universo de partidos del master. Es la tabla
`footballdata_match.parquet` (1 fila/partido, ya con `match_key`, `home_key`,
`away_key`, target `ftr` y todas las cuotas), ya recortada a 16/17+ en la capa
procesada, y enriquecida con dos fuentes historicas keyed por partido:

  - Understat  -> `home_xg`/`away_xg` (+ npxg/ppda/deep/xpts)   (merge por match_key, fallback ±2d)
  - ClubElo    -> `home_elo`/`away_elo`/`elo_diff` en nivel y
                  `home_elo_z`/`away_elo_z` tipificados en la jornada
                                                                 (merge_asof: ultimo Elo <= fecha)

Notas de diseno (2026-07-13, tras el reprocesado de la capa procesada):
  - `understat_match.parquet` YA viene reducido a 1 fila/partido con `match_key`,
    `home_key`, `away_key`, `date` y las metricas `us_home_*`/`us_away_*`. No hay
    que reconstruir claves; solo mapear los nombres a `home_*`/`away_*`.
  - Aun asi se mantiene el fallback tolerante en fecha: las fechas de Understat
    y football-data pueden desfasar 1-2 dias en algunos partidos, con lo que el
    `match_key` exacto no casa. Con el exacto + fallback por (home_key, away_key)
    y |dias|<=2 la cobertura de xG queda ~99.7 %.
  - `clubelo_elo.parquet` es formato largo por intervalos (`from`/`to`, una fila
    por club y tramo de validez del Elo) con `team_key` YA normalizado. El Elo
    vigente el dia del partido se toma con merge_asof backward sobre `date`.
    Esa serie es el EMPALME de dos vias con escalas distintas, asi que lo que se
    publica para el modelo no es el nivel sino su tipificacion dentro de la
    jornada, que es invariante al cambio de escala (ver `add_clubelo`). El nivel
    sigue viajando, como columna descriptiva.
  - La jornada YA NO es una fuente propia (`wk`): vive en `sofascore.round`, que
    se une al master en la fase sofascore. Por eso el spine no la incorpora.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import assert_columns, assert_unique  # noqa: E402
from clubelo import ELO_CORTE  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"

FIRST_SEASON = 2016  # 16/17: primera temporada con la familia SofaScore completa
_TOL_DAYS = 2        # tolerancia de fecha para el fallback de understat
# Clubes minimos en la base de tipificacion del Elo. Primera tiene 20; por debajo
# de 18 la media de la jornada describe otra liga y la fuerza relativa que sale de
# ella no es comparable con la del resto de fechas.
_MIN_BASE = 18

# us_home_X / us_away_X  ->  home_X / away_X
_US_MAP = {
    "us_home_xg": "home_xg",     "us_away_xg": "away_xg",
    "us_home_npxg": "home_npxg", "us_away_npxg": "away_npxg",
    "us_home_ppda": "home_ppda", "us_away_ppda": "away_ppda",
    "us_home_deep": "home_deep", "us_away_deep": "away_deep",
    "us_home_xpts": "home_xpts", "us_away_xpts": "away_xpts",
}


# --------------------------------------------------------------------------- #
# merge keyed-por-partido con fallback tolerante en fecha
# --------------------------------------------------------------------------- #
def _prekeyed_tolerant(spine: pd.DataFrame, src: pd.DataFrame,
                       value_cols: list[str], tol: int = _TOL_DAYS) -> pd.DataFrame:
    """Une `src` (que YA trae match_key/home_key/away_key/date) al spine.

    1) merge exacto por `match_key`.
    2) para los partidos que quedan sin casar (desfase de fecha), busca en `src`
       una fila con el mismo (home_key, away_key) y |dias| <= tol, y toma la mas
       cercana. Devuelve solo `spine[['match_key'] + value_cols]`.
    """
    s = src[["match_key", "home_key", "away_key", "date"] + value_cols].copy()
    s["_dsrc"] = pd.to_datetime(s["date"].astype("string").str.slice(0, 10),
                                errors="coerce")

    # 1) exacto por match_key
    exact = s.drop_duplicates("match_key").set_index("match_key")[value_cols]
    out = spine[["match_key"]].join(exact, on="match_key")

    # 2) fallback por (home_key, away_key) con tolerancia de fecha
    miss = out[value_cols].isna().all(axis=1)
    if miss.any():
        sp = spine.loc[miss, ["match_key", "home_key", "away_key", "date"]].copy()
        sp["_dsp"] = pd.to_datetime(sp["date"].astype("string").str.slice(0, 10),
                                    errors="coerce")
        cand = sp.merge(s[["home_key", "away_key", "_dsrc"] + value_cols],
                        on=["home_key", "away_key"], how="left")
        cand["_gap"] = (cand["_dsp"] - cand["_dsrc"]).abs().dt.days
        cand = (cand[cand["_gap"] <= tol]
                .sort_values("_gap").drop_duplicates("match_key"))
        if not cand.empty:
            fill = cand.set_index("match_key")[value_cols]
            out = out.set_index("match_key")
            out.loc[fill.index, value_cols] = fill
            out = out.reset_index()
    return out


# --------------------------------------------------------------------------- #
# fuentes
# --------------------------------------------------------------------------- #
def add_understat(spine: pd.DataFrame, us: pd.DataFrame) -> pd.DataFrame:
    """Anade xG y derivados de Understat (`home_*`/`away_*`), left, tolerante ±2d."""
    src_cols = list(_US_MAP)
    assert_columns(us, ["match_key", "home_key", "away_key", "date"] + src_cols,
                   "understat")
    got = _prekeyed_tolerant(spine, us, src_cols)
    got = got.rename(columns=_US_MAP)
    return spine.merge(got, on="match_key", how="left")


def censo_por_temporada(df: pd.DataFrame) -> dict[int, list[str]]:
    """temporada -> clubes de Primera de esa temporada, segun el propio universo.

    La pertenencia a Primera se lee de los partidos, no de la clasificacion que
    publica ClubElo. Es una decision, no una comodidad: en la via web el `level`
    es el VIGENTE del club y se propaga hacia atras a toda su serie, asi que 1.693
    tramos lo traen nulo y hay veinte fechas con menos de veinte clubes de nivel 1
    (minimo medido: cero). El universo de partidos, en cambio, es exacto por
    construccion —quien juega la liga la juega— y es el mismo en las dos vias, lo
    que mantiene la base de tipificacion estable a traves del empalme.
    """
    assert_columns(df, ["season_year", "home_key", "away_key"], "spine.censo")
    largo = pd.concat([
        df[["season_year", lado]].rename(columns={lado: "team_key"})
        for lado in ("home_key", "away_key")], ignore_index=True)
    largo = largo.dropna(subset=["season_year", "team_key"])
    largo["season_year"] = pd.to_numeric(largo["season_year"],
                                         errors="coerce").astype("Int64")
    return {int(s): sorted(g.astype(str).unique())
            for s, g in largo.groupby("season_year")["team_key"]}


def add_clubelo(spine: pd.DataFrame, elo: pd.DataFrame, *,
                censo: dict[int, list[str]] | None = None,
                min_base: int = _MIN_BASE) -> pd.DataFrame:
    """Anade el Elo del dia del partido, en nivel y TIPIFICADO dentro de la jornada.

    Emite seis columnas:
      home_elo, away_elo, elo_diff   el rating en nivel, tal como lo publica la
                                     fuente. Son columnas descriptivas: viajan en
                                     el master para poder auditar y mostrar el
                                     numero, pero no entran al modelo.
      home_elo_z, away_elo_z         la fuerza RELATIVA: cuantas desviaciones
                                     separan al club de la media de la liga ese
                                     dia. Es lo que consume el catalogo.
      elo_via, elo_base_n            de que via sale el Elo de esa fecha y con
                                     cuantos clubes se tipifico. Rastro de
                                     auditoria del empalme.

    Por que tipificado y no en nivel
    --------------------------------
    La serie publicada es el empalme de dos vias que miden lo mismo en escalas
    distintas (ver `clubelo.assert_una_escala`): la web va ~85 puntos por encima
    del archivo de la API y comprime el rango un 11%. En nivel, el empalme seria
    un escalon de fuerza simultaneo para los veinte clubes; el modelo lo leeria
    como un acontecimiento deportivo. Restar la media de la jornada y dividir por
    su desviacion deshace exactamente esa transformacion afin, de modo que la
    cifra que ve el modelo significa lo mismo antes y despues del corte.

    Medido sobre estas mismas columnas y sobre los veinte clubes que cruzan el
    corte dentro de 2022/23, el salto de fuerza tipificada entre el partido
    anterior y el primero posterior al empalme queda en 0.073 desviaciones de
    mediana, 0.170 en el p90 y 0.194 en el peor caso (Sevilla). Una semana
    corriente de competicion mueve esa misma cifra 0.048 de mediana, 0.110 en el
    p90, 0.180 en el p99 y hasta 0.343 en el extremo (n=7.380): el empalme es
    indistinguible de una jornada normal. En nivel crudo seria un escalon de ~85
    puntos simultaneo para los veinte. Quien vigila ese presupuesto partido a
    partido es `qa_master`.

    La tipificacion vive aqui y no en el extractor porque la base son los clubes
    que juegan Primera en cada temporada, y eso solo lo sabe la capa que tiene el
    universo de partidos delante. La base se puede imponer con `censo` —asi la
    ruta de inferencia usa el censo de la temporada completa y no solo los dos
    equipos de los partidos que va a puntuar, que daria una tipificacion distinta
    de la del entrenamiento.
    """
    assert_columns(elo, ["team_key", "elo", "date"], "clubelo")
    assert_columns(spine, ["match_key", "home_key", "away_key", "date",
                           "season_year"], "spine.add_clubelo")
    e = elo.copy()
    e["_d"] = pd.to_datetime(e["date"].astype("string").str.slice(0, 10),
                             errors="coerce")
    e = e.dropna(subset=["_d"])
    cols_e = ["_d", "team_key", "elo"] + (["fuente"] if "fuente" in e else [])
    e = e[cols_e].sort_values("_d")

    sp = spine[["match_key", "home_key", "away_key", "date",
                "season_year"]].copy()
    sp["_d"] = pd.to_datetime(sp["date"].astype("string").str.slice(0, 10),
                              errors="coerce")
    sp["season_year"] = pd.to_numeric(sp["season_year"],
                                      errors="coerce").astype("Int64")
    censo = censo if censo is not None else censo_por_temporada(sp)

    # rejilla (temporada x fecha de partido x clubes de la temporada): el Elo de
    # TODOS los clubes de la liga en cada fecha en la que se juega, que es la base
    # sobre la que se tipifica. No basta con los equipos del partido.
    fechas = sp[["season_year", "_d"]].dropna().drop_duplicates()
    miembros = pd.DataFrame(
        [(int(s), k) for s, ks in censo.items() for k in ks],
        columns=["season_year", "team_key"])
    miembros["season_year"] = miembros["season_year"].astype("Int64")
    base = fechas.merge(miembros, on="season_year", how="inner").sort_values("_d")
    base = pd.merge_asof(base, e, on="_d", by="team_key", direction="backward")

    _assert_base_sana(base, min_base=min_base)

    mom = base.groupby(["season_year", "_d"])["elo"].agg(
        _mu="mean", _sd=lambda s: float(s.std(ddof=0)), _n="size").reset_index()
    base = base.merge(mom, on=["season_year", "_d"], how="left")
    base["elo_z"] = (base["elo"] - base["_mu"]) / base["_sd"]

    llaves = ["season_year", "_d", "team_key"]
    z = base.set_index(llaves)["elo_z"]
    nivel = base.set_index(llaves)["elo"]

    add = sp[["match_key", "season_year", "_d"]].copy()
    for lado, clave in (("home", "home_key"), ("away", "away_key")):
        idx = pd.MultiIndex.from_arrays(
            [sp["season_year"], sp["_d"], sp[clave].astype(str)])
        add[f"{lado}_elo"] = nivel.reindex(idx).to_numpy()
        add[f"{lado}_elo_z"] = z.reindex(idx).to_numpy()
    add["elo_diff"] = add["home_elo"] - add["away_elo"]

    aud = base.drop_duplicates(subset=["season_year", "_d"]).set_index(
        ["season_year", "_d"])
    pares = pd.MultiIndex.from_arrays([add["season_year"], add["_d"]])
    add["elo_base_n"] = aud["_n"].reindex(pares).to_numpy()
    add["elo_via"] = (aud["fuente"].reindex(pares).to_numpy()
                      if "fuente" in aud.columns else "api")

    add = add.drop(columns=["season_year", "_d"])
    return spine.merge(add, on="match_key", how="left")


def _assert_base_sana(base: pd.DataFrame, *, min_base: int) -> None:
    """La base de tipificacion tiene que estar completa, ser ancha y de una via.

    Son tres fallos que de otro modo pasarian inadvertidos, porque los tres
    producen un numero plausible:

    1. Un club de la liga SIN Elo en una fecha deja la media y la desviacion de
       ese dia calculadas sobre menos equipos, de modo que la fuerza relativa de
       todos los partidos de esa jornada cambia de significado.
    2. Una base estrecha hace lo mismo de forma menos visible.
    3. Dos vias conviviendo en una misma fecha mezclaria las dos escalas DENTRO
       de la media, que es justo lo que la tipificacion pretende evitar. Se exige
       ademas que la via sea la que toca segun la fecha: `merge_asof` cruza por
       `from` y no mira `to`, asi que un club sin cobertura web heredaria en
       silencio su ultimo Elo del archivo. Esta es la asercion que lo impide.
    """
    huecos = base[base["elo"].isna()]
    if len(huecos):
        muestra = ", ".join(
            f"{r.team_key}@{r._d.date()}" for r in huecos.head(5).itertuples())
        raise AssertionError(
            f"[spine.add_clubelo] {len(huecos)} de {len(base)} celdas de la base "
            f"de tipificacion sin Elo ({muestra}). La media y la desviacion de "
            "esas fechas saldrian de una liga incompleta y la fuerza relativa de "
            "sus partidos no seria comparable con el resto.")

    n = base.groupby(["season_year", "_d"]).size()
    if (n < min_base).any():
        flaca = n[n < min_base]
        raise AssertionError(
            f"[spine.add_clubelo] {len(flaca)} fecha(s) con base de tipificacion "
            f"de menos de {min_base} clubes (minimo {int(flaca.min())}, p.ej. "
            f"{flaca.index[0]}). Revisa el censo de la temporada.")

    sd = base.groupby(["season_year", "_d"])["elo"].std(ddof=0)
    if (sd <= 0).any():
        raise AssertionError(
            f"[spine.add_clubelo] {int((sd <= 0).sum())} fecha(s) con desviacion "
            "nula en la base: la tipificacion seria una division por cero.")

    if "fuente" not in base.columns:
        return
    vias = base.groupby(["season_year", "_d"])["fuente"].nunique()
    if (vias > 1).any():
        raise AssertionError(
            f"[spine.add_clubelo] {int((vias > 1).sum())} fecha(s) con dos vias "
            "de Elo en la misma base de tipificacion: la media mezclaria las dos "
            "escalas. La serie empalmada tiene que resolver cada fecha por una "
            "sola via (ver `clubelo.serie_empalmada`).")
    corte = pd.Timestamp(ELO_CORTE)
    mal = base[(base["_d"] >= corte) & (base["fuente"] != "web")]
    if len(mal):
        clubes = sorted(mal["team_key"].astype(str).unique())[:5]
        raise AssertionError(
            f"[spine.add_clubelo] {len(mal)} celda(s) en/despues del corte "
            f"{ELO_CORTE} resueltas por el archivo de la API en vez de por la "
            f"web (p.ej. {clubes}). `merge_asof` cruza por `from` y no mira `to`: "
            "esos clubes estan arrastrando un Elo de 2022 en la escala antigua. "
            "Re-ejecuta `clubelo.py weekly` para completar la cobertura web.")


# --------------------------------------------------------------------------- #
# orquestacion del spine
# --------------------------------------------------------------------------- #
def build_spine(fd: pd.DataFrame, us: pd.DataFrame, elo: pd.DataFrame,
                *, first_season: int = FIRST_SEASON) -> pd.DataFrame:
    """football-data (16/17+) + xG + Elo, keyed por match_key."""
    assert_columns(fd, ["match_key", "season_year", "home_key", "away_key",
                        "date", "ftr"], "footballdata")
    spine = fd[fd["season_year"] >= first_season].copy().reset_index(drop=True)
    assert_unique(spine, ["match_key"], "spine.base")

    spine = add_understat(spine, us)
    spine = add_clubelo(spine, elo)

    assert_unique(spine, ["match_key"], "spine.final")
    return spine


# --------------------------------------------------------------------------- #
# self-check offline:  python3 scripts/consolidate/spine.py
# --------------------------------------------------------------------------- #
def _selfcheck() -> None:
    fd = pd.read_parquet(PROC / "footballdata_match.parquet")
    us = pd.read_parquet(PROC / "understat_match.parquet")
    elo = pd.read_parquet(PROC / "clubelo_elo.parquet")

    spine = build_spine(fd, us, elo)

    print("=" * 70)
    print("SELF-CHECK spine (offline) —", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 70)
    print(f"partidos={len(spine)}  cols={spine.shape[1]}  match_key unico={spine['match_key'].is_unique}")
    seas = sorted(spine["season"].dropna().unique())
    print(f"temporadas: {seas[:2]} ... {seas[-2:]}")

    def cov(col):
        return 1 - spine[col].isna().mean()
    print("\nCobertura por fuente (no-nulos):")
    print(f"  home_xg  : {cov('home_xg'):6.1%}   away_xg : {cov('away_xg'):6.1%}")
    print(f"  home_elo : {cov('home_elo'):6.1%}   away_elo: {cov('away_elo'):6.1%}")
    print(f"  home_elo_z: {cov('home_elo_z'):5.1%}  away_elo_z: {cov('away_elo_z'):5.1%}")

    assert spine["match_key"].is_unique, "spine: match_key no unico"
    assert cov("home_elo") > 0.99 and cov("away_elo") > 0.99, "Elo con baja cobertura"
    assert cov("home_elo_z") > 0.99, "Elo tipificado con baja cobertura"
    assert cov("home_xg") > 0.98, "xG con baja cobertura"

    print("\nEmpalme de vias y base de tipificacion:")
    print(spine.groupby("elo_via")["elo_base_n"].agg(
        partidos="size", base_min="min", base_max="max").to_string())
    print(f"  corte declarado: {ELO_CORTE}")

    print("\nMuestra:")
    cols = ["match_key", "season", "ftr", "home_xg", "away_xg",
            "home_elo", "away_elo", "elo_diff", "home_elo_z", "away_elo_z",
            "elo_via"]
    print(spine[cols].head(4).to_string())
    print("\nOK: spine 16/17+ con xG/Elo (nivel y tipificado), 1 fila por partido.")


if __name__ == "__main__":
    _selfcheck()
