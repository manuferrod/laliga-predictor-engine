"""Bajas ponderadas: cuanto vale y cuanto juega el futbolista que falta.

`team_availability` (procesado) solo trae CONTEOS de bajas, y un conteo trata
igual la ausencia del delantero titular y la del tercer portero. Este modulo pone
peso a cada baja por dos vias complementarias, que responden a preguntas
distintas y por eso viajan las dos:

  VALOR DE MERCADO -- cuanto vale el jugador que falta. Es una medida de calidad
  absoluta, comparable entre clubes y estable a lo largo de la temporada, pero
  ciega al papel que el jugador tiene AHORA: un fichaje caro que no juega pesa
  igual que el titular indiscutible.

  CUOTA DE MINUTOS -- que fraccion de los minutos posibles ha disputado el
  jugador con su equipo en lo que va de temporada (`extractors/lineups.py`). Es
  una medida de rol, relativa al propio equipo, que capta justo lo que el valor
  ignora: si el ausente es titular fijo o rotacion. A cambio no distingue calidad
  --el titular fijo de un colista tiene cuota 1 igual que el de un campeon-- y
  por eso no sustituye al valor, lo complementa.

Ambas se cruzan sobre el RAW `sofascore_missing_players.csv`, que es el unico
sitio donde consta QUIEN falta (el procesado solo cuenta cuantos), y se agregan a
nivel partido:

  home_out_n / away_out_n                  -> nº de ausentes
  home_out_value / away_out_value          -> suma de valor (M€) de los ausentes
  home_out_value_pct / away_out_value_pct  -> % sobre el valor de plantilla
  home_out_minshare / away_out_minshare    -> suma de cuotas de minutos perdidas
  home_out_minshare_max / away_..._max     -> la baja mas importante del bloque
  *_diff                                   -> local - visitante

CONVIVEN DOS ESPACIOS DE CLAVE DE JUGADOR, y conviene no confundirlos:

  1. Para el VALOR se normaliza el nombre igual que `transfermarkt._player_key`
     (deaccent + minusculas + guiones) y se cruza con `player_values.player_key`.
     Es un puente por nombre, aproximado por naturaleza: lo que no casa aporta
     0 € y se reporta la cobertura del cruce.
  2. Para la CUOTA se usa `player_slug`, el identificador que Sofascore asigna al
     jugador. `player_minutes.player_key` es ese mismo slug, asi que el cruce es
     exacto y no depende de como se escriba el nombre. Solo si el crudo no
     trajera slug se recurre a la normalizacion por nombre como respaldo.

SUMA Y MAXIMO
-------------
La suma de cuotas responde a "cuanto once le falta al equipo" y crece con el
numero de bajas; el maximo responde a "de que tamano es la peor baja" y no se
diluye cuando ademas faltan tres suplentes. Un equipo sin su titular fijo y otro
con cuatro rotaciones fuera pueden compartir suma y no parecerse en nada, de modo
que las dos agregaciones se publican y es el modelo quien decide.

Cuando la fuente de minutos no esta disponible --o cuando el equipo aun no ha
jugado bastante en la temporada para que la cuota tenga denominador-- el bloque
de cuotas queda en NaN, hueco honesto que `build_preprocess.py` marca e imputa,
nunca en cero: cero significaria "el ausente no juega nunca", que es una
afirmacion muy distinta de "no se sabe".

LAG t-1 (anti-fuga, coherente con squadvalue.py)
------------------------------------------------
Los valores de Transfermarkt de una temporada `s` se consolidan DURANTE `s`
(fichajes de invierno, revalorizaciones por rendimiento), asi que no se conocen
integramente antes de un partido de `s`. Igual que el prior de fuerza economica
(`consolidate/squadvalue.py`), aqui se toma la foto de la temporada `s-1`, ya
cerrada, tanto para el NUMERADOR (valor de los ausentes) como para el DENOMINADOR
(valor de plantilla). Usar la misma referencia temporal en ambos es lo que hace que
`out_value_pct` sea interpretable: es la fraccion del valor de plantilla conocido
antes de la temporada que se pierde por bajas.

El cruce de jugador se hace en dos niveles, para no penalizar los traspasos:

  1. exacto     (s-1, team_key, player_key)  el jugador ya estaba en ese club
  2. respaldo   (s-1, player_key)            cambio de club: se conserva su valor

En el nivel 2 se toma el valor maximo si el jugador aparece en varias fichas de esa
temporada (traspaso a mitad de curso). Los debutantes sin ficha en `s-1` aportan
0 €: es un sesgo conocido y conservador (subestima la baja), preferible a inyectar
un valor posterior al partido.

EL INSTANTE DE CORTE DE LA CUOTA DE MINUTOS
-------------------------------------------
La cuota no admite un lag de temporada como el valor: es una magnitud que se
construye dentro del curso y que solo tiene sentido leida al dia. El equivalente
anti-fuga aqui es el instante de corte: la cuota de cada ausente se evalua a las
00:00 UTC del dia del partido, momento que es posterior a todos los encuentros
anteriores y anterior a cualquier saque de esa jornada. Asi el numerador solo
contiene minutos ya disputados y la cuota es conocible con dias de antelacion, lo
mismo en el master historico que en la ruta de inferencia.

Ese corte tiene una consecuencia deseable. Mientras dura una ausencia el jugador
no suma minutos, pero su equipo sigue jugando: el numerador se congela y el
denominador crece, de modo que el peso de una baja de larga duracion decae solo,
sin regla ad hoc. Un equipo que lleva dos meses sin un futbolista ya no lo echa
de menos como el primer dia, y la feature lo refleja.

En las primeras jornadas de cada temporada el denominador es cero --el equipo
todavia no ha jugado-- y la cuota es NaN. Es el limite honesto del indicador y no
se rellena con la temporada anterior: los minutos de un curso pasado, con otra
plantilla y otro entrenador, no dicen que rol tiene hoy el ausente.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

import teamnames as tn  # noqa: E402
from _common import assert_columns  # noqa: E402
from lineups import minutes_share_at  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
RAW = _ROOT / "data" / "01_raw"

# Sub-bloque de cuota de minutos: se nombra aparte porque es el unico que puede
# quedar entero en NaN (falta la fuente de minutos) sin que falle nada mas.
MINSHARE_COLS = ["home_out_minshare", "away_out_minshare", "out_minshare_diff",
                 "home_out_minshare_max", "away_out_minshare_max"]

# Esquema de salida (contrato con columns._ABSENCES y con la ruta de inferencia).
ABSENCE_COLS = ["match_key",
                "home_out_n", "away_out_n",
                "home_out_value", "away_out_value", "out_value_diff",
                "home_out_value_pct", "away_out_value_pct", "out_value_pct_diff"
                ] + MINSHARE_COLS


def _player_key(name) -> str:
    """Igual que transfermarkt._player_key: deaccent + minusculas + guiones."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = tn._deaccent(str(name)).lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def _corte_ts(date_col: pd.Series) -> pd.Series:
    """Instante de corte de la cuota: 00:00 UTC del dia del partido, en epoch.

    Se toma la medianoche y no la hora del saque por dos razones. La primera es
    que basta: ningun partido de LaLiga empieza antes de las 00:00 UTC de su
    propio dia ni ningun partido anterior termina despues, asi que el corte
    separa exactamente el pasado del presente. La segunda es que el sello de
    saque no siempre esta disponible en la ruta de inferencia --un fixture
    lejano puede tener hora por confirmar-- mientras que la fecha si, de modo
    que el mismo criterio vale para el master historico y para la prediccion.
    """
    d = pd.to_datetime(date_col.astype("string").str.slice(0, 10),
                       format="%Y-%m-%d", errors="coerce")
    return (d - pd.Timestamp("1970-01-01")).dt.total_seconds()


def build_absences(missing: pd.DataFrame, player_values: pd.DataFrame,
                   match_meta: pd.DataFrame, event2key: pd.Series,
                   tv: pd.DataFrame, lag: int = 1,
                   player_minutes: pd.DataFrame | None = None) -> pd.DataFrame:
    """Tabla de bajas ponderadas por valor y por rol, keyed por match_key.

    `missing`     : raw sofascore_missing_players (event_id, side, player; y
                    `player_slug` si esta, que es la clave exacta de minutos).
    `player_values`: transfermarkt_player_values (season, team_key, player_key, value_mio).
    `match_meta`  : [match_key, season, home_key, away_key, date] del spine.
    `event2key`   : Series match_id -> match_key (de la tabla canonica sofascore).
    `tv`          : team_squad_value (para el % sobre plantilla).
    `lag`         : temporadas de desfase anti-fuga en los valores economicos. Con
                    `lag=1` (por defecto) un partido de la temporada `s` se valora
                    con la foto de `s-1`, igual que en `squadvalue.py`. `lag=0`
                    recupera el comportamiento mismo-temporada (no recomendado:
                    introduce informacion posterior al partido).
    `player_minutes`: tabla jugador-partido de `extractors/lineups.py`. Si no se
                    pasa, el sub-bloque de cuota de minutos sale entero en NaN y
                    el resto de la tabla se calcula igual: el modulo no depende
                    de que la fuente de minutos exista.
    """
    assert_columns(missing, ["event_id", "side", "player"], "missing_players")
    assert_columns(player_values, ["season", "team_key", "player_key", "value_mio"],
                   "player_values")
    assert_columns(match_meta, ["match_key", "season", "home_key", "away_key", "date"],
                   "absences.meta")

    cols_in = ["event_id", "side", "player"]
    if "player_slug" in missing.columns:
        cols_in.append("player_slug")
    m = missing[cols_in].copy()
    if not set(m["side"].unique()) <= {"home", "away"}:
        raise ValueError(f"missing_players: 'side' inesperado: {m['side'].unique()}")
    m["match_key"] = m["event_id"].map(event2key)
    m = m.dropna(subset=["match_key"])
    m["player_key"] = m["player"].map(_player_key)
    # clave de MINUTOS (espacio sofascore), distinta de la clave de VALOR
    # (espacio transfermarkt, por nombre normalizado): ver docstring del modulo.
    m["minutes_key"] = (m["player_slug"].astype("string")
                        if "player_slug" in m.columns else pd.Series(pd.NA, index=m.index))
    m["minutes_key"] = m["minutes_key"].fillna(pd.Series(m["player_key"].to_numpy(),
                                                         index=m.index, dtype="string"))

    # partido -> temporada + equipos + fecha
    m = m.merge(match_meta, on="match_key", how="left").dropna(subset=["season"])
    m["season"] = m["season"].astype(int)
    m["team_key"] = np.where(m["side"] == "home", m["home_key"], m["away_key"])

    # valor por jugador en la temporada de referencia `s-lag` (0 si no casa).
    # El desfase se aplica desplazando la temporada del catalogo de valores, no la
    # del partido, para no tocar las claves del spine.
    pv = player_values[["season", "team_key", "player_key", "value_mio"]].copy()
    pv["season"] = pv["season"].astype(int) + lag
    pv = pv.dropna(subset=["player_key"])

    # nivel 1: el jugador ya estaba en ese club en la temporada de referencia
    pv_team = pv.drop_duplicates(["season", "team_key", "player_key"])
    m = m.merge(pv_team, on=["season", "team_key", "player_key"], how="left")

    # nivel 2 (respaldo): cambio de club entre `s-lag` y `s`. Se conserva el valor
    # que el jugador tenia en su club anterior; con varias fichas esa temporada
    # (traspaso a mitad de curso) se toma el maximo.
    pv_any = (pv.groupby(["season", "player_key"], as_index=False)["value_mio"]
                .max().rename(columns={"value_mio": "_value_any"}))
    m = m.merge(pv_any, on=["season", "player_key"], how="left")
    m["_matched_team"] = m["value_mio"].notna()
    m["value_mio"] = m["value_mio"].fillna(m["_value_any"])
    m["_matched"] = m["value_mio"].notna()
    m["value_mio"] = m["value_mio"].fillna(0.0)

    if m.empty:
        # Ningun partido del universo tiene parte de bajas publicado. Ocurre de
        # forma legitima en inferencia, cuando se puntua antes de que sofascore
        # publique alineaciones. Se devuelve el bloque con NaN ("sin dato", que
        # preprocess.py traduce a flag_no_absences) en vez de fabricar ceros.
        res = pd.DataFrame({"match_key": match_meta["match_key"].to_numpy()})
        for c in ABSENCE_COLS[1:]:
            res[c] = np.nan
        res.attrs.update({"lag": lag, "n_absences": 0,
                          "match_rate_same_team": 0.0, "match_rate_total": 0.0,
                          "minshare_rate": np.nan})
        return res

    # temporadas con dato de bajas (missing_players empieza en 17/18). Fuera de
    # este universo, las bajas quedan NaN (no 0 fabricado) -> "sin dato".
    covered_seasons = set(m["season"].unique())

    # ---- peso por ROL: cuota de minutos del ausente a las 00:00 UTC del partido.
    # El ausente, por definicion, NO figura en la alineacion del encuentro que se
    # esta valorando, asi que no basta con leer la cuota que `player_minutes`
    # publica junto a cada fila jugador-partido: hay que EVALUARLA en un instante
    # en el que ese jugador no aparece. Eso es exactamente lo que ofrece
    # `lineups.minutes_share_at`.
    minshare_seasons: set[int] = set()
    hay_minutos = player_minutes is not None and not player_minutes.empty
    if hay_minutos:
        assert_columns(player_minutes,
                       ["team_key", "season", "player_key", "start_ts", "minutes_played"],
                       "player_minutes")
        pm = player_minutes.copy()
        pm["season"] = pd.to_numeric(pm["season"], errors="coerce")
        minshare_seasons = set(pm["season"].dropna().astype(int).unique())
        q = pd.DataFrame({"team_key": m["team_key"].to_numpy(),
                          "season": m["season"].astype(float).to_numpy(),
                          "player_key": m["minutes_key"].astype(str).to_numpy(),
                          "start_ts": _corte_ts(m["date"]).to_numpy()},
                         index=m.index)
        m["minutes_share"] = minutes_share_at(pm, q)["minutes_share"].to_numpy()
    else:
        m["minutes_share"] = np.nan
    m["_share_known"] = m["minutes_share"].notna()

    # agregacion por partido-lado
    agg = (m.groupby(["match_key", "side"])
             .agg(out_n=("player_key", "size"),
                  out_value=("value_mio", "sum"),
                  out_matched=("_matched", "sum"),
                  out_minshare=("minutes_share", "sum"),
                  out_minshare_max=("minutes_share", "max"),
                  out_share_known=("_share_known", "sum"))
             .reset_index())
    piv = agg.pivot_table(index="match_key", columns="side",
                          values=["out_n", "out_value", "out_matched",
                                  "out_minshare", "out_minshare_max",
                                  "out_share_known"], aggfunc="first")
    piv.columns = [f"{side}_{stat}" for stat, side in piv.columns]
    piv = piv.reset_index()

    # % sobre valor de plantilla, con el MISMO desfase que el numerador: si el
    # valor de los ausentes es la foto de `s-lag`, el total tambien lo es.
    tvv = tv[["season", "team_key", "squad_value"]].copy()
    tvv["season"] = tvv["season"].astype(int) + lag
    base = match_meta[["match_key", "season", "home_key", "away_key"]].copy()
    base["season"] = base["season"].astype(int)
    base = base.merge(tvv.rename(columns={"team_key": "home_key",
                                          "squad_value": "home_sv"}),
                      on=["season", "home_key"], how="left")
    base = base.merge(tvv.rename(columns={"team_key": "away_key",
                                          "squad_value": "away_sv"}),
                      on=["season", "away_key"], how="left")

    out = base[["match_key", "season", "home_sv", "away_sv"]].merge(
        piv, on="match_key", how="left")
    in_universe = out["season"].isin(covered_seasons)
    # El sub-bloque de cuotas tiene su PROPIO universo: el de las temporadas con
    # minutos descargados. Un partido puede tener parte de bajas y no tener
    # minutos (o al reves), y cada bloque debe declarar su hueco por separado.
    in_minshare = in_universe & out["season"].isin(minshare_seasons)
    for side in ("home", "away"):
        for c in (f"{side}_out_n", f"{side}_out_value", f"{side}_out_matched"):
            if c not in out:
                out[c] = np.nan
            # dentro del universo: NaN de un partido sin filas = 0 bajas reales;
            # fuera del universo (16/17): se deja NaN = sin dato.
            out.loc[in_universe, c] = out.loc[in_universe, c].fillna(0.0)
        sv = out[f"{side}_sv"].replace(0, np.nan)
        out[f"{side}_out_value_pct"] = out[f"{side}_out_value"] / sv

        for c in (f"{side}_out_minshare", f"{side}_out_minshare_max",
                  f"{side}_out_share_known"):
            if c not in out:
                out[c] = np.nan
        # Un partido sin bajas de ese lado pierde cuota cero: eso SI es un cero
        # real y se rellena, siempre que la temporada tenga minutos.
        sin_bajas = out[f"{side}_out_n"] == 0
        for c in (f"{side}_out_minshare", f"{side}_out_minshare_max"):
            out.loc[in_minshare & sin_bajas, c] = out.loc[in_minshare & sin_bajas, c].fillna(0.0)
        # Con bajas pero sin ninguna cuota conocida --jornada 1, o equipo cuyos
        # minutos no constan-- la suma que devuelve pandas es 0.0 y mentiria: no
        # es que no se pierda nada, es que no se sabe cuanto. Se restituye NaN.
        ciego = (~in_minshare) | (~sin_bajas & (out[f"{side}_out_share_known"].fillna(0) == 0))
        out.loc[ciego, [f"{side}_out_minshare", f"{side}_out_minshare_max"]] = np.nan

    out["out_value_diff"] = out["home_out_value"] - out["away_out_value"]
    out["out_value_pct_diff"] = out["home_out_value_pct"] - out["away_out_value_pct"]
    # Solo la SUMA viaja como diferencia: es la magnitud aditiva, la que admite
    # leerse como "cuanto once de ventaja tiene un equipo sobre el otro". Restar
    # dos maximos compararia la peor baja de cada lado, un contraste entre dos
    # jugadores concretos que dice poco del equilibrio del partido.
    out["out_minshare_diff"] = out["home_out_minshare"] - out["away_out_minshare"]

    res = out.loc[:, ABSENCE_COLS].reset_index(drop=True)
    # trazabilidad de los dos cruces, para poder reportarla sin re-calcularla:
    # que fraccion de bajas recibio valor en el nivel 1 (mismo club) y en total,
    # y que fraccion recibio cuota de minutos.
    res.attrs["lag"] = lag
    res.attrs["n_absences"] = len(m)
    res.attrs["match_rate_same_team"] = float(m["_matched_team"].mean()) if len(m) else 0.0
    res.attrs["match_rate_total"] = float(m["_matched"].mean()) if len(m) else 0.0
    # La cobertura de cuota distingue dos situaciones que no deben confundirse: si
    # la tabla de minutos existe, "0.0" significa que ninguna baja recibio cuota
    # --un cruce que falla y hay que investigar--; si no existe, la cobertura no
    # es cero sino indefinida, porque la medicion no llego a intentarse.
    res.attrs["minshare_rate"] = (float(m["_share_known"].mean())
                                  if (hay_minutos and len(m)) else np.nan)
    return res


def _selfcheck() -> None:
    import spine as sp
    from reducers import event_to_match_key

    fd = pd.read_parquet(PROC / "footballdata_match.parquet")
    us = pd.read_parquet(PROC / "understat_match.parquet")
    elo = pd.read_parquet(PROC / "clubelo_elo.parquet")
    sofa = pd.read_parquet(PROC / "sofascore_team_match.parquet")
    missing = pd.read_csv(RAW / "sofascore" / "sofascore_missing_players.csv")

    pv = pd.read_parquet(PROC / "transfermarkt_player_values.parquet")
    tv = pd.read_parquet(PROC / "team_squad_value.parquet")

    spine = sp.build_spine(fd, us, elo)
    meta = spine[["match_key", "season_year", "home_key", "away_key", "date"]].rename(
        columns={"season_year": "season"})
    e2k = event_to_match_key(sofa)

    # la fuente de minutos es opcional: si el extractor de alineaciones todavia no
    # se ha volcado, el resto del modulo se comprueba igual.
    pm_path = PROC / "player_minutes.parquet"
    pm = pd.read_parquet(pm_path) if pm_path.exists() else None

    r = build_absences(missing, pv, meta, e2k, tv, player_minutes=pm)
    print("=" * 70)
    print("SELF-CHECK bajas ponderadas (offline, lag t-1)")
    print("=" * 70)
    print(f"filas={len(r)}  match_key unico={r['match_key'].is_unique}")
    print(f"bajas cruzadas: {r.attrs['n_absences']} ausencias  |  "
          f"con valor mismo club (s-{r.attrs['lag']}): "
          f"{r.attrs['match_rate_same_team']:.1%}  |  "
          f"con valor incl. traspasos: {r.attrs['match_rate_total']:.1%}")
    print(f"cobertura (partidos con dato de bajas): {r['match_key'].isin(set(spine['match_key'])).mean():.1%}")
    print(f"partidos con >=1 baja local: {(r['home_out_n']>0).sum()}  visitante: {(r['away_out_n']>0).sum()}")
    print(f"valor medio bajas local: {r['home_out_value'].mean():.1f} M€  | "
          f"% medio sobre plantilla: {r['home_out_value_pct'].mean()*100:.1f}%")
    if pm is None:
        print("cuota de minutos     : sin fuente (player_minutes.parquet no volcado) "
              "-> sub-bloque en NaN, como esta previsto")
    else:
        print(f"cuota de minutos     : {r.attrs['minshare_rate']:.1%} de las bajas con "
              f"cuota conocida  |  cobertura del bloque: "
              f"{r['home_out_minshare'].notna().mean():.1%} de partidos")
        print(f"cuota media perdida (local): {r['home_out_minshare'].mean():.2f} onces  |  "
              f"peor baja media: {r['home_out_minshare_max'].mean():.2f}")
    print(r.sort_values('home_out_value', ascending=False).head(4).to_string())


if __name__ == "__main__":
    _selfcheck()
