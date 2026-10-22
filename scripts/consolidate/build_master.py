"""Orquestador del MASTER de partido (LaLiga 1X2), 16/17+.

Une, sobre el spine (football-data + xG + Elo), todas las fuentes ya reducidas a
"1 fila por partido" (clave `match_key`):

    spine  (footballdata_match + understat + clubelo)
      + sofascore   (ratings + estadistica de partido + jornada `round` + sello
                     de saque `kickoff_ts`, del que sale el contexto de calendario)
      + fatiga      (descanso/congestion multi-competicion, home/away)
      + disponibilidad (conteos de bajas, home/away)
      + squad_value (valor de plantilla por temporada, home/away)
      + bajas €     (valor economico Y cuota de minutos de los ausentes)
      + alineaciones (rotacion del once y carga de minutos, home/away)
      + calidad de ocasion (forma de la distribucion de tiros, home/away)

Todo por left-merge sobre `match_key`, garantizando 1 fila por partido. Escribe
`data/02_processed/master_matches.parquet` de forma atomica.

Las tres ultimas fuentes proceden de extractores cuyo volcado inicial es largo y
se hace una sola vez —`lineups.py` pide una alineacion por partido y
`understat_shots.py` un detalle de tiro por partido—. Por eso se leen como
OPCIONALES: si el artefacto no esta, el master se construye sin ese bloque en
lugar de fallar. Lo que ocurre entonces aguas abajo esta previsto y es limpio:
las columnas del bloque quedan enteras en NaN, la poda por constancia de
`features/preprocess.py` las descarta antes de imputar nada, y el modelo se
ajusta sin ellas en lugar de recibir un bloque completo relleno con la mediana.
Ninguna otra parte de la cadena cambia.

Conviene subrayar la diferencia de NATURALEZA entre esos dos bloques opcionales,
porque su tratamiento aguas abajo es opuesto. La rotacion viene ya calculada
mirando hacia atras y por tanto es informacion pre-partido que pasa tal cual al
conjunto de variables. La calidad de ocasion, en cambio, describe lo que ocurrio
en ESTE partido: es materia prima, viaja al master como tal y solo se convierte
en variable a traves de las ventanas retrospectivas de `features/form.py`, igual
que el xG o los goles. `features/columns.py` la declara como fuga precisamente
para que ninguna via la deje entrar cruda.

Uso:
    python scripts/consolidate/build_master.py          # construye y guarda
    python scripts/consolidate/build_master.py --dry    # construye y solo reporta
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import assert_unique, write_df_required  # noqa: E402

from spine import build_spine  # noqa: E402
from reducers import (  # noqa: E402
    event_to_match_key, reduce_sofascore, reduce_fatigue, reduce_availability,
    reduce_lineups, reduce_shot_quality)
from squadvalue import reduce_squad_value  # noqa: E402
from absences import build_absences  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
RAW = _ROOT / "data" / "01_raw"
OUT = PROC / "master_matches.parquet"


def _load() -> dict:
    return dict(
        fd=pd.read_parquet(PROC / "footballdata_match.parquet"),
        us=pd.read_parquet(PROC / "understat_match.parquet"),
        elo=pd.read_parquet(PROC / "clubelo_elo.parquet"),
        sofa=pd.read_parquet(PROC / "sofascore_team_match.parquet"),
        fat=pd.read_parquet(PROC / "team_fatigue.parquet"),
        av=pd.read_parquet(PROC / "team_availability.parquet"),
        tv=pd.read_parquet(PROC / "team_squad_value.parquet"),
        pv=pd.read_parquet(PROC / "transfermarkt_player_values.parquet"),
        missing=pd.read_csv(RAW / "sofascore" / "sofascore_missing_players.csv"),
        lin=_opcional(PROC / "team_lineups.parquet"),
        pm=_opcional(PROC / "player_minutes.parquet"),
        shot=_opcional(PROC / "team_shot_quality.parquet"),
    )


def _opcional(path: Path) -> pd.DataFrame | None:
    """Lee un parquet que puede no existir todavia.

    Los artefactos de alineaciones y minutos (`extractors/lineups.py`) y el de
    calidad de ocasion (`extractors/understat_shots.py`) nacen de backfills
    largos y propios, de una peticion por partido. El master se construye igual
    sin ellos --con los bloques correspondientes en NaN, que la poda por
    constancia del preprocesador elimina-- para que la cadena no dependa de haber
    ejecutado esos volcados.
    """
    return pd.read_parquet(path) if path.exists() else None


def _left(master: pd.DataFrame, part: pd.DataFrame, name: str,
          drop: list[str] | None = None) -> pd.DataFrame:
    """Left-merge por match_key evitando colisiones de columnas."""
    part = part.drop(columns=[c for c in (drop or []) if c in part.columns])
    dup = (set(part.columns) & set(master.columns)) - {"match_key"}
    if dup:
        part = part.drop(columns=list(dup))
    return master.merge(part, on="match_key", how="left")


def build_master(data: dict | None = None) -> pd.DataFrame:
    """Construye el master en memoria (no escribe)."""
    d = data or _load()

    spine = build_spine(d["fd"], d["us"], d["elo"])
    # `date` viaja en la meta porque las bajas ponderadas por minutos necesitan un
    # instante de corte (00:00 UTC del dia del partido) para evaluar la cuota.
    meta = spine[["match_key", "season_year", "home_key", "away_key", "date"]].rename(
        columns={"season_year": "season"})
    e2k = event_to_match_key(d["sofa"])

    r_sofa = reduce_sofascore(d["sofa"])
    r_fat = reduce_fatigue(d["fat"], e2k)
    r_av = reduce_availability(d["av"], e2k)
    r_sv = reduce_squad_value(meta, d["tv"])
    r_abs = build_absences(d["missing"], d["pv"], meta, e2k, d["tv"],
                           player_minutes=d.get("pm"))
    r_lin = reduce_lineups(d["lin"], e2k) if d.get("lin") is not None else None
    r_shot = (reduce_shot_quality(d["shot"]) if d.get("shot") is not None
              else None)

    master = spine
    # de sofascore: quitar lo que ya trae el spine (season/date); conservar match_id/round/sofa_*
    master = _left(master, r_sofa, "sofascore", drop=["season", "date"])
    master = _left(master, r_fat, "fatigue")
    master = _left(master, r_av, "availability")
    master = _left(master, r_sv, "squad_value")
    master = _left(master, r_abs, "absences")
    if r_lin is not None:
        master = _left(master, r_lin, "lineups")
    if r_shot is not None:
        master = _left(master, r_shot, "shot_quality")

    assert_unique(master, ["match_key"], "master")
    return master.reset_index(drop=True)


def _report(m: pd.DataFrame) -> None:
    seas = sorted(m["season"].dropna().unique())
    print("=" * 70)
    print("MASTER —", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 70)
    print(f"partidos={len(m)}  columnas={m.shape[1]}  match_key unico={m['match_key'].is_unique}")
    print(f"temporadas: {seas[0]} .. {seas[-1]}  ({len(seas)} temporadas)")
    fam = {
        "target/cuotas (fd)": [c for c in m if c in ("ftr",) or c.startswith("odds_") or c.startswith("mkt_")],
        "xG (understat)": [c for c in m if c.endswith("_xg")],
        "Elo (clubelo)": [c for c in m if "elo" in c],
        "sofascore": [c for c in m if c.startswith("sofa_") or c.endswith("_sofa_rating")],
        "calendario (saque)": [c for c in m if c == "kickoff_ts"],
        "fatiga": [c for c in m if "_fat_" in c],
        "bajas conteo": [c for c in m if c.startswith(("home_n_", "away_n_"))],
        "valor plantilla": [c for c in m if "squad_value" in c or "avg_age" in c or c.endswith("squad_size")],
        "bajas €": [c for c in m if "out_value" in c or c.endswith("_out_n")],
        "bajas por rol": [c for c in m if "out_minshare" in c],
        "rotacion y minutos": [c for c in m if "xi_continuity" in c
                               or "starters_minutes" in c
                               or "minutes_hhi" in c or "n_players_used" in c],
        # Materia prima, no features: se cuenta aparte para que el recuento no
        # sugiera que estas columnas alimentan el modelo tal cual.
        "calidad de ocasion*": [c for c in m if "_shot_" in c],
    }
    print("\nBloques de features:")
    for k, v in fam.items():
        print(f"  {k:22s}: {len(v):>3} cols")
    print("  (*) materia prima: no entra al modelo tal cual, solo via forma.")


def main(save: bool = True) -> pd.DataFrame:
    m = build_master()
    _report(m)
    if save:
        write_df_required(m, OUT, fmt="parquet", etiqueta="build_master")
        print(f"\n[OK] master -> {OUT} ({len(m)} filas, {m.shape[1]} cols)")
    else:
        print("\n[DRY] no se ha escrito nada.")
    return m


if __name__ == "__main__":
    main(save="--dry" not in sys.argv)
