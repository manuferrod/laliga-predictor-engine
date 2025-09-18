# scripts/make_b365_template.py
from __future__ import annotations
import argparse, os
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path.cwd()
DATA = ROOT / "data" / "02_processed"
MANUAL = ROOT / "manual"
MANUAL.mkdir(parents=True, exist_ok=True)

CANDIDATE_PARQUETS = [
    DATA / "df_new_features.parquet",
    DATA / "df_clean_vars.parquet",
    ROOT / "df_final.parquet",  # por si lo usas así
]

def load_df() -> pd.DataFrame:
    for p in CANDIDATE_PARQUETS:
        if p.exists():
            df = pd.read_parquet(p)
            print(f"[i] Cargado: {p} ({len(df):,} filas, {df.shape[1]} cols)")
            return df
    raise SystemExit("No encontré ningún parquet de trabajo. Revisa rutas en CANDIDATE_PARQUETS.")

def make_b365_template(df: pd.DataFrame, n_tail: int, out_csv: Path) -> pd.DataFrame:
    # asegura columnas base
    for c in ["B365H","B365D","B365A"]:
        if c not in df.columns:
            df[c] = np.nan

    order_idx = pd.to_datetime(df["Date"], errors="coerce").argsort(kind="mergesort")
    tail_idx = df.iloc[order_idx].tail(n_tail).index

    # tomamos solo las filas del “tail” con B365* totalmente NaN
    na_mask = df.loc[tail_idx, ["B365H","B365D","B365A"]].isna().all(axis=1)
    target = df.loc[tail_idx[na_mask], ["Date","HomeTeam_norm","AwayTeam_norm"]].copy()

    # formateo y row_id para poder reinyectar después
    target["Date"] = pd.to_datetime(target["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    target.insert(0, "row_id", target.index.astype(int))
    target["B365H"] = np.nan
    target["B365D"] = np.nan
    target["B365A"] = np.nan

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    target.to_csv(out_csv, index=False)
    print(f"[✓] Plantilla guardada: {out_csv} ({len(target)} filas)")
    return target

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", dest="run_date", required=True,
                    help="Fecha de referencia YYYY-MM-DD (p.ej. 2025-09-15)")
    args = ap.parse_args()

    df = load_df()

    d = pd.to_datetime(df["Date"], errors="coerce")
    mask_future = d >= pd.to_datetime(args.run_date)
    mask_nan = df[["B365H","B365D","B365A"]].isna().all(axis=1)
    n_tail = int(df[mask_future & mask_nan].shape[0])

    if n_tail == 0:
        recent = df.sort_values("Date").tail(30)
        n_tail = int(recent[["B365H","B365D","B365A"]].isna().all(axis=1).sum())

    n_tail = max(1, min(n_tail, 12))

    out_csv = MANUAL / f"b365_template_{args.run_date}.csv"
    make_b365_template(df, n_tail=n_tail, out_csv=out_csv)

if __name__ == "__main__":
    main()
