# scripts/build_cumprofit_curves_from_matchlogs.py
from pathlib import Path
import json
import pandas as pd
import numpy as np
import re
import sys

BASE = Path("outputs")
ML_MODEL_DIR = BASE / "matchlogs_base"
ML_B365_DIR  = BASE / "bet365_matchlogs"
CURVES_DIR   = BASE / "cumprofit_curves"

CLASS2LABEL = {0: "Away", 1: "Draw", 2: "Home"}
TXT2LABEL   = {"A": "Away", "D": "Draw", "H": "Home"}

def _season_from_name(path: Path) -> int | None:
    m = re.search(r"(\d{4})", path.stem)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _safe_dt(s):
    return pd.to_datetime(s, errors="coerce")

def _ensure_float(s):
    return pd.to_numeric(s, errors="coerce")

def _load_matchlog_model(season: int) -> pd.DataFrame | None:
    p = ML_MODEL_DIR / f"matchlog_{season}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    # columnas mínimas esperadas
    need = ["Date","HomeTeam_norm","AwayTeam_norm","net_profit"]
    for c in need:
        if c not in df.columns:
            return None
    return df

def _load_matchlog_b365(season: int) -> pd.DataFrame | None:
    p = ML_B365_DIR / f"matchlog_{season}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    need = ["Date","HomeTeam_norm","AwayTeam_norm","net_profit"]
    for c in need:
        if c not in df.columns:
            return None
    return df

def build_curves_for_season(season: int) -> tuple[pd.DataFrame, dict] | tuple[None, None]:
    ml_m = _load_matchlog_model(season)
    ml_b = _load_matchlog_b365(season)
    if ml_m is None or ml_b is None or ml_m.empty or ml_b.empty:
        return None, None

    key = ["Date","HomeTeam_norm","AwayTeam_norm"]
    both = pd.merge(
        ml_m, ml_b, on=key, how="inner",
        suffixes=("_model","_b365")
    )
    if both.empty:
        return None, None

    # Orden temporal
    both["Date"] = _safe_dt(both["Date"])
    both = both.sort_values("Date").reset_index(drop=True)
    dates_str = both["Date"].dt.strftime("%Y-%m-%d")

    # Retornos por partido (ya calculados)
    m_ret = _ensure_float(both.get("net_profit_model", both["net_profit"] if "net_profit" in both.columns else np.nan)).fillna(0.0)
    b_ret = _ensure_float(both.get("net_profit_b365",  both["net_profit_b365"] if "net_profit_b365" in both.columns else both["net_profit"] if "net_profit" in both.columns else np.nan)).fillna(0.0)

    # Si el merge trajo dos columnas 'net_profit' indistintas, preferimos las sufijadas
    if "net_profit_model" not in both.columns and "net_profit" in both.columns:
        # Caso raro: renombramos la del modelo si no existe sufijada
        m_ret = _ensure_float(both["net_profit"]).fillna(0.0)
    if "net_profit_b365" not in both.columns:
        # Puede llamarse 'net_profit_y' según pandas si había conflicto
        for cand in ["net_profit_y","net_profit_b"]:
            if cand in both.columns:
                b_ret = _ensure_float(both[cand]).fillna(0.0)
                break

    m_cum = m_ret.cumsum()
    b_cum = b_ret.cumsum()

    # Labels (texto)
    # true_result (num) puede venir en cualquiera de los dos; preferimos el del modelo
    if "true_result_model" in both.columns:
        true_num = _ensure_float(both["true_result_model"]).astype("Int64")
    elif "true_result" in both.columns:
        true_num = _ensure_float(both["true_result"]).astype("Int64")
    else:
        true_num = pd.Series([np.nan]*len(both), dtype="float64")

    true_txt = true_num.map(CLASS2LABEL)

    # model_txt
    if "Pred" in both.columns:
        model_txt = both["Pred"].map(TXT2LABEL).fillna("")  # nuestras salidas traían Pred="A/D/H"
    elif "predicted_result" in both.columns:
        model_txt = _ensure_float(both["predicted_result"]).astype("Int64").map(CLASS2LABEL)
    else:
        model_txt = pd.Series([""]*len(both), dtype="string")

    # bet365_txt
    if "bet365_pred" in both.columns:
        b365_txt = _ensure_float(both["bet365_pred"]).astype("Int64").map(CLASS2LABEL)
    else:
        b365_txt = pd.Series([""]*len(both), dtype="string")

    series_df = pd.DataFrame({
        "match_num": np.arange(1, len(both)+1, dtype=int),
        "date": dates_str,
        "model_cum": m_cum.round(3),
        "bet365_cum": b_cum.round(3),
        "model_ret": m_ret.round(3),
        "bet365_ret": b_ret.round(3),
        "home": both["HomeTeam_norm"].astype("string"),
        "away": both["AwayTeam_norm"].astype("string"),
        "true_txt": true_txt.astype("string"),
        "model_txt": model_txt.astype("string"),
        "bet365_txt": b365_txt.astype("string"),
    })

    n = len(series_df)
    final_m = float(series_df["model_cum"].iloc[-1]) if n else 0.0
    final_b = float(series_df["bet365_cum"].iloc[-1]) if n else 0.0

    summary = {
        "train_until": int(season - 1),
        "test_season": int(season),
        "n_matches": int(n),
        "profit_model": final_m,
        "profit_bet365": final_b,
        "roi_model": float(final_m / n) if n else 0.0,
        "roi_bet365": float(final_b / n) if n else 0.0,
    }
    return series_df, summary

def main():
    CURVES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Detectar temporadas disponibles por matchlogs del modelo
    if not ML_MODEL_DIR.exists():
        print("No hay outputs/matchlogs_base; nada que hacer.")
        sys.exit(0)

    seasons = sorted({
        _season_from_name(p)
        for p in ML_MODEL_DIR.glob("matchlog_*.csv")
    } - {None})

    if not seasons:
        print("No se detectaron temporadas en matchlogs_base.")
        sys.exit(0)

    CURVES_DIR.mkdir(parents=True, exist_ok=True)

    index_rows = []
    for s in seasons:
        series_df, summary = build_curves_for_season(s)
        if series_df is None or series_df.empty:
            continue

        # CSV
        csv_path = CURVES_DIR / f"cumprofit_{s}.csv"
        series_df.to_csv(csv_path, index=False)

        # JSON compacto para la web
        payload = {
            "train_until": summary["train_until"],
            "test_season": summary["test_season"],
            "n_matches": summary["n_matches"],
            "series": [
                {
                    "i": int(r.match_num),
                    "d": str(r.date),
                    "m": float(r.model_cum),
                    "b": float(r.bet365_cum),
                    "hm": str(r.home),
                    "aw": str(r.away),
                    "t":  str(r.true_txt),
                    "pm": str(r.model_txt),
                    "pb": str(r.bet365_txt),
                }
                for _, r in series_df.iterrows()
            ],
            "final": {
                "model": float(summary["profit_model"]),
                "bet365": float(summary["profit_bet365"]),
                "roi_model": float(summary["roi_model"]),
                "roi_bet365": float(summary["roi_bet365"]),
            }
        }
        (CURVES_DIR / f"cumprofit_{s}.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )

        index_rows.append({
            "test_season": int(s),
            "train_until": int(s - 1),
            "n_matches": int(summary["n_matches"]),
            "profit_model": float(summary["profit_model"]),
            "profit_bet365": float(summary["profit_bet365"]),
            "roi_model": float(summary["roi_model"]),
            "roi_bet365": float(summary["roi_bet365"]),
            "csv_file": f"cumprofit_{s}.csv",
            "json_file": f"cumprofit_{s}.json",
        })
        print(f"[CURVAS] Season {s}: {len(series_df)} puntos → guardado CSV/JSON.")

    # Índice global (opcional, pero útil)
    if index_rows:
        idx_df = pd.DataFrame(index_rows).sort_values("test_season")
        idx_df.to_csv(BASE / "cumprofit_index.csv", index=False)
        (BASE / "cumprofit_index.json").write_text(
            json.dumps(index_rows, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("Guardados índice de curvas:")
        print("-", BASE / "cumprofit_index.csv")
        print("-", BASE / "cumprofit_index.json")
    else:
        print("No se generaron curvas (¿faltan bet365_matchlogs o no coinciden partidos?).")

if __name__ == "__main__":
    main()
