# scripts/build_cumprofit_curves_from_matchlogs.py
from pathlib import Path
import json
import pandas as pd
import numpy as np
import re
import sys

BASE = Path("outputs")
CURVES_DIR = BASE / "cumprofit_curves"

# Mapeos de texto (A/D/H) a Nombre completo
TXT2LABEL = {"A": "Away", "D": "Draw", "H": "Home", "1": "Home", "0": "Draw", "2": "Away"}
CLASS2LABEL = {0: "Away", 1: "Draw", 2: "Home"}

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

def _load_and_standardize(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"Error leyendo {path.name}: {e}")
        return None

    # 1. Estandarizar columna PROFIT
    if "net_profit" not in df.columns:
        if "profit" in df.columns:
            df["net_profit"] = df["profit"]
        else:
            # Si no hay profit, no nos sirve
            return None

    # 2. Estandarizar columna DATE (asegurar datetime)
    if "Date" in df.columns:
        df["Date"] = _safe_dt(df["Date"])
    elif "date" in df.columns:
        df["Date"] = _safe_dt(df["date"])
    else:
        return None

    # 3. Validar columnas mínimas para el merge
    need = ["Date", "HomeTeam_norm", "AwayTeam_norm", "net_profit"]
    for c in need:
        if c not in df.columns:
            return None
            
    return df

def build_curves_for_season(season: int) -> tuple[pd.DataFrame, dict] | tuple[None, None]:
    # Rutas esperadas en la raíz outputs/
    path_m = BASE / f"matchlogs_{season}.csv"
    path_b = BASE / f"matchlogs_market_{season}.csv"

    ml_m = _load_and_standardize(path_m)
    ml_b = _load_and_standardize(path_b)

    if ml_m is None or ml_b is None or ml_m.empty or ml_b.empty:
        return None, None

    # Merge por claves del partido
    key = ["Date", "HomeTeam_norm", "AwayTeam_norm"]
    # Usamos inner join para quedarnos solo con partidos presentes en ambos
    both = pd.merge(
        ml_m, ml_b, on=key, how="inner",
        suffixes=("_model", "_b365")
    )
    
    if both.empty:
        return None, None

    # Ordenar cronológicamente
    both = both.sort_values("Date").reset_index(drop=True)
    dates_str = both["Date"].dt.strftime("%Y-%m-%d")

    # --- CALCULO DE RETORNOS ---
    # Preferimos la columna del sufijo si existe, si no la genérica
    def get_col(df, base_col, suffix):
        if f"{base_col}{suffix}" in df.columns:
            return df[f"{base_col}{suffix}"]
        if base_col in df.columns:
            return df[base_col]
        return pd.Series(0.0, index=df.index)

    m_ret = _ensure_float(get_col(both, "net_profit", "_model")).fillna(0.0)
    b_ret = _ensure_float(get_col(both, "net_profit", "_b365")).fillna(0.0)

    m_cum = m_ret.cumsum()
    b_cum = b_ret.cumsum()

    # --- CALCULO DE ETIQUETAS (LABELS) ---
    # Intentamos recuperar el texto real (A/D/H)
    
    # 1. Resultado Real (True Result)
    # Buscamos 'y_true', 'true_result', etc.
    true_raw = pd.Series("", index=both.index, dtype="string")
    for col in ["y_true_model", "y_true", "true_result_model", "true_result"]:
        if col in both.columns:
            true_raw = both[col].astype(str)
            break
    
    # Mapear A/D/H -> Away/Draw/Home
    true_txt = true_raw.map(TXT2LABEL).fillna(true_raw)

    # 2. Predicción Modelo
    model_raw = pd.Series("", index=both.index, dtype="string")
    for col in ["y_pred_model", "y_pred", "Pred", "predicted_result"]:
        if col in both.columns:
            model_raw = both[col].astype(str)
            break
    model_txt = model_raw.map(TXT2LABEL).fillna(model_raw)

    # 3. Predicción Mercado (Bet365)
    b365_raw = pd.Series("", index=both.index, dtype="string")
    for col in ["y_pred_market", "bet365_pred", "y_pred_b365"]:
        if col in both.columns:
            b365_raw = both[col].astype(str)
            break
    b365_txt = b365_raw.map(TXT2LABEL).fillna(b365_raw)

    # Construir DataFrame final para el gráfico
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
    if not BASE.exists():
        print("No existe outputs/; nada que hacer.")
        sys.exit(0)

    CURVES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Detectar temporadas disponibles en la RAÍZ de outputs
    base_matchlogs = []
    for p in BASE.glob("matchlogs_*.csv"):
        if "market" in p.name or "smote" in p.name:
            continue
        base_matchlogs.append(p)

    seasons = sorted({_season_from_name(p) for p in base_matchlogs} - {None})

    if not seasons:
        print("No se detectaron temporadas (matchlogs_YYYY.csv) en outputs/.")
        sys.exit(0)

    print(f"Temporadas detectadas para curvas: {seasons}")

    index_rows = []
    for s in seasons:
        series_df, summary = build_curves_for_season(s)
        if series_df is None or series_df.empty:
            print(f"⚠️  Season {s}: No se pudo cruzar modelo vs bet365 (o faltan archivos/columnas).")
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

    # Índice global
    if index_rows:
        idx_df = pd.DataFrame(index_rows).sort_values("test_season")
        idx_df.to_csv(BASE / "cumprofit_index.csv", index=False)
        (BASE / "cumprofit_index.json").write_text(
            json.dumps(index_rows, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("Guardados índice de curvas.")
    else:
        print("No se generaron curvas.")

if __name__ == "__main__":
    main()
