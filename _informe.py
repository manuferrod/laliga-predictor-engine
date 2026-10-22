"""Las métricas de lo publicado, en global y temporada a temporada.

No recalcula nada: lee los parquets que dejó `build_models.py`, que es la regla 8
—una cifra recalculada en el informe podría no ser la que auditó `qa_models`—.

Uso:  python _informe.py [semanal|retrospectivo]
"""
import sys

import pandas as pd

REG = sys.argv[1] if len(sys.argv) > 1 else "semanal"
BASE = f"outputs/models/{REG}"

pd.set_option("display.width", 240)
pd.set_option("display.max_columns", 60)


def pc(x):
    return "" if pd.isna(x) else f"{x:+.2%}"


def main() -> None:
    met = pd.read_parquet(f"{BASE}/metricas_modelos.parquet")
    tmp = pd.read_parquet(f"{BASE}/metricas_por_temporada.parquet")
    eco = pd.read_parquet(f"{BASE}/economia_por_temporada.parquet")

    orden = ["modelo", "pinnacle", "consenso", "b365", "prior", "local"]

    print("=" * 110)
    print(f"GLOBAL — bloque de validación ({REG})")
    print("=" * 110)
    cols = ["contendiente", "n", "rps", "logloss", "brier", "accuracy",
            "auc_macro", "ece_confianza", "f1_D", "pred_D",
            "fiabilidad", "resolucion", "rps_ic_bajo", "rps_ic_alto"]
    g = met.set_index("contendiente").reindex(orden).reset_index()[cols]
    print(g.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    print()
    print("=" * 110)
    print("POR TEMPORADA — modelo frente al precio de Bet365")
    print("=" * 110)
    t = tmp[tmp.contendiente.isin(["modelo", "b365", "prior"])]
    piv = t.pivot_table(index=["bloque", "season"], columns="contendiente",
                        values=["n", "rps", "accuracy", "logloss", "auc_macro"])
    piv = piv.reorder_levels([1, 0], axis=1).sort_index(axis=1)
    print(piv.to_string(float_format=lambda v: f"{v:.4f}"))

    print()
    print("=" * 110)
    print("ECONOMÍA POR TEMPORADA — rendimiento sobre lo arriesgado")
    print("=" * 110)
    e = eco.copy()
    for c in ("rendimiento_modelo", "rendimiento_ref", "diferencia",
              "dif_inf", "dif_sup", "acierto_modelo", "acierto_ref"):
        e[c] = e[c].map(pc)
    print(e[["cuotas", "bloque", "season", "n_modelo", "acierto_modelo",
             "cuota_media_modelo", "rendimiento_modelo", "n_ref",
             "acierto_ref", "rendimiento_ref", "diferencia",
             "dif_inf", "dif_sup"]].to_string(index=False,
                                              float_format=lambda v: f"{v:.2f}"))


if __name__ == "__main__":
    main()
