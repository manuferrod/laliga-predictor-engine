"""¿Cuánto del ROI lo decide el modelo y cuánto la casa donde cobras?

El proyecto lleva cinco rutas de búsqueda intentando mover el ROI cambiando las
variables del modelo, y lo máximo que se ha movido la diferencia contra el listón son
cuatro décimas de punto. Esta medida pregunta otra cosa: **a igualdad de modelo y de
pronóstico, cuánto cambia el ROI según a qué cuota liquides**.

Importa porque el margen de la casa no es el mismo en todas, y eso está medido en este
proyecto: `_devig.py` mide sobrerredondos medianos de **5.35 % en Bet365, 4.74 % en la
media del mercado y 0.33 % en la mejor cuota disponible**. Ese 5 % de diferencia entre
Bet365 y la mejor cuota no es una opinión sobre fútbol: es dinero que se queda la casa
o se queda el apostante, y entra directo en el ROI sin pasar por el modelo.

Las tres familias del maestro:
  `odds_b365_*`  la casa de referencia del proyecto (`backtest.REFERENCIA`)
  `odds_avg_*`   media de las casas que publica football-data
  `odds_max_*`   la mejor cuota disponible en el mercado para cada signo

Dos estrategias, porque responden a preguntas distintas y Manuel usa la primera:
  **plana**  una unidad al signo que el modelo señala, en todos los partidos
  **valor**  una unidad solo donde la probabilidad del modelo supera a la implícita

Advertencia que hay que leer antes de usar `odds_max`: es la mejor cuota **observada a
posteriori** en el panel de football-data. Suponer que la consigues siempre es
optimista —hay que tener cuenta en esa casa, aceptar su límite de apuesta y llegar
antes de que la corrija—. Es un techo, no un plan de negocio. Pero es el techo
correcto que hay que citar al lado del suelo de Bet365, porque la distancia entre los
dos mide lo que cuesta no buscar precio.

Uso:
    python _dondecobras.py
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

_RAIZ = Path(__file__).resolve().parent
PRED = _RAIZ / "outputs" / "models" / "semanal" / "predicciones_validacion.parquet"
MAESTRO = _RAIZ / "data" / "02_processed" / "master_matches.parquet"
SALIDA = _RAIZ / "outputs" / "models" / "semanal" / "donde_cobras.csv"

PROB = ["p_home", "p_draw", "p_away"]
FAMILIAS = {"b365": ["odds_b365_h", "odds_b365_d", "odds_b365_a"],
            "avg": ["odds_avg_h", "odds_avg_d", "odds_avg_a"],
            "max": ["odds_max_h", "odds_max_d", "odds_max_a"]}


def _carga() -> pd.DataFrame:
    cols = sorted({c for v in FAMILIAS.values() for c in v})
    m = pd.read_parquet(MAESTRO, columns=["match_key"] + cols)
    p = pd.read_parquet(PRED).merge(m, on="match_key", how="left")
    # Todas las familias tienen que cubrir los mismos partidos, o la comparación
    # mezclaría el precio con el censo.
    p = p.loc[p["y_num"].notna() & p[cols].notna().all(axis=1)].copy()
    p["y_num"] = p["y_num"].astype(int)
    return p


def _mide(p: pd.DataFrame, cuotas: list[str], estrategia: str) -> dict:
    C = p[cuotas].to_numpy(dtype=float)
    P = p[PROB].to_numpy(dtype=float)
    sel = P.argmax(axis=1)
    fila = np.arange(len(p))
    q = C[fila, sel]
    entra = np.ones(len(p), dtype=bool)
    if estrategia == "valor":
        # Valor esperado positivo: la probabilidad del modelo por la cuota supera 1.
        entra = P[fila, sel] * q > 1.0
    ok = sel == p["y_num"].to_numpy()
    ben = np.where(ok, q - 1.0, -1.0)[entra]
    return {"estrategia": estrategia, "casa": cuotas[0].split("_")[1],
            "n": int(entra.sum()), "acierto": float(ok[entra].mean()),
            "cuota_media": float(q[entra].mean()),
            "roi": float(ben.mean()),
            "ee": float(ben.std(ddof=1) / np.sqrt(len(ben)))}


def main() -> int:
    p = _carga()
    filas = [_mide(p, c, e) for e in ("plana", "valor") for c in FAMILIAS.values()]
    tab = pd.DataFrame(filas)
    print(f"\n=== mismo modelo, mismo pronóstico, {len(p)} partidos de validación "
          f"===\n")
    print(f"{'estrategia':>11} {'casa':>5} {'n':>5} {'acierto':>8} {'cuota':>6} "
          f"{'ROI':>9} {'ee':>7}")
    for _, r in tab.iterrows():
        print(f"{r['estrategia']:>11} {r['casa']:>5} {int(r['n']):>5} "
              f"{100 * r['acierto']:>7.2f}% {r['cuota_media']:>6.2f} "
              f"{100 * r['roi']:>+8.2f}% {100 * r['ee']:>6.2f}")
    for e in ("plana", "valor"):
        s = tab.loc[tab["estrategia"] == e].set_index("casa")
        print(f"\n{e}: pasar de Bet365 a la mejor cuota vale "
              f"{100 * (s.loc['max', 'roi'] - s.loc['b365', 'roi']):+.2f} "
              f"puntos de ROI")
    tab.to_csv(SALIDA, index=False, encoding="utf-8")
    print(f"\n{SALIDA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
