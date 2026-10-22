"""Qué política de apuesta publica la mezcla: el plano a todo o el filtro de valor.

La cadena publica hoy `politica="valor"` con `umbral=UMBRAL_VALOR`: apuesta solo
donde la probabilidad del modelo supera a la implícita en la cuota por ese margen.
El ROI con el que se midió y se adoptó la mezcla es otro, el de Manuel: **una unidad
plana al signo que el modelo señala, en todos los partidos con precio**, que en
`backtest` es `politica="favorito"` y no tiene ningún parámetro que elegir.

Las dos no son la misma cifra y no se pueden intercambiar sin mirar. Publicar `valor`
afirmaría del modelo una rentabilidad que no es la que justificó adoptarlo, y además
descansa sobre un umbral que es una decisión tomada sobre datos; `favorito` no decide
nada. Pero eso es un argumento, y aquí hace falta la medida.

Se mide sobre los **1900 partidos de fuera** —`medicion` más `confirmacion`, los que
no participaron en elegir el peso— y por separado sobre `seleccion`, para ver si el
orden entre las dos políticas se mantiene al cambiar de ventana o se invierte. Cada
cifra va emparejada con el listón de Bet365 (regla 3) y la ventaja lleva su intervalo
por remuestreo de jornadas completas, porque una diferencia de rendimiento sin
intervalo no se puede leer.

Uso:
    python _politica.py
"""
from __future__ import annotations

import os

for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

import sys                                                   # noqa: E402
import time                                                  # noqa: E402
from pathlib import Path                                     # noqa: E402

import pandas as pd                                          # noqa: E402

_RAIZ = Path(__file__).resolve().parent
if str(_RAIZ) not in sys.path:
    sys.path.insert(0, str(_RAIZ))

import _mezcla as mz                                         # noqa: E402
import scripts.models.backtest as bt                         # noqa: E402
import scripts.models.laboratorio as lb                      # noqa: E402
import scripts.models.seleccion as sl                        # noqa: E402

# Las políticas que se enfrentan. `favorito` no lleva umbral —apuesta siempre— y el
# valor que se le pasa es inerte; `select_bets` lo ignora cuando la política no es
# `valor`. Los tres umbrales de valor cubren el declarado del proyecto y uno a cada
# lado, para que la lectura no dependa de haber acertado con el número.
POLITICAS = (("favorito", 0.0),
             ("valor", 0.0),
             ("valor", bt.UMBRAL_VALOR),
             ("valor", 0.06))

N_BOOT = 400
SEMILLA = 7


def _bloques(pred: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Los partidos de `seleccion` y los 1900 de fuera, con las cuotas pegadas."""
    d = pred.copy()
    d["season"] = d["season"].astype(str)
    sel = set(mz.BLOQUES["seleccion"])
    fuera = set(mz.BLOQUES["medicion"]) | set(mz.BLOQUES["confirmacion"])
    salida = {}
    for nombre, temporadas in (("seleccion", sel), ("FUERA (2+3)", fuera)):
        sub = d.loc[d["season"].isin(temporadas)].reset_index(drop=True)
        sub.attrs = dict(pred.attrs)
        salida[nombre] = sub
    return salida


def main() -> int:
    t0 = time.time()
    feat = lb.datos()
    cands = sl.candidatas(feat, regime="semanal")
    mercado = set(lb.dominios(cands)["mercado"])
    cols = list(sl.SEMILLA_MERCADO) + [c for c in cands if c not in mercado]

    print(f"ajustando la mezcla (w=0.85) sobre {len(cols)} columnas...", flush=True)
    e = lb.evalua(cols, "mezcla", cfg=mz.cfg_abierto(), feat=feat,
                  evaluar="validacion", cuotas=mz.CASA, n_boot=0,
                  registrar=False, verbose=False)
    print(f"  n={len(e.pred)}  [{time.time() - t0:.0f}s]", flush=True)

    # Las cuotas de cierre promediadas, que es el eje con el que se publica.
    # `lb.evalua(cuotas="b365")` ya dejó pegadas las de la casa de referencia, y
    # `attach_odds` las sufijaría en lugar de sustituirlas, así que se retiran antes.
    desnudo = e.pred.drop(columns=bt.CUOTA_COLS, errors="ignore")
    desnudo.attrs = dict(e.pred.attrs)
    pred = bt.attach_odds(desnudo, feat, cuotas="cierre_avg")

    print(f"\n{'bloque':<13} {'politica':<9} {'umbral':>7} {'apuestas':>9} "
          f"{'n':>5} {'ROI':>8} {'ROI b365':>9} {'ventaja':>9} "
          f"{'IC de la ventaja':>22}")
    for nombre, sub in _bloques(pred).items():
        for politica, umbral in POLITICAS:
            r = bt.compara_con_mercado(
                sub, politica=politica, umbral=umbral, importe="plana",
                n_boot=N_BOOT, seed=SEMILLA, intervalos_temporada=False)
            m, ref, iv = r["modelo"], r["referencia"], r["intervalo"]
            ic = ("        —        " if iv is None
                  else f"[{100 * iv['inf']:+6.2f}, {100 * iv['sup']:+6.2f}]")
            print(f"{nombre:<13} {politica:<9} {umbral:>7.2f} "
                  f"{m['n_apuestas']:>9} {m['n_partidos']:>5} "
                  f"{100 * m['rendimiento']:>+7.2f}% "
                  f"{100 * ref['rendimiento']:>+8.2f}% "
                  f"{100 * r['diferencia']:>+8.2f}% {ic:>22}", flush=True)

    print(f"\n{time.time() - t0:.0f} s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
