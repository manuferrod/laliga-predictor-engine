"""Control de entrada de la familia `mezcla`: que reproduzca lo que midió el estudio.

`_mezcla.py` midió la mezcla **fuera** del motor: ajustó dos modelos por separado con
`lb.evalua`, guardó las dos ternas de probabilidades en `mezcla_pred.parquet` y las
combinó después con `numpy`. La familia `estimators.mezcla` hace lo mismo **dentro**
del motor, en un solo estimador que el protocolo entrena como a cualquier otro.

Las dos cosas tienen que dar el mismo número, y esto lo comprueba antes de que la
familia toque nada publicado. No es una formalidad: hay tres sitios donde podrían
divergir y ninguno es evidente.

  · El reparto de columnas. El estudio pasó dos listas explícitas; la familia lo
    deduce del prefijo `mkt_p_`. Si una columna de juego lo llevara, caería en la
    mitad equivocada.
  · La poda por colinealidad se reajusta DENTRO de cada pliegue, de modo que lo que
    ve el submodelo depende de qué columnas sobrevivieron en esa ventana. El estudio
    pidió 3 y 59 por separado; la familia pide las 62 juntas y parte después.
  · El orden de las columnas. `ModeloConColumnas` respeta el orden de la matriz, no
    el de la lista pedida, y un cambio de orden movería el ajuste.

Si el máximo de la diferencia absoluta entre las dos ternas no es cero hasta el límite
de la aritmética, la familia NO es el modelo que se midió y no se puede publicar con
las cifras del estudio.

Resultado de la ejecución del 19-oct-2026, antes de cablear nada: unión de 62 columnas
(3 de precio + 59 de juego), **ninguna columna de juego con prefijo de precio**, 3040
partidos comparados, 0 ausentes, y **máxima diferencia absoluta 0.000e+00**. Las siete
cifras publicables cuadran: RPS de fuera 0.194977, acierto 53.7368 %, ROI a la mejor
cuota +2.5121 % sobre 1900 partidos, y por bloque +4.07 % / +5.31 % / −1.69 %. Los tres
riesgos de divergencia del párrafo anterior quedan descartados por esa ejecución, no por
razonamiento: el reparto por prefijo es exacto, la poda por pliegue no depende del
subconjunto pedido (`ausentes=0` y el mismo censo de 3040) y el orden de columnas es
inocuo porque `ModeloConColumnas._resuelve` respeta el de la matriz.

Uso:
    python _mezcla_control.py
"""
from __future__ import annotations

import os

for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

import sys                                                   # noqa: E402
import time                                                  # noqa: E402
from pathlib import Path                                     # noqa: E402

import numpy as np                                           # noqa: E402
import pandas as pd                                          # noqa: E402

_RAIZ = Path(__file__).resolve().parent
if str(_RAIZ) not in sys.path:
    sys.path.insert(0, str(_RAIZ))

import _mezcla as mz                                         # noqa: E402
import scripts.models.laboratorio as lb                      # noqa: E402
import scripts.models.seleccion as sl                        # noqa: E402

W = 0.85                      # el peso publicado, el que eligió el acierto
TOL = 1e-10                   # límite de la aritmética, no una tolerancia de diseño

# Lo que midió el estudio, escrito aquí para que el control compare contra una cifra
# y no contra una impresión. Son las tres métricas del peso publicado sobre los 1900
# partidos que no participaron en elegirlo, más el ROI a la mejor cuota por bloque.
ESPERADO = {"n_fuera": 1900, "rps_fuera": 0.194977, "acierto_fuera": 0.5374,
            "roi_max_fuera": 0.0251,
            "roi_max": {"seleccion": 0.0407, "medicion": 0.0531,
                        "confirmacion": -0.0169}}

CUOTAS_EXTRA = [c for v in mz.FAMILIAS_CUOTA.values() for c in v]


def _censo(pred: pd.DataFrame, feat: pd.DataFrame) -> pd.DataFrame:
    """Los partidos liquidables, con las tres familias de cuota pegadas."""
    d = pred[["match_key", "season", "y_num"] + mz.PROB + mz.CUOTA].copy()
    d["season"] = d["season"].astype(str)
    d = d.merge(feat[["match_key"] + CUOTAS_EXTRA].drop_duplicates("match_key"),
                on="match_key", how="left")
    return d.loc[d[mz.CUOTA].notna().all(axis=1)].reset_index(drop=True)


def main() -> int:
    t0 = time.time()
    feat = lb.datos()
    cands = sl.candidatas(feat, regime="semanal")
    mercado = set(lb.dominios(cands)["mercado"])
    juego = [c for c in cands if c not in mercado]
    cols = list(sl.SEMILLA_MERCADO) + juego

    print(f"union pedida: {len(cols)} columnas "
          f"({len(sl.SEMILLA_MERCADO)} de precio + {len(juego)} de juego)")
    intrusos = [c for c in juego if c.startswith("mkt_p_")]
    print(f"columnas de juego con prefijo de precio: {len(intrusos)}")
    if intrusos:
        print(f"  ABORTA: {intrusos}")
        return 1

    print(f"\najustando la familia 'mezcla' (w={W}) sobre las ocho temporadas "
          f"abiertas...", flush=True)
    e = lb.evalua(cols, "mezcla", cfg=mz.cfg_abierto(), feat=feat,
                  evaluar="validacion", cuotas=mz.CASA, n_boot=0,
                  registrar=False, verbose=False)
    print(f"  n={len(e.pred)}  ausentes={len(e.ausentes)}  "
          f"[{time.time() - t0:.0f}s]", flush=True)

    # --- 1. la terna, contra la que combinó el estudio a mano ------------------
    ref = pd.read_parquet(mz.MATRIZ)
    Pm = ref[[f"{c}_mercado" for c in mz.PROB]].to_numpy(dtype=float)
    Pj = ref[[f"{c}_juego" for c in mz.PROB]].to_numpy(dtype=float)
    ref = ref[["match_key", "season"]].assign(
        **{f"ref_{c}": v for c, v in zip(mz.PROB, mz._mezcla(Pm, Pj, W).T)})

    d = _censo(e.pred, feat).merge(ref, on=["match_key", "season"], how="inner")
    A = d[mz.PROB].to_numpy(dtype=float)
    B = d[[f"ref_{c}" for c in mz.PROB]].to_numpy(dtype=float)
    peor = float(np.abs(A - B).max())
    print(f"\npartidos comparados: {len(d)}")
    print(f"maxima diferencia absoluta de probabilidad: {peor:.3e}")
    casa = peor <= TOL
    print(f"{'REPRODUCE' if casa else 'NO REPRODUCE'} la terna del estudio")

    # --- 2. las cifras publicables, contra las del estudio --------------------
    idx = {b: d["season"].isin(ss).to_numpy() for b, ss in mz.BLOQUES.items()}
    fuera = idx["medicion"] | idx["confirmacion"]
    print(f"\n{'bloque':<13} {'n':>5} {'RPS':>9} {'acierto':>8} {'ROI b365':>9} "
          f"{'ROI max':>9}")
    filas = {}
    for b in ("seleccion", "medicion", "confirmacion"):
        m = idx[b]
        r = mz._cifras(A[m], d.loc[m])
        filas[b] = r
        print(f"{b:<13} {r['n']:>5} {r['rps']:>9.6f} {100 * r['acierto']:>7.2f}% "
              f"{100 * r['roi']:>+8.2f}% {100 * r['roi_max']:>+8.2f}%")
    rf = mz._cifras(A[fuera], d.loc[fuera])
    print(f"{'FUERA (2+3)':<13} {rf['n']:>5} {rf['rps']:>9.6f} "
          f"{100 * rf['acierto']:>7.2f}% {100 * rf['roi']:>+8.2f}% "
          f"{100 * rf['roi_max']:>+8.2f}%")

    print("\ncontra lo que escribio el estudio:")
    pruebas = [
        ("n de fuera", rf["n"], ESPERADO["n_fuera"], 0),
        ("RPS de fuera", rf["rps"], ESPERADO["rps_fuera"], 5e-6),
        ("acierto de fuera", rf["acierto"], ESPERADO["acierto_fuera"], 5e-5),
        ("ROI max de fuera", rf["roi_max"], ESPERADO["roi_max_fuera"], 5e-5),
    ]
    for b, v in ESPERADO["roi_max"].items():
        pruebas.append((f"ROI max en {b}", filas[b]["roi_max"], v, 5e-5))
    todo = casa
    for nombre, visto, esperado, tol in pruebas:
        ok = abs(float(visto) - float(esperado)) <= tol
        todo = todo and ok
        print(f"  [{'OK ' if ok else 'MAL'}] {nombre:<22} "
              f"visto {visto:.6f}  escrito {esperado:.6f}")

    print(f"\n{'=' * 74}")
    print("CONTROL PASADO: la familia es el modelo que se midio" if todo
          else "CONTROL FALLADO: la familia NO es el modelo que se midio")
    print(f"{'=' * 74}\n{time.time() - t0:.0f} s")
    return 0 if todo else 1


if __name__ == "__main__":
    raise SystemExit(main())
