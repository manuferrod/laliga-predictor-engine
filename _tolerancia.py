"""Cuánto vale una diezmilésima en cada criterio: el listón de parada, medido.

`seleccion.TOLERANCIA = 2e-4` está en unidades de **RPS**, y su comentario dice de
dónde sale: la diferencia emparejada por partido entre dos conjuntos que comparten la
semilla y difieren en una columna tiene una desviación típica de 0.0086 sobre el bloque
de selección, es decir un error típico de 0.00026 remuestreando jornadas completas, y
dos diezmilésimas son cuatro quintos de ese error típico.

Ese número **no se puede reutilizar para `accuracy` ni para `ventaja`**, porque sus
escalas no son la del RPS. El acierto se mueve en pasos de 1/n —0.088 puntos sobre mil
ciento cuarenta partidos— y el ROI plano paga cuotas, así que su diferencia por partido
vive en unidades de la cuota y no en unidades de probabilidad. Con 2e-4 en esas dos
escalas la condición de parada no se dispararía nunca y la voraz recorrería hasta
`MAX_VARS` adoptando ruido. Aquí se mide el error típico emparejado de cada criterio con
el mismo método con el que se midió el del RPS, para que cada uno tenga su propio listón
y la razón entre listón y error típico sea la misma en los tres.

Dos decisiones del experimento, que son las que lo hacen comparable:

  · **La liquidación es `politica="favorito"`**, una unidad plana al signo del modelo en
    todos los partidos con precio, que es la definición de ROI de Manuel y la que
    publica la cadena. No es un detalle: con `politica="valor"` el conjunto de partidos
    apostados se mueve con cada columna que entra —es la objeción que el comentario de
    `CRITERIOS` levanta contra perseguir el ROI—, y entonces la diferencia emparejada no
    es emparejada, porque los dos brazos no liquidan los mismos partidos. Con `favorito`
    el denominador es fijo y la comparación vuelve a estar bien planteada.
  · **Se mide en las dos ventanas**: el bloque de selección de hoy (1140 partidos) y el
    abierto de 2017/18 a 2024/25 (3040), que es el que pide evaluar todas las
    temporadas. El error típico escala como 1/√n, así que la tolerancia depende de la
    ventana y hay que fijarla sobre la que se vaya a usar.

Las candidatas no son un conjunto cualquiera: son las tres del tramo final de la voraz
—las dos que adoptó y la que rechazó por jackknife—, dos fuertes de `fuerza` y una
inerte. Cubren el rango de lo que una candidata real le hace al modelo, para que el
error típico no sea el de un par afortunado.

Uso:
    python _tolerancia.py
"""
from __future__ import annotations

import os

# Antes de numpy, por el mismo motivo que en `_mezcla.py`: BLAS multihilo se estorba.
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

import json                                                 # noqa: E402
import sys                                                  # noqa: E402
import time                                                 # noqa: E402
from pathlib import Path                                    # noqa: E402

import numpy as np                                          # noqa: E402
import pandas as pd                                         # noqa: E402

_RAIZ = Path(__file__).resolve().parent
if str(_RAIZ) not in sys.path:
    sys.path.insert(0, str(_RAIZ))

import _mezcla as mz                                        # noqa: E402
import scripts.models.backtest as bt                        # noqa: E402
import scripts.models.evaluate as ev                        # noqa: E402
import scripts.models.laboratorio as lb                     # noqa: E402
import scripts.models.seleccion as sl                       # noqa: E402

FAMILIA = "logistica"

# El eje de liquidación. **No puede ser `cierre_avg`, y el motivo está medido**: la
# cuota de cierre promediada tiene cobertura CERO antes de 2019/20 —0 de 380 en 16/17,
# 17/18 y 18/19, contra 380 de 380 de Bet365, consenso y máxima—, de modo que medir el
# ROI sobre la ventana abierta con el eje que hoy publica la cadena liquidaría 2280 de
# los 3040 partidos y el promedio se calcularía sobre un subconjunto sin decirlo. Se usa
# el listón declarado del proyecto (`backtest.REFERENCIA = "b365"`), que es además el
# único precio con el que la cifra del modelo y la del listón son comparables (regla 3)
# y el que ya usan `_maximo.py` y `_censo.py`.
CUOTAS = "b365"

# Las seis candidatas, con el papel que ya jugaron:
#   out_minshare_max_diff  4ª de la voraz, adoptada y luego devuelta por `confirma()`
#   relative_perf_diff     5ª de la voraz, adoptada
#   n_missing_diff         6ª, rechazada por el jackknife de temporada
#   elo_z_diff             el corazón de `fuerza`, el dominio que bate a las 59 juntas
#   strength_attack_diff   el modelo de goles, la otra mitad de `fuerza`
#   is_derby               contexto, el dominio con ganancia negativa (-0.025)
CANDIDATAS = ("out_minshare_max_diff", "relative_perf_diff", "n_missing_diff",
              "elo_z_diff", "strength_attack_diff", "is_derby")

VENTANAS = {
    "seleccion (19/20-21/22)": ("2019/20", "2020/21", "2021/22"),
    "abierto (17/18-24/25)": None,
}

N_BOOT = 400
SEMILLA_BOOT = 7

# La razón entre el listón y el error típico que el proyecto ya eligió para el RPS:
# 2e-4 / 0.00026. Se conserva para que los tres criterios paren con la misma exigencia
# y no con tres severidades distintas elegidas de una en una.
RAZON = 2e-4 / 0.00026

OUT = _RAIZ / "outputs" / "models" / "seleccion"
TABLA = OUT / "tolerancia.csv"
RESUMEN = OUT / "tolerancia.json"

_COLS = ["match_key", "season", "jornada", "y_num"]
PROB = ["p_home", "p_draw", "p_away"]


def _evalua(cols: list[str], nombre: str, feat: pd.DataFrame) -> pd.DataFrame:
    """Un conjunto, ajustado una vez sobre las ocho temporadas abiertas."""
    t0 = time.time()
    e = lb.evalua(list(cols), FAMILIA, cfg=mz.cfg_abierto(), feat=feat,
                  evaluar="validacion", cuotas=CUOTAS, n_boot=0,
                  registrar=False, verbose=False)
    p = e.pred[_COLS + PROB + bt.CUOTA_COLS].copy()
    p["season"] = p["season"].astype(str)
    print(f"  {nombre:24s} k={len(cols):2d}  n={len(p):4d}  "
          f"ausentes={len(e.ausentes)}  [{time.time() - t0:.0f}s]", flush=True)
    return p


def _liquida(d: pd.DataFrame) -> dict[str, np.ndarray]:
    """Las tres métricas partido a partido, con la liquidación plana de Manuel.

    `ok` es el acierto del signo señalado; `ben` es la ganancia de una unidad plana a
    ese signo —la cuota menos uno si acierta, menos uno si falla—; `rps` es el RPS del
    partido. Las tres se promedian para dar el criterio, así que la diferencia de medias
    entre dos conjuntos es la media de las diferencias por partido, y es ahí donde se
    puede emparejar.
    """
    P = d[PROB].to_numpy(dtype=float)
    y = d["y_num"].to_numpy(dtype=int)
    fila = np.arange(len(d))
    sel = P.argmax(axis=1)
    ok = sel == y
    Q = d[bt.CUOTA_COLS].to_numpy(dtype=float)
    return {"accuracy": ok.astype(float),
            "ventaja": np.where(ok, Q[fila, sel] - 1.0, -1.0),
            "rps": ev.rps_vector(y, P)}


def _ee(marco: pd.DataFrame, col: str) -> dict:
    """Media de la diferencia emparejada, su ee iid y su ee de bloque de jornada.

    El de bloque es el que vale: los partidos de una misma jornada comparten el estado
    del mundo de esa semana y el iid los cuenta como independientes. Se reportan los dos
    porque su cociente dice cuánta correlación intrajornada hay, que es información
    sobre el dato y no solo sobre el método.
    """
    d = marco[col].to_numpy(dtype=float)
    # El NaN se corta aquí y no se salta. `block_bootstrap` promedia con pandas, que
    # ignora los faltantes sin avisar, así que un eje de cuota incompleto daría un error
    # típico finito calculado sobre un subconjunto mudo — es el defecto que tuvo la
    # primera pasada de este módulo con `cierre_avg`, que no existe antes de 2019/20.
    if not np.isfinite(d).all():
        raise ValueError(
            f"{col}: {int((~np.isfinite(d)).sum())} de {len(d)} partidos sin cifra; "
            f"el eje de cuota no cubre la ventana y el promedio sería mudo")
    iid = float(d.std(ddof=1) / np.sqrt(len(d))) if len(d) > 1 else 0.0
    r = ev.block_bootstrap(marco, lambda s: float(s[col].mean()),
                           n_boot=N_BOOT, seed=SEMILLA_BOOT)
    return {"media": float(d.mean()), "sd_partido": float(d.std(ddof=1)),
            "ee_iid": iid, "ee_bloque": float(r["ee"]),
            "inf": float(r["inf"]), "sup": float(r["sup"])}


def main() -> int:
    t0 = time.time()
    feat = lb.datos()
    semilla = list(sl.SEMILLA_MERCADO)

    print(f"\nliquidación «{CUOTAS}», política «favorito» (una unidad plana a todo)")
    print(f"arranque bajado a {sl.WARMUP_CONFIRMACION} para abrir 17/18 y 18/19\n")
    print("ajustes:")
    base = _evalua(semilla, "semilla", feat)

    # Cobertura del eje de cuota, declarada antes de medir: sin esto, una ventana con
    # precio incompleto da un error típico sobre un subconjunto que nadie ha contado.
    con = base[bt.CUOTA_COLS].notna().all(axis=1)
    print("\ncobertura del precio por temporada:")
    for s, g in base.assign(ok=con).groupby("season"):
        print(f"  {s}  {int(g['ok'].sum()):3d}/{len(g):3d}", flush=True)
    brazos = {c: _evalua(semilla + [c], c, feat) for c in CANDIDATAS}

    filas = []
    for cand, p in brazos.items():
        d = base.merge(p.drop(columns=["season", "jornada", "y_num"]
                              + bt.CUOTA_COLS),
                       on="match_key", suffixes=("_a", "_b"), how="inner")
        a = _liquida(d.rename(columns={f"{c}_a": c for c in PROB}))
        b = _liquida(d.rename(columns={f"{c}_b": c for c in PROB}))
        dif = d[["season", "jornada"]].copy()
        for k in a:
            dif[k] = b[k] - a[k]          # signo + = la candidata mejora
        dif["rps"] = -dif["rps"]          # el RPS se minimiza: se invierte el signo
        for ventana, temporadas in VENTANAS.items():
            sub = (dif if temporadas is None
                   else dif.loc[dif["season"].isin(temporadas)]).reset_index(drop=True)
            for crit in ("rps", "accuracy", "ventaja"):
                filas.append({"candidata": cand, "ventana": ventana,
                              "criterio": crit, "n": len(sub), **_ee(sub, crit)})

    tab = pd.DataFrame(filas)
    OUT.mkdir(parents=True, exist_ok=True)
    tab.to_csv(TABLA, index=False, encoding="utf-8")
    _informe(tab)
    print(f"\n{time.time() - t0:.0f} s\n{TABLA}\n{RESUMEN}")
    return 0


def _informe(tab: pd.DataFrame) -> None:
    resumen = {}
    for ventana in VENTANAS:
        sub = tab.loc[tab["ventana"] == ventana]
        print(f"\n{'=' * 86}")
        print(f"{ventana}  ·  n={int(sub['n'].iloc[0])} partidos")
        print(f"{'=' * 86}")
        print(f"{'criterio':<10} {'candidata':<24} {'media':>11} "
              f"{'sd/partido':>11} {'ee iid':>10} {'ee bloque':>10} {'coc.':>5}")
        for crit in ("rps", "accuracy", "ventaja"):
            s = sub.loc[sub["criterio"] == crit]
            for _, r in s.iterrows():
                print(f"{crit:<10} {r['candidata']:<24} {r['media']:>+11.6f} "
                      f"{r['sd_partido']:>11.4f} {r['ee_iid']:>10.6f} "
                      f"{r['ee_bloque']:>10.6f} "
                      f"{r['ee_bloque'] / max(r['ee_iid'], 1e-12):>5.2f}")
            ee = float(s["ee_bloque"].median())
            print(f"{'':<10} {'MEDIANA':<24} {'':>11} {'':>11} {'':>10} "
                  f"{ee:>10.6f}\n")
            resumen.setdefault(ventana, {})[crit] = {
                "ee_bloque_mediana": ee,
                "tolerancia_propuesta": float(RAZON * ee),
                "n": int(s["n"].iloc[0]),
            }

    print(f"{'=' * 86}")
    print(f"TOLERANCIA PROPUESTA = {RAZON:.2f} x el error típico de bloque, que es la "
          f"razón\nque el proyecto ya eligió para el RPS (2e-4 sobre 0.00026)")
    print(f"{'=' * 86}")
    print(f"{'ventana':<26} {'criterio':<10} {'ee bloque':>11} "
          f"{'tolerancia':>12}   en lenguaje llano")
    llano = {"rps": "de RPS", "accuracy": "pp de acierto",
             "ventaja": "pp de ROI plano"}
    for ventana, crits in resumen.items():
        for crit, r in crits.items():
            tol = r["tolerancia_propuesta"]
            escala = 1.0 if crit == "rps" else 100.0
            print(f"{ventana:<26} {crit:<10} {r['ee_bloque_mediana']:>11.6f} "
                  f"{tol:>12.6f}   {escala * tol:+.4f} {llano[crit]}")
    RESUMEN.write_text(json.dumps(resumen, indent=2, ensure_ascii=False),
                       encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
