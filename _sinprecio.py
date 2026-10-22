"""¿Las variables de juego no miden nada, o el precio ya lo ha leído?

Todo lo que se ha medido hasta aquí evalúa las variables **como añadido** a un modelo
que ya lleva el precio dentro: la voraz parte de la semilla de mercado y pregunta qué
suma cada candidata. De ahí salió un resultado negativo y bien medido —el banco entero
no bate a la semilla desnuda en 55 de 57 pasos—, y de ahí salió también una frase que
se escribió en tres sitios:

    «no significa que las 62 variables restantes no midan nada: significa que lo que
     miden ya está dentro del precio»

Esa frase es una interpretación, no una medida. Nadie ha ajustado nunca las variables
de juego **sin** el precio, y las dos lecturas posibles son muy distintas para la
memoria:

  · si el modelo sin precio se queda pegado a la previa de frecuencias, entonces las
    variables de juego de este banco de verdad no miden casi nada, y la frase de arriba
    es falsa y hay que corregirla;
  · si el modelo sin precio se sitúa muy por encima de la previa —y bastante cerca del
    precio—, entonces las variables sí miden, y el resultado negativo cambia de
    significado: no es «mi banco es pobre», es «el mercado ya ha incorporado lo que mi
    banco observa», que es un hallazgo publicable y mucho más interesante.

Se mide sobre el bloque de SELECCIÓN (2019/20–2021/22). Es el bloque que ya está
gastado —la voraz eligió ahí— y por eso mirarlo otra vez no cuesta presupuesto: sus
cifras no son citables (regla 5) y aquí no se decide nada, solo se describe. La
alternativa, mirar el bloque de medición, convertiría la única cifra publicable del
trabajo en un segundo bloque de selección (regla 6).

Uso:
    python _sinprecio.py            # el estudio completo
    python _sinprecio.py --rapido   # solo las cuatro lecturas principales
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_RAIZ = Path(__file__).resolve().parent
for _sub in ("scripts", "scripts/models", "scripts/features"):
    sys.path.insert(0, str(_RAIZ / _sub))

import evaluate as ev           # noqa: E402
import laboratorio as lb        # noqa: E402
import seleccion as sl          # noqa: E402

SALIDA = _RAIZ / "outputs" / "models" / "seleccion" / "sinprecio.csv"


def _prior(pred: pd.DataFrame) -> dict[str, float]:
    """RPS y acierto de la previa de frecuencias sobre los mismos partidos.

    Es el suelo del cuadro. Se calcula con `ev.reference_probs`, que la construye con
    la distribución de clases del propio conjunto evaluado —ligeramente optimista, y
    admisible justo porque su papel es ser un suelo que el modelo tiene que superar.
    """
    P = ev.reference_probs(pred, "prior")
    y = ev._objetivo(pred)
    ok = np.isfinite(y) & np.isfinite(P).all(axis=1)
    y, P = y[ok].astype(int), P[ok]
    # El RPS se le pide a `ev.rps_vector` en lugar de reimplementarlo aquí: dos
    # definiciones de la métrica principal que empiezan iguales se separan el día que
    # una de las dos cambie, y entonces el suelo del cuadro dejaría de ser comparable
    # con las cifras del modelo que están justo al lado.
    return {"rps": float(np.mean(ev.rps_vector(y, P))),
            "accuracy": float(np.mean(P.argmax(axis=1) == y)),
            "n": int(ok.sum())}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    rapido = "--rapido" in argv

    feat = lb.datos()
    cands = sl.candidatas(feat, regime="semanal")
    mapa = lb.dominios(cands)
    mercado = list(mapa["mercado"])
    juego = [c for c in cands if c not in set(mercado)]
    semilla = list(sl.SEMILLA_MERCADO)

    print(f"candidatas {len(cands)} · mercado {len(mercado)} · juego {len(juego)}")
    print(f"bloque {sl.BLOQUE_SELECCION} · familia {sl.FAMILIA}\n", flush=True)

    # El orden importa: el primer ensayo paga la caché de estados de preprocesado
    # (que es lo caro) y los siguientes la reutilizan, porque comparten `cfg`.
    planes: list[tuple[str, list[str]]] = [
        ("semilla_mercado", semilla),
        ("juego_sin_precio", juego),
        ("mercado_entero", mercado),
        ("todo", list(cands)),
    ]
    if not rapido:
        planes += [(f"solo_{d}", list(cols))
                   for d, cols in mapa.items() if d != "mercado"]

    filas: list[dict] = []
    suelo: dict[str, float] | None = None
    for nombre, cols in planes:
        t0 = time.time()
        try:
            e = lb.evalua(cols, sl.FAMILIA, nombre=nombre,
                          evaluar=sl.BLOQUE_SELECCION, feat=feat,
                          registrar=False, verbose=False)
        except Exception as exc:                            # noqa: BLE001
            print(f"  {nombre:20s} FALLA  {type(exc).__name__}: {exc}", flush=True)
            continue
        if suelo is None:
            suelo = _prior(e.pred)
            print(f"  {'previa_frecuencias':20s} k=--  RPS {suelo['rps']:.6f}  "
                  f"acc {100 * suelo['accuracy']:5.2f} %  n={suelo['n']}", flush=True)
            filas.append({"conjunto": "previa_frecuencias", "k": 0,
                          "rps": suelo["rps"], "accuracy": suelo["accuracy"],
                          "logloss": float("nan"), "roi": float("nan"),
                          "roi_ref": float("nan"), "n": suelo["n"],
                          "n_vistas": 0, "ausentes": ""})
        vistas = len(cols) - len(e.ausentes)
        filas.append({
            "conjunto": nombre, "k": len(cols), "rps": e.rps,
            "accuracy": e.accuracy, "logloss": float(e.metricas["logloss"]),
            "roi": e.roi, "roi_ref": e.roi_referencia, "n": int(e.metricas["n"]),
            "n_vistas": vistas, "ausentes": " ".join(e.ausentes)})
        print(f"  {nombre:20s} k={len(cols):3d}  RPS {e.rps:.6f}  "
              f"acc {100 * e.accuracy:5.2f} %  ROI {100 * e.roi:+6.2f} %  "
              f"vistas {vistas}  [{time.time() - t0:.0f}s]", flush=True)

    tab = pd.DataFrame(filas)
    if suelo is not None and not tab.empty:
        # La lectura que motiva el estudio: qué parte del camino entre la previa de
        # frecuencias y el precio recorre cada conjunto. Cero es la previa; uno es el
        # mercado entero. Es la forma honesta de contestar «¿miden algo?» sin que la
        # respuesta dependa de la cuarta cifra decimal de un RPS.
        # El techo es la SEMILLA, no el dominio de mercado entero: la semilla es el
        # modelo que el proyecto publica, y además es el mejor de los dos (las tres
        # columnas de precio que sobran del dominio empeoran el RPS en 0.000449). Con
        # el dominio entero como techo, la semilla salía a 1.015 y un uno de referencia
        # que se puede rebasar no sirve de referencia.
        base = suelo["rps"]
        techo = tab.loc[tab["conjunto"] == "semilla_mercado", "rps"]
        if not techo.empty and base > float(techo.iloc[0]):
            t = float(techo.iloc[0])
            tab["camino"] = (base - tab["rps"]) / (base - t)

    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    tab.to_csv(SALIDA, index=False, encoding="utf-8")
    print(f"\n{SALIDA}")
    print(tab.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
