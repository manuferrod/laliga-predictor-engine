"""El camino entero: de 3 a 65 variables, sin pararse nunca.

Lo que pidió Manuel, literal: añadir en cada paso la variable que más mejora, hasta
llegar al modelo de las sesenta y cinco, y quedarse con el que mejores métricas da.
La primera mitad de eso ya estaba hecha hasta 25 variables; esto lo lleva al final.

LA TRAMPA QUE HAY QUE EVITAR, Y CÓMO SE EVITA
=============================================
«Quedarse con el que mejores métricas da» tiene una respuesta distinta según DÓNDE se
lean las métricas, y la diferencia no es un detalle: es la diferencia entre un número
publicable y uno que no lo es. Si se lee sobre los mismos partidos que eligieron las
variables, crecer casi siempre parece mejorar —cada variable nueva tiene sesenta
oportunidades de ajustarse al ruido de ESE bloque— y el óptimo aparente se va muy
arriba. Esa curva no estima nada del futuro.

De modo que el camino se recorre con el corte adelantado a 2021/22:

    2019/20 - 2020/21   la voraz elige aquí, y solo aquí (760 partidos)
    2021/22             no participa en elegir, y es donde se lee la curva (380)

Las dos columnas se publican juntas, `rps_dentro` y `rps_fuera`, porque la distancia
entre ellas es la medida del sobreajuste de la propia búsqueda y es la cifra que
contesta a la pregunta de si merece la pena crecer.

2022/23 en adelante NO se toca: el bloque de medición ya se leyó una vez y el holdout
sigue sellado (reglas 5, 6 y 7).

FASE 1 escribe el orden de adopción; FASE 2 puntúa los sesenta y tres prefijos. Las
dos dejan parte en disco a cada paso, porque el camino entero son unos dos mil
protocolos walk-forward y conviene poder leerlo a mitad.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev            # noqa: E402
import seleccion as sl           # noqa: E402

OUT = Path("outputs/models/seleccion")
ORDEN = OUT / "camino65_orden.json"
CURVA = OUT / "camino65.csv"
CORTE = "2021/22"
FUERA = "2021/22"

# La ventana de entrenamiento con la que se recorre el camino. Se fija aquí, y no se
# deja al azar del valor por defecto, porque el estudio de ventana (`ventana.csv`) se
# hace ANTES justamente para que esta constante esté decidida: recorrer dos mil
# protocolos con una ventana que luego se cambia sería tirar el cómputo.
VENTANA = {"window": "expansiva", "sliding_seasons": pt.SLIDING_SEASONS,
           "halflife_days": None}


def cfg_elige() -> pt.ProtocolConfig:
    """El protocolo de la FASE 1: el corte adelantado deja 2021/22 fuera."""
    return pt.ProtocolConfig(regime="semanal", corte_seleccion=CORTE, **VENTANA)


def cfg_lee() -> pt.ProtocolConfig:
    """El de la FASE 2. Corte por defecto, para que el bloque de selección cubra
    las tres temporadas y se puedan separar por `season` las que eligieron de la
    que no. Nunca llega a 2022/23: ese es el bloque de medición."""
    return pt.ProtocolConfig(regime="semanal", **VENTANA)


def fase1() -> list[str]:
    if ORDEN.exists():
        orden = json.loads(ORDEN.read_text(encoding="utf-8"))["orden"]
        print(f"FASE 1 ya hecha: {len(orden)} variables en orden de adopción",
              flush=True)
        return orden

    feat = lb.datos()
    cfg = cfg_elige()
    sem = list(sl.SEMILLA_MERCADO)
    pool = sl.criba(feat=feat, cfg=cfg, guardar=False,
                    verbose=False)["candidata"].tolist()
    pool = [c for c in pool if c not in set(sem) and c != "(base)"]
    print(f"FASE 1 · {cfg.describe()}", flush=True)
    print(f"semilla {sem} · {len(pool)} candidatas · hasta "
          f"{len(sem) + len(pool)} variables", flush=True)

    # `tolerancia` muy negativa = adopción forzada en todos los pasos: el camino no
    # se detiene por umbral y llega hasta el final del pool.
    sel = sl.adelante(pool=pool, semilla=sem, criterio="rps", tolerancia=-1.0,
                      anchura=0, max_vars=len(sem) + len(pool), cfg=cfg,
                      feat=feat, guardar=False, verbose=True)

    orden = [p.elegida for p in sel.pasos if p.adoptada]
    ORDEN.write_text(json.dumps(
        {"semilla": sem, "orden": orden, "corte": CORTE,
         "protocolo": cfg.describe(),
         "pasos": [{"paso": p.paso, "elegida": p.elegida, "rps": p.rps,
                    "mejora": p.mejora, "n_probadas": p.n_probadas,
                    "n_variables": p.n_variables} for p in sel.pasos]},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nFASE 1 hecha: {len(orden)} adopciones -> {ORDEN}", flush=True)
    return orden


def fase2(orden: list[str]) -> None:
    feat = lb.datos()
    cfg = cfg_lee()
    sem = list(sl.SEMILLA_MERCADO)
    print(f"\nFASE 2 · {cfg.describe()}", flush=True)
    print(f"{len(orden) + 1} prefijos, de {len(sem)} a "
          f"{len(sem) + len(orden)} variables", flush=True)

    filas = []
    for i in range(len(orden) + 1):
        cols = sem + orden[:i]
        try:
            e = lb.evalua(cols, "logistica", cfg=cfg, feat=feat,
                          evaluar=sl.BLOQUE_SELECCION, n_boot=0,
                          registrar=False, verbose=False)
        except Exception as exc:                            # noqa: BLE001
            print(f"  k={len(cols):>2}  no evaluable: "
                  f"{type(exc).__name__}: {exc}", flush=True)
            continue
        p = e.pred.loc[ev.evaluable(e.pred)].copy()
        p["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
        p["acierto"] = (np.asarray(ev._terna(p, ev.PROB_COLS)).argmax(1)
                        == np.asarray(ev._objetivo(p))).astype(float)
        es_fuera = p["season"].astype(str) == FUERA
        d, f = p[~es_fuera], p[es_fuera]
        fila = {"k": len(cols), "entra": "(semilla)" if i == 0 else orden[i - 1],
                "rps_dentro": d["rps"].mean(), "rps_fuera": f["rps"].mean(),
                "acc_dentro": d["acierto"].mean(),
                "acc_fuera": f["acierto"].mean(),
                "n_dentro": len(d), "n_fuera": len(f)}
        filas.append(fila)
        pd.DataFrame(filas).to_csv(CURVA, index=False)
        print(f"  k={fila['k']:>2}  dentro {fila['rps_dentro']:.6f}  "
              f"fuera {fila['rps_fuera']:.6f}  acc fuera "
              f"{fila['acc_fuera']:6.2%}   entra {fila['entra']}", flush=True)

    t = pd.DataFrame(filas)
    mejor_d = t.loc[t["rps_dentro"].idxmin()]
    mejor_f = t.loc[t["rps_fuera"].idxmin()]
    print("\n===== la curva entera =====")
    print(t.to_string(index=False))
    print(f"\nmejor DENTRO de donde elige: k={int(mejor_d['k'])}  "
          f"RPS {mejor_d['rps_dentro']:.6f}  (y fuera "
          f"{mejor_d['rps_fuera']:.6f})")
    print(f"mejor FUERA de donde elige : k={int(mejor_f['k'])}  "
          f"RPS {mejor_f['rps_fuera']:.6f}")
    print("\nLa primera línea es la curva contaminada; la segunda, la que estima "
          "algo del futuro.")


if __name__ == "__main__":
    fase2(fase1())
