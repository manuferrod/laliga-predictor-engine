"""El mercado más «las mejores del resto», con las mejores elegidas sin mirar el precio.

Queda una ruta que ni la voraz ni `_bloques.py` recorren, y es la que la pregunta pide
en su forma más literal. La voraz ordena las candidatas por lo que añaden **dado el
precio**, y ése es exactamente el sitio donde vive la confusión que `_sinprecio.py`
destapó: una variable que mide muy bien lo mismo que el precio puntúa cero en ese orden,
y queda al fondo de la cola no por mala sino por redundante. Si luego se construye el
conjunto por la cabeza de esa cola, se está escogiendo entre las que menos miden.

Así que aquí el orden se construye **sin el precio delante**: cada variable de juego
sola, k=1, y se ordenan por su RPS propio. En ese cuadro ninguna puede quedar tapada por
el mercado, porque el mercado no está. Después se junta la semilla con las N mejores de
esa lista, para N creciente, cada conjunto con SU penalización.

Esta fase es **deliberadamente generosa** con la hipótesis, y conviene decirlo: ordenar
59 variables sobre los mismos 1140 partidos y quedarse con la cabeza es sesgo de máximo
en estado puro, y el conjunto resultante llega a la comparación habiendo visto ya los
partidos en los que se le puntúa. Se hace así a propósito: si ni con esa ventaja gana a
la semilla, la respuesta es firme, y si gana, hay una puerta que no perdona —
`seleccion.confirma()` sobre 2017/18-2018/19, que es donde las dos variables de la voraz
se cayeron—.

Sobre el bloque de SELECCIÓN (reglas 5 y 6): ninguna cifra de aquí es citable.

Uso:
    python _mejores.py            # las 59 solas y luego los conjuntos acumulados
    python _mejores.py --orden    # solo el orden por mérito propio, sin la segunda fase
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

_RAIZ = Path(__file__).resolve().parent
for _sub in ("scripts", "scripts/models", "scripts/features"):
    sys.path.insert(0, str(_RAIZ / _sub))

import evaluate as ev           # noqa: E402
import laboratorio as lb        # noqa: E402
import protocol as pt           # noqa: E402
import seleccion as sl          # noqa: E402

SALIDA = _RAIZ / "outputs" / "models" / "seleccion" / "mejores.csv"
ORDEN = _RAIZ / "outputs" / "models" / "seleccion" / "mejores_orden.csv"

# Para ordenar las 59 solas basta una penalización, y tiene que ser la misma para todas
# o el orden mezclaría mérito con configuración. Se usa la declarada en `laboratorio`
# (C=0.3, L2): con k=1 la penalización casi no muerde, así que la elección es inocua
# aquí, y ser la de la casa evita introducir un mando nuevo donde no hace falta.
HIPER_ORDEN = {"C": 0.3, "l1_ratio": 0.0}

# Para los conjuntos acumulados sí importa, y mucho: `_ridge.py` midió 0.0061 de RPS
# entre la mejor y la peor penalización sobre las mismas 65 columnas.
REJILLA: list[dict] = [
    {"C": 0.01, "l1_ratio": 1.0},
    {"C": 0.03, "l1_ratio": 1.0},
    {"C": 0.1, "l1_ratio": 1.0},
    {"C": 0.3, "l1_ratio": 0.0},    # el de la búsqueda
    {"C": 1.0, "l1_ratio": 0.0},
]

CORTES = (3, 5, 8, 12, 20, 30)
CLAVE = ["season", "jornada", "match_key"]


def _puntua(cols, h, cfg, feat) -> pd.DataFrame | None:
    try:
        e = lb.evalua(list(cols), sl.FAMILIA, hiper=h, cfg=cfg, feat=feat,
                      evaluar=sl.BLOQUE_SELECCION, n_boot=0,
                      registrar=False, verbose=False)
    except Exception as exc:                                # noqa: BLE001
        print(f"      FALLA {type(exc).__name__}: {exc}", flush=True)
        return None
    p = e.pred.loc[ev.evaluable(e.pred)].copy()
    p["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
    p["acierto"] = (ev._terna(p, ev.PROB_COLS).argmax(axis=1)
                    == ev._objetivo(p)).astype(float)
    return p.set_index(CLAVE)


def _contra(base: pd.DataFrame, otro: pd.DataFrame) -> dict:
    """Diferencia emparejada sobre la intersección. Positivo = el otro gana."""
    com = base.index.intersection(otro.index)
    d = pd.DataFrame({"dif": base.loc[com, "rps"] - otro.loc[com, "rps"]})
    iv = ev.block_bootstrap(d.reset_index(),
                            lambda x: float(x["dif"].mean()),
                            n_boot=2000, seed=7)
    return {"gana": float(d["dif"].mean()), "inf": iv["inf"], "sup": iv["sup"],
            "n_par": int(len(com))}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    solo_orden = "--orden" in argv

    cfg = pt.ProtocolConfig(regime="semanal")
    feat = lb.datos()
    cands = sl.candidatas(feat, regime="semanal")
    mapa = lb.dominios(cands)
    mercado = set(mapa["mercado"])
    juego = [c for c in cands if c not in mercado]
    semilla = list(sl.SEMILLA_MERCADO)

    print(f"bloque {sl.BLOQUE_SELECCION} · familia {sl.FAMILIA}", flush=True)
    print(f"ordenando {len(juego)} variables de juego por merito propio (k=1, "
          f"sin el precio delante)\n", flush=True)

    filas_orden: list[dict] = []
    for i, c in enumerate(juego, 1):
        t0 = time.time()
        p = _puntua([c], HIPER_ORDEN, cfg, feat)
        if p is None:
            continue
        filas_orden.append({"variable": c, "rps": float(p["rps"].mean()),
                            "accuracy": float(p["acierto"].mean()),
                            "n": int(len(p))})
        print(f"  [{i:2d}/{len(juego)}] {c:<34} RPS {p['rps'].mean():.6f}  "
              f"acc {100 * p['acierto'].mean():5.2f} %  "
              f"[{time.time() - t0:.0f}s]", flush=True)

    orden = pd.DataFrame(filas_orden).sort_values("rps").reset_index(drop=True)
    orden["puesto"] = orden.index + 1
    ORDEN.parent.mkdir(parents=True, exist_ok=True)
    orden.to_csv(ORDEN, index=False, encoding="utf-8")
    print(f"\n=== orden por merito propio, las 15 primeras ===\n"
          f"{orden.head(15).to_string(index=False)}", flush=True)
    print(f"\n{ORDEN}", flush=True)
    if solo_orden:
        return 0

    ranking = list(orden["variable"])
    conjuntos: list[tuple[str, list[str]]] = [("semilla", semilla)]
    for n in CORTES:
        if n > len(ranking):
            break
        extra = [c for c in ranking[:n] if c not in set(semilla)]
        conjuntos.append((f"semilla+top{n}", semilla + extra))

    print(f"\n=== semilla + las N mejores, {len(conjuntos)} conjuntos x "
          f"{len(REJILLA)} penalizaciones ===", flush=True)
    filas: list[dict] = []
    mejores: dict[str, pd.DataFrame] = {}
    for nombre, cols in conjuntos:
        print(f"{nombre}  (k={len(cols)})", flush=True)
        mejor_rps, mejor_p, mejor_h = float("inf"), None, None
        for h in REJILLA:
            t0 = time.time()
            p = _puntua(cols, h, cfg, feat)
            if p is None:
                continue
            r = float(p["rps"].mean())
            print(f"    C={h['C']}/l1={h['l1_ratio']:<4} RPS {r:.6f}  "
                  f"acc {100 * p['acierto'].mean():5.2f} %  "
                  f"[{time.time() - t0:.0f}s]", flush=True)
            filas.append({"conjunto": nombre, "k": len(cols), "C": h["C"],
                          "l1_ratio": h["l1_ratio"], "rps": r,
                          "accuracy": float(p["acierto"].mean()),
                          "n": int(len(p))})
            if r < mejor_rps:
                mejor_rps, mejor_p, mejor_h = r, p, h
        if mejor_p is not None:
            mejores[nombre] = mejor_p
            print(f"    -> mejor C={mejor_h['C']}/l1={mejor_h['l1_ratio']} "
                  f"con {mejor_rps:.6f}", flush=True)

    tab = pd.DataFrame(filas)
    if "semilla" in mejores:
        base = mejores["semilla"]
        print("\n=== cada conjunto en su mejor penalizacion, contra la semilla en "
              "la suya ===", flush=True)
        comp: list[dict] = []
        for nombre, p in mejores.items():
            if nombre == "semilla":
                continue
            c = _contra(base, p)
            comp.append({"conjunto": nombre, "rps": float(p["rps"].mean()), **c})
            print(f"{nombre:<20} RPS {p['rps'].mean():.6f}  "
                  f"gana {c['gana']:+.6f}  "
                  f"[{c['inf']:+.6f}, {c['sup']:+.6f}]", flush=True)
        if comp:
            cm = pd.DataFrame(comp).sort_values("gana", ascending=False)
            tab = tab.merge(cm[["conjunto", "gana", "inf", "sup"]],
                            on="conjunto", how="left")
            print(f"\nsemilla sola: {base['rps'].mean():.6f} "
                  f"(acc {100 * base['acierto'].mean():.2f} %)", flush=True)
            gana = cm.loc[cm["gana"] > 0]
            if gana.empty:
                print("NINGUN conjunto mejora a la semilla, ni eligiendo las "
                      "mejores sin el precio delante, ni con su mejor "
                      "penalizacion, ni habiendo visto los partidos en los que "
                      "se le puntua. La ruta queda cerrada por las cuatro vias.",
                      flush=True)
            else:
                print(f"conjuntos que MEJORAN a la semilla: "
                      f"{', '.join(gana['conjunto'])}\n"
                      f"-> a `seleccion.confirma()` sobre 2017/18-2018/19",
                      flush=True)

    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    tab.to_csv(SALIDA, index=False, encoding="utf-8")
    print(f"\n{SALIDA}")
    print(tab.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
