"""¿Cada cuánto hay que reaprender el preprocesado?

El tercer mando del protocolo que nunca se había medido. Ojo con lo que gobierna,
porque el nombre engaña: NO es la cadencia de reajuste del modelo —el modelo se vuelve
a ajustar desde cero en cada jornada, siempre, y eso no es un parámetro— sino la del
ESTADO del preprocesado: los valores de imputación, las lambdas de Yeo-Johnson, las
medias y desviaciones del escalado y la lista de columnas podadas por correlación.

  · `temporada` (vigente): un estado por temporada, compartido por sus 38 jornadas.
  · `jornada`: estado nuevo en cada pliegue. Es el más fiel —usa todo el pasado
    disponible en el momento de predecir— y el más caro.
  · `unico`: un solo estado ajustado sobre TODO el desarrollo. El propio `_StateCache`
    lo declara como término de comparación y no como protocolo válido, porque mira el
    futuro del pliegue. Se mide precisamente para poner número a lo que esa fuga
    regalaría: si `unico` no gana, la pregunta se cierra por arriba.

La diferencia entre `temporada` y `jornada` es cuánta información del propio curso
entra en el preprocesado. Con `temporada`, la jornada 38 se preprocesa con un estado
aprendido antes de la jornada 1; si las distribuciones se mueven dentro del curso,
`jornada` tiene que ganar. Medirlo es la única forma de saber si ese desfase importa.

Se mide sobre el bloque de SELECCIÓN y con los dos conjuntos, igual que la ventana.
Comparación emparejada partido a partido contra la cadencia vigente, con el intervalo
remuestreando jornadas enteras.
"""
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev            # noqa: E402
import seleccion as sl           # noqa: E402

SALIDA = "outputs/models/seleccion/cadencia.csv"
CADENCIAS = ["temporada", "jornada", "unico"]
CLAVE = ["season", "jornada", "match_key"]


def puntua(cols, cfg, feat):
    e = lb.evalua(cols, "logistica", cfg=cfg, feat=feat,
                  evaluar=sl.BLOQUE_SELECCION, n_boot=0,
                  registrar=False, verbose=False)
    p = e.pred.loc[ev.evaluable(e.pred)].copy()
    p["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
    p["acierto"] = (np.asarray(ev._terna(p, ev.PROB_COLS)).argmax(1)
                    == np.asarray(ev._objetivo(p))).astype(float)
    return p


def main() -> None:
    feat = lb.datos()
    conjuntos = {"k3 semilla": list(sl.SEMILLA_MERCADO),
                 "k5 conjunto": list(sl.carga()["conjunto"])}

    filas = []
    for nombre, cols in conjuntos.items():
        base = None
        print(f"\n===== {nombre}: {len(cols)} variables =====", flush=True)
        for cad in CADENCIAS:
            cfg = pt.ProtocolConfig(regime="semanal", refit_cadence=cad)
            try:
                p = puntua(cols, cfg, feat)
            except Exception as exc:                        # noqa: BLE001
                print(f"  {cad:<12} no evaluable: "
                      f"{type(exc).__name__}: {exc}", flush=True)
                continue
            q = p.set_index(CLAVE)
            fila = {"conjunto": nombre, "cadencia": cad,
                    "rps": p["rps"].mean(), "acierto": p["acierto"].mean(),
                    "n": len(p)}
            if base is None:
                base = q
                fila["gana"] = 0.0
                print(f"  {cad:<12} RPS {fila['rps']:.6f}  "
                      f"acierto {fila['acierto']:6.2%}   (vigente)", flush=True)
            else:
                com = base.index.intersection(q.index)
                dif = pd.DataFrame(
                    {"dif": base.loc[com, "rps"] - q.loc[com, "rps"]}
                ).reset_index()
                iv = ev.block_bootstrap(dif, lambda d: float(d["dif"].mean()),
                                        n_boot=2000, seed=7)
                fila.update(gana=dif["dif"].mean(), inf=iv["inf"],
                            sup=iv["sup"], ee=iv["ee"], n_com=len(com))
                print(f"  {cad:<12} RPS {fila['rps']:.6f}  "
                      f"acierto {fila['acierto']:6.2%}   "
                      f"gana {fila['gana']:+.6f} "
                      f"[{iv['inf']:+.6f}, {iv['sup']:+.6f}]", flush=True)
            filas.append(fila)
            pd.DataFrame(filas).to_csv(SALIDA, index=False)

    t = pd.DataFrame(filas)
    print("\n\n===== resumen =====")
    print(t.to_string(index=False))
    print("\n`gana` positivo = la cadencia nueva MEJORA sobre la vigente.")
    print("Si `unico` no gana, ni la fuga de distribución futura compraría nada y la")
    print("cadencia vigente se queda por medición, no por herencia.")


if __name__ == "__main__":
    main()
