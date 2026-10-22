"""¿Cuánto ruido se quita agrandando el bloque de selección?

El arranque declarado son tres temporadas (16/17, 17/18, 18/19) y la cobertura de
las variables dice que solo la primera lo necesita: 17/18 y 18/19 tienen 96.4 % y
96.7 % de cobertura media, idéntica a la de 2019/20 (96.7 %) y a la de las temporadas
de medición (97 %). Excluirlas no estaba sustentado.

Lo que se gana es potencia, que es exactamente lo que faltó: la búsqueda fracasó
porque el ruido de una comparación emparejada (0.00026) era del tamaño de las mejoras
reales (0.0003). El ruido baja con la raíz del número de partidos, de modo que pasar
de 1140 a 1900 debería bajarlo a unos 0.00020.

Se mide, no se supone. Para cada arranque candidato se puntúan los mismos dos
conjuntos sobre el bloque de selección que ese arranque define, y se calcula el error
típico de la diferencia emparejada remuestreando jornadas. El bloque de MEDICIÓN no
cambia con el arranque —sigue siendo 2022/23-2024/25, los mismos 1140 partidos— y
eso es lo que hace que esta sea una decisión del lado de la selección.
"""
import sys

import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev           # noqa: E402
import seleccion as sl           # noqa: E402

SALIDA = "outputs/models/seleccion/arranque.csv"
ARRANQUES = {
    "3 temporadas (vigente)": ["2016/17", "2017/18", "2018/19"],
    "2 temporadas": ["2016/17", "2017/18"],
    "1 temporada": ["2016/17"],
}
CLAVE = ["season", "jornada", "match_key"]


def puntua(cols, cfg, feat):
    e = lb.evalua(cols, "logistica", cfg=cfg, feat=feat,
                  evaluar=sl.BLOQUE_SELECCION, n_boot=0,
                  registrar=False, verbose=False)
    p = e.pred.loc[ev.evaluable(e.pred)].copy()
    p["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
    return p


def main() -> None:
    feat = lb.datos()
    sem = list(sl.SEMILLA_MERCADO)
    k5 = list(sl.carga()["conjunto"])

    filas = []
    for nombre, warm in ARRANQUES.items():
        cfg = pt.ProtocolConfig(regime="semanal", warmup_seasons=warm)
        folds = pt.make_folds(feat, cfg, evaluar=sl.BLOQUE_SELECCION)
        temporadas = sorted({f.season for f in folds})
        print(f"\n--- arranque de {nombre} ---", flush=True)
        print(f"bloque de selección: {len(folds)} pliegues, "
              f"{temporadas[0]}-{temporadas[-1]}", flush=True)

        a, b = puntua(sem, cfg, feat), puntua(k5, cfg, feat)
        ia = a.set_index(CLAVE)
        ib = b.set_index(CLAVE)
        com = ia.index.intersection(ib.index)
        dif = pd.DataFrame(
            {"dif": ia.loc[com, "rps"] - ib.loc[com, "rps"]}).reset_index()
        iv = ev.block_bootstrap(dif, lambda d: float(d["dif"].mean()),
                                n_boot=2000, seed=7)
        fila = {"arranque": nombre, "temporadas": len(warm),
                "pliegues": len(folds), "n": len(com),
                "rps_k3": a["rps"].mean(), "rps_k5": b["rps"].mean(),
                "k5_gana": dif["dif"].mean(), "ee": iv["ee"],
                "inf": iv["inf"], "sup": iv["sup"]}
        filas.append(fila)
        print(f"n={fila['n']}  RPS k3 {fila['rps_k3']:.6f}  "
              f"k5 {fila['rps_k5']:.6f}", flush=True)
        print(f"el k5 gana al k3 {fila['k5_gana']:+.6f}  "
              f"ERROR TÍPICO {fila['ee']:.6f}  "
              f"[{iv['inf']:+.6f}, {iv['sup']:+.6f}]", flush=True)
        pd.DataFrame(filas).to_csv(SALIDA, index=False)

    t = pd.DataFrame(filas)
    print("\n===== resumen =====")
    print(t[["arranque", "pliegues", "n", "rps_k3", "rps_k5", "k5_gana", "ee"]]
          .to_string(index=False))
    base = t.iloc[0]["ee"]
    print("\nEl error típico es el listón que una mejora tiene que superar para no "
          "ser ruido.")
    for _, f in t.iterrows():
        print(f"  {f['arranque']:<24} ee {f['ee']:.6f}  "
              f"({f['ee'] / base:.2f}x del vigente)")


if __name__ == "__main__":
    main()
