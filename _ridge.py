"""Si la voraz no puede distinguir señal de ruido, quitarle la elección.

La búsqueda hacia delante tiene cincuenta y nueve oportunidades por paso de confundir
ruido con mérito, y el bloque de medición ya dijo que las aprovechó. La alternativa no
es buscar mejor: es no buscar. Una logística penalizada sobre TODO el banco no elige
ninguna variable mirando cifras de evaluación —la penalización encoge los coeficientes
dentro de cada ventana de entrenamiento— y por tanto no tiene sesgo de máximo.

Queda un solo mando, la fuerza de la penalización, con cinco valores en lugar de
59^k combinaciones. Y se compara contra los dos listones que importan: la semilla de
mercado ajustada y el precio crudo, sobre los mismos partidos.

Todo sobre el bloque de SELECCIÓN, que es donde se diseña (regla 6).
"""
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev            # noqa: E402
import seleccion as sl           # noqa: E402

REJILLA = [{"C": c, "l1_ratio": r}
           for r in (0.0, 1.0) for c in (0.01, 0.03, 0.1, 0.3)]
RAW = ["mkt_p_home_raw", "mkt_p_draw_raw", "mkt_p_away_raw"]


def rps_de(p):
    return float(ev.rps_vector(ev._objetivo(p),
                               ev._terna(p, ev.PROB_COLS)).mean())


def main() -> None:
    cfg = pt.ProtocolConfig(regime="semanal")
    feat = lb.datos()
    sem = list(sl.SEMILLA_MERCADO)
    pool = list(sl.candidatas(feat))
    print(f"banco completo: {len(pool)} variables", flush=True)

    def mide(cols, hiper, nombre):
        e = lb.evalua(cols, "logistica", hiper=hiper, cfg=cfg, feat=feat,
                      evaluar=sl.BLOQUE_SELECCION, n_boot=0,
                      registrar=False, verbose=False)
        p = e.pred.loc[ev.evaluable(e.pred)].copy()
        print(f"{nombre:<34} RPS {rps_de(p):.6f}  n={len(p)}", flush=True)
        return p

    # Los dos listones. El precio crudo no ajusta nada: es la columna del mercado.
    p0 = mide(sem, lb.HIPER["logistica"], "semilla k3 (C=0.3, L2)")
    mkt = p0[RAW].to_numpy(float)
    mkt = mkt / mkt.sum(1, keepdims=True)
    rps_precio = float(ev.rps_vector(ev._objetivo(p0), mkt).mean())
    print(f"{'precio crudo de mercado':<34} RPS {rps_precio:.6f}", flush=True)
    base = p0.set_index(["season", "jornada", "match_key"])
    base["rps"] = ev.rps_vector(ev._objetivo(p0), ev._terna(p0, ev.PROB_COLS))

    print(f"\n--- banco entero ({len(pool)} variables), penalización variable ---",
          flush=True)
    filas = []
    for h in REJILLA:
        etiqueta = f"todo C={h['C']} l1={h['l1_ratio']}"
        try:
            p = mide(pool, h, etiqueta)
        except Exception as exc:                        # noqa: BLE001
            print(f"{etiqueta:<34} no evaluable: {type(exc).__name__}: {exc}",
                  flush=True)
            continue
        q = p.set_index(["season", "jornada", "match_key"])
        q["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
        com = base.index.intersection(q.index)
        dif = pd.DataFrame({"dif": base.loc[com, "rps"] - q.loc[com, "rps"]})
        dif = dif.reset_index()
        iv = ev.block_bootstrap(dif, lambda d: float(d["dif"].mean()),
                                n_boot=1000, seed=7)
        filas.append({"hiper": etiqueta, "rps": rps_de(p),
                      "gana_a_semilla": dif["dif"].mean(),
                      "inf": iv["inf"], "sup": iv["sup"]})
        print(f"{'':<34} frente a la semilla {dif['dif'].mean():+.6f} "
              f"[{iv['inf']:+.6f}, {iv['sup']:+.6f}]", flush=True)

    t = pd.DataFrame(filas).sort_values("rps")
    print("\n--- resumen, de mejor a peor RPS ---")
    print(t.to_string(index=False))
    print(f"\nlistón semilla k3 : {rps_de(p0):.6f}")
    print(f"listón precio crudo: {rps_precio:.6f}")
    t.to_csv("outputs/models/seleccion/ridge.csv", index=False)


if __name__ == "__main__":
    main()
