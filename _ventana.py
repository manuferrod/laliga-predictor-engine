"""¿Con cuánto pasado hay que entrenar, y pesa más lo reciente?

Dos mandos del protocolo que nunca se han medido y que afectan a TODOS los modelos
por igual, no a un conjunto de variables concreto:

  · `window`: expansiva —todo el pasado disponible— o deslizante de N temporadas.
  · `halflife_days`: peso de recencia. Un partido disputado hace `halflife` días
    pesa la mitad que uno de hoy. `None` es sin peso.

La hipótesis que hay que poner a prueba es la de Manuel, y es razonable: el fútbol
de hace nueve temporadas puede no informar sobre el de la jornada que viene, y si es
así una ventana corta con peso de recencia tiene que ganar a la expansiva.

Se mide sobre el bloque de SELECCIÓN (2019/20-2021/22, 1140 partidos), que es donde
se diseña (regla 6). Con dos conjuntos, para que la respuesta no dependa de uno:
la semilla de mercado de tres variables y el conjunto de cinco. La comparación es
emparejada partido a partido contra la configuración vigente —expansiva, sin peso—
y el intervalo remuestrea jornadas enteras.
"""
import sys
import itertools
import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev            # noqa: E402
import seleccion as sl           # noqa: E402

SALIDA = "outputs/models/seleccion/ventana.csv"
VENTANAS = [("expansiva", 0)] + [("deslizante", n) for n in (2, 3, 4, 5, 6)]
SEMIVIDAS = [None, 180.0, 365.0, 730.0, 1460.0]
RAW = ["mkt_p_home_raw", "mkt_p_draw_raw", "mkt_p_away_raw"]
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
        for (win, n), hl in itertools.product(VENTANAS, SEMIVIDAS):
            cfg = pt.ProtocolConfig(regime="semanal", window=win,
                                    sliding_seasons=n or pt.SLIDING_SEASONS,
                                    halflife_days=hl)
            etiq = (f"{win}{'' if win == 'expansiva' else f'-{n}'}"
                    f" · {'sin peso' if hl is None else f'hl={hl:g}d'}")
            try:
                p = puntua(cols, cfg, feat)
            except Exception as exc:                        # noqa: BLE001
                print(f"  {etiq:<30} no evaluable: "
                      f"{type(exc).__name__}: {exc}", flush=True)
                continue
            q = p.set_index(CLAVE)
            fila = {"conjunto": nombre, "ventana": win, "temporadas": n,
                    "semivida": hl, "etiqueta": etiq,
                    "rps": p["rps"].mean(), "acierto": p["acierto"].mean(),
                    "n": len(p)}
            if base is None:
                # La configuración vigente, que es la primera de la rejilla.
                base = q
                mkt = p[RAW].to_numpy(float)
                mkt = mkt / mkt.sum(1, keepdims=True)
                fila["precio"] = float(
                    ev.rps_vector(ev._objetivo(p), mkt).mean())
                fila["gana"] = 0.0
                print(f"  {etiq:<30} RPS {fila['rps']:.6f}  "
                      f"acierto {fila['acierto']:6.2%}   (vigente; precio crudo "
                      f"{fila['precio']:.6f})", flush=True)
            else:
                com = base.index.intersection(q.index)
                dif = pd.DataFrame(
                    {"dif": base.loc[com, "rps"] - q.loc[com, "rps"]}
                ).reset_index()
                iv = ev.block_bootstrap(dif, lambda d: float(d["dif"].mean()),
                                        n_boot=1000, seed=7)
                fila.update(gana=dif["dif"].mean(), inf=iv["inf"],
                            sup=iv["sup"], n_com=len(com))
                print(f"  {etiq:<30} RPS {fila['rps']:.6f}  "
                      f"acierto {fila['acierto']:6.2%}   "
                      f"gana {fila['gana']:+.6f} "
                      f"[{iv['inf']:+.6f}, {iv['sup']:+.6f}]", flush=True)
            filas.append(fila)
            pd.DataFrame(filas).to_csv(SALIDA, index=False)

    t = pd.DataFrame(filas)
    print("\n\n===== resumen, de mejor a peor RPS dentro de cada conjunto =====")
    for nombre in conjuntos:
        s = t[t["conjunto"] == nombre].sort_values("rps")
        print(f"\n--- {nombre} ---")
        print(s[["etiqueta", "rps", "acierto", "gana", "inf", "sup"]]
              .to_string(index=False))


if __name__ == "__main__":
    main()
