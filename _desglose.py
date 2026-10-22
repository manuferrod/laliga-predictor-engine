"""El conjunto de cinco, temporada a temporada, dentro y fuera de donde se eligió.

El estudio del arranque (`arranque.csv`) dejó una pista que vale más que su propia
pregunta. Al bajar el arranque a una sola temporada, el bloque de selección se come
17/18 y 18/19 —setecientos sesenta partidos que **no participaron en elegir** el
conjunto de cinco, porque la búsqueda se hizo sobre 2019/20-2021/22— y la mejora
agregada del k5 sobre la semilla se desploma de +0.000636 a +0.000195.

Eso solo puede significar una cosa: en esas dos temporadas el k5 pierde. Aquí se mide
en vez de deducirlo de medias redondeadas, y se parte la diferencia emparejada por
temporada con su intervalo.

Lo que está en juego no es el arranque, que es un detalle. Es una TERCERA lectura
independiente del veredicto. Ya hay dos:

    bloque de medición (2022/23-2024/25)   el k5 pierde 0.000608  [-0.00122, -0.00004]

y si 17/18-18/19 apunta en la misma dirección, el conjunto de cinco queda desmentido
por todos los partidos que no lo eligieron, que es la prueba más dura que se le puede
pedir a un conjunto de variables. Las dos lecturas son independientes entre sí: una es
anterior al bloque de selección y la otra posterior.

Nota sobre por qué el RPS sube al añadir temporadas tempranas (0.1932 -> 0.1958): no es
deterioro del modelo sino dificultad del pliegue. En 17/18 el walk-forward entrena con
una sola temporada de pasado, justo en el mínimo de 380 partidos, y las variables de
forma, racha y enfrentamiento directo apenas tienen historia detrás. Son pliegues
legítimos pero más pobres, y por eso cada partido se predice peor.
"""
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev            # noqa: E402
import seleccion as sl           # noqa: E402

SALIDA = "outputs/models/seleccion/desglose.csv"
ELIGIERON = {"2019/20", "2020/21", "2021/22"}
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


def intervalo(dif):
    iv = ev.block_bootstrap(dif, lambda d: float(d["dif"].mean()),
                            n_boot=2000, seed=7)
    return iv


def main() -> None:
    feat = lb.datos()
    cfg = pt.ProtocolConfig(regime="semanal", warmup_seasons=["2016/17"])
    sem = list(sl.SEMILLA_MERCADO)
    k5 = list(sl.carga()["conjunto"])
    print(f"protocolo: {cfg.describe()}", flush=True)
    print(f"semilla {sem}", flush=True)
    print(f"conjunto {k5}\n", flush=True)

    a, b = puntua(sem, cfg, feat), puntua(k5, cfg, feat)
    ia, ib = a.set_index(CLAVE), b.set_index(CLAVE)
    com = ia.index.intersection(ib.index)
    dif = pd.DataFrame({
        "dif": ib.loc[com, "rps"] - ia.loc[com, "rps"],
        "rps_k3": ia.loc[com, "rps"], "rps_k5": ib.loc[com, "rps"],
        "acc_k3": ia.loc[com, "acierto"], "acc_k5": ib.loc[com, "acierto"],
    }).reset_index()
    # `dif` es k5 - k3 en RPS, que es una pérdida: NEGATIVO significa que el k5 gana.
    dif["season"] = dif["season"].astype(str)

    filas = []
    for temp, g in dif.groupby("season"):
        iv = intervalo(g)
        filas.append({"tramo": temp,
                      "eligio": temp in ELIGIERON,
                      "n": len(g), "rps_k3": g["rps_k3"].mean(),
                      "rps_k5": g["rps_k5"].mean(),
                      "acc_k3": g["acc_k3"].mean(),
                      "acc_k5": g["acc_k5"].mean(),
                      "k5_menos_k3": g["dif"].mean(),
                      "ee": iv["ee"], "inf": iv["inf"], "sup": iv["sup"]})

    for etiq, sub in [("ELIGIERON (19/20-21/22)",
                       dif[dif["season"].isin(ELIGIERON)]),
                      ("NO eligieron (17/18-18/19)",
                       dif[~dif["season"].isin(ELIGIERON)]),
                      ("todo el bloque", dif)]:
        iv = intervalo(sub)
        filas.append({"tramo": etiq, "eligio": None, "n": len(sub),
                      "rps_k3": sub["rps_k3"].mean(),
                      "rps_k5": sub["rps_k5"].mean(),
                      "acc_k3": sub["acc_k3"].mean(),
                      "acc_k5": sub["acc_k5"].mean(),
                      "k5_menos_k3": sub["dif"].mean(),
                      "ee": iv["ee"], "inf": iv["inf"], "sup": iv["sup"]})

    t = pd.DataFrame(filas)
    t.to_csv(SALIDA, index=False)
    print("`k5_menos_k3` es diferencia de RPS, que es pérdida:")
    print("NEGATIVO = el conjunto de cinco GANA a la semilla desnuda.\n")
    print(t.to_string(index=False))
    print(f"\n-> {SALIDA}")


if __name__ == "__main__":
    main()
