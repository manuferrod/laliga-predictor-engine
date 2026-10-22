"""Las dos variables adoptadas, juzgadas por todas las temporadas que no las eligieron.

El desglose por temporada (`desglose.csv`) dejó un patrón que no admite dos lecturas:
el conjunto de cinco gana en las TRES temporadas que lo eligieron y pierde en las DOS
anteriores que no participaron. Y el bloque de medición, que es posterior e
independiente, ya había dicho lo mismo. Cinco temporadas fuera de la elección, las
cinco en contra.

Aquí se junta todo en una sola pasada para poder dar UN intervalo sobre el agregado de
fuera, que es la cifra que contesta a la pregunta. Separado, cada tramo se queda corto
de potencia —0.00035 y 0.00030 de error típico contra un efecto de 0.0005—; juntos son
mil novecientos partidos y el intervalo debería excluir el cero.

    ANTES de la selección   2017/18-2018/19    760 partidos
    la selección            2019/20-2021/22   1140 partidos   (gastado; no es citable)
    DESPUÉS, la medición    2022/23-2024/25   1140 partidos   (ya leído una vez)

Sobre leer otra vez el bloque de medición: no se toma ninguna decisión aquí, y la cifra
que produce es la misma que ya está escrita en la cabecera de `seleccion.py`. Lo que se
añade es el tramo anterior a la selección, que nunca se había mirado y que es
información nueva de verdad. Agrupar dos lecturas ya hechas no gasta presupuesto; lo
gastaría elegir algo con ellas, y lo que se elija a partir de aquí lo decide Manuel.

El holdout de 2025/26 sigue sellado y no se toca (regla 7).
"""
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev            # noqa: E402
import seleccion as sl           # noqa: E402

SALIDA = "outputs/models/seleccion/veredicto.csv"
ELIGIERON = {"2019/20", "2020/21", "2021/22"}
ANTES = {"2017/18", "2018/19"}
CLAVE = ["season", "jornada", "match_key"]


def puntua(cols, cfg, feat):
    e = lb.evalua(cols, "logistica", cfg=cfg, feat=feat,
                  evaluar="validacion", n_boot=0,
                  registrar=False, verbose=False)
    p = e.pred.loc[ev.evaluable(e.pred)].copy()
    p["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
    p["acierto"] = (np.asarray(ev._terna(p, ev.PROB_COLS)).argmax(1)
                    == np.asarray(ev._objetivo(p))).astype(float)
    return p


def tramo(s: str) -> str:
    if s in ANTES:
        return "antes"
    if s in ELIGIERON:
        return "eligio"
    return "despues"


def main() -> None:
    feat = lb.datos()
    cfg = pt.ProtocolConfig(regime="semanal", warmup_seasons=["2016/17"])
    sem = list(sl.SEMILLA_MERCADO)
    k5 = list(sl.carga()["conjunto"])
    print(f"protocolo: {cfg.describe()}", flush=True)
    print(f"las dos adoptadas: {[c for c in k5 if c not in sem]}\n", flush=True)

    a, b = puntua(sem, cfg, feat), puntua(k5, cfg, feat)
    ia, ib = a.set_index(CLAVE), b.set_index(CLAVE)
    com = ia.index.intersection(ib.index)
    dif = pd.DataFrame({
        "dif": ib.loc[com, "rps"] - ia.loc[com, "rps"],
        "rps_k3": ia.loc[com, "rps"], "rps_k5": ib.loc[com, "rps"],
        "acc_k3": ia.loc[com, "acierto"], "acc_k5": ib.loc[com, "acierto"],
    }).reset_index()
    dif["season"] = dif["season"].astype(str)
    dif["tramo"] = dif["season"].map(tramo)

    def mide(etiq, sub):
        iv = ev.block_bootstrap(sub, lambda d: float(d["dif"].mean()),
                                n_boot=4000, seed=7)
        return {"grupo": etiq, "n": len(sub),
                "rps_k3": sub["rps_k3"].mean(), "rps_k5": sub["rps_k5"].mean(),
                "acc_k3": sub["acc_k3"].mean(), "acc_k5": sub["acc_k5"].mean(),
                "k5_menos_k3": sub["dif"].mean(), "ee": iv["ee"],
                "inf": iv["inf"], "sup": iv["sup"]}

    filas = [mide(s, g) for s, g in dif.groupby("season")]
    for etiq, sub in [
        ("[antes] 17/18-18/19", dif[dif["tramo"] == "antes"]),
        ("[eligio] 19/20-21/22", dif[dif["tramo"] == "eligio"]),
        ("[despues] 22/23-24/25", dif[dif["tramo"] == "despues"]),
        ("FUERA de la eleccion", dif[dif["tramo"] != "eligio"]),
        ("todo el desarrollo", dif),
    ]:
        filas.append(mide(etiq, sub))

    t = pd.DataFrame(filas)
    t.to_csv(SALIDA, index=False)
    print("`k5_menos_k3` es diferencia de RPS, que es pérdida:")
    print("POSITIVO = las dos variables adoptadas EMPEORAN el modelo.\n")
    print(t.to_string(index=False))

    f = t[t["grupo"] == "FUERA de la eleccion"].iloc[0]
    print("\n===== la cifra =====")
    print(f"Sobre los {int(f['n'])} partidos que NO eligieron el conjunto, las dos "
          f"variables adoptadas")
    print(f"empeoran el RPS en {f['k5_menos_k3']:+.6f}, con error típico "
          f"{f['ee']:.6f} e intervalo")
    print(f"[{f['inf']:+.6f}, {f['sup']:+.6f}] remuestreando jornadas enteras.")
    print(f"t = {f['k5_menos_k3'] / f['ee']:.2f}")
    excluye = (f["inf"] > 0) or (f["sup"] < 0)
    print(f"El intervalo {'EXCLUYE' if excluye else 'incluye'} el cero.")
    print(f"\n-> {SALIDA}")


if __name__ == "__main__":
    main()
