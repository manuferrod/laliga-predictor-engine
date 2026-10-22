"""¿Eligió la búsqueda señal, o el máximo de un montón de ruido?

El bloque de medición dice que el conjunto de cinco es PEOR que la semilla desnuda
(+0.000608 a favor de la semilla, intervalo que excluye el cero). La pregunta que eso
obliga a hacer es si las dos variables adoptadas lo fueron por mérito o por ser el
máximo de cincuenta y nueve sorteos.

La prueba no necesita permutaciones: basta la distribución de las cincuenta y nueve
mejoras del primer paso. Si no hay señal, esas mejoras son ruido de media cero y
desviación típica s, y el máximo de 59 sorteos así vale unas 2.3·s. Comparar la mejora
realmente adoptada con ese 2.3·s dice si la voraz encontró algo o se lo encontró.

Todo sobre el bloque de selección (regla 6): es el bloque que ya eligió, de modo que
mirarlo otra vez no gasta nada que no estuviera gastado.
"""
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import seleccion as sl           # noqa: E402

SALIDA = "outputs/models/seleccion/nulo.csv"


def main() -> None:
    cfg = pt.ProtocolConfig(regime="semanal")
    feat = lb.datos()
    sem = list(sl.SEMILLA_MERCADO)
    pool = [c for c in sl.candidatas(feat) if c not in sem]
    print(f"semilla: {sem}\ncandidatas: {len(pool)}", flush=True)

    base = sl._lote([sem], sl.FAMILIA, cfg, kw={})[0]
    if isinstance(base, str):
        raise SystemExit(f"la semilla no es evaluable: {base}")
    print(f"semilla sola: RPS {base.rps:.6f}", flush=True)

    hecho = {"n": 0}

    def informa(i, cols, r):
        hecho["n"] = i
        if i % 10 == 0:
            print(f"  {i}/{len(pool)}", flush=True)

    bruto = sl._lote([sem + [c] for c in pool], sl.FAMILIA, cfg, kw={},
                     informa=informa)

    filas = []
    for c, r in zip(pool, bruto):
        if isinstance(r, str):
            filas.append({"candidata": c, "mejora": np.nan, "motivo": r})
        else:
            filas.append({"candidata": c, "mejora": base.rps - r.rps,
                          "rps": r.rps, "motivo": ""})
    t = pd.DataFrame(filas).sort_values("mejora", ascending=False)
    t.to_csv(SALIDA, index=False)

    m = t["mejora"].dropna().to_numpy()
    n = len(m)
    mu, s = float(m.mean()), float(m.std(ddof=1))

    print(f"\ncandidatas evaluables: {n}")
    print(f"media de las mejoras      : {mu:+.6f}")
    print(f"desviación típica         : {s:.6f}")
    print(f"mejoras positivas         : {(m > 0).sum()} de {n}")
    print(f"por encima de TOLERANCIA  : {(m > sl.TOLERANCIA).sum()}")

    # El techo de ruido se centra en la MEDIA de la nube, no en cero. La nube está
    # desplazada a -0.000175 porque añadir una variable que no dice nada a una
    # logística penalizada cuesta RPS, y comparar el máximo contra cero regalaría
    # al mejor candidato un desplazamiento que no es mérito suyo.
    from scipy.stats import norm
    k = np.arange(1, n + 1)
    esp = mu + norm.ppf((n - k + 1 - 0.375) / (n + 0.25)) * s
    obs = np.sort(m)[::-1]
    print("\n  k   observado    techo de ruido    sd sobre la media")
    for i in range(6):
        print(f"  {i+1}   {obs[i]:+.6f}     {esp[i]:+.6f}        "
              f"{(obs[i] - mu) / s:5.2f}")
    print(f"\nrazón del mejor observado sobre su techo: {obs[0] / esp[0]:.2f}")
    print("\nlas diez primeras:")
    print(t.head(10)[["candidata", "mejora", "rps"]].to_string(index=False))


if __name__ == "__main__":
    main()
