"""Todos los PARES. La búsqueda exhaustiva más grande que cabe en una noche.

La pregunta de Manuel es si se ha probado «la mejor combinación, ir probando todas las
posibles combinaciones». Exhaustivo de verdad no cabe: son 2^62 = 4.6 trillones de
conjuntos y a 8.5 s por evaluación —medidos— son noventa veces la edad del universo.
Pero eso no es la razón de fondo para no hacerlo, y la razón de fondo es la que esta
medida deja por escrito.

**El máximo de una búsqueda crece con el logaritmo del tamaño de la búsqueda.** La nube
de ruido de este banco está medida (`_nulo.py`, n=62): media −0.000175, desviación
típica 0.000184. Con esos dos números, el mejor conjunto **de un mundo sin ninguna
señal** gana a la semilla por:

| búsqueda | conjuntos | mejor por puro azar |
|---|---|---|
| una variable (hecho) | 62 | +0.000254 |
| **todos los pares (esto)** | **1891** | **+0.000454** |
| todos los tríos | 37 820 | +0.000592 |
| todas las combinaciones | 4.6·10^18 | +0.001542 |

La última fila es la que contesta la pregunta: una búsqueda exhaustiva sobre las 62
devolvería, **sin que exista una sola variable útil**, un ganador con RPS 0.191632
contra los 0.193174 de la semilla. Parecería un hallazgo enorme y no sería nada. Y no es
especulación: la voraz encontró +0.000351 —1.38 veces el techo de 62 sorteos—, pasó tres
puertas, y la cuarta la rechazó igual (+0.000466 de RPS donde no se eligió).

Así que los pares no se hacen por ser exhaustivos, sino porque son **el sitio exacto
donde la voraz es ciega y el techo de ruido sigue siendo bajo**: un par que ayuda cuando
ninguno
de sus dos miembros ayuda por separado es invisible a una búsqueda que añade de una en
una, y eso no es hipotético en este banco —`fuerza` como bloque bate a las 59 de juego
juntas—. A 1891 conjuntos el listón de ruido son +0.000454, y es alcanzable.

**El listón se declara aquí, antes de mirar el resultado**, que es lo que separa esto de
pescar. Para que un par cuente tiene que hacer las tres cosas:

1. ganar a la semilla más de **+0.000454** (el techo de ruido de 1891 sorteos);
2. ganarle también con **su propia penalización** de entre cinco, no solo con la de la
   casa — porque `_bloques.py` midió 0.0061 de RPS entre la mejor y la peor; y
3. pasar `seleccion.confirma()` sobre **2017/18-2018/19**, que es la única puerta que no
   ha visto estos partidos.

Se mide además la **sinergia** de cada par: lo que gana el par menos la suma de lo que
ganan sus dos miembros solos (que `_nulo.py` ya dejó en `nulo.csv`). La sinergia es la
magnitud que la voraz no puede ver, y si la hipótesis de la miopía es cierta, el par
ganador tendrá sinergia grande y positiva. Si los mejores pares son simplemente los dos
mejores sueltos juntos, la miopía no costaba nada y queda contestado también.

Regla 5: ninguna cifra de aquí es citable; sale del bloque de selección.

Uso:
    python _pares.py            # las 1891 parejas y luego la rejilla sobre las mejores
    python _pares.py --rapido   # solo las 60 primeras parejas, para cronometrar
"""
from __future__ import annotations

import sys
import time
from itertools import combinations
from math import log, sqrt
from pathlib import Path

import pandas as pd

_RAIZ = Path(__file__).resolve().parent
for _sub in ("scripts", "scripts/models", "scripts/features"):
    sys.path.insert(0, str(_RAIZ / _sub))

import laboratorio as lb        # noqa: E402
import protocol as pt           # noqa: E402
import seleccion as sl          # noqa: E402

SALIDA = _RAIZ / "outputs" / "models" / "seleccion" / "pares.csv"
NULO = _RAIZ / "outputs" / "models" / "seleccion" / "nulo.csv"
FINOS = _RAIZ / "outputs" / "models" / "seleccion" / "pares_finos.csv"

# La nube de ruido medida por `_nulo.py` sobre este mismo banco y bloque.
NUBE_MU, NUBE_SD = -0.000175, 0.000184

REJILLA: list[dict] = [
    {"C": 0.01, "l1_ratio": 1.0},
    {"C": 0.03, "l1_ratio": 1.0},
    {"C": 0.1, "l1_ratio": 1.0},
    {"C": 0.3, "l1_ratio": 0.0},    # el de la búsqueda
    {"C": 1.0, "l1_ratio": 0.0},
]
BASE_HIPER = {"C": 0.3, "l1_ratio": 0.0}
N_FINOS = 20


def techo_ruido(n: int) -> float:
    """Mejor mejora esperable de `n` conjuntos si NINGUNO lleva señal.

    Es la esperanza del máximo de n normales, y se calcula **exacta** por la integral
    E[max] = ∫ x·n·φ(x)·Φ(x)^(n−1) dx y no por la asintótica `sqrt(2·ln n)`. La
    diferencia no es cosmética a estos tamaños: para n=1891 la asintótica cruda da
    4.033·s y la exacta 3.420·s, o sea **un listón un 25 % más alto del que toca**
    (+0.000567 contra +0.000454), y un listón inflado rechazaría un hallazgo real
    tanto como uno laxo aceptaría ruido. Con n=62 la exacta da 2.33·s, que es el 2.3·s
    con el que `_nulo.py` juzgó a la voraz: las dos medidas hablan en la misma escala,
    que es lo que esta función tiene que garantizar.

    Para n enorme la integral se desborda y ahí sí vale la asintótica, porque el
    término de corrección γ/b ya es despreciable frente a b.
    """
    if n <= 10 ** 7:
        from scipy.integrate import quad
        from scipy.stats import norm
        v, _ = quad(lambda x: x * n * norm.pdf(x) * norm.cdf(x) ** (n - 1),
                    -12.0, 16.0, limit=400)
        return NUBE_MU + NUBE_SD * v
    b = sqrt(2.0 * log(n))
    return NUBE_MU + NUBE_SD * (b + 0.5772 / b)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    rapido = "--rapido" in argv

    cfg = pt.ProtocolConfig(regime="semanal")
    feat = lb.datos()
    sem = list(sl.SEMILLA_MERCADO)
    pool = [c for c in sl.candidatas(feat, regime="semanal") if c not in sem]

    pares = list(combinations(pool, 2))
    if rapido:
        pares = pares[:60]
    listón = techo_ruido(len(pares))

    print(f"bloque {sl.BLOQUE_SELECCION} · familia {sl.FAMILIA}", flush=True)
    print(f"semilla k={len(sem)} · candidatas {len(pool)} · pares {len(pares)}",
          flush=True)
    print(f"LISTON declarado antes de mirar: ganar mas de {listón:+.6f} de RPS "
          f"(techo de ruido de {len(pares)} sorteos)", flush=True)
    print(f"estimado {len(pares) * 8.5 / 3600:.1f} h en serie, menos repartido\n",
          flush=True)

    base = sl._lote([sem], sl.FAMILIA, cfg, kw={})[0]
    if isinstance(base, str):
        raise SystemExit(f"la semilla no es evaluable: {base}")
    print(f"semilla sola: RPS {base.rps:.6f}\n", flush=True)

    t0 = time.time()
    total = len(pares)

    def informa(i, cols, r):
        if i % 50 == 0 or i == total:
            hecho = time.time() - t0
            queda = hecho / max(i, 1) * (total - i)
            print(f"  {i}/{total}  [{hecho / 60:.0f} min hechos, "
                  f"~{queda / 60:.0f} min quedan]", flush=True)

    bruto = sl._lote([sem + list(p) for p in pares], sl.FAMILIA, cfg,
                     kw={"hiper": BASE_HIPER}, informa=informa)

    # La sinergia necesita lo que gana cada miembro SOLO, y eso ya está medido.
    solo: dict[str, float] = {}
    if NULO.exists():
        nu = pd.read_csv(NULO).dropna(subset=["mejora"])
        solo = dict(zip(nu["candidata"], nu["mejora"]))

    filas: list[dict] = []
    for (a, b), r in zip(pares, bruto):
        if isinstance(r, str):
            filas.append({"a": a, "b": b, "rps": None, "mejora": None,
                          "sinergia": None, "motivo": r})
            continue
        mej = base.rps - r.rps
        sa, sb = solo.get(a), solo.get(b)
        sin = None if (sa is None or sb is None) else mej - (sa + sb)
        filas.append({"a": a, "b": b, "rps": float(r.rps), "mejora": float(mej),
                      "sinergia": sin, "motivo": ""})

    t = pd.DataFrame(filas).sort_values("mejora", ascending=False)
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    t.to_csv(SALIDA, index=False, encoding="utf-8")

    ev_ok = t.dropna(subset=["mejora"])
    print(f"\n=== {len(ev_ok)} pares evaluables de {total} "
          f"[{(time.time() - t0) / 60:.0f} min] ===", flush=True)
    print(f"mejoras positivas        : {int((ev_ok['mejora'] > 0).sum())}")
    print(f"por encima del LISTON    : "
          f"{int((ev_ok['mejora'] > listón).sum())}  (listón {listón:+.6f})")
    print("\n=== los quince mejores pares ===")
    print(ev_ok.head(15).to_string(index=False))

    if "sinergia" in ev_ok and ev_ok["sinergia"].notna().any():
        print("\n=== los diez de mayor SINERGIA (lo que la voraz no puede ver) ===")
        print(ev_ok.sort_values("sinergia", ascending=False)
              .head(10).to_string(index=False))
        print(f"\nsinergia mediana de los 1891 pares: "
              f"{ev_ok['sinergia'].median():+.6f}")
        print(f"pares con sinergia positiva        : "
              f"{int((ev_ok['sinergia'] > 0).sum())} de {len(ev_ok)}")

    ganan = ev_ok.loc[ev_ok["mejora"] > listón]
    if ganan.empty:
        print(f"\nNINGUN par de los {len(ev_ok)} supera el techo de ruido. La "
              f"respuesta a «¿y probando todas las combinaciones?» es que la "
              f"combinacion exhaustiva que cabe medir ya esta medida y no hay nada, "
              f"y que la que no cabe devolveria por puro azar "
              f"{techo_ruido(2 ** len(pool)):+.6f}, cuatro veces mas de lo que la "
              f"voraz encontro y la cuarta puerta rechazo.", flush=True)
        return 0

    # Segunda vuelta: los finalistas, cada uno con SU penalización de entre cinco,
    # porque `_bloques.py` midió 0.0061 de RPS entre la mejor y la peor. La semilla
    # también, o la comparación volvería a ser la injusta de la búsqueda original.
    finos = list(ganan.head(N_FINOS)[["a", "b"]].itertuples(index=False, name=None))
    print(f"\n=== segunda vuelta: {len(finos)} finalistas x {len(REJILLA)} "
          f"penalizaciones, y la semilla tambien ===", flush=True)
    conj = [("semilla", tuple(sem))] + [(f"{a}+{b}", tuple(sem) + (a, b))
                                        for a, b in finos]
    rf: list[dict] = []
    for nombre, cols in conj:
        mejor = (float("inf"), None)
        for h in REJILLA:
            r = sl._lote([list(cols)], sl.FAMILIA, cfg, kw={"hiper": h})[0]
            if isinstance(r, str):
                continue
            rf.append({"conjunto": nombre, "C": h["C"], "l1_ratio": h["l1_ratio"],
                       "rps": float(r.rps)})
            mejor = min(mejor, (float(r.rps), h), key=lambda x: x[0])
        print(f"  {nombre:<46} mejor RPS {mejor[0]:.6f}  con {mejor[1]}", flush=True)

    tf = pd.DataFrame(rf)
    tf.to_csv(FINOS, index=False, encoding="utf-8")
    mj = tf.loc[tf.groupby("conjunto")["rps"].idxmin()].sort_values("rps")
    semf = float(mj.loc[mj["conjunto"] == "semilla", "rps"].iloc[0])
    mj["gana_semilla"] = semf - mj["rps"]
    print("\n=== cada uno en su mejor penalizacion, contra la semilla en la suya ===")
    print(mj.to_string(index=False))
    vivos = mj.loc[(mj["conjunto"] != "semilla") & (mj["gana_semilla"] > listón)]
    if vivos.empty:
        print(f"\nNinguno aguanta el listón {listón:+.6f} cuando la semilla tambien "
              f"elige su penalizacion. Cerrado sin necesidad de la cuarta puerta.")
    else:
        print(f"\nAGUANTAN: {', '.join(vivos['conjunto'])}\n"
              f"-> a `seleccion.confirma()` sobre 2017/18-2018/19, con el `hiper` con "
              f"el que se eligieron.")
    print(f"\n{SALIDA}\n{FINOS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
