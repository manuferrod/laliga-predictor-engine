"""¿Y un modelo con el mercado MÁS lo mejor del resto? Dos cosas que nunca se variaron
a la vez.

La voraz contestó «nada mejora al precio», y la pregunta de Manuel es por qué no existe
el conjunto que junta el mercado con las mejores del resto. Al revisar cómo se hizo la
búsqueda aparecen dos limitaciones suyas que no son de los datos sino del procedimiento,
y que explican por qué ese conjunto no podía encontrarse aunque existiera.

**Una: la penalización estaba clavada.** `laboratorio.HIPER["logistica"]` fija
`C=0.3, l1_ratio=0.0` para toda la búsqueda, y el propio módulo declara el precio de esa
decisión: «un conjunto puede parecer peor que otro por llevar una configuración que
no le convenía. Se paga a conciencia y se compensa al final, afinando sobre el conjunto
ya elegido con `afina()`». Pero `afina()` solo afina el conjunto que ganó —k=3—, así que
la compensación nunca llega a los conjuntos que la necesitaban. Y la necesitaban mucho:
`_ridge.py` midió que las MISMAS 65 columnas pasan de **0.200669** con `C=0.3, L2` a
**0.194557** con `C=0.03, L1`. Son **0.0061 de RPS recuperados por la penalización sola,
sin tocar una sola variable**. Dicho de otro modo: la voraz comparaba k=3 contra k=12
con una penalización calibrada para k=3, y la diferencia que medía era en buena parte
el castigo de la varianza, no la falta de señal.

**Dos: la voraz es miope.** Añade de una en una y se queda con la que más mejora sola.
Si un BLOQUE ayuda pero ninguno de sus miembros ayuda por separado, no lo encuentra
nunca. Y eso no es hipotético aquí: `_sinprecio.py` midió que el dominio de `fuerza`
—nueve columnas— llega a **0.198001** y bate a las 59 de juego juntas. Un bloque puede
llevar lo que ningún miembro suyo lleva.

Así que se mide lo que nunca se midió: **la semilla de mercado más cada bloque entero,
cada conjunto con SU mejor penalización**, incluida la semilla, para que la comparación
sea justa en los dos ejes a la vez. Después, voraz POR BLOQUES sobre los que hayan
ganado.

Sobre el bloque de SELECCIÓN, que es donde se diseña (reglas 5 y 6): ninguna cifra de
aquí es citable. Elegir la penalización mirando los mismos partidos es sesgo de máximo,
sí —cinco miradas por conjunto—, pero es simétrico entre conjuntos y por tanto no
favorece a los grandes, que es lo que se pone a prueba. La puerta de verdad es
`seleccion.confirma()` sobre 2017/18-2018/19, y ahí va lo que gane.

Uso:
    python _bloques.py            # los ocho conjuntos por las cinco penalizaciones
    python _bloques.py --rapido   # solo la semilla y fuerza, para cronometrar
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

SALIDA = _RAIZ / "outputs" / "models" / "seleccion" / "bloques.csv"

# Las cinco penalizaciones. L1 domina a L2 en conjuntos grandes según `_ridge.py` (sus
# cuatro mejores filas son las cuatro de `l1=1.0`), así que la rejilla lleva tres L1 y
# las dos L2 que acotan: `C=0.3` es el valor por defecto con el que corrió la búsqueda
# —y por tanto el listón a batir— y `C=1.0` comprueba que no se esté dejando señal por
# penalizar de más.
REJILLA: list[dict] = [
    {"C": 0.01, "l1_ratio": 1.0},
    {"C": 0.03, "l1_ratio": 1.0},
    {"C": 0.1, "l1_ratio": 1.0},
    {"C": 0.3, "l1_ratio": 0.0},    # el de la búsqueda
    {"C": 1.0, "l1_ratio": 0.0},
]

CLAVE = ["season", "jornada", "match_key"]


def _etiqueta(h: dict) -> str:
    return f"C={h['C']}/l1={h['l1_ratio']}"


def _puntua(cols, h, cfg, feat) -> pd.DataFrame | None:
    """Predicciones del protocolo con su RPS por partido, indexadas por partido."""
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
    """Diferencia emparejada partido a partido, con intervalo por jornadas enteras.

    Se empareja sobre la INTERSECCIÓN y no se comparan dos medias por separado: los
    pliegues que se saltan por falta de entrenamiento o por columna constante no son
    los mismos en los dos conjuntos, y promediar sobre censos distintos mezcla la
    diferencia entre conjuntos con la diferencia entre partidos. Positivo = el otro
    conjunto GANA a la base (le quita RPS).
    """
    com = base.index.intersection(otro.index)
    d = pd.DataFrame({"dif": base.loc[com, "rps"] - otro.loc[com, "rps"]})
    iv = ev.block_bootstrap(d.reset_index(),
                            lambda x: float(x["dif"].mean()),
                            n_boot=2000, seed=7)
    return {"gana": float(d["dif"].mean()), "inf": iv["inf"], "sup": iv["sup"],
            "n_par": int(len(com))}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    rapido = "--rapido" in argv

    cfg = pt.ProtocolConfig(regime="semanal")
    feat = lb.datos()
    cands = sl.candidatas(feat, regime="semanal")
    mapa = lb.dominios(cands)
    semilla = list(sl.SEMILLA_MERCADO)

    orden = ["fuerza", "forma", "clasificacion", "historial", "fatiga",
             "bajas", "contexto"]
    orden = [d for d in orden if d in mapa]
    if rapido:
        orden = orden[:1]

    conjuntos: list[tuple[str, list[str]]] = [("semilla", semilla)]
    for d in orden:
        extra = [c for c in mapa[d] if c not in set(semilla)]
        conjuntos.append((f"semilla+{d}", semilla + extra))

    # La escalera acumulativa, que es la pregunta en su forma literal: el mercado MÁS
    # lo mejor del resto, añadiendo dominios enteros de mejor a peor. El orden no se
    # elige aquí mirando estas cifras —eso sería la voraz otra vez, con el mismo sesgo
    # de máximo—: es el que `_sinprecio.py` ya midió SIN el precio delante, donde el
    # precio no puede haber tapado a nadie (`fuerza` 0.841 > `forma` 0.679 >
    # `clasificacion` 0.525 > `historial` 0.317 > `fatiga` 0.143 > `bajas` 0.011 >
    # `contexto` -0.025). Usar un orden medido fuera de este cuadro es lo que permite
    # leer la escalera como una curva y no como una búsqueda.
    if not rapido:
        acum = list(semilla)
        for d in orden:
            acum = acum + [c for c in mapa[d] if c not in set(acum)]
            conjuntos.append((f"escalera+{d}", list(acum)))

    print(f"bloque {sl.BLOQUE_SELECCION} · familia {sl.FAMILIA} · "
          f"{len(conjuntos)} conjuntos x {len(REJILLA)} penalizaciones", flush=True)
    for n, c in conjuntos:
        print(f"  {n:<24} k={len(c)}", flush=True)
    print(flush=True)

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
            print(f"    {_etiqueta(h):<16} RPS {r:.6f}  "
                  f"acc {100 * p['acierto'].mean():5.2f} %  n={len(p)}  "
                  f"[{time.time() - t0:.0f}s]", flush=True)
            filas.append({"conjunto": nombre, "k": len(cols),
                          "C": h["C"], "l1_ratio": h["l1_ratio"],
                          "rps": r, "accuracy": float(p["acierto"].mean()),
                          "n": int(len(p))})
            if r < mejor_rps:
                mejor_rps, mejor_p, mejor_h = r, p, h
        if mejor_p is not None:
            mejores[nombre] = mejor_p
            print(f"    -> mejor {_etiqueta(mejor_h)} con {mejor_rps:.6f}",
                  flush=True)

    tab = pd.DataFrame(filas)

    # La lectura que contesta la pregunta: cada conjunto en su mejor penalización,
    # contra la SEMILLA en la suya. Es la comparación que la búsqueda no hizo nunca.
    if "semilla" in mejores:
        base = mejores["semilla"]
        print("\n=== cada bloque en su mejor penalizacion, contra la semilla en la "
              "suya ===", flush=True)
        print(f"{'conjunto':<24} {'k':>3}  {'RPS':>9}  {'gana a semilla':>15}"
              f"  {'intervalo':>24}", flush=True)
        comp: list[dict] = []
        for nombre, p in mejores.items():
            if nombre == "semilla":
                continue
            c = _contra(base, p)
            comp.append({"conjunto": nombre, "rps": float(p["rps"].mean()), **c})
            print(f"{nombre:<24} {'':>3}  {p['rps'].mean():9.6f}  "
                  f"{c['gana']:+15.6f}  [{c['inf']:+.6f}, {c['sup']:+.6f}]",
                  flush=True)
        if comp:
            cm = pd.DataFrame(comp).sort_values("gana", ascending=False)
            tab = tab.merge(cm[["conjunto", "gana", "inf", "sup"]],
                            on="conjunto", how="left")
            gana = cm.loc[cm["gana"] > 0]
            print(f"\nsemilla sola: {base['rps'].mean():.6f} "
                  f"(acc {100 * base['acierto'].mean():.2f} %)", flush=True)
            if gana.empty:
                print("NINGUN bloque mejora a la semilla, ni con su mejor "
                      "penalizacion. La respuesta a la pregunta es que ese "
                      "conjunto no existe en este banco, y ahora esta medido en "
                      "los dos ejes.", flush=True)
            else:
                print(f"bloques que MEJORAN a la semilla: "
                      f"{', '.join(gana['conjunto'])}", flush=True)
                print("siguiente paso: voraz POR BLOQUES sobre estos, y luego "
                      "`seleccion.confirma()` sobre 2017/18-2018/19.", flush=True)

    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    tab.to_csv(SALIDA, index=False, encoding="utf-8")
    print(f"\n{SALIDA}")
    print(tab.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
