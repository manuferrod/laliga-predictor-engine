"""Búsqueda en haz de anchura cinco, con poda. El algoritmo que pidió Manuel, literal.

Su descripción, palabra por palabra: «primero pruebes las 65 logísticas y veas el top 5
por ejemplo de mejores resultados, después a esas 5 pruebas a ir añadiendo una a una
respectivamente el resto de las variables para ver cuál sería la segunda mejor variable
y vas viendo cuáles quedan en el top 3-5 y cuáles se repiten entre las peores para ir
descartando, y así sucesivamente hasta llegar a un modelo decente, que mejore el de las
cuotas de mercado».

Eso es **búsqueda en haz** de anchura 5. Lo que corrió este proyecto —`adelante`— es el
mismo algoritmo con anchura **1**: en cada paso se queda con un único conjunto, el
mejor, y tira los otros sesenta y un caminos. Por eso es ciega a una variable que no
ayude hoy y sea imprescindible mañana. El parámetro `anchura` de `adelante` **no** es
esto: recorta el pool a sus N primeras candidatas, que es otra cosa. La anchura de haz
no existía en el código y hay que escribirla.

Arranca **desde cero**, no desde la semilla de mercado, porque es lo que él pidió y
porque así el algoritmo tiene que *descubrir* las variables de precio en lugar de
recibirlas puestas. Y las descubre: la criba de una variable sola —`criba.parquet`,
13-oct— pone `mkt_p_away` (0.193442) y `mkt_p_home` (0.193661) en los dos primeros, y la
mejor que no es precio, `elo_z_diff`, a 0.198131. De los cinco haces iniciales, dos son
de mercado y tres no.

Tres decisiones de método, cada una con su medida:

1. **La penalización no va clavada.** Dejarla fija en `C=0.3, L2` para toda la búsqueda
   es el defecto que ya costó una conclusión en este proyecto: `_ridge.py` mide
   **0.0061 de RPS entre la mejor y la peor penalización sobre las mismas 65 columnas**
   (0.194557 con `C=0.03, L1`; 0.200669 con `C=0.3, L2`), y `_bloques.py` mide que
   dejar elegir recupera **0.005663** al conjunto grande y **0.000006** a la semilla. O
   sea que el castigo fijo no es neutral: penaliza a los conjuntos grandes, que son
   los que la pregunta de Manuel quiere poner a prueba. Aquí cada conjunto se mide con
   las **dos** penalizaciones que acotan ese rango y se queda con la mejor. Son 2x
   evaluaciones; es el precio de que la comparación sea limpia.

2. **La poda es la suya, y es segura.** Una variable que cae en la peor mitad de su
   paso se lleva una marca; con dos marcas sale del pool. Nunca se baja de `SUELO`
   candidatas. Conviene decir por qué esto no puede inventar un hallazgo: podar solo
   puede hacer que la búsqueda **se pierda** señal real, nunca que encuentre señal donde
   no hay. Y además *reduce* el número de configuraciones examinadas, que es lo que fija
   el listón de ruido. El riesgo de la poda es de potencia, no de validez.

3. **Se juzga fuera de la elección.** FASE 1 corre el haz con `corte_seleccion=2021/22`,
   así que **2021/22 no participa en elegir nada**. FASE 2 vuelve a puntuar el mejor
   conjunto de cada paso con el corte por defecto y separa por temporada: `rps_dentro`
   son las dos que eligieron, `rps_fuera` es la que no. Es el protocolo de
   `_camino65.py`, y es lo único que convierte «mejora el modelo de las cuotas» en una
   afirmación. Vale
   recordar lo que dijo ese recorrido con la voraz: **el RPS de fuera fue peor que el de
   la semilla desnuda en 55 de los 57 pasos**.

El listón está prerregistrado y se escribe aquí antes de mirar nada: el conjunto que
salga tiene que **batir a la semilla en 2021/22**, que son 380 partidos en los que no se
eligió, y después sobrevivir a `seleccion.confirma()` sobre 2017/18-2018/19 con la misma
penalización con la que fue adoptado. Dentro del bloque no hay cifra citable (regla 5).

Uso:
    python _haz.py              # FASE 1 + FASE 2 + FASE 3
    python _haz.py --fase2      # repuntuar y confirmar, leyendo el orden guardado
    python _haz.py --fase3      # solo la puerta, leyendo `haz_fuera.csv`
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

_RAIZ = Path(__file__).resolve().parent

# Un hilo de BLAS por proceso, FIJADO ANTES DE IMPORTAR NUMPY. No es cosmética:
# `_coste.py` mide 12.87 s/protocolo en serie con un hilo, 5.53 s con el reparto en diez
# procesos de un hilo, y la medida ingenua —sin fijar, diez hijos abriendo doce hilos
# cada uno— daba 7.59 s y un lote de 200 conjuntos tardó 22 minutos cuando en serie
# habría tardado 28. Con 120 hilos sobre 12 núcleos el coste se va en cambios de
# contexto. El paralelismo tiene que estar en el reparto de conjuntos, que es donde el
# trabajo es independiente de verdad.
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

for _sub in ("scripts", "scripts/models", "scripts/features"):
    sys.path.insert(0, str(_RAIZ / _sub))
sys.path.insert(0, str(_RAIZ))

import numpy as np                  # noqa: E402
import pandas as pd                 # noqa: E402

import evaluate as ev               # noqa: E402
import laboratorio as lb            # noqa: E402
import protocol as pt               # noqa: E402
import seleccion as sl              # noqa: E402
from _pares import techo_ruido      # noqa: E402

ANCHURA = 5
MAX_K = 20
PACIENCIA = 4
SUELO = 12
MARCAS_FUERA = 2

# Las dos que acotan el rango de 0.0061 que mide `_ridge.py`: la base con la que corrió
# toda la búsqueda anterior —y por tanto el listón— y la que gana sobre las 65.
REJILLA: list[dict] = [
    {"C": 0.3, "l1_ratio": 0.0},
    {"C": 0.03, "l1_ratio": 1.0},
]

CORTE = "2021/22"
FUERA = "2021/22"
VENTANA = {"window": "expansiva", "sliding_seasons": pt.SLIDING_SEASONS,
           "halflife_days": None}

OUT = _RAIZ / "outputs" / "models" / "seleccion"
CURVA = OUT / "haz.csv"
ORDEN = OUT / "haz_orden.json"
DETALLE = OUT / "haz_detalle.csv"

CLAVE = ["season", "jornada", "match_key"]


def cfg_elige() -> pt.ProtocolConfig:
    """El protocolo de la FASE 1: el corte adelantado deja 2021/22 fuera de elegir."""
    return pt.ProtocolConfig(regime="semanal", corte_seleccion=CORTE, **VENTANA)


def cfg_lee() -> pt.ProtocolConfig:
    """Corte por defecto, para que el bloque cubra las tres temporadas y se puedan
    separar por `season` las dos que eligieron de la que no."""
    return pt.ProtocolConfig(regime="semanal", **VENTANA)


def _etiq(h: dict) -> str:
    return f"C={h['C']}/l1={h['l1_ratio']}"


# ===========================================================================
# FASE 1 — el haz
# ===========================================================================
def _mide_tanda(conjuntos: list[tuple], cfg) -> list[dict]:
    """Mide cada conjunto con las dos penalizaciones y devuelve la mejor de las dos.

    Un `_lote` por penalización, porque `kw` es único para todo el lote. Los conjuntos
    inviables —columna constante en algún pliegue, singularidad— vuelven como texto y
    se descartan: una candidata que no se puede ajustar es un resultado del paso, no un
    motivo para tirar la búsqueda.

    Devuelve, por conjunto PEDIDO, su conjunto **efectivo**: las columnas que de verdad
    llegaron al ajuste, que son las pedidas menos las que el protocolo reporta en
    `ausentes`. Esa distinción es el eje de la corrección descrita en `_absorbe`.
    """
    best: dict[tuple, dict] = {}
    for h in REJILLA:
        res = sl._lote(conjuntos, sl.FAMILIA, cfg, kw={"hiper": h}, n_jobs=0)
        for c, r in zip(conjuntos, res):
            if isinstance(r, str) or not np.isfinite(getattr(r, "rps", np.nan)):
                continue
            if c not in best or r.rps < best[c]["rps"]:
                best[c] = {"pedido": c,
                           "efectivo": tuple(sorted(set(c) - set(r.ausentes))),
                           "rps": float(r.rps), "hiper": h}
    return list(best.values())


def _absorbe(res: list[dict], k: int) -> dict[tuple, dict]:
    """Deja competir solo a los conjuntos que de verdad tienen `k` columnas.

    Aquí vive la corrección de un error mío que conviene dejar escrito, porque la
    lectura equivocada era plausible y habría sesgado la búsqueda entera.

    **Lo que creí:** que el preprocesado fija `drop_cols` una vez sobre todo `dev` y lo
    aplica después a cualquier conjunto, de modo que `ah_line` —colineal con
    `mkt_p_home` a 0.9768— y `strength_p_home` —con `strength_p_away` a 0.9583— serían
    inútiles SIEMPRE. Lo leí en `preprocess_state_semanal.json`, cuyo `drop_cols` lleva
    esas dos, y saqué de ahí que el banco utilizable eran 63 candidatas y no 65.

    **Lo que mide el protocolo:** que `drop_cols` se **reajusta para cada conjunto**.
    `('strength_p_home', 'mkt_p_home')` ajusta sin bajas y da 0.194320;
    `('strength_p_away', 'mkt_p_home')`, 0.194055. Las dos columnas que el estado
    publicado descarta son perfectamente utilizables en cuanto no está presente su
    pareja. El `drop_cols` del fichero describe el conjunto de 65, no una ley.

    **Y por qué fallaban solas**, que era la pista que mal interpreté: las tres fallan
    con `k=1` y el motivo lo dice el propio error —«ninguna de las 1 columnas elegidas
    sobrevive en esta ventana; la matriz quedaría vacía»—. En alguna ventana temprana
    del recorrido la columna es constante, se cae por la regla de constancia y deja la
    matriz sin ninguna columna. Con una segunda columna al lado el pliegue sale
    adelante. Fallar sola no es ser inerte: es ser la única.

    Mi regla anterior las expulsaba del pool para siempre a partir del fallo en k=1, y
    eso sí habría sido grave: quitaba de la búsqueda dos variables de fuerza utilizables
    —el dominio que en `_sinprecio.py` bate a las 59 de juego juntas— por un artefacto
    del tamaño uno.

    Lo que el haz necesitaba de verdad no era podar nada, sino **no contar dos veces el
    mismo modelo**. Si a un haz que lleva `mkt_p_home` se le añade `ah_line`, el
    protocolo la descarta y el conjunto resultante tiene k−1 columnas efectivas: es el
    haz padre con otro nombre, y dejarlo competir hacía que un paso presumiera de
    mejorar al anterior siendo idéntico (en la prueba en seco, k=3 «ganaba» con
    0.193756, el número exacto de k=2). Se resuelve mirando el conjunto efectivo:
    compiten los que conservan sus k columnas, y los absorbidos se colapsan. La
    candidata sigue en el pool, porque en otra compañía puede no ser redundante.
    """
    mejor: dict[tuple, dict] = {}
    for d in res:
        if len(d["efectivo"]) != k:
            continue
        ef = d["efectivo"]
        if ef not in mejor or d["rps"] < mejor[ef]["rps"]:
            mejor[ef] = d
    return mejor


def fase1(verbose: bool = True, rapido: bool = False) -> dict:
    cfg = cfg_elige()
    feat = lb.datos()
    pool = sl.candidatas(feat, regime=cfg.regime)
    banco = len(pool)
    semilla = tuple(sorted(sl.SEMILLA_MERCADO))
    if rapido:
        # Prueba en seco: que recorra las dos fases y escriba los tres ficheros, no que
        # mida nada. Las cifras de un `--rapido` no son citables ni comparables.
        global ANCHURA, MAX_K, SUELO
        ANCHURA, MAX_K, SUELO = 2, 3, 4
        pool = pool[:8]

    filas: list[dict] = []
    detalle: list[dict] = []
    pasos: list[dict] = []
    marcas: dict[str, int] = {v: 0 for v in pool}
    gastadas = 0
    t_ini = time.time()

    def anota(k: int, mejor: dict[tuple, dict], res: list[dict]) -> None:
        """Al parte detallado va todo lo medido, absorbidos incluidos, y por eso
        lleva las dos columnas: `pedido` es lo que la búsqueda quiso probar y
        `efectivo` lo que llegó al ajuste. Donde difieren, el conjunto no compitió."""
        for d in sorted(res, key=lambda d: d["rps"]):
            detalle.append({"k": k, "pedido": " ".join(d["pedido"]),
                            "efectivo": " ".join(d["efectivo"]),
                            "compite": int(d["efectivo"] in mejor),
                            "rps": d["rps"], "hiper": _etiq(d["hiper"])})

    # --- la semilla de mercado, para tener el listón en la misma escala -------------
    ref = _mide_tanda([semilla], cfg)
    rps_sem = ref[0]["rps"] if ref else float("nan")
    h_sem = ref[0]["hiper"] if ref else REJILLA[0]
    gastadas += len(REJILLA)
    if verbose:
        print(f"semilla de mercado {semilla} -> RPS {rps_sem:.6f} "
              f"({_etiq(h_sem)})\n", flush=True)

    # --- paso 1: las 65 solas -------------------------------------------------------
    k = 1
    conj = [(v,) for v in pool]
    res = _mide_tanda(conj, cfg)
    gastadas += len(conj) * len(REJILLA)
    mejor = _absorbe(res, k)
    if not mejor:
        raise RuntimeError("ninguna candidata se pudo ajustar sola")
    anota(k, mejor, res)

    # Las que no se pueden ajustar SOLAS no se marcan ni se expulsan: el fallo es del
    # tamaño uno —la matriz se queda vacía cuando la única columna es constante en una
    # ventana temprana—, no de la variable. Quedan simplemente fuera de este paso y
    # vuelven a competir en k=2, donde tienen una compañera que sostiene la matriz.
    solas = {d["efectivo"][0] for d in res if len(d["efectivo"]) == 1}
    sin_ajuste = [v for v in pool if v not in solas]
    if verbose and sin_ajuste:
        print(f"no ajustan solas, siguen en el pool ({len(sin_ajuste)}): "
              f"{', '.join(sin_ajuste)}\n", flush=True)

    orden = sorted(mejor.values(), key=lambda d: d["rps"])
    haz = [d["efectivo"] for d in orden[:ANCHURA]]
    if verbose:
        print(f"=== paso k=1 · {len(conj)} candidatas solas ===", flush=True)
        for i, d in enumerate(orden[:ANCHURA], 1):
            print(f"  {i}. {d['efectivo'][0]:<30} {d['rps']:.6f}  "
                  f"{_etiq(d['hiper'])}", flush=True)
    filas.append({"k": k, "rps": orden[0]["rps"], "hiper": _etiq(orden[0]["hiper"]),
                  "conjunto": " ".join(orden[0]["efectivo"]), "n_pool": len(pool),
                  "n_eval": len(conj) * len(REJILLA)})
    pasos.append({"k": k, "haz": [list(c) for c in haz],
                  "rps": [mejor[c]["rps"] for c in haz],
                  "hiper": [mejor[c]["hiper"] for c in haz]})
    _marca(mejor, pool, marcas, haz_vars=None, probadas=None)

    mejor_hist = orden[0]["rps"]
    sin_mejora = 0

    # --- pasos 2..MAX_K -------------------------------------------------------------
    while k < MAX_K:
        k += 1
        pool = [v for v in pool if marcas[v] < MARCAS_FUERA]
        conj = sorted({tuple(sorted(set(b) | {v})) for b in haz for v in pool
                       if v not in b})
        if not conj:
            break
        if verbose:
            print(f"\n=== paso k={k} · {len(haz)} haces x pool {len(pool)} = "
                  f"{len(conj)} conjuntos x {len(REJILLA)} penalizaciones "
                  f"· {(time.time() - t_ini) / 60:.0f} min ===", flush=True)
        res = _mide_tanda(conj, cfg)
        gastadas += len(conj) * len(REJILLA)
        mejor = _absorbe(res, k)
        if not mejor:
            if verbose:
                print("     todos los conjuntos del paso quedaron absorbidos",
                      flush=True)
            break
        anota(k, mejor, res)
        orden = sorted(mejor.values(), key=lambda d: d["rps"])
        haz_nuevo = [d["efectivo"] for d in orden[:ANCHURA]]
        previas = set().union(*map(set, haz))
        if verbose:
            n_abs = len(res) - len(mejor)
            for i, d in enumerate(orden[:ANCHURA], 1):
                nuevo = sorted(set(d["efectivo"]) - previas)
                print(f"  {i}. {d['rps']:.6f}  {_etiq(d['hiper']):<14} "
                      f"+{','.join(nuevo) or '-':<28}"
                      f" | {' '.join(d['efectivo'])}", flush=True)
            if n_abs:
                print(f"     absorbidos (no compiten): {n_abs}", flush=True)
        filas.append({"k": k, "rps": orden[0]["rps"],
                      "hiper": _etiq(orden[0]["hiper"]),
                      "conjunto": " ".join(orden[0]["efectivo"]),
                      "n_pool": len(pool),
                      "n_eval": len(conj) * len(REJILLA)})
        pasos.append({"k": k, "haz": [list(c) for c in haz_nuevo],
                      "rps": [mejor[c]["rps"] for c in haz_nuevo],
                      "hiper": [mejor[c]["hiper"] for c in haz_nuevo]})

        fuera = _marca(mejor, pool, marcas,
                       haz_vars=set().union(*map(set, haz_nuevo)),
                       probadas=set(pool))
        if verbose and fuera:
            print(f"     poda: salen {len(fuera)} -> pool {len(pool) - len(fuera)}",
                  flush=True)
        haz = haz_nuevo

        if orden[0]["rps"] < mejor_hist - 1e-9:
            mejor_hist = orden[0]["rps"]
            sin_mejora = 0
        else:
            sin_mejora += 1
            if sin_mejora >= PACIENCIA:
                if verbose:
                    print(f"\nse para: {PACIENCIA} pasos sin mejorar dentro del "
                          f"bloque", flush=True)
                break

    estado = {"semilla": list(semilla), "rps_semilla": rps_sem,
              "hiper_semilla": h_sem, "corte": CORTE, "pasos": pasos,
              "n_banco": banco, "sin_ajuste_solas": sin_ajuste,
              "n_evaluaciones": gastadas,
              "techo_ruido": techo_ruido(max(gastadas, 2)),
              "minutos": (time.time() - t_ini) / 60}
    OUT.mkdir(parents=True, exist_ok=True)
    ORDEN.write_text(json.dumps(estado, ensure_ascii=False, indent=1),
                     encoding="utf-8")
    pd.DataFrame(filas).to_csv(CURVA, index=False, encoding="utf-8")
    pd.DataFrame(detalle).to_csv(DETALLE, index=False, encoding="utf-8")
    return estado


def _marca(mejor: dict[tuple, dict], pool: list[str], marcas: dict[str, int],
           haz_vars, probadas: set[str] | None) -> list[str]:
    """La poda de Manuel: la peor mitad del paso se lleva una marca; con dos, fuera.

    El mérito de una variable en el paso es la **mejor** cifra de los conjuntos en los
    que entró, no la media: lo que se pregunta es si sirve en alguna parte, y castigar a
    una variable por los haces malos en los que también apareció confundiría su valor
    con el del camino. Las que están en el haz vigente quedan indemnes —ya demostraron
    algo— y nunca se baja de `SUELO` candidatas, porque una poda que vacía el pool
    convierte la parada en un artefacto del recorte.

    `probadas` son las candidatas que el paso intentó añadir. Las que se intentaron y
    **no sobreviven en ningún conjunto** —absorbidas en los cinco haces, o inajustables
    al lado de todos ellos— entran en el orden con mérito infinito, de modo que caen en
    la peor mitad y se marcan. Esto es lo que recoge a las redundantes sin necesidad de
    una regla aparte: `ah_line` junto a `mkt_p_home` no aporta ninguna columna, y lo
    correcto no es declararla inerte para siempre sino que acumule marcas como
    cualquiera. En `k=1` se pasa `None`, porque allí no ajustar es un artefacto del
    tamaño uno y no un demérito.
    """
    propio: dict[str, float] = {}
    for d in mejor.values():
        for v in d["efectivo"]:
            if v in marcas and (v not in propio or d["rps"] < propio[v]):
                propio[v] = d["rps"]
    for v in (probadas or ()):
        if v in marcas and v not in propio:
            propio[v] = float("inf")
    vivos = [v for v in pool if v in propio]
    if len(vivos) <= SUELO:
        return []
    orden = sorted(vivos, key=lambda v: propio[v])
    peores = orden[len(orden) // 2:]
    salen = []
    for v in peores:
        if haz_vars is not None and v in haz_vars:
            continue
        marcas[v] += 1
        if marcas[v] >= MARCAS_FUERA:
            salen.append(v)
    # Respetar el suelo: si la poda dejaría el pool por debajo, se perdonan las menos
    # malas hasta cumplirlo.
    cupo = max(0, len(vivos) - SUELO)
    if len(salen) > cupo:
        salen = sorted(salen, key=lambda v: -propio[v])[:cupo]
        for v in set(peores) - set(salen):
            marcas[v] = min(marcas[v], MARCAS_FUERA - 1)
    return salen


# ===========================================================================
# FASE 2 — puntuar fuera de la elección
# ===========================================================================
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


def _parte(p: pd.DataFrame) -> dict:
    s = p.index.get_level_values("season").astype(str)
    d, f = p.loc[s != FUERA], p.loc[s == FUERA]
    return {"rps_dentro": float(d["rps"].mean()), "n_dentro": int(len(d)),
            "rps_fuera": float(f["rps"].mean()), "n_fuera": int(len(f)),
            "acc_fuera": float(f["acierto"].mean())}


def fase2(verbose: bool = True) -> pd.DataFrame:
    estado = json.loads(ORDEN.read_text(encoding="utf-8"))
    cfg = cfg_lee()
    feat = lb.datos()
    semilla = list(estado["semilla"])

    base = _puntua(semilla, estado["hiper_semilla"], cfg, feat)
    bs = _parte(base)
    if verbose:
        print(f"\n=== FASE 2 · {FUERA} nunca participó en elegir ===")
        print(f"semilla desnuda        dentro {bs['rps_dentro']:.6f}  "
              f"fuera {bs['rps_fuera']:.6f}  acc {100 * bs['acc_fuera']:.2f} %\n")
        print(f"{'k':>3} {'dentro':>10} {'fuera':>10} {'gana fuera':>11} "
              f"{'acc':>7}  intervalo (fuera)")

    filas = []
    for paso in estado["pasos"]:
        k = paso["k"]
        cols, h = paso["haz"][0], paso["hiper"][0]
        p = _puntua(cols, h, cfg, feat)
        if p is None:
            continue
        r = _parte(p)
        com = base.index.intersection(p.index)
        s = com.get_level_values("season").astype(str)
        cf = com[s == FUERA]
        dif = pd.DataFrame({"dif": base.loc[cf, "rps"] - p.loc[cf, "rps"]})
        iv = ev.block_bootstrap(dif.reset_index(),
                                lambda x: float(x["dif"].mean()),
                                n_boot=2000, seed=7)
        gana = float(dif["dif"].mean())
        filas.append({"k": k, "conjunto": " ".join(cols), "hiper": _etiq(h),
                      **r, "gana_fuera": gana, "inf": iv["inf"], "sup": iv["sup"],
                      "rps_semilla_fuera": bs["rps_fuera"]})
        if verbose:
            print(f"{k:>3} {r['rps_dentro']:>10.6f} {r['rps_fuera']:>10.6f} "
                  f"{gana:>+11.6f} {100 * r['acc_fuera']:>6.2f} % "
                  f" [{iv['inf']:+.6f}, {iv['sup']:+.6f}]", flush=True)

    tab = pd.DataFrame(filas)
    sal = OUT / "haz_fuera.csv"
    tab.to_csv(sal, index=False, encoding="utf-8")
    if verbose and not tab.empty:
        g = tab.loc[tab["rps_fuera"].idxmin()]
        print(f"\nmejor fuera: k={int(g['k'])}  RPS {g['rps_fuera']:.6f} "
              f"contra {bs['rps_fuera']:.6f} de la semilla "
              f"({g['gana_fuera']:+.6f})")
        print(f"pasos que baten a la semilla fuera: "
              f"{int((tab['gana_fuera'] > 0).sum())} de {len(tab)}")
        print(f"\n{sal}")
    return tab


# ===========================================================================
# FASE 3 — la cuarta puerta, sobre el bloque de confirmación
# ===========================================================================
def fase3(verbose: bool = True) -> pd.DataFrame:
    """Pasa por `seleccion.confirma()` lo que haya sobrevivido a la FASE 2.

    Son dos lecturas independientes y hay que exigir las dos, porque fallan por
    motivos distintos: `2021/22` es posterior al bloque de elección y mide si la
    ganancia aguanta **hacia adelante**; `2017/18-2018/19` son anteriores y miden si
    aguanta **hacia atrás**. Un conjunto que gana en los 760 partidos de uno y pierde
    en los 380 del otro no es una variable que aporte, es la huella de la búsqueda — y
    esa firma ya apareció dos veces en este proyecto, con las dos adopciones de la
    voraz y con `semilla+bajas` en `_bloques.py`.

    Se le pasa a la puerta la **misma penalización** con la que el paso fue adoptado.
    Juzgar con otra no distinguiría la falta de señal del cambio de configuración.
    """
    tab = pd.read_csv(OUT / "haz_fuera.csv")
    if tab.empty:
        print("no hay nada que confirmar: la FASE 2 no dejó ningún paso")
        return tab
    vivos = tab.loc[tab["gana_fuera"] > 0].sort_values("rps_fuera")
    if verbose:
        print(f"\n=== FASE 3 · {len(vivos)} de {len(tab)} pasos baten a la semilla "
              f"fuera y van a la puerta ===", flush=True)
    if vivos.empty:
        print("ninguno bate a la semilla en 2021/22; la puerta no llega a abrirse")
        return vivos
    feat = lb.datos()
    filas = []
    for _, f in vivos.iterrows():
        cols = str(f["conjunto"]).split()
        C, l1 = str(f["hiper"]).replace("C=", "").split("/l1=")
        h = {"C": float(C), "l1_ratio": float(l1)}
        r = sl.confirma(cols, feat=feat, hiper=h, verbose=verbose)
        filas.append({"k": int(f["k"]), "conjunto": f["conjunto"],
                      "hiper": f["hiper"], "gana_fuera": f["gana_fuera"],
                      "confirmado": bool(r.get("confirmado")),
                      "cuesta": r.get("cuesta"), "n_conf": r.get("n"),
                      "motivo": r.get("motivo")})
    res = pd.DataFrame(filas)
    sal = OUT / "haz_confirma.csv"
    res.to_csv(sal, index=False, encoding="utf-8")
    if verbose:
        ok = res.loc[res["confirmado"]]
        print(f"\nconfirmados: {len(ok)} de {len(res)}")
        if not ok.empty:
            g = ok.loc[ok["gana_fuera"].idxmax()]
            print(f"el que aguanta las dos lecturas: k={int(g['k'])} · "
                  f"{g['conjunto']} ({g['hiper']})")
        print(f"\n{sal}")
    return res


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    solo2 = "--fase2" in argv
    solo3 = "--fase3" in argv
    if not solo2 and not solo3:
        est = fase1(rapido="--rapido" in argv)
        print(f"\nFASE 1: {est['n_evaluaciones']} evaluaciones en "
              f"{est['minutos']:.0f} min · listón de ruido dentro del bloque "
              f"{est['techo_ruido']:+.6f}")
    if not solo3:
        fase2()
    fase3()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
