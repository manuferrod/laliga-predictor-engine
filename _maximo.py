"""El conjunto que maximiza ACIERTO y ROI sobre las seis temporadas de validación.

Esto es lo que Manuel pidió, literalmente: «el modelo con el conjunto de variables que
mejore el accuracy y el roi al maximo para todas las temporadas disponibles». Las
búsquedas anteriores del proyecto —la voraz, el haz de `_haz.py`, el lasso de
`_ridge.py`, los bloques— **seleccionan todas por RPS**, y la regresión se ajusta por
máxima verosimilitud, que es log-loss. De modo que cuando esas rutas concluyeron «las
variables no ayudan», lo que demostraron es que no ayudan *al RPS*. Este módulo cierra
ese hueco: busca guiándose por las dos métricas que Manuel quiere maximizar.

Tres decisiones de diseño, con su motivo medido:

**1. El bloque es `validacion` entero: 2019/20–2024/25, 2280 partidos, seis
temporadas.** No se parte en selección y medición. Es lo que se pidió —todas las
temporadas disponibles— y tiene una ventaja técnica real para el acierto: sobre 1140
partidos el acierto solo se mueve a saltos de 0.088 puntos y la voraz guiada por él
adoptaba tres de seis pasos por desempate ciego (está escrito en `seleccion.CRITERIOS`);
sobre 2280 el salto es 0.0439 y esa patología se reduce a la mitad.

El holdout 2025/26 **no entra, y no es una decisión mía**: `laboratorio.evalua` lanza
`PermissionError` para `evaluar="holdout"` y para `evaluar="produccion"`. El banco no
puede abrirlos. `validacion` es todo lo que hay, y es exactamente «del 19 hasta hoy»
descontando la temporada sellada y 2026/27, que son **78 partidos en diez pliegues** y no
sostienen ninguna lectura: el error típico del ROI plano ahí es de **10.93 pp**.

**2. Se liquida a cuota de Bet365**, no a la que trae el protocolo por defecto. El
defecto de `laboratorio` es `cierre_avg` —media de cierre—, y eso es un precio
retrospectivo que el sistema semanal no puede conseguir: predice con días de
antelación. Medido sobre la semilla, la diferencia no es cosmética: el ROI plano sale
**−0.4421 % con `cierre_avg` y −1.2990 % con Bet365**, 0.86 puntos de regalo por usar
un precio que no existe cuando se apuesta. Bet365 es además el listón declarado del
proyecto (`backtest.REFERENCIA`), así que es el único precio con el que la cifra del
modelo y la del listón son comparables (regla 3).

Como las tres familias de cuota viven en la matriz sin un solo hueco, **una sola
evaluación da el ROI plano a las tres**: Bet365, consenso y máxima. Sale gratis y es la
palanca más grande del proyecto —`_dondecobras.py` mide +4.483 puntos [+4.264, +4.701]
entre Bet365 y la mejor cuota, a modelo y pronóstico idénticos—, así que viaja en cada
fila para que no haya que volver a buscarla.

**3. El ROI es el de Manuel: una unidad plana al signo que el modelo señala, en todos
los partidos.** No es el `roi` de `Cifras`, que es apuesta de valor sobre el
subconjunto donde la probabilidad supera a la implícita. La distinción es la que hace
que perseguir el ROI funcione o no: el ROI de valor se liquida sobre un subconjunto que
**se mueve con cada columna que entra** —el primer paso forzado de la voraz guiada por
`ventaja` lo lanza de +1.67 % a +6.97 % con una sola variable—, así que guiar por él es
guiar por el tamaño de la muestra tanto como por el acierto. El ROI plano tiene n fijo
en los 2280 partidos y es acierto ponderado por cuota: discrimina sin trampa.

**El haz es doble.** Cada paso conserva las `ANCHURA` mejores por acierto y las
`ANCHURA` mejores por ROI plano, y la unión de las dos es el haz del paso siguiente. Un
solo recorrido entrega las dos respuestas, y además las deja cruzarse: un conjunto
bueno de acierto puede extenderse en el mejor de ROI.

Lo que esta medida NO puede decir, dicho antes de verla: elige y mide sobre los mismos
2280 partidos, así que la cifra ganadora es optimista por construcción, y el optimismo
crece con el número de configuraciones probadas. Eso no se arregla con un argumento, se
acota con una lectura de fuera, y la única que queda sin tocar el holdout son 2017/18 y
2018/19 —760 partidos anteriores al bloque, el arranque—. `fase3` la pide. La cifra de
dentro es la que se pidió y va primero; la de fuera va al lado, sin negociar.

Uso:
    python _maximo.py              # las 65 solas (rápido, ~15 min) y luego el haz
    python _maximo.py --criba      # solo las 65 solas
    python _maximo.py --fase3      # juzga a los ganadores en 2017/18-2018/19
"""
from __future__ import annotations

import os

# Antes de numpy: cada proceso hijo ajusta matrices pequeñas y el BLAS multihilo se
# estorba a sí mismo. Medido en `_coste.py`: 12.87 s por protocolo con un hilo contra
# 5.53 s de media repartiendo en diez procesos de un hilo; sin fijar, la
# sobresuscripción de 12x12 hilos lo empeoraba.
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

import json                                                 # noqa: E402
import sys                                                  # noqa: E402
import time                                                 # noqa: E402
from pathlib import Path                                    # noqa: E402

import numpy as np                                          # noqa: E402
import pandas as pd                                         # noqa: E402

_RAIZ = Path(__file__).resolve().parent
if str(_RAIZ) not in sys.path:
    sys.path.insert(0, str(_RAIZ))

import scripts.models.laboratorio as lb                     # noqa: E402
import scripts.models.protocol as pt                        # noqa: E402
import scripts.models.seleccion as sl                       # noqa: E402

BLOQUE = "validacion"
FAMILIA = "logistica"
CASA = "b365"

ANCHURA = 3          # por objetivo; el haz efectivo es la unión, hasta 6 conjuntos
MAX_K = 10
PACIENCIA = 3
SUELO = 15           # el pool nunca baja de aquí: podar hasta el hueso es decidir
TERCIO = 3           # se marca al peor tercio por suma de rangos

# Las dos que acotan el rango de 0.0061 de RPS que mide `_ridge.py` sobre las 65
# columnas: la base con la que corrió toda la búsqueda anterior y la que la bate. Se
# conservan las dos porque dejar la penalización clavada fue uno de los dos defectos
# de procedimiento que documenta el CLAUDE.md, y repetirlo aquí sería reintroducirlo.
REJILLA: list[dict] = [
    {"C": 0.3, "l1_ratio": 0.0},
    {"C": 0.03, "l1_ratio": 1.0},
]

VENTANA = {"window": "expansiva", "sliding_seasons": pt.SLIDING_SEASONS,
           "halflife_days": None}

PROB = ["p_home", "p_draw", "p_away"]
CUOTA = ["cuota_home", "cuota_draw", "cuota_away"]
FAMILIAS_CUOTA = {"avg": ["odds_avg_h", "odds_avg_d", "odds_avg_a"],
                  "max": ["odds_max_h", "odds_max_d", "odds_max_a"]}

# Las rutas las declara `seleccion.py`, junto a las del resto de las rutas, y aquí solo
# se les pone nombre corto. El mismo literal escrito en dos ficheros es exactamente como
# se descuelga el cuaderno de lo que el guion dejó en disco.
OUT = sl.OUT
CRIBA = sl.MAXIMO_CRIBA
CURVA = sl.MAXIMO_CURVA
DETALLE = sl.MAXIMO_DETALLE
GANADORES = sl.MAXIMO_GANADORES
LOG = OUT / "maximo.log"

_OTRAS: pd.DataFrame | None = None


def cfg() -> pt.ProtocolConfig:
    return pt.ProtocolConfig(regime="semanal", **VENTANA)


def _etiq(h: dict) -> str:
    return f"C={h['C']}/l1={h['l1_ratio']}"


def _otras(feat: pd.DataFrame) -> pd.DataFrame:
    """Consenso y máxima, por `match_key`. Se cachea por proceso."""
    global _OTRAS
    if _OTRAS is None:
        cols = [c for v in FAMILIAS_CUOTA.values() for c in v]
        _OTRAS = feat[["match_key"] + cols].drop_duplicates("match_key")
    return _OTRAS


# ===========================================================================
# 1. Medir un conjunto
# ===========================================================================
def _plano(ok: np.ndarray, q: np.ndarray) -> np.ndarray:
    """Beneficio de una unidad plana: el acierto paga `cuota - 1`, el fallo pierde 1."""
    return np.where(ok, q - 1.0, -1.0)


def _cifras(e, pedido: tuple, hiper: dict) -> dict:
    feat = lb.datos()
    p = e.pred.merge(_otras(feat), on="match_key", how="left")
    y = p["y_num"].to_numpy(dtype=int)
    fila = np.arange(len(p))
    P = p[PROB].to_numpy(dtype=float)
    sel = P.argmax(axis=1)
    ok = sel == y

    Q = p[CUOTA].to_numpy(dtype=float)
    ben = _plano(ok, Q[fila, sel])
    # El listón de la regla 3: el favorito de Bet365 es su cuota MÁS BAJA, una unidad
    # plana, los mismos partidos. Emparejado, así que su diferencia tiene error típico
    # propio y mucho menor que el de los dos niveles por separado.
    fav = Q.argmin(axis=1)
    okr = fav == y
    benr = _plano(okr, Q[fila, fav])
    d = ben - benr

    roi_otras = {}
    for nom, cols in FAMILIAS_CUOTA.items():
        Qo = p[cols].to_numpy(dtype=float)
        roi_otras[nom] = float(_plano(ok, Qo[fila, sel]).mean())

    temp = {}
    for s, g in p.assign(_b=ben, _ok=ok).groupby(p["season"].astype(str)):
        temp[s] = {"n": int(len(g)), "acierto": float(g["_ok"].mean()),
                   "roi": float(g["_b"].mean())}

    return {"pedido": pedido,
            "efectivo": tuple(sorted(set(pedido) - set(e.ausentes))),
            "hiper": hiper, "rps": float(e.rps), "acierto": float(ok.mean()),
            "roi": float(ben.mean()), "roi_avg": roi_otras["avg"],
            "roi_max": roi_otras["max"], "acierto_ref": float(okr.mean()),
            "roi_ref": float(benr.mean()), "vent_roi": float(d.mean()),
            "ee_vent": float(d.std(ddof=1) / np.sqrt(len(d))),
            "cuota_media": float(Q[fila, sel].mean()), "n": int(len(p)),
            "temporadas": temp}


def _mide_uno(pedido: tuple, hiper: dict, c) -> dict | str:
    """Un conjunto, un protocolo completo, sobre las seis temporadas.

    Nivel de módulo porque es lo que se envía a los procesos hijos; una función
    anidada no se puede serializar. Devuelve el motivo como texto si el conjunto no
    se puede ajustar: una candidata inviable es un resultado del paso, no un motivo
    para tirar la búsqueda.
    """
    try:
        e = lb.evalua(list(pedido), FAMILIA, hiper=hiper, cfg=c,
                      feat=lb.datos(), evaluar=BLOQUE, cuotas=CASA,
                      n_boot=0, registrar=False, verbose=False)
        return _cifras(e, pedido, hiper)
    except Exception as exc:                                # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"


def _lote(conjuntos: list[tuple], hiper: dict, c, n_jobs: int = 0) -> list:
    n = sl._nucleos(n_jobs)
    if n == 1 or len(conjuntos) == 1:
        return [_mide_uno(cc, hiper, c) for cc in conjuntos]
    from joblib import Parallel, delayed
    return list(Parallel(n_jobs=n, backend="loky", batch_size=1)(
        delayed(_mide_uno)(cc, hiper, c) for cc in conjuntos))


def _tanda(conjuntos: list[tuple], c) -> list[dict]:
    """Cada conjunto con las dos penalizaciones; se queda la mejor de las dos.

    «Mejor» aquí no es evidente, porque hay dos objetivos. Se resuelve por **suma de
    rangos** dentro de la propia pareja: la penalización que gane en acierto y ROI
    gana; si cada una gana en una, decide el ROI, que es la métrica con resolución
    continua. Lo que NO se hace es fijar una penalización para todo el recorrido: eso
    compararía un conjunto de diez con un castigo de varianza calibrado para tres, y
    `_bloques.py` midió lo que cuesta —0.005663 de RPS al conjunto más grande contra
    0.000006 a la semilla—.
    """
    best: dict[tuple, dict] = {}
    for h in REJILLA:
        for cc, r in zip(conjuntos, _lote(conjuntos, h, c)):
            if isinstance(r, str) or not np.isfinite(r["roi"]):
                continue
            viejo = best.get(cc)
            if viejo is None:
                best[cc] = r
                continue
            gana = ((r["acierto"] > viejo["acierto"]) + (r["roi"] > viejo["roi"]))
            pierde = ((r["acierto"] < viejo["acierto"]) + (r["roi"] < viejo["roi"]))
            if gana > pierde or (gana == pierde and r["roi"] > viejo["roi"]):
                best[cc] = r
    return list(best.values())


def _absorbe(res: list[dict], k: int) -> list[dict]:
    """Solo compiten los conjuntos que de verdad conservan sus `k` columnas.

    Si a un haz que lleva `mkt_p_home` se le añade `ah_line` —colineal a 0.9768— el
    preprocesado la descarta y el conjunto resultante es el haz padre con otro nombre.
    Dejarlo competir hacía que un paso presumiera de mejorar al anterior siendo
    idéntico. Se mira el conjunto EFECTIVO y los absorbidos se colapsan; la candidata
    sigue en el pool, porque en otra compañía puede no ser redundante.
    """
    mejor: dict[tuple, dict] = {}
    for d in res:
        if len(d["efectivo"]) != k:
            continue
        ef = d["efectivo"]
        if ef not in mejor or d["roi"] > mejor[ef]["roi"]:
            mejor[ef] = d
    return list(mejor.values())


# ===========================================================================
# 2. El haz doble
# ===========================================================================
def _haz_doble(cands: list[dict]) -> tuple[list[tuple], dict]:
    """Las `ANCHURA` mejores por acierto más las `ANCHURA` mejores por ROI."""
    por_ac = sorted(cands, key=lambda d: -d["acierto"])[:ANCHURA]
    por_roi = sorted(cands, key=lambda d: -d["roi"])[:ANCHURA]
    haz, vistos = [], set()
    for d in por_ac + por_roi:
        if d["efectivo"] not in vistos:
            vistos.add(d["efectivo"])
            haz.append(d["efectivo"])
    return haz, {"acierto": por_ac[0], "roi": por_roi[0]}


def _rangos(cands: list[dict], pool: list[str]) -> dict[str, float]:
    """Suma de rangos de cada variable: el mejor conjunto en que aparece, por métrica.

    Promediar sobre todos los conjuntos castigaría a una variable buena que apareció
    en malas compañías. Se le atribuye su mejor resultado, que es la pregunta correcta:
    «¿hay alguna combinación en la que esta variable sirva?».
    """
    mejor_ac = {v: -np.inf for v in pool}
    mejor_roi = {v: -np.inf for v in pool}
    for d in cands:
        for v in d["efectivo"]:
            if v in mejor_ac:
                mejor_ac[v] = max(mejor_ac[v], d["acierto"])
                mejor_roi[v] = max(mejor_roi[v], d["roi"])
    ra = pd.Series(mejor_ac).rank(ascending=False)
    rr = pd.Series(mejor_roi).rank(ascending=False)
    return (ra + rr).to_dict()


def criba(verbose: bool = True) -> pd.DataFrame:
    """Las 65 candidatas, una a una, y la semilla de mercado. El paso k=1."""
    c = cfg()
    feat = lb.datos()
    pool = sl.candidatas(feat)
    OUT.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    ref = _tanda([tuple(sl.SEMILLA_MERCADO)], c)
    sem = ref[0] if ref else None
    if verbose and sem:
        print(f"semilla de mercado ({len(sl.SEMILLA_MERCADO)} var, "
              f"{_etiq(sem['hiper'])}): acierto {100 * sem['acierto']:.2f}% · "
              f"ROI {100 * sem['roi']:+.3f}% · RPS {sem['rps']:.6f}", flush=True)
        print(f"listón b365 (favorito, plana): acierto "
              f"{100 * sem['acierto_ref']:.2f}% · ROI {100 * sem['roi_ref']:+.3f}%\n",
              flush=True)

    res = _tanda([(v,) for v in pool], c)
    tab = pd.DataFrame([{"variable": d["efectivo"][0] if d["efectivo"] else "",
                         "hiper": _etiq(d["hiper"]), "rps": d["rps"],
                         "acierto": d["acierto"], "roi": d["roi"],
                         "roi_avg": d["roi_avg"], "roi_max": d["roi_max"],
                         "cuota_media": d["cuota_media"]}
                        for d in res if len(d["efectivo"]) == 1])
    tab = tab.sort_values("acierto", ascending=False).reset_index(drop=True)
    tab.to_csv(CRIBA, index=False, encoding="utf-8")

    if verbose:
        print(f"=== las {len(tab)} candidatas solas, {len(pool)} en el banco, "
              f"{time.time() - t0:.0f} s ===\n")
        print(f"{'#':>3} {'variable':<28} {'acierto':>8} {'ROI':>9} {'RPS':>9}")
        for i, r in tab.head(12).iterrows():
            print(f"{i + 1:>3} {r['variable']:<28} {100 * r['acierto']:>7.2f}% "
                  f"{100 * r['roi']:>+8.2f}% {r['rps']:>9.6f}")
        mr = tab.sort_values("roi", ascending=False).head(8)
        print("\nlas 8 mejores por ROI plano:")
        for i, r in mr.iterrows():
            print(f"    {r['variable']:<28} {100 * r['roi']:>+8.2f}% "
                  f"acierto {100 * r['acierto']:>6.2f}%")
        n_fallan = len(pool) - len(tab)
        if n_fallan:
            print(f"\n{n_fallan} no ajustan solas (matriz vacía en alguna ventana); "
                  f"siguen en el pool")
    return tab


def fase1(verbose: bool = True) -> dict:
    """El haz doble, de k=1 a MAX_K, con poda por suma de rangos."""
    c = cfg()
    feat = lb.datos()
    pool = sl.candidatas(feat)
    banco = len(pool)
    OUT.mkdir(parents=True, exist_ok=True)

    ref = _tanda([tuple(sl.SEMILLA_MERCADO)], c)
    sem = ref[0]
    filas: list[dict] = []
    detalle: list[dict] = []
    marcas: dict[str, int] = {}
    gastadas = 1
    t0 = time.time()

    def anota(k: int, cands: list[dict], haz: list[tuple]) -> None:
        en_haz = set(haz)
        for d in sorted(cands, key=lambda x: -x["roi"]):
            detalle.append({"k": k, "pedido": " ".join(d["pedido"]),
                            "efectivo": " ".join(d["efectivo"]),
                            "en_haz": int(d["efectivo"] in en_haz),
                            "hiper": _etiq(d["hiper"]), "rps": d["rps"],
                            "acierto": d["acierto"], "roi": d["roi"],
                            "roi_avg": d["roi_avg"], "roi_max": d["roi_max"]})

    def guarda(k: int, lider: dict, objetivo: str, d: dict) -> None:
        filas.append({"k": k, "objetivo": objetivo,
                      "conjunto": " ".join(d["efectivo"]), "hiper": _etiq(d["hiper"]),
                      "rps": d["rps"], "acierto": d["acierto"], "roi": d["roi"],
                      "roi_avg": d["roi_avg"], "roi_max": d["roi_max"],
                      "acierto_ref": d["acierto_ref"], "roi_ref": d["roi_ref"],
                      "vent_roi": d["vent_roi"], "ee_vent": d["ee_vent"],
                      "cuota_media": d["cuota_media"], "n": d["n"],
                      "temporadas": json.dumps(d["temporadas"])})

    haz: list[tuple] = []
    mejor_ac, mejor_roi = sem["acierto"], sem["roi"]
    quieto_ac = quieto_roi = 0
    lideres: dict[str, dict] = {"acierto": sem, "roi": sem}

    for k in range(1, MAX_K + 1):
        if k == 1:
            conj = [(v,) for v in pool]
        else:
            conj = [tuple(sorted(set(h) | {v})) for h in haz
                    for v in pool if v not in h]
            conj = list(dict.fromkeys(conj))
        if not conj:
            break
        gastadas += len(conj)
        res = _tanda(conj, c)
        cands = _absorbe(res, k)
        if not cands:
            if verbose:
                print(f"k={k}: ningún conjunto conserva {k} columnas; se para")
            break

        haz, top = _haz_doble(cands)
        anota(k, cands, haz)
        for obj in ("acierto", "roi"):
            guarda(k, top[obj], obj, top[obj])
            if top[obj][obj] > lideres[obj][obj]:
                lideres[obj] = top[obj]

        if verbose:
            a, r = top["acierto"], top["roi"]
            print(f"k={k:>2} | mejor acierto {100 * a['acierto']:>6.2f}% "
                  f"(ROI {100 * a['roi']:>+6.2f}%) | mejor ROI "
                  f"{100 * r['roi']:>+6.2f}% (acierto {100 * r['acierto']:>6.2f}%)"
                  f" | pool {len(pool):>2} | {time.time() - t0:.0f} s", flush=True)
            print(f"      acierto: {' '.join(a['efectivo'])}")
            print(f"      roi    : {' '.join(r['efectivo'])}", flush=True)

        quieto_ac = 0 if top["acierto"]["acierto"] > mejor_ac else quieto_ac + 1
        quieto_roi = 0 if top["roi"]["roi"] > mejor_roi else quieto_roi + 1
        mejor_ac = max(mejor_ac, top["acierto"]["acierto"])
        mejor_roi = max(mejor_roi, top["roi"]["roi"])
        if quieto_ac >= PACIENCIA and quieto_roi >= PACIENCIA:
            if verbose:
                print(f"\nparo: {PACIENCIA} pasos sin mejorar ni acierto ni ROI")
            break

        # Poda. Se marca al peor tercio por suma de rangos, nunca a una variable del
        # haz, y el pool no baja de SUELO. Dos marcas expulsan.
        inmunes = set().union(*map(set, haz)) if haz else set()
        rk = _rangos(cands, pool)
        orden = sorted(pool, key=lambda v: -rk.get(v, 0.0))
        corte = max(0, len(pool) - SUELO) and len(pool) // TERCIO
        for v in orden[:corte]:
            if v not in inmunes:
                marcas[v] = marcas.get(v, 0) + 1
        nuevo = [v for v in pool if marcas.get(v, 0) < 2 or v in inmunes]
        pool = nuevo if len(nuevo) >= SUELO else pool

    pd.DataFrame(filas).to_csv(CURVA, index=False, encoding="utf-8")
    pd.DataFrame(detalle).to_csv(DETALLE, index=False, encoding="utf-8")
    estado = {"bloque": BLOQUE, "casa": CASA, "n_banco": banco,
              "evaluaciones": gastadas * len(REJILLA),
              "minutos": (time.time() - t0) / 60,
              "semilla": {k: sem[k] for k in
                          ("rps", "acierto", "roi", "roi_avg", "roi_max",
                           "acierto_ref", "roi_ref")},
              "ganador_acierto": {"conjunto": list(lideres["acierto"]["efectivo"]),
                                  "hiper": lideres["acierto"]["hiper"],
                                  "acierto": lideres["acierto"]["acierto"],
                                  "roi": lideres["acierto"]["roi"],
                                  "rps": lideres["acierto"]["rps"]},
              "ganador_roi": {"conjunto": list(lideres["roi"]["efectivo"]),
                              "hiper": lideres["roi"]["hiper"],
                              "acierto": lideres["roi"]["acierto"],
                              "roi": lideres["roi"]["roi"],
                              "rps": lideres["roi"]["rps"]}}
    GANADORES.write_text(json.dumps(estado, indent=2, ensure_ascii=False),
                         encoding="utf-8")

    if verbose:
        _resumen(estado, sem, lideres)
    return estado


def _resumen(estado: dict, sem: dict, lideres: dict) -> None:
    print(f"\n{'=' * 74}")
    print(f"{estado['evaluaciones']} evaluaciones · "
          f"{estado['minutos']:.0f} min · {sem['n']} partidos, seis temporadas")
    print(f"{'=' * 74}\n")
    print(f"{'quien':<22} {'acierto':>8} {'ROI b365':>9} {'ROI avg':>9} "
          f"{'ROI max':>9} {'RPS':>9}")
    fil = [("listón b365 (favorito)", sem["acierto_ref"], sem["roi_ref"],
            float("nan"), float("nan"), float("nan")),
           ("semilla de mercado", sem["acierto"], sem["roi"], sem["roi_avg"],
            sem["roi_max"], sem["rps"])]
    for obj in ("acierto", "roi"):
        d = lideres[obj]
        fil.append((f"ganador por {obj}", d["acierto"], d["roi"], d["roi_avg"],
                    d["roi_max"], d["rps"]))
    for nom, a, r, ra, rm, rp in fil:
        print(f"{nom:<22} {100 * a:>7.2f}% {100 * r:>+8.2f}% "
              f"{100 * ra:>+8.2f}% {100 * rm:>+8.2f}% {rp:>9.6f}")
    for obj in ("acierto", "roi"):
        d = lideres[obj]
        print(f"\nganador por {obj} ({len(d['efectivo'])} var, {_etiq(d['hiper'])}):")
        print(f"  {' '.join(d['efectivo'])}")
        t = pd.DataFrame(d["temporadas"]).T
        print(f"  {'temporada':>10} {'n':>5} {'acierto':>8} {'ROI':>9}")
        for s, r in t.iterrows():
            print(f"  {s:>10} {int(r['n']):>5} {100 * r['acierto']:>7.2f}% "
                  f"{100 * r['roi']:>+8.2f}%")
    print(f"\n{CURVA}\n{DETALLE}\n{GANADORES}")


# ===========================================================================
# 3. La lectura de fuera: 2017/18-2018/19, que no participaron en elegir
# ===========================================================================
TEMPORADAS_FUERA = ("2017/18", "2018/19")
FUERA = sl.MAXIMO_FUERA


def _fuera_uno(cols: list[str], hiper: dict, feat: pd.DataFrame) -> pd.DataFrame:
    """Liquidación partido a partido en las dos temporadas de arranque.

    El bloque se abre igual que en `seleccion.confirma()`: bajando el arranque a
    2016/17 sola, de modo que 2017/18 y 2018/19 pasan a ser evaluables y el resto del
    protocolo —ventana expansiva, ajuste solo con pasado— no se toca. La regla 6 queda
    en pie porque estas dos temporadas son ANTERIORES a todo lo que participó en
    elegir.
    """
    abierto = pt.ProtocolConfig(**{**cfg().__dict__,
                                   "warmup_seasons": list(sl.WARMUP_CONFIRMACION)})
    e = lb.evalua(list(cols), FAMILIA, hiper=hiper, cfg=abierto, feat=feat,
                  evaluar="validacion", cuotas=CASA, n_boot=0, registrar=False,
                  verbose=False)
    p = e.pred.loc[e.pred["season"].astype(str).isin(TEMPORADAS_FUERA)]
    p = p.merge(_otras(feat), on="match_key", how="left")
    fila = np.arange(len(p))
    y = p["y_num"].to_numpy(dtype=int)
    sel = p[PROB].to_numpy(dtype=float).argmax(axis=1)
    Q = p[CUOTA].to_numpy(dtype=float)
    ok = sel == y
    fav = Q.argmin(axis=1)
    okr = fav == y
    out = {"ok": ok.astype(float), "ben": _plano(ok, Q[fila, sel]),
           "ok_ref": okr.astype(float),
           "ben_ref": _plano(okr, Q[fila, fav])}
    # El mismo pronóstico liquidado en las otras dos familias de cuota. Viaja aquí
    # porque es la única palanca del proyecto que se ha medido a 40 errores típicos
    # (`_dondecobras.py`: +4.483 puntos [+4.264, +4.701] de Bet365 a la mejor cuota
    # dentro del bloque) y hay que ver si replica fuera, donde ningún conjunto lo hace.
    for nom, cols in FAMILIAS_CUOTA.items():
        Qo = p[cols].to_numpy(dtype=float)
        out[f"ben_{nom}"] = _plano(ok, Qo[fila, sel])
    return pd.DataFrame(out, index=p["match_key"].to_numpy())


def _par(a: np.ndarray, b: np.ndarray) -> tuple[float, float, float, float]:
    """Diferencia emparejada y su error típico: media, ee, inferior, superior."""
    d = a - b
    m = float(d.mean())
    ee = float(d.std(ddof=1) / np.sqrt(len(d)))
    return m, ee, m - 1.96 * ee, m + 1.96 * ee


def fase3(verbose: bool = True, rps: bool = True) -> pd.DataFrame:
    """Qué hacen los ganadores en 760 partidos que no los eligieron.

    No es un holdout y no rompe la regla 7: son las dos temporadas de arranque, y
    `seleccion.confirma()` ya las usa para lo mismo bajando el arranque a 2016/17
    sola. Es la única lectura de fuera que queda, y existe porque elegir y medir sobre
    los mismos 2280 partidos es optimista por construcción. La cifra de dentro es la
    que se pidió; esta va al lado.

    **Se mide en acierto y en ROI plano, no solo en RPS.** `seleccion.confirma()`
    juzga exclusivamente por RPS, y eso era correcto para las búsquedas que guiaba:
    todas seleccionaban por RPS, así que la puerta preguntaba por la misma magnitud que
    la elección. Esta búsqueda no. Pasarle los ganadores a `confirma()` tal cual
    respondería a una pregunta distinta de la que se hizo —y además una que ya sabemos
    que contestan mal, porque su RPS de DENTRO ya es peor que el de la semilla, luego
    el de fuera no puede ser la prueba de nada nuevo—. El veredicto de RPS se conserva
    igualmente en la tabla, porque la regla 4 dice que cuando ROI y RPS discrepan manda
    RPS, y para invocarla hay que tener las dos cifras delante.

    Todo se compara **emparejado y sobre el mismo censo**: mismos partidos, misma
    jornada, mismo precio, y la diferencia se promedia partido a partido. Promediar dos
    niveles medidos sobre censos distintos mezclaría la diferencia entre conjuntos con
    la diferencia entre partidos, que es el motivo por el que `confirma()` intersecta.
    """
    estado = json.loads(GANADORES.read_text(encoding="utf-8"))
    feat = lb.datos()

    cuales = [("semilla", list(sl.SEMILLA_MERCADO), REJILLA[0])]
    for obj in ("acierto", "roi"):
        g = estado[f"ganador_{obj}"]
        cuales.append((f"ganador_{obj}", list(g["conjunto"]), g["hiper"]))

    liq, rps_fuera = {}, {}
    for nom, cols, hiper in cuales:
        if verbose:
            print(f"midiendo {nom} ({len(cols)} var, {_etiq(hiper)}) en "
                  f"{'+'.join(TEMPORADAS_FUERA)}...", flush=True)
        liq[nom] = _fuera_uno(cols, hiper, feat)

    censo = liq["semilla"].index
    for nom in liq:
        censo = censo.intersection(liq[nom].index)
    liq = {k: v.loc[censo] for k, v in liq.items()}
    n = len(censo)

    base = liq["semilla"]
    fil = []
    for nom in ("semilla", "ganador_acierto", "ganador_roi"):
        d = liq[nom]
        f = {"quien": nom, "n": n, "acierto": float(d["ok"].mean()),
             "roi": float(d["ben"].mean()),
             "acierto_ref": float(d["ok_ref"].mean()),
             "roi_ref": float(d["ben_ref"].mean())}
        # Contra el listón de Bet365 (regla 3) y contra la semilla, las dos
        # emparejadas sobre los mismos partidos.
        for suf, otro in (("liston", d[["ok_ref", "ben_ref"]].to_numpy()),
                          ("semilla", base[["ok", "ben"]].to_numpy())):
            va, ea, ia, sa = _par(d["ok"].to_numpy(), otro[:, 0])
            vr, er, ir, sr = _par(d["ben"].to_numpy(), otro[:, 1])
            f |= {f"dac_{suf}": va, f"ee_dac_{suf}": ea,
                  f"inf_dac_{suf}": ia, f"sup_dac_{suf}": sa,
                  f"droi_{suf}": vr, f"ee_droi_{suf}": er,
                  f"inf_droi_{suf}": ir, f"sup_droi_{suf}": sr}
        fil.append(f)
    tab = pd.DataFrame(fil)

    if verbose:
        print(f"\n=== fuera de bloque: {'+'.join(TEMPORADAS_FUERA)}, {n} partidos "
              f"que no eligieron nada ===\n")
        print(f"{'quien':<16} {'acierto':>8} {'ROI b365':>9} | "
              f"{'vs listón':>10} {'ee':>6} | {'vs semilla':>11} {'ee':>6}")
        for _, r in tab.iterrows():
            print(f"{r['quien']:<16} {100 * r['acierto']:>7.2f}% "
                  f"{100 * r['roi']:>+8.2f}% | "
                  f"{100 * r['droi_liston']:>+9.2f}% {100 * r['ee_droi_liston']:>5.2f} "
                  f"| {100 * r['droi_semilla']:>+10.2f}% "
                  f"{100 * r['ee_droi_semilla']:>5.2f}")
        print(f"\nlistón b365 en estos partidos: "
              f"{100 * tab.loc[0, 'acierto_ref']:.2f}% de acierto, "
              f"{100 * tab.loc[0, 'roi_ref']:+.2f}% de ROI plano\n")
        for _, r in tab.iloc[1:].iterrows():
            print(f"{r['quien']}:")
            print(f"  acierto {100 * r['dac_semilla']:+.2f} pp sobre la semilla "
                  f"[{100 * r['inf_dac_semilla']:+.2f}, "
                  f"{100 * r['sup_dac_semilla']:+.2f}]")
            print(f"  ROI     {100 * r['droi_semilla']:+.2f} pp sobre la semilla "
                  f"[{100 * r['inf_droi_semilla']:+.2f}, "
                  f"{100 * r['sup_droi_semilla']:+.2f}]")
            print(f"  ROI     {100 * r['droi_liston']:+.2f} pp sobre el listón "
                  f"[{100 * r['inf_droi_liston']:+.2f}, "
                  f"{100 * r['sup_droi_liston']:+.2f}]", flush=True)

    # La palanca del precio, emparejada y sobre estos mismos partidos. Va al lado de
    # la tabla de conjuntos a propósito: las dos responden a «¿cómo subo el ROI?» y
    # conviene verlas a la misma escala.
    for nom in ("semilla", "ganador_acierto", "ganador_roi"):
        d = liq[nom]
        for fam in FAMILIAS_CUOTA:
            v, ee, i, s = _par(d[f"ben_{fam}"].to_numpy(), d["ben"].to_numpy())
            tab.loc[tab["quien"] == nom, f"roi_{fam}"] = float(d[f"ben_{fam}"].mean())
            tab.loc[tab["quien"] == nom, f"d_{fam}"] = v
            tab.loc[tab["quien"] == nom, f"ee_{fam}"] = ee
            tab.loc[tab["quien"] == nom, f"inf_{fam}"] = i
            tab.loc[tab["quien"] == nom, f"sup_{fam}"] = s
    if verbose:
        print(f"\n--- la palanca del precio en los mismos {n} partidos de fuera "
              f"---\n")
        print(f"{'quien':<16} {'b365':>9} {'avg':>9} {'max':>9} | "
              f"{'max-b365':>9} {'ee':>5}")
        for _, r in tab.iterrows():
            print(f"{r['quien']:<16} {100 * r['roi']:>+8.2f}% "
                  f"{100 * r['roi_avg']:>+8.2f}% {100 * r['roi_max']:>+8.2f}% | "
                  f"{100 * r['d_max']:>+8.2f}% {100 * r['ee_max']:>4.2f}")
        r = tab.iloc[0]
        print(f"\nsemilla: pasar de Bet365 a la mejor cuota vale "
              f"{100 * r['d_max']:+.2f} puntos "
              f"[{100 * r['inf_max']:+.2f}, {100 * r['sup_max']:+.2f}], "
              f"o {r['d_max'] / r['ee_max']:.0f} errores típicos\n", flush=True)

    # El veredicto de RPS, al lado, para poder invocar la regla 4 con las dos cifras
    # delante en vez de elegir en silencio.
    for obj in ("acierto", "roi") if rps else ():
        g = estado[f"ganador_{obj}"]
        r = sl.confirma(g["conjunto"], feat=feat, hiper=g["hiper"], verbose=False)
        rps_fuera[obj] = r
        tab.loc[tab["quien"] == f"ganador_{obj}", "rps_cuesta"] = r["cuesta"]
        tab.loc[tab["quien"] == f"ganador_{obj}", "rps_inf"] = r["inf"]
        tab.loc[tab["quien"] == f"ganador_{obj}", "rps_sup"] = r["sup"]
        tab.loc[tab["quien"] == f"ganador_{obj}", "rps_confirmado"] = r["confirmado"]
        if verbose:
            print(f"\nRPS · ganador por {obj}: "
                  f"{'CONFIRMA' if r['confirmado'] else 'RECHAZA'} · cuesta "
                  f"{r['cuesta']:+.6f} [{r['inf']:+.6f}, {r['sup']:+.6f}] "
                  f"sobre {r['n']} partidos", flush=True)

    tab.to_csv(FUERA, index=False, encoding="utf-8")
    if verbose:
        print(f"\n{FUERA}")
    return tab


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if "--fase3" in argv:
        # `--sinrps` salta el bootstrap de `confirma()`, que es el 90 % del tiempo y
        # cuyo veredicto ya está escrito en `maximo_fuera.csv` de la pasada anterior.
        fase3(rps="--sinrps" not in argv)
        return 0
    if "--criba" in argv:
        criba()
        return 0
    fase1()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
