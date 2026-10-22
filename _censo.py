"""La misma búsqueda de acierto + ROI que `_maximo.py`, con el censo partido.

`_maximo.py` contesta literalmente a lo que se pidió —maximizar acierto y ROI sobre
todas las temporadas disponibles— y lo hace bien, pero su propia tabla lo delata: el
campo `temporadas` de `maximo.csv` enumera 2019/20 … 2024/25, las seis mismas sobre las
que reporta. Elige y mide en los mismos 2280 partidos. Sus dos ganadores —55.00 % de
acierto con nueve variables y +4.56 % de ROI con diez— son por tanto **incitables**: la
regla 5 del proyecto existe exactamente para esto, y el optimismo crece con el número
de configuraciones probadas, que allí fueron miles.

Este módulo cierra ese hueco de la única forma que hay: **partiendo el censo**.

- **Elige** en `seleccion`: 2019/20, 2020/21, 2021/22 — 1140 partidos.
- **Mide** en `medicion` (2022/23–2024/25, 1140) y en `confirmacion` (2017/18–2018/19,
  760, abriendo el arranque a 2016/17 sola como hace `seleccion.confirma()`). Ninguno de
  los 1900 participó en elegir nada, y los 760 son anteriores a todo, así que la regla 6
  queda en pie.

Lo que esto permite y la otra ruta no: si un conjunto bate a la semilla en acierto y en
ROI **en las dos ventanas de fuera**, su cifra es citable y es el modelo del TFM. Si
solo gana en una, o invierte el signo, es el máximo de una nube de configuraciones y
se descarta — que es lo que les pasó a las ocho rutas anteriores.

**Se selecciona por ACIERTO, y el motivo está medido.** En la mezcla de `_mezcla.py`,
elegir el peso por ROI en `seleccion` da w=0.35 y por acierto da w=0.85, y fuera gana el
segundo en las dos convenciones de liquidación: +2.51 % contra +1.38 % a la mejor cuota
entre casas (columna `roi_max`) y +1.08 % contra −0.05 % de ventaja sobre el listón a
cierre promediado (columna `vent`, la emparejada). El ROI sobre 1140 partidos tiene un
error
típico de ~3 pp y seleccionar con él es seleccionar ruido; el acierto se mueve a saltos
de 0.088 pp pero su error típico es 1.48 pp, la mitad en relación a lo que discrimina.
El haz es doble igualmente —conserva las mejores por acierto y por ROI— porque así la
tabla de fuera somete esa misma afirmación a una segunda prueba independiente: si el
ganador por ROI replica fuera mejor que el ganador por acierto, me he equivocado y se
verá aquí.

El ROI es el de Manuel: **una unidad plana al signo que el modelo señala, en todos los
partidos**. Se liquida a Bet365 (el listón declarado, regla 3) y, en la misma pasada y
gratis, a consenso y a la mejor cuota, porque la palanca del precio vale más que
cualquier conjunto medido en el proyecto (+5.23/+2.95/+5.64 pp según bloque, los tres
intervalos excluyendo el cero).

**RESULTADO (19-oct-2026): rechaza, y es la décima ruta nula.** 2314 evaluaciones, 107
minutos. Dentro de `seleccion` el ganador por acierto —siete variables, `C=0.3, L2`—
sube de 52.46 % a **54.30 %** de acierto y de +2.20 % a **+9.50 %** de ROI a la mejor
cuota, que es con diferencia el mayor salto medido en el proyecto. Fuera **se parte en
dos**: gana en `medicion` (+1.55 pp de ROI max, +0.09 pp de acierto) y pierde en
`confirmacion` (−1.82 pp, −1.05 pp), de modo que sobre los 1900 juntos queda en +0.20 pp
de ROI [−2.71, +3.11] y **−0.37 pp de acierto**. El signo no repite: es el máximo de una
nube. El ganador por ROI, nueve variables, es peor todavía (−0.31 pp y −0.53 pp).

Y hay un daño que sí es significativo: **el RPS empeora en las tres ventanas con los
tres intervalos excluyendo el cero** (+0.001500 en `medicion`, +0.002828 en
`confirmacion`, +0.002031 en los 1900). Comparado con la mezcla de `_mezcla.py`, que
pierde 0.000527 de RPS sin significación y cuyo ROI **sí** repite signo en las dos
ventanas (+1.14 y +1.53 pp, +1.30 pp juntas), el conjunto de siete es estrictamente
peor en las dos cosas a la vez.

La pista estaba en su propia estructura y se puede volver a usar como detector: de las
tres columnas de precio retiene **solo `mkt_p_away`**. Tirar dos tercios de la
información del mercado y aun así subir el acierto dentro del bloque solo puede pasar
ajustando ruido.

Uso:
    python _censo.py            # la búsqueda en `seleccion`
    python _censo.py --fuera    # juzga a los ganadores en los 1900 de fuera
"""
from __future__ import annotations

import os

# Antes de numpy, por el mismo motivo medido que en `_maximo.py`: los hijos ajustan
# matrices pequeñas y el BLAS multihilo se estorba a sí mismo.
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
for _sub in (".", "scripts/models", "scripts/features"):
    if str(_RAIZ / _sub) not in sys.path:
        sys.path.insert(0, str(_RAIZ / _sub))

# Planos, como los importa `seleccion.py` entre ellos. Por el paquete serían otros
# objetos de módulo con otra caché, y la guarda de `seleccion._lote` —que compara
# `feat is lb._feat_cache` para no mandarle a un hijo una matriz que no leería— caería
# a la serie sin avisar. Es lo que explica que la pasada de este censo costase 107 min.
import laboratorio as lb                                    # noqa: E402
import protocol as pt                                       # noqa: E402
import seleccion as sl                                      # noqa: E402

ELIGE = "seleccion"
FAMILIA = "logistica"
CASA = "b365"

ANCHURA = 3
MAX_K = 10
PACIENCIA = 3
SUELO = 15
TERCIO = 3

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

CONFIRMACION = ("2017/18", "2018/19")

# Las rutas las declara `seleccion.py`, junto a las del resto de las rutas, y aquí solo
# se les pone nombre corto. El mismo literal escrito en dos ficheros es exactamente como
# se descuelga el cuaderno de lo que el guion dejó en disco.
OUT = sl.OUT
CURVA = sl.CENSO_CURVA
DETALLE = sl.CENSO_DETALLE
GANADORES = sl.CENSO_GANADORES
FUERA = sl.CENSO_FUERA

_OTRAS: pd.DataFrame | None = None


def cfg() -> pt.ProtocolConfig:
    return pt.ProtocolConfig(regime="semanal", **VENTANA)


def _etiq(h: dict) -> str:
    return f"C={h['C']}/l1={h['l1_ratio']}"


def _otras(feat: pd.DataFrame) -> pd.DataFrame:
    global _OTRAS
    if _OTRAS is None:
        cols = [c for v in FAMILIAS_CUOTA.values() for c in v]
        _OTRAS = feat[["match_key"] + cols].drop_duplicates("match_key")
    return _OTRAS


def _plano(ok: np.ndarray, q: np.ndarray) -> np.ndarray:
    """Beneficio de una unidad plana: el acierto paga `cuota - 1`, el fallo pierde 1."""
    return np.where(ok, q - 1.0, -1.0)


def _liquida(p: pd.DataFrame) -> dict:
    """Acierto, ROI plano a tres precios y el listón, partido a partido."""
    fila = np.arange(len(p))
    y = p["y_num"].to_numpy(dtype=int)
    P = p[PROB].to_numpy(dtype=float)
    sel = P.argmax(axis=1)
    ok = sel == y
    Q = p[CUOTA].to_numpy(dtype=float)
    fav = Q.argmin(axis=1)
    okr = fav == y
    out = {"ok": ok.astype(float), "ben": _plano(ok, Q[fila, sel]),
           "ok_ref": okr.astype(float), "ben_ref": _plano(okr, Q[fila, fav]),
           "cuota": Q[fila, sel]}
    for nom, cols in FAMILIAS_CUOTA.items():
        Qo = p[cols].to_numpy(dtype=float)
        out[f"ben_{nom}"] = _plano(ok, Qo[fila, sel])
    return out


def _cifras(e, pedido: tuple, hiper: dict) -> dict:
    p = e.pred.merge(_otras(lb.datos()), on="match_key", how="left")
    q = _liquida(p)
    d = q["ben"] - q["ben_ref"]
    temp = {}
    for s, g in p.assign(_b=q["ben"], _ok=q["ok"]).groupby(
            p["season"].astype(str)):
        temp[s] = {"n": int(len(g)), "acierto": float(g["_ok"].mean()),
                   "roi": float(g["_b"].mean())}
    return {"pedido": pedido,
            "efectivo": tuple(sorted(set(pedido) - set(e.ausentes))),
            "hiper": hiper, "rps": float(e.rps),
            "acierto": float(q["ok"].mean()), "roi": float(q["ben"].mean()),
            "roi_avg": float(q["ben_avg"].mean()),
            "roi_max": float(q["ben_max"].mean()),
            "acierto_ref": float(q["ok_ref"].mean()),
            "roi_ref": float(q["ben_ref"].mean()), "vent_roi": float(d.mean()),
            "ee_vent": float(d.std(ddof=1) / np.sqrt(len(d))),
            "cuota_media": float(q["cuota"].mean()), "n": int(len(p)),
            "temporadas": temp}


def _mide_uno(pedido: tuple, hiper: dict, c) -> dict | str:
    """Un conjunto, un protocolo completo, sobre los 1140 de `seleccion`."""
    try:
        e = lb.evalua(list(pedido), FAMILIA, hiper=hiper, cfg=c,
                      feat=lb.datos(), evaluar=ELIGE, cuotas=CASA,
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
    """Cada conjunto con las dos penalizaciones; gana la mejor por suma de rangos.

    Dejar la penalización clavada para todo el recorrido fue uno de los dos defectos
    de procedimiento que documenta el CLAUDE.md: `_bloques.py` midió que la rejilla le
    recupera 0.005663 de RPS al conjunto más grande y 0.000006 a la semilla, es decir
    que clavarla sesga la comparación a favor del conjunto pequeño.
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

    Una candidata colineal con otra ya presente la descarta el preprocesado, y el
    conjunto resultante es su padre con otro nombre: dejarlo competir hacía que un paso
    presumiera de mejorar al anterior siendo idéntico.
    """
    mejor: dict[tuple, dict] = {}
    for d in res:
        if len(d["efectivo"]) != k:
            continue
        ef = d["efectivo"]
        if ef not in mejor or d["roi"] > mejor[ef]["roi"]:
            mejor[ef] = d
    return list(mejor.values())


def _haz_doble(cands: list[dict]) -> tuple[list[tuple], dict]:
    por_ac = sorted(cands, key=lambda d: -d["acierto"])[:ANCHURA]
    por_roi = sorted(cands, key=lambda d: -d["roi"])[:ANCHURA]
    haz, vistos = [], set()
    for d in por_ac + por_roi:
        if d["efectivo"] not in vistos:
            vistos.add(d["efectivo"])
            haz.append(d["efectivo"])
    return haz, {"acierto": por_ac[0], "roi": por_roi[0]}


def _rangos(cands: list[dict], pool: list[str]) -> dict[str, float]:
    """Suma de rangos: a cada variable se le atribuye su MEJOR conjunto, por métrica."""
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


# ===========================================================================
# 1. La búsqueda, solo en `seleccion`
# ===========================================================================
def busca(verbose: bool = True) -> dict:
    c = cfg()
    feat = lb.datos()
    pool = sl.candidatas(feat)
    banco = len(pool)
    OUT.mkdir(parents=True, exist_ok=True)

    sem = _tanda([tuple(sl.SEMILLA_MERCADO)], c)[0]
    if verbose:
        print(f"=== elige en {ELIGE}: {sem['n']} partidos, "
              f"{banco} candidatas ===\n")
        print(f"semilla de mercado ({_etiq(sem['hiper'])}): acierto "
              f"{100 * sem['acierto']:.2f}% · ROI b365 {100 * sem['roi']:+.3f}% · "
              f"ROI max {100 * sem['roi_max']:+.3f}% · RPS {sem['rps']:.6f}")
        print(f"listón b365 (favorito, plana): acierto "
              f"{100 * sem['acierto_ref']:.2f}% · ROI "
              f"{100 * sem['roi_ref']:+.3f}%\n", flush=True)

    filas: list[dict] = []
    detalle: list[dict] = []
    marcas: dict[str, int] = {}
    gastadas = 1
    t0 = time.time()
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
        cands = _absorbe(_tanda(conj, c), k)
        if not cands:
            if verbose:
                print(f"k={k}: ningún conjunto conserva {k} columnas; se para")
            break

        haz, top = _haz_doble(cands)
        en_haz = set(haz)
        for d in sorted(cands, key=lambda x: -x["roi"]):
            detalle.append({"k": k, "pedido": " ".join(d["pedido"]),
                            "efectivo": " ".join(d["efectivo"]),
                            "en_haz": int(d["efectivo"] in en_haz),
                            "hiper": _etiq(d["hiper"]), "rps": d["rps"],
                            "acierto": d["acierto"], "roi": d["roi"],
                            "roi_avg": d["roi_avg"], "roi_max": d["roi_max"]})
        for obj in ("acierto", "roi"):
            d = top[obj]
            filas.append({"k": k, "objetivo": obj,
                          "conjunto": " ".join(d["efectivo"]),
                          "hiper": _etiq(d["hiper"]), "rps": d["rps"],
                          "acierto": d["acierto"], "roi": d["roi"],
                          "roi_avg": d["roi_avg"], "roi_max": d["roi_max"],
                          "acierto_ref": d["acierto_ref"],
                          "roi_ref": d["roi_ref"], "vent_roi": d["vent_roi"],
                          "ee_vent": d["ee_vent"],
                          "cuota_media": d["cuota_media"], "n": d["n"],
                          "temporadas": json.dumps(d["temporadas"])})
            if d[obj] > lideres[obj][obj]:
                lideres[obj] = d

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
    estado = {"elige": ELIGE, "casa": CASA, "n_banco": banco,
              "evaluaciones": gastadas * len(REJILLA),
              "minutos": (time.time() - t0) / 60,
              "semilla": {k: sem[k] for k in
                          ("rps", "acierto", "roi", "roi_avg", "roi_max",
                           "acierto_ref", "roi_ref", "n")}}
    for obj in ("acierto", "roi"):
        d = lideres[obj]
        estado[f"ganador_{obj}"] = {
            "conjunto": list(d["efectivo"]), "hiper": d["hiper"],
            "acierto": d["acierto"], "roi": d["roi"],
            "roi_max": d["roi_max"], "rps": d["rps"]}
    GANADORES.write_text(json.dumps(estado, indent=2, ensure_ascii=False),
                         encoding="utf-8")

    if verbose:
        print(f"\n{'=' * 74}")
        print(f"{estado['evaluaciones']} evaluaciones · "
              f"{estado['minutos']:.0f} min · elegido sobre {sem['n']} partidos")
        print(f"{'=' * 74}\n")
        print(f"{'quien':<22} {'acierto':>8} {'ROI b365':>9} {'ROI max':>9} "
              f"{'RPS':>9}")
        print(f"{'liston b365 (favorito)':<22} {100 * sem['acierto_ref']:>7.2f}% "
              f"{100 * sem['roi_ref']:>+8.2f}% {'':>9} {'':>9}")
        for nom, d in (("semilla de mercado", sem),
                       ("ganador por acierto", lideres["acierto"]),
                       ("ganador por roi", lideres["roi"])):
            print(f"{nom:<22} {100 * d['acierto']:>7.2f}% "
                  f"{100 * d['roi']:>+8.2f}% {100 * d['roi_max']:>+8.2f}% "
                  f"{d['rps']:>9.6f}")
        for obj in ("acierto", "roi"):
            d = lideres[obj]
            print(f"\nganador por {obj} ({len(d['efectivo'])} var, "
                  f"{_etiq(d['hiper'])}):")
            print(f"  {' '.join(d['efectivo'])}")
        print("\nNINGUNA de estas cifras es citable (regla 5): se eligieron aqui.")
        print("La lectura que decide es `python _censo.py --fuera`.\n")
        print(f"{CURVA}\n{DETALLE}\n{GANADORES}")
    return estado


# ===========================================================================
# 2. La lectura de fuera: 1900 partidos que no eligieron nada
# ===========================================================================
def _pred(cols: list[str], hiper: dict, feat: pd.DataFrame,
          bloque: str) -> pd.DataFrame:
    """Liquidación partido a partido en un bloque que no participó en elegir.

    `confirmacion` no es un bloque del protocolo: se obtiene bajando el arranque a
    2016/17 sola —igual que `seleccion.confirma()`— para que 2017/18 y 2018/19 pasen a
    ser evaluables, sin tocar nada más. El holdout no se abre: `lb.evalua` lanza
    `PermissionError` para `holdout` y `produccion`, y está bien que lo haga.
    """
    if bloque == "confirmacion":
        c = pt.ProtocolConfig(**{**cfg().__dict__,
                                 "warmup_seasons": list(sl.WARMUP_CONFIRMACION)})
        evaluar, filtra = "validacion", CONFIRMACION
    else:
        c, evaluar, filtra = cfg(), bloque, None
    e = lb.evalua(list(cols), FAMILIA, hiper=hiper, cfg=c, feat=feat,
                  evaluar=evaluar, cuotas=CASA, n_boot=0, registrar=False,
                  verbose=False)
    p = e.pred
    if filtra is not None:
        p = p.loc[p["season"].astype(str).isin(filtra)]
    p = p.merge(_otras(feat), on="match_key", how="left")
    q = _liquida(p)
    q["rps"] = _rps(p[PROB].to_numpy(dtype=float),
                    p["y_num"].to_numpy(dtype=int))
    return pd.DataFrame(q, index=p["match_key"].to_numpy())


def _rps(P: np.ndarray, y: np.ndarray) -> np.ndarray:
    acum = np.cumsum(P, axis=1)[:, :2]
    obs = np.zeros_like(P)
    obs[np.arange(len(y)), y] = 1.0
    obs = np.cumsum(obs, axis=1)[:, :2]
    return ((acum - obs) ** 2).sum(axis=1) / 2.0


def _par(a: np.ndarray, b: np.ndarray) -> tuple[float, float, float, float]:
    """Diferencia emparejada y su error típico: media, ee, inferior, superior."""
    d = a - b
    m = float(d.mean())
    ee = float(d.std(ddof=1) / np.sqrt(len(d))) if len(d) > 1 else 0.0
    return m, ee, m - 1.96 * ee, m + 1.96 * ee


def fuera(verbose: bool = True) -> pd.DataFrame:
    """Qué hacen los ganadores en los 1900 partidos que no los eligieron.

    Tres lecturas, no una: `medicion` (1140, posterior), `confirmacion` (760, anterior)
    y las dos juntas (1900), que es la que tiene el error típico más pequeño y por tanto
    la que puede excluir el cero. Todo emparejado contra la semilla y contra el listón
    sobre el mismo censo, porque promediar niveles medidos en censos distintos mezcla la
    diferencia entre modelos con la diferencia entre partidos.

    El criterio es el que ya se aplicó a las ocho rutas anteriores: **el signo tiene que
    repetirse en las dos ventanas**. Un conjunto que gana en `medicion` y pierde en
    `confirmacion` es el máximo de una nube, no una señal.
    """
    estado = json.loads(GANADORES.read_text(encoding="utf-8"))
    feat = lb.datos()
    cuales = [("semilla", list(sl.SEMILLA_MERCADO), REJILLA[0])]
    for obj in ("acierto", "roi"):
        g = estado[f"ganador_{obj}"]
        cuales.append((f"ganador_{obj}", list(g["conjunto"]), g["hiper"]))

    liq: dict[str, dict[str, pd.DataFrame]] = {}
    for bloque in ("medicion", "confirmacion"):
        liq[bloque] = {}
        for nom, cols, hiper in cuales:
            if verbose:
                print(f"midiendo {nom} ({len(cols)} var, {_etiq(hiper)}) en "
                      f"{bloque}...", flush=True)
            liq[bloque][nom] = _pred(cols, hiper, feat, bloque)
        censo = None
        for v in liq[bloque].values():
            censo = v.index if censo is None else censo.intersection(v.index)
        liq[bloque] = {k: v.loc[censo] for k, v in liq[bloque].items()}

    liq["ambos"] = {nom: pd.concat([liq["medicion"][nom],
                                    liq["confirmacion"][nom]])
                    for nom, _, _ in cuales}

    fil = []
    for bloque in ("medicion", "confirmacion", "ambos"):
        base = liq[bloque]["semilla"]
        for nom, _, _ in cuales:
            d = liq[bloque][nom]
            f = {"bloque": bloque, "quien": nom, "n": int(len(d)),
                 "acierto": float(d["ok"].mean()), "roi": float(d["ben"].mean()),
                 "roi_avg": float(d["ben_avg"].mean()),
                 "roi_max": float(d["ben_max"].mean()),
                 "rps": float(d["rps"].mean()),
                 "acierto_ref": float(base["ok_ref"].mean()),
                 "roi_ref": float(base["ben_ref"].mean())}
            rivales = (("liston", d["ok_ref"].to_numpy(), d["ben_ref"].to_numpy(),
                        None),
                       ("semilla", base["ok"].to_numpy(), base["ben"].to_numpy(),
                        base["rps"].to_numpy()))
            for suf, oa, ob, orp in rivales:
                for pref, mio, suyo in (("dac", d["ok"].to_numpy(), oa),
                                        ("droi", d["ben"].to_numpy(), ob)):
                    m, ee, i, s = _par(mio, suyo)
                    f |= {f"{pref}_{suf}": m, f"ee_{pref}_{suf}": ee,
                          f"inf_{pref}_{suf}": i, f"sup_{pref}_{suf}": s}
                if orp is not None:
                    m, ee, i, s = _par(d["rps"].to_numpy(), orp)
                    f |= {"drps": m, "ee_drps": ee, "inf_drps": i, "sup_drps": s}
            # La misma apuesta a la mejor cuota, emparejada contra la semilla a la
            # mejor cuota: es la comparación que de verdad decide cuál se publica,
            # porque la palanca del precio es mucho mayor que la del conjunto.
            m, ee, i, s = _par(d["ben_max"].to_numpy(),
                               base["ben_max"].to_numpy())
            f |= {"droimax_semilla": m, "ee_droimax_semilla": ee,
                  "inf_droimax_semilla": i, "sup_droimax_semilla": s}
            fil.append(f)
    tab = pd.DataFrame(fil)
    tab.to_csv(FUERA, index=False, encoding="utf-8")
    if verbose:
        _informe(tab, estado)
        print(f"\n{FUERA}")
    return tab


def _informe(tab: pd.DataFrame, estado: dict) -> None:
    print(f"\n{'=' * 78}")
    print("fuera de la eleccion: ni medicion ni confirmacion eligieron nada")
    print(f"{'=' * 78}")
    for obj in ("acierto", "roi"):
        g = estado[f"ganador_{obj}"]
        print(f"\nganador por {obj} ({len(g['conjunto'])} var, {g['hiper']}), "
              f"dentro: acierto {100 * g['acierto']:.2f}% · "
              f"ROI b365 {100 * g['roi']:+.2f}% · ROI max "
              f"{100 * g['roi_max']:+.2f}%")
        print(f"  {' '.join(g['conjunto'])}")

    for bloque in ("medicion", "confirmacion", "ambos"):
        sub = tab.loc[tab["bloque"] == bloque]
        n = int(sub["n"].iloc[0])
        print(f"\n--- {bloque}, n={n} ---")
        print(f"{'quien':<16} {'acierto':>8} {'b365':>8} {'avg':>8} {'max':>8} "
              f"{'RPS':>9}")
        r0 = sub.iloc[0]
        print(f"{'liston b365':<16} {100 * r0['acierto_ref']:>7.2f}% "
              f"{100 * r0['roi_ref']:>+7.2f}% {'':>8} {'':>8} {'':>9}")
        for _, r in sub.iterrows():
            print(f"{r['quien']:<16} {100 * r['acierto']:>7.2f}% "
                  f"{100 * r['roi']:>+7.2f}% {100 * r['roi_avg']:>+7.2f}% "
                  f"{100 * r['roi_max']:>+7.2f}% {r['rps']:>9.6f}")
        for _, r in sub.iloc[1:].iterrows():
            print(f"  {r['quien']} contra la semilla, emparejado:")
            print(f"    acierto  {100 * r['dac_semilla']:+.2f} pp "
                  f"[{100 * r['inf_dac_semilla']:+.2f}, "
                  f"{100 * r['sup_dac_semilla']:+.2f}]")
            print(f"    ROI b365 {100 * r['droi_semilla']:+.2f} pp "
                  f"[{100 * r['inf_droi_semilla']:+.2f}, "
                  f"{100 * r['sup_droi_semilla']:+.2f}]")
            print(f"    ROI max  {100 * r['droimax_semilla']:+.2f} pp "
                  f"[{100 * r['inf_droimax_semilla']:+.2f}, "
                  f"{100 * r['sup_droimax_semilla']:+.2f}]")
            print(f"    RPS      {r['drps']:+.6f} "
                  f"[{r['inf_drps']:+.6f}, {r['sup_drps']:+.6f}]")

    print(f"\n{'=' * 78}")
    print("veredicto: el signo tiene que repetirse en medicion Y en confirmacion")
    print(f"{'=' * 78}")
    med = tab.loc[tab["bloque"] == "medicion"].set_index("quien")
    con = tab.loc[tab["bloque"] == "confirmacion"].set_index("quien")
    amb = tab.loc[tab["bloque"] == "ambos"].set_index("quien")
    for nom in ("ganador_acierto", "ganador_roi"):
        marcas = []
        for col, etiq in (("dac_semilla", "acierto"),
                          ("droimax_semilla", "ROI max")):
            a, b = med.loc[nom, col], con.loc[nom, col]
            rep = (a > 0) and (b > 0)
            marcas.append(f"{etiq} {'repite' if rep else 'NO repite'}")
        lo = amb.loc[nom, "inf_droimax_semilla"]
        hi = amb.loc[nom, "sup_droimax_semilla"]
        excl = "excluye el cero" if (lo > 0 or hi < 0) else "no excluye el cero"
        print(f"{nom:<16} {' · '.join(marcas)} · ROI max de los 1900 "
              f"{100 * amb.loc[nom, 'droimax_semilla']:+.2f} pp, {excl}")


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if "--fuera" in argv:
        fuera()
        return 0
    busca()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
