"""Mezclar precio y juego con UN peso, en vez de concatenar sesenta columnas.

Siete rutas de búsqueda han dicho lo mismo: añadir variables de juego a la semilla de
precio no mejora nada que sobreviva fuera del bloque donde se eligió. Pero el
diagnóstico de POR QUÉ falla está medido, y no dice que las variables no sepan nada:

  · las 59 de juego, sin ver una sola columna de precio, dan RPS 0.202853 contra
    0.223518 de la previa de frecuencias y 0.193174 de la semilla: recorren el 68.1 %
    del hueco (`_sinprecio.py`). Saben mucho.
  · y el dominio `fuerza` SOLO —nueve columnas— da 0.198001 y **bate a las 59 juntas**.

Esas dos cifras juntas dicen que el problema es la varianza del ajuste, no la
información: cincuenta y nueve parámetros sobre 1140 partidos cuestan más de lo que
traen, y nueve cuestan menos. De ahí la idea de este módulo: si el problema es el
número de parámetros, **usar un solo parámetro**.

La mezcla es un `log-opinion pool`, que es la generalización multiclase de combinar en
escala logit:

    p ∝ p_mercado^w · p_juego^(1-w)

y se renormaliza. Con `w = 1` es exactamente la semilla publicada; con `w = 0`, el
modelo de juego desnudo. **Un escalar entre los dos.** La diferencia con todo lo
anterior no es cosmética: la voraz elegía entre 2^65 conjuntos, y esto elige entre 21
valores de una rejilla. El optimismo de selección escala con el número de
configuraciones probadas —es lo que invirtió el signo de `_maximo.py` siete veces— y
aquí es, por construcción, casi nulo.

**El peso se elige en el bloque de selección y se reporta en los otros dos.** 1140
partidos eligen; 1140 de medición y 760 de confirmación puntúan sin haber participado.
Eso es la regla 6 y además es la lección de esta sesión metida en el diseño del
experimento en vez de aplicada después: en `_maximo.py` busqué dentro y comprobé fuera,
y la comprobación invirtió el signo. Aquí las tres columnas salen en la misma tabla.

Se mezclan dos modelos de juego, no uno, porque la comparación entre ellos es la que
distingue las dos explicaciones posibles de un resultado nulo:

  `juego`   las 59, el que más información tiene y más varianza paga
  `fuerza`  las 9 de Elo y valor de plantilla, el que menos paga y aun así gana

Si la mezcla con `fuerza` funciona y la de `juego` no, la causa era la varianza. Si no
funciona ninguna, la información de juego es redundante con el precio de verdad y no
es un problema de forma funcional — y entonces el trabajo queda cerrado con ocho rutas.

El ROI es el plano de Manuel —una unidad al signo señalado, todos los partidos— a las
tres familias de cuota, y el listón de Bet365 va emparejado en cada fila (regla 3).

Uso:
    python _mezcla.py
"""
from __future__ import annotations

import os

# Antes de numpy, por el mismo motivo que en `_maximo.py`: BLAS multihilo se estorba.
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

FAMILIA = "logistica"
CASA = "b365"

# Las tres ventanas de temporadas. `confirmacion` y `seleccion` son anteriores a
# `medicion`, así que elegir en `seleccion` no mira hacia adelante (regla 6).
BLOQUES = {"confirmacion": ("2017/18", "2018/19"),
           "seleccion": ("2019/20", "2020/21", "2021/22"),
           "medicion": ("2022/23", "2023/24", "2024/25")}
ELIGE_EN = "seleccion"

PESOS = np.round(np.arange(0.0, 1.0001, 0.05), 3)

PROB = ["p_home", "p_draw", "p_away"]
CUOTA = ["cuota_home", "cuota_draw", "cuota_away"]
FAMILIAS_CUOTA = {"avg": ["odds_avg_h", "odds_avg_d", "odds_avg_a"],
                  "max": ["odds_max_h", "odds_max_d", "odds_max_a"]}

OUT = _RAIZ / "outputs" / "models" / "seleccion"
CURVA = OUT / "mezcla.csv"
RESUMEN = OUT / "mezcla_resumen.json"
MATRIZ = OUT / "mezcla_pred.parquet"
PAR = OUT / "mezcla_par.csv"

# Pesos a los que se mide la diferencia emparejada contra la semilla. Salen de la
# lectura de la curva: `seleccion` elige w=1.00 por RPS, w=0.85 por acierto y w=0.35
# por ROI, y en `medicion` el acierto y el ROI tienen su máximo en la franja 0.70-0.85.
PESOS_PAR = (0.35, 0.50, 0.70, 0.80, 0.85, 0.90)

_EPS = 1e-12


def cfg_abierto() -> pt.ProtocolConfig:
    """El protocolo con el arranque bajado a 2016/17, para que 17/18 y 18/19 puntúen.

    Es la misma apertura que usa `seleccion.confirma()`; el resto del protocolo
    —ventana expansiva, ajuste solo con pasado— no se toca.
    """
    base = pt.ProtocolConfig(regime="semanal", window="expansiva",
                             sliding_seasons=pt.SLIDING_SEASONS, halflife_days=None)
    return pt.ProtocolConfig(**{**base.__dict__,
                                "warmup_seasons": list(sl.WARMUP_CONFIRMACION)})


def _plano(ok: np.ndarray, q: np.ndarray) -> np.ndarray:
    return np.where(ok, q - 1.0, -1.0)


def _rps(P: np.ndarray, y: np.ndarray) -> float:
    """RPS multiclase ordenado (local < empate < visitante), como `evaluate`."""
    acum = np.cumsum(P, axis=1)[:, :2]
    obs = np.zeros_like(P)
    obs[np.arange(len(y)), y] = 1.0
    obs = np.cumsum(obs, axis=1)[:, :2]
    return float(((acum - obs) ** 2).sum(axis=1).mean() / 2.0)


def _mezcla(Pm: np.ndarray, Pj: np.ndarray, w: float) -> np.ndarray:
    """`p ∝ p_mercado^w · p_juego^(1-w)`, renormalizado. Un solo parámetro."""
    lg = w * np.log(np.clip(Pm, _EPS, 1.0)) + (1.0 - w) * np.log(
        np.clip(Pj, _EPS, 1.0))
    lg -= lg.max(axis=1, keepdims=True)
    P = np.exp(lg)
    return P / P.sum(axis=1, keepdims=True)


def _cifras(P: np.ndarray, d: pd.DataFrame) -> dict:
    """Todas las métricas de un conjunto de probabilidades sobre unos partidos."""
    y = d["y_num"].to_numpy(dtype=int)
    fila = np.arange(len(d))
    sel = P.argmax(axis=1)
    ok = sel == y
    Q = d[CUOTA].to_numpy(dtype=float)
    ben = _plano(ok, Q[fila, sel])
    fav = Q.argmin(axis=1)
    benr = _plano(fav == y, Q[fila, fav])
    dif = ben - benr
    out = {"rps": _rps(P, y), "acierto": float(ok.mean()),
           "roi": float(ben.mean()), "acierto_ref": float((fav == y).mean()),
           "roi_ref": float(benr.mean()), "vent": float(dif.mean()),
           "ee_vent": float(dif.std(ddof=1) / np.sqrt(len(dif))),
           "cuota_media": float(Q[fila, sel].mean()), "n": int(len(d))}
    for nom, cols in FAMILIAS_CUOTA.items():
        Qo = d[cols].to_numpy(dtype=float)
        out[f"roi_{nom}"] = float(_plano(ok, Qo[fila, sel]).mean())
    return out


def _liquida(P: np.ndarray, d: pd.DataFrame) -> dict:
    """Las tres métricas partido a partido, para poder emparejarlas y medir el ee."""
    y = d["y_num"].to_numpy(dtype=int)
    fila = np.arange(len(d))
    sel = P.argmax(axis=1)
    ok = sel == y
    Q = d[CUOTA].to_numpy(dtype=float)
    return {"ok": ok.astype(float), "ben": _plano(ok, Q[fila, sel]),
            "rps": _rps_vector(P, y)}


def _rps_vector(P: np.ndarray, y: np.ndarray) -> np.ndarray:
    acum = np.cumsum(P, axis=1)[:, :2]
    obs = np.zeros_like(P)
    obs[np.arange(len(y)), y] = 1.0
    obs = np.cumsum(obs, axis=1)[:, :2]
    return ((acum - obs) ** 2).sum(axis=1) / 2.0


def _dif(a: np.ndarray, b: np.ndarray) -> tuple[float, float, float, float]:
    """Diferencia emparejada y su error típico: media, ee, inferior, superior."""
    d = a - b
    m = float(d.mean())
    ee = float(d.std(ddof=1) / np.sqrt(len(d))) if len(d) > 1 else 0.0
    return m, ee, m - 1.96 * ee, m + 1.96 * ee


def paridad(verbose: bool = True) -> pd.DataFrame:
    """Diferencia emparejada de la mezcla contra la semilla, en los tres bloques.

    La curva de `mezcla.csv` compara niveles, y un nivel no dice si la diferencia
    cabe en el ruido. Aquí se liquida partido a partido y se empareja: el mismo
    partido entra en los dos brazos, así que la varianza del partido se cancela y
    el error típico mide solo lo que separa a los dos modelos. Es la prueba que
    faltaba, y la que convierte una lectura de la curva en un veredicto.

    El peso NO se elige aquí. Se miden los seis de `PESOS_PAR`, que salen de leer
    la curva, y se reporta cada uno en los tres bloques. Lo que decide es que el
    signo se repita fuera de `seleccion`: en las ocho rutas anteriores se invertía.
    """
    d = pd.read_parquet(MATRIZ)
    Pm = d[[f"{c}_mercado" for c in PROB]].to_numpy(dtype=float)
    idx = {b: d["season"].isin(ss).to_numpy() for b, ss in BLOQUES.items()}
    filas = []
    for rival in ("juego", "fuerza"):
        Pj = d[[f"{c}_{rival}" for c in PROB]].to_numpy(dtype=float)
        for w in PESOS_PAR:
            P = _mezcla(Pm, Pj, float(w))
            for b, m in idx.items():
                sub = d.loc[m]
                mez, sem = _liquida(P[m], sub), _liquida(Pm[m], sub)
                fila = {"rival": rival, "w": float(w), "bloque": b,
                        "n": int(m.sum())}
                for k in ("ok", "ben", "rps"):
                    mm, ee, lo, hi = _dif(mez[k], sem[k])
                    fila |= {f"d_{k}": mm, f"ee_{k}": ee,
                             f"lo_{k}": lo, f"hi_{k}": hi}
                filas.append(fila)
    tab = pd.DataFrame(filas)
    tab.to_csv(PAR, index=False, encoding="utf-8")
    if verbose:
        _informe_par(tab)
    return tab


def _informe_par(tab: pd.DataFrame) -> None:
    for rival in ("juego", "fuerza"):
        print("\n" + "=" * 78)
        print(f"mezcla mercado + {rival}: diferencia emparejada contra la semilla")
        print("=" * 78)
        print("signo + = la mezcla gana. intervalo al 95 % entre corchetes.\n")
        for w in PESOS_PAR:
            sub = tab[(tab["rival"] == rival) & (tab["w"] == w)]
            print(f"w={w:.2f}")
            for b in ("seleccion", "medicion", "confirmacion"):
                r = sub[sub["bloque"] == b].iloc[0]
                print(f"  {b:12s} n={int(r['n']):4d}  "
                      f"acierto {100 * r['d_ok']:+5.2f} pp "
                      f"[{100 * r['lo_ok']:+5.2f},{100 * r['hi_ok']:+5.2f}]  "
                      f"ROI {100 * r['d_ben']:+6.2f} pp "
                      f"[{100 * r['lo_ben']:+6.2f},{100 * r['hi_ben']:+6.2f}]  "
                      f"RPS {-r['d_rps']:+.6f} "
                      f"[{-r['hi_rps']:+.6f},{-r['lo_rps']:+.6f}]")
            fuera = [sub[sub["bloque"] == b].iloc[0] for b in
                     ("medicion", "confirmacion")]
            ok = all(r["d_ok"] > 0 for r in fuera)
            roi = all(r["d_ben"] > 0 for r in fuera)
            sig = any(r["lo_ok"] > 0 or r["lo_ben"] > 0 for r in fuera)
            cero = "alguna" if sig else "ninguna"
            print(f"  fuera: acierto {'repite' if ok else 'NO repite'} · "
                  f"ROI {'repite' if roi else 'NO repite'} · "
                  f"{cero} diferencia de fuera excluye el cero\n")


def _base(cols: list[str], nombre: str, feat: pd.DataFrame,
          verbose: bool = True) -> pd.DataFrame:
    """Un modelo base, evaluado una sola vez sobre las ocho temporadas abiertas."""
    t0 = time.time()
    e = lb.evalua(list(cols), FAMILIA, cfg=cfg_abierto(), feat=feat,
                  evaluar="validacion", cuotas=CASA, n_boot=0, registrar=False,
                  verbose=False)
    p = e.pred[["match_key", "season", "y_num"] + PROB + CUOTA].copy()
    p["season"] = p["season"].astype(str)
    if verbose:
        print(f"  {nombre:9s} k={len(cols):3d}  n={len(p):4d}  "
              f"ausentes={len(e.ausentes):2d}  [{time.time() - t0:.0f}s]", flush=True)
    return p.rename(columns={c: f"{c}_{nombre}" for c in PROB})


def main() -> int:
    t0 = time.time()
    feat = lb.datos()
    cands = sl.candidatas(feat, regime="semanal")
    mercado = list(lb.dominios(cands)["mercado"])
    juego = [c for c in cands if c not in set(mercado)]
    fuerza = list(lb.dominios(cands)["fuerza"])
    semilla = list(sl.SEMILLA_MERCADO)

    print(f"\ncandidatas {len(cands)} · semilla {len(semilla)} · "
          f"juego {len(juego)} · fuerza {len(fuerza)}")
    print(f"peso elegido en '{ELIGE_EN}', reportado en los tres bloques\n")

    print("modelos base (una evaluación cada uno, ocho temporadas):")
    bases = {n: _base(c, n, feat) for n, c in
             (("mercado", semilla), ("juego", juego), ("fuerza", fuerza))}

    d = bases["mercado"]
    for n in ("juego", "fuerza"):
        d = d.merge(bases[n].drop(columns=["season", "y_num"] + CUOTA),
                    on="match_key", how="inner")
    d = d.merge(feat[["match_key"] + [c for v in FAMILIAS_CUOTA.values()
                                      for c in v]].drop_duplicates("match_key"),
                on="match_key", how="left")
    d = d.loc[d[CUOTA].notna().all(axis=1)].reset_index(drop=True)
    # Se guarda la matriz de probabilidades para que la prueba emparejada de
    # `paridad()` no tenga que volver a ajustar los tres modelos base (117 s).
    d.to_parquet(MATRIZ, index=False)
    print(f"\ncenso común: {len(d)} partidos")

    Pm = d[[f"{c}_mercado" for c in PROB]].to_numpy(dtype=float)
    idx = {b: d["season"].isin(ss).to_numpy() for b, ss in BLOQUES.items()}
    for b, m in idx.items():
        print(f"  {b:12s} {int(m.sum()):4d} partidos")

    filas = []
    for rival in ("juego", "fuerza"):
        Pj = d[[f"{c}_{rival}" for c in PROB]].to_numpy(dtype=float)
        for w in PESOS:
            P = _mezcla(Pm, Pj, float(w))
            for b, m in idx.items():
                filas.append({"rival": rival, "w": float(w), "bloque": b,
                              **_cifras(P[m], d.loc[m])})
    tab = pd.DataFrame(filas)
    tab.to_csv(CURVA, index=False, encoding="utf-8")

    _informe(tab)
    print(f"\n{time.time() - t0:.0f} s\n{CURVA}\n{RESUMEN}")
    return 0


def _informe(tab: pd.DataFrame) -> None:
    resumen = {}
    for rival in ("juego", "fuerza"):
        sub = tab.loc[tab["rival"] == rival]
        eleg = sub.loc[sub["bloque"] == ELIGE_EN]
        # El peso se elige por RPS, que es la métrica principal (regla 4). Se guarda
        # también el que habría elegido el acierto y el que habría elegido el ROI, para
        # que se vea si los tres objetivos señalan al mismo sitio o no.
        w_rps = float(eleg.loc[eleg["rps"].idxmin(), "w"])
        w_ac = float(eleg.loc[eleg["acierto"].idxmax(), "w"])
        w_roi = float(eleg.loc[eleg["roi"].idxmax(), "w"])
        print(f"\n{'=' * 78}")
        print(f"mezcla mercado + {rival}")
        print(f"{'=' * 78}")
        print(f"peso elegido en '{ELIGE_EN}': por RPS w={w_rps:.2f} · "
              f"por acierto w={w_ac:.2f} · por ROI w={w_roi:.2f}")
        print(f"  (w=1 es la semilla publicada; w=0 es {rival} desnudo)\n")
        print(f"{'bloque':<13} {'n':>5} {'RPS':>9} {'acierto':>8} {'ROI b365':>9} "
              f"{'ROI max':>9} | {'vs listón':>10} {'ee':>5}")
        for b in ("seleccion", "medicion", "confirmacion"):
            for et, w in ((f"w={w_rps:.2f}", w_rps), ("w=1.00 (semilla)", 1.0)):
                r = sub.loc[(sub["bloque"] == b) & (np.isclose(sub["w"], w))].iloc[0]
                marca = "*" if b == ELIGE_EN else " "
                print(f"{b[:11]:<11}{marca} {et:<16} {int(r['n']):>4} "
                      f"{r['rps']:>8.6f} {100 * r['acierto']:>7.2f}% "
                      f"{100 * r['roi']:>+8.2f}% {100 * r['roi_max']:>+8.2f}% | "
                      f"{100 * r['vent']:>+9.2f}% {100 * r['ee_vent']:>4.2f}")
            print()
        # Lo que decide: el peso elegido fuera contra la semilla, en los dos bloques
        # que no participaron en elegirlo. Si el signo se invierte entre ellos, es la
        # misma firma que invirtió a `_maximo.py` y la respuesta es no.
        gan = {}
        for b in ("medicion", "confirmacion"):
            a = sub.loc[(sub["bloque"] == b) & (np.isclose(sub["w"], w_rps))].iloc[0]
            s = sub.loc[(sub["bloque"] == b) & (np.isclose(sub["w"], 1.0))].iloc[0]
            gan[b] = {"d_rps": float(s["rps"] - a["rps"]),
                      "d_acierto": float(a["acierto"] - s["acierto"]),
                      "d_roi": float(a["roi"] - s["roi"]), "n": int(a["n"])}
            print(f"fuera, {b}: RPS {gan[b]['d_rps']:+.6f} · "
                  f"acierto {100 * gan[b]['d_acierto']:+.2f} pp · "
                  f"ROI {100 * gan[b]['d_roi']:+.2f} pp  (signo + = la mezcla gana)")
        ok = all(g["d_rps"] > 0 for g in gan.values())
        print(f"\n  veredicto: {'MEJORA' if ok else 'NO MEJORA'} "
              f"en los dos bloques de fuera")
        resumen[rival] = {"w_rps": w_rps, "w_acierto": w_ac, "w_roi": w_roi,
                          "fuera": gan, "mejora": bool(ok)}
    RESUMEN.write_text(json.dumps(resumen, indent=2, ensure_ascii=False),
                       encoding="utf-8")


if __name__ == "__main__":
    if "--par" in sys.argv:
        paridad()
        print(f"\n{PAR}")
        raise SystemExit(0)
    raise SystemExit(main())
