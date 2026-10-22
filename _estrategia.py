"""La ventaja del modelo está medida; ¿la está tirando la regla de apuesta?

El modelo publicado **bate al mercado en probabilidades** y no en ROI, y las dos cosas
están medidas: RPS 0.1922 contra 0.1927 de Bet365, pero ROI plano −0.92 % contra
−0.90 % del favorito. Siete rutas de búsqueda han intentado cerrar ese hueco cambiando
las variables, y ninguna ha producido nada que sobreviva fuera del bloque donde se
eligió.

Este módulo pregunta por el otro extremo de la cadena. La regla de liquidación del
proyecto —una unidad al signo más probable, todos los partidos— **reduce un vector de
tres probabilidades a una sola elección**. Y resulta que esa operación descarta justo
aquello en lo que el modelo es bueno:

  · señala empate **29 veces en 2280 partidos**, pese a que su `p_draw` media es
    0.2704 contra una tasa real de 0.2711 — calibrado casi perfecto, nunca modal,
    porque su máximo en seis temporadas es 0.3708;
  · así que 609 de los 618 empates son pérdida automática, y toda la información que
    el modelo tiene sobre el empate no llega nunca a una apuesta.

Lo que se mide aquí es el espacio de estrategias a **modelo y pronóstico fijos**. No se
ajusta nada: se cambia solo la regla que convierte probabilidades en apuestas. Es la
palanca más barata que queda y no la ha tocado ninguna ruta anterior.

**El control que hace honesta la comparación.** Una estrategia de valor apuesta en un
subconjunto que ella misma elige, y un subconjunto favorable puede dar ROI positivo sin
que el mérito sea del modelo. Así que cada fila lleva, emparejado y sobre **los mismos
partidos que esa estrategia apostó**, el ROI del favorito de Bet365 (regla 3). Si el
subconjunto era fácil, las dos columnas suben juntas y la diferencia no se mueve: eso
es lo que distingue «el modelo encuentra valor» de «ahí el favorito iba bien».

Esto le faltaba a `_dondecobras.py`, cuyas filas de valor daban +3.60 % en Bet365 sin
listón emparejado ni lectura de fuera. También le faltaba el error típico: con 179
apuestas el ee del ROI ronda los 6 puntos, así que un +3.60 % sin su barra no es un
resultado. Aquí van las dos cosas.

**El umbral se elige en el bloque de selección y se reporta en los otros dos**, igual
que en `_mezcla.py`: 1140 partidos eligen, 1140 de medición y 760 de confirmación
puntúan sin haber participado. En `_maximo.py` busqué dentro y comprobé fuera, y la
comprobación invirtió el signo siete veces; aquí las tres columnas salen juntas.

Uso:
    python _estrategia.py
"""
from __future__ import annotations

import os

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

BLOQUES = {"confirmacion": ("2017/18", "2018/19"),
           "seleccion": ("2019/20", "2020/21", "2021/22"),
           "medicion": ("2022/23", "2023/24", "2024/25")}
ELIGE_EN = "seleccion"

PROB = ["p_home", "p_draw", "p_away"]
CUOTA = ["cuota_home", "cuota_draw", "cuota_away"]
FAMILIAS = {"b365": CUOTA,
            "avg": ["odds_avg_h", "odds_avg_d", "odds_avg_a"],
            "max": ["odds_max_h", "odds_max_d", "odds_max_a"]}

# Umbral de valor esperado: se apuesta donde `p * cuota - 1 > umbral`. El cero es
# valor esperado positivo a secas; los demás exigen margen, que es lo que protege de
# que un error de calibración del 2 % se lea como una oportunidad.
UMBRALES = (0.00, 0.02, 0.05, 0.10)

OUT = _RAIZ / "outputs" / "models" / "seleccion"
CURVA = OUT / "estrategia.csv"
RESUMEN = OUT / "estrategia_resumen.json"


def cfg_abierto() -> pt.ProtocolConfig:
    base = pt.ProtocolConfig(regime="semanal", window="expansiva",
                             sliding_seasons=pt.SLIDING_SEASONS, halflife_days=None)
    return pt.ProtocolConfig(**{**base.__dict__,
                                "warmup_seasons": list(sl.WARMUP_CONFIRMACION)})


def _plano(ok: np.ndarray, q: np.ndarray) -> np.ndarray:
    return np.where(ok, q - 1.0, -1.0)


def _apuestas(P: np.ndarray, Q: np.ndarray, estrategia: str,
              umbral: float) -> tuple[np.ndarray, np.ndarray]:
    """Qué signo se apuesta en cada partido y en cuáles se entra.

    Devuelve `(signo, entra)`. El signo es siempre un índice 0/1/2 válido; `entra`
    dice si esa apuesta se juega. Separarlos deja que todas las estrategias compartan
    la misma liquidación y que la única diferencia sea la regla.
    """
    n = len(P)
    fila = np.arange(n)
    ev = P * Q - 1.0                      # valor esperado por unidad, signo a signo
    if estrategia == "plana":
        sig = P.argmax(axis=1)
        return sig, np.ones(n, dtype=bool)
    if estrategia == "valor":
        # El signo de MAYOR valor esperado, que no tiene por qué ser el más probable.
        sig = ev.argmax(axis=1)
        return sig, ev[fila, sig] > umbral
    if estrategia == "valor_maxprob":
        # Valor, pero restringido al signo más probable: es la regla de `backtest`
        # y aísla cuánto del resultado viene de permitir cambiar de signo.
        sig = P.argmax(axis=1)
        return sig, ev[fila, sig] > umbral
    if estrategia == "empate_valor":
        sig = np.ones(n, dtype=int)
        return sig, ev[:, 1] > umbral
    if estrategia == "empate_plano":
        # Control: apostar el empate SIEMPRE. Si esto ya es positivo, el mérito es
        # del precio del empate y no de que el modelo sepa cuándo.
        return np.ones(n, dtype=int), np.ones(n, dtype=bool)
    raise ValueError(estrategia)


def _mide(P: np.ndarray, d: pd.DataFrame, cols: list[str], estrategia: str,
          umbral: float) -> dict | None:
    Q = d[cols].to_numpy(dtype=float)
    y = d["y_num"].to_numpy(dtype=int)
    fila = np.arange(len(d))
    sig, entra = _apuestas(P, Q, estrategia, umbral)
    if entra.sum() < 10:
        return None
    ok = sig == y
    ben = _plano(ok, Q[fila, sig])[entra]
    # El listón, emparejado y SOLO en los partidos que esta estrategia apostó: una
    # unidad plana al favorito de Bet365. Es el control del censo.
    Qb = d[CUOTA].to_numpy(dtype=float)
    fav = Qb.argmin(axis=1)
    benr = _plano(fav == y, Qb[fila, fav])[entra]
    dif = ben - benr
    # Las unidades por cada 100 partidos DISPONIBLES, que es lo que decide. El ROI
    # por apuesta no sirve para comparar reglas que apuestan distinto número de
    # veces: filtrar sube el ROI por apuesta y baja el dinero ganado. Medido aquí:
    # `valor_maxprob` a cuota max y umbral 0.02 bate al listón emparejado por
    # +6.81 pp ± 1.91 en confirmación —intervalo que excluye el cero— y pierde
    # 4.87 unidades por 100 partidos, porque solo apuesta el 52 % y en absoluto
    # rinde −9.30 % contra el −3.22 % de apostar a todo a la misma cuota. El
    # filtro no encuentra valor: encuentra partidos donde el favorito también
    # falla, y el listón emparejado sobre el subconjunto apostado no lo distingue.
    # La alternativa real a filtrar es apostar a todo, no no apostar.
    pad = np.zeros(len(d))
    pad[entra] = ben
    return {"estrategia": estrategia, "umbral": umbral,
            "n_apuestas": int(entra.sum()), "n_bloque": int(len(d)),
            "cobertura": float(entra.mean()),
            "acierto": float(ok[entra].mean()),
            "cuota_media": float(Q[fila, sig][entra].mean()),
            "roi": float(ben.mean()),
            "ee_roi": float(ben.std(ddof=1) / np.sqrt(len(ben))),
            "unidades": float(ben.sum()),
            "u100": float(100.0 * pad.mean()),
            "ee_u100": float(100.0 * pad.std(ddof=1) / np.sqrt(len(pad))),
            "roi_liston": float(benr.mean()),
            "vent": float(dif.mean()),
            "ee_vent": float(dif.std(ddof=1) / np.sqrt(len(dif)))}


def main() -> int:
    t0 = time.time()
    feat = lb.datos()
    print("\nevaluando la semilla de mercado sobre ocho temporadas...", flush=True)
    e = lb.evalua(list(sl.SEMILLA_MERCADO), FAMILIA, cfg=cfg_abierto(), feat=feat,
                  evaluar="validacion", cuotas=CASA, n_boot=0, registrar=False,
                  verbose=False)
    d = e.pred[["match_key", "season", "y_num"] + PROB + CUOTA].copy()
    d["season"] = d["season"].astype(str)
    extra = [c for k, v in FAMILIAS.items() if k != "b365" for c in v]
    d = d.merge(feat[["match_key"] + extra].drop_duplicates("match_key"),
                on="match_key", how="left")
    todas = [c for v in FAMILIAS.values() for c in v]
    d = d.loc[d[todas].notna().all(axis=1)].reset_index(drop=True)
    P = d[PROB].to_numpy(dtype=float)
    print(f"censo {len(d)} partidos  [{time.time() - t0:.0f}s]")
    idx = {b: d["season"].isin(ss).to_numpy() for b, ss in BLOQUES.items()}
    for b, m in idx.items():
        print(f"  {b:12s} {int(m.sum()):4d}")

    filas = []
    for casa, cols in FAMILIAS.items():
        for est in ("plana", "valor", "valor_maxprob", "empate_valor",
                    "empate_plano"):
            for u in (UMBRALES if "valor" in est else (0.0,)):
                for b, m in idx.items():
                    r = _mide(P[m], d.loc[m], cols, est, u)
                    if r:
                        filas.append({"casa": casa, "bloque": b, **r})
    tab = pd.DataFrame(filas)
    tab.to_csv(CURVA, index=False, encoding="utf-8")
    _informe(tab)
    print(f"\n{time.time() - t0:.0f} s\n{CURVA}\n{RESUMEN}")
    return 0


def _informe(tab: pd.DataFrame) -> None:
    print(f"\n{'=' * 92}")
    print("ROI por estrategia · modelo y pronóstico FIJOS · listón emparejado "
          "sobre los mismos partidos")
    print(f"{'=' * 92}")
    for casa in FAMILIAS:
        print(f"\n--- cuota {casa} ---")
        print(f"{'estrategia':<15} {'u':>5} {'bloque':<13} {'apu':>5} {'cob':>5} "
              f"{'acierto':>8} {'cuota':>6} {'ROI':>9} {'ee':>6} "
              f"{'listón':>8} {'vent':>9} {'ee':>6}")
        sub = tab.loc[tab["casa"] == casa]
        for (est, u), g in sub.groupby(["estrategia", "umbral"], sort=False):
            for b in ("seleccion", "medicion", "confirmacion"):
                r = g.loc[g["bloque"] == b]
                if r.empty:
                    continue
                r = r.iloc[0]
                marca = "*" if b == ELIGE_EN else " "
                print(f"{est:<15} {u:>5.2f} {b[:11]:<11}{marca} "
                      f"{int(r['n_apuestas']):>5} {100 * r['cobertura']:>4.0f}% "
                      f"{100 * r['acierto']:>7.2f}% {r['cuota_media']:>6.2f} "
                      f"{100 * r['roi']:>+8.2f}% {100 * r['ee_roi']:>5.2f} "
                      f"{100 * r['roi_liston']:>+7.2f}% "
                      f"{100 * r['vent']:>+8.2f}% {100 * r['ee_vent']:>5.2f}")
            print()

    _veredicto(tab)


def _veredicto(tab: pd.DataFrame) -> None:
    """El umbral se elige en `seleccion`, y el rival es apostar a TODO, no el listón.

    La ventaja sobre el listón emparejado se mide solo en los partidos que la
    estrategia apostó, y eso contesta «¿batió el modelo al favorito ahí?», no
    «¿conviene filtrar?». La alternativa real a filtrar es la regla plana sobre
    todos los partidos a la misma cuota, y la moneda común es la unidad por
    partido disponible. Con el listón como rival, `max/valor_maxprob` a umbral
    0.02 parecía replicar en los tres bloques —+5.33, +2.07 y +6.81 pp, este
    último con intervalo que excluye el cero—; con la regla plana como rival y en
    unidades por 100 partidos disponibles pierde en los tres: −0.72, +0.18 y
    −4.87 contra +2.20, +4.17 y −3.22. El filtro no halla valor: halla partidos
    donde el favorito también falla.
    """
    res = {}
    print(f"\n{'=' * 92}")
    print(f"umbral elegido en '{ELIGE_EN}' por unidades por 100 partidos "
          f"disponibles · rival = apostar a todo")
    print(f"{'=' * 92}")
    for casa in FAMILIAS:
        base = tab.loc[(tab["casa"] == casa) & (tab["estrategia"] == "plana")]
        pla = {r["bloque"]: float(r["u100"]) for _, r in base.iterrows()}
        print(f"\n{casa:>5} {'plana':<15} "
              + " · ".join(f"{b[:4]} {pla[b]:+5.2f}" for b in
                           ("seleccion", "medicion", "confirmacion")))
        for est in ("valor", "valor_maxprob", "empate_valor", "empate_plano"):
            s = tab.loc[(tab["casa"] == casa) & (tab["estrategia"] == est)]
            el = s.loc[s["bloque"] == ELIGE_EN]
            if el.empty:
                continue
            u = float(el.loc[el["u100"].idxmax(), "umbral"])
            fuera = {}
            for b in ("medicion", "confirmacion"):
                r = s.loc[(s["bloque"] == b) & (np.isclose(s["umbral"], u))]
                if not r.empty:
                    r = r.iloc[0]
                    fuera[b] = {"u100": float(r["u100"]),
                                "ee": float(r["ee_u100"]),
                                "gana_a_plana": bool(float(r["u100"]) > pla[b]),
                                "n": int(r["n_apuestas"])}
            ok = bool(fuera) and all(v["gana_a_plana"] for v in fuera.values())
            sig = any(v["u100"] - 1.96 * v["ee"] > pla[b]
                      for b, v in fuera.items())
            res[f"{casa}/{est}"] = {"umbral": u, "plana": pla, "fuera": fuera,
                                    "bate_a_plana": ok, "excluye_cero": bool(sig)}
            txt = " · ".join(
                f"{b[:4]} {v['u100']:+5.2f}±{v['ee']:.2f} (n={v['n']})"
                for b, v in fuera.items())
            marca = "BATE A PLANA" if ok else "no"
            if ok and not sig:
                marca = "bate a plana, dentro del ruido"
            print(f"{casa:>5} {est:<15} u={u:.2f}  {txt}  → {marca}")
    RESUMEN.write_text(json.dumps(res, indent=2, ensure_ascii=False),
                       encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
