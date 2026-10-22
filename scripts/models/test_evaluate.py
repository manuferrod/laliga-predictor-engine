"""Banco de pruebas de `evaluate.py`, sin red y sin artefactos.

Una metrica mal implementada no se detecta sola. Produce un numero del orden de
magnitud correcto, ordena los modelos de forma plausible y solo falla en el sitio
donde nadie mira: el signo de una habilidad, el orden de las clases en una matriz
de confusion, el peso de un tramo vacio en un error de calibracion. Por eso este
banco no comprueba que las funciones "devuelvan algo razonable" sino que
devuelvan **el numero exacto** que sale de calcularlo a mano.

Casi todas las comprobaciones se apoyan en casos con solucion cerrada:

    - pronosticos degenerados, donde la perdida se conoce de antemano (una terna
      que acierta con probabilidad uno da perdida cero; una que falla con
      probabilidad uno da el recorte),
    - pronosticos perfectamente calibrados por construccion, donde la fiabilidad
      debe ser cero y la resolucion positiva,
    - conjuntos pequenos donde el RPS, el log-loss y el Brier se escriben a mano,
    - simetrias que ninguna implementacion correcta puede romper: permutar el
      orden de los partidos no cambia nada, duplicar el conjunto tampoco, y la
      habilidad de una terna frente a si misma es exactamente cero.

Secciones
---------
    1  PERDIDAS        RPS, log-loss, Brier y acierto sobre casos cerrados
    2  ORDINALIDAD     el RPS distingue lo que el Brier no
    3  CALIBRACION     fiabilidad, ECE y descomposicion de Murphy
    4  CLASIFICACION   confusion, informe por clase y AUC
    5  REFERENCIAS     consenso, Pinnacle, prior y local
    6  COMPARACION     conjunto comun, habilidad y cobertura
    7  DESACUERDO      agrupacion por distancia al mercado
    8  INCERTIDUMBRE   remuestreo por bloques y contraste de permutacion
    9  GUARDAS         lo que debe romperse y como
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import evaluate as ev  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")


def _cerca(a, b, tol=1e-9) -> bool:
    return bool(np.all(np.abs(np.asarray(a, dtype="float64")
                              - np.asarray(b, dtype="float64")) < tol))


# ===========================================================================
# Material sintetico
# ===========================================================================
def _pred(y, P, *, Q=None, R=None, B=None, season=None, jornada=None) -> pd.DataFrame:
    """Arma un marco con la forma exacta que publica `run_protocol`."""
    y = np.asarray(y, dtype="float64")
    P = np.asarray(P, dtype="float64")
    n = len(y)
    df = pd.DataFrame({
        "season": season if season is not None else ["2019/20"] * n,
        "jornada": jornada if jornada is not None else np.arange(n) // 5 + 1,
        "fold": np.arange(n) // 5 + 1,
        "match_key": [f"m{i:04d}" for i in range(n)],
        "y_num": y,
        "p_home": P[:, 0], "p_draw": P[:, 1], "p_away": P[:, 2],
    })
    if Q is not None:
        Q = np.asarray(Q, dtype="float64")
        df["mkt_p_home_raw"], df["mkt_p_draw_raw"], df["mkt_p_away_raw"] = \
            Q[:, 0], Q[:, 1], Q[:, 2]
    if R is not None:
        R = np.asarray(R, dtype="float64")
        df["mkt_p_ps_home_raw"], df["mkt_p_ps_draw_raw"], df["mkt_p_ps_away_raw"] = \
            R[:, 0], R[:, 1], R[:, 2]
    if B is not None:
        B = np.asarray(B, dtype="float64")
        df["mkt_p_b365_home_raw"], df["mkt_p_b365_draw_raw"], \
            df["mkt_p_b365_away_raw"] = B[:, 0], B[:, 1], B[:, 2]
    return df


def _liga(n=800, semilla=17, ventaja=0.25):
    """Conjunto grande y realista: un pronosticador informado frente a otro mejor.

    El mercado se construye con mas ventaja que el modelo a proposito. Es el
    desenlace mayoritario del dominio y el que hace utiles las comprobaciones de
    signo: si la habilidad saliera positiva aqui, estaria mal calculada.
    """
    rng = np.random.default_rng(semilla)
    y = rng.choice([0, 1, 2], size=n, p=[0.46, 0.25, 0.29])
    P = rng.dirichlet([4.0, 3.0, 3.0], size=n)
    P[np.arange(n), y] += ventaja
    P /= P.sum(axis=1, keepdims=True)
    Q = rng.dirichlet([5.0, 4.0, 4.0], size=n)
    Q[np.arange(n), y] += ventaja + 0.10
    Q /= Q.sum(axis=1, keepdims=True)
    season = np.where(np.arange(n) < n // 2, "2019/20", "2020/21")
    jornada = np.tile(np.repeat(np.arange(1, n // 20 + 1), 10), 2)[:n]
    return _pred(y, P, Q=Q, season=season, jornada=jornada)


# ===========================================================================
# 1. Perdidas por partido
# ===========================================================================
def test_perdidas() -> None:
    print("\n1. PERDIDAS POR PARTIDO")

    cierto = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])
    y_ok = np.array([0.0, 1.0, 2.0])
    check(_cerca(ev.rps_vector(y_ok, cierto), 0.0), "RPS del pronostico perfecto es 0")
    check(_cerca(ev.brier_vector(y_ok, cierto), 0.0), "Brier del perfecto es 0")
    check(_cerca(ev.logloss_vector(y_ok, cierto), 0.0), "log-loss del perfecto es 0")
    check(_cerca(ev.hit_vector(y_ok, cierto), 1.0), "acierto del perfecto es 1")

    # Terna uniforme: cada acumulada vale 1/3 y 2/3.
    #   y=0 -> (1/3-1)^2 + (2/3-1)^2 = 4/9 + 1/9 = 5/9 ; /2 -> 5/18
    #   y=1 -> (1/3-0)^2 + (2/3-1)^2 = 1/9 + 1/9 = 2/9 ; /2 -> 1/9
    #   y=2 -> (1/3-0)^2 + (2/3-0)^2 = 1/9 + 4/9 = 5/9 ; /2 -> 5/18
    unif = np.full((3, 3), 1 / 3)
    check(_cerca(ev.rps_vector(y_ok, unif), [5 / 18, 1 / 9, 5 / 18]),
          "RPS de la terna uniforme coincide con el calculo a mano")
    check(_cerca(ev.logloss_vector(y_ok, unif), np.log(3.0)),
          "log-loss de la uniforme es log(3) en las tres clases")
    # Brier uniforme: (1/3-1)^2 + 2*(1/3)^2 = 4/9 + 2/9 = 6/9
    check(_cerca(ev.brier_vector(y_ok, unif), 2 / 3),
          "Brier de la uniforme es 2/3 en las tres clases")

    # Un caso asimetrico escrito a mano: p = (0.5, 0.3, 0.2), y = empate.
    #   acumuladas p = (0.5, 0.8) ; acumuladas o = (0, 1)
    #   RPS = (0.25 + 0.04)/2 = 0.145
    #   Brier = 0.25 + 0.49 + 0.04 = 0.78
    p = np.array([[0.5, 0.3, 0.2]])
    check(_cerca(ev.rps_vector(np.array([1.0]), p), 0.145),
          "RPS de un caso asimetrico escrito a mano")
    check(_cerca(ev.brier_vector(np.array([1.0]), p), 0.78),
          "Brier del mismo caso")
    check(_cerca(ev.logloss_vector(np.array([1.0]), p), -np.log(0.3)),
          "log-loss del mismo caso")

    # El recorte convierte el infinito en un numero grande y declarado.
    fallo = np.array([[1.0, 0.0, 0.0]])
    ll = ev.logloss_vector(np.array([2.0]), fallo)[0]
    check(np.isfinite(ll) and ll > 30,
          "una probabilidad cero para el resultado ocurrido se recorta, no diverge")

    # Los empates de argmax se resuelven por el orden declarado H, D, A.
    check(ev.hit_vector(np.array([0.0]), unif[:1])[0] == 1.0,
          "el empate de argmax se resuelve por el orden H, D, A")


# ===========================================================================
# 2. Ordinalidad: lo que el RPS ve y el Brier no
# ===========================================================================
def test_ordinalidad() -> None:
    print("\n2. ORDINALIDAD DEL RPS")

    # Gana el local. Dos pronosticos igual de equivocados en masa, pero uno se
    # equivoca "cerca" (pone la masa en el empate) y el otro "lejos" (la pone en
    # la victoria visitante).
    cerca = np.array([[0.2, 0.8, 0.0]])
    lejos = np.array([[0.2, 0.0, 0.8]])
    y = np.array([0.0])

    r_cerca = ev.rps_vector(y, cerca)[0]
    r_lejos = ev.rps_vector(y, lejos)[0]
    check(r_lejos > r_cerca,
          "el RPS penaliza mas equivocarse de local a visitante que de local a empate")
    check(_cerca(ev.brier_vector(y, cerca), ev.brier_vector(y, lejos)),
          "el Brier no distingue los dos errores: da exactamente el mismo numero")
    check(_cerca(ev.logloss_vector(y, cerca), ev.logloss_vector(y, lejos)),
          "el log-loss tampoco los distingue")

    # Y la ordenacion es la del dominio, no una etiqueta arbitraria: el RPS de
    # "lejos" es exactamente el de "cerca" mas el termino del segundo corte.
    #   cerca: acumuladas (0.2, 1.0) vs (1,1) -> (0.64 + 0.00)/2 = 0.32
    #   lejos: acumuladas (0.2, 0.2) vs (1,1) -> (0.64 + 0.64)/2 = 0.64
    check(_cerca(r_cerca, 0.32) and _cerca(r_lejos, 0.64),
          "ambos RPS coinciden con el calculo a mano (0.32 y 0.64)")

    # Simetria: invertir el orden de los partidos no cambia el promedio.
    df = _liga(n=200)
    m1 = ev.metrics(df, completo=False)
    m2 = ev.metrics(df.iloc[::-1].reset_index(drop=True), completo=False)
    check(all(_cerca(m1[k], m2[k]) for k in ("rps", "logloss", "brier", "accuracy")),
          "reordenar los partidos no altera ninguna metrica promediable")


# ===========================================================================
# 3. Calibracion
# ===========================================================================
def test_calibracion() -> None:
    print("\n3. CALIBRACION Y DESCOMPOSICION")

    # Pronostico perfectamente calibrado por construccion: la terna es siempre la
    # misma y coincide con la frecuencia real de las clases en la muestra.
    base = np.array([0.5, 0.2, 0.3])
    y = np.array([0] * 50 + [1] * 20 + [2] * 30, dtype="float64")
    P = np.tile(base, (100, 1))

    d = ev.brier_decomposition(y, P)
    check(_cerca(d["fiabilidad"], 0.0, tol=1e-12),
          "un pronostico calibrado tiene fiabilidad exactamente 0")
    check(_cerca(d["resolucion"], 0.0, tol=1e-12),
          "un pronostico constante no tiene resolucion: nunca separa dos grupos")
    # incertidumbre = sum_k p_k(1-p_k) = .25 + .16 + .21 = .62
    check(_cerca(d["incertidumbre"], 0.62),
          "la incertidumbre es la del propio resultado, no la del modelo")
    check(_cerca(d["residuo"], 0.0, tol=1e-12),
          "un pronostico constante no reparte nada dentro del tramo: residuo 0")
    check(_cerca(d["brier"], d["fiabilidad"] - d["resolucion"] + d["incertidumbre"]
                 + d["residuo"], tol=1e-12),
          "la identidad de Murphy cierra: BS = fiab. - resol. + incert. + residuo")
    check(_cerca(ev.ece(y, P, clase=0), 0.0, tol=1e-12),
          "el ECE por clase de un pronostico calibrado es 0")

    # Un pronostico con resolucion: dos grupos con frecuencias distintas y
    # declaradas correctamente. Fiabilidad sigue siendo cero, resolucion no.
    y2 = np.array([0] * 40 + [2] * 10 + [0] * 10 + [2] * 40, dtype="float64")
    P2 = np.vstack([np.tile([0.8, 0.0, 0.2], (50, 1)),
                    np.tile([0.2, 0.0, 0.8], (50, 1))])
    d2 = ev.brier_decomposition(y2, P2)
    check(_cerca(d2["fiabilidad"], 0.0, tol=1e-12),
          "sigue calibrado cuando el pronostico varia entre grupos")
    check(d2["resolucion"] > 0.15,
          "ahora si hay resolucion: los dos grupos tienen frecuencias distintas")
    check(d2["brier"] < d["brier"],
          "a igual fiabilidad, mas resolucion significa mejor Brier")

    # Un pronostico descalibrado a proposito: dice 0.9 y acierta la mitad.
    y3 = np.array([0, 2] * 50, dtype="float64")
    P3 = np.tile([0.9, 0.0, 0.1], (100, 1))
    check(ev.brier_decomposition(y3, P3)["fiabilidad"] > 0.3,
          "un pronostico que dice 0.9 y acierta 0.5 tiene fiabilidad alta")
    check(_cerca(ev.ece(y3, P3, clase=0), 0.4),
          "su ECE en la clase local es exactamente |0.9 - 0.5| = 0.4")

    # Un pronostico continuo: dentro de cada tramo conviven probabilidades
    # distintas, que es la situacion real de cualquier modelo. Ahi el residuo deja
    # de ser cero, y la identidad solo cierra si se le cuenta. Es la comprobacion
    # que importa, porque es el caso que se da en produccion.
    rng = np.random.default_rng(7)
    P4 = rng.dirichlet([4.0, 3.0, 3.0], size=500)
    y4 = np.array([rng.choice(3, p=fila) for fila in P4], dtype="float64")
    d4 = ev.brier_decomposition(y4, P4)
    check(abs(d4["residuo"]) > 1e-6,
          "con probabilidades continuas el reparto por tramos deja resto")
    check(_cerca(d4["brier"], d4["fiabilidad"] - d4["resolucion"]
                 + d4["incertidumbre"] + d4["residuo"], tol=1e-12),
          "y aun asi la identidad cierra exacta, que es para lo que sirve")
    fino = ev.brier_decomposition(y4, P4, bins=100)
    check(abs(fino["residuo"]) < abs(d4["residuo"]),
          "el resto encoge al afinar los tramos: es el precio de agrupar")

    # La tabla de fiabilidad: un tramo por decil, todos presentes aunque vacios.
    tab = ev.reliability_table(y3, P3, clase=0)
    check(len(tab) == 10, "la tabla de fiabilidad publica los diez deciles")
    check(int(tab.n.sum()) == 100, "y reparte todos los partidos entre ellos")
    check(tab.loc[tab.n == 0, "p_media"].isna().all(),
          "los tramos vacios se marcan como huecos, no como ceros")
    ocupado = tab[tab.n > 0].iloc[0]
    check(_cerca(ocupado.brecha, ocupado.frec_obs - ocupado.p_media),
          "la brecha es la diferencia entre lo observado y lo declarado")

    # El ECE pondera por ocupacion: un tramo con un solo partido no puede pesar
    # lo mismo que uno con noventa y nueve.
    y4 = np.array([0.0] * 99 + [2.0])
    P4 = np.vstack([np.tile([0.99, 0.0, 0.01], (99, 1)), [[0.05, 0.0, 0.95]]])
    check(ev.ece(y4, P4, clase=0) < 0.05,
          "el ECE ponderado no se desboca por un unico partido en un tramo aislado")


# ===========================================================================
# 4. Clasificacion
# ===========================================================================
def test_clasificacion() -> None:
    print("\n4. CLASIFICACION")

    #   real \ pred     H   D   A
    #   H               2   1   0
    #   D               0   1   1
    #   A               1   0   2
    y = np.array([0, 0, 0, 1, 1, 2, 2, 2], dtype="float64")
    P = np.array([
        [0.7, 0.2, 0.1], [0.7, 0.2, 0.1], [0.2, 0.7, 0.1],   # H,H,D
        [0.1, 0.8, 0.1], [0.1, 0.2, 0.7],                    # D,A
        [0.8, 0.1, 0.1], [0.1, 0.2, 0.7], [0.1, 0.2, 0.7],   # H,A,A
    ])
    M = ev.confusion_matrix(y, P)
    check(M.to_numpy().tolist() == [[2, 1, 0], [0, 1, 1], [1, 0, 2]],
          "la matriz de confusion coincide celda a celda con la escrita a mano")
    check(list(M.index) == ["real_H", "real_D", "real_A"],
          "las filas son la realidad y las columnas la prediccion, en orden H D A")
    check(int(M.to_numpy().sum()) == len(y),
          "la matriz reparte todos los partidos")

    rep = ev.classification_report(y, P)
    fila_H = rep[rep.clase == "H"].iloc[0]
    # H: tp=2, fp=1 (el A predicho como H) -> precision 2/3 ; fn=1 -> exhaust. 2/3
    check(_cerca(fila_H.precision, 2 / 3) and _cerca(fila_H.exhaustividad, 2 / 3),
          "precision y exhaustividad de la clase local salen 2/3 y 2/3")
    check(_cerca(fila_H.f1, 2 / 3), "y su F1, la media armonica, tambien")
    fila_D = rep[rep.clase == "D"].iloc[0]
    # D: tp=1, fp=1 -> 1/2 ; fn=1 -> 1/2
    check(_cerca(fila_D.precision, 0.5) and _cerca(fila_D.exhaustividad, 0.5),
          "el empate sale 1/2 y 1/2, que es su lectura separada")
    macro = rep[rep.clase == "macro"].iloc[0]
    check(_cerca(macro.f1, rep.f1[:3].mean()),
          "el promedio macro trata las tres clases por igual")
    pond = rep[rep.clase == "ponderado"].iloc[0]
    check(_cerca(pond.f1, float(rep.f1[:3] @ (rep.soporte[:3] / len(y)))),
          "el ponderado las pesa por soporte")
    check(int(macro.soporte) == len(y) and int(pond.soporte) == len(y),
          "ambos promedios declaran el soporte total")

    # AUC: separacion perfecta y separacion nula, los dos extremos conocidos.
    y_bin = np.array([0, 0, 2, 2], dtype="float64")
    perfecto = np.array([[0.9, 0.0, 0.1], [0.8, 0.0, 0.2],
                         [0.2, 0.0, 0.8], [0.1, 0.0, 0.9]])
    check(_cerca(ev.roc_auc_ovr(y_bin, perfecto)["auc_H"], 1.0),
          "una separacion perfecta da AUC 1")
    plano = np.tile([0.5, 0.0, 0.5], (4, 1))
    check(_cerca(ev.roc_auc_ovr(y_bin, plano)["auc_H"], 0.5),
          "una puntuacion constante da AUC 0.5, con los empates a rango medio")
    invertido = perfecto[::-1]
    check(_cerca(ev.roc_auc_ovr(y_bin, invertido)["auc_H"], 0.0),
          "invertir la puntuacion da AUC 0, que es la simetria del estadistico")

    # Una clase ausente no puede inventarse un AUC.
    y_sin_D = np.array([0, 0, 2, 2], dtype="float64")
    check(not np.isfinite(ev.roc_auc_ovr(y_sin_D, perfecto)["auc_D"]),
          "sin partidos de una clase, su AUC es un hueco y no un 0.5 silencioso")
    check(np.isfinite(ev.roc_auc_ovr(y_sin_D, perfecto)["auc_macro"]),
          "el macro se calcula sobre las clases que si existen")


# ===========================================================================
# 5. Referencias
# ===========================================================================
def test_referencias() -> None:
    print("\n5. REFERENCIAS")

    y = np.array([0.0, 0.0, 1.0, 2.0])
    P = np.tile([0.4, 0.3, 0.3], (4, 1))
    Q = np.tile([0.5, 0.2, 0.3], (4, 1))
    R = np.tile([0.6, 0.2, 0.2], (4, 1))
    B = np.tile([0.55, 0.22, 0.23], (4, 1))
    df = _pred(y, P, Q=Q, R=R, B=B)

    check(_cerca(ev.reference_probs(df, "consenso"), Q),
          "la referencia de consenso se lee de las columnas crudas del maestro")
    check(_cerca(ev.reference_probs(df, "pinnacle"), R),
          "la de Pinnacle, de las suyas")
    check(_cerca(ev.reference_probs(df, "b365"), B),
          "y la de Bet365, de las suyas: los tres precios son referencias distintas")
    check(_cerca(ev.reference_probs(df, "prior")[0], [0.5, 0.25, 0.25]),
          "el prior reproduce la distribucion de clases del conjunto evaluado")
    check(_cerca(ev.reference_probs(df, "local")[0], [1.0, 0.0, 0.0]),
          "la referencia local da toda la masa a la victoria del anfitrion")

    sin_pinnacle = df.drop(columns=[c for c in df.columns if "_ps_" in c])
    check(ev.reference_probs(sin_pinnacle, "pinnacle") is None,
          "una referencia que el artefacto no trae devuelve hueco, no un error")
    sin_b365 = df.drop(columns=[c for c in df.columns if "_b365_" in c])
    check(ev.reference_probs(sin_b365, "b365") is None,
          "lo mismo vale para Bet365: ausencia es hueco, no fallo")

    # El prior es, por construccion, la terna que minimiza el log-loss entre
    # todas las constantes. Ninguna otra constante puede batirlo.
    y_g = np.array([0] * 46 + [1] * 25 + [2] * 29, dtype="float64")
    df_g = _pred(y_g, np.tile([0.4, 0.3, 0.3], (100, 1)))
    prior = ev.reference_probs(df_g, "prior")
    ll_prior = ev.logloss_vector(y_g, prior).mean()
    peor = ev.logloss_vector(y_g, np.tile([0.34, 0.33, 0.33], (100, 1))).mean()
    check(ll_prior < peor,
          "el prior es la mejor constante posible: ninguna otra lo bate en log-loss")

    # La referencia local es degenerada a proposito y su log-loss lo demuestra.
    local = ev.reference_probs(df_g, "local")
    check(ev.logloss_vector(y_g, local).mean() > 10,
          "la regla 'gana el local' se hunde como pronostico probabilistico")
    check(_cerca(ev.hit_vector(y_g, local).mean(), 0.46),
          "aunque su acierto sea exactamente la frecuencia de victorias locales")

    try:
        ev.reference_probs(df, "mercado_negro")
        check(False, "una referencia inexistente se detiene con error explicito")
    except ValueError as e:
        check("desconocida" in str(e),
              "una referencia inexistente se detiene con error explicito")


# ===========================================================================
# 6. Comparacion sobre el mismo conjunto
# ===========================================================================
def test_comparacion() -> None:
    print("\n6. COMPARACION Y HABILIDAD")

    df = _liga(n=600)
    tab = ev.compare(df)

    check("modelo" in tab.index and "consenso" in tab.index,
          "la tabla enfrenta al modelo con las referencias disponibles")
    check(tab.attrs["referencias_ausentes"] == ["pinnacle", "b365"],
          "declara por su nombre las referencias que el artefacto no trae")
    check(len(set(tab.n)) == 1,
          "todos los contendientes se miden sobre el MISMO numero de partidos")
    check(tab.attrs["cobertura"]["comunes"] == int(tab.n.iloc[0]),
          "y la cobertura declarada coincide con ese numero")

    # El mercado de `_liga` esta construido mejor que el modelo: la habilidad
    # tiene que salir negativa. Un signo invertido pasaria desapercibido en una
    # ejecucion real y aqui no.
    check(tab.loc["modelo", "rps"] > tab.loc["consenso", "rps"],
          "el mercado sintetico, mejor informado, gana en RPS")
    check(tab.loc["modelo", "skill_rps"] < 0,
          "y la habilidad frente a el sale NEGATIVA, con el signo correcto")
    check(_cerca(tab.loc["modelo", "skill_rps"],
                 1 - tab.loc["modelo", "rps"] / tab.loc["consenso", "rps"]),
          "la habilidad es exactamente 1 - metrica/referencia")

    # La habilidad de una terna frente a si misma es cero por definicion.
    espejo = df.copy()
    for c, r in zip(ev.PROB_COLS, ev.REFERENCIAS["consenso"]):
        espejo[c] = espejo[r]
    t2 = ev.compare(espejo, referencias=("consenso",))
    check(_cerca(t2.loc["modelo", "skill_rps"], 0.0, tol=1e-12),
          "una terna comparada consigo misma tiene habilidad exactamente 0")
    check(_cerca(t2.loc["modelo", "skill_accuracy"], 0.0, tol=1e-12),
          "tambien en la metrica de acierto, cuya orientacion es la contraria")

    # La orientacion de accuracy es la contraria y debe estar contemplada: un
    # modelo que acierta mas tiene habilidad positiva.
    mejor = ev.skill({"accuracy": 0.55, "rps": 0.18},
                     {"accuracy": 0.50, "rps": 0.20})
    check(mejor["skill_accuracy"] > 0 and mejor["skill_rps"] > 0,
          "acertar mas y perder menos dan ambos habilidad positiva")

    # Cobertura parcial: Pinnacle solo cubre la mitad de la muestra. La
    # interseccion debe reducirse para TODOS, no solo para Pinnacle.
    parcial = df.copy()
    R = df[ev.REFERENCIAS["consenso"]].to_numpy(dtype="float64").copy()
    R[: len(df) // 2] = np.nan
    for c, v in zip(ev.REFERENCIAS["pinnacle"], R.T):
        parcial[c] = v
    t3 = ev.compare(parcial)
    check(int(t3.n.iloc[0]) == len(df) - len(df) // 2,
          "una referencia de cobertura parcial reduce el conjunto de todos")
    check(len(set(t3.n)) == 1,
          "y lo reduce por igual: nadie se mide sobre mas partidos que otro")
    check(t3.attrs["cobertura"]["filas"] > t3.attrs["cobertura"]["comunes"],
          "la perdida de cobertura queda registrada, no oculta")

    # Los tres precios a la vez. Es la lectura que se publica en el trabajo, y lo
    # que hay que comprobar es que ninguno se cae por el camino y que los tres se
    # miden sobre los mismos partidos que el modelo.
    tres = df.copy()
    rng = np.random.default_rng(5)
    for cols, ruido in ((ev.REFERENCIAS["pinnacle"], 0.02),
                        (ev.REFERENCIAS["b365"], 0.05)):
        M = df[ev.REFERENCIAS["consenso"]].to_numpy(dtype="float64")
        M = np.abs(M + rng.normal(0.0, ruido, M.shape))
        M /= M.sum(axis=1, keepdims=True)
        for c, v in zip(cols, M.T):
            tres[c] = v
    t4 = ev.compare(tres)
    check(set(t4.index) == {"modelo", "consenso", "pinnacle", "b365",
                            "prior", "local"},
          "con los tres precios presentes, el cuadro los enfrenta a todos")
    check(t4.attrs["referencias_ausentes"] == [] and len(set(t4.n)) == 1,
          "ninguno falta y todos se miden sobre los mismos partidos")

    # Por temporada: las filas deben sumar el total y las cifras ser distintas.
    por_t = ev.metrics_by_season(df)
    check(len(por_t) == 2 and int(por_t.n.sum()) == len(df),
          "el desglose por temporada reparte todos los partidos")


# ===========================================================================
# 7. Desacuerdo con el mercado
# ===========================================================================
def test_desacuerdo() -> None:
    print("\n7. DESACUERDO CON EL MERCADO")

    df = _liga(n=600)
    tab = ev.disagreement(df, n_grupos=5)

    check(len(tab) == 5, "los partidos se reparten en los cinco grupos pedidos")
    check(int(tab.n.sum()) == len(df), "y ningun partido se queda fuera")
    check(tab.dist_media.is_monotonic_increasing,
          "los grupos estan ordenados por distancia creciente al mercado")
    check(_cerca(tab.skill_rps, 1 - tab.rps_modelo / tab.rps_mercado),
          "la habilidad por grupo se calcula igual que la global")

    # Si el modelo ES el mercado, la distancia es cero en todos los partidos y
    # la habilidad es nula en todos los grupos que sobrevivan a los cortes.
    espejo = df.copy()
    for c, r in zip(ev.PROB_COLS, ev.REFERENCIAS["consenso"]):
        espejo[c] = espejo[r]
    t2 = ev.disagreement(espejo, n_grupos=5)
    check(_cerca(t2.dist_media.to_numpy(), 0.0, tol=1e-12),
          "replicar el precio da distancia cero")
    check(_cerca(t2.skill_rps.to_numpy(), 0.0, tol=1e-12),
          "y habilidad cero en todos los grupos")
    check(len(t2) >= 1,
          "los cortes repetidos se colapsan sin romper la agrupacion")

    # Un modelo construido para ganar solo donde discrepa debe verse como tal.
    n = 400
    rng = np.random.default_rng(5)
    y = rng.choice([0, 1, 2], size=n)
    Q = np.tile([0.45, 0.27, 0.28], (n, 1))
    P = Q.copy()
    mitad = np.arange(n) >= n // 2
    P[mitad] = 0.15                       # discrepa fuerte en la segunda mitad
    P[np.arange(n)[mitad], y[mitad]] = 0.70
    dis = ev.disagreement(_pred(y, P, Q=Q), n_grupos=2)
    check(dis.skill_rps.iloc[-1] > dis.skill_rps.iloc[0],
          "un modelo que aporta donde discrepa se ve en el ultimo grupo")

    sin_mercado = df.drop(columns=list(ev.REFERENCIAS["consenso"]))
    try:
        ev.disagreement(sin_mercado)
        check(False, "sin referencia de mercado el analisis se detiene")
    except ValueError:
        check(True, "sin referencia de mercado el analisis se detiene")


# ===========================================================================
# 8. Incertidumbre
# ===========================================================================
def test_incertidumbre() -> None:
    print("\n8. INCERTIDUMBRE")

    df = _liga(n=600)
    boot = ev.bootstrap_metrics(df, n_boot=300, seed=3)

    check(list(boot.index) == ["rps", "logloss", "accuracy"],
          "el remuestreo publica las metricas promediables pedidas")
    check(bool((boot.inf < boot.estimacion).all() and
               (boot.estimacion < boot.sup).all()),
          "la estimacion puntual cae dentro de su propio intervalo")
    check(_cerca(boot.loc["rps", "estimacion"], ev.metrics(df, completo=False)["rps"]),
          "la estimacion puntual es la de la muestra, no la media de las replicas")
    check(bool((boot.ee > 0).all()), "el error estandar de cada metrica es positivo")

    # Reproducibilidad: la misma semilla da el mismo intervalo.
    b2 = ev.bootstrap_metrics(df, n_boot=300, seed=3)
    check(_cerca(boot.inf.to_numpy(), b2.inf.to_numpy()),
          "la misma semilla reproduce el intervalo exacto")
    b3 = ev.bootstrap_metrics(df, n_boot=300, seed=99)
    check(not _cerca(boot.inf.to_numpy(), b3.inf.to_numpy()),
          "y otra semilla da uno distinto, como corresponde a un remuestreo")

    # El bloque es la jornada dentro de su temporada, no la jornada nominal: dos
    # temporadas con la misma etiqueta de jornada son dos bloques distintos.
    ancho = df.copy()
    ancho["jornada"] = 1
    check(len(pd.unique(ev._bloques(ancho))) == df.season.nunique(),
          "la etiqueta de bloque combina temporada y jornada, no solo jornada")

    # Con un unico bloque no hay nada que remuestrear: todas las replicas son la
    # muestra entera y el error estandar es cero. Es el caso limite que confirma
    # que la variabilidad publicada procede del reparto en bloques y no de otra
    # fuente de ruido dentro del procedimiento.
    unico = df.copy()
    unico["season"], unico["jornada"] = "2019/20", 1
    b_unico = ev.bootstrap_metrics(unico, metricas=("rps",), n_boot=200, seed=3)
    check(_cerca(b_unico.loc["rps", "ee"], 0.0, tol=1e-12),
          "con un solo bloque el remuestreo no puede variar y el error es 0")

    # La propiedad que justifica el diseno: cuando los resultados estan
    # correlacionados dentro de la jornada —jornadas en las que el modelo acierta
    # en bloque y jornadas en las que falla en bloque—, remuestrear partidos
    # sueltos rompe esa dependencia y devuelve un error estandar demasiado
    # estrecho. El remuestreo por jornadas la respeta y publica el error mayor,
    # que es el honesto. Se construye a proposito una liga con ese patron.
    rng = np.random.default_rng(11)
    filas, jornadas = [], []
    for j in range(60):
        facil = rng.uniform(0.05, 0.95)      # caracter comun a toda la jornada
        for _ in range(10):
            yj = int(rng.choice([0, 1, 2], p=[0.45, 0.27, 0.28]))
            pj = np.array([0.45, 0.27, 0.28])
            if rng.random() < facil:
                pj = np.full(3, 0.10)
                pj[yj] = 0.80
            filas.append((yj, pj))
            jornadas.append(j)
    yc = np.array([f[0] for f in filas])
    Pc = np.vstack([f[1] for f in filas])
    corr = _pred(yc, Pc, season=np.full(len(yc), "2019/20"),
                 jornada=np.array(jornadas))
    fino = corr.copy()
    fino["jornada"] = np.arange(len(fino))            # un bloque por partido
    ee_bloque = ev.bootstrap_metrics(corr, metricas=("rps",),
                                     n_boot=400, seed=3).loc["rps", "ee"]
    ee_suelto = ev.bootstrap_metrics(fino, metricas=("rps",),
                                     n_boot=400, seed=3).loc["rps", "ee"]
    check(ee_suelto < ee_bloque,
          "remuestrear partidos sueltos estrecha el intervalo: por eso no se hace")
    check(ee_bloque / ee_suelto > 1.2,
          "y la dependencia dentro de la jornada no es un matiz: el error crece")

    # Contraste de permutacion.
    t = ev.permutation_test(df, ev.PROB_COLS, list(ev.REFERENCIAS["consenso"]),
                            n_perm=800, seed=3)
    check(t["mejor"] == "b",
          "el contraste identifica al mercado como el mejor de los dos")
    check(t["diferencia"] > 0,
          "y el signo de la diferencia es coherente con esa lectura")
    check(t["p_valor"] < 0.05,
          "con una ventaja construida a proposito, la diferencia es significativa")
    check(t["n_bloques"] == df.jornada.astype(str).radd(
        df.season.astype(str) + "|").nunique(),
          "el contraste permuta bloques de jornada, no partidos")

    # Una terna contra si misma: diferencia nula y valor p maximo.
    t0 = ev.permutation_test(df, ev.PROB_COLS, ev.PROB_COLS, n_perm=200, seed=3)
    check(_cerca(t0["diferencia"], 0.0, tol=1e-12),
          "una terna contra si misma da diferencia exactamente 0")
    check(_cerca(t0["p_valor"], 1.0),
          "y valor p 1: no hay nada que distinguir")
    check(t0["mejor"] == "empate", "el veredicto es empate")

    # El valor p nunca puede ser cero: la muestra observada cuenta como caso.
    check(ev.permutation_test(_liga(n=400, ventaja=0.9), ev.PROB_COLS,
                              list(ev.REFERENCIAS["consenso"]),
                              n_perm=100, seed=1)["p_valor"] > 0,
          "el valor p incluye la muestra observada y por tanto nunca es 0")


# ===========================================================================
# 9. Guardas
# ===========================================================================
def test_guardas() -> None:
    print("\n9. GUARDAS")

    y = np.array([0.0, 1.0, 2.0])
    ok = np.tile([0.4, 0.3, 0.3], (3, 1))

    mala = _pred(y, ok)
    mala.loc[0, "p_home"] = 0.9                      # la terna deja de sumar 1
    try:
        ev.metrics(mala)
        check(False, "una terna que no suma 1 se detiene con error explicito")
    except ValueError as e:
        check("suman 1" in str(e),
              "una terna que no suma 1 se detiene con error explicito")

    fuera = _pred(y, ok)
    fuera.loc[0, ["p_home", "p_draw", "p_away"]] = [1.4, -0.2, -0.2]
    try:
        ev.metrics(fuera)
        check(False, "una probabilidad fuera de [0,1] se detiene")
    except ValueError as e:
        check("fuera de" in str(e), "una probabilidad fuera de [0,1] se detiene")

    sin_y = _pred(y, ok).drop(columns=["y_num"])
    try:
        ev.metrics(sin_y)
        check(False, "sin objetivo no se puede evaluar nada")
    except KeyError as e:
        check("y_num" in str(e), "sin objetivo no se puede evaluar nada")

    objetivo_raro = _pred(y, ok)
    objetivo_raro.loc[0, "y_num"] = 7
    try:
        ev.metrics(objetivo_raro)
        check(False, "un objetivo fuera de {0,1,2} se detiene")
    except ValueError as e:
        check("fuera de" in str(e), "un objetivo fuera de {0,1,2} se detiene")

    sin_cols = _pred(y, ok).drop(columns=["p_draw"])
    try:
        ev.metrics(sin_cols)
        check(False, "una columna de probabilidad ausente se detiene")
    except KeyError:
        check(True, "una columna de probabilidad ausente se detiene")

    # Huecos: se excluyen de la evaluacion, no se imputan.
    con_hueco = _pred(np.array([0.0, 1.0, 2.0, 0.0]),
                      np.tile([0.4, 0.3, 0.3], (4, 1)))
    con_hueco.loc[3, ["p_home", "p_draw", "p_away"]] = np.nan
    check(ev.metrics(con_hueco)["n"] == 3,
          "una terna incompleta se excluye del conjunto evaluado")
    check(int(ev.evaluable(con_hueco).sum()) == 3,
          "y la mascara de evaluables lo declara")

    todo_hueco = con_hueco.copy()
    todo_hueco[ev.PROB_COLS] = np.nan
    try:
        ev.metrics(todo_hueco)
        check(False, "si no queda ningun partido evaluable, se avisa")
    except ValueError as e:
        check("evaluable" in str(e), "si no queda ningun partido evaluable, se avisa")

    # Metricas no promediables en sitios donde solo caben promedios.
    try:
        ev.bootstrap_metrics(_liga(n=100), metricas=("auc_macro",), n_boot=5)
        check(False, "el remuestreo rechaza una metrica que no es un promedio")
    except ValueError as e:
        check("promediable" in str(e),
              "el remuestreo rechaza una metrica que no es un promedio")

    try:
        ev.permutation_test(_liga(n=100), ev.PROB_COLS, ev.PROB_COLS,
                            metrica="auc_macro", n_perm=5)
        check(False, "el contraste tambien la rechaza")
    except ValueError as e:
        check("admisible" in str(e), "el contraste tambien la rechaza")

    # Sin jornada ni pliegue no hay bloques que remuestrear.
    sin_bloques = _liga(n=100).drop(columns=["season", "jornada", "fold"])
    try:
        ev.block_bootstrap(sin_bloques, lambda d: 0.0, n_boot=5)
        check(False, "sin jornada ni pliegue el remuestreo se detiene")
    except KeyError as e:
        check("bloques" in str(e), "sin jornada ni pliegue el remuestreo se detiene")

    # El informe completo se ejecuta de principio a fin sobre material realista.
    res = ev.evaluation_report(_liga(n=400), n_boot=50, verbose=False)
    check(set(res) == {"comparacion", "intervalos", "por_temporada", "desacuerdo",
                       "confusion", "clasificacion", "fiabilidad"},
          "el informe publica el juego completo de tablas")
    check(int(res["confusion"].to_numpy().sum()) == 400,
          "y su matriz de confusion cuadra con los partidos evaluados")


# ===========================================================================
def _run() -> None:
    print("=" * 74)
    print("BANCO DE PRUEBAS — evaluate.py")
    print("=" * 74)
    test_perdidas()
    test_ordinalidad()
    test_calibracion()
    test_clasificacion()
    test_referencias()
    test_comparacion()
    test_desacuerdo()
    test_incertidumbre()
    test_guardas()
    print("\n" + "=" * 74)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    if FAIL:
        for f in FAIL:
            print(f"   - {f}")
    print("=" * 74)


def test_evaluate() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
