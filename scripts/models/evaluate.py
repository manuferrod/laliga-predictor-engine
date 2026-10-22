"""Batería de evaluación: qué de bueno tiene un conjunto de probabilidades 1X2.

Este módulo consume la salida de `protocol.py` —una fila por partido evaluado con
su terna predicha— y no sabe nada de cómo se produjo. Esa ignorancia es
deliberada: la misma función que mide el modelo mide el precio de mercado y mide
la regla «gana siempre el local», de modo que la comparación entre ellos no puede
esconder una diferencia de procedimiento.

Qué se mide y por qué
---------------------
El sistema publica **probabilidades**, no etiquetas, y por eso el orden de
importancia de las métricas está invertido respecto al que espera un lector
acostumbrado a la clasificación.

    RPS         (Ranked Probability Score) es la métrica principal. A diferencia
                del Brier, respeta el orden natural del dominio: equivocarse
                dando la victoria local cuando gana el visitante es peor que
                equivocarse cuando se empata, y el RPS lo penaliza más. Es el
                estándar en predicción de resultados de fútbol.
    log-loss    Métrica estrictamente propia y la moneda común de la literatura
                y de las casas de apuestas. Castiga con dureza la confianza mal
                puesta, que es exactamente el fallo que arruina un producto.
    Brier       Se reporta descompuesto en fiabilidad, resolución e
                incertidumbre. La descomposición separa dos diagnósticos que se
                confunden a menudo: «el modelo está mal calibrado» —sus
                probabilidades no significan lo que dicen— y «el modelo no
                discrimina» —sus probabilidades significan lo que dicen pero
                apenas se mueven del promedio—. Tienen soluciones distintas.
    calibración Curva de fiabilidad por clase y error de calibración esperado.
                Es lo que un usuario percibe sin conocer el nombre: cuando el
                sistema dice sesenta por ciento, debe acertar seis de cada diez.
    acierto     `accuracy`, matriz de confusión, precisión/exhaustividad/F1 por
                clase y ROC-AUC uno-contra-resto. Se conservan porque son las
                que espera un lector no especialista, pero se subordinan a las
                anteriores: un modelo puede ganar en acierto y perder dinero.

El empate merece lectura separada en todas ellas. Es la clase minoritaria, la que
ningún sistema predice bien y la que explica la mayor parte de la diferencia
entre dos modelos razonables.

Contra qué se mide
------------------
Una cifra de log-loss no significa nada en abstracto. Toda métrica se reporta
además como **habilidad**: la mejora relativa respecto a una referencia medida
sobre exactamente los mismos partidos.

    consenso    probabilidad implícita del consenso de casas, sin margen.
    pinnacle    la misma lectura sobre el precio de referencia informado. Si
                resulta mejor que el consenso, es ella la que debe figurar como
                número a batir: superar un consenso peor no demuestra nada.
    b365        la misma lectura sobre Bet365, la casa minorista de gran
                difusión. Es la referencia que el lector reconoce y con la que
                compararía el pronóstico sin ayuda; su margen es más ancho, de
                modo que batirla es condición necesaria y no suficiente.
    prior       la distribución de clases de la ventana de entrenamiento. Da el
                suelo: un modelo que no la bate no ha aprendido nada.
    local       «gana el local» con probabilidad uno. Es la referencia ingenua
                clásica y sirve para dimensionar la accuracy.

`compare` impone que todos los contendientes se midan sobre la **intersección**
de filas evaluables. Medir cada uno sobre su propia cobertura es la forma más
fácil de fabricar una mejora ficticia, porque Pinnacle no cubre las temporadas
antiguas y son precisamente las más difíciles.

El análisis de **desacuerdo** cierra el bloque y es, probablemente, la lectura
más informativa del trabajo: agrupa los partidos por la distancia entre la terna
del modelo y la del mercado y mide quién acierta más en cada grupo. Si el modelo
solo gana donde coincide con el precio, no aporta; si gana donde discrepa,
aporta de verdad.

Incertidumbre
-------------
Ninguna métrica se publica como cifra puntual. Con unos 2.300 partidos evaluados
la diferencia entre dos modelos razonables cabe dentro del ruido, y decirlo es
más defendible que presentar una ordenación falsamente nítida.

El remuestreo es **por bloques de jornada**, no por partido: los partidos de una
misma jornada comparten estado del mundo —una ola de lesiones, un parón
internacional, el clima competitivo de esa semana— y remuestrear partidos
sueltos trataría esa dependencia como información independiente, estrechando los
intervalos de forma artificial. El contraste entre dos modelos es de
**permutación por signo sobre bloques**, que es la versión pareada de la misma
idea: se comparan sobre los mismos partidos, así que lo que se permuta es el
signo de la diferencia media de cada jornada.

Uso
---
    from evaluate import metrics, compare, evaluation_report

    m   = metrics(pred)                    # batería completa del modelo
    tab = compare(pred)                    # modelo frente a las referencias
    evaluation_report(pred)                # informe legible de una ejecución
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

import columns as C  # noqa: E402
import preprocess as pp  # noqa: E402

# --- convenciones del objetivo ------------------------------------------------
CLASES = ("H", "D", "A")          # orden ordinal del dominio: local, empate, visitante
PROB_COLS = ["p_home", "p_draw", "p_away"]
_EPS = 1e-15                      # recorte del log-loss; evita el infinito, no lo maquilla
_BINS = 10                        # deciles de probabilidad para fiabilidad y Brier

# --- referencias disponibles --------------------------------------------------
# Cada una declara de dónde salen sus tres columnas de probabilidad. Las de
# mercado viajan en la salida del protocolo en escala CRUDA, sin escalar ni
# transformar, porque una probabilidad estandarizada ya no es una probabilidad.
#
# Son tres precios y no uno porque «el mercado» no es un objeto único: el consenso
# promedia las casas recogidas, Pinnacle cotiza con el margen más estrecho y refleja
# dinero informado, y Bet365 es la casa minorista de gran difusión, la que el lector
# reconoce. Batir a la minorista y perder contra el dinero informado no sería un
# resultado presentable, así que el trabajo publica el pronóstico contra las tres y
# no elige a posteriori la más cómoda. Sus coberturas difieren —Pinnacle no aparece
# en todas las temporadas de la fuente—, de modo que `compare` restringe a las filas
# comunes a todos los contendientes y esa merma se informa junto a las métricas.
REFERENCIAS = {
    "consenso": pp.MARKET_RAW_COLS,
    "pinnacle": pp.SHARP_RAW_COLS,
    "b365": pp.B365_RAW_COLS,
}


# ===========================================================================
# 1. Extracción y guardas
# ===========================================================================
def _terna(pred: pd.DataFrame, cols: list[str]) -> np.ndarray:
    """Matriz N×3 de probabilidades, validada como tal.

    Se comprueba aquí y no en cada métrica porque una terna que no suma uno
    produce un RPS perfectamente calculable y perfectamente falso.
    """
    faltan = [c for c in cols if c not in pred.columns]
    if faltan:
        raise KeyError(f"faltan las columnas de probabilidad {faltan}")
    P = pred[cols].to_numpy(dtype="float64")
    finitas = np.isfinite(P).all(axis=1)
    if finitas.any():
        val = P[finitas]
        if (val < -1e-9).any() or (val > 1 + 1e-9).any():
            raise ValueError(f"{cols[0][:8]}…: hay probabilidades fuera de [0,1]")
        if not np.allclose(val.sum(axis=1), 1.0, atol=1e-6):
            raise ValueError(f"{cols[0][:8]}…: hay ternas que no suman 1")
    return P


def _objetivo(pred: pd.DataFrame) -> np.ndarray:
    """Vector de resultados como entero 0=local, 1=empate, 2=visitante."""
    if C.TARGET_NUM not in pred.columns:
        raise KeyError(f"falta la columna objetivo '{C.TARGET_NUM}'")
    y = pd.to_numeric(pred[C.TARGET_NUM], errors="coerce").to_numpy()
    if not np.isin(y[np.isfinite(y)], [0, 1, 2]).all():
        raise ValueError("el objetivo contiene valores fuera de {0,1,2}")
    return y.astype("float64")


def _onehot(y: np.ndarray) -> np.ndarray:
    """Codificación indicadora del resultado observado, N×3."""
    O = np.zeros((len(y), 3), dtype="float64")
    ok = np.isfinite(y)
    O[np.flatnonzero(ok), y[ok].astype("int64")] = 1.0
    return O


def evaluable(pred: pd.DataFrame, cols: list[str] = PROB_COLS) -> np.ndarray:
    """Máscara de filas con terna completa y resultado conocido."""
    P = pred[cols].to_numpy(dtype="float64") if all(
        c in pred.columns for c in cols) else np.full((len(pred), 3), np.nan)
    return np.isfinite(P).all(axis=1) & np.isfinite(_objetivo(pred))


# ===========================================================================
# 2. Pérdidas por partido
# ===========================================================================
# Todas devuelven un vector de longitud N. Que sean vectores y no escalares es lo
# que permite promediarlas por temporada, por tramo de confianza o dentro de un
# remuestreo sin reimplementar nada.
def rps_vector(y: np.ndarray, P: np.ndarray) -> np.ndarray:
    """Ranked Probability Score por partido, en [0,1].

    Se acumulan las probabilidades y los indicadores a lo largo del orden
    ordinal H < D < A y se promedia el cuadrado de la diferencia sobre los K−1
    cortes. El último corte no aporta información —ambas acumuladas valen uno—
    y por eso se descarta; dividir entre K−1 deja la métrica en [0,1] y
    comparable con la literatura.
    """
    O = _onehot(y)
    dif = np.cumsum(P, axis=1)[:, :-1] - np.cumsum(O, axis=1)[:, :-1]
    return (dif ** 2).sum(axis=1) / (P.shape[1] - 1)


def logloss_vector(y: np.ndarray, P: np.ndarray) -> np.ndarray:
    """Logaritmo negativo de la probabilidad asignada al resultado ocurrido."""
    idx = np.where(np.isfinite(y), y, 0).astype("int64")
    p = P[np.arange(len(y)), idx]
    return -np.log(np.clip(p, _EPS, 1.0))


def brier_vector(y: np.ndarray, P: np.ndarray) -> np.ndarray:
    """Brier multiclase por partido, en [0,2]."""
    return ((P - _onehot(y)) ** 2).sum(axis=1)


def hit_vector(y: np.ndarray, P: np.ndarray) -> np.ndarray:
    """Indicador de acierto de la clase más probable.

    Los empates de probabilidad se resuelven por el orden H, D, A, que es el
    que impone `argmax`. Es una convención arbitraria pero declarada, y sobre
    probabilidades continuas su efecto es nulo.
    """
    return (P.argmax(axis=1) == np.where(np.isfinite(y), y, -1)).astype("float64")


PERDIDAS = {
    "rps": (rps_vector, "min"),
    "logloss": (logloss_vector, "min"),
    "brier": (brier_vector, "min"),
    "accuracy": (hit_vector, "max"),
}


# ===========================================================================
# 3. Calibración y descomposición
# ===========================================================================
def _bins_uniformes(p: np.ndarray, bins: int) -> np.ndarray:
    """Índice de tramo de cada probabilidad, en tramos de igual anchura."""
    idx = np.floor(np.clip(p, 0.0, 1.0) * bins).astype("int64")
    return np.clip(idx, 0, bins - 1)


def reliability_table(y: np.ndarray, P: np.ndarray, bins: int = _BINS,
                      clase: int | None = None) -> pd.DataFrame:
    """Tabla de fiabilidad: probabilidad declarada frente a frecuencia observada.

    Con `clase` se lee una clase concreta uno-contra-resto; sin ella se lee la
    **confianza**, es decir, la probabilidad de la clase predicha frente a su
    tasa de acierto. La segunda lectura es la que interesa a un usuario de la
    aplicación y la primera la que diagnostica dónde falla la calibración.
    """
    if clase is None:
        p, o = P.max(axis=1), hit_vector(y, P)
        etiqueta = "confianza"
    else:
        p, o = P[:, clase], (y == clase).astype("float64")
        etiqueta = CLASES[clase]

    tramo = _bins_uniformes(p, bins)
    filas = []
    for b in range(bins):
        m = tramo == b
        n = int(m.sum())
        filas.append({
            "clase": etiqueta,
            "tramo": f"[{b / bins:.1f},{(b + 1) / bins:.1f})",
            "n": n,
            "p_media": float(p[m].mean()) if n else np.nan,
            "frec_obs": float(o[m].mean()) if n else np.nan,
            "brecha": float(o[m].mean() - p[m].mean()) if n else np.nan,
        })
    return pd.DataFrame(filas)


def ece(y: np.ndarray, P: np.ndarray, bins: int = _BINS,
        clase: int | None = None) -> float:
    """Error de calibración esperado: brecha media, ponderada por ocupación.

    Resume la tabla de fiabilidad en un número. Ponderar por ocupación evita que
    un tramo con tres partidos pese lo mismo que uno con seiscientos, que es el
    defecto por el que un ECE sin ponderar suele parecer peor de lo que es.
    """
    tab = reliability_table(y, P, bins=bins, clase=clase)
    ocupados = tab[tab.n > 0]
    if ocupados.empty:
        return float("nan")
    peso = ocupados.n.to_numpy(dtype="float64")
    return float((peso * ocupados.brecha.abs().to_numpy()).sum() / peso.sum())


def brier_decomposition(y: np.ndarray, P: np.ndarray,
                        bins: int = _BINS) -> dict:
    """Descomposición de Murphy del Brier multiclase.

        BS = fiabilidad − resolución + incertidumbre + residuo

    La **fiabilidad** mide cuánto se aparta la frecuencia observada de la
    probabilidad declarada dentro de cada tramo: es el término que se corrige
    calibrando y el único de los tres que conviene que sea cero. La
    **resolución** mide cuánto se separan los tramos entre sí: es la señal
    genuina, y cuanto mayor mejor. La **incertidumbre** es la varianza del
    propio resultado y no depende del modelo; fija el suelo del problema.

    El cuarto término es el precio de agrupar. La identidad de Murphy se enuncia
    casi siempre con tres piezas porque se demuestra sobre un pronosticador que
    emite un número finito de probabilidades distintas: entonces cada tramo
    contiene un único valor declarado y el reparto es exacto. Un modelo continuo
    no cumple esa condición —dentro de un tramo conviven probabilidades que solo
    se parecen— y sustituir cada una por la media del tramo deja un resto:

        residuo = (1/n) Σ_b Σ_{i∈b} [ (p_i − p̄_b)² − 2 (p_i − p̄_b)(o_i − ō_b) ]

    Se devuelve explícitamente en lugar de esconderlo porque su tamaño se lee: es
    la parte del Brier que la lectura por tramos no explica, y encoge conforme los
    tramos se afinan. Declararlo es además lo que permite comprobar la identidad
    como igualdad exacta y no como aproximación, que es la única forma de que la
    comprobación sirva para detectar un error de cálculo.

    La descomposición se calcula clase a clase y se suma, que es la extensión
    natural del Brier multiclase como suma de tres Brier binarios.
    """
    rel = res = unc = resto = 0.0
    n = len(y)
    for k in range(P.shape[1]):
        p, o = P[:, k], (y == k).astype("float64")
        base = o.mean()
        unc += base * (1.0 - base)
        tramo = _bins_uniformes(p, bins)
        for b in range(bins):
            m = tramo == b
            nb = int(m.sum())
            if not nb:
                continue
            pb, ob = p[m].mean(), o[m].mean()
            rel += nb * (pb - ob) ** 2 / n
            res += nb * (ob - base) ** 2 / n
            d = p[m] - pb
            resto += float((d * d).sum() - 2.0 * (d * (o[m] - ob)).sum()) / n
    return {"brier": float(brier_vector(y, P).mean()),
            "fiabilidad": float(rel), "resolucion": float(res),
            "incertidumbre": float(unc), "residuo": float(resto)}


# ===========================================================================
# 4. Clasificación
# ===========================================================================
def confusion_matrix(y: np.ndarray, P: np.ndarray) -> pd.DataFrame:
    """Matriz de confusión con las clases en el orden del dominio."""
    yhat = P.argmax(axis=1)
    M = np.zeros((3, 3), dtype="int64")
    for real, pred_ in zip(y.astype("int64"), yhat):
        M[real, pred_] += 1
    return pd.DataFrame(M, index=[f"real_{c}" for c in CLASES],
                        columns=[f"pred_{c}" for c in CLASES])


def classification_report(y: np.ndarray, P: np.ndarray) -> pd.DataFrame:
    """Precisión, exhaustividad y F1 por clase, con promedios macro y ponderado.

    El promedio **macro** trata las tres clases por igual y es el que expone el
    problema del empate; el **ponderado** las pesa por frecuencia y siempre sale
    mejor. Reportar solo el segundo oculta precisamente lo que hay que discutir.
    """
    yhat = P.argmax(axis=1)
    filas = []
    for k, nombre in enumerate(CLASES):
        tp = int(((yhat == k) & (y == k)).sum())
        fp = int(((yhat == k) & (y != k)).sum())
        fn = int(((yhat != k) & (y == k)).sum())
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        filas.append({"clase": nombre, "precision": prec, "exhaustividad": rec,
                      "f1": f1, "soporte": int((y == k).sum())})
    rep = pd.DataFrame(filas)
    soporte = rep.soporte.to_numpy(dtype="float64")
    for nombre, peso in (("macro", np.ones(3) / 3),
                         ("ponderado", soporte / max(soporte.sum(), 1))):
        rep.loc[len(rep)] = {
            "clase": nombre,
            "precision": float(rep.precision[:3] @ peso),
            "exhaustividad": float(rep.exhaustividad[:3] @ peso),
            "f1": float(rep.f1[:3] @ peso),
            "soporte": int(soporte.sum()),
        }
    return rep


def _auc_binaria(o: np.ndarray, s: np.ndarray) -> float:
    """AUC por el estadístico de rangos, con empates resueltos por rango medio.

    Equivale a la probabilidad de que un positivo tomado al azar reciba mayor
    puntuación que un negativo tomado al azar. La formulación por rangos es
    exacta y no depende de un umbral, a diferencia de integrar una curva
    muestreada.
    """
    n_pos, n_neg = int(o.sum()), int((1 - o).sum())
    if not n_pos or not n_neg:
        return float("nan")
    orden = np.argsort(s, kind="mergesort")
    rangos = np.empty(len(s), dtype="float64")
    rangos[orden] = np.arange(1, len(s) + 1, dtype="float64")
    s_ord = s[orden]
    i = 0
    while i < len(s_ord):                      # rango medio dentro de cada empate
        j = i
        while j + 1 < len(s_ord) and s_ord[j + 1] == s_ord[i]:
            j += 1
        if j > i:
            rangos[orden[i:j + 1]] = (i + j + 2) / 2.0
        i = j + 1
    return float((rangos[o == 1].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def roc_auc_ovr(y: np.ndarray, P: np.ndarray) -> dict:
    """ROC-AUC uno-contra-resto por clase, con promedios macro y micro.

    El **macro** promedia las tres AUC y el **micro** agrupa los N×3 pares
    (indicador, probabilidad) en un solo problema binario. Difieren cuando las
    clases están desbalanceadas, que es siempre en este dominio.
    """
    por_clase = {CLASES[k]: _auc_binaria((y == k).astype("float64"), P[:, k])
                 for k in range(3)}
    micro = _auc_binaria(_onehot(y).ravel(), P.ravel())
    validas = [v for v in por_clase.values() if np.isfinite(v)]
    return {**{f"auc_{k}": v for k, v in por_clase.items()},
            "auc_macro": float(np.mean(validas)) if validas else float("nan"),
            "auc_micro": micro}


# ===========================================================================
# 5. Referencias
# ===========================================================================
def reference_probs(pred: pd.DataFrame, cual: str) -> np.ndarray | None:
    """Terna de una referencia, o `None` si el artefacto no la trae.

    `consenso`, `pinnacle` y `b365` se leen de las columnas crudas que el
    protocolo arrastra desde el maestro. `prior` usa la distribución de clases
    del conjunto evaluado y `local` da toda la masa a la victoria local.

    Las tres lecturas del precio no cubren las mismas filas: el consenso existe
    siempre que haya cuotas, Pinnacle falta en parte del histórico y Bet365
    tiene sus propios huecos. Devolver `None` cuando el artefacto no trae una de
    ellas es deliberado —el contendiente desaparece del cuadro en lugar de
    contaminarlo—, pero cuando sí está, la regla de fila común de `compare`
    recorta la muestra al mínimo de las tres. Ese recorte se informa junto a las
    métricas: comparar sobre coberturas distintas mediría la dificultad de los
    partidos que cada precio cubre y no la calidad del pronóstico.

    El `prior` merece una advertencia que conviene no perder de vista: al
    calcularse sobre el propio conjunto evaluado es ligeramente optimista, y
    solo es admisible porque su papel es fijar un suelo. Si el modelo no bate
    una referencia que hace trampas a su favor, la conclusión es más clara aún.

    El `local` es degenerado a propósito: da probabilidad uno a la victoria
    local y cero a las otras dos, de modo que su log-loss se dispara hasta el
    recorte numérico en cuanto falla un solo partido. Esa cifra no es un
    artefacto que haya que ocultar, sino la lectura correcta de una regla de
    clasificación medida como si fuera un pronóstico probabilístico: su
    `accuracy` es informativa y su log-loss demuestra por qué el producto
    publica probabilidades y no etiquetas.
    """
    if cual in REFERENCIAS:
        cols = REFERENCIAS[cual]
        if not all(c in pred.columns for c in cols):
            return None
        return _terna(pred, cols)
    if cual == "prior":
        y = _objetivo(pred)
        ok = np.isfinite(y)
        if not ok.any():
            return None
        base = np.array([(y[ok] == k).mean() for k in range(3)], dtype="float64")
        return np.tile(base, (len(pred), 1))
    if cual == "local":
        P = np.zeros((len(pred), 3), dtype="float64")
        P[:, 0] = 1.0
        return P
    raise ValueError(f"referencia desconocida: {cual!r}. Disponibles: "
                     f"{sorted(set(REFERENCIAS) | {'prior', 'local'})}")


def _mascara_comun(pred: pd.DataFrame, ternas: dict) -> np.ndarray:
    """Filas con resultado conocido y terna completa en TODOS los contendientes."""
    m = np.isfinite(_objetivo(pred))
    for P in ternas.values():
        m &= np.isfinite(P).all(axis=1)
    return m


# ===========================================================================
# 6. Batería completa
# ===========================================================================
def metrics(pred: pd.DataFrame, cols: list[str] = PROB_COLS, *,
            bins: int = _BINS, mask: np.ndarray | None = None,
            completo: bool = True) -> dict:
    """Batería de métricas de una terna sobre un conjunto de partidos.

    Con `completo=False` se calculan solo las cuatro métricas promediables, que
    es lo que necesita un remuestreo y evita pagar el coste de la calibración y
    de las AUC mil veces.
    """
    P = _terna(pred, cols)
    y = _objetivo(pred)
    m = np.isfinite(P).all(axis=1) & np.isfinite(y) if mask is None else mask
    if not m.any():
        raise ValueError("no queda ningún partido evaluable con esas columnas")
    P, y = P[m], y[m]

    out = {"n": int(m.sum())}
    for nombre, (fn, _) in PERDIDAS.items():
        out[nombre] = float(fn(y, P).mean())
    if not completo:
        return out

    out.update(brier_decomposition(y, P, bins=bins))
    out["ece_confianza"] = ece(y, P, bins=bins)
    for k, nombre in enumerate(CLASES):
        out[f"ece_{nombre}"] = ece(y, P, bins=bins, clase=k)
    out.update(roc_auc_ovr(y, P))
    rep = classification_report(y, P)
    out["f1_macro"] = float(rep.loc[rep.clase == "macro", "f1"].iloc[0])
    out["f1_D"] = float(rep.loc[rep.clase == "D", "f1"].iloc[0])
    out["pred_D"] = float((P.argmax(axis=1) == 1).mean())
    return out


def skill(metricas: dict, referencia: dict) -> dict:
    """Habilidad: mejora relativa frente a una referencia, métrica a métrica.

    Para una métrica de pérdida la habilidad es `1 − m/ref`, positiva cuando el
    modelo pierde menos; para una de acierto es `m/ref − 1`. En ambos casos cero
    significa empate y el signo se lee igual, que es lo que permite ponerlas en
    la misma tabla sin que el lector tenga que recordar la orientación de cada
    una.
    """
    out = {}
    for nombre, (_, orientacion) in PERDIDAS.items():
        m, r = metricas.get(nombre), referencia.get(nombre)
        if m is None or r is None or not np.isfinite(r) or abs(r) < 1e-12:
            continue
        out[f"skill_{nombre}"] = float(1 - m / r) if orientacion == "min" \
            else float(m / r - 1)
    return out


def compare(pred: pd.DataFrame, cols: list[str] = PROB_COLS, *,
            referencias: tuple[str, ...] = ("consenso", "pinnacle", "b365",
                                            "prior", "local"),
            bins: int = _BINS) -> pd.DataFrame:
    """Modelo y referencias sobre EXACTAMENTE los mismos partidos.

    Las referencias que el artefacto no puede construir se omiten con una nota
    en `attrs`, no se rellenan con huecos: una fila vacía en una tabla de
    comparación se lee como un mal resultado y es otra cosa.

    La restricción a la intersección tiene un coste que hay que declarar: si
    Pinnacle solo cubre parte de la muestra, exigir su presencia reduce el
    conjunto para todos. Por eso `attrs["cobertura"]` guarda cuántas filas se
    perdieron y por culpa de quién.

    Los tres precios entran por defecto porque la pregunta del trabajo no es si
    el modelo bate a «el mercado» sino a cuál de sus lecturas: el consenso de
    casas, el dinero informado de Pinnacle y la casa minorista Bet365, que es la
    que el lector reconoce y con la que compararía el pronóstico. Pedirlas todas
    a la vez es también lo que impide quedarse con la más favorable después de
    ver el resultado.
    """
    ternas = {"modelo": _terna(pred, cols)}
    ausentes = []
    for r in referencias:
        P = reference_probs(pred, r)
        if P is None:
            ausentes.append(r)
        else:
            ternas[r] = P

    m = _mascara_comun(pred, ternas)
    if not m.any():
        raise ValueError("la intersección de filas evaluables está vacía; revise "
                         "la cobertura de las referencias solicitadas")

    filas = []
    for nombre, P in ternas.items():
        aux = pred.loc[:, []].copy()
        for c, col in zip(range(3), PROB_COLS):
            aux[col] = P[:, c]
        aux[C.TARGET_NUM] = pred[C.TARGET_NUM].to_numpy()
        filas.append({"contendiente": nombre,
                      **metrics(aux, PROB_COLS, bins=bins, mask=m)})

    tab = pd.DataFrame(filas).set_index("contendiente")
    ref = tab.loc["consenso"] if "consenso" in tab.index else None
    if ref is not None:
        for k, v in skill(tab.loc["modelo"].to_dict(), ref.to_dict()).items():
            tab.loc["modelo", k] = v
    tab.attrs["referencias_ausentes"] = ausentes
    tab.attrs["cobertura"] = {"filas": len(pred), "comunes": int(m.sum())}
    return tab


def metrics_by_season(pred: pd.DataFrame, cols: list[str] = PROB_COLS, *,
                      bins: int = _BINS) -> pd.DataFrame:
    """La batería, temporada a temporada.

    Es la lectura que alimenta la página de métricas de la aplicación y la que
    revela si el rendimiento se sostiene o se apoya en una temporada afortunada.
    """
    filas = []
    for temporada, sub in pred.groupby("season", sort=True):
        try:
            filas.append({"season": temporada,
                          **metrics(sub, cols, bins=bins)})
        except ValueError:                     # temporada sin filas evaluables
            continue
    return pd.DataFrame(filas).set_index("season")


# ===========================================================================
# 7. Desacuerdo con el mercado
# ===========================================================================
def disagreement(pred: pd.DataFrame, cols: list[str] = PROB_COLS, *,
                 referencia: str = "consenso", n_grupos: int = 5) -> pd.DataFrame:
    """Quién acierta más según cuánto discrepen modelo y mercado.

    La distancia es la variación total entre las dos ternas, `½·Σ|p−q|`, que
    vale cero cuando coinciden y uno cuando son incompatibles. Los partidos se
    agrupan en cuantiles de esa distancia.

    La lectura es directa y es la que responde a la pregunta que de verdad
    importa. Si el modelo solo mejora al precio en el grupo de menor distancia,
    está reproduciéndolo con ruido y no aporta información; si mejora en el de
    mayor distancia, está viendo algo que el precio no incorpora. Un modelo que
    empeora justo donde más discrepa es el patrón clásico del sobreajuste.
    """
    Q = reference_probs(pred, referencia)
    if Q is None:
        raise ValueError(f"el artefacto no trae la referencia {referencia!r}")
    P = _terna(pred, cols)
    y = _objetivo(pred)
    m = np.isfinite(P).all(axis=1) & np.isfinite(Q).all(axis=1) & np.isfinite(y)
    P, Q, y = P[m], Q[m], y[m]

    d = 0.5 * np.abs(P - Q).sum(axis=1)
    # `duplicates="drop"` es necesario: si muchos partidos comparten distancia
    # —típico cuando el modelo replica el precio— varios cortes coinciden. En el
    # límite de esa misma situación, cuando la distancia es una única constante,
    # no queda ningún corte y el reparto en cuantiles no está definido; entonces
    # el análisis se reduce a su lectura correcta, que es un solo grupo con todos
    # los partidos, en lugar de a una tabla vacía.
    grupo = pd.qcut(d, q=n_grupos, labels=False, duplicates="drop")
    grupo = np.asarray(grupo, dtype=float)
    if not np.isfinite(grupo).any():
        grupo = np.zeros(len(d))

    filas = []
    for g in sorted(pd.unique(grupo[~pd.isna(grupo)])):
        s = grupo == g
        filas.append({
            "grupo": int(g) + 1,
            "n": int(s.sum()),
            "dist_media": float(d[s].mean()),
            "rps_modelo": float(rps_vector(y[s], P[s]).mean()),
            "rps_mercado": float(rps_vector(y[s], Q[s]).mean()),
            "acc_modelo": float(hit_vector(y[s], P[s]).mean()),
            "acc_mercado": float(hit_vector(y[s], Q[s]).mean()),
        })
    tab = pd.DataFrame(filas)
    tab["skill_rps"] = 1 - tab.rps_modelo / tab.rps_mercado
    return tab


# ===========================================================================
# 8. Incertidumbre
# ===========================================================================
def _bloques(pred: pd.DataFrame) -> np.ndarray:
    """Identificador de bloque de remuestreo: la jornada dentro de su temporada."""
    if "jornada" in pred.columns and "season" in pred.columns:
        return (pred.season.astype(str) + "|" + pred.jornada.astype(str)).to_numpy()
    if "fold" in pred.columns:
        return pred.fold.to_numpy()
    raise KeyError("no hay ni (season, jornada) ni fold para formar bloques")


def block_bootstrap(pred: pd.DataFrame, fn, *, n_boot: int = 1000, seed: int = 7,
                    nivel: float = 0.95) -> dict:
    """Intervalo por remuestreo de bloques de jornada.

    Se remuestrean **jornadas completas** con reemplazo, tantas como haya, y se
    recalcula `fn` sobre la concatenación. Remuestrear partidos sueltos daría un
    intervalo más estrecho y más falso: los partidos de una misma jornada
    comparten el estado del mundo de esa semana y no son observaciones
    independientes.

    Se devuelve el percentil, que no corrige el sesgo del estimador pero es
    transparente y no depende de supuestos que aquí no se pueden verificar.
    """
    bloques = _bloques(pred)
    unicos = pd.unique(bloques)
    indices = {b: np.flatnonzero(bloques == b) for b in unicos}
    rng = np.random.default_rng(seed)

    muestras = []
    for _ in range(n_boot):
        elegidos = rng.choice(unicos, size=len(unicos), replace=True)
        filas = np.concatenate([indices[b] for b in elegidos])
        try:
            muestras.append(float(fn(pred.iloc[filas])))
        except (ValueError, ZeroDivisionError):   # réplica sin filas evaluables
            continue
    if not muestras:
        raise ValueError("ninguna réplica del remuestreo ha sido evaluable")

    a = (1 - nivel) / 2
    v = np.asarray(muestras, dtype="float64")
    return {"estimacion": float(fn(pred)), "inf": float(np.quantile(v, a)),
            "sup": float(np.quantile(v, 1 - a)), "n_replicas": len(v),
            "ee": float(v.std(ddof=1))}


def bootstrap_metrics(pred: pd.DataFrame, cols: list[str] = PROB_COLS, *,
                      metricas: tuple[str, ...] = ("rps", "logloss", "accuracy"),
                      n_boot: int = 1000, seed: int = 7,
                      nivel: float = 0.95) -> pd.DataFrame:
    """Las métricas promediables con su intervalo, en una tabla publicable."""
    filas = []
    for nombre in metricas:
        if nombre not in PERDIDAS:
            raise ValueError(f"métrica no promediable: {nombre!r}")

        def fn(sub, _n=nombre):
            return metrics(sub, cols, completo=False)[_n]

        filas.append({"metrica": nombre,
                      **block_bootstrap(pred, fn, n_boot=n_boot, seed=seed,
                                        nivel=nivel)})
    return pd.DataFrame(filas).set_index("metrica")


def permutation_test(pred: pd.DataFrame, cols_a: list[str], cols_b: list[str], *,
                     metrica: str = "rps", n_perm: int = 2000,
                     seed: int = 7) -> dict:
    """¿Es real la diferencia entre dos ternas medidas sobre los mismos partidos?

    El contraste es **pareado y por bloques**: se calcula la diferencia de
    pérdida partido a partido, se promedia dentro de cada jornada y se permuta
    el **signo** de esos promedios. La hipótesis nula es que ninguna de las dos
    ternas es sistemáticamente mejor, en cuyo caso el signo de la diferencia de
    una jornada es intercambiable.

    Permutar signos por jornada y no por partido es lo que respeta la
    dependencia temporal: dentro de una jornada los errores están correlacionados
    —un fin de semana de resultados sorprendentes castiga a los dos
    contendientes a la vez— y tratarlos como independientes produciría un valor
    p tan pequeño como se quiera.
    """
    if metrica not in PERDIDAS:
        raise ValueError(f"métrica no admisible en el contraste: {metrica!r}")
    fn, orientacion = PERDIDAS[metrica]

    A, B = _terna(pred, cols_a), _terna(pred, cols_b)
    y = _objetivo(pred)
    m = np.isfinite(A).all(axis=1) & np.isfinite(B).all(axis=1) & np.isfinite(y)
    if not m.any():
        raise ValueError("no hay partidos comunes a las dos ternas")

    d = fn(y[m], A[m]) - fn(y[m], B[m])        # negativo = A mejor si es pérdida
    if orientacion == "max":
        d = -d
    por_bloque = pd.Series(d).groupby(_bloques(pred)[m]).mean().to_numpy()

    obs = float(por_bloque.mean())
    rng = np.random.default_rng(seed)
    signos = rng.choice([-1.0, 1.0], size=(n_perm, len(por_bloque)))
    nulos = (signos * por_bloque).mean(axis=1)
    # El +1 en numerador y denominador incluye la muestra observada entre las
    # posibles: sin él un valor p puede salir exactamente cero, que no es un
    # resultado admisible en un contraste de aleatorización.
    p_valor = float((np.abs(nulos) >= abs(obs)).sum() + 1) / (n_perm + 1)
    return {"metrica": metrica, "diferencia": obs, "p_valor": p_valor,
            "n_bloques": len(por_bloque), "n_partidos": int(m.sum()),
            "mejor": "a" if obs < 0 else ("b" if obs > 0 else "empate")}


# ===========================================================================
# 9. Informe
# ===========================================================================
def evaluation_report(pred: pd.DataFrame, cols: list[str] = PROB_COLS, *,
                      n_boot: int = 500, verbose: bool = True) -> dict:
    """Informe legible de una ejecución del protocolo.

    Reúne la comparación con las referencias, el intervalo de las métricas
    principales, el desglose por temporada y el análisis de desacuerdo. Es lo
    que el banco manual muestra por cada conjunto de variables que se prueba.
    """
    tab = compare(pred, cols)
    boot = bootstrap_metrics(pred, cols, n_boot=n_boot)
    por_temporada = metrics_by_season(pred, cols)
    try:
        desacuerdo = disagreement(pred, cols)
    except ValueError:
        desacuerdo = pd.DataFrame()

    m = evaluable(pred, cols)
    P, y = _terna(pred, cols)[m], _objetivo(pred)[m]
    res = {"comparacion": tab, "intervalos": boot,
           "por_temporada": por_temporada, "desacuerdo": desacuerdo,
           "confusion": confusion_matrix(y, P),
           "clasificacion": classification_report(y, P),
           "fiabilidad": pd.concat(
               [reliability_table(y, P)]
               + [reliability_table(y, P, clase=k) for k in range(3)],
               ignore_index=True)}

    if verbose:
        print("=" * 78)
        print(f"EVALUACIÓN — {len(pred)} partidos, "
              f"{tab.attrs['cobertura']['comunes']} comunes a todos los contendientes")
        if tab.attrs["referencias_ausentes"]:
            print(f"  referencias no disponibles en el artefacto: "
                  f"{', '.join(tab.attrs['referencias_ausentes'])}")
        print("-" * 78)
        vista = [c for c in ("n", "rps", "logloss", "brier", "accuracy", "f1_D",
                             "auc_macro", "ece_confianza") if c in tab.columns]
        print(tab[vista].round(4).to_string())
        if "skill_rps" in tab.columns:
            print(f"\n  habilidad frente al consenso: "
                  f"RPS {tab.loc['modelo', 'skill_rps']:+.3%}   "
                  f"log-loss {tab.loc['modelo', 'skill_logloss']:+.3%}")
        print("-" * 78)
        print("Intervalos al 95 % (remuestreo por bloques de jornada):")
        for nombre, fila in boot.iterrows():
            print(f"  {nombre:<10} {fila.estimacion:.4f}  "
                  f"[{fila.inf:.4f}, {fila.sup:.4f}]")
        print("-" * 78)
        print("Matriz de confusión y clase minoritaria:")
        print(res["confusion"].to_string())
        print(res["clasificacion"].round(4).to_string(index=False))
        if not desacuerdo.empty:
            print("-" * 78)
            print("Desacuerdo con el mercado (grupo 1 = más coincidencia):")
            print(desacuerdo.round(4).to_string(index=False))
        print("=" * 78)
    return res
