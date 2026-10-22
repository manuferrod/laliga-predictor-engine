"""Banco de pruebas de `estimators.py`, sin red y sin artefactos.

Un estimador roto rara vez falla. Devuelve una terna que suma uno, ordena los
partidos de forma plausible y produce una metrica del orden esperado; lo que se
rompe es algo interno que ninguna metrica global delata. Este banco ataca
precisamente eso: comprueba que el peso se normaliza, que el tramo reservado esta
DESPUES en el calendario y no mezclado, que el piso publicable impide el cero
exacto de una hoja vacia, que la red elige sus epocas mirando el futuro de su
ventana y no un tramo al azar, y que lo construible y lo declarado son el mismo
conjunto.

Los datos son sinteticos y los valores esperados estan calculados a mano o contra
una relacion plantada a proposito, de modo que cada comprobacion tiene una
respuesta correcta conocida antes de ejecutar nada.

    1. FIRMA COMUN            toda fabrica se construye y se usa igual
    2. PESOS                  normalizacion y guardas del peso de recencia
    3. CORTE TEMPORAL         el tramo reservado es el final por fecha
    4. LOGISTICA              el resolutor que entra segun `l1_ratio`
    5. PISO PUBLICABLE        ninguna familia de arboles publica un cero exacto
    6. RED                    epocas elegidas contra el futuro de la ventana
    7. MEZCLA                 el reparto por prefijo y la combinacion en el logit
    8. REGISTRO Y ESPACIOS    lo construible y lo declarado coinciden
    9. INTEGRACION            las familias a traves del protocolo
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

import estimators as es  # noqa: E402
import evaluate as ev  # noqa: E402
import explain as ex  # noqa: E402
import laboratorio as lb  # noqa: E402
import preprocess as pp  # noqa: E402
import protocol as pt  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    if cond:
        PASS.append(msg)
        print(f"  [OK ] {msg}")
    else:
        FAIL.append(msg)
        print(f"  [FAIL] {msg}")


def _cerca(a, b, tol: float = 1e-9) -> bool:
    return bool(np.allclose(np.asarray(a, dtype="float64"),
                            np.asarray(b, dtype="float64"),
                            rtol=0.0, atol=tol, equal_nan=True))


# ---------------------------------------------------------------------------
# material sintetico
# ---------------------------------------------------------------------------
def _ctx(n: int, dias: int = 30, mercado=None, sharp=None) -> pt.Contexto:
    """Contexto con `n` filas repartidas en `dias` fechas consecutivas."""
    base = pd.Timestamp("2021-08-20")
    fechas = pd.Series([base + pd.Timedelta(days=int(i * dias / n))
                        for i in range(n)])
    return pt.Contexto(fechas=fechas, mercado=mercado, mercado_sharp=sharp)


def _tabla(n: int = 600, semilla: int = 7, senal: float = 1.2):
    """Matriz con una senal plantada y el objetivo generado desde ella.

    `x_senal` empuja hacia la victoria local y `x_ruido` no empuja hacia nada. Un
    modelo que funcione debe encontrar la primera y no la segunda.
    """
    rng = np.random.default_rng(semilla)
    x1 = rng.normal(0.0, 1.0, n)
    x2 = rng.normal(0.0, 1.0, n)
    Z = np.stack([senal * x1, np.zeros(n), -senal * x1], axis=1)
    P = np.exp(Z) / np.exp(Z).sum(axis=1, keepdims=True)
    y = np.array([rng.choice(3, p=p) for p in P])
    X = pd.DataFrame({"x_senal": x1, "x_ruido": x2})
    return X, y, P


def _liga_protocolo() -> pd.DataFrame:
    """Liga sintetica completa, con precio de mercado."""
    tp = __import__("test_protocol")
    return tp._liga(semilla=11)


class _Constante:
    """Estimador minimo: publica la frecuencia de clases de su ventana.

    No pertenece al modulo —lo construible son las familias de `lb.FAMILIAS`— pero
    el banco necesita algo trivial y de coste nulo con que recorrer el protocolo
    cuando lo que se comprueba es el canal y no el modelo.
    """

    def __init__(self):
        self.classes_ = np.array([0, 1, 2])

    def fit(self, X, y, sample_weight=None):
        y = np.asarray(y).astype("int64")
        w = np.ones(len(y)) if sample_weight is None else np.asarray(sample_weight)
        self.previa_ = np.array([w[y == c].sum() for c in (0, 1, 2)])
        self.previa_ = self.previa_ / self.previa_.sum()
        return self

    def predict_proba(self, X):
        return np.tile(self.previa_, (len(X), 1))


# ===========================================================================
# 1. Firma comun
# ===========================================================================
def test_firma() -> None:
    print("\n1. FIRMA COMUN")

    X, y, _ = _tabla(n=300)
    ctx = _ctx(300)

    fabricas = {
        "logistica": es.logistica(C=1.0),
        "bosque": es.bosque(n_estimators=30, min_samples_leaf=5),
        "red": es.red(capas=(8,), max_epocas=20, paso=10, paciencia=10),
    }
    check(sorted(es.CONSTRUCTORES) == sorted(lb.FAMILIAS),
          "el modulo construye exactamente las familias que declara el banco manual")
    check(all(callable(f) for f in fabricas.values()),
          "toda fabrica es un invocable sin argumentos")
    a, b = fabricas["logistica"](), fabricas["logistica"]()
    check(a is not b, "cada llamada devuelve un estimador NUEVO, no uno compartido")

    # Las familias que no reservan tramo se entrenan con la firma minima.
    for nombre in ("logistica", "bosque"):
        m = fabricas[nombre]()
        m.fit(X, y)
        P = np.asarray(m.predict_proba(X), dtype="float64")
        check(P.shape == (len(X), 3), f"{nombre}: publica una terna por partido")
        check(_cerca(P.sum(axis=1), 1.0, tol=1e-9),
              f"{nombre}: sus tres probabilidades suman 1")
        check(bool((P >= 0).all()), f"{nombre}: ninguna probabilidad es negativa")
        check(hasattr(m, "classes_"), f"{nombre}: declara el orden de sus clases")
        check(pt._acepta(m.fit, "sample_weight"),
              f"{nombre}: admite el peso de recencia")

    # La que reserva tramo declara `contexto` en la firma; el protocolo lo detecta
    # por inspeccion, asi que la firma es el contrato.
    r = fabricas["red"]()
    check(pt._acepta(r.fit, "contexto"),
          "red: declara `contexto` en fit, que es como el protocolo ve la reserva")
    check(not pt._acepta(fabricas["logistica"]().fit, "contexto"),
          "y la logistica no lo declara, de modo que el protocolo no se lo pasa")
    r.fit(X, y, contexto=ctx)
    P = np.asarray(r.predict_proba(X), dtype="float64")
    check(_cerca(P.sum(axis=1), 1.0, tol=1e-9),
          "red: sus tres probabilidades suman 1")

    # La familia de dependencia opcional falla al construir, no al importar: el
    # modulo tiene que poder cargarse en un entorno sin xgboost.
    try:
        es.refuerzo_xgb()()
        check(True, "refuerzo_xgb: la dependencia esta y la familia se construye")
    except ImportError as e:
        check("xgboost" in str(e),
              "refuerzo_xgb: sin la dependencia el fallo dice exactamente que falta")


# ===========================================================================
# 2. Pesos
# ===========================================================================
def test_pesos() -> None:
    print("\n2. PESOS")

    check(_cerca(es._pesos(None, 4), [1, 1, 1, 1]),
          "sin peso declarado la muestra pesa uno por partido")
    w = es._pesos(np.array([1.0, 2.0, 3.0, 4.0]), 4)
    check(_cerca(w.mean(), 1.0),
          "un peso declarado se normaliza a media uno")
    check(_cerca(w, np.array([0.4, 0.8, 1.2, 1.6])),
          "y conserva las proporciones relativas entre partidos")
    check(_cerca(es._pesos(np.full(4, 7.0), 4), 1.0),
          "un peso constante equivale a no declarar peso, sea cual sea su nivel")

    for mal, texto, etiqueta in [
        (np.array([1.0, -1.0, 1.0, 1.0]), "negativos", "un peso negativo se detiene"),
        (np.zeros(4), "suman cero", "un peso que suma cero se detiene"),
        (np.ones(3), "elementos", "un peso de longitud distinta a la muestra se detiene"),
    ]:
        try:
            es._pesos(mal, 4)
            check(False, etiqueta)
        except ValueError as e:
            check(texto in str(e), etiqueta)

    # La normalizacion no es cosmetica: sin ella la regularizacion cambiaria de
    # fuerza efectiva al cambiar la escala del peso, sin que nadie tocase el
    # hiperparametro.
    X, y, _ = _tabla(n=400)
    a = _Constante().fit(X, y, sample_weight=np.ones(400))
    b = _Constante().fit(X, y, sample_weight=np.full(400, 5.0))
    check(_cerca(a.previa_, b.previa_),
          "multiplicar todos los pesos por una constante no cambia el ajuste")

    # Y el objetivo se valida: una etiqueta fuera de {0,1,2} no llega al modelo.
    try:
        es._objetivo(np.array([0, 1, 3]))
        check(False, "una etiqueta fuera de {0,1,2} se detiene")
    except ValueError as e:
        check("fuera de" in str(e), "una etiqueta fuera de {0,1,2} se detiene")


# ===========================================================================
# 3. Corte temporal
# ===========================================================================
def test_corte() -> None:
    print("\n3. CORTE TEMPORAL")

    n = 500
    ctx = _ctx(n, dias=50)
    reserva = es._corte_temporal(ctx, n, frac=0.2, minimo=50)
    fechas = pd.to_datetime(ctx.fechas)

    check(int(reserva.sum()) >= 100,
          "el tramo reservado cubre al menos la fraccion pedida")
    check(int(reserva.sum()) < n, "y deja entrenamiento por delante")
    check(fechas[~reserva].max() < fechas[reserva].min(),
          "TODA fila reservada es posterior a TODA fila de ajuste")

    # La propiedad delicada: ninguna fecha aparece a la vez en los dos lados. Si
    # apareciese, el modelo habria visto el resultado del sabado al decidir sobre
    # el domingo, que es en pequeno la misma fuga que el protocolo evita en grande.
    comunes = set(fechas[reserva]) & set(fechas[~reserva])
    check(not comunes, "ninguna fecha queda partida entre ajuste y reserva")

    # Una fraccion mayor reserva mas, y sigue siendo un corte por fecha.
    r2 = es._corte_temporal(ctx, n, frac=0.4, minimo=50)
    check(int(r2.sum()) > int(reserva.sum()),
          "pedir mas fraccion reserva mas partidos")
    check(bool((reserva <= r2).all()),
          "y el tramo mayor contiene al menor: el corte solo se mueve hacia atras")

    try:
        es._corte_temporal(None, n)
        check(False, "sin contexto el corte se detiene con error explicito")
    except ValueError as e:
        check("fechas del pliegue" in str(e),
              "sin contexto el corte se detiene con error explicito")

    try:
        es._corte_temporal(_ctx(30, dias=10), 30, frac=0.2, minimo=200)
        check(False, "una ventana demasiado corta para la reserva se detiene")
    except ValueError as e:
        check("tramo reservado" in str(e),
              "una ventana demasiado corta para la reserva se detiene")

    plano = pt.Contexto(fechas=pd.Series([pd.Timestamp("2021-01-01")] * 300))
    try:
        es._corte_temporal(plano, 300, frac=0.2, minimo=10)
        check(False, "una ventana con una sola fecha se detiene")
    except ValueError as e:
        check("comparten fecha" in str(e),
              "una ventana con una sola fecha se detiene")

    try:
        es._corte_temporal(_ctx(100), 250)
        check(False, "un contexto de longitud distinta a la ventana se detiene")
    except ValueError as e:
        check("fechas" in str(e),
              "un contexto de longitud distinta a la ventana se detiene")


# ===========================================================================
# 4. Logistica
# ===========================================================================
def test_logistica() -> None:
    print("\n4. LOGISTICA")

    # El reparto de resolutor es una equivalencia exacta en los extremos, y lo
    # unico que cambia es quien resuelve. Que entre el que debe importa mucho:
    # `saga` con trescientas columnas no converge en el presupuesto disponible.
    cresta = es.logistica(C=0.5, l1_ratio=0.0)()
    lasso = es.logistica(C=0.5, l1_ratio=1.0)()
    mezcla = es.logistica(C=0.5, l1_ratio=0.5)()
    check(cresta.penalty == "l2" and cresta.solver == "lbfgs",
          "l1_ratio=0 es cresta pura y la resuelve lbfgs")
    check(lasso.penalty == "l1" and lasso.solver == "saga",
          "l1_ratio=1 es lasso puro y la resuelve saga")
    check(mezcla.penalty == "elasticnet" and mezcla.solver == "saga",
          "un valor intermedio es mezcla elastica, tambien con saga")
    check(_cerca(mezcla.l1_ratio, 0.5),
          "y el reparto declarado llega intacto al estimador")

    # La tolerancia por defecto depende del resolutor porque los dos criterios de
    # parada no miden lo mismo; pasarla explicita la fija para ambos.
    check(_cerca(cresta.tol, es.TOL_LBFGS) and _cerca(lasso.tol, es.TOL_SAGA),
          "sin declarar `tol`, cada resolutor toma la suya")
    check(_cerca(es.logistica(l1_ratio=0.0, tol=1e-2)().tol, 1e-2),
          "y declararla la fija por encima del valor por defecto")

    # Encuentra la senal plantada y no el ruido: el coeficiente de `x_senal` tiene
    # que dominar al de `x_ruido` en la clase de victoria local.
    X, y, _ = _tabla(n=800, senal=1.5)
    m = es.logistica(C=1.0)().fit(X, y)
    coef = pd.DataFrame(m.coef_, columns=X.columns)
    check(abs(coef.loc[0, "x_senal"]) > 3 * abs(coef.loc[0, "x_ruido"]),
          "la senal plantada pesa mucho mas que el ruido en el logit del local")
    check(coef.loc[0, "x_senal"] > 0 > coef.loc[2, "x_senal"],
          "y su signo es el que la generacion planto: sube el local y baja el visitante")

    # El aviso de no convergencia se silencia, pero el hecho sigue siendo legible
    # en el estimador ajustado.
    import warnings
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        corto = es.logistica(C=1.0, l1_ratio=0.5, max_iter=2)().fit(X, y)
    check(not [a for a in avisos if "converge" in str(a.message).lower()],
          "un ajuste que agota su presupuesto no inunda el registro")
    check(hasattr(corto, "n_iter_"),
          "y el numero de epocas gastadas sigue disponible en el estimador")


# ===========================================================================
# 5. El piso publicable
# ===========================================================================
def test_piso() -> None:
    print("\n5. PISO PUBLICABLE")

    X, y, _ = _tabla(n=400)

    m = es.bosque(n_estimators=40, min_samples_leaf=1)()
    m.fit(X, y)
    P = np.asarray(m.predict_proba(X), dtype="float64")
    check(bool((P > 0).all()),
          "bosque: ninguna probabilidad publicada es un cero exacto")
    check(bool((P >= es._PISO * 0.99).all()),
          "bosque: el piso publicable se respeta hoja a hoja")
    check(_cerca(P.sum(axis=1), 1.0, tol=1e-9),
          "bosque: tras recortar al piso se renormaliza, y la terna sigue sumando 1")

    # Sin el piso, un bosque de hojas minusculas produce ceros exactos: es el caso
    # que la envoltura existe para evitar, y conviene que la prueba lo demuestre en
    # lugar de suponerlo. Una sola fila asi hunde el log-loss de una temporada.
    from sklearn.ensemble import RandomForestClassifier
    crudo = RandomForestClassifier(n_estimators=10, min_samples_leaf=1,
                                   random_state=0).fit(X, y)
    check(bool((crudo.predict_proba(X) == 0.0).any()),
          "el conjunto sin envoltura si produce ceros exactos, que es el motivo del piso")

    # La envoltura delega: la interpretabilidad tiene que seguir viendo el objeto
    # real, no un intermediario que le esconda los atributos del modelo.
    check(hasattr(m, "feature_importances_"),
          "la envoltura delega los atributos del estimador que envuelve")
    check(isinstance(m.base, RandomForestClassifier),
          "y expone el estimador real bajo `base`")


# ===========================================================================
# 6. La red
# ===========================================================================
def test_red() -> None:
    print("\n6. RED")

    X, y, _ = _tabla(n=400)
    ctx = _ctx(400)

    r = es.red(capas=(8,), max_epocas=30, paso=10, paciencia=10)()
    r.fit(X, y, contexto=ctx)
    check(r.n_epocas_ <= 30 and r.n_epocas_ % 10 == 0,
          "el numero de epocas elegido cae en la rejilla de la sonda")
    check(0 < r.n_reserva_ < len(X),
          "reserva un tramo final y deja ventana de ajuste")
    check(list(r.curva_.columns) == ["epocas", "logloss_reserva"],
          "y deja la curva de la sonda para poder mostrarla")
    check(r.n_reserva_ == int(es._corte_temporal(ctx, len(y)).sum()),
          "el tramo reservado es exactamente el que decide el corte por fecha")

    # Sin parada temprana no hace falta contexto: es la rama que usa el afinado
    # cuando el numero de epocas ya esta decidido.
    fijo = es.red(capas=(8,), max_epocas=20, parada_temprana=False)()
    fijo.fit(X, y)
    check(fijo.n_epocas_ == 20,
          "sin parada temprana se gastan las epocas declaradas, sin reservar nada")

    try:
        es.red(capas=(8,), max_epocas=20, paso=10)().fit(X, y)
        check(False, "con parada temprana y sin contexto se detiene")
    except ValueError as e:
        check("fechas del pliegue" in str(e),
              "con parada temprana y sin contexto se detiene")

    # El remuestreo ponderado es reproducible: dos ajustes con el mismo peso y la
    # misma semilla dan el mismo modelo. Si no lo fuera, la fila de esta familia en
    # la tabla no seria repetible, y una cifra que no se repite no se defiende.
    w = np.linspace(0.2, 1.0, len(y))
    a = es.red(capas=(8,), max_epocas=20, paso=10, paciencia=10)()
    b = es.red(capas=(8,), max_epocas=20, paso=10, paciencia=10)()
    a.fit(X, y, sample_weight=w, contexto=ctx)
    b.fit(X, y, sample_weight=w, contexto=ctx)
    check(_cerca(a.predict_proba(X), b.predict_proba(X), tol=1e-10),
          "el remuestreo ponderado es reproducible con semilla fija")

    sin_peso = es.red(capas=(8,), max_epocas=20, paso=10, paciencia=10)()
    Xs, ys = sin_peso._remuestrea(X, y, None)
    check(Xs is X and ys is y,
          "sin peso declarado no se remuestrea, y no se paga varianza de muestreo")

    # El remuestreo conserva el tamano de la ventana: en esperanza equivale a la
    # verosimilitud ponderada, no a entrenar sobre menos partidos.
    Xr, yr = sin_peso._remuestrea(X, y, w)
    check(len(Xr) == len(X) and len(yr) == len(y),
          "y con peso remuestrea la ventana entera, no un subconjunto")


# ===========================================================================
# 7. La mezcla
# ===========================================================================
def _tabla_mezcla(n: int = 500, semilla: int = 3):
    """Matriz con las dos mitades de la mezcla y una senal en cada una.

    `mkt_p_home` empuja hacia la victoria local y `form_x` tambien, con la mitad de
    fuerza. Asi la terna de la mezcla tiene que quedar ENTRE las dos ternas que la
    componen, que es la propiedad que se comprueba y la unica forma de atrapar un
    peso aplicado del reves.
    """
    rng = np.random.default_rng(semilla)
    a = rng.normal(0.0, 1.0, n)
    b = rng.normal(0.0, 1.0, n)
    Z = np.stack([1.2 * a + 0.6 * b, np.zeros(n), -1.2 * a - 0.6 * b], axis=1)
    P = np.exp(Z) / np.exp(Z).sum(axis=1, keepdims=True)
    y = np.array([rng.choice(3, p=p) for p in P])
    X = pd.DataFrame({"mkt_p_home": a, "mkt_p_draw": rng.normal(0.0, 1.0, n),
                      "mkt_p_away": -a, "form_x": b,
                      "form_y": rng.normal(0.0, 1.0, n)})
    return X, y


def test_mezcla() -> None:
    print("\n7. MEZCLA")

    X, y = _tabla_mezcla()
    m = es.construye("mezcla", **lb.HIPER["mezcla"])()
    m.fit(X, y)
    P = np.asarray(m.predict_proba(X), dtype="float64")

    check(P.shape == (len(X), 3), "publica una terna por partido")
    check(_cerca(P.sum(axis=1), 1.0), "sus tres probabilidades suman 1")
    check(hasattr(m, "classes_"), "declara el orden de sus clases")
    check(tuple(m.cols_precio_) == ("mkt_p_home", "mkt_p_draw", "mkt_p_away")
          and tuple(m.cols_juego_) == ("form_x", "form_y"),
          "parte la matriz por el prefijo del precio, no por una lista aparte")

    # Que la mezcla sea la combinacion declarada y no otra. Se recalcula a mano con
    # dos logisticas independientes: si el peso se aplicase a la mitad equivocada o
    # la combinacion fuese aritmetica en vez de logaritmica, esto no coincidiria.
    f = es.logistica(**{k: v for k, v in lb.HIPER["mezcla"].items() if k != "w"})
    a = f().fit(X[list(m.cols_precio_)], y)
    b = f().fit(X[list(m.cols_juego_)], y)
    w = lb.HIPER["mezcla"]["w"]
    lg = (w * np.log(a.predict_proba(X[list(m.cols_precio_)]))
          + (1.0 - w) * np.log(b.predict_proba(X[list(m.cols_juego_)])))
    lg -= lg.max(axis=1, keepdims=True)
    Q = np.exp(lg)
    check(_cerca(P, Q / Q.sum(axis=1, keepdims=True), tol=1e-12),
          "es exactamente `p ∝ p_precio^w · p_juego^(1-w)` renormalizado")

    # Los dos extremos del espacio declarado son los dos modelos que la mezcla tiene
    # que batir, y comprobarlos es lo que impide que el peso quede invertido.
    uno = es.construye("mezcla", w=1.0, C=0.3, l1_ratio=0.0)()
    uno.fit(X, y)
    cero = es.construye("mezcla", w=0.0, C=0.3, l1_ratio=0.0)()
    cero.fit(X, y)
    check(_cerca(uno.predict_proba(X), a.predict_proba(X[list(m.cols_precio_)]),
                 tol=1e-9),
          "con w=1 es exactamente el modelo de precio desnudo")
    check(_cerca(cero.predict_proba(X), b.predict_proba(X[list(m.cols_juego_)]),
                 tol=1e-9),
          "con w=0 es exactamente el modelo de juego desnudo")

    # Los coeficientes salen escalados por el peso: es lo que los deja en la escala
    # del logit que de verdad se publica, y sin eso una variable de juego pareceria
    # seis veces mas influyente de lo que es.
    tab = m.coeficientes()
    check(len(tab) == 5 and "magnitud" in tab.columns,
          "publica una fila por variable de las dos mitades")
    propio = ex.linear_coefficients(a, list(m.cols_precio_))
    check(_cerca(tab.loc["mkt_p_home", "home"],
                 w * propio.loc["mkt_p_home", "home"], tol=1e-12),
          "y cada mitad viene escalada por su peso en la mezcla")

    # Media matriz no se tolera: la mezcla degeneraria en una sola logistica y
    # seguiria publicandose con el nombre de la mezcla.
    for mitad in (["mkt_p_home", "mkt_p_draw"], ["form_x", "form_y"]):
        try:
            es.construye("mezcla", **lb.HIPER["mezcla"])().fit(X[mitad], y)
            check(False, f"una ventana con solo {len(mitad)} columnas de una mitad "
                         f"se detiene")
        except ValueError as e:
            check("las dos mitades" in str(e),
                  "una ventana sin una de las dos mitades se detiene")

    try:
        es.construye("mezcla", w=1.5, C=0.3, l1_ratio=0.0)()
        check(False, "un peso fuera de [0, 1] se detiene")
    except ValueError as e:
        check("[0, 1]" in str(e), "un peso fuera de [0, 1] se detiene")


# ===========================================================================
# 8. Registro y espacios declarados
# ===========================================================================
def test_registro() -> None:
    print("\n8. REGISTRO Y ESPACIOS")

    check(sorted(es.CONSTRUCTORES) == sorted(es.ESPACIOS),
          "toda familia construible tiene espacio declarado, y al reves")
    check(set(es.CONSTRUCTORES) == set(lb.FAMILIAS),
          "y el registro coincide con las familias del estudio")
    check(all(isinstance(v, tuple) and len(v) >= 1
              for esp in es.ESPACIOS.values() for v in esp.values()),
          "todo eje declarado ofrece al menos un valor")

    # El contrato con el banco manual: la configuracion fija y las rejillas del
    # afinado solo pueden nombrar parametros que este modulo declare, porque
    # `construye` es la unica puerta y rechaza cualquier otro.
    for familia, hiper in lb.HIPER.items():
        intrusos = sorted(set(hiper) - set(es.ESPACIOS[familia]))
        check(not intrusos,
              f"{familia}: la configuracion fija solo usa parametros declarados "
              f"(intrusos: {intrusos})")
    for familia, esp in lb.ESPACIOS_FINAL.items():
        intrusos = sorted(set(esp) - set(es.ESPACIOS[familia]))
        check(not intrusos,
              f"{familia}: la rejilla del afinado solo usa parametros declarados "
              f"(intrusos: {intrusos})")

    # Cada parametro declarado tiene que ser aceptado de verdad por su familia. No
    # se comprueba por inspeccion sino construyendo: es el unico modo de atrapar un
    # nombre que la firma acepta con otro significado.
    for nombre, esp in es.ESPACIOS.items():
        punto = {k: v[0] for k, v in esp.items()}
        try:
            fab = es.construye(nombre, **punto)
            fab()
            check(True, f"{nombre}: su espacio declarado es construible")
        except ImportError:
            check(nombre == "refuerzo_xgb",
                  f"{nombre}: la dependencia ausente se declara como ImportError")
        except TypeError as e:
            check(False, f"{nombre}: su espacio declarado es construible ({e})")

    try:
        es.construye("inexistente")
        check(False, "una familia desconocida se detiene")
    except KeyError as e:
        check("desconocida" in str(e), "una familia desconocida se detiene")

    try:
        es.construye("logistica", parametro_inventado=1)
        check(False, "un parametro no declarado en ESPACIOS se detiene")
    except KeyError as e:
        check("no declarados" in str(e),
              "un parametro no declarado en ESPACIOS se detiene")

    # Y las familias retiradas lo estan de verdad: si alguna volviese por la puerta
    # de atras, la tabla del estudio dejaria de ser la que la memoria describe.
    retiradas = ["previa", "local", "goles", "refuerzo", "residuo", "apilado",
                 "calibrado", "arboles_extra", "escalera", "catalogo",
                 "run_ladder"]
    vivas = [n for n in retiradas if hasattr(es, n)]
    check(not vivas, f"el modulo no reexpone lo retirado (siguen: {vivas})")


# ===========================================================================
# 9. Integracion con el protocolo
# ===========================================================================
def test_integracion() -> None:
    print("\n9. INTEGRACION")

    feat = _liga_protocolo()
    cfg = pt.ProtocolConfig(regime="semanal", window="expansiva",
                            refit_cadence="temporada")
    reserva_original = es.MIN_RESERVA
    es.MIN_RESERVA = 60          # la liga sintetica es pequena a proposito
    try:
        folds = pt.make_folds(feat, cfg)
        cache = pt._StateCache(feat, cfg)
        modelos = {
            "constante": lambda: _Constante(),
            "logistica": es.logistica(C=1.0),
            "bosque": es.bosque(n_estimators=60, min_samples_leaf=10),
        }
        pred = {n: pt.run_protocol(feat, f, cfg, folds=folds, cache=cache)
                for n, f in modelos.items()}

        claves = [set(d.match_key) for d in pred.values()]
        check(all(k == claves[0] for k in claves),
              "todas las familias se evaluan sobre EXACTAMENTE los mismos partidos")
        check(all(d.fold.nunique() == len(folds) for d in pred.values()),
              "y sobre los mismos pliegues, que se construyen una sola vez")

        for nombre, d in pred.items():
            P = d[ev.PROB_COLS].to_numpy()
            check(_cerca(P.sum(axis=1), 1.0, tol=1e-6),
                  f"{nombre}: las probabilidades publicadas suman 1")

        m_prev = ev.metrics(pred["constante"], completo=False)
        m_log = ev.metrics(pred["logistica"], completo=False)
        check(m_log["rps"] <= m_prev["rps"] + 0.02,
              "la logistica no queda por detras de la previa de forma anomala")

        # El canal de contexto: lo que el protocolo entrega es lo que el pliegue
        # tiene, ni una fila mas ni una menos.
        visto = {}

        class _Espia(_Constante):
            def fit(self, X, y, sample_weight=None, contexto=None):
                visto["n_fit"] = len(X)
                visto["fechas"] = pd.to_datetime(contexto.fechas).to_numpy()
                visto["mercado"] = None if contexto.mercado is None \
                    else contexto.mercado
                return super().fit(X, y, sample_weight=sample_weight)

        pliegue = folds[0]
        pt.run_protocol(feat, lambda: _Espia(), cfg, folds=[pliegue])
        check(len(visto["fechas"]) == visto["n_fit"],
              "el contexto trae una fecha por fila de la ventana")
        check(visto["mercado"].shape == (visto["n_fit"], 3),
              "y la terna de mercado con la misma altura que la matriz")

        # La propiedad que importa no es que las fechas vengan ordenadas —el
        # calendario real alterna sabado y domingo dentro de la misma jornada, de
        # modo que el marco NO viene ordenado por fecha— sino que la fila i del
        # contexto sea el partido de la fila i de la matriz. El canal se justifica
        # precisamente porque el orden no puede darse por supuesto.
        esperadas = pd.to_datetime(feat.loc[pliegue.train_idx, "date"]).to_numpy()
        check(bool((visto["fechas"] == esperadas).all()),
              "cada fecha del contexto es la del partido de esa misma fila")
        check(not pd.Series(esperadas).is_monotonic_increasing,
              "y no es que el marco venga ordenado: dentro de la jornada no lo esta")
        # Y el precio llega tal como entro en el marco, no estandarizado: es la
        # copia que el preprocesado aparta como metadato la que viaja por el canal.
        precio = feat.loc[pliegue.train_idx, list(pp.MARKET_PROB_COLS)].to_numpy()
        check(_cerca(visto["mercado"], precio, tol=1e-12),
              "y el precio viaja en su escala original, fila a fila")
    finally:
        es.MIN_RESERVA = reserva_original


# ===========================================================================
# Orquestacion
# ===========================================================================
def _run() -> None:
    print("=" * 74)
    print("BANCO DE PRUEBAS — estimators.py")
    print("=" * 74)
    test_firma()
    test_pesos()
    test_corte()
    test_logistica()
    test_piso()
    test_red()
    test_mezcla()
    test_registro()
    test_integracion()
    print("\n" + "=" * 74)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    if FAIL:
        for f in FAIL:
            print(f"   - {f}")
    print("=" * 74)


def test_estimators() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
