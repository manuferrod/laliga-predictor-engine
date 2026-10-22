"""Las familias del estudio: un constructor por familia, con firma común.

El protocolo no sabe qué modelo entrena. Recibe una **fábrica** —un invocable sin
argumentos que devuelve un estimador nuevo— y la usa una vez por pliegue. Este
módulo contiene las fábricas, y su disciplina es que todas se construyan igual:

    import estimators as es
    pred = run_protocol(feat, es.logistica(C=0.3), cfg)

De ahí se sigue la propiedad que hace comparables las familias: dos modelos
evaluados con el mismo `cfg` recorren exactamente los mismos pliegues, con las
mismas ventanas, el mismo preprocesado y el mismo conjunto de partidos. Cualquier
diferencia entre sus métricas es diferencia de modelo, no de protocolo.

Qué familias y por qué esas
---------------------------
La escalera del estudio son cuatro familias, y la razón no es de presupuesto sino
de diseño experimental: cada una encarna una hipótesis distinta sobre la forma de
la relación entre las variables y el resultado, y una quinta que compartiese
hipótesis con alguna de estas alargaría la tabla sin añadir una respuesta.

    logistica       lineal en el logit. Multinomial con penalización elástica.
                    Es la familia interpretable —sus coeficientes se leen como
                    aportación de cada variable— y la que mejor tolera doscientas
                    variables frente a poco más de dos mil partidos de ventana.
    bosque          promedio de árboles independientes. Cada árbol se ajusta por
                    su cuenta sobre una submuestra y un subconjunto de columnas, y
                    lo que reduce el error es promediar modelos que se equivocan en
                    sitios distintos. Con variables correlacionadas, señal débil y
                    ruido alto, esa estrategia compite de verdad.
    refuerzo_xgb    suma secuencial de árboles que se corrigen. Es la hipótesis
                    contraria a la del bosque: en lugar de promediar errores
                    independientes, cada árbol ataca el residuo del anterior. Es
                    quien mejor extrae interacciones no lineales entre forma,
                    fatiga, bajas y precio. La biblioteca es una dependencia
                    OPCIONAL: si no está instalada, la fábrica levanta `ImportError`
                    al construirse, y esa falta consta en lugar de desaparecer.
    red             composición libre de interacciones. Perceptrón multicapa, la
                    única familia que compone interacciones de orden alto sin que
                    nadie las declare, y por eso la prueba de si queda estructura
                    que los árboles no capturan. Con la muestra disponible se
                    espera que sobreajuste, y comprobarlo es parte del resultado.

Las cuatro compiten bajo el mismo protocolo, sobre los mismos pliegues y en la
misma tabla. Quien las hace competir es el banco manual de `laboratorio.py`: allí
se decide con qué conjunto de variables corre cada una y allí se registran las
cifras. Este módulo solo construye.

A ellas se añade una quinta que no entra en esa escalera porque no propone otra
forma funcional, sino otra manera de **repartir la varianza**:

    mezcla          dos logísticas sobre mitades disjuntas de la matriz —precio y
                    juego—, combinadas en el logit por un solo peso. Es la familia
                    del modelo publicado, y lo que la justifica es que ninguna
                    logística sobre la unión de las dos mitades puede
                    representarla: un único ajuste reparte su presupuesto de
                    varianza entre las 62 columnas a la vez, y eso es exactamente
                    lo que deterioraba el modelo cada vez que el banco de
                    variables entraba entero. Tiene un parámetro, así que no hay
                    nada que sobreajustar.

Los espacios declarados
-----------------------
Cada familia declara en `ESPACIOS` el conjunto de valores admisibles de su
configuración. Están escritos aquí, junto al modelo, por dos razones. La primera
es que un espacio es una afirmación sobre el modelo —qué profundidad es
razonable, en qué orden de magnitud vive su regularización— y pertenece a quien
lo define. La segunda es que escribirlo antes de buscar impide ampliarlo a
posteriori para acomodar el valor que resultó ganar, que es la forma más común de
sobreajustar una búsqueda sin darse cuenta.

`ESPACIOS` cumple además una función de contrato. Durante la búsqueda de
variables la configuración está FIJA —la declara `laboratorio.HIPER`— y solo se
afina al final, sobre el conjunto ya cerrado y con las rejillas cortas de
`laboratorio.ESPACIOS_FINAL`. Ambas cosas pasan por `construye`, que rechaza todo
parámetro no declarado aquí: así un valor no puede colarse por la firma de un
constructor sin que la memoria diga que formaba parte del espacio.

Sobre el reajuste dentro del pliegue
------------------------------------
Dos familias necesitan reservar un tramo de su ventana de entrenamiento para
aprender algo que no puede aprenderse sobre los mismos datos que ajustaron el
modelo: cuántas épocas entrenar. Ese tramo es siempre el **final temporal de la
ventana**, y esa elección no es un detalle de implementación.

Reservarlo al azar sería más cómodo y produciría mejores números: un tramo
aleatorio comparte jornada con las filas de entrenamiento y hereda su nivel, de
modo que la detención temprana se dispararía tarde y el modelo saldría más
ajustado de lo que la validación honesta permitiría. Reservar el final temporal
reproduce la situación real —se decide con el pasado y se aplica al futuro— y
tiene además la propiedad de que el tramo reservado es el más parecido al pliegue
que viene a continuación.

El tramo se identifica por fecha, a través del `Contexto` que entrega el
protocolo, y no por posición en el marco. Confiar en el orden de las filas es una
suposición silenciosa: un reordenamiento inocente la rompería sin dejar rastro en
ninguna métrica, porque el resultado seguiría siendo plausible.
"""
from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

_CLASES = np.array([0, 1, 2])          # y_num: 0=H, 1=D, 2=A
_EPS = 1e-12
_PISO = 1e-9                           # probabilidad mínima publicable

# Fracción final de la ventana que se reserva para lo que no puede aprenderse
# sobre los datos de ajuste. Una temporada de liga son 380 partidos; con ventanas
# de dos a seis mil, un 20 % deja entre cuatrocientos y mil doscientos partidos
# de tramo reservado, que es suficiente para estimar un puñado de parámetros sin
# vaciar el entrenamiento.
FRAC_RESERVA = 0.20
MIN_RESERVA = 200


# ===========================================================================
# 1. Utilidades comunes
# ===========================================================================
def _objetivo(y) -> np.ndarray:
    y = np.asarray(y).astype("int64")
    if not np.isin(y, _CLASES).all():
        raise ValueError("el objetivo tiene valores fuera de {0, 1, 2}")
    return y


def _pesos(w, n: int) -> np.ndarray:
    """Vector de pesos normalizado a media uno, o unos si no se ha pedido peso.

    La normalización importa: la fuerza de la regularización se interpreta frente
    a la suma de los pesos, de modo que un vector de recencia con media distinta
    de uno cambiaría de hecho el hiperparámetro sin que nadie lo hubiese tocado.
    """
    if w is None:
        return np.ones(n, dtype="float64")
    w = np.asarray(w, dtype="float64")
    if len(w) != n:
        raise ValueError(f"el peso tiene {len(w)} elementos y la muestra {n}")
    if (w < 0).any():
        raise ValueError("hay pesos negativos")
    s = w.sum()
    if s <= 0:
        raise ValueError("los pesos suman cero: no hay muestra efectiva")
    return w * (n / s)


def _normaliza(P: np.ndarray) -> np.ndarray:
    """Recorta al piso y renormaliza, para que la salida sea siempre publicable."""
    P = np.clip(np.asarray(P, dtype="float64"), _PISO, None)
    return P / P.sum(axis=1, keepdims=True)


def _corte_temporal(contexto, n: int, frac: float = FRAC_RESERVA,
                    minimo: int = MIN_RESERVA) -> np.ndarray:
    """Máscara booleana del tramo FINAL de la ventana, decidido por fecha.

    Devuelve `True` en las filas reservadas. El corte se hace sobre una fecha, no
    sobre un número de filas, para que una jornada no quede partida entre el tramo
    de ajuste y el reservado: si lo estuviese, el modelo habría visto el resultado
    del sábado al decidir cuántas épocas entrenar para el domingo, que es en
    pequeño la misma fuga que el protocolo evita en grande.
    """
    if contexto is None or getattr(contexto, "fechas", None) is None:
        raise ValueError(
            "esta familia reserva el tramo final de la ventana y necesita las "
            "fechas del pliegue; el protocolo las entrega en `Contexto`, así que "
            "esto solo ocurre si se ha llamado a `fit` fuera del protocolo")
    fechas = pd.to_datetime(pd.Series(contexto.fechas).reset_index(drop=True))
    if len(fechas) != n:
        raise ValueError(f"el contexto trae {len(fechas)} fechas y la ventana {n}")

    objetivo = max(minimo, int(round(frac * n)))
    if objetivo >= n:
        raise ValueError(
            f"la ventana tiene {n} partidos y el tramo reservado pediría "
            f"{objetivo}: no queda entrenamiento. Amplíe `min_train_matches` o "
            f"reduzca la reserva")
    # Se recorren las fechas distintas de atrás hacia delante y se acumulan
    # jornadas enteras hasta cubrir el objetivo.
    unicas = np.sort(fechas.unique())
    acumulado, corte = 0, unicas[-1]
    for f in unicas[::-1]:
        acumulado += int((fechas == f).sum())
        corte = f
        if acumulado >= objetivo:
            break
    reserva = (fechas >= corte).to_numpy()
    if reserva.all():
        raise ValueError("todas las filas de la ventana comparten fecha")
    return reserva


class _ConPiso:
    """Envoltura que impone el piso de probabilidad publicable a un estimador ajeno.

    Un conjunto de árboles por votación puede devolver un cero exacto: basta con
    que ninguna hoja alcanzada por un partido contenga empates. El log-loss de esa
    fila se convierte entonces en el recorte de la métrica —treinta y cinco nats—
    y una sola fila así hunde la media de una temporada entera. El resultado no
    sería falso, pero mediría la aritmética del recorte y no la calidad del
    pronóstico, y además pondría a estas familias en desventaja frente a las que
    producen probabilidades por una exponencial y nunca tocan el cero.

    El piso es el mismo `_PISO` que declara el módulo como mínimo publicable, de
    modo que ninguna familia se evalúa con un umbral propio. Tras recortar se
    renormaliza, porque la suma tiene que seguir valiendo uno: el protocolo lo
    comprueba antes de aceptar la predicción.

    La envoltura delega todo lo demás en el estimador que envuelve, de manera que
    la interpretabilidad siga viendo el objeto real.
    """

    def __init__(self, base):
        self.base = base

    def fit(self, X, y, sample_weight=None):
        self.base.fit(X, y, sample_weight=sample_weight)
        self.classes_ = self.base.classes_
        return self

    def predict_proba(self, X):
        return _normaliza(self.base.predict_proba(X))

    def __getattr__(self, nombre):
        if nombre == "base":                       # evita la recursión infinita
            raise AttributeError(nombre)
        return getattr(self.base, nombre)


# ===========================================================================
# 2. Regresión logística multinomial regularizada
# ===========================================================================
# Tolerancia de parada por resolutor. No es el mismo número porque no miden lo
# mismo: `lbfgs` para cuando la norma del gradiente baja del umbral y `saga` cuando
# el mayor cambio de un coeficiente entre épocas lo hace. Los dos valores están
# medidos, no supuestos, contra un ajuste llevado a convergencia sobre una matriz
# del tamaño de la nuestra —2.300 partidos y unas 356 columnas estandarizadas—:
#
#   lbfgs a 1e-5   36 iteraciones, la probabilidad de un partido se mueve 1e-3
#   lbfgs a 1e-3   13 iteraciones, pero se mueve 9e-2, que ya es visible en la web
#   saga  a 1e-4   476 épocas, la selección de variables está asentada
#   saga  a 1e-3   166 épocas, pero anula 298 coeficientes donde el converso anula
#                  362, y en esta familia *qué* se anula es media lectura
#
# De ahí los dos números: apurar `lbfgs` es casi gratis y aflojar `saga` costaría
# la parte interpretable del modelo.
TOL_LBFGS = 1e-5
TOL_SAGA = 1e-4


def logistica(C: float = 1.0, l1_ratio: float = 0.0, max_iter: int = 1000,
              tol: float | None = None):
    """Fábrica de `logistica`: multinomial con penalización elástica.

    `C` es la inversa de la fuerza de regularización y `l1_ratio` reparte entre la
    penalización que selecciona —anulando coeficientes— y la que encoge sin anular.
    Ambos son hiperparámetros y como tales se eligen dentro del bloque de
    validación, nunca mirando el holdout.

    La matriz llega ya estandarizada por el preprocesado, de modo que aquí no se
    vuelve a escalar. Es una condición necesaria y no un detalle: una penalización
    aplicada a variables en escalas distintas castiga a las de unidades grandes por
    el mero hecho de serlo, y los coeficientes dejarían de ser comparables entre sí,
    que es justamente lo que esta familia aporta.

    El valor declarado de `l1_ratio` es cero —cresta pura— y no un punto intermedio.
    La razón es de método antes que de coste: la búsqueda de variables corre con la
    configuración fija que declara `laboratorio.HIPER`, de modo que lo que allí
    debe correr es la forma canónica de una logística regularizada; la mezcla
    elástica pertenece al afinado final, ya con el conjunto cerrado. El espacio
    declarado en `ESPACIOS` recorre `l1_ratio` de cero a uno, así que la capacidad
    de anular coeficientes no se pierde: se decide en lugar de suponerse.

    Qué resolutor entra según ese reparto:

        l1_ratio = 0        cresta pura, penalización `l2`, resolutor `lbfgs`
        0 < l1_ratio < 1    mezcla elástica, resolutor `saga`
        l1_ratio = 1        lasso puro, penalización `l1`, resolutor `saga`

    El reparto es una equivalencia exacta y no una aproximación: en los extremos la
    penalización elástica *es* la cresta y *es* el lasso, y lo único que cambia es
    quién resuelve el problema. Cambia mucho, eso sí. `saga` es un método de
    gradiente promediado cuyo coste por época crece con el número de partidos de la
    ventana, y con trescientas y pico columnas estandarizadas agota su presupuesto
    de iteraciones sin converger; `lbfgs` es cuasi-Newton sobre un problema liso y
    termina en un par de centenares. Como el protocolo reajusta una vez por jornada
    —del orden de doscientos treinta ajustes por candidato y por régimen—, esa
    diferencia es la que separa un recorrido de minutos de uno de días.

    `tol` sin declarar toma el valor que corresponde al resolutor que vaya a entrar
    —`TOL_LBFGS` o `TOL_SAGA`, justificados donde se declaran—; pasarlo explícito lo
    fija para los dos, que es lo que hace falta cuando se quiere comparar ramas con
    la misma exigencia de parada.
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.exceptions import ConvergenceWarning

    class _Logistica(LogisticRegression):
        """Logística que no inunda el registro cuando `saga` agota su presupuesto.

        El aviso de no convergencia se emite una vez por ajuste, y un ajuste por
        jornada son doscientas treinta líneas idénticas por candidato: el registro
        de la ejecución deja de poder leerse y con él se pierden las cifras que sí
        importan. Silenciarlo aquí no oculta el hecho —`n_iter_` sigue disponible en
        el estimador ajustado y dice exactamente cuántas épocas se gastaron—, sino
        que lo saca del canal equivocado. Es el mismo trato que recibe la red
        neuronal, y por el mismo motivo.
        """

        def fit(self, X, y, sample_weight=None):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=ConvergenceWarning)
                return super().fit(X, y, sample_weight=sample_weight)

    def fabrica():
        if l1_ratio <= 0.0:
            return _Logistica(penalty="l2", solver="lbfgs", C=C,
                              max_iter=max_iter,
                              tol=TOL_LBFGS if tol is None else tol,
                              fit_intercept=True, n_jobs=None)
        t = TOL_SAGA if tol is None else tol
        if l1_ratio >= 1.0:
            return _Logistica(penalty="l1", solver="saga", C=C,
                              max_iter=max_iter, tol=t, fit_intercept=True,
                              n_jobs=None)
        return _Logistica(
            penalty="elasticnet", solver="saga", l1_ratio=l1_ratio, C=C,
            max_iter=max_iter, tol=t, fit_intercept=True, n_jobs=None)

    return fabrica


# ===========================================================================
# 3. Bosque aleatorio
# ===========================================================================
def bosque(n_estimators: int = 600, max_depth: int | None = None,
           min_samples_leaf: int = 20, max_features="sqrt",
           random_state: int = 0):
    """Fábrica de `bosque`: bosque aleatorio de árboles profundos promediados.

    La hipótesis es la opuesta a la del refuerzo. Allí cada árbol corrige el error
    del anterior y el riesgo es memorizar; aquí cada árbol se ajusta por su cuenta
    sobre una submuestra y un subconjunto de columnas, y lo que reduce el error es
    promediar modelos que se equivocan en sitios distintos. En un problema con
    doscientas variables correlacionadas, señal débil y ruido alto, esa segunda
    estrategia no es evidentemente peor, y decidirlo a ojo sería exactamente lo que
    la comparación existe para evitar.

    `min_samples_leaf` es el parámetro que gobierna el compromiso: hojas grandes
    producen probabilidades suaves y un modelo que apenas se mueve; hojas pequeñas
    producen probabilidades extremas que el log-loss castiga sin piedad. El valor
    por defecto es un punto de partida y no una elección: la elección se hace en el
    afinado final, sobre el espacio declarado en `ESPACIOS`.

    Conviene leer sus probabilidades con cautela: un bosque las produce como
    proporción de votos de las hojas, que es un estimador consistente pero
    típicamente mal calibrado en los extremos. La curva de fiabilidad que publica
    la evaluación es el sitio donde eso se ve.
    """
    from sklearn.ensemble import RandomForestClassifier

    def fabrica():
        return _ConPiso(RandomForestClassifier(
            n_estimators=int(n_estimators), max_depth=max_depth,
            min_samples_leaf=int(min_samples_leaf), max_features=max_features,
            bootstrap=True, oob_score=False, n_jobs=-1,
            random_state=int(random_state)))

    return fabrica


# ===========================================================================
# 4. Refuerzo de gradiente (XGBoost)
# ===========================================================================
def refuerzo_xgb(n_estimators: int = 400, learning_rate: float = 0.05,
                 max_depth: int = 3, min_child_weight: float = 20.0,
                 subsample: float = 0.8, colsample_bytree: float = 0.6,
                 reg_lambda: float = 1.0, random_state: int = 0):
    """Fábrica de `refuerzo_xgb`: refuerzo de gradiente con XGBoost.

    Es la familia que suma árboles secuencialmente, cada uno ajustado sobre lo que
    el anterior no explicó. Su hipótesis es que la señal vive en interacciones
    entre variables —forma con fatiga, bajas con precio— que ni un modelo lineal
    compone ni un promedio de árboles independientes busca.

    El número de árboles es fijo y no se detiene sobre un tramo reservado. La
    decisión es deliberada: la detención temprana obliga a recortar la ventana de
    entrenamiento en cada uno de los doscientos y pico ajustes del recorrido, y con
    `min_child_weight` alto y `learning_rate` bajo el sobreajuste se controla por
    regularización sin pagar ese precio. Lo que gobierna el compromiso es la pareja
    profundidad-tasa, y es lo que recorre el afinado final.

    XGBoost es una dependencia **opcional** del proyecto, igual que las bibliotecas
    de descarga de las que dependen algunos extractores. La fábrica no la importa
    al definirse sino al construir el estimador, de modo que un entorno sin ella no
    rompe la importación de este módulo: la familia falla al instanciarse, con un
    mensaje que dice qué falta. Esa es la conducta que se busca —una familia que no
    pudo probarse deja constancia— frente a la alternativa de excluirla y que el
    lector no sepa nunca que se consideró.
    """
    def fabrica():
        try:
            from xgboost import XGBClassifier
        except ImportError as e:                    # noqa: F841 - se reexpone
            raise ImportError(
                "la familia `refuerzo_xgb` requiere la dependencia opcional "
                "`xgboost`, que no está instalada en este entorno; instálela o "
                "retire la familia de la comparación") from e
        return _ConPiso(XGBClassifier(
            n_estimators=int(n_estimators), learning_rate=float(learning_rate),
            max_depth=int(max_depth), min_child_weight=float(min_child_weight),
            subsample=float(subsample), colsample_bytree=float(colsample_bytree),
            reg_lambda=float(reg_lambda), objective="multi:softprob",
            num_class=3, tree_method="hist", n_jobs=-1,
            random_state=int(random_state), verbosity=0))

    return fabrica


# ===========================================================================
# 5. Red neuronal
# ===========================================================================
class EstimadorRed:
    """Perceptrón multicapa, detenido sobre el futuro temporal de su ventana.

    Es la única familia que compone interacciones de orden alto sin que nadie las
    declare, y por eso su papel es responder a una pregunta concreta: si queda
    estructura en los datos que los árboles no estén capturando. Con una ventana
    del orden de dos mil partidos y doscientas variables se espera que sobreajuste;
    comprobarlo con el mismo protocolo que todo lo demás es un resultado del
    trabajo, no un fracaso de la ejecución.

    El número de épocas se elige sobre el pasado de la ventana: se ajusta, se sigue
    la log-verosimilitud sobre su **tramo final por fecha** y se para cuando deja de
    mejorar. No se usa la parada temprana de la biblioteca porque reserva un tramo
    ALEATORIO, y un tramo aleatorio comparte jornada con las filas de ajuste: la
    parada se dispararía tarde y el modelo saldría más ajustado de lo que la
    validación honesta permite, sin dejar rastro en ninguna métrica.

    El peso de recencia merece explicación aparte. `MLPClassifier` no admite
    `sample_weight`, y había dos salidas malas y una aceptable. Ignorar el peso
    dejaría a esta familia corriendo un protocolo distinto del de las demás y su
    fila de la tabla no sería comparable. Declarar que no lo admite la expulsaría
    de la comparación en cuanto la configuración pidiese semivida. Lo que se hace
    es **remuestrear la ventana con probabilidad proporcional al peso** y semilla
    fija: en esperanza equivale a la verosimilitud ponderada, es reproducible, y su
    coste —la varianza que añade el muestreo— queda escrito aquí en lugar de
    esconderse en una diferencia de métrica que nadie sabría atribuir.

    La matriz llega estandarizada del preprocesado, condición necesaria para esta
    familia mucho más que para las de árboles: una red con entradas en escalas
    dispares avanza casi solo en las direcciones de unidades grandes y el
    regularizador deja de significar lo mismo en cada peso.
    """

    def __init__(self, capas: tuple[int, ...] = (64, 32), alpha: float = 1e-3,
                 learning_rate_init: float = 1e-3, batch_size: int = 256,
                 max_epocas: int = 300, paciencia: int = 30, paso: int = 10,
                 parada_temprana: bool = True, random_state: int = 0):
        self.capas = tuple(int(c) for c in capas)
        self.alpha = float(alpha)
        self.learning_rate_init = float(learning_rate_init)
        self.batch_size = int(batch_size)
        self.max_epocas = int(max_epocas)
        self.paciencia = int(paciencia)
        self.paso = int(paso)
        self.parada_temprana = bool(parada_temprana)
        self.random_state = int(random_state)

    def _nuevo(self, max_iter: int, warm: bool = False):
        from sklearn.neural_network import MLPClassifier
        return MLPClassifier(
            hidden_layer_sizes=self.capas, activation="relu", solver="adam",
            alpha=self.alpha, learning_rate_init=self.learning_rate_init,
            batch_size=min(self.batch_size, 512), max_iter=max_iter,
            early_stopping=False, warm_start=warm, shuffle=True,
            n_iter_no_change=self.max_epocas,   # la parada la decide este objeto
            tol=0.0, random_state=self.random_state)

    def _remuestrea(self, X, y, w):
        """Muestra con reemplazo y probabilidad proporcional al peso de recencia.

        Devuelve la ventana tal cual cuando no hay peso, de modo que el caso
        habitual —sin semivida declarada— no paga ninguna varianza de muestreo.
        """
        if w is None:
            return X, y
        p = np.asarray(w, dtype="float64")
        if p.sum() <= 0:
            return X, y
        p = p / p.sum()
        rng = np.random.default_rng(self.random_state)
        idx = rng.choice(len(y), size=len(y), replace=True, p=p)
        Xr = X.iloc[idx] if isinstance(X, pd.DataFrame) else X[idx]
        return Xr, y[idx]

    def fit(self, X, y, sample_weight=None, contexto=None):
        from sklearn.exceptions import ConvergenceWarning

        y = _objetivo(y)
        w = None if sample_weight is None else _pesos(sample_weight, len(y))
        Xm, ym = self._remuestrea(X, y, w)

        def _ajusta(modelo, Xd, yd):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=ConvergenceWarning)
                with np.errstate(all="ignore"):
                    return modelo.fit(Xd, yd)

        if not self.parada_temprana:
            self.n_epocas_ = self.max_epocas
            self.modelo_ = _ajusta(self._nuevo(self.max_epocas), Xm, ym)
            self.classes_ = self.modelo_.classes_
            return self

        # El tramo reservado se decide sobre la ventana ORIGINAL, no sobre la
        # remuestreada: el remuestreo repite filas y perdería la correspondencia
        # con las fechas, que es lo que define el corte.
        reserva = _corte_temporal(contexto, len(y))
        ajuste = ~reserva
        Xa = X.iloc[ajuste] if isinstance(X, pd.DataFrame) else X[ajuste]
        Xr = X.iloc[reserva] if isinstance(X, pd.DataFrame) else X[reserva]
        if w is not None:
            Xa, ya = self._remuestrea(Xa, y[ajuste], w[ajuste])
        else:
            ya = y[ajuste]

        # Cada `fit` sobre la sonda tibia corre `max_iter` épocas ADICIONALES: en
        # esta familia `max_iter` es el presupuesto de la llamada y no el total
        # acumulado. Por eso se fija en `paso` una sola vez y el acumulado lo
        # lleva la variable del bucle; ponerlo a `n` en cada vuelta entrenaría
        # `paso + 2·paso + ...` épocas y la curva mentiría sobre su propio eje.
        # La sonda tibia conserva los pesos pero reinicia los momentos del
        # optimizador en cada tramo, de modo que la trayectoria no es idéntica a
        # la de un ajuste corrido. Es un coste asumido y no un sesgo: el modelo
        # publicado se reajusta desde cero con las épocas elegidas, y lo que la
        # sonda tiene que estimar es dónde deja de mejorar, no reproducirlo.
        sonda = self._nuevo(self.paso, warm=True)
        mejor_nll, mejor_ep, sin_mejora = np.inf, self.paso, 0
        curva = []
        for n in range(self.paso, self.max_epocas + 1, self.paso):
            # La sonda se detiene por diseño antes de converger, de modo que el
            # aviso de convergencia de la biblioteca es aquí ruido y no señal.
            _ajusta(sonda, Xa, ya)
            P = np.clip(sonda.predict_proba(Xr), _EPS, 1.0)
            idx = np.searchsorted(sonda.classes_, y[reserva])
            nll = float(-np.log(P[np.arange(len(idx)), idx]).mean())
            curva.append((n, nll))
            if nll < mejor_nll - 1e-6:
                mejor_nll, mejor_ep, sin_mejora = nll, n, 0
            else:
                sin_mejora += self.paso
                if sin_mejora >= self.paciencia:
                    break

        self.curva_ = pd.DataFrame(curva, columns=["epocas", "logloss_reserva"])
        self.n_epocas_ = mejor_ep
        self.n_reserva_ = int(reserva.sum())
        self.modelo_ = _ajusta(self._nuevo(mejor_ep), Xm, ym)
        self.classes_ = self.modelo_.classes_
        return self

    def predict_proba(self, X):
        return _normaliza(self.modelo_.predict_proba(X))


def red(**kwargs):
    """Fábrica de `red`."""
    return lambda: EstimadorRed(**kwargs)


# ===========================================================================
# 6. Mezcla logit de precio y juego
# ===========================================================================
# Prefijo que parte la matriz en dos mitades. Las tres columnas de probabilidad
# implícita del mercado lo llevan y ninguna de las 59 de juego lo lleva, así que
# el reparto se deduce de los nombres y no hay dos listas que puedan
# desincronizarse del conjunto publicado. Se midió antes de escribirlo:
# `candidatas()` da 65 columnas, el dominio `mercado` son 6 y las 59 restantes no
# tienen ni una con prefijo `mkt`.
PREFIJO_PRECIO = "mkt_p_"


class EstimadorMezcla:
    """Dos logísticas sobre mitades disjuntas, combinadas en el logit por un peso.

    Ajusta una logística sobre las columnas de precio, otra sobre las de juego, y
    publica `p ∝ p_mercado^w · p_juego^(1-w)` renormalizado. Las dos se ajustan
    sobre las mismas filas y con la misma configuración; lo único que las separa es
    qué columnas ven.

    Por qué no es la misma cosa que una logística sobre la unión
    -----------------------------------------------------------
    Un solo ajuste sobre las 62 columnas reparte su presupuesto de varianza entre
    todas a la vez: cada coeficiente de juego que estima lo estima a costa de la
    precisión con la que estima los tres del precio, y ese es exactamente el modo
    en que el banco de variables deterioraba el modelo. Aquí los dos ajustes son
    independientes —el submodelo de juego no puede robarle varianza al del precio,
    porque no comparte matriz con él— y lo único que se estima en común es el peso.
    Ninguna logística sobre la unión puede representar eso.

    De ahí la propiedad que la hace defendible y que ninguna de las diez rutas de
    búsqueda de variables tenía: **un solo parámetro**. No hay nada que
    sobreajustar, y por eso es la única ruta del proyecto cuyo signo NO se invierte
    al salir del bloque que lo eligió.

    Lo que está medido (19-oct-2026, `_mezcla.py`)
    ----------------------------------------------
    `w` se eligió sobre el bloque de `seleccion` por ACIERTO —no por ROI— y salió
    0.85. Elegirlo por ROI daba 0.35, y fuera pierde contra el 0.85 en las dos
    convenciones de liquidación —+1.38 % contra +2.51 % a la mejor cuota entre
    casas, −0.05 % contra +1.08 % de ventaja sobre el listón a cierre promediado—:
    el ROI sobre 1140 partidos tiene un error típico de ~3 pp y seleccionar con él
    es seleccionar ruido. Las dos cifras hay que citarlas con su convención al
    lado, porque los niveles no son intercambiables.

    Sobre los **1900 partidos que no participaron en elegir nada** —`medicion` más
    `confirmacion`, con el arranque abierto a 2016/17—, liquidando a la mejor cuota
    entre casas: ROI **+2.51 %** contra +1.21 % de la semilla de precio y −2.81 %
    del listón de Bet365; acierto **53.74 %** contra 53.37 % y 53.47 %. La ventaja
    sobre el listón es +5.32 pp [+3.41, +7.24]; la ventaja sobre la semilla, +1.30
    pp, son 1.4 errores típicos y NO excluye el cero. El ROI sube en las dos
    ventanas de fuera por separado (+1.14 pp en `medicion`, +1.53 pp en
    `confirmacion`), que es la repetición de signo que todo lo demás falló.

    Y el precio de la decisión, declarado: su **RPS es 0.000527 peor** que el de la
    semilla (0.194977 contra 0.194450), sin significación. Adoptarla exigía levantar
    la regla 4 del proyecto —«cuando ROI y RPS discrepan, manda RPS»—, y la levantó
    Manuel el 19-oct-2026: «quiero que el ROI mande, no el RPS». No es una decisión
    técnica tomada aquí.

    Y que esta clase sea ese modelo y no otro parecido está comprobado, no supuesto.
    `_mezcla.py` midió la mezcla FUERA del motor —dos ajustes por separado y la
    combinación a mano con numpy—; esta familia la hace DENTRO, con el protocolo
    entrenándola como a cualquier otra y partiendo las 62 columnas por el prefijo.
    `_mezcla_control.py` enfrenta las dos ternas sobre los 3040 partidos del arranque
    abierto: la **máxima diferencia absoluta de probabilidad es 0.000e+00** y las
    siete cifras publicables cuadran hasta el último decimal escrito. Eso es lo que
    autoriza a publicar esta familia con las cifras del estudio.
    """

    def __init__(self, w: float = 0.85, C: float = 0.3, l1_ratio: float = 0.0,
                 prefijo: str = PREFIJO_PRECIO):
        if not 0.0 <= float(w) <= 1.0:
            raise ValueError(f"el peso de la mezcla vive en [0, 1] y llega {w!r}")
        self.w = float(w)
        self.C = float(C)
        self.l1_ratio = float(l1_ratio)
        self.prefijo = str(prefijo)

    def _parte(self, X) -> tuple[list[str], list[str]]:
        """Reparte las columnas de la matriz en precio y juego, por el prefijo.

        Que falte una de las dos mitades no se tolera: la mezcla degeneraría en una
        sola logística y seguiría publicándose con el nombre de la mezcla, que es
        justo la confusión que el modelo publicado no puede permitirse.
        """
        if not isinstance(X, pd.DataFrame):
            raise TypeError(
                "la mezcla reparte columnas por su nombre y necesita un DataFrame; "
                f"ha llegado {type(X).__name__}")
        cols = [str(c) for c in X.columns]
        precio = [c for c in cols if c.startswith(self.prefijo)]
        juego = [c for c in cols if not c.startswith(self.prefijo)]
        if not precio or not juego:
            raise ValueError(
                f"la mezcla necesita las dos mitades y esta ventana trae "
                f"{len(precio)} columnas con prefijo {self.prefijo!r} y "
                f"{len(juego)} sin él")
        return precio, juego

    def fit(self, X, y, sample_weight=None):
        self.cols_precio_, self.cols_juego_ = self._parte(X)
        hacer = logistica(C=self.C, l1_ratio=self.l1_ratio)
        self.precio_ = hacer().fit(X[self.cols_precio_], _objetivo(y),
                                   sample_weight=sample_weight)
        self.juego_ = hacer().fit(X[self.cols_juego_], _objetivo(y),
                                  sample_weight=sample_weight)
        if not np.array_equal(self.precio_.classes_, self.juego_.classes_):
            raise ValueError(
                "los dos submodelos han visto conjuntos de clases distintos y sus "
                "columnas de probabilidad no son comparables")
        self.classes_ = self.precio_.classes_
        return self

    def predict_proba(self, X):
        Pm = np.asarray(self.precio_.predict_proba(X[self.cols_precio_]),
                        dtype="float64")
        Pj = np.asarray(self.juego_.predict_proba(X[self.cols_juego_]),
                        dtype="float64")
        lg = (self.w * np.log(np.clip(Pm, _EPS, 1.0))
              + (1.0 - self.w) * np.log(np.clip(Pj, _EPS, 1.0)))
        # Restar el máximo de la fila antes de exponenciar no cambia el resultado
        # —la renormalización lo cancela— y evita el desbordamiento cuando una de
        # las dos ternas da una probabilidad muy pequeña a la clase que ocurrió.
        lg -= lg.max(axis=1, keepdims=True)
        return _normaliza(np.exp(lg))

    def coeficientes(self, nombres=None) -> pd.DataFrame:
        """Los coeficientes de los dos submodelos, **ya escalados por el peso**.

        El argumento se ignora, como en el resto del estudio: rotular con una lista
        que no es la que el submodelo recibió desplazaría cada coeficiente a la
        variable equivocada.

        El escalado no es cosmético, es lo que hace que la tabla sea la del modelo
        publicado. El logit de la mezcla es por construcción `w` veces el del
        submodelo de precio más `1-w` veces el del de juego, de modo que multiplicar
        cada mitad por su peso deja a las 62 variables en una escala común y
        comparable: la de su aportación al logit que de verdad se publica. Sin
        escalar, un coeficiente de juego parecería seis veces más influyente de lo
        que es.
        """
        import explain as ex          # perezoso: `explain` importa el protocolo

        partes = []
        for peso, modelo, cols in ((self.w, self.precio_, self.cols_precio_),
                                   (1.0 - self.w, self.juego_, self.cols_juego_)):
            t = ex.linear_coefficients(modelo, cols)
            partes.append(t * peso)
        return pd.concat(partes).sort_values("magnitud", ascending=False)


def mezcla(w: float = 0.85, C: float = 0.3, l1_ratio: float = 0.0):
    """Fábrica de `mezcla`."""
    return lambda: EstimadorMezcla(w=w, C=C, l1_ratio=l1_ratio)


# ===========================================================================
# 7. El registro de familias y sus espacios declarados
# ===========================================================================
# Nombre de familia -> función que devuelve la fábrica. El banco manual instancia
# una familia sin conocer la firma de cada constructor; esta tabla es ese puente,
# y su orden es el del relato: lineal, promedio de árboles, suma de árboles,
# composición libre.
CONSTRUCTORES: dict = {
    "logistica": logistica,
    "bosque": bosque,
    "refuerzo_xgb": refuerzo_xgb,
    "red": red,
    "mezcla": mezcla,
}


# Rejilla declarada por familia. Se declara AQUÍ, junto a los modelos y antes de
# buscar, por dos razones. La primera es de método: un espacio que se retoca
# después de ver resultados deja de ser una búsqueda y pasa a ser un ajuste sobre
# el conjunto evaluado, y tenerlo escrito en el módulo hace que ese retoque sea
# visible en el control de versiones en lugar de silencioso. La segunda es de
# lectura: quien juzgue el trabajo puede comprobar que el rango explorado era
# razonable y que el óptimo encontrado no cae en un borde, que es la señal de que
# el espacio se quedó corto.
#
# Los valores son escasos a propósito. El afinado ocurre al final, sobre el
# conjunto de variables ya cerrado y con las rejillas cortas de
# `laboratorio.ESPACIOS_FINAL`; lo que se busca es distinguir órdenes de magnitud
# de regularización y de capacidad, no afinar el tercer decimal.
ESPACIOS: dict[str, dict[str, tuple]] = {
    "logistica": {
        "C": (0.03, 0.1, 0.3, 1.0, 3.0, 10.0),
        "l1_ratio": (0.0, 0.15, 0.5, 0.85, 1.0),
    },
    "bosque": {
        "n_estimators": (600,),
        "max_depth": (None, 8, 14),
        "min_samples_leaf": (10, 20, 40),
        "max_features": ("sqrt", 0.3),
    },
    "refuerzo_xgb": {
        "n_estimators": (200, 400, 800),
        "learning_rate": (0.02, 0.05, 0.1),
        "max_depth": (2, 3, 4),
        "min_child_weight": (5.0, 20.0, 50.0),
        "subsample": (0.7, 0.9),
        "colsample_bytree": (0.5, 0.8),
        "reg_lambda": (0.5, 1.0, 5.0),
    },
    "red": {
        "capas": ((32,), (64, 32), (128, 64)),
        "alpha": (1e-4, 1e-3, 1e-2),
        "learning_rate_init": (3e-4, 1e-3, 3e-3),
    },
    # El eje de la mezcla es el peso, y el espacio declarado es exactamente el que
    # recorrió `_mezcla.py`: veintiún valores de cero —solo juego— a uno —solo
    # precio—, en pasos de cinco centésimas. Los dos extremos están dentro a
    # propósito, porque son los dos modelos que la mezcla tiene que batir para
    # justificarse, y el valor adoptado, 0.85, no cae en un borde. `C` y
    # `l1_ratio` gobiernan las DOS logísticas a la vez y repiten el espacio de la
    # familia lineal; el estudio los dejó en la configuración fija del banco.
    "mezcla": {
        "w": (0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
              0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.0),
        "C": (0.03, 0.1, 0.3, 1.0, 3.0, 10.0),
        "l1_ratio": (0.0, 0.15, 0.5, 0.85, 1.0),
    },
}


def construye(nombre: str, **params):
    """Devuelve la fábrica de `nombre` configurada con `params`.

    Es la única puerta por la que el banco manual instancia una familia, y valida
    dos cosas antes de construir: que la familia exista y que todo parámetro pedido
    esté declarado en su `ESPACIOS`. Lo segundo puede parecer excesivo —el
    constructor ya fallaría con un argumento desconocido— pero atrapa el caso
    contrario, el del parámetro que la firma acepta y el espacio no declara: ese se
    colaría en silencio y la memoria diría que se buscó sobre una rejilla que no es
    la que se recorrió.
    """
    if nombre not in CONSTRUCTORES:
        raise KeyError(
            f"familia desconocida: {nombre!r}; "
            f"declaradas: {sorted(CONSTRUCTORES)}")
    declarados = set(ESPACIOS.get(nombre, {}))
    intrusos = sorted(set(params) - declarados)
    if intrusos:
        raise KeyError(
            f"parámetros no declarados en ESPACIOS[{nombre!r}]: {intrusos}; "
            f"declarados: {sorted(declarados)}")
    return CONSTRUCTORES[nombre](**params)


if __name__ == "__main__":       # pragma: no cover - guía de uso
    print(__doc__)
    for _n, _e in ESPACIOS.items():
        print(f"{_n:14s} {sorted(_e)}")
