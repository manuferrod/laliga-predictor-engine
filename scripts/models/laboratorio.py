"""El banco de pruebas manual de la fase de modelado.

QUÉ ES ESTE MÓDULO
==================
Un banco, no un buscador. Se le entrega una lista de columnas y una familia de
modelo, y devuelve lo que esa combinación rinde temporada a temporada frente al
apostante que sigue al favorito de Bet365. No elige nada, no recorre el espacio de
conjuntos por su cuenta y no publica ninguna decisión: elegir es trabajo de quien
lee la tabla.

La razón de que sea así y no un algoritmo voraz automático es de método, y conviene
dejarla escrita porque el tribunal la va a preguntar. Una búsqueda voraz sobre las
doscientas y pico columnas del preprocesado tiene dos problemas. El primero es de
coste: cada candidata exige reentrenar el protocolo entero —un ajuste por jornada
evaluada—, de modo que una sola ronda sobre doscientas columnas son doscientos
protocolos completos, y una selección de veinte variables son cuatro mil. El segundo
es más grave y no se arregla con máquina: una voraz que mira todas las columnas y se
queda con la que más sube el ROI está eligiendo sobre los mismos partidos con los que
después mide, y la cifra que publica ya no es una estimación del rendimiento futuro
sino el máximo de un ruido con doscientas oportunidades de parecer señal.

La alternativa que este módulo sirve es la que un analista defiende sin sonrojarse:
mirar primero qué rinde cada dominio de variables por separado —el mercado, la
fuerza de las plantillas, la forma, las bajas—, quedarse con los que aportan, y
componer a mano conjuntos pequeños cuyas variables se pueden justificar una por una.
El espacio explorado es minúsculo comparado con el de una voraz, y esa es
precisamente la propiedad que hace creíble la cifra final.

CÓMO SE MIDE
============
Con el mismo protocolo que todo lo demás en el proyecto, sin excepciones ni atajos:
`protocol.run_protocol` recorre las jornadas del bloque de validación en orden, y
para cada una entrena con los partidos **estrictamente anteriores** y predice la
jornada. El preprocesado se congela dentro de la ventana. El bloque 2025/26 no se
toca: es el holdout y se abre una sola vez, al final, y no desde aquí.

Las cifras se reportan por temporada además de en agregado. Una combinación que gana
solo en 2019/20 no es una combinación buena, es una temporada afortunada, y el corte
temporal es lo único que distingue una cosa de la otra.

QUÉ SE PUBLICA DE CADA ENSAYO
=============================
Acierto y RPS del lado probabilístico; rendimiento, cobertura y beneficio del lado
económico; y las tres cifras del apostante de referencia sobre exactamente los mismos
partidos, con la ventaja como resta. La referencia no es decorativa: en este dominio
la pregunta «¿acierta mucho?» no tiene respuesta interpretable sin un patrón de
comparación, porque el acierto depende de cuántos favoritos claros trajo el
calendario. Superar al mercado es la única forma de rendimiento que significa algo.

EL REGISTRO
===========
Cada ensayo se anota en `outputs/models/laboratorio/registro.parquet` con su
conjunto de columnas, su familia y todas sus cifras. El registro es acumulativo y
sobrevive al cierre del cuaderno, que es lo que permite trabajar en varias sesiones
sin perder el hilo de lo que ya se probó ni repetirlo por olvido. `historial()` lo
lee y `compara()` lo ordena.

EL COSTE, Y CÓMO SE PAGA MENOS
==============================
Un ensayo con la familia logística sobre el bloque de validación completo son unos
trescientos ajustes. Lo caro no es el ajuste sino el preprocesado que lo precede, y
ese estado no depende del estimador ni de las columnas elegidas: lo aprenden las
mismas filas y lo definen las mismas reglas. Por eso el banco mantiene una caché de
estados por configuración de protocolo y la reutiliza entre ensayos. El primer ensayo
de una sesión paga el preprocesado; los siguientes, con la misma configuración, no.

Para tantear, `evalua(..., desde="2021/22")` recorta el bloque evaluado a las
temporadas recientes y devuelve una respuesta en una fracción del tiempo. Sirve para
descartar, no para concluir: la cifra de un conjunto que se va a defender se calcula
sobre el bloque entero.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import backtest as bt  # noqa: E402
import columns as C  # noqa: E402
import estimators as es  # noqa: E402
import evaluate as ev  # noqa: E402
import explain as ex  # noqa: E402
import protocol as pt  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
FEATURES = PROC / "features_matches.parquet"
OUT = _ROOT / "outputs" / "models" / "laboratorio"
REGISTRO = OUT / "registro.parquet"


# ===========================================================================
# 1. Las familias y sus hiperparámetros declarados
# ===========================================================================
# La escalera del estudio son cuatro familias, una por cada hipótesis distinta sobre
# la forma de la relación entre las variables y el resultado: lineal en el logit
# (logística multinomial), promedio de árboles independientes (bosque), suma
# secuencial de árboles que se corrigen (refuerzo) y composición libre de
# interacciones (red). Añadir una quinta familia que comparta hipótesis con alguna de
# estas alarga la tabla sin añadir una respuesta.
#
# `mezcla` es la quinta y no comparte hipótesis con ninguna: no propone otra forma
# funcional sino otro reparto de la varianza —dos logísticas sobre mitades disjuntas
# combinadas en el logit por un peso—, y es la familia del modelo publicado. Se
# declara aquí porque `fabrica()` solo construye lo que esta tupla nombra, pero no
# entra en la escalera de `por_familia()`.
FAMILIAS: tuple[str, ...] = ("logistica", "bosque", "refuerzo_xgb", "red",
                             "mezcla")

# Configuración declarada por familia, fija durante toda la búsqueda de variables.
#
# Fijarla es lo que hace viable el trabajo manual: si cada conjunto candidato
# arrastrase su propia rejilla de hiperparámetros, comparar dos conjuntos costaría
# el producto de los dos espacios y ninguna sesión de trabajo terminaría. El precio
# de la decisión está declarado: un conjunto puede parecer peor que otro por llevar
# una configuración que no le convenía. Se paga a conciencia y se compensa al final,
# afinando sobre el conjunto ya elegido con `afina()`.
#
# Los valores son deliberadamente conservadores. Con ventanas de dos o tres mil
# partidos y señal débil, el riesgo dominante es el sobreajuste, no la falta de
# capacidad: hojas grandes, profundidad corta y regularización presente.
#
# Solo aparecen aquí parámetros declarados en `estimators.ESPACIOS`: es la puerta
# por la que pasa `construye`, y rechaza cualquier otro. La semilla y el número de
# iteraciones no se declaran porque no son decisiones del estudio sino defensas del
# constructor, y viven con su valor por defecto donde se escribieron.
HIPER: dict[str, dict] = {
    "logistica": {"C": 0.3, "l1_ratio": 0.0},
    "bosque": {"n_estimators": 600, "max_depth": None,
               "min_samples_leaf": 20, "max_features": "sqrt"},
    "refuerzo_xgb": {"n_estimators": 400, "learning_rate": 0.05,
                     "max_depth": 3, "min_child_weight": 20.0,
                     "subsample": 0.7, "colsample_bytree": 0.5,
                     "reg_lambda": 1.0},
    "red": {"capas": (64, 32), "alpha": 1e-3, "learning_rate_init": 1e-3},
    # El peso de la mezcla es el único parámetro del modelo publicado y no es un
    # valor conservador por omisión: se eligió sobre el bloque de `seleccion`, por
    # ACIERTO y no por ROI, recorriendo los veintiún valores que declara
    # `estimators.ESPACIOS`. Elegirlo por ROI daba 0.35, y fuera del bloque pierde
    # contra el 0.85 en las dos convenciones: +1.38 % contra +2.51 % a la mejor cuota
    # entre casas, −0.05 % contra +1.08 % de ventaja sobre el listón a cierre
    # promediado. El ROI sobre 1140 partidos tiene un error típico de ~3 pp y
    # seleccionar con él es seleccionar ruido. `C` y `l1_ratio`
    # gobiernan las dos logísticas y se quedan en la configuración de la familia
    # lineal, que es la que corrió el estudio.
    "mezcla": {"w": 0.85, "C": 0.3, "l1_ratio": 0.0},
}

# Rejillas cortas para el afinado final, ya con el conjunto de variables cerrado.
ESPACIOS_FINAL: dict[str, dict[str, tuple]] = {
    "logistica": {"C": (0.03, 0.1, 0.3, 1.0, 3.0)},
    "bosque": {"min_samples_leaf": (10, 20, 40),
               "max_features": ("sqrt", 0.3)},
    "refuerzo_xgb": {"max_depth": (2, 3, 4),
                     "learning_rate": (0.02, 0.05, 0.1)},
    "red": {"alpha": (1e-4, 1e-3, 1e-2),
            "learning_rate_init": (3e-4, 1e-3, 3e-3)},
}

# --- decisiones económicas por defecto -------------------------------------
# El precio de cierre es el que la memoria reporta: es el que un apostante real
# habría encontrado si hubiese esperado a tener la alineación, y es el más eficiente
# de los tres, de modo que batirlo es la afirmación más difícil y la única que vale
# la pena defender.
CUOTAS = "cierre_avg"
CASA = bt.REFERENCIA          # "b365"

# AVISO: la política del banco NO es la que se publica. Aquí se tantea con `valor`
# —apostar solo donde la probabilidad del modelo supera a la implícita en la cuota por
# `UMBRAL`—, que es la lectura con la que se recorrió la búsqueda de variables entera y
# la que hay escrita en las cifras de ROI de los estudios ya cerrados. La publicada es
# `build_models.POLITICA_ECONOMIA = "favorito"`: una unidad plana al signo que el modelo
# señala, en todos los partidos con precio. No son el mismo número y la diferencia no es
# pequeña —`_politica.py` mide 5.75 puntos en el bloque de selección y 1.84 fuera, a
# favor de `favorito`—, así que un ROI leído de `evalua` no se puede citar como el
# rendimiento publicado. El banco conserva `valor` a propósito: cambiarlo reescribiría
# el significado de las cifras de los estudios cerrados sin reejecutarlos.
POLITICA = "valor"
UMBRAL = bt.UMBRAL_VALOR
IMPORTE = "plana"
N_BOOT = 200                  # menos réplicas que en la publicación: aquí se tantea

METRICAS = ("accuracy", "rps", "logloss", "brier")


# ===========================================================================
# 2. El modelo restringido a unas columnas
# ===========================================================================
def _acepta(fn, nombre: str) -> bool:
    """Si un invocable admite un argumento con ese nombre."""
    try:
        return nombre in inspect.signature(fn).parameters
    except (TypeError, ValueError):        # pragma: no cover - builtins exóticos
        return False


class ModeloConColumnas:
    """Envoltura que entrega al estimador solo las columnas elegidas.

    Es la pieza que convierte «este conjunto de variables» en algo que el protocolo
    puede entrenar sin saber que hay una selección de por medio. El protocolo pasa
    la matriz completa de la ventana, ya preprocesada, y la envoltura se queda con
    las columnas nombradas.

    Guardar columnas por su nombre tiene un riesgo que conviene declarar: la poda
    por colinealidad se reajusta dentro de cada pliegue, de modo que una columna
    elegida sobre los datos de 2020 puede no sobrevivir a la poda de 2024. Aquí ese
    riesgo se asume a conciencia —seleccionar variables consiste precisamente en
    nombrarlas— y se maneja en vez de evitarse: las que faltan se anotan en
    `columnas_ausentes_` y el ajuste continúa con las que quedan. Solo si no queda
    ninguna se interrumpe, porque entonces no hay matriz que ajustar.

    Esa tolerancia tiene una consecuencia: el modelo de una temporada puede leer una
    columna menos que el de otra sin que el conjunto cambie de nombre. Por eso
    `columnas_ausentes_` no es información de depuración sino parte del resultado, y
    el ensayo la publica junto a las cifras.

    Declara `sample_weight` y `contexto` porque el protocolo inspecciona la firma
    para decidir qué le puede pasar, y los reenvía solo si el estimador base los
    admite.
    """

    def __init__(self, base, columnas):
        self.base = base
        self.columnas = tuple(dict.fromkeys(str(c) for c in columnas))
        if not self.columnas:
            raise ValueError("ModeloConColumnas necesita al menos una columna")

    def _resuelve(self, X: pd.DataFrame) -> list[str]:
        presentes = set(map(str, X.columns))
        self.columnas_ausentes_ = tuple(c for c in self.columnas
                                        if c not in presentes)
        elegidas = set(self.columnas)
        # Se respeta el orden de la matriz y no el de la lista elegida: así dos
        # conjuntos con las mismas columnas en distinto orden ajustan exactamente
        # el mismo modelo y sus coeficientes son comparables.
        cols = [c for c in map(str, X.columns) if c in elegidas]
        if not cols:
            raise ValueError(
                f"ninguna de las {len(self.columnas)} columnas elegidas sobrevive "
                f"en esta ventana; la matriz quedaría vacía")
        return cols

    def fit(self, X, y, sample_weight=None, contexto=None):
        self.columnas_ = self._resuelve(X)
        extra = {}
        if contexto is not None and _acepta(self.base.fit, "contexto"):
            extra["contexto"] = contexto
        if sample_weight is not None:
            if not _acepta(self.base.fit, "sample_weight"):
                raise TypeError(
                    f"se ha pedido peso de recencia pero "
                    f"{type(self.base).__name__}.fit no admite `sample_weight`")
            self.base.fit(X[self.columnas_], y, sample_weight=sample_weight, **extra)
        else:
            self.base.fit(X[self.columnas_], y, **extra)
        self.classes_ = self.base.classes_
        return self

    def predict_proba(self, X, contexto=None):
        if contexto is not None and _acepta(self.base.predict_proba, "contexto"):
            return self.base.predict_proba(X[self.columnas_], contexto=contexto)
        return self.base.predict_proba(X[self.columnas_])

    def coeficientes(self, nombres=None) -> pd.DataFrame:
        """Coeficientes del estimador base, rotulados con lo que de verdad recibió.

        El argumento se ignora a propósito: rotular con la lista completa cuando la
        poda retiró una columna desplazaría cada coeficiente a la variable
        equivocada, y el gráfico se dibujaría igual diciendo otra cosa.
        """
        return ex.linear_coefficients(self.base, self.columnas_)

    def __repr__(self) -> str:              # pragma: no cover - cosmético
        return (f"<{type(self.base).__name__} sobre {len(self.columnas)} columnas>")


def fabrica(familia: str, columnas, hiper: dict | None = None):
    """Devuelve la fábrica que el protocolo espera: un invocable sin argumentos.

    Cada llamada construye un estimador nuevo. Es requisito del protocolo y no un
    detalle: reutilizar la instancia entre pliegues arrastraría el ajuste de una
    jornada a la siguiente y convertiría el walk-forward en una ficción.
    """
    if familia not in FAMILIAS:
        raise ValueError(f"familia desconocida: {familia!r}; "
                         f"el estudio se reduce a {list(FAMILIAS)}")
    params = dict(HIPER[familia])
    params.update(hiper or {})
    cols = tuple(columnas)

    def _fabrica():
        # `construye` devuelve la fábrica de la familia, no el estimador: hay que
        # invocarla para que cada pliegue reciba una instancia recién hecha.
        return ModeloConColumnas(es.construye(familia, **params)(), cols)

    _fabrica.familia = familia
    _fabrica.params = params
    return _fabrica


# ===========================================================================
# 3. Los datos y el catálogo de variables por dominio
# ===========================================================================
_feat_cache: pd.DataFrame | None = None


def datos(ruta: Path | None = None, *, recarga: bool = False) -> pd.DataFrame:
    """La matriz de variables, leída una sola vez por sesión.

    Se devuelve entera —con el holdout dentro— porque el protocolo necesita ver
    todas las filas para construir sus ventanas; quien decide qué se evalúa es el
    argumento `evaluar` de `make_folds`, que por defecto es el bloque de validación.
    """
    global _feat_cache
    if _feat_cache is None or recarga or ruta is not None:
        _feat_cache = pd.read_parquet(ruta or FEATURES)
    return _feat_cache


def columnas_disponibles(feat: pd.DataFrame | None = None) -> list[str]:
    """Las columnas que el preprocesado admite como candidatas a variable.

    Es la lista de la que se elige. No incluye identificadores, ni fechas, ni el
    objetivo, ni las cuotas en escala cruda, que viajan junto a la predicción para
    la evaluación económica pero no entran en ninguna matriz. Tampoco incluye la
    probabilidad implícita de Bet365, que en este proyecto es la referencia contra
    la que se mide y no puede ser a la vez una de las variables que se miden.

    La lista no se construye aquí: se le pide a `preprocess.feature_candidates`,
    que es quien decide qué es una candidata cuando llega la hora de ajustar.
    Reconstruirla por separado sería tener dos definiciones que empiezan iguales y
    se separan en cuanto una de las dos cambie, y el banco ofrecería variables que
    el preprocesado no va a mirar —o peor, alguna que tiene prohibido mirar.

    El filtro de régimen se deja fuera a propósito: esta es la lista completa, y
    quién la recorta al bloque conocible en el momento de predecir es el cuaderno,
    a la vista, para que se lea qué excluye cada régimen y por qué.
    """
    import preprocess as pp
    feat = datos() if feat is None else feat
    return pp.feature_candidates(feat, regime=None)


def dominios(columnas=None) -> dict[str, list[str]]:
    """Dominio explicativo -> columnas, en el orden canónico.

    Los ocho dominios son los de `explain.BLOQUES_EXPLICATIVOS` y no una taxonomía
    nueva: lo que el lector reconoce como «una idea» al leer la memoria tiene que ser
    lo mismo que el conjunto de columnas que se prueba junto. Tener dos taxonomías
    obligaría a traducir entre ellas cada vez que se cuenta un resultado.
    """
    cols = columnas_disponibles() if columnas is None else list(columnas)
    return ex.mapa_bloques(cols)


def catalogo(columnas=None) -> pd.DataFrame:
    """Los dominios con su glosa y cuántas columnas trae cada uno.

    Es la tabla por la que se empieza: dice qué hay disponible y cómo se llama.
    """
    cols = columnas_disponibles() if columnas is None else list(columnas)
    return ex.block_registry(cols)


def variables(*dominios_pedidos: str, columnas=None) -> list[str]:
    """Las columnas de uno o varios dominios, concatenadas y sin repetir.

    Es el atajo con el que se componen los conjuntos a mano:

        lb.variables("mercado")                     # un dominio entero
        lb.variables("mercado", "fuerza", "forma")  # tres
    """
    mapa = dominios(columnas)
    desconocidos = [d for d in dominios_pedidos if d not in mapa]
    if desconocidos:
        raise KeyError(f"dominios sin columnas o inexistentes: {desconocidos}; "
                       f"disponibles {sorted(mapa)}")
    fuera: list[str] = []
    for d in dominios_pedidos:
        fuera.extend(mapa[d])
    return list(dict.fromkeys(fuera))


# ===========================================================================
# 4. La caché de estados de preprocesado
# ===========================================================================
# Lo caro de un ensayo no es ajustar el estimador sino congelar el preprocesado de
# cada ventana, y ese estado es independiente del estimador y de las columnas: lo
# aprenden las mismas filas y lo definen las mismas reglas. Compartirlo entre ensayos
# que usan la misma configuración de protocolo no cambia ninguna cifra y es la
# diferencia entre una sesión de trabajo y una tarde perdida.
_caches: dict[tuple, "pt._StateCache"] = {}
_folds: dict[tuple, list] = {}


def _clave(cfg: pt.ProtocolConfig, evaluar: str, desde: str | None) -> tuple:
    return (cfg.regime, cfg.window, cfg.sliding_seasons, cfg.refit_cadence,
            cfg.halflife_days, cfg.corr_threshold, cfg.transform,
            cfg.min_train_matches, tuple(cfg.warmup_seasons),
            cfg.corte_seleccion, evaluar, desde)


def pliegues(cfg: pt.ProtocolConfig, *, evaluar: str = "validacion",
             desde: str | None = None, feat: pd.DataFrame | None = None) -> list:
    """Los pliegues de una configuración, construidos una vez y reutilizados.

    `desde` recorta el bloque evaluado a las temporadas iguales o posteriores a la
    indicada. Las ventanas de entrenamiento no se recortan: un pliegue de 2023 se
    sigue entrenando con todo lo anterior. Lo que se ahorra es evaluar temporadas que
    en un tanteo no aportan.
    """
    feat = datos() if feat is None else feat
    k = _clave(cfg, evaluar, desde)
    if k not in _folds:
        fs = pt.make_folds(feat, cfg, evaluar=evaluar)
        if desde is not None:
            # El recorte es una comparación de cadenas, que sobre el rótulo de
            # temporada ordena bien pero perdona una barra escrita como guion:
            # «2023-24» se cuela porque el guion va antes que la barra en la tabla
            # de caracteres, y recorta casi por donde se pretendía. Casi. Un
            # rótulo que no existe en la matriz no es un recorte, es una errata, y
            # se para aquí antes de que devuelva pliegues plausibles.
            temporadas = sorted(set(map(str, feat[C.SEASON])))
            if str(desde) not in temporadas:
                raise ValueError(
                    f"desde={desde!r} no es una temporada de la matriz; el rótulo "
                    f"se escribe como en los datos ({temporadas[0]!r} … "
                    f"{temporadas[-1]!r})")
            fs = [f for f in fs if str(f.season) >= str(desde)]
            if not fs:
                raise ValueError(f"no queda ningún pliegue desde {desde!r}")
        _folds[k] = fs
    return _folds[k]


def _cache(cfg: pt.ProtocolConfig, evaluar: str, desde: str | None,
           feat: pd.DataFrame) -> "pt._StateCache":
    k = _clave(cfg, evaluar, desde)
    if k not in _caches:
        _caches[k] = pt._StateCache(feat, cfg)
    return _caches[k]


def estados(cfg: pt.ProtocolConfig, *, evaluar: str = "validacion",
            desde: str | None = None,
            feat: pd.DataFrame | None = None) -> dict:
    """Los estados de preprocesado de una configuración, para diagnóstico.

    La caché los guarda porque los reutiliza, pero mirarlos tiene valor por sí
    mismo: son la mediana de imputación y la λ de simetrización que cada ventana
    aprendió, y su deriva a lo largo del histórico es la respuesta a si el
    preprocesado está viendo el mismo fútbol en 2017 y en 2025. Se ajustan aquí
    todos los que falten, de modo que la tabla salga completa aunque todavía no se
    haya lanzado ningún ensayo; el coste se comparte con el primer ensayo que
    venga después, que ya los encuentra hechos.
    """
    feat = datos() if feat is None else feat
    cache = _cache(cfg, evaluar, desde, feat)
    for fold in pliegues(cfg, evaluar=evaluar, desde=desde, feat=feat):
        cache.get(fold)
    return cache.states


def olvida() -> None:
    """Vacía las cachés. Solo hace falta tras cambiar el parquet de variables."""
    _caches.clear()
    _folds.clear()


# ===========================================================================
# 5. El ensayo
# ===========================================================================
@dataclass
class Ensayo:
    """Una combinación de columnas y familia, ya medida.

    Contiene las predicciones fuera de muestra, las métricas por temporada y en
    agregado, y la comparación económica con el apostante de referencia. Es el
    objeto que se guarda en una variable del cuaderno para volver a mirarlo sin
    recalcular.
    """
    nombre: str
    familia: str
    columnas: tuple[str, ...]
    hiper: dict
    cfg: pt.ProtocolConfig
    pred: pd.DataFrame
    metricas: dict
    por_temporada: pd.DataFrame
    economia: dict
    ausentes: tuple[str, ...] = ()
    sello: str = ""

    # --- lecturas ------------------------------------------------------------
    @property
    def accuracy(self) -> float:
        return float(self.metricas["accuracy"])

    @property
    def rps(self) -> float:
        return float(self.metricas["rps"])

    @property
    def roi(self) -> float:
        return float(self.economia["modelo"]["rendimiento"])

    @property
    def roi_referencia(self) -> float:
        return float(self.economia["referencia"]["rendimiento"])

    @property
    def ventaja(self) -> float:
        """Rendimiento del modelo menos el del apostante de referencia."""
        return float(self.economia["diferencia"])

    @property
    def efectivas(self) -> tuple[str, ...]:
        """Las columnas que llegaron a entrar en alguna matriz.

        Es `columnas` menos `ausentes`. La procedencia de un conjunto es la de lo que
        de verdad se ajustó: una columna que no entró en ningún pliegue no aporta
        dominio, y si además se pidió con un nombre que la tabla de procedencia no
        conoce, preguntar por su origen detendría el ensayo en lugar de describirlo.
        """
        fuera = set(self.ausentes)
        return tuple(c for c in self.columnas if c not in fuera)

    def fila(self) -> dict:
        """Una línea con todo lo que resume el ensayo. Es lo que se registra."""
        m, r = self.economia["modelo"], self.economia["referencia"]
        iv = self.economia.get("intervalo") or {}
        return {
            "nombre": self.nombre,
            "familia": self.familia,
            "n_variables": len(self.columnas),
            "dominios": ", ".join(sorted(dominios(self.efectivas))),
            "n": int(self.metricas["n"]),
            "accuracy": self.accuracy,
            "rps": self.rps,
            "logloss": float(self.metricas["logloss"]),
            "brier": float(self.metricas["brier"]),
            "acc_referencia": float(r.get("acierto", np.nan)),
            "roi": self.roi,
            "roi_referencia": self.roi_referencia,
            "ventaja": self.ventaja,
            "ventaja_inf": float(iv.get("inf", np.nan)),
            "ventaja_sup": float(iv.get("sup", np.nan)),
            "n_apuestas": int(m.get("n_apuestas", 0)),
            "cobertura": float(m.get("cobertura", np.nan)),
            "beneficio": float(m.get("beneficio", np.nan)),
            "caida_max_rel": float(m.get("caida_max_rel", np.nan)),
            "n_ausentes": len(self.ausentes),
            "sello": self.sello,
            "variables": " + ".join(self.columnas),
        }

    def tabla(self) -> pd.DataFrame:
        """Temporada a temporada: acierto, RPS y las dos cifras económicas.

        Es la lectura que decide. Un conjunto que mejora al mercado en el agregado
        pero solo gana en dos de ocho temporadas no ha mejorado al mercado: ha
        tenido suerte dos veces, y esta tabla es donde eso se ve.
        """
        izq = self.por_temporada[["n", "accuracy", "rps"]].copy()
        eco = self.economia.get("por_temporada")
        if eco is None or eco.empty:
            return izq.reset_index()
        # `comparacion_por_temporada` sufija la referencia como `_ref`; aquí se
        # renombra a `_referencia` para que la tabla se lea sin diccionario.
        eco = eco.set_index("season").rename(columns={
            "n_ref": "n_referencia",
            "acierto_ref": "acierto_referencia",
            "beneficio_ref": "beneficio_referencia",
            "rendimiento_ref": "rendimiento_referencia",
            "cuota_media_ref": "cuota_media_referencia",
        })
        cols = [c for c in ("n_modelo", "acierto_modelo", "acierto_referencia",
                            "rendimiento_modelo", "rendimiento_referencia",
                            "diferencia", "dif_inf", "dif_sup")
                if c in eco.columns]
        return izq.join(eco[cols], how="left").reset_index()

    def resumen(self) -> str:
        """Una línea legible, para imprimir tras cada ensayo."""
        return (f"{self.nombre}  ·  {self.familia}  ·  {len(self.columnas)} vars"
                f"\n  acierto {self.accuracy:6.2%}  (referencia "
                f"{self.economia['referencia'].get('acierto', float('nan')):6.2%})"
                f"   RPS {self.rps:.5f}"
                f"\n  ROI     {self.roi:+6.2%}  (referencia "
                f"{self.roi_referencia:+6.2%})"
                f"   ventaja {self.ventaja:+.2%}")

    def __repr__(self) -> str:              # pragma: no cover - cosmético
        return (f"<Ensayo {self.nombre!r} {self.familia} "
                f"{len(self.columnas)}v acc={self.accuracy:.4f} "
                f"roi={self.roi:+.4f}>")


def _sello(familia: str, columnas, hiper: dict, cfg: pt.ProtocolConfig,
           evaluar: str, desde: str | None) -> str:
    """Huella de un ensayo: misma huella, mismas cifras.

    Sirve para reconocer en el registro que una combinación ya se probó, incluso si
    se le puso otro nombre. Sin ella, el registro acumularía duplicados con rótulos
    distintos y la comparación entre sesiones sería engañosa.
    """
    crudo = json.dumps({
        "familia": familia,
        "columnas": sorted(map(str, columnas)),
        "hiper": {k: str(v) for k, v in sorted(hiper.items())},
        "cfg": cfg.describe(),
        "evaluar": evaluar,
        "desde": desde,
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(crudo.encode("utf-8")).hexdigest()[:12]


def evalua(columnas, familia: str = "logistica", *, nombre: str | None = None,
           hiper: dict | None = None, cfg: pt.ProtocolConfig | None = None,
           feat: pd.DataFrame | None = None, evaluar: str = "validacion",
           desde: str | None = None, cuotas: str = CUOTAS, casa: str = CASA,
           politica: str = POLITICA, umbral: float = UMBRAL,
           importe: str = IMPORTE, n_boot: int = N_BOOT,
           registrar: bool = True, verbose: bool = True) -> Ensayo:
    """Mide un conjunto de columnas con una familia. El corazón del banco.

    No decide nada. Devuelve un `Ensayo` con las cifras, lo imprime si se le pide y
    lo anota en el registro. Qué se hace con el resultado es asunto de quien lo lee.

    `desde` recorta las temporadas evaluadas para tantear rápido; las ventanas de
    entrenamiento siguen siendo completas. Un ensayo recortado se anota en el
    registro con su propio sello, de modo que nunca se confunde con uno completo.
    """
    feat = datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    cols = tuple(dict.fromkeys(str(c) for c in columnas))
    if not cols:
        raise ValueError("un ensayo necesita al menos una columna")
    hiper = dict(hiper or {})
    nombre = nombre or f"{familia}·{len(cols)}v"

    if evaluar == "holdout":
        raise PermissionError(
            "el banco no evalúa sobre el holdout. El bloque 2025/26 se abre una "
            "sola vez, al final, desde build_models.py --holdout, y con el "
            "conjunto ya cerrado. Abrirlo aquí convertiría cada tanteo en una "
            "mirada al examen")
    if evaluar == "produccion":
        raise PermissionError(
            "el banco no evalúa sobre la temporada en curso. Toda jornada "
            "posterior al holdout se entrena CON el holdout, así que comparar "
            "familias ahí sería elegir mirando la temporada sellada por la "
            "puerta de atrás. El bloque 'produccion' es de lectura —cómo va el "
            "modelo ya cerrado en la temporada que se juega—, no de decisión")

    fs = pliegues(cfg, evaluar=evaluar, desde=desde, feat=feat)
    cache = _cache(cfg, evaluar, desde, feat)

    if verbose:
        print(f"· {nombre}: {len(cols)} variables, familia {familia}, "
              f"{len(fs)} pliegues…", flush=True)

    fab = fabrica(familia, cols, hiper)
    pred = pt.run_protocol(feat, fab, cfg, evaluar=evaluar, folds=fs, cache=cache)
    pred = bt.attach_odds(pred, feat, cuotas=cuotas)

    metricas = ev.metrics(pred, completo=False)
    portemp = ev.metrics_by_season(pred)
    economia = bt.compara_con_mercado(
        pred, casa=casa, politica=politica, umbral=umbral, importe=importe,
        n_boot=n_boot, intervalos_temporada=False, verbose=False)

    # Las columnas que la poda por colinealidad se llevó en TODOS los estados de
    # preprocesado, es decir, las que el estimador no llegó a ver en ningún pliegue.
    # No es un error: es información sobre el conjunto, y se publica con él, porque
    # un conjunto de doce variables del que tres nunca entran es en realidad un
    # conjunto de nueve y la memoria no puede decir otra cosa.
    vistas: set[str] = set()
    for st in (pred.attrs.get("states") or {}).values():
        vistas.update(map(str, getattr(st, "feature_cols", ())))
    ausentes = tuple(sorted(set(cols) - vistas)) if vistas else ()

    ensayo = Ensayo(
        nombre=nombre, familia=familia, columnas=cols, hiper=fab.params, cfg=cfg,
        pred=pred, metricas=metricas, por_temporada=portemp, economia=economia,
        ausentes=ausentes,
        sello=_sello(familia, cols, fab.params, cfg, evaluar, desde))

    if registrar:
        registra(ensayo, desde=desde)
    if verbose:
        print(ensayo.resumen(), flush=True)
    return ensayo


def ajusta(columnas, familia: str = "logistica", *, hiper: dict | None = None,
           cfg: pt.ProtocolConfig | None = None, feat: pd.DataFrame | None = None,
           evaluar: str = "validacion", desde: str | None = None,
           n_pliegues: int = 6) -> list:
    """Reentrena los últimos pliegues y devuelve los ajustes, para interpretar.

    La evaluación no conserva los modelos: `run_protocol` entrena, predice y
    descarta, que es lo correcto cuando lo que se mide son las predicciones fuera
    de muestra. La interpretabilidad necesita lo contrario —el estimador y la
    matriz con la que se le entrenó—, así que se rehacen aquí unos pocos pliegues,
    los más recientes, que son los que describen el modelo que se publicaría.

    Se usa la misma caché de estados que la evaluación, de modo que reentrenar seis
    pliegues no vuelve a ajustar ningún preprocesado.
    """
    feat = datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    cols = tuple(dict.fromkeys(str(c) for c in columnas))
    if not cols:
        raise ValueError("hacen falta columnas para ajustar")
    if evaluar in ("holdout", "produccion"):
        raise PermissionError(
            f"el banco no ajusta sobre el bloque {evaluar!r}: el holdout se abre "
            "una sola vez desde build_models.py, y la temporada en curso se "
            "entrena con él")

    fs = pliegues(cfg, evaluar=evaluar, desde=desde, feat=feat)
    cache = _cache(cfg, evaluar, desde, feat)
    fab = fabrica(familia, cols, dict(hiper or {}))
    elegidos = fs[-int(n_pliegues):] if n_pliegues else fs
    return [pt.fit_fold(feat, fab, cfg, f, cache=cache) for f in elegidos]


# ===========================================================================
# 6. Comparar, y la ronda que puntúa sin decidir
# ===========================================================================
def compara(*ensayos: Ensayo, ordenar: str = "ventaja") -> pd.DataFrame:
    """Varios ensayos, una fila cada uno, ordenados por lo que se le pida.

    Las columnas son siempre las mismas y en el mismo orden aunque los ensayos
    vengan de familias distintas: comparar exige una rejilla común.
    """
    if not ensayos:
        raise ValueError("compara necesita al menos un ensayo")
    tabla = pd.DataFrame([e.fila() for e in ensayos])
    if ordenar:
        if ordenar not in tabla.columns:
            raise KeyError(f"no se puede ordenar por {ordenar!r}; "
                           f"columnas {list(tabla.columns)}")
        creciente = ordenar in ("rps", "logloss", "brier", "n_variables")
        tabla = tabla.sort_values(ordenar, ascending=creciente)
    return tabla.reset_index(drop=True)


def ronda(base, candidatas, familia: str = "logistica", *,
          etiqueta: str = "ronda", **kw) -> pd.DataFrame:
    """Puntúa `base + una candidata` para cada candidata, y devuelve la tabla.

    Esto es un paso de selección hacia delante hecho a mano, y la palabra importante
    es *hecha a mano*: la función NO adopta ninguna candidata, NO itera y NO devuelve
    un ganador. Devuelve una tabla ordenada para que quien la lea decida —o decida
    que ninguna merece entrar, que es una decisión igual de válida y la que el ruido
    aconseja más a menudo de lo que parece.

    La primera fila de la tabla es siempre el conjunto base sin añadir nada, para que
    la mejora de cada candidata se lea como diferencia contra algo y no en el vacío.

    Coste: una candidata es un protocolo completo. Con la caché de estados caliente y
    la familia logística, unas decenas de candidatas son manejables; con la red o el
    refuerzo, conviene recortar con `desde` o pasar menos candidatas.
    """
    base = list(dict.fromkeys(str(c) for c in base))
    candidatas = [str(c) for c in candidatas if str(c) not in set(base)]
    if not candidatas:
        raise ValueError("no queda ninguna candidata fuera del conjunto base")
    # La ronda imprime su propia línea por candidata; el detalle de cada ensayo
    # sobraría y taparía la tabla, que es lo que aquí se viene a leer.
    kw.pop("verbose", None)

    filas = []
    if base:
        e0 = evalua(base, familia, nombre=f"{etiqueta}·base", verbose=False, **kw)
        f0 = e0.fila()
        f0.update({"candidata": "(base)", "d_accuracy": 0.0, "d_rps": 0.0,
                   "d_roi": 0.0, "d_ventaja": 0.0})
        filas.append(f0)
        ref = e0
        print(f"  base: {len(base)} vars · acc {e0.accuracy:6.2%}  "
              f"RPS {e0.rps:.5f}  ventaja {e0.ventaja:+.2%}", flush=True)
    else:
        ref = None

    fallos = []
    for i, c in enumerate(candidatas, 1):
        try:
            e = evalua(base + [c], familia, nombre=f"{etiqueta}·{c}",
                       verbose=False, **kw)
        except Exception as exc:                    # noqa: BLE001
            # Una candidata que no se puede ajustar es un resultado, no un corte:
            # la ronda anota el motivo y sigue con las demás.
            fallos.append({"candidata": c, "motivo": f"{type(exc).__name__}: {exc}"})
            print(f"  [{i:>3}/{len(candidatas)}] {c:<40} no evaluable — {exc}",
                  flush=True)
            continue
        f = e.fila()
        f["candidata"] = c
        if ref is not None:
            f["d_accuracy"] = e.accuracy - ref.accuracy
            f["d_rps"] = e.rps - ref.rps
            f["d_roi"] = e.roi - ref.roi
            f["d_ventaja"] = e.ventaja - ref.ventaja
        filas.append(f)
        print(f"  [{i:>3}/{len(candidatas)}] {c:<40} "
              f"acc {e.accuracy:6.2%}  RPS {e.rps:.5f}  "
              f"ventaja {e.ventaja:+.2%}", flush=True)

    tabla = pd.DataFrame(filas)
    if fallos:
        tabla.attrs["no_evaluables"] = fallos
    cabeza = ["candidata", "accuracy", "d_accuracy", "rps", "d_rps",
              "roi", "d_roi", "ventaja", "d_ventaja", "n_apuestas", "cobertura"]
    resto = [c for c in tabla.columns if c not in cabeza]
    return tabla[[c for c in cabeza if c in tabla.columns] + resto]


def por_dominio(familia: str = "logistica", *, dominios_pedidos=None,
                columnas=None, **kw) -> pd.DataFrame:
    """Un ensayo por dominio, cada uno con sus columnas y nada más.

    Es el primer experimento del estudio y responde a la pregunta con la que empieza
    cualquier selección honesta: ¿cuánto vale cada familia de variables por sí sola?
    La respuesta ordena el trabajo posterior, porque los dominios que no baten al
    azar por separado rara vez aportan dentro de una mezcla.

    `columnas` es el conjunto del que se reparten los dominios, y se declara aquí en
    vez de dejarlo caer en `**kw` por dos razones. La primera es que en `evalua` ese
    mismo nombre significa otra cosa —las variables de UN ensayo— y reenviarlo choca
    con el primer parámetro. La segunda pesa más: quien llama a esta función pasa la
    lista ya recortada por el régimen, y si el recorte no llegase hasta aquí el
    reconocimiento se haría sobre columnas que el modelo publicable no puede usar.
    """
    mapa = dominios(columnas)
    pedidos = list(dominios_pedidos) if dominios_pedidos else list(mapa)
    ensayos, fallos = [], []
    for d in pedidos:
        cols = mapa.get(d)
        if not cols:
            fallos.append({"dominio": d, "motivo": "sin columnas"})
            print(f"· {d}: sin columnas, se omite")
            continue
        try:
            ensayos.append(evalua(cols, familia, nombre=f"solo·{d}", **kw))
        except Exception as exc:                    # noqa: BLE001
            # Un dominio cuyas columnas no sobreviven al preprocesado —o que la
            # familia no puede ajustar— se anota y no interrumpe el recorrido.
            fallos.append({"dominio": d, "motivo": f"{type(exc).__name__}: {exc}"})
            print(f"· {d}: no evaluable — {exc}")
    if not ensayos:
        tabla = pd.DataFrame()
        tabla.attrs["no_evaluables"] = fallos
        return tabla
    tabla = compara(*ensayos)
    tabla.insert(0, "dominio", [str(n).split("·", 1)[1] for n in tabla["nombre"]])
    tabla = tabla.sort_values("ventaja", ascending=False).reset_index(drop=True)
    if fallos:
        tabla.attrs["no_evaluables"] = fallos
    return tabla


ESCALERA: tuple[str, ...] = ("logistica", "bosque", "refuerzo_xgb", "red")


def por_familia(columnas, familias=ESCALERA, **kw) -> pd.DataFrame:
    """El mismo conjunto de columnas, con cada una de las cuatro familias.

    Recorre `ESCALERA` y no `FAMILIAS`: la mezcla necesita que el conjunto traiga
    las dos mitades y se detendría con cualquier conjunto que no las tenga, de modo
    que incluirla por omisión convertiría un recorrido legítimo en un fallo. Quien
    la quiera en la tabla la nombra.

    Una familia que falla al instanciarse —el refuerzo si falta XGBoost— se registra
    con su motivo en lugar de detener el recorrido: que una familia no pudiera
    probarse es un resultado del trabajo y tiene que quedar escrito.
    """
    ensayos, fallos = [], []
    for f in familias:
        try:
            ensayos.append(evalua(columnas, f, nombre=f"{f}·{len(list(columnas))}v",
                                  **kw))
        except Exception as exc:                    # noqa: BLE001
            fallos.append({"familia": f, "motivo": f"{type(exc).__name__}: {exc}"})
            print(f"· {f}: no aplicable — {exc}")
    tabla = compara(*ensayos) if ensayos else pd.DataFrame()
    if fallos:
        tabla.attrs["no_aplicables"] = fallos
    return tabla


def afina(columnas, familia: str, *, espacio: dict | None = None,
          **kw) -> pd.DataFrame:
    """Rejilla corta de hiperparámetros, ya con el conjunto de variables cerrado.

    Es el último paso y va deliberadamente al final. Afinar antes de cerrar las
    variables multiplicaría el coste de cada comparación de conjuntos por el tamaño
    de la rejilla, y afinar sobre un conjunto que todavía va a cambiar es trabajo que
    se tira. Cerrado el conjunto, la rejilla cuesta lo que cuesta una sola vez.
    """
    from itertools import product
    espacio = espacio if espacio is not None else ESPACIOS_FINAL.get(familia, {})
    if not espacio:
        raise ValueError(f"no hay rejilla declarada para {familia!r}")
    claves = list(espacio)
    ensayos = []
    for valores in product(*(espacio[k] for k in claves)):
        h = dict(zip(claves, valores))
        etiqueta = ", ".join(f"{k}={v}" for k, v in h.items())
        ensayos.append(evalua(columnas, familia, nombre=f"afina·{etiqueta}",
                              hiper=h, **kw))
    return compara(*ensayos, ordenar="rps")


# ===========================================================================
# 7. El registro persistente
# ===========================================================================
def registra(ensayo: Ensayo, *, desde: str | None = None,
             ruta: Path | None = None) -> Path:
    """Anota un ensayo en el registro acumulativo.

    El registro es un parquet que crece. Si el sello ya está, la fila se sustituye:
    volver a medir la misma combinación tiene que dar lo mismo, y si diese otra cosa
    lo que hay que arreglar no es el registro.
    """
    ruta = ruta or REGISTRO
    ruta.parent.mkdir(parents=True, exist_ok=True)
    fila = ensayo.fila()
    fila["desde"] = desde or ""
    fila["protocolo"] = ensayo.cfg.describe()
    fila["momento"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    previo = pd.read_parquet(ruta) if ruta.exists() else pd.DataFrame()
    if not previo.empty and "sello" in previo.columns:
        previo = previo[previo["sello"] != ensayo.sello]
    nuevo = pd.concat([previo, pd.DataFrame([fila])], ignore_index=True)
    nuevo.to_parquet(ruta, index=False)
    return ruta


def historial(ruta: Path | None = None, *, familia: str | None = None,
              ordenar: str = "ventaja") -> pd.DataFrame:
    """Todo lo que se ha probado, en todas las sesiones.

    Es la memoria del estudio. Sin ella, cada sesión empezaría de cero y la
    tentación de repetir lo ya descartado sería constante.
    """
    ruta = ruta or REGISTRO
    if not ruta.exists():
        return pd.DataFrame()
    tabla = pd.read_parquet(ruta)
    if familia is not None:
        tabla = tabla[tabla["familia"] == familia]
    if ordenar and ordenar in tabla.columns:
        creciente = ordenar in ("rps", "logloss", "brier", "n_variables")
        tabla = tabla.sort_values(ordenar, ascending=creciente)
    return tabla.reset_index(drop=True)


def conjunto(nombre: str, ruta: Path | None = None) -> list[str]:
    """Recupera del registro la lista de columnas de un ensayo, por su nombre.

    Permite retomar en una sesión nueva el conjunto con el que se cerró la anterior
    sin tenerlo copiado en ningún sitio.
    """
    tabla = historial(ruta, ordenar="")
    fila = tabla[tabla["nombre"] == nombre]
    if fila.empty:
        raise KeyError(f"no hay ningún ensayo llamado {nombre!r} en el registro")
    return str(fila.iloc[-1]["variables"]).split(" + ")


# Fin del banco. Lo que sigue —qué conjunto se elige y con qué familia— no vive
# aquí: vive en el cuaderno 05, que es donde se decide, y en build_models.py, que
# es donde lo decidido se convierte en artefacto.
