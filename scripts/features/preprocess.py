"""preprocess.py — módulo REPRODUCIBLE de preprocesado (fit-dev / apply-holdout+upcoming).

Codifica las decisiones del análisis por familia de variables que se documenta y
justifica en el notebook `04_ANALISIS_Y_PREPROCESADO.ipynb`. El reparto de papeles
es estricto: el notebook MUESTRA y AUDITA lo que este módulo HACE, de modo que
existe una única implementación de cada decisión.

Decisiones implementadas
------------------------
D0  Valor de plantilla laggeado a t-1 (en `squadvalue.py`): un partido de la
    temporada `s` usa el valor de `s-1` (anti-fuga). Los ascendidos quedan sin
    valor -> cold-start, que se trata aquí (D2, `weak_prior`).
D1  Mercado: se MANTIENE como feature, y además la probabilidad implícita se
    conserva SIN TRANSFORMAR en columnas `*_raw` para poder calcular el baseline de
    mercado (el número a batir) desde el propio artefacto. Las `mkt_p_*` que ve el
    modelo pasan por las mismas capas que el resto de features (D5/D6), de modo que
    una vez escaladas ya NO son probabilidades; sin la copia cruda, el baseline
    operaría sobre valores estandarizados —con negativos y sumas distintas de 1— y
    quedaría invalidado. Las columnas `_raw` son METADATO, no features: están en
    `_NON_FEATURE` y nunca entran en `feature_cols`, así que no pueden filtrarse al
    modelo.

    El baseline se conserva POR TRIPLICADO porque hay tres lecturas legítimas del
    precio y no es evidente a priori cuál es la exigente. `mkt_p_*` resume el
    CONSENSO (media de las casas recogidas); `mkt_p_ps_*` es el precio de Pinnacle,
    que opera con margen estrecho y admite apostantes informados en lugar de
    limitarlos, por lo que su cotización suele estar mejor calibrada que la media del
    sector; `mkt_p_b365_*` es el precio de Bet365, la casa minorista de gran difusión
    y la referencia que el lector reconoce. Batir un consenso peor que el mejor precio
    disponible no demostraría nada, así que las tres se calculan y el número a batir
    que se declara en la memoria es el MEJOR de ellas (`baselines_report`).

    De las tres, solo el consenso es VARIABLE del modelo. Pinnacle y Bet365 existen
    únicamente como referencia de evaluación: el consenso ya incorpora el precio de
    Bet365 —es una de las casas que promedia—, de modo que añadirla como feature
    aportaría colinealidad casi perfecta y ninguna información, y la poda tendría que
    romper el par de forma arbitraria. Las ternas de referencia están protegidas de
    esa poda, que de otro modo eliminaría una por su correlación con otra y dejaría el
    benchmark sin poder calcularse.
D2  Valores faltantes: imputación POR CAUSA (no una mediana ciega). Cada NaN
    tiene un origen distinto y se trata según su significado:
      * `weak_prior`  ascendidos sin valor t-1 en Primera -> cuantil BAJO de dev.
                      Un ascendido NO es un equipo promedio: su fuerza económica
                      cae en la cola inferior de la distribución, así que se imputa
                      con un cuantil bajo (no con la mediana). Solo valor/tamaño de
                      plantilla; los diferenciales derivados se RECALCULAN para que
                      el signo sea coherente.
      * `zero`        ausencia REAL cuyo neutro es 0: sin historial de
                      enfrentamientos (H2H) o sin partidos en ese campo (venue).
      * `known`       el hueco tiene un valor EXACTO deducible del propio dominio,
                      de modo que estimarlo sería sustituir un dato cierto por uno
                      inventado. Es el caso del bloque de trascendencia en la
                      primera jornada: antes de que se dispute un solo partido la
                      tabla está vacía, todos los equipos comparten los mismos cero
                      puntos y por tanto TODA distancia a un puesto de corte
                      —liderato, Champions, Europa, permanencia— vale exactamente 0;
                      por la misma razón ningún objetivo está decidido, así que los
                      indicadores de salvación matemática, de Europa inalcanzable y
                      de partido intrascendente valen 0; y restan por jugar los 38
                      partidos del calendario. Ninguna de esas cifras se estima:
                      se sabe.
      * `neutral`     mediana de dev (≈ "equipo promedio, aún sin señal"): cold-
                      start de forma y huecos de disponibilidad por época. También
                      la posición previa y su variación dinámica, que en la primera
                      jornada sí son genuinamente desconocidas —no hay clasificación
                      de la que leerlas— y no admiten por tanto el trato anterior.
    Además, un FLAG binario por CAUSA (no por columna) para que el modelo pueda
    distinguir un valor imputado de uno real.
D3  Redundancia: poda por reglas -> (a) columnas constantes, (b) colineales
    casi-perfectos (greedy sobre |r|>umbral, se queda el de mayor utilidad
    MI*cobertura, protegiendo el baseline de mercado).

    Las dos reglas que quedan son las dos que dependen de los DATOS: si una
    columna es constante o si dos columnas van juntas no se sabe hasta que se
    miran las filas de dev. Todo lo que se sabía de antemano ya no se poda aquí
    porque ya no se construye: el duplicado determinista de `elo_diff`, los
    residuos de suerte frente al xG y las columnas de señal nula los excluye
    `catalogo.py` (reglas 2 y 5) antes de que ningún módulo las calcule. Podarlas
    al final era describir un conjunto de variables que el proyecto no tiene, y
    dejaba en el informe la huella de un trabajo hecho para tirarlo.
D4  Categóricas (`position_zone`, `playstyle`, `kickoff_slot`): encoding ORDINAL.
    Las tres admiten un orden real —zona de la tabla, carácter del juego y franja
    horaria—, así que el código conserva esa relación sin postular distancias que
    la variable no tiene.
D5  Simetrización de colas: transformación de potencia Yeo-Johnson (λ ajustado por
    MLE SOLO en dev y congelado) sobre las columnas continuas con asimetría fuerte
    (|skew|>1). Yeo-Johnson admite valores negativos, así que sirve también para los
    diferenciales (`*_diff`). Se excluyen las variables acotadas en [0,1]
    (probabilidades) y las categóricas. Capa activable (`transform=True` por defecto).
D6  Escalado: estandarización (media/desv. aprendidas SOLO en dev, sobre los
    valores YA imputados/recalculados/transformados, para que dev quede exactamente
    media 0 / desv 1).

Disciplina anti-fuga (paridad train<->inferencia)
-------------------------------------------------
TODO estadístico (cuantiles, medianas, medias, desviaciones, correlaciones, MI y
la lista de columnas a tirar) se aprende EXCLUSIVAMENTE en dev dentro de `fit()`
y se CONGELA en `PreprocessState`. `apply()` nunca vuelve a mirar holdout/upcoming:
solo reproduce las mismas operaciones con los mismos parámetros.

Régimen de información
----------------------
El preprocesado se ajusta bajo un RÉGIMEN, que declara qué columnas se consideran
conocibles en el momento de emitir la predicción (`columns.regime_columns`). El
régimen se fija en `fit()`, se congela en el estado y gobierna todas las capas
posteriores, no solo la selección final de variables. La razón es que la poda
colineal (D3) es supervisada y compara pares: la cuota de apertura y su gemela de
cierre correlacionan por encima de cualquier umbral razonable, de modo que ajustar
con ambas presentes y filtrar después dejaría al régimen semanal sin ninguna de las
dos. Por el mismo motivo se filtran también los flags de faltante: un flag cuya
columna fuente no es conocible al predecir valdría 0 casi siempre en entrenamiento
y 1 siempre en inferencia, que es exactamente el desajuste que el régimen previene.

Cada régimen produce por tanto su propio par de artefactos, y la ruta de inferencia
semanal consume únicamente el estado del régimen `semanal`.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import columns as C  # noqa: E402
# La regla de des-margen se importa del módulo que la escribió en lugar de
# reescribirla: la probabilidad de Pinnacle que este módulo reconstruye tiene que
# ser la misma función de las cuotas que produjo `mkt_p_*` en su día —mismo mínimo
# de cuota válida, mismo rango de overround admisible—, o los tres precios de
# referencia dejarían de estar medidos con la misma vara.
from prematch_derived import _implied_probs as _probs_de_cuotas  # noqa: E402

# Régimen por defecto: el que se publica. Ajustar sin decir nada nunca puede
# producir el modelo con más información, solo el más conservador.
REGIMES = C.REGIMES
DEFAULT_REGIME = "semanal"

# --- identidad / no-features --------------------------------------------------
KEYS = ["match_key", "season", "season_year", "date", "home_team", "away_team",
        "home_key", "away_key", "match_id", "round"]
TARGET = "y"
TARGET_NUM = "y_num"
SPLIT = C.SPLIT

# --- baseline de mercado (D1): probabilidad implícita conservada sin transformar --
# Las columnas de probabilidad del consenso y de Pinnacle son features (se imputan,
# transforman y escalan como el resto). Las `*_raw` son su copia intacta, adjuntada
# por `apply()` como METADATO para que los baselines dispongan de probabilidades de
# verdad en el artefacto.
#   MARKET_* -> consenso de las casas recogidas.
#   SHARP_*  -> Pinnacle, precio de referencia informado (margen estrecho).
#   B365_*   -> Bet365, casa minorista de gran difusión.
#
# Bet365 es el caso distinto y merece su propio párrafo: entra SOLO como referencia
# de evaluación y nunca como variable. La razón es que su precio ya está dentro del
# consenso —el consenso es la media de las casas, y Bet365 es una de ellas—, de modo
# que añadirlo a la matriz no aportaría información nueva y sí una colinealidad casi
# perfecta con `mkt_p_*`, justo el tipo de pareja que la poda tendría que deshacer
# eligiendo arbitrariamente una de las dos. Como referencia, en cambio, es la lectura
# que el lector reconoce, y por eso se conserva en escala cruda igual que las otras.
MARKET_PROB_COLS = ["mkt_p_home", "mkt_p_draw", "mkt_p_away"]
# Pinnacle es el caso aparte desde que el catálogo curó la capa de features: su
# probabilidad NO viaja en la matriz. La razón es la regla 1 del catálogo —una sola
# parametrización por atributo—: lo que el modelo necesita de la casa informada es
# su DESACUERDO con el consenso (`sharp_div_*`), no el nivel, que es casi el mismo
# número que `mkt_p_*`. Así que `mkt_p_ps_*` se calcula dentro de
# `prematch_derived`, se usa para restar y se queda allí.
#
# Como referencia de evaluación, en cambio, Pinnacle sigue haciendo falta: es el
# precio del dinero informado y el más exigente de los tres. Se reconstruye por
# tanto de sus cuotas crudas `odds_ps_*`, que sí viajan en la matriz como SERVICIO
# porque el backtest liquida con ellas. Los nombres `mkt_p_ps_*` se conservan como
# nombres de SALIDA de esa terna reconstruida, que es como los conocen `evaluate` y
# `protocol`.
SHARP_ODDS_COLS = ["odds_ps_h", "odds_ps_d", "odds_ps_a"]
SHARP_PROB_COLS = ["mkt_p_ps_home", "mkt_p_ps_draw", "mkt_p_ps_away"]
# La terna de Bet365 no se vuelve a escribir aquí: es servicio y `columns` ya la
# declara a partir del catálogo. Un segundo literal con los mismos tres nombres
# sería un sitio más donde un renombrado puede quedarse a medias.
B365_PROB_COLS = list(C.SERVICIO_DERIVADO)
MARKET_RAW_SUFFIX = "_raw"
MARKET_RAW_COLS = [c + MARKET_RAW_SUFFIX for c in MARKET_PROB_COLS]
SHARP_RAW_COLS = [c + MARKET_RAW_SUFFIX for c in SHARP_PROB_COLS]
B365_RAW_COLS = [c + MARKET_RAW_SUFFIX for c in B365_PROB_COLS]

# Todas las ternas de precio que `apply()` copia sin tocar, en el orden en que se
# emiten. Tenerlas en una sola lista evita que añadir una lectura del mercado exija
# recordar los cuatro sitios donde había que declararla.
PRICE_PROB_COLS = MARKET_PROB_COLS + SHARP_PROB_COLS + B365_PROB_COLS
PRICE_RAW_COLS = MARKET_RAW_COLS + SHARP_RAW_COLS + B365_RAW_COLS

# Todo lo que viaja en la matriz sin ser variable. Son cinco poblaciones con
# motivos distintos y conviene no confundirlas: la identidad del partido y el
# objetivo (no son magnitudes), el metadato del máster (`kickoff_ts`: un índice
# temporal casi único por partido, del que el contexto ya extrae lo que
# significa), las copias crudas del precio que `apply()` adjunta como metadato,
# las columnas de SERVICIO —cuotas de cada casa y probabilidad de Bet365— que el
# backtest y el patrón de comparación necesitan y el estimador no debe ver, y el
# par de PRESENTACIÓN, la lectura por equipo de las magnitudes cuya señal el
# modelo ya recibe como diferencia y que la aplicación necesita sin restar para
# poder dibujar a cada equipo.
#
# Las dos últimas se toman de `columns` y no de listas propias: son las mismas
# decisiones escritas en el módulo que ensambla la matriz, y tenerlas dos veces es
# tenerlas mal la mitad de las veces.
_NON_FEATURE = (set(KEYS) | {TARGET, TARGET_NUM, SPLIT}
                | set(C.MASTER_METADATA) | set(PRICE_RAW_COLS)
                | set(C.SERVICIO) | set(C.PRESENTACION))

# --- encoding ordinal de categóricas (D4) -------------------------------------
ZONE_ORDER = {"descenso": 0, "mid_table": 1, "europa": 2, "champions": 3}
PLAYSTYLE_ORDER = {"defensivo": 0, "equilibrado": 1, "ofensivo": 2}
# La franja de saque admite orden porque es la hora discretizada: mediodía va
# antes que tarde y tarde antes que noche, de modo que el código conserva la
# única relación que la variable tiene y no inventa ninguna otra.
SLOT_ORDER = {"mediodia": 0, "tarde": 1, "noche": 2}
# se casa por subcadena del nombre de columna -> orden ordinal
CATEG_ORDERS = {"position_zone": ZONE_ORDER, "playstyle": PLAYSTYLE_ORDER,
                "kickoff_slot": SLOT_ORDER}

# --- flags de faltante por CAUSA (nombre -> col fuente que revela la causa) ----
# Cada flag marca UNA causa distinta y con poco solape entre sí (ver notebook 04).
# El flag se calcula del df de ENTRADA (antes de podar/imputar), así que sigue
# disponible aunque su columna fuente acabe eliminada por colinealidad.
#
# `flag_coldstart` cubre además el bloque de trascendencia: sus huecos y los de la
# forma reciente son el MISMO suceso —la temporada aún no ha empezado— de modo que
# un flag propio sería una copia exacta de este y solo añadiría colinealidad.
#
# `flag_no_strength`, en cambio, sí marca una causa propia. El bloque de fuerza
# latente falta cuando alguno de los dos clubes no acumula todavía historia
# suficiente en la categoría, que ocurre en las primeras jornadas de un recién
# ascendido y en el arranque del histórico. Ese suceso no se deduce de ningún otro
# flag: `flag_coldstart` se activa para TODOS los equipos en la primera jornada de
# cada temporada, y `flag_no_squadvalue` se mantiene activo para el ascendido
# durante el año entero. Sin un flag propio, el modelo recibiría el bloque
# completo imputado con la mediana de la liga —una descripción de un partido medio
# donde no hay descripción— sin nada que se lo advirtiera.
#
# Cinco de las seis causas se leen en una columna DIFERENCIA, y no en la lectura
# del local, y eso no es cosmética sino lo que hace al flag correcto: una resta es
# NaN cuando falta CUALQUIERA de los dos operandos, de modo que la diferencia marca
# el hueco DEL PARTIDO —que es lo que el modelo sufre— mientras que `home_x` solo
# marcaba el del equipo local y dejaba sin señalar el caso del visitante recién
# ascendido contra un histórico.
#
# El valor de plantilla es el caso que merece explicación, porque de ese bloque el
# catálogo emite el PAR y no la resta —el hueco del ascendido solo admite relleno
# por lado— y aun así su flag se lee de `age_diff`. El motivo es que el hueco de
# Transfermarkt no es por columna sino por FICHA: las tres columnas del bloque
# (valor, edad, tamaño) faltan juntas y exactamente para los tres equipos recién
# ascendidos de cada temporada, 15 % de los lados y 28 % de los partidos. `age_diff`
# es la única columna del bloque que se enciende en los dos casos, así que leer de
# ella da el indicador del suceso completo con una sola columna; leer de
# `home_squad_value` daría la mitad del suceso y obligaría a un segundo flag para
# la otra mitad.
FLAG_BLOCKS = {
    "flag_no_closing_odds": "odds_close_avg_h",   # hueco de época 2016-2018 (cierre no recogido)
    "flag_no_absences":     "out_value_pct_diff",  # bajas no recogidas en 2016/17
    "flag_coldstart":       "form_points_ewm_diff",  # sin forma reciente (jornada 1 / inicio)
    "flag_no_h2h":          "h2h_gd_ewm_diff",    # primer enfrentamiento histórico (sin H2H)
    "flag_no_squadvalue":   "age_diff",           # ascendido: sin ficha t-1 en Primera
    "flag_no_strength":     "strength_attack_diff",  # sin recorrido suficiente en la categoría
}

# --- política de imputación por causa (D2) ------------------------------------
_WEAK_PRIOR_Q = 0.10  # cuantil bajo = "cola inferior" donde caen los ascendidos
# El tamaño de plantilla ya no figura: de las cuatro lecturas del bloque de
# Transfermarkt el catálogo se queda con el valor (par) y la edad (diferencia),
# porque contar fichas no describe fuerza. Lo que llega con hueco de ascendido es
# por tanto solo el valor.
_WEAK_PRIOR_COLS = {"home_squad_value", "away_squad_value"}
_ZERO_FAMILIES = ("h2h", "venue_form")  # ausencia real -> neutro 0

# Arranque de temporada: el hueco tiene un valor CONOCIDO, no estimable. El bloque
# de trascendencia se calcula sobre la clasificación PREVIA al partido, de modo que
# en la primera jornada no existe tal clasificación y las columnas llegan vacías.
# Ese vacío, sin embargo, no es ignorancia: describe una situación perfectamente
# determinada —marcador de la liga a cero— cuya lectura es aritmética.
#
#   * distancia a un corte  : con todos los equipos a cero puntos, la diferencia
#     de puntos contra el decimoctavo es 0 para cualquier equipo. Las distancias
#     al líder, al cuarto y al sexto ya no figuran: el catálogo no las emite
#     porque los puntos del corte se cancelan al restar los dos lados y las tres
#     quedan reducidas a esta misma columna salvo el signo.
#   * indicadores binarios  : nada está decidido tras cero jornadas, así que ni hay
#     salvación matemática ni el partido carece de trascendencia; los dos valen 0.
#     (`europe_unreachable` ya no se construye: el catálogo lo descarta por ser el
#     complemento casi determinista de la distancia a Europa.)
#   * partidos restantes    : quedan por disputar los 38 del calendario.
#
# La coincidencia es por subcadena para alcanzar a la vez las lecturas local y
# visitante y sus diferenciales. En un diferencial la cifra correcta es siempre 0,
# porque ambos lados comparten el mismo valor de arranque; de ahí que
# `matches_left` se distinga por sufijo y el resto se resuelva directamente a cero.
_KNOWN_ZERO_PATTERNS = ("points_above_drop", "safe_from_drop", "stakes_none")
_KNOWN_MATCHES_LEFT = "matches_left"
_TOTAL_MATCHES = 38  # jornadas de una temporada de Primera División


def _known_cold_start(col: str) -> float | None:
    """Valor cierto de `col` cuando el hueco es el arranque de temporada.

    Devuelve `None` si la columna no pertenece al bloque de trascendencia, es
    decir, si su hueco no admite una lectura exacta y debe estimarse.
    """
    if _KNOWN_MATCHES_LEFT in col:
        return 0.0 if col.endswith("_diff") else float(_TOTAL_MATCHES)
    if any(p in col for p in _KNOWN_ZERO_PATTERNS):
        return 0.0
    return None


# Aquí vivían tres podas más —el duplicado determinista de `elo_diff`, los
# residuos de suerte frente al xG y las columnas de señal nula—, y las tres se han
# ido a `catalogo.py`. El motivo es que ninguna de ellas necesitaba mirar los
# datos: se sabía de antemano que `elo_exp_home` es una función de `elo_diff`, que
# la distancia entre goles y goles esperados es varianza y no habilidad, y que
# `n_suspension` y `post_big_fav_loss` tienen información mutua del orden del ruido
# de muestreo. Lo que se sabe de antemano se decide al declarar el conjunto, no
# construyendo las columnas para tirarlas en el último paso.
#
# columnas PROTEGIDAS de la poda colineal: la probabilidad implícita del CONSENSO,
# que es a la vez variable, baseline (número a batir) y la lectura interpretable del
# precio. Correlaciona con media matriz —el precio resume lo que todas las demás
# variables intentan medir—, de modo que sin protección la poda podría quedarse con
# una forma reciente cualquiera en su lugar y la comparación con el mercado dejaría
# de ser calculable desde el artefacto.
#
# Las otras dos lecturas del precio ya no aparecen aquí porque ninguna llega a
# candidata: de Pinnacle el catálogo se queda con el desacuerdo (`sharp_div_*`) y no
# con el nivel, y Bet365 sale excluida en `_NON_FEATURE` porque el consenso promedia
# esa misma casa. Las dos viajan igualmente en el artefacto como referencia.
_PROTECTED = set(MARKET_PROB_COLS)

# --- simetrización de colas: Yeo-Johnson (D5) ---------------------------------
# Solo se transforma una columna continua si su cola es realmente fuerte y la
# transformación la corrige de forma clara (evita tocar variables ya razonables o
# recuentos dispersos donde λ degeneraría).
_TRANSFORM_MIN_UNIQUE = 10        # "continua": suficientes valores distintos
_TRANSFORM_MIN_SKEW = 1.0         # umbral de asimetría para intervenir
_TRANSFORM_LAMBDA_BOUNDS = (-5.0, 5.0)   # λ fuera de rango -> ajuste degenerado
_TRANSFORM_MAX_SKEW_AFTER = 0.75  # se acepta si |skew| baja de esto ...
_TRANSFORM_MIN_IMPROVE = 0.5      # ... o si al menos se reduce a la mitad
_TRANSFORM_MIN_RESOLUTION = 0.99  # la transf. debe preservar los valores distintos

# --- tipificación (D6) --------------------------------------------------------
# Cardinalidad mínima para tipificar. Figura como constante, y no como número
# suelto dentro del bucle, porque es la pregunta de diseño del paso y no un
# detalle: dividir una columna de dos valores por su desviación cambia lo que la
# penalización L2 le cobra frente a las continuas, y en una indicadora rara la
# desviación es pequeña, de modo que tipificar abarata su coeficiente justo donde
# menos apoyo empírico tiene.
#
# Con el valor 3 las indicadoras llegan al modelo en 0/1 y solo se tipifica lo
# continuo. La alternativa —tipificarlo todo— se midió con el protocolo real
# sobre el bloque de selección y sale peor en las tres métricas a la vez:
# RPS +0.00026, log-loss +0.00239 y acierto -0.00088. Es poco, pero es del mismo
# signo en las tres, así que el valor se queda donde está por medida y no por
# costumbre.
_SCALE_MIN_UNIQUE = 3


@dataclass
class PreprocessState:
    """Estado congelado en dev. Serializable (JSON) para reproducir en inferencia."""
    regime: str = DEFAULT_REGIME                      # régimen de información (columns.REGIMES)
    flag_cols: dict = field(default_factory=dict)     # flag_name -> source col
    categ_maps: dict = field(default_factory=dict)    # col -> {categoría: código}
    drop_cols: list = field(default_factory=list)     # columnas eliminadas (D3)
    impute_value: dict = field(default_factory=dict)  # col -> valor de imputación (dev)
    impute_strat: dict = field(default_factory=dict)  # col -> estrategia (weak_prior/zero/known/neutral)
    transform_lambda: dict = field(default_factory=dict)  # col -> λ Yeo-Johnson (dev, D5)
    scale_mean: dict = field(default_factory=dict)    # col -> media(dev, imput./transf.)
    scale_std: dict = field(default_factory=dict)     # col -> desv(dev, imput./transf.)
    feature_cols: list = field(default_factory=list)  # orden final de features
    meta: dict = field(default_factory=dict)          # trazabilidad (umbral, MI...)

    def save(self, path) -> None:
        Path(path).write_text(json.dumps(asdict(self), indent=2, ensure_ascii=False))

    @classmethod
    def load(cls, path) -> "PreprocessState":
        return cls(**json.loads(Path(path).read_text()))


# ---------------------------------------------------------------------------
# candidatas a variable
# ---------------------------------------------------------------------------
def feature_candidates(df: pd.DataFrame,
                       regime: str | None = DEFAULT_REGIME) -> list[str]:
    """Columnas del frame admisibles como features bajo el régimen indicado.

    Dos filtros encadenados: identidad/metadato (`_NON_FEATURE`) y momento de
    disponibilidad (`columns.regime_columns`). El segundo se aplica AQUÍ, a la
    entrada, para que ninguna capa posterior llegue a ver una columna que el
    régimen no admite.

    `regime=None` aplica solo el primer filtro. No es una puerta trasera al bloque
    de cierre: es la lista que necesita quien va a MOSTRAR qué excluye cada
    régimen —el banco manual la enseña antes de componer nada— y por eso la
    entrega sin filtrar en vez de obligar a reconstruirla por fuera, que es como
    se acaba con dos definiciones de «candidata» que discrepan.

    Esta es la única definición de candidata del proyecto. Que sea pública importa:
    el preprocesado la usa para ajustar, el banco manual para ofrecer variables y
    los cuadernos para contarlas, y las tres respuestas tienen que ser la misma.
    """
    cand = [c for c in df.columns if c not in _NON_FEATURE]
    return cand if regime is None else C.regime_columns(cand, regime)


def _categ_order_for(col: str) -> dict | None:
    for token, order in CATEG_ORDERS.items():
        if token in col:
            return order
    return None


def _encode_categoricals(df: pd.DataFrame, categ_maps: dict) -> pd.DataFrame:
    """Devuelve una copia con las categóricas ya en su código ordinal (float)."""
    out = df.copy()
    for c, order in categ_maps.items():
        if c in out.columns:
            out[c] = out[c].map(order).astype("float64")
    return out


def _impute_strategy(col: str) -> str:
    """Estrategia de imputación por causa para la columna `col`.

    El orden de las comprobaciones expresa una prioridad: primero las causas cuyo
    relleno se deduce (valor cierto, neutro estructural, cola de los ascendidos) y
    solo al final la estimación estadística, que es el recurso por defecto para los
    huecos que no admiten lectura exacta.
    """
    if col in _WEAK_PRIOR_COLS:
        return "weak_prior"
    if any(fam in col for fam in _ZERO_FAMILIES):
        return "zero"
    if _known_cold_start(col) is not None:
        return "known"
    return "neutral"


def _impute_fill_value(col: str, s: pd.Series) -> float:
    """Valor de relleno (aprendido en dev) según la estrategia de la columna.

    Las estrategias `zero` y `known` no leen `s`: su relleno se deduce del
    significado de la variable y es por tanto independiente de la muestra, lo que
    las hace inmunes a cualquier deriva entre dev, holdout e inferencia.
    """
    strat = _impute_strategy(col)
    if strat == "weak_prior":
        return float(s.quantile(_WEAK_PRIOR_Q))
    if strat == "zero":
        return 0.0
    if strat == "known":
        return float(_known_cold_start(col))
    return float(s.median())


# Aquí vivía `_recompute_squad_derived`, que tras imputar volvía a restar las dos
# bases de valor de plantilla para que `squad_value_diff`, `squad_value_logratio` y
# `age_diff` no contradijeran a sus operandos. Ya no hay nada que recalcular: de las
# cuatro lecturas del bloque de Transfermarkt el catálogo emite el valor como PAR y
# la edad como DIFERENCIA, de modo que las dos derivadas de valor no se construyen y
# de la edad llega la resta pero no las dos edades con las que rehacerla. La
# coherencia que el recálculo defendía la da ahora la propia declaración: no puede
# haber desacuerdo entre una derivada y sus bases si no viajan las dos.


# --- Yeo-Johnson (D5): forma cerrada, sin dependencias en apply() -------------
def _yeojohnson_transform(x: np.ndarray, lam: float) -> np.ndarray:
    """Transformación Yeo-Johnson directa con λ ya conocido (forma cerrada).

    El ajuste de λ (MLE) se hace en `fit()` con scipy; aquí solo se REPRODUCE la
    fórmula, para que `apply()` sea puro numpy y el estado serializable baste para
    la inferencia. Idéntica definición que scipy.stats.yeojohnson.
    """
    x = np.asarray(x, dtype="float64")
    out = np.empty_like(x)
    pos = x >= 0
    neg = ~pos
    if abs(lam) < 1e-6:                      # λ ≈ 0
        out[pos] = np.log1p(x[pos])
    else:
        out[pos] = (np.power(x[pos] + 1.0, lam) - 1.0) / lam
    if abs(lam - 2.0) < 1e-6:                # λ ≈ 2
        out[neg] = -np.log1p(-x[neg])
    else:
        out[neg] = -(np.power(-x[neg] + 1.0, 2.0 - lam) - 1.0) / (2.0 - lam)
    return out


def _skew(x: np.ndarray) -> float:
    """Asimetría (bias-corregida, como pandas) de un vector finito."""
    return float(pd.Series(np.asarray(x, dtype="float64")).skew())


def _transform_eligible(col: str, s: pd.Series, categ_maps: dict) -> bool:
    """¿La columna es candidata a Yeo-Johnson? (continua, no acotada, no categórica)."""
    if col in categ_maps or col in _PROTECTED:
        return False
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.nunique() < _TRANSFORM_MIN_UNIQUE:
        return False
    # Una columna acotada en [0,1] —probabilidad, tasa o cuota de reparto— ya está
    # en una escala comparable y bien condicionada, y es ahí donde la simetrización
    # aporta menos: lo que corrige Yeo-Johnson es una cola que se extiende, no un
    # amontonamiento dentro de un intervalo cerrado. La exención se ha medido en lo
    # único que la ejerce hoy: de las dos columnas acotadas con cola fuerte,
    # `mkt_spread_mean` (skew +3.77) la rechazaría igualmente el guardián de λ
    # —el ajuste pide λ=-29.9, fuera del rango admisible— y `h2h_draw_rate_ewm`
    # (skew +1.03) resulta indiferente: en las ventanas del bloque de selección su
    # asimetría se queda en 0.98-1.00, por debajo del umbral, y levantar la
    # exención deja las tres métricas idénticas hasta el quinto decimal.
    lo, hi = float(x.min()), float(x.max())
    if lo >= 0.0 and hi <= 1.0:
        return False
    return True


def _fit_transform_lambda(col: str, s: pd.Series, categ_maps: dict):
    """Ajusta λ en dev y decide si la transformación MERECE la pena.

    Devuelve (lam, skew_antes, skew_despues) si se acepta, o None si se rechaza.
    Criterio de aceptación (todo evaluado en dev):
      * candidata continua/no acotada/no categórica,
      * asimetría fuerte antes (|skew|>1),
      * λ dentro de rango razonable (evita ajustes degenerados),
      * la transformación PRESERVA la resolución (no colapsa valores por desborde
        numérico: un λ extremo sobre una variable de gran magnitud —p.ej. elo≈1500—
        hunde (x+1)^λ a denormales y aniquila la varianza, aunque la asimetría
        "mejore" a 0 sobre un vector ya constante),
      * y la cola se corrige de forma clara (|skew| final < 0.75 o reducido a la mitad).
    """
    if not _transform_eligible(col, s, categ_maps):
        return None
    from scipy.stats import yeojohnson
    x = pd.to_numeric(s, errors="coerce").to_numpy(dtype="float64")
    x = x[np.isfinite(x)]
    sk0 = _skew(x)
    if not np.isfinite(sk0) or abs(sk0) <= _TRANSFORM_MIN_SKEW:
        return None
    try:
        lam = float(yeojohnson(x)[1])
    except Exception:
        return None
    lo, hi = _TRANSFORM_LAMBDA_BOUNDS
    if not (lo <= lam <= hi):
        return None
    t = _yeojohnson_transform(x, lam)
    # guard anti-colapso: una transformación monótona correcta conserva el nº de
    # valores distintos; si colapsan es que λ ha desbordado y ha destruido la señal.
    nu_in = np.unique(np.round(x, 10)).size
    nu_out = np.unique(t).size
    if nu_in > 0 and nu_out / nu_in < _TRANSFORM_MIN_RESOLUTION:
        return None
    sk1 = _skew(t)
    accept = (abs(sk1) < _TRANSFORM_MAX_SKEW_AFTER
              or abs(sk1) <= _TRANSFORM_MIN_IMPROVE * abs(sk0))
    return (lam, sk0, sk1) if accept else None


def _mi(x: pd.Series, y: pd.Series, bins: int = 10) -> float:
    """Información mutua marginal (x discretizada por cuantiles) vs target."""
    from sklearn.metrics import mutual_info_score
    xn = pd.to_numeric(x, errors="coerce")
    m = xn.notna() & y.notna()
    if m.sum() < 50:
        return 0.0
    xr = xn[m].rank(method="first")
    nb = min(bins, xr.nunique())
    if nb < 2:
        return 0.0
    xb = pd.qcut(xr, q=nb, labels=False, duplicates="drop")
    return float(mutual_info_score(xb, y[m].astype(str)))


def _greedy_collinear_drop(work: pd.DataFrame, cols: list[str], y: pd.Series,
                           thr: float) -> tuple[list[str], list[tuple]]:
    """Poda colineal greedy: en cada par |r|>thr, se queda el de MAYOR utilidad.

    Utilidad = MI * cobertura, para que a MI casi igual gane la columna con
    MENOS imputación (p.ej. cuota de apertura 100% cov. > cierre 66,7% cov.).
    Las columnas en `_PROTECTED` (baseline de mercado) nunca se tiran.
    `work` debe traer ya las categóricas codificadas a numérico.
    """
    num = work[cols].apply(pd.to_numeric, errors="coerce")
    corr = num.corr().abs()
    cov = {c: float(num[c].notna().mean()) for c in cols}
    util = {c: _mi(work[c], y) * cov[c] for c in cols}
    dropped: set[str] = set()
    pairs = []
    order = sorted(cols, key=lambda c: util[c], reverse=True)  # protege alta-utilidad
    for i, a in enumerate(order):
        if a in dropped:
            continue
        for b in order[i + 1:]:
            if b in dropped or b in _PROTECTED or a not in corr.index \
                    or b not in corr.columns:
                continue
            r = corr.at[a, b]
            if pd.notna(r) and r > thr:
                dropped.add(b)  # a tiene utilidad >= b -> se queda a
                pairs.append((a, b, round(float(r), 4)))
    return sorted(dropped), pairs


# ---------------------------------------------------------------------------
# FIT (solo dev)
# ---------------------------------------------------------------------------
def fit(dev: pd.DataFrame, corr_threshold: float = 0.95,
        transform: bool = True, regime: str = DEFAULT_REGIME) -> PreprocessState:
    """Aprende el preprocesado SOLO en dev y lo congela en PreprocessState.

    `transform` activa la capa Yeo-Johnson (D5, on por defecto): simetriza las
    colas de las continuas muy asimétricas con un λ ajustado y congelado en dev.

    `regime` declara qué información se considera disponible al emitir la
    predicción (ver `columns.REGIMES`). Restringe el conjunto candidato ANTES de
    la poda, la imputación, la transformación y el escalado, de modo que todos los
    estadísticos congelados se aprenden sobre exactamente la misma matriz que el
    modelo verá después.
    """
    if regime not in REGIMES:
        raise ValueError(f"régimen desconocido: {regime!r}; use uno de {REGIMES}")
    if SPLIT in dev.columns and (dev[SPLIT] != "dev").any():
        raise ValueError("fit() debe recibir SOLO filas dev (holdout sellado).")
    y = dev[TARGET_NUM]
    st = PreprocessState(regime=regime)

    # --- D2: flags de faltante (las causas presentes en el frame Y admitidas) ---
    # Un flag hereda el régimen de su columna fuente: si la fuente no es conocible
    # al predecir, el flag tampoco lo es y marcarlo sería el mismo desajuste que se
    # quiere evitar (0 casi siempre en entrenamiento, 1 siempre en inferencia).
    flag_src_ok = C.regime_columns(list(FLAG_BLOCKS.values()), regime)
    st.flag_cols = {f: src for f, src in FLAG_BLOCKS.items()
                    if src in dev.columns and src in set(flag_src_ok)}

    cand = feature_candidates(dev, regime)

    # --- D4: detectar categóricas y su orden ordinal ---
    for c in cand:
        if dev[c].dtype == object or str(dev[c].dtype) == "string":
            order = _categ_order_for(c)
            if order is None:
                raise ValueError(f"Categórica sin orden ordinal definido: {c}")
            st.categ_maps[c] = order

    # frame de trabajo con TODAS las candidatas ya numéricas
    work = _encode_categoricals(dev, st.categ_maps)

    # --- D3: poda por reglas ---
    # Solo quedan las dos reglas que necesitan mirar las filas de dev: si una
    # columna es constante y si dos columnas van juntas. Lo que se sabía de
    # antemano lo excluye `catalogo.py` antes de construirlo.
    num = work[cand].apply(pd.to_numeric, errors="coerce")
    const_cols = [c for c in cand if num[c].nunique(dropna=True) <= 1]

    pre_drop = set(const_cols)
    remaining = [c for c in cand if c not in pre_drop]
    collinear_cols, pairs = _greedy_collinear_drop(work, remaining, y, corr_threshold)

    st.drop_cols = sorted(pre_drop | set(collinear_cols))
    kept = [c for c in cand if c not in st.drop_cols]

    # --- D2: imputación por causa (valores aprendidos en dev) ---
    imp_cols = {}
    for c in kept:
        s = pd.to_numeric(work[c], errors="coerce")
        val = _impute_fill_value(c, s)
        st.impute_value[c] = val
        st.impute_strat[c] = _impute_strategy(c)
        imp_cols[c] = s.fillna(val)
    imp = pd.DataFrame(imp_cols, index=work.index)

    # --- D5: simetrización de colas (Yeo-Johnson, λ ajustado en dev) ---
    transform_info = []
    if transform:
        for c in kept:
            res = _fit_transform_lambda(c, imp[c], st.categ_maps)
            if res is not None:
                lam, sk0, sk1 = res
                st.transform_lambda[c] = lam
                imp[c] = _yeojohnson_transform(imp[c].to_numpy(dtype="float64"), lam)
                transform_info.append((c, round(lam, 4),
                                       round(sk0, 3), round(sk1, 3)))

    # --- D6: escalado sobre los valores YA imputados/recalculados/transformados ---
    scale_cols = []
    for c in kept:
        s_imp = imp[c]
        if s_imp.nunique() >= _SCALE_MIN_UNIQUE:
            mu = float(s_imp.mean()); sd = float(s_imp.std(ddof=0))
            st.scale_mean[c] = mu
            st.scale_std[c] = sd if sd > 1e-12 else 1.0
            scale_cols.append(c)

    st.feature_cols = kept + list(st.flag_cols.keys())

    n_strat = {k: sum(1 for v in st.impute_strat.values() if v == k)
               for k in ("weak_prior", "zero", "known", "neutral")}
    st.meta = {
        "regime": regime,
        "n_regime_excluded": len(C.weekly_unavailable(dev.columns)) if regime == "semanal" else 0,
        "regime_excluded_cols": C.weekly_unavailable(dev.columns) if regime == "semanal" else [],
        "corr_threshold": corr_threshold,
        "n_candidates": len(cand),
        "n_const": len(const_cols), "const_cols": sorted(const_cols),
        "n_collinear": len(collinear_cols),
        "collinear_pairs_kept_dropped": pairs,
        "weak_prior_q": _WEAK_PRIOR_Q,
        "weak_prior_values": {c: st.impute_value[c] for c in kept
                              if st.impute_strat[c] == "weak_prior"},
        "known_values": {c: st.impute_value[c] for c in kept
                         if st.impute_strat[c] == "known"},
        "impute_strategy_counts": n_strat,
        "transform_enabled": bool(transform),
        "n_transformed": len(st.transform_lambda),
        "transform_skew_before_after": transform_info,
        "n_features_out": len(st.feature_cols),
        "n_scaled": len(scale_cols),
    }
    return st


# ---------------------------------------------------------------------------
# APPLY (dev, holdout o upcoming — misma transformación congelada)
# ---------------------------------------------------------------------------
def apply(df: pd.DataFrame, st: PreprocessState, keep_meta: bool = True,
          strict: bool = True) -> pd.DataFrame:
    """Aplica el estado congelado. Idéntico en dev/holdout/upcoming.

    `strict=True` (por defecto) exige que estén TODAS las columnas de
    `state.feature_cols`. Es la garantía de paridad train<->inferencia: una matriz de
    scoring más estrecha que la de entrenamiento produciría predicciones degradadas
    sin ningún síntoma visible, así que aquí se convierte en un error explícito.

    El régimen no se vuelve a evaluar aquí: quedó impreso en `state.feature_cols`,
    `state.drop_cols` y `state.flag_cols` al ajustar, de modo que una columna que el
    régimen no admite no tiene por dónde entrar. Si el frame la trae, se ignora.
    """
    # 1) flags de faltante ANTES de tocar nada (capturan el patrón NaN original)
    flags = {}
    for flag, src in st.flag_cols.items():
        flags[flag] = df[src].isna().astype("int8") if src in df.columns \
            else pd.Series(np.int8(0), index=df.index)

    # 1b) copia CRUDA de la probabilidad implícita del mercado (D1), en sus tres
    #     lecturas: consenso, Pinnacle y Bet365. Se toma aquí, antes de imputar/
    #     transformar/escalar, porque después esas columnas ya no son probabilidades y
    #     el baseline no sería calculable desde el artefacto.
    market_raw = pd.DataFrame(index=df.index)
    for c, c_raw in zip(PRICE_PROB_COLS, PRICE_RAW_COLS):
        if c in df.columns:
            market_raw[c_raw] = pd.to_numeric(df[c], errors="coerce")
    # Pinnacle no tiene columna de probabilidad que copiar —no es variable—, así que
    # su terna se reconstruye aquí de las cuotas de servicio. Se adjunta con los
    # mismos nombres `*_raw` que las otras dos para que `evaluate` y `protocol` sigan
    # leyendo los tres precios por la misma puerta.
    if not all(c in market_raw.columns for c in SHARP_RAW_COLS):
        ps = _pinnacle_probs(df)
        if ps is not None:
            for c_raw, lado in zip(SHARP_RAW_COLS, ("home", "draw", "away")):
                market_raw[c_raw] = ps[lado]

    # 2) encoding ordinal de categóricas
    out = _encode_categoricals(df, st.categ_maps)

    # 3) poda (D3)
    out = out.drop(columns=[c for c in st.drop_cols if c in out.columns])

    # 4) imputación por causa (valor aprendido en dev)
    for c, val in st.impute_value.items():
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(val)

    # 5) simetrización Yeo-Johnson (λ congelado en dev), antes de escalar
    for c, lam in st.transform_lambda.items():
        if c in out.columns:
            vals = pd.to_numeric(out[c], errors="coerce").to_numpy(dtype="float64")
            out[c] = _yeojohnson_transform(vals, float(lam))

    # 6) escalado (media/desv dev, sobre valores imputados/recalculados/transformados)
    for c, mu in st.scale_mean.items():
        if c in out.columns:
            out[c] = (pd.to_numeric(out[c], errors="coerce") - mu) / st.scale_std[c]

    # 7) añadir flags y ordenar (concat de golpe: evita fragmentación)
    flags_df = pd.DataFrame(flags, index=out.index)
    # Lo que se adjunta aquí —los flags de faltante y la copia cruda del precio— se
    # acaba de calcular del marco de entrada. Si ese marco ya los trajera —porque
    # proceda de un artefacto que pasó por aquí antes— los viejos se retiran antes de
    # concatenar. Sin este descarte el marco quedaría con la columna repetida, y el
    # fallo no se vería aquí sino mucho después, cuando alguien seleccione
    # `out["mkt_p_home_raw"]` o `out["flag_coldstart"]` y reciba una matriz de dos
    # columnas donde esperaba una serie.
    adjuntas = list(flags_df.columns) + list(market_raw.columns)
    out = out.drop(columns=[c for c in adjuntas if c in out.columns])
    out = pd.concat([out, flags_df, market_raw], axis=1)
    missing = [c for c in st.feature_cols if c not in out.columns]
    if missing:
        msg = (f"apply(): faltan {len(missing)} de las {len(st.feature_cols)} features "
               f"del estado congelado: {missing[:10]}"
               f"{' ...' if len(missing) > 10 else ''}. El modelo se entrenó con esa "
               "matriz exacta, así que predecir sin estas columnas no es comparable.")
        if strict:
            raise ValueError(msg)
        print(f"[WARN] {msg}")
    feats = [c for c in st.feature_cols if c in out.columns]
    if keep_meta:
        meta_cols = [c for c in (KEYS + [TARGET, TARGET_NUM, SPLIT])
                     if c in out.columns]
        meta_cols += [c for c in market_raw.columns]
        return out[meta_cols + feats]
    return out[feats]


def fit_apply(dev: pd.DataFrame, corr_threshold: float = 0.95,
              transform: bool = True, regime: str = DEFAULT_REGIME):
    st = fit(dev, corr_threshold, transform=transform, regime=regime)
    return apply(dev, st), st


# ---------------------------------------------------------------------------
# baseline de mercado (D1) — se usa aparte, como número a batir
# ---------------------------------------------------------------------------
_MARKET_TOL = 0.02  # las mkt_p_* salen ya normalizadas a suma 1 en footballdata.py


def _implied_probs(df: pd.DataFrame, prob_cols: list[str], raw_cols: list[str],
                   who: str) -> pd.DataFrame:
    """Terna de probabilidades 1X2 des-margen, leída del sitio correcto y validada.

    Toma preferentemente las columnas crudas `*_raw` que `apply()` adjunta al artefacto
    preprocesado; si no están, usa las columnas de probabilidad directamente (correcto
    únicamente sobre un frame SIN preprocesar, p.ej. `features_matches.parquet`).
    Devuelve siempre las columnas con el nombre canónico `home/draw/away`.

    Comprueba que lo recibido son probabilidades de verdad y falla si no lo son. Sin
    esta guarda, aplicarla a una matriz ya estandarizada devolvería números con signo
    arbitrario y el benchmark quedaría silenciosamente invalidado.
    """
    if all(c in df.columns for c in raw_cols):
        p = df[raw_cols].apply(pd.to_numeric, errors="coerce")
        source = ", ".join(raw_cols)
    elif all(c in df.columns for c in prob_cols):
        p = df[prob_cols].apply(pd.to_numeric, errors="coerce")
        source = ", ".join(prob_cols)
    else:
        raise KeyError(
            f"{who}: no encuentro la probabilidad implícita. Se esperaba {raw_cols} "
            f"(artefacto preprocesado) o {prob_cols} (features sin preprocesar)."
        )
    p = p.set_axis(["home", "draw", "away"], axis=1)

    full = p.notna().all(axis=1)
    if not full.any():
        raise ValueError(f"{who}: ninguna fila tiene las tres probabilidades "
                         f"({source}).")
    q = p[full]
    bad_range = ((q < -_MARKET_TOL) | (q > 1 + _MARKET_TOL)).any(axis=1)
    bad_sum = (q.sum(axis=1) - 1.0).abs() > _MARKET_TOL
    bad = bad_range | bad_sum
    if bad.any():
        ej = q[bad].head(3).round(4).to_dict("records")
        raise ValueError(
            f"{who}: '{source}' no contiene probabilidades válidas "
            f"({int(bad.sum())} de {int(full.sum())} filas fuera de [0,1] o con suma "
            f"distinta de 1; ejemplos: {ej}). La causa habitual es aplicarla sobre una "
            f"matriz ya preprocesada: usa las columnas crudas `{raw_cols[0]}`... que "
            "`apply()` adjunta, o parte de `features_matches.parquet`."
        )

    s = p.sum(axis=1).replace(0, np.nan)
    return p.div(s, axis=0)  # renormaliza por si acaso


def market_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Prob. 1X2 del CONSENSO de casas, con nombres `mkt_p_home/draw/away`."""
    p = _implied_probs(df, MARKET_PROB_COLS, MARKET_RAW_COLS, "market_baseline")
    return p.set_axis(MARKET_PROB_COLS, axis=1)


def b365_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Prob. 1X2 de BET365, con nombres `mkt_p_b365_home/draw/away`.

    Es la lectura minorista del precio: la casa que el lector conoce y con la que
    compararía el pronóstico si tuviera que hacerlo mentalmente. Su margen es más
    ancho que el de Pinnacle, de modo que se espera que sea la peor de las tres
    referencias; batirla es una condición necesaria y no suficiente, y por eso el
    número a batir sigue siendo el mejor de los tres precios y no este.
    """
    p = _implied_probs(df, B365_PROB_COLS, B365_RAW_COLS, "b365_baseline")
    return p.set_axis(B365_PROB_COLS, axis=1)


def _pinnacle_probs(df: pd.DataFrame) -> pd.DataFrame | None:
    """Terna des-margen de Pinnacle reconstruida de sus cuotas crudas, o None.

    Devuelve None —y no un error— cuando las tres cuotas no están: la ausencia de
    Pinnacle es un caso legítimo (no cotiza todas las temporadas de la fuente) y
    quien llama sabe si puede prescindir de esta referencia o no.
    """
    if not all(c in df.columns for c in SHARP_ODDS_COLS):
        return None
    h, d, a = (df[c] for c in SHARP_ODDS_COLS)
    return _probs_de_cuotas(h, d, a)


def sharp_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Prob. 1X2 de PINNACLE, con nombres `mkt_p_ps_home/draw/away`.

    Misma lectura que `market_baseline` sobre la casa de referencia informada, pero
    por otro camino: como su probabilidad no es una variable del modelo, no hay
    columna `mkt_p_ps_*` que leer y la terna se reconstruye de las cuotas
    `odds_ps_*` que la matriz transporta como servicio. Si un frame ya trae las
    copias crudas `mkt_p_ps_*_raw` —porque pasó por `apply()`— se usan esas, que es
    exactamente la misma terna calculada antes.

    Su cobertura es menor que la del consenso —Pinnacle no aparece en todas las
    temporadas de la fuente—, de modo que las métricas de ambos baselines solo son
    comparables restringidas a las filas con las dos ternas disponibles, que es lo que
    hace `baselines_report`.
    """
    if all(c in df.columns for c in SHARP_RAW_COLS):
        p = _implied_probs(df, SHARP_PROB_COLS, SHARP_RAW_COLS, "sharp_baseline")
        return p.set_axis(SHARP_PROB_COLS, axis=1)
    p = _pinnacle_probs(df)
    if p is None:
        raise KeyError(
            f"sharp_baseline: no encuentro el precio de Pinnacle. Se esperaba "
            f"{SHARP_RAW_COLS} (artefacto preprocesado) o {SHARP_ODDS_COLS} "
            f"(cuotas crudas de servicio, presentes en features_matches.parquet)."
        )
    if not p.notna().all(axis=1).any():
        raise ValueError("sharp_baseline: ninguna fila tiene las tres cuotas de "
                         f"Pinnacle válidas ({', '.join(SHARP_ODDS_COLS)}).")
    return p.set_axis(SHARP_PROB_COLS, axis=1)


def _score_probs(p: pd.DataFrame, y: pd.Series, mask: pd.Series) -> dict:
    """Log-loss y acierto de una terna de probabilidades sobre las filas de `mask`.

    El orden de columnas (home, draw, away) coincide por construcción con el encoding
    del target (`y_num` = H:0, D:1, A:2).
    """
    P = p[mask].to_numpy(dtype="float64").clip(1e-12, 1.0)
    yy = y[mask].to_numpy(dtype="int64")
    return {
        "n": int(mask.sum()),
        "coverage": float(mask.mean()),
        "log_loss": float(-np.mean(np.log(P[np.arange(len(yy)), yy]))),
        "accuracy": float(np.mean(P.argmax(axis=1) == yy)),
    }


def market_baseline_metrics(df: pd.DataFrame) -> dict:
    """Log-loss y acierto del consenso de mercado.

    Se evalúa solo sobre las filas con las tres probabilidades disponibles y con
    resultado conocido, y se informa de esa cobertura para que la comparación sea
    honesta.
    """
    if TARGET_NUM not in df.columns:
        raise KeyError(f"market_baseline_metrics: falta la columna '{TARGET_NUM}'.")
    p = market_baseline(df)
    y = pd.to_numeric(df[TARGET_NUM], errors="coerce")
    m = p.notna().all(axis=1) & y.notna()
    if not m.any():
        raise ValueError("market_baseline_metrics: ninguna fila evaluable "
                         "(sin cuotas o sin resultado).")
    return _score_probs(p, y, m)


def sharp_baseline_metrics(df: pd.DataFrame) -> dict:
    """Log-loss y acierto de Pinnacle, en las mismas condiciones que el consenso."""
    if TARGET_NUM not in df.columns:
        raise KeyError(f"sharp_baseline_metrics: falta la columna '{TARGET_NUM}'.")
    p = sharp_baseline(df)
    y = pd.to_numeric(df[TARGET_NUM], errors="coerce")
    m = p.notna().all(axis=1) & y.notna()
    if not m.any():
        raise ValueError("sharp_baseline_metrics: ninguna fila evaluable "
                         "(sin cuotas de Pinnacle o sin resultado).")
    return _score_probs(p, y, m)


# Las tres lecturas del precio, con el nombre por el que se las cita en el trabajo.
# `consenso` es la única obligatoria: es la que alimenta las features de mercado y sin
# ella el artefacto no tendría baseline alguno.
PRECIOS = {
    "consenso": market_baseline,
    "pinnacle": sharp_baseline,
    "b365": b365_baseline,
}
_PRECIO_OBLIGATORIO = "consenso"


def baselines_report(df: pd.DataFrame) -> dict:
    """Compara las tres lecturas del precio y declara cuál es el número a batir.

    Un pronóstico no se juzga contra «el mercado» en abstracto sino contra un precio
    concreto, y los tres disponibles no son intercambiables: el consenso promedia las
    casas recogidas, Pinnacle es el precio con margen más estrecho y límites más altos
    —el que refleja dinero informado— y Bet365 es la casa minorista de gran difusión.
    Publicar las tres impide la tentación de elegir a posteriori la que más favorezca
    al modelo.

    La comparación se hace sobre la INTERSECCIÓN de filas evaluables por todas ellas.
    Medir cada precio sobre su propia cobertura los enfrentaría en muestras distintas y
    la diferencia de log-loss recogería tanto la calidad del precio como la dificultad
    de los partidos que cada uno cubre. Sobre esa base común se elige como referencia
    la de MENOR log-loss, porque una referencia laxa convierte en aparente cualquier
    mejora del modelo: batir a la casa minorista y perder contra el dinero informado no
    sería un resultado presentable.

    Devuelve las métricas de cada precio sobre su cobertura propia (`precios`), las
    métricas pareadas sobre la base común (`paired`) y el veredicto (`reference`). Un
    precio que el frame no traiga queda a None y se excluye de la base común sin
    interrumpir la ejecución: la referencia sigue siendo la mejor de las disponibles.
    """
    y = pd.to_numeric(df[TARGET_NUM], errors="coerce") if TARGET_NUM in df.columns \
        else None
    if y is None:
        raise KeyError(f"baselines_report: falta la columna '{TARGET_NUM}'.")

    ternas: dict[str, pd.DataFrame] = {}
    precios: dict[str, dict | None] = {}
    notas: list[str] = []
    for nombre, fn in PRECIOS.items():
        try:
            p = fn(df)
        except (KeyError, ValueError) as e:
            precios[nombre] = None
            notas.append(f"{nombre} no evaluable: {e}")
            if nombre == _PRECIO_OBLIGATORIO:
                raise
            continue
        m = p.notna().all(axis=1) & y.notna()
        if not m.any():
            precios[nombre] = None
            notas.append(f"{nombre} sin ninguna fila evaluable")
            continue
        ternas[nombre] = p
        precios[nombre] = _score_probs(p, y, m)

    out: dict = {"precios": precios, "paired": None,
                 "reference": _PRECIO_OBLIGATORIO, "note": "; ".join(notas)}

    comun = y.notna()
    for p in ternas.values():
        comun &= p.notna().all(axis=1)
    if len(ternas) < 2 or not comun.any():
        if len(ternas) >= 2:
            out["note"] = "; ".join(notas + ["sin filas con todos los precios: "
                                             "comparación no concluyente"])
        return out

    par = {"n": int(comun.sum()), "coverage": float(comun.mean()),
           "por_precio": {n: _score_probs(p, y, comun) for n, p in ternas.items()}}
    base = par["por_precio"][_PRECIO_OBLIGATORIO]["log_loss"]
    par["delta_log_loss"] = {n: v["log_loss"] - base
                             for n, v in par["por_precio"].items()}
    out["paired"] = par
    out["reference"] = min(par["por_precio"],
                           key=lambda n: par["por_precio"][n]["log_loss"])
    return out


def report(st: PreprocessState) -> None:
    m = st.meta
    print("=" * 72)
    print(f"PREPROCESS.fit — resumen  [régimen: {st.regime}]")
    print("=" * 72)
    print(f"candidatas={m['n_candidates']}  ->  features salida={m['n_features_out']}"
          f"  (escaladas: {m['n_scaled']}; flags: {len(st.flag_cols)})")
    if m.get("n_regime_excluded"):
        print(f"excluidas por régimen ({m['n_regime_excluded']}): "
              f"{m['regime_excluded_cols']}")
    print(f"umbral colinealidad |r|>{m['corr_threshold']}")
    print("\n— PODA POR REGLAS (D3) —")
    print(f"constantes  ({m['n_const']}): {m['const_cols']}")
    print(f"colineales tirados ({m['n_collinear']}) — se queda el de mayor MI*cobertura:")
    for a, b, r in m["collinear_pairs_kept_dropped"]:
        print(f"    r={r:.3f}  KEEP {a:28s} DROP {b}")
    if m.get("transform_enabled"):
        print(f"\n— SIMETRIZACIÓN YEO-JOHNSON (D5) — {m['n_transformed']} columnas")
        for c, lam, s0, s1 in m.get("transform_skew_before_after", []):
            print(f"    lambda={lam:+.3f}  skew {s0:+.2f} -> {s1:+.2f}   {c}")
    print("\n— IMPUTACIÓN POR CAUSA (D2) —")
    sc = m["impute_strategy_counts"]
    print(f"weak_prior (ascendidos, cuantil {m['weak_prior_q']:.0%}): {sc['weak_prior']} cols")
    for c, v in m["weak_prior_values"].items():
        print(f"    {c:22s} <- {v:,.1f}")
    print(f"zero (ausencia real: {list(_ZERO_FAMILIES)}): {sc['zero']} cols")
    print(f"known (arranque de temporada, valor cierto): {sc['known']} cols")
    print(f"neutral (mediana dev, cold-start/época): {sc['neutral']} cols")
    print(f"\ncategóricas ordinales: {list(st.categ_maps)}")
    print(f"flags de faltante por causa: {list(st.flag_cols)}")


if __name__ == "__main__":
    proc = _HERE.parent.parent / "data" / "02_processed"
    feat = pd.read_parquet(proc / "features_matches.parquet")
    dev = feat[feat[SPLIT] == "dev"].copy()
    for reg in REGIMES:
        st = fit(dev, regime=reg)
        report(st)
        Xdev = apply(dev, st)
        Xhold = apply(feat[feat[SPLIT] == "holdout"].copy(), st)
        fc = [c for c in st.feature_cols if c in Xdev.columns]
        print(f"\ndev -> {Xdev.shape}   holdout -> {Xhold.shape}")
        print(f"NaN feats dev={int(Xdev[fc].isna().sum().sum())} "
              f"holdout={int(Xhold[fc].isna().sum().sum())}   "
              f"esquema idéntico={list(Xdev.columns) == list(Xhold.columns)}\n")
