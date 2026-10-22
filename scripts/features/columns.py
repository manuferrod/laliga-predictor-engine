"""Contrato de columnas del máster: qué es PRE-partido (usable) vs FUGA (prohibido).

Este módulo es la ÚNICA fuente de verdad sobre qué columnas del máster pueden
entrar a un modelo tal cual, cuáles describen el propio partido (y por tanto son
fuga de información si se usan crudas), y cuál es el objetivo.

La lógica es simple y auditable:

  * PREMATCH_PASSTHROUGH : se conocen ANTES del pitido inicial (precio del
    mercado, Elo, valor de plantilla, fatiga, bajas, rotación). Pasan al set de
    features sin transformar. QUÉ se transporta de cada bloque lo decide
    `catalogo.py`, no este módulo: aquí solo se traducen los atributos que el
    catálogo declara a los nombres con que el máster los rotula.
  * SERVICIO            : columnas que VIAJAN en la matriz pero NO son features.
    Son las cuotas crudas de cada casa: el backtest económico las necesita para
    liquidar apuestas y el patrón de comparación necesita el precio de Bet365,
    pero como variables serían la misma información repetida decenas de veces
    (regla 3 del catálogo). `preprocess` las excluye del conjunto del modelo.
  * LEAKAGE_RAW         : describen lo que PASÓ en el partido (goles, tiros,
    posesión, xG del propio partido, calidad de las ocasiones que generó cada
    equipo, rating...). NUNCA entran crudas. Solo se
    usan como MATERIA PRIMA para construir "forma" de partidos ANTERIORES
    (ver form.py), siempre con shift().
  * TARGET_SOURCE       : de dónde sale la etiqueta 1X2.

qa_features.py comprueba que ninguna columna LEAKAGE_RAW acabe en el set final.

Sobre el bloque pre-partido se superpone un SEGUNDO eje, independiente del
anterior: el MOMENTO en que cada dato llega a estar disponible. Ser pre-partido
significa "existe antes del pitido inicial"; no significa "existe cuando se
emite la predicción". Las cuotas de cierre son el caso paradigmático y se tratan
en `weekly_unavailable`, junto al segundo motivo de indisponibilidad: una casa
que el histórico publica pero que el feed de la jornada por jugar no trae (ver
`SHARP_ODDS`). Los dos ejes son ortogonales y deben leerse juntos: una
columna de fuga está prohibida siempre, y una columna pre-partido no disponible
en el momento de predecir está permitida en el estudio retrospectivo y prohibida
en el modelo que se publica.
"""
from __future__ import annotations

import catalogo

# --- clave e identidad del partido (no son features) --------------------------
KEYS = ["match_key", "season", "season_year", "date",
        "home_team", "away_team", "home_key", "away_key", "match_id", "round"]

# Las dos claves que el resto del proyecto nombra a todas horas —el eje temporal
# del protocolo y el corte por temporada de la evaluación— con nombre propio, para
# que un cambio de rótulo en el maestro no haya que perseguirlo por literales.
DATE = "date"
SEASON = "season"

# --- objetivo -----------------------------------------------------------------
# ftr in {"H","D","A"}. El numérico se deriva en build_features (H=0,D=1,A=2).
TARGET_SOURCE = "ftr"
TARGET = "y"
TARGET_NUM = "y_num"

# --- COLUMNAS DE SERVICIO: viajan en la matriz, no son features ---------------
# Las cuotas crudas de cada casa, en apertura y cierre, para el 1X2, el over/under
# y el hándicap asiático. No entran al modelo (regla 3 del catálogo) pero no
# pueden salir de la matriz: `models/backtest.py` liquida las apuestas con el
# precio realmente disponible y `models/evaluate.py` construye con las de Bet365
# el patrón de comparación frente al que se mide todo el trabajo. Sacarlas del
# parquet obligaría a volver al máster en cada evaluación y a mantener alineados
# dos ficheros distintos.
SERVICIO_MASTER = [
    "odds_b365_h", "odds_b365_d", "odds_b365_a",
    "odds_ps_h", "odds_ps_d", "odds_ps_a",
    "odds_max_h", "odds_max_d", "odds_max_a",
    "odds_avg_h", "odds_avg_d", "odds_avg_a",
    "odds_close_avg_h", "odds_close_avg_d", "odds_close_avg_a",
    "odds_close_max_h", "odds_close_max_d", "odds_close_max_a",
    "odds_b365_close_h", "odds_b365_close_d", "odds_b365_close_a",
    "odds_over25", "odds_under25", "odds_b365_over25", "odds_b365_under25",
    "odds_close_over25", "odds_close_under25",
    "odds_ah_home", "odds_ah_away",
    "ah_close_line", "odds_ah_close_home", "odds_ah_close_away",
]

# La otra mitad del servicio no llega del máster: la calcula `prematch_derived`
# al quitarle el margen a las cuotas de Bet365. Es el patrón de comparación —la
# probabilidad que publica la casa contra la que se mide el modelo— y por eso se
# construye en la misma pasada y sobre exactamente las mismas filas que el
# pronóstico. Tampoco es una variable: el consenso del mercado ya contiene ese
# precio, de modo que entregarla al estimador sería darle dos veces lo mismo.
SERVICIO_DERIVADO = list(catalogo.SERVICIO_B365)

# Lo que el preprocesado y el modelado tienen que descartar es la unión de las
# dos. La distinción entre ellas solo importa a quien ENSAMBLA la matriz, porque
# unas se copian del máster y las otras se calculan.
SERVICIO = SERVICIO_MASTER + SERVICIO_DERIVADO

# --- COLUMNAS DE PRESENTACIÓN: viajan en la matriz, no son features -----------
# El par por lados de los atributos que la aplicación dibuja equipo a equipo
# (`catalogo.PRESENTACION`). Es el mismo caso que las cuotas de servicio con otro
# consumidor: el radar pre-partido y la línea temporal de `models/publish_web.py`
# necesitan el número de cada equipo —una diferencia no dibuja a un equipo— y el
# estimador no debe verlas, porque la señal ya le llega en la resta y el par solo
# la repetiría.
#
# Que viajen en el parquet en lugar de releerse del máster al publicar es lo que
# sostiene la garantía de la que vive la web: la foto que acompaña a una
# predicción es la que vio el modelo, misma fila y mismo fichero. Releyéndolas
# habría que volver a alinear dos ficheros en cada publicación, que es
# exactamente el motivo por el que las cuotas tampoco salen de aquí.
#
# El infijo `fat_` se aplica aquí y no en el catálogo por el mismo motivo que en
# `_FATIGUE`: cómo se rotula la columna es decisión del módulo que ensambla la
# matriz, y el catálogo declara el ATRIBUTO (`rest_days`), no su rótulo.
_PRESENTACION_PREFIJO = {"rest_days": "fat_"}
PRESENTACION = [n for a in catalogo.PRESENTACION
                for n in catalogo.nombres(a, _PRESENTACION_PREFIJO.get(a, ""))
                if not n.endswith("_diff")]

# --- BLOQUE PRE-PARTIDO: se conoce antes del partido, pasa tal cual -----------
# El precio del mercado, reducido a lo que el catálogo declara: la probabilidad
# de consenso ya sin margen y la línea del hándicap asiático de apertura, que
# expresa el favoritismo en goles y no es una transformación monótona de aquella.
_MERCADO = list(catalogo.MERCADO_CONSENSO) + list(catalogo.MERCADO_HANDICAP)

# La fuerza relativa de club: la diferencia de los dos Elo TIPIFICADOS dentro de
# la jornada. El par de niveles tipificados viaja también, pero como PRESENTACIÓN
# y no como variable —el radar dibuja la fuerza de cada equipo—, de modo que lo que
# el modelo ve de este bloque sigue siendo una sola columna.
#
# El nivel CRUDO no entra al modelo ni viaja aquí: se queda en el máster como
# columna descriptiva —ahí lo audita `qa_master`— porque el histórico publicado
# empalma las dos escalas de ClubElo y solo la tipificación por fecha es comparable
# de 2016 a hoy. Tampoco viaja como SERVICIO: `backtest` y `evaluate` no liquidan
# nada con el nivel, y meterlo ahí convertiría esa lista, que hoy dice exactamente
# con qué se liquida, en un cajón de columnas «por si acaso».
_ELO = catalogo.nombres(catalogo.ELO_ATRIBUTO)

# Valor de mercado y edad de la plantilla, como prior de fuerza. De las tres
# lecturas del valor —par, resta y log-razón— se queda el par, por el hueco del
# ascendido que solo admite relleno por lado; de la edad, la diferencia, que es
# donde está la información: lo que discrimina no es tener una plantilla joven
# sino ser más joven que el rival.
_SQUAD = [n for a in catalogo.PLANTILLA_PAR + catalogo.PLANTILLA_DIFERENCIA
          for n in catalogo.nombres(a)]

# Fatiga y congestión. El máster rotula estas columnas con el infijo `fat_`.
_FATIGUE = [n for a in catalogo.FATIGA_ATRIBUTOS
            for n in catalogo.nombres(a, "fat_")]

# Bajas: el recuento total y las dos lecturas que PESAN la ausencia en lugar de
# contarla —qué porción de los minutos del once falta y de qué tamaño es la peor
# baja—, más el porcentaje de valor de plantilla ausente.
_ABSENCES = [n for a in catalogo.BAJAS_ATRIBUTOS for n in catalogo.nombres(a)]

# ROTACIÓN Y CARGA DE MINUTOS (`extractors/lineups.py` -> `reduce_lineups`).
# Merece subrayarse por qué un bloque construido sobre las alineaciones —que son
# un hecho DEL partido, y por tanto fuga— puede figurar aquí: porque ninguna de
# estas columnas describe el encuentro que se va a predecir. Todas se calculan
# mirando exclusivamente a partidos ANTERIORES del mismo equipo, igual que las
# features de forma, de modo que lo que llega al máster ya es información
# pre-partido. La materia prima (quién jugó y cuántos minutos) se queda en
# `player_minutes.parquet` y nunca entra en el conjunto de entrenamiento.
_LINEUPS = [n for a in catalogo.ROTACION_ATRIBUTOS for n in catalogo.nombres(a)]


PREMATCH_PASSTHROUGH = _MERCADO + _ELO + _SQUAD + _FATIGUE + _ABSENCES + _LINEUPS


# --- METADATO DEL MÁSTER: pre-partido, pero NO pasa tal cual ------------------
# Columnas que el máster transporta para que un módulo de features las derive y
# que, sin embargo, no son features. La distinción importa porque no encajan en
# ninguno de los dos bloques anteriores: no describen lo ocurrido en el partido
# —así que no son fuga— pero tampoco significan nada por sí mismas.
#
#   * kickoff_ts : sello de saque en segundos desde la época. De él salen la
#     franja de emisión y el fin de semana (`features/context.py`); la hora local
#     se calcula por el camino pero no se emite, porque es la misma información
#     que la franja sin agrupar en los escalones que significan algo. En crudo es un
#     índice temporal casi único por partido, de modo que dejarlo pasar ofrecería
#     al estimador una vía para memorizar el calendario en lugar de aprender de
#     él; `qa_features` comprueba que no aparezca en el set de features.
MASTER_METADATA = ["kickoff_ts"]


# --- RÉGIMEN DE INFORMACIÓN: qué se conoce al EMITIR la predicción ------------
# Ser pre-partido y ser conocible en el momento de predecir son propiedades
# distintas. La predicción de una jornada se emite con días de antelación; las
# cuotas de CIERRE, en cambio, solo existen minutos antes del saque, porque son
# el último precio que publica el mercado tras incorporar alineaciones, partes
# médicos y dinero de última hora.
#
# La consecuencia de ignorar esa distinción es un desajuste silencioso: el
# modelo aprendería a apoyarse en la variable más informativa del dominio y
# luego, semana tras semana, recibiría esa variable vacía y por tanto imputada.
# La métrica histórica describiría entonces un sistema que no es el que se
# ejecuta. Para evitarlo, el trabajo distingue dos REGÍMENES de información y
# entrena y evalúa los dos con el mismo protocolo:
#
#   * SEMANAL       : solo columnas conocibles al emitir. Es el modelo que se
#                     publica y cuya métrica se defiende como capacidad
#                     predictiva del sistema.
#   * RETROSPECTIVO : incluye además el cierre. No se publica; su comparación
#                     con el semanal cuantifica cuánta información añade el
#                     mercado entre la apertura y el cierre, es decir, el techo
#                     alcanzable con información pre-partido completa.
#
# `footballdata.load_upcoming_processed` publica por diseño solo cuotas de
# apertura, de modo que la ruta de inferencia es coherente con el régimen
# semanal por construcción; `build_predict` lo verifica antes de puntuar.
#
# CONVENIO DE NOMBRES (obligatorio para lo que se añada): toda columna derivada
# del cierre lleva el token `close` en su nombre. Gracias a él el predicado de
# abajo es completo por construcción y no hay que mantener una lista paralela
# que se desincronice.
_CLOSING_TOKEN = "close"

# Columnas de cierre nombradas explícitamente. La lista documenta el bloque y
# sirve de red de seguridad frente a un renombrado que rompiera el convenio.
CLOSING_ODDS = [c for c in SERVICIO if _CLOSING_TOKEN in c]

# --- segundo motivo de indisponibilidad: la casa no viaja en el feed de la
# --- jornada por jugar -------------------------------------------------------
# Ser de APERTURA no implica ser SERVIBLE, y confundirlo es la misma categoría de
# error que el párrafo de arriba denuncia para «pre-partido». El histórico de
# football-data (`SP1.csv`) trae una columna por casa, entre ellas Pinnacle
# (`PSH/PSD/PSA`); el feed de la jornada por jugar (`fixtures.csv`) trae solo
# Bet365, el máximo y la media del mercado. No hay Pinnacle. Así que
# `odds_ps_*` —cuota de apertura, pre-partido y perfectamente legítima en el
# estudio retrospectivo— llega VACÍA cada semana en la ruta de inferencia, y con
# ella las columnas que se derivan de la casa afilada.
#
# El efecto es exactamente el desajuste que este módulo está para evitar: el
# modelo aprendería `sharp_div_*` sobre el 97.9 % de los partidos de desarrollo y
# luego recibiría esas tres columnas imputadas con la mediana en los diez
# partidos de cada jornada, de modo que su métrica histórica describiría un
# sistema que no es el que se ejecuta. Imputar no avisa: no sale ningún NaN, sale
# una constante.
#
# A esto se añade que Pinnacle **desapareció** del histórico a mitad de la
# temporada 2025/26 (cobertura de `PSH` en el año natural 2026: 6.5 %), así que
# tampoco es una columna que se pueda recuperar reextrayendo. Lo que queda por
# decidir —y es decisión de Manuel, no del código— es si el régimen
# RETROSPECTIVO sigue usándola: ahí sigue siendo medible en 2016/17–2024/25, pero
# el techo que ese régimen cuantifica se apoyaría en parte en una casa que ya no
# publica. Mientras eso no se decida, el retrospectivo la conserva y el semanal
# no, que es la única combinación que no publica una cifra falsa.
_SHARP_TOKEN = "ps"

# Columnas derivadas de la casa afilada, nombradas una sola vez. Se listan en
# lugar de resolverse por subcadena porque «ps» es un fragmento demasiado común
# para usarlo como convenio: casaría con `points_...` y con media docena más.
SHARP_ODDS = ["odds_ps_h", "odds_ps_d", "odds_ps_a",
              "mkt_p_ps_home", "mkt_p_ps_draw", "mkt_p_ps_away",
              "mkt_overround_ps",
              "sharp_div_home", "sharp_div_draw", "sharp_div_away",
              "sharp_div_tv"]


def weekly_unavailable(columns) -> list[str]:
    """Columnas de `columns` NO conocibles en el momento de emitir la predicción.

    Dos motivos, no uno. El precio de CIERRE no existe todavía cuando se emite;
    se resuelve por el convenio de nombres (`close`) y se completa con la lista
    explícita, de modo que una columna del bloque de cierre nunca puede
    escaparse por haber sido renombrada sin actualizar el convenio. La casa
    AFILADA sí existe pero no viaja en el feed de la jornada por jugar, de modo
    que llegaría vacía e imputada; se resuelve por lista declarada porque su
    token no admite convenio (ver `SHARP_ODDS` arriba).
    """
    por_convenio = {c for c in columns if _CLOSING_TOKEN in c}
    explicitas = {c for c in CLOSING_ODDS if c in columns}
    afiladas = {c for c in SHARP_ODDS if c in columns}
    return sorted(por_convenio | explicitas | afiladas)


def weekly_available(columns) -> list[str]:
    """Complemento de `weekly_unavailable`: el régimen semanal sobre `columns`."""
    fuera = set(weekly_unavailable(columns))
    return [c for c in columns if c not in fuera]


# Nombre de los dos regímenes, para que los artefactos y los informes usen la
# misma etiqueta y no proliferen sinónimos.
REGIMES = ("semanal", "retrospectivo")


def regime_columns(columns, regime: str) -> list[str]:
    """Subconjunto de `columns` admisible bajo el régimen indicado."""
    if regime not in REGIMES:
        raise ValueError(f"régimen desconocido: {regime!r}; use uno de {REGIMES}")
    return weekly_available(columns) if regime == "semanal" else list(columns)

# --- BLOQUE FUGA: describe el propio partido. Prohibido en crudo --------------
# resultado y parciales
_RESULT_RAW = ["fthg", "ftag", "hthg", "htag", "htr"]
# xG/Understat del propio partido (materia prima para forma, no feature cruda)
_UNDERSTAT_RAW = [
    "home_xg", "away_xg", "home_npxg", "away_npxg",
    "home_ppda", "away_ppda", "home_deep", "away_deep",
    "home_xpts", "away_xpts",
]
# rating SofaScore del propio partido
_RATING_RAW = ["home_sofa_rating", "away_sofa_rating"]
# CALIDAD DE OCASIÓN del propio partido (`extractors/understat_shots.py`).
# Merece explicarse por qué este bloque cae aquí y no en PREMATCH_PASSTHROUGH,
# siendo que el bloque de alineaciones —construido también sobre un hecho del
# partido— sí pasa tal cual. La diferencia no está en la fuente sino en QUÉ
# describe cada columna. La rotación llega al máster ya desplazada: cuenta lo que
# el equipo hizo en encuentros ANTERIORES, de modo que en el momento de predecir
# existe. Estas cuatro magnitudes, en cambio, resumen la distribución de tiros de
# ESTE partido, que es exactamente lo que se quiere pronosticar; usarlas crudas
# sería predecir el resultado a partir de las ocasiones que lo produjeron. Su
# aprovechamiento legítimo es idéntico al del xG: pasan por `teamlong.py` y las
# ventanas retrospectivas de `form.py`, que solo miran hacia atrás.
_SHOTQUALITY_RAW = [
    "home_shot_xg_mean", "away_shot_xg_mean",
    "home_shot_dist_mean", "away_shot_dist_mean",
    "home_shot_setpiece_xg_pct", "away_shot_setpiece_xg_pct",
    "home_shot_xg_max", "away_shot_xg_max",
]

def _all_sofa_stat_cols(columns) -> list[str]:
    """Todas las columnas de estadística de partido de SofaScore (124)."""
    return [c for c in columns if c.startswith("sofa_")]

# LEAKAGE_RAW fijas (las que no dependen de introspección de columnas)
LEAKAGE_RAW_FIXED = (_RESULT_RAW + _UNDERSTAT_RAW + _RATING_RAW
                     + _SHOTQUALITY_RAW)


def leakage_columns(columns) -> list[str]:
    """Lista completa de columnas de fuga presentes en `columns`."""
    fixed = [c for c in LEAKAGE_RAW_FIXED if c in columns]
    return sorted(set(fixed) | set(_all_sofa_stat_cols(columns)))


# --- HOLDOUT CIEGO ------------------------------------------------------------
# Temporada(s) SELLADA(S): no se miran para elegir features ni hiperparámetros.
# La selección se hace por walk-forward SOLO sobre 'dev'. El holdout se toca una
# única vez, al final, para reportar la métrica honesta. El sellado responde a un
# riesgo concreto: si el conjunto de variables se elige comparando resultados sobre
# la temporada de evaluación, esa temporada deja de ser una prueba independiente y
# la métrica publicada queda inflada por fuga de selección.
HOLDOUT_SEASONS = ["2025/26"]

# Columna que materializa ese sellado en la tabla de variables: `build_features` la
# escribe con los valores «dev» y «holdout», y a partir de ahí toda la cadena la lee
# en lugar de volver a comparar la temporada contra la lista. El nombre vive aquí,
# junto al criterio que lo produce, porque el preprocesado y el protocolo de
# validación lo consultan por separado y dos literales iguales escritos en dos
# módulos distintos son dos literales que pueden dejar de serlo.
SPLIT = "split"
