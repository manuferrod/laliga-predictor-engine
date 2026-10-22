"""El CATÁLOGO de variables: qué mide el proyecto, y con qué forma lo mide.

Este módulo es la única fuente de verdad sobre el conjunto de variables que la
capa de features produce. Los módulos generadores (`form`, `h2h`, `standings`,
`market_form`, `prematch_derived`, `strength`, `context`) leen de aquí qué
atributos calcular y en qué lados emitirlos, de modo que el conjunto no es el
residuo de lo que cada módulo tuvo a bien construir sino una decisión declarada
en un solo sitio y auditable de un vistazo.

POR QUÉ EXISTE UN CATÁLOGO
==========================
Una fuente de datos completa invita a construir todo lo que permite construir, y
el resultado natural de esa invitación es una matriz de varios cientos de
columnas en la que la mayoría son la misma medida repetida. El coste no es de
disco: es que una selección de variables sobre un espacio enorme elige, con alta
probabilidad, ruido que se parece a señal; y que un modelo con más columnas que
partidos por temporada no se puede interpretar ni defender.

El catálogo invierte el orden. Primero se decide qué CONCEPTOS debe conocer el
modelo —la forma reciente, el precio del mercado, la fuerza latente, las bajas,
la fatiga, la rotación, la clasificación, lo que el partido se juega, la
historia del enfrentamiento y el contexto—, y después se elige la forma mínima
de medir cada concepto. Lo que no está aquí no se construye.

LAS CINCO REGLAS QUE PRODUCEN ESTE CONJUNTO
===========================================
1. UNA PARAMETRIZACIÓN POR ATRIBUTO, salvo que la segunda responda a otra
   pregunta. La ponderación exponencial (`_ewm`) es la memoria por defecto:
   decae de forma continua con una semivida declarada y no obliga a elegir una
   ventana arbitraria. Se le añade una ventana corta (`_3`) porque «cómo llega
   ahora mismo» y «cómo ha ido últimamente» son preguntas distintas. Una tercera
   ventana intermedia no añade una tercera pregunta, solo una tercera respuesta
   a la misma, y por eso no está.

2. O EL PAR O LA DIFERENCIA, NUNCA LOS TRES. `home_x`, `away_x` y `x_diff` son
   linealmente dependientes: emitir los tres regala una columna redundante por
   atributo. Se emite la diferencia cuando lo que importa es la ventaja relativa
   (forma, historial, clasificación, trascendencia) y el par cuando el nivel
   absoluto informa por sí mismo (Elo, valor de plantilla, bajas, fatiga,
   rotación: dos equipos con cuatro bajas cada uno no es lo mismo que dos
   equipos sin ninguna, y la diferencia no distingue ambos casos). Para el
   núcleo de la forma se emiten ambos, que es la única excepción y está aquí
   declarada como tal.

3. LAS CUOTAS CRUDAS NO SON VARIABLES. Las columnas de cada casa de apuestas son
   el insumo del que se calcula la probabilidad de consenso sin margen; meterlas
   además como features es entregar la misma información decenas de veces. Viajan
   en la matriz como columnas de servicio —el backtest económico y el patrón de
   comparación de Bet365 las necesitan— y `columns.SERVICIO` las mantiene fuera
   del conjunto de features.

4. UN ATRIBUTO POR CONCEPTO MEDIDO. Entre `xg` y `npxg` se queda el segundo, que
   es el primero sin el ruido de los penaltis. Entre remates, remates a puerta,
   córners y ocasiones claras se quedan los dos que separan volumen de peligro.
   De las tasas de victoria, empate y derrota se quedan dos, porque la tercera es
   uno menos las otras dos.

5. NO SE CONSTRUYE LO QUE YA SE SABE QUE ES RUIDO. Los residuos de suerte
   (`fin_resid`, `def_resid`) miden lo que un equipo se desvió de su xG, que es
   por definición la parte no reproducible de su rendimiento.
"""
from __future__ import annotations

# =============================================================================
# FORMA RECIENTE
# =============================================================================
# Los catorce atributos con los que se describe cómo llega un equipo. Cada uno
# mide un concepto distinto; donde dos medían el mismo se ha quedado uno.
#
#   points, gf, ga         el resultado y sus dos mitades (gd = gf - ga, sobra)
#   npxg, npxga            el peligro generado y concedido, sin penaltis
#   xpts                   los puntos merecidos, que resumen ambas
#   sot, big_chances       volumen de remate y ocasión clara: llegar y llegar bien
#   ppda, deep             presión y progresión: el estilo, no el acierto
#   shot_xg_mean(_against) la calidad del remate medio propio y concedido, que
#                          separa disparar mucho y mal de disparar poco y bien
#   rating                 la nota agregada, como resumen de lo no capturado
#   opp_elo_z              la dureza del calendario reciente, sin la cual todo lo
#                          anterior se lee fuera de contexto
#
# El sufijo `_z` de `opp_elo_z` no es decorativo: el Elo viaja TIPIFICADO dentro
# de la jornada (desviaciones respecto a la media de Primera ese día), no en
# nivel. El nombre lleva la unidad porque una columna rotulada `opp_elo` se
# leería como un rating de la escala 1700 y aquí vale del orden de ±2.
FORMA_ATRIBUTOS = [
    "points", "gf", "ga",
    "npxg", "npxga", "xpts",
    "sot", "big_chances",
    "ppda", "deep",
    "shot_xg_mean", "shot_xg_mean_against",
    "rating",
    "opp_elo_z",
]

# La ventana corta. Se sigue CALCULANDO en `form.py` —de ella salen los
# acumulados por escenario y es el contraste con el que se lee la memoria
# exponencial— pero no se emite a la matriz, y el motivo está medido: de los
# catorce atributos, la ventana rectangular de tres partidos pierde contra su
# gemela exponencial en trece, con correlaciones de 0.83 a 0.94 entre ambas. El
# único donde gana es `opp_elo_z` (0.00281 frente a 0.00242), y ahí las dos
# lecturas están por debajo del ruido de muestreo, así que la victoria no
# significa nada. Dos parametrizaciones del mismo atributo solo se justifican si
# la segunda responde a otra pregunta (regla 1); medido, responde a la misma peor.
FORMA_VENTANA = 3

# Semivida en partidos de la forma exponencial. Se DECLARA, no se ajusta: si se
# optimizase sobre el bloque de evaluación dejaría de ser una descripción del
# pasado para convertirse en un hiperparámetro más.
FORMA_SEMIVIDA = 3.0

# Los seis atributos de resultado y peligro, que son el núcleo de la forma. Se
# emiten, como todo el bloque, SOLO como diferencia.
#
# Aquí vivía la excepción: de estos seis se emitía además el par, con el
# argumento de que el nivel absoluto discrimina —dos equipos que marcan mucho
# producen un partido distinto que dos que no marcan, aunque su diferencia sea la
# misma—. El argumento es razonable y resultó ser falso, y conviene ver por qué,
# porque es el mismo error que la regla 2 persigue. Emitir `home_x`, `away_x` y
# `x_diff` son tres columnas que ocupan DOS dimensiones: la diferencia es la
# resta exacta de las otras dos (error máximo 0.000e+00 en las 3384 filas de
# desarrollo con las tres lecturas). La única dimensión que el par añade sobre la
# diferencia es el NIVEL —la suma de los dos lados—, de modo que la hipótesis del
# nivel se puede medir directamente, y medida no se sostiene: la suma tiene una
# información mutua con el resultado de 0.002 a 0.005 según el atributo, frente a
# 0.021 a 0.055 de la diferencia. El nivel es ruido, la tercera columna era
# redundante por construcción, y de las tres se queda la que lleva la señal.
FORMA_NUCLEO = ["points", "gf", "ga", "npxg", "npxga", "xpts"]

# Acumulados de temporada por partido. `gd` se cae por ser la resta de dos
# acumulados que ya viajan; `xgd` se queda porque es la versión de esa resta que
# no depende de la suerte en la definición.
FORMA_ACUMULADOS = ["points", "xgd"]

# Rendimiento restringido al escenario (local en casa, visitante fuera).
FORMA_ESCENARIO = ["points", "gd"]
FORMA_ESCENARIO_VENTANA = 5

# =============================================================================
# ENFRENTAMIENTOS DIRECTOS
# =============================================================================
# Solo la memoria exponencial: la ventana de ocho enfrentamientos respondía a la
# misma pregunta con otro supuesto de olvido, y en un h2h —donde la muestra es de
# uno o dos partidos por temporada— la ventana rectangular apenas se distingue de
# la exponencial. De las tres tasas se quedan dos (regla 4).
#
# El h2h se calcula sobre un long DIRIGIDO: la misma secuencia de partidos vista
# desde los dos lados. Eso obliga a distinguir qué magnitudes son del EQUIPO y
# cuáles del PAR. La tasa de victoria y la diferencia de goles son del equipo
# —lo que es victoria para uno es derrota para el otro— y por eso su resta
# informa. La tasa de empate no: un empate lo es para los dos a la vez, de modo
# que ambos lados llevan exactamente el mismo número y su diferencia es cero en
# todas las filas del histórico. Emitirla como diferencia añadiría una columna
# constante —ruido de dimensión, cero información— así que se declara aparte y
# sale una sola vez, sin lado, como magnitud del enfrentamiento.
H2H_ATRIBUTOS = ["win_rate_ewm", "gd_ewm"]
H2H_PARTIDO = ["draw_rate_ewm"]
H2H_SEMIVIDA = 6

# =============================================================================
# CLASIFICACIÓN Y TRASCENDENCIA
# =============================================================================
# Dónde está cada equipo y qué se juega. `europe_unreachable` se cae por ser el
# complemento casi determinista de `points_to_europa`, y `stakes_none` por lado
# se cae en favor del producto de ambos, que es la pregunta real: un partido es
# intrascendente cuando lo es para los dos.
#
# Las tres DISTANCIAS A UN CORTE —líder, cuarto, sexto— no se emiten, y el motivo
# es aritmético y no empírico. La distancia de un equipo a un corte es
# `puntos(corte) − puntos(equipo)`, y los puntos del corte son los mismos para los
# dos contendientes porque salen de la misma tabla en la misma fecha: al restar
# local menos visitante se cancelan y las tres diferencias quedan reducidas a
# `−(pts_local − pts_visitante)`, que es exactamente `points_above_drop_diff`
# salvo el signo. No son tres variables correlacionadas con la cuarta: son la
# cuarta, con correlación 1.0000 medida sobre las nueve temporadas de desarrollo.
# Emitirlas y dejar que la poda colineal las encuentre funcionaba, pero esto se
# sabe antes de mirar un solo dato, y lo que se sabe de antemano se decide al
# declarar el conjunto. `safe_from_drop` sí se queda: cuenta rivales
# inalcanzables, no mide una distancia, y su diferencia no se cancela.
#
# El nivel por equipo sigue calculándose en `standings.py` como INSUMO —de ahí
# salen los indicadores de trascendencia— y es ahí donde se puede auditar.
#
# `dynamic_pos_change` —el puesto actual contra el puesto final de la temporada
# pasada— tampoco se emite, y el motivo es su hueco. Un equipo recién ascendido no
# tiene puesto final en Primera, así que la columna llega vacía en el 38 % de los
# partidos de desarrollo y su cobertura no pasa del 70 % en ninguna temporada. Ese
# hueco no admite relleno honesto: la mediana de dev vale exactamente 0.0, es
# decir, «no ha cambiado de sitio», que es a la vez la categoría menos informativa
# de la variable y lo contrario de lo que se sabe del ascendido, cuyo cambio de
# nivel es el mayor de la liga. Una columna que hay que inventar en cuatro de cada
# diez filas, y con el valor equivocado, no es una variable. Lo que medía —si el
# equipo rinde por encima o por debajo de su propio punto de partida— lo cubren con
# cobertura completa `prev_position_diff` junto a `elo_z_diff` y al valor de
# plantilla.
CLASIFICACION_ATRIBUTOS = ["prev_position", "points_above_drop"]
CLASIFICACION_ZONA = "position_zone"          # se emite como par (ordinal)
TRASCENDENCIA_ATRIBUTOS = ["safe_from_drop"]
TRASCENDENCIA_CALENDARIO = "matches_left"     # basta el del local: es el mismo
TRASCENDENCIA_PARTIDO = "stakes_none_match"

# =============================================================================
# MERCADO
# =============================================================================
# La probabilidad de consenso sin margen es la variable; el resto describe la
# ESTRUCTURA de ese precio, que es información que la probabilidad no lleva: cuánto
# margen se queda la casa, cuánto discrepan entre sí, y cuánto se separa el
# consenso minorista de la casa que ajusta con dinero informado.
MERCADO_CONSENSO = ["mkt_p_home", "mkt_p_draw", "mkt_p_away"]
MERCADO_ESTRUCTURA = ["mkt_overround_open", "mkt_spread_mean",
                      "sharp_div_home", "sharp_div_draw", "sharp_div_away"]
# El hándicap asiático de apertura: el favoritismo expresado en goles, que es una
# escala distinta de la probabilidad y no una transformación monótona de ella.
MERCADO_HANDICAP = ["ah_line"]
# Movimiento de la línea entre apertura y cierre. Lleva el token `close`, de modo
# que el régimen semanal lo excluye por sí solo: solo vive en el retrospectivo.
MERCADO_MOVIMIENTO = ["close_move_fav"]

# La probabilidad implícita de Bet365, sin margen. NO es una variable —el
# consenso ya lleva ese precio (regla 3)— sino el patrón de comparación contra el
# que se mide todo el trabajo. Se declara aquí, junto al resto del mercado,
# porque quien lea el catálogo para saber qué construye el proyecto necesita ver
# que esto también se construye; `columns.SERVICIO_DERIVADO` es quien lo mantiene
# fuera del conjunto que ve el estimador.
SERVICIO_B365 = ["mkt_p_b365_home", "mkt_p_b365_draw", "mkt_p_b365_away"]

# =============================================================================
# FUERZA
# =============================================================================
# La fuerza de club de ClubElo, TIPIFICADA dentro de la jornada: cuántas
# desviaciones separan al club de la media de los veinte de Primera ese día. No
# es una normalización de conveniencia sino la única lectura comparable a lo
# largo de todo el histórico, porque ClubElo cambió de escala en octubre de 2022
# y la serie publicada empalma dos tramos que difieren en un desplazamiento y una
# compresión (ver `consolidate/spine.add_clubelo`). La tipificación por fecha es
# invariante a esa transformación afín: la quita entera. El nivel crudo
# (`home_elo`, `away_elo`, `elo_diff`) viaja en el máster como descriptivo y
# NO entra al modelo.
#
# Se emite la DIFERENCIA de las dos tipificadas (regla 2), y la decisión está
# medida: el nivel conjunto —la suma de las dos `z`— tiene una información mutua
# con el resultado de 0.0072, del orden del ruido, mientras `elo_z_diff` alcanza
# 0.0894, que es la segunda cifra más alta de todo el banco por detrás solo del
# precio del mercado, y aproximadamente el doble de la de cada lado por separado
# (0.0417 el local, 0.0488 el visitante). Tiene sentido: la `z` ya está medida
# contra la media de Primera de esa jornada, así que el nivel conjunto solo dice
# si a ese partido le tocaron dos equipos buenos, y lo que decide un 1X2 es quién
# de los dos es mejor. La diferencia es además la forma en que el Elo se usa
# siempre que se usa para pronosticar —la fórmula de expectativa de Elo solo mira
# la resta— de modo que esta lectura es la coherente con la fuente, no un recorte.
ELO_ATRIBUTO = "elo_z"
# El modelo de goles publica su propio pronóstico 1X2, que es la forma más
# compacta de entregar lo que ha estimado. `strength_exp_goals` se cae porque los
# goles esperados son exactamente lo que esa terna resume.
FUERZA_PRONOSTICO = ["strength_p_home", "strength_p_draw", "strength_p_away"]
FUERZA_ATRIBUTOS = ["strength_attack", "strength_defence"]

# =============================================================================
# PLANTILLA, BAJAS, FATIGA Y ROTACIÓN
# =============================================================================
# El valor de plantilla es la única magnitud de equipo que se emite como par, y no
# porque su nivel lleve señal —no la lleva: 0.0049 frente a 0.0845 de la
# diferencia— sino porque su hueco lo exige. Un equipo recién ascendido llega sin
# cotización, y la lectura correcta de ese hueco es «plantilla barata», que es un
# valor del lado y no de la resta: imputar la resta con su mediana diría «los dos
# valen lo mismo», que es justo lo contrario de lo que se sabe. Emitido el par,
# `preprocess` rellena cada lado con el decil inferior de dev y la resta la hace
# después el estimador, que para eso es lineal. La edad sí sale como diferencia:
# su hueco no tiene lectura de lado.
PLANTILLA_PAR = ["squad_value"]
PLANTILLA_DIFERENCIA = ["age"]

# De las cinco maneras de contar ausentes se queda el total; las otras cuatro son
# su desglose. A él se añaden las dos que pesan la baja en vez de contarla: qué
# porción del once falta y de qué tamaño es la peor ausencia.
BAJAS_ATRIBUTOS = ["n_missing", "out_minshare", "out_minshare_max",
                   "out_value_pct"]
# `games_last_7` se cae por ser el tramo inicial de `games_last_14`, y
# `is_post_intl_break` por concentrarse en un puñado de jornadas al año.
#
# `is_midweek` tampoco se emite, por dos motivos que se suman. El primero es de
# construcción: una jornada entre semana lo es para los dos equipos a la vez, de
# modo que las lecturas local y visitante llevan exactamente el mismo número
# —correlación 1.0000 en las nueve temporadas de desarrollo— y el par es un
# duplicado, el mismo defecto que el catálogo ya resolvió en `h2h_draw_rate`. El
# segundo es de señal: su información mutua con el resultado (0.0016) era la más
# baja de las 102 variables del banco y la única por debajo del ruido de muestreo,
# así que la regla 5 la descarta por sí sola. `context.is_weekend` conserva la
# parte informativa del reparto del calendario, y la congestión real —la que sí
# cansa— la miden `rest_days` y `games_last_14`.
FATIGA_ATRIBUTOS = ["rest_days", "games_last_14", "euro_trip_recent"]
# La continuidad del once, la carga de los titulares y la concentración de
# minutos. `n_players_used` es la lectura gruesa de lo que `minutes_hhi` mide fino.
ROTACION_ATRIBUTOS = ["xi_continuity", "starters_minutes_14", "minutes_hhi"]

# =============================================================================
# PSICOLÓGICAS DE MERCADO Y CONTEXTO
# =============================================================================
# `post_big_fav_loss` no se construye: su información mutua con el resultado es
# del orden del ruido de muestreo (regla 5).
PSICOLOGICAS_DIFERENCIA = ["relative_perf"]
PSICOLOGICAS_PAR = ["post_big_win"]

# `kickoff_hour` se cae en favor de la franja, que es la misma información
# agrupada en las tres categorías que significan algo (mediodía, tarde, noche).
CONTEXTO = ["is_weekend", "away_travel_km", "is_derby"]
CONTEXTO_CATEGORICO = ["kickoff_slot"]


# =============================================================================
# LADOS: qué versión de cada atributo de equipo se emite
# =============================================================================
# Un atributo medido por equipo puede salir a la matriz de partido de tres
# formas: `home_x`, `away_x` y `x_diff`. La regla 2 dice que se elige el par o la
# diferencia, y este mapa la aplica. Las tres a la vez no son una opción, y no por
# prudencia: la diferencia es la resta exacta de las otras dos, así que tres
# columnas ocuparían dos dimensiones y la tercera sería redundante por
# construcción, no solo correlacionada.
#
# Cuál de las dos lecturas se elige se decide midiendo, y la medida es la misma
# para todos los atributos del proyecto. El par y la pareja {diferencia, nivel}
# generan el mismo espacio, de modo que la pregunta «¿par o diferencia?» es
# exactamente «¿el NIVEL —la suma de los dos lados— lleva señal?». En los catorce
# atributos continuos medidos sobre las nueve temporadas de desarrollo, no la
# lleva: la información mutua del nivel con el resultado va de 0.002 a 0.007,
# mientras la de la diferencia va de 0.016 a 0.089 y es en todos los casos
# aproximadamente el doble de la de cada lado por separado. Por eso el mapa dice
# DIFERENCIA casi siempre, y el PAR queda para dos casos en los que la resta no
# es una operación legítima: las categóricas —`playstyle`, `position_zone`—,
# donde restar códigos no significa nada, y el valor de plantilla, cuyo hueco de
# equipo ascendido solo admite relleno por lado (ver `preprocess._WEAK_PRIOR_COLS`:
# la lectura correcta de un ascendido es «plantilla barata», y eso se escribe en
# `home_squad_value`, no en una resta ya consumada).
DIFERENCIA = ("diff",)
PAR = ("home", "away")
# Un tercer caso, para lo que no se mide por equipo sino por enfrentamiento: sale
# una sola vez y sin rótulo de lado, porque preguntar «¿el del local o el del
# visitante?» no tiene sentido cuando el número es el mismo para los dos.
PARTIDO = ("partido",)
# Y un cuarto, que no es una decisión de modelado sino de PRESENTACIÓN. La
# aplicación dibuja cada equipo por separado —el radar pre-partido y la línea
# temporal de `models/publish_web.py`— y una diferencia no dibuja a un equipo:
# para pintar al Betis hace falta su número, no su ventaja sobre el rival. Estos
# atributos viajan por tanto en los tres lados: la resta, que es la que lleva la
# señal y la única que el modelo ve, y el par, que existe solo para que la
# publicación lo lea.
#
# La objeción que obliga a elegir entre par y resta en el resto del catálogo —los
# tres juntos dejan la matriz de diseño con rango deficiente, porque la resta es
# combinación lineal del par— no aplica aquí, y ese es el punto: el par de
# presentación no es candidato a variable. `columns.PRESENTACION` lo recoge y
# `preprocess._NON_FEATURE` lo absorbe, igual que las cuotas crudas de servicio,
# de modo que nunca llega a la matriz de diseño y no puede hacerla singular.
PAR_Y_DIFERENCIA = ("home", "away", "diff")

# Los atributos que la aplicación dibuja por equipo y que el catálogo emite como
# DIFERENCIA, de modo que el par hay que añadirlo aparte. `squad_value` no figura
# aquí aunque el radar también lo dibuje: ya viaja como PAR por una decisión de
# modelado —el hueco del ascendido—, así que no necesita par añadido ni debe
# salir del conjunto de candidatas.
#
# La lista vive en el catálogo y no en `publish_web` porque es una declaración de
# lo que se EMITE, y eso es competencia de este módulo; `publish_web` no la lee,
# consulta `emite(base, "home")`, de forma que un eje sin fuente falla al importar
# en lugar de publicar una columna vacía.
PRESENTACION = (
    "elo_z",
    "form_points_ewm", "form_xpts_ewm", "form_npxg_ewm", "form_npxga_ewm",
    "out_value_pct",
    "rest_days",
)


def _lados_forma() -> dict[str, tuple[str, ...]]:
    m: dict[str, tuple[str, ...]] = {}
    for a in FORMA_ATRIBUTOS:
        m[f"form_{a}_ewm"] = DIFERENCIA
    for a in FORMA_ACUMULADOS:
        m[f"cum_{a}_pg"] = DIFERENCIA
    for a in FORMA_ESCENARIO:
        m[f"venue_form_{a}_{FORMA_ESCENARIO_VENTANA}"] = DIFERENCIA
    m["matches_prev"] = DIFERENCIA
    m["effectiveness"] = DIFERENCIA
    m["elo_mom_5_z"] = DIFERENCIA
    m["playstyle"] = PAR          # categórica: la resta no significa nada
    return m


def _lados_resto() -> dict[str, tuple[str, ...]]:
    m: dict[str, tuple[str, ...]] = {}
    for a in H2H_ATRIBUTOS:
        m[a] = DIFERENCIA
    for a in H2H_PARTIDO:
        m[a] = PARTIDO
    for a in CLASIFICACION_ATRIBUTOS + TRASCENDENCIA_ATRIBUTOS:
        m[a] = DIFERENCIA
    m[CLASIFICACION_ZONA] = PAR
    m[TRASCENDENCIA_CALENDARIO] = ("home",)   # el rival tiene el mismo calendario
    for a in FUERZA_ATRIBUTOS:
        m[a] = DIFERENCIA
    for a in PSICOLOGICAS_DIFERENCIA:
        m[a] = DIFERENCIA
    for a in PSICOLOGICAS_PAR:
        m[a] = PAR
    for a in BAJAS_ATRIBUTOS + FATIGA_ATRIBUTOS + ROTACION_ATRIBUTOS:
        m[a] = DIFERENCIA
    for a in PLANTILLA_PAR:
        m[a] = PAR
    for a in PLANTILLA_DIFERENCIA:
        m[a] = DIFERENCIA
    m[ELO_ATRIBUTO] = DIFERENCIA
    return m


def _con_presentacion(m: dict[str, tuple[str, ...]]) -> dict[str, tuple[str, ...]]:
    """Añade el par de presentación a los atributos declarados en `PRESENTACION`.

    Se aplica como un paso aparte y no editando las declaraciones de arriba porque
    aquellas recorren listas de atributos: escribir el caso a mano dentro del bucle
    obligaría a sacar siete atributos de sus listas y perdería de vista que su
    decisión de modelado sigue siendo DIFERENCIA. Aquí la excepción se lee entera y
    en un sitio.
    """
    for a in PRESENTACION:
        if a not in m:
            raise KeyError(
                f"catalogo: PRESENTACION declara {a!r}, que no es un atributo del "
                f"catálogo. Un eje de la aplicación no puede inventar una magnitud.")
        if m[a] != DIFERENCIA:
            raise ValueError(
                f"catalogo: PRESENTACION solo añade el par a lo que viaja como "
                f"DIFERENCIA, y {a!r} viaja como {m[a]}. Si ya sale por lados, "
                f"sobra en la lista.")
        m[a] = PAR_Y_DIFERENCIA
    return m


LADOS: dict[str, tuple[str, ...]] = _con_presentacion(
    {**_lados_forma(), **_lados_resto()})


def lados(base: str) -> tuple[str, ...]:
    """Los lados en que se emite el atributo de equipo `base`.

    Un atributo que no figura en el catálogo devuelve la tupla vacía, y eso es
    justo lo que debe pasar: los generadores llaman a esta función para decidir
    qué escribir, de modo que lo no declarado no se construye. La alternativa
    —un valor por defecto permisivo— haría que cualquier atributo añadido a un
    módulo entrase al conjunto sin que nadie lo hubiera decidido, que es
    exactamente cómo se llega a cuatrocientas columnas.
    """
    return LADOS.get(base, ())


def emite(base: str, lado: str) -> bool:
    """Si el atributo `base` se emite en el lado indicado ('home'/'away'/'diff')."""
    return lado in lados(base)


def nombres(base: str, prefijo: str = "") -> list[str]:
    """Los nombres finales de columna que produce el atributo `base`.

    `prefijo` cubre el caso del h2h, cuyas columnas se rotulan `home_h2h_x` y
    `h2h_x_diff` en lugar de `home_x` y `x_diff`.
    """
    out = []
    for lado in lados(base):
        if lado == "diff":
            out.append(f"{prefijo}{base}_diff")
        elif lado == "partido":
            out.append(f"{prefijo}{base}")
        else:
            out.append(f"{lado}_{prefijo}{base}")
    return out
