"""Orquestador de MODELADO (Fase 5): features_matches -> artefactos de evaluación.

Es el equivalente ejecutable del notebook 05, y la división del trabajo entre los
dos es estricta: el cuaderno y `seleccion.py` BUSCAN y el script CIERRA. Allí se
prueban conjuntos de variables —a mano en la Parte C, y de forma sistemática con la
búsqueda hacia delante de C.8–C.10— y se descarta lo que no mejora; aquí no se
prueba nada. Este módulo recibe un conjunto ya elegido, lo mide con el protocolo
completo, lo explica y escribe los artefactos que consumen el control de calidad y
la aplicación.

Que la búsqueda no viva aquí es una decisión de método, no de comodidad. Un
orquestador que eligiera variables mientras publica cifras haría imposible saber
cuántas combinaciones se miraron antes de quedarse con la que se cuenta, y ese
número es justamente lo que gradúa la confianza que merece el resultado. El
registro del banco (`outputs/models/laboratorio/registro.parquet`) guarda todos
los ensayos y la traza de la búsqueda queda en `outputs/models/seleccion/`; el
manifiesto de aquí solo declara cuál de ellos se cerró.

Qué hace, en orden
------------------
1. Lee el conjunto declarado —variables, familia e hiperparámetros— y comprueba
   que sus columnas existen en la matriz.
2. Recorre el bloque de validación con el protocolo walk-forward por jornada.
3. Mide el pronóstico contra las tres lecturas del precio —consenso, Pinnacle y
   Bet365— sobre exactamente los mismos partidos, y contra la previa más favorable
   que puede construirse sin modelo.
4. Explica el modelo por las tres vías y guarda los tres relatos.
5. Simula el rendimiento económico de una unidad plana al signo que el modelo
   señala, en todos los partidos con precio, y lo enfrenta al apostante que sigue
   siempre al favorito de Bet365, temporada a temporada y jornada a jornada.
6. Escribe los artefactos y un manifiesto que los sella.

Contra quién se mide el dinero
------------------------------
El rendimiento del modelo se publica siempre junto al de una referencia que ignora
el modelo y respalda la más baja de las tres cuotas de Bet365, cobrada al mismo
precio que el modelo. El listón no es el cero: en un mercado con margen, perder
poco donde el mercado pierde mucho es un resultado, y ganar poco donde el mercado
gana más no lo es. La diferencia entre ambos rendimientos lleva su intervalo por
remuestreo de jornadas completas, y el remuestreo es pareado —cada réplica vuelve
a simular las dos estrategias sobre los mismos partidos—, de modo que el ruido
común a las dos se cancela en lugar de sumarse.

La única guarda que puede impedir la publicación
------------------------------------------------
No hay elección entre candidatos que arbitrar: el conjunto viene decidido. Lo que
sí se comprueba, y antes de mirar ninguna cifra, es que el modelo aporte algo: su
RPS tiene que quedar por debajo del de una previa que reparte siempre la
frecuencia observada de las tres clases **en los propios partidos evaluados**. Es
la previa más favorable posible —conoce el reparto que un pronosticador real
tendría que estimar— y por eso batirla es una condición mínima y no un adorno: un
modelo que no la bate no ha aprendido nada del partido, solo del campeonato.

El régimen
----------
Todo el recorrido se ejecuta bajo los dos regímenes de información. El `semanal`
es el que se publica; el `retrospectivo` no se publica y solo existe para medir,
con el mismo protocolo, cuánta información añade el mercado entre la apertura y el
cierre. Ejecutarlos con el mismo código es la condición para que la diferencia se
lea como una magnitud del dominio.

El holdout
----------
La temporada sellada se puntúa **exactamente una vez**, con el conjunto ya cerrado.
La orden que lo hace es distinta de la del recorrido normal y deja un sello en
disco: una segunda ejecución se niega a repetirla. El sello no es una comodidad de
implementación sino la prueba material de que la cifra del holdout se emitió una
sola vez, y `qa_models.py` lo verifica.

Uso
---
    python scripts/models/build_models.py                # validación, ambos regímenes
    python scripts/models/build_models.py --dry          # ejecuta y audita, NO escribe
    python scripts/models/build_models.py --regimen semanal
    python scripts/models/build_models.py --holdout      # la pasada única y final
"""
from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
_EXTRACTORS = _HERE.parent / "extractors"
for _p in (_HERE, _FEATURES, _EXTRACTORS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from _common import write_df_required  # noqa: E402
import backtest as bt  # noqa: E402
import columns as C  # noqa: E402
import evaluate as ev  # noqa: E402
import explain as ex  # noqa: E402
import laboratorio as lb  # noqa: E402
import preprocess as pp  # noqa: E402
import protocol as pt  # noqa: E402
import seleccion as sl  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
FEATURES = PROC / "features_matches.parquet"
OUT = _ROOT / "outputs" / "models"


# ===========================================================================
# 1. El conjunto declarado
# ===========================================================================
# Las seis constantes que siguen son el contrato entre el cuaderno y el resto del
# proyecto. El cuaderno 05 las imprime en su sección E.2 y comprueba que coinciden
# con lo que allí se ha elegido; si no coinciden, uno de los dos se ha quedado
# atrás y la comprobación lo dice en voz alta.
#
# CONJUNTO es una tupla y no una lista a propósito: lo que aquí se escribe está
# cerrado, y una estructura que no se puede modificar al vuelo es la forma más
# barata de que no se modifique al vuelo.
#
# Dejarla vacía NO significa renunciar a declarar: significa delegar en el artefacto
# que la búsqueda hacia delante escribe en `outputs/models/seleccion/conjunto.json`,
# del que este módulo la recoge unas líneas más abajo. Copiar aquí a mano la lista
# que salió de la búsqueda crearía una segunda fuente de verdad, y las dos fuentes
# se separan el día en que se repite la búsqueda y nadie se acuerda de actualizar la
# copia. Escribirla explícitamente sigue siendo legítimo —y tiene prioridad sobre el
# artefacto— para fijar un conjunto que no venga de la búsqueda.
#
# Y eso es exactamente lo que ocurre aquí. El modelo publicado NO sale de la búsqueda
# voraz: sale de la lectura de por qué la búsqueda no encontró nada. `conjunto.json`
# sigue guardando su respuesta —las tres columnas de precio— y es la respuesta correcta
# a la pregunta que la búsqueda hacía, que era «qué variables añadir a la semilla». La
# mezcla hace otra cosa: coge el banco ENTERO y no lo concatena, lo reparte en dos
# mitades que se ajustan por separado y se combinan con un peso. Un conjunto así no
# tiene «pasos», ni «motivo de parada», ni variables descartadas, y escribirlo en
# `conjunto.json` inventaría esos campos. Va escrito aquí, que es la vía que el propio
# módulo reserva para un conjunto que no viene de la búsqueda.
#
# Son las 62 columnas que midió el estudio: las tres de precio de `sl.SEMILLA_MERCADO`
# más las 59 de juego, que es `sl.candidatas(feat, "semanal")` —65— menos el dominio
# `mercado` —6—. La lista está GENERADA, no escogida a mano, y queda escrita como
# literal a propósito: el modelo publicado tiene que ser reproducible aunque el
# catálogo cambie mañana, y si cambia, `conjunto_efectivo` aborta nombrando la columna
# que falta en lugar de publicar un modelo distinto con el mismo nombre. Ninguna de
# las 59 lleva el prefijo `mkt_p_`, que es lo que hace que la familia pueda partirlas
# por el nombre; `_mezcla_control.py` lo comprueba antes de cada publicación.
#
# Son 62 PEDIDAS y el modelo ajusta 61, que es por lo que las tablas de
# interpretabilidad suman 61. Las tres últimas —las probabilidades del modelo de
# goles— suman uno, de modo que la poda por colinealidad del preprocesado retira una
# de las tres en cada pliegue: medido sobre los 255 pliegues de los dos regímenes,
# `strength_p_home` en 208 y `strength_p_away` en los 47 restantes. Se declaran las
# tres a propósito, porque quitar la que cae más veces dejaría 60 columnas en los 47
# pliegues donde la poda elige otra: la elección es del pliegue y no del conjunto.
CONJUNTO: tuple[str, ...] = (
    "mkt_p_home", "mkt_p_draw", "mkt_p_away", "elo_z_diff",
    "home_squad_value", "away_squad_value", "age_diff",
    "fat_rest_days_diff", "fat_games_last_14_diff",
    "fat_euro_trip_recent_diff", "n_missing_diff", "out_minshare_diff",
    "out_minshare_max_diff", "out_value_pct_diff", "xi_continuity_diff",
    "starters_minutes_14_diff", "minutes_hhi_diff", "home_playstyle",
    "away_playstyle", "matches_prev_diff", "cum_points_pg_diff",
    "cum_xgd_pg_diff", "form_points_ewm_diff", "form_gf_ewm_diff",
    "form_ga_ewm_diff", "form_npxg_ewm_diff", "form_npxga_ewm_diff",
    "form_xpts_ewm_diff", "form_sot_ewm_diff",
    "form_big_chances_ewm_diff", "form_ppda_ewm_diff",
    "form_deep_ewm_diff", "form_shot_xg_mean_ewm_diff",
    "form_shot_xg_mean_against_ewm_diff", "form_rating_ewm_diff",
    "form_opp_elo_z_ewm_diff", "venue_form_points_5_diff",
    "venue_form_gd_5_diff", "effectiveness_diff", "elo_mom_5_z_diff",
    "home_post_big_win", "away_post_big_win", "relative_perf_diff",
    "h2h_win_rate_ewm_diff", "h2h_draw_rate_ewm", "h2h_gd_ewm_diff",
    "home_matches_left", "home_position_zone", "away_position_zone",
    "prev_position_diff", "points_above_drop_diff", "safe_from_drop_diff",
    "stakes_none_match", "is_weekend", "away_travel_km", "is_derby",
    "kickoff_slot", "strength_attack_diff", "strength_defence_diff",
    "strength_p_home", "strength_p_draw", "strength_p_away",
)

# La familia y su configuración, en los nombres que entiende `laboratorio`. Los
# hiperparámetros son los que salgan de `lb.afina()` sobre el conjunto ya cerrado;
# mientras no se afine, los declarados en `lb.HIPER` para esa familia.
#
# `mezcla` y no `logistica` porque es la única de las once rutas que el proyecto
# recorrió cuyo resultado NO se invierte al salir del bloque que lo eligió. Lo que la
# distingue es que tiene un solo parámetro —el peso— frente a los 2^65 conjuntos entre
# los que elegía la voraz, así que el optimismo de selección es casi nulo por
# construcción. Su precio está declarado y es real: el RPS empeora 0.000527, y adoptarla
# exigía levantar la regla 4 del proyecto. La levantó Manuel el 19-oct-2026 —«quiero que
# el ROI mande, no el RPS»—; no es una decisión tomada en el código.
#
# `HIPER` se deja vacío para que rijan los de `lb.HIPER["mezcla"]` —w=0.85, C=0.3, L2—,
# que son los que midió el estudio. Copiarlos aquí crearía una segunda fuente de verdad
# para el único parámetro que el modelo publicado elige.
FAMILIA: str = "mezcla"
HIPER: dict = {}

# El protocolo con el que se midió el conjunto. Viaja aquí y no solo en el
# manifiesto porque el cuaderno lo lee para comprobar que la cifra que enseña y la
# que se publica proceden del mismo esquema de validación: dos configuraciones de
# protocolo distintas dan dos números que no son comparables aunque lo parezcan.
#
# El arranque se baja a UNA temporada —`seleccion.WARMUP_CONFIRMACION`— y el corte se
# queda donde estaba. Lo pidió Manuel el 20-oct-2026 («todos esos partidos son
# necesarios de evaluar», de 17/18 a 26/27 sin excepción) y lo confirmó el 21-oct
# sabiendo lo que cuesta. `pt.WARMUP_SEASONS` sigue declarando tres y eso NO es una
# incoherencia: son dos preguntas distintas. El arranque declarado es el que midió el
# estudio de potencia de la búsqueda —donde agrandar el bloque de selección empeoraba
# la discriminación de 1.67 a 0.73 errores típicos, l. 228— y aquí no se busca nada:
# el conjunto y el peso ya están cerrados, así que bajar el arranque no mete partidos
# en ninguna elección, solo saca de la sombra 760 que ya se podían puntuar.
#
# Por eso estas dos temporadas son la mejor lectura del trabajo y no la peor. No
# participaron en elegir el conjunto, ni la familia, ni el peso de la mezcla, así que
# son `confirmacion` y sus cifras SÍ son citables (regla 5). Publicar las siete
# temporadas después de haber visto que el nivel baja sería elegir el censo por su
# resultado, que es exactamente lo que el corte de selección y el sello del holdout
# existen para impedir un escalón más arriba.
#
# **Lo que cuesta, medido el 21-oct-2026, y la parte en la que me equivoqué.** El nivel
# absoluto baja: acierto 0.538158 -> 0.534211 y RPS 0.192418 -> 0.194494 al pasar de
# 2280 a 3040 partidos puntuados, porque 2018/19 fue dura para todo el mundo —el
# favorito de Bet365 acertó el 48.16 % y perdió el 13.03 % a cuota media—. Anuncié que
# la ventaja sobre el listón subiría a unos +1.52 pp citando la ruta rodante, y NO es lo
# que pasa:
#   · a `cierre_avg` la cifra publicada no se mueve ni un decimal —+0.84 %, las mismas
#     2280 apuestas— y no puede moverse, porque football-data NO PUBLICA CUOTA DE CIERRE
#     antes de 2019/20: cobertura medida 0 % en 16/17-18/19 y 100 % desde 19/20. Las dos
#     temporadas nuevas no son liquidables en ese eje; no hay máscara que corregir,
#     falta el precio en la fuente.
#   · a `consenso` —cuota media de casas, cobertura 100 % hasta 16/17— sí entran, y la
#     ventaja pasa de +1.00 % a +1.39 % [-0.43 %, +3.33 %] sobre 3040 apuestas: sube, en
#     la dirección que anuncié y no a la cifra que anuncié, y sigue sin excluir el cero.
#     Es el único eje en el que la pregunta del dinero se puede hacer sobre las nueve
#     temporadas, así que es el eje citable para el universo completo.
# Y el RPS se da la vuelta: el modelo pasa de batir a consenso y a b365 a quedar DETRÁS
# DE LOS TRES (0.194494 contra pinnacle 0.193958, consenso 0.194116, b365 0.194237;
# `skill_rps` -0.001947 contra consenso). El daño está localizado en las dos temporadas
# que entran —pierde 0.00214 en 17/18 y 0.00232 en 18/19 contra consenso— y en ellas
# GANA EN ACIERTO (55.53 % contra 54.74 %, y 48.95 % contra 48.68 %). Esa es la firma
# exacta que `_estabilidad.py` había predicho de una búsqueda guiada por acierto:
# compra daño en RPS sin anunciarlo. La regla 4 dice que el ROI y el acierto mandan y
# que el RPS se reporta igual; esto es reportarlo.
PROTOCOLO = pt.ProtocolConfig(warmup_seasons=list(sl.WARMUP_CONFIRMACION))

# De dónde salió el conjunto. Cuando lo recoge la búsqueda, es el nombre del ensayo del
# registro del banco, y entonces `lb.conjunto(PROCEDENCIA)` devuelve las mismas
# variables. Aquí el conjunto no viene de la búsqueda, así que la procedencia nombra el
# estudio que lo decidió y el control que comprueba que la familia es ese estudio: la
# trazabilidad hacia atrás es `_mezcla.py` para la elección del peso y
# `_mezcla_control.py` para la identidad del modelo.
PROCEDENCIA: str = "C.9.4 · mezcla logit w=0.85 sobre las 62 (_mezcla.py)"


def de_la_seleccion(*, verbose: bool = True,
                    ruta: Path | None = None) -> tuple[tuple[str, ...], str]:
    """El conjunto que dejó la búsqueda hacia delante, si es utilizable aquí.

    Devuelve `((), "")` —y no levanta— cuando no hay ninguna búsqueda guardada,
    porque ejecutar la fase antes de haber buscado es un estado legítimo del
    trabajo: para eso está el conjunto provisional.

    Sí se desentiende, con aviso, cuando la hay pero se eligió con otro corte o con
    otra familia. Un conjunto elegido bajo un reparto de temporadas distinto no es
    incorrecto: es la respuesta a otra pregunta. Publicarlo bajo este manifiesto
    afirmaría del modelo algo que no se ha medido, y el desajuste no se vería en
    ninguna cifra porque todas saldrían con su aspecto normal.

    La procedencia que devuelve es el nombre con el que `seleccion.mide()` registra
    la medición en el libro del banco. Así `lb.conjunto(PROCEDENCIA)` sigue
    devolviendo las mismas variables, que es la trazabilidad hacia atrás que la
    sección E.2 del cuaderno comprueba.
    """
    try:
        guardado = sl.carga(ruta)
    except FileNotFoundError:
        return (), ""

    corte = str(guardado.get("corte_seleccion", ""))
    familia = str(guardado.get("familia", ""))
    if corte != str(PROTOCOLO.corte_seleccion) or familia != FAMILIA:
        if verbose:
            print(f"AVISO · hay una selección guardada, pero se hizo con corte "
                  f"{corte!r} y familia {familia!r}, y aquí rigen "
                  f"{str(PROTOCOLO.corte_seleccion)!r} y {FAMILIA!r}. Se ignora: "
                  f"repita la búsqueda o alinee la declaración antes de publicar.")
        return (), ""

    cols = tuple(dict.fromkeys(str(c) for c in guardado.get("conjunto", ())))
    if not cols:
        return (), ""
    return cols, f"medicion·{familia}·{len(cols)}v"


# La recogida, una sola vez al importar. Va aquí y no dentro de `conjunto_efectivo`
# para que `CONJUNTO` y `PROCEDENCIA` —que son el contrato que el cuaderno lee y el
# manifiesto sella— digan lo mismo mire quien las mire, y para que `conjunto_cerrado`
# siga siendo exactamente `bool(CONJUNTO)` sin excepciones que explicar.
if not CONJUNTO:
    CONJUNTO, _PROCEDENCIA_SEL = de_la_seleccion(verbose=False)
    if CONJUNTO:
        PROCEDENCIA = _PROCEDENCIA_SEL

# Mientras el conjunto no esté escrito arriba, el recorrido puede ejecutarse con un
# conjunto PROVISIONAL para probar la mecánica de la fase, y todo lo que produzca
# queda marcado como tal: el manifiesto lleva `conjunto_cerrado: false` y el
# control de calidad lo rechaza para publicación. Es preferible a no poder ejecutar
# nada hasta el final, y preferible también a un valor por defecto silencioso que
# acabara publicándose sin que nadie lo hubiera elegido.
DOMINIOS_PROVISIONALES = ("mercado", "fuerza", "forma")


def conjunto_cerrado() -> bool:
    """Si el conjunto publicable está escrito en el módulo."""
    return bool(CONJUNTO)


def conjunto_efectivo(feat: pd.DataFrame, *, verbose: bool = True) -> list[str]:
    """Las variables con las que se ejecuta, y el aviso si no son las definitivas.

    Comprueba además que las columnas declaradas existen en la matriz. Una columna
    que se renombra aguas arriba desaparecería aquí sin ruido —`ModeloConColumnas`
    tolera las ausencias por diseño, porque la poda por colinealidad las produce
    dentro de cada pliegue— y el modelo publicado leería una variable menos de las
    que la memoria dice. La tolerancia es correcta dentro del pliegue y no lo es en
    el contrato: aquí se exige que todas estén.
    """
    if conjunto_cerrado():
        faltan = [c for c in CONJUNTO if c not in feat.columns]
        if faltan:
            raise KeyError(
                f"el conjunto declarado nombra {len(faltan)} columnas que no están "
                f"en {FEATURES.name}: {faltan}. O el conjunto se quedó atrás o la "
                f"matriz cambió de rótulos; en ambos casos hay que volver al "
                f"cuaderno antes de publicar nada")
        return list(CONJUNTO)

    cols = lb.variables(*DOMINIOS_PROVISIONALES,
                        columnas=lb.columnas_disponibles(feat))
    if verbose:
        print("AVISO · el conjunto no está cerrado. Se ejecuta con las variables de "
              f"los dominios {list(DOMINIOS_PROVISIONALES)} ({len(cols)} columnas) "
              "y todo lo que salga queda marcado como provisional.")
    return cols


def hiper_efectivo() -> dict:
    """La configuración de la familia: la declarada aquí sobre la del banco."""
    params = dict(lb.HIPER[FAMILIA])
    params.update(HIPER)
    return params


def fabrica_declarada(feat: pd.DataFrame, *, verbose: bool = True):
    """La fábrica del modelo publicado, tal y como el banco lo construye.

    Se pide a `laboratorio` y no se rehace aquí para que el objeto que se publica
    sea idéntico —la misma envoltura, el mismo orden de columnas, la misma
    tolerancia a las ausencias— al que produjo las cifras del cuaderno. Dos
    construcciones equivalentes en apariencia bastarían para que la cifra publicada
    no fuese exactamente la que se midió, y la diferencia no se vería en ningún
    sitio.
    """
    return lb.fabrica(FAMILIA, conjunto_efectivo(feat, verbose=verbose),
                      hiper_efectivo())


METRICA_ELECCION = "rps"

# El esfuerzo de remuestreo con el que se acota la ventaja económica sobre el
# apostante de referencia. Cuatrocientas réplicas dan un intervalo estable al
# segundo decimal del porcentaje sin que el bloque económico domine el tiempo de la
# ejecución. Los intervalos por temporada se calculan solo para el precio de
# cierre —el que la memoria reporta— porque repetirlos para la apertura
# multiplicaría ese coste sin añadir una lectura distinta: la diferencia entre los
# dos momentos ya se ve en la cifra agregada.
N_BOOT_ECONOMIA = 400
SEMILLA_ECONOMIA = 7

# Con qué política se liquida. `favorito` apuesta una unidad plana al signo que el
# modelo señala, en todos los partidos con precio, y no tiene ningún parámetro que
# elegir; `valor` apuesta solo donde la probabilidad del modelo supera a la
# implícita en la cuota por un margen, y ese margen es una decisión tomada sobre
# datos.
#
# Se publica `favorito` por dos razones, y la primera es la medida. `_politica.py`
# enfrenta las dos sobre el modelo publicado, a cuota de cierre promediada, en el
# bloque de selección y en los partidos de fuera, cada cifra emparejada con el
# listón de Bet365 (regla 3):
#
#     bloque        politica  umbral  apuestas      ROI  ROI b365   ventaja
#     seleccion     favorito    0.00      1140   -0.35%    -2.18%    +1.83%
#     seleccion     valor       0.02       659   -7.93%    -2.18%    -5.75%
#     FUERA (2+3)   favorito    0.00      1140   +1.93%    +2.09%    -0.16%
#     FUERA (2+3)   valor       0.02       576   +0.25%    +2.09%    -1.84%
#
# `favorito` gana en las dos ventanas, por 5.75 y 1.84 puntos, y con umbral 0.00 y
# 0.06 el orden es el mismo, así que la lectura no depende de haber acertado con el
# margen. Lo que sostiene la elección es esa estabilidad del orden al cambiar de
# ventana, NO la significación: la diferencia entre las dos políticas no se ha
# acotado, y las dos ventajas sobre el listón que sí llevan intervalo lo cruzan. El
# umbral cuesta además la mitad de las apuestas —576 de 1140—, y eso se ve en la
# anchura: [-15.67, +11.55] frente a [-2.78, +2.28].
#
# La segunda razón es que `favorito` es el ROI con el que se midió y se adoptó la
# mezcla. Publicar `valor` afirmaría del modelo una rentabilidad que no es la que
# justificó adoptarlo.
#
# Lo que esta cifra NO dice: fuera del bloque que eligió el peso, la ventaja sobre
# el listón es -0.16 % con intervalo [-2.78, +2.28], compatible con cero. El modelo
# rinde como el apostante de referencia, no por encima. Y las 1140 apuestas de los
# 1900 partidos de fuera son las que tienen precio de cierre promediado: las 760 de
# confirmación no lo tienen, de modo que la cifra económica de fuera descansa sobre
# el bloque de medición.
POLITICA_ECONOMIA = "favorito"

# Cuántos pliegues finales se reajustan para explicar el modelo. Los últimos y no
# todo el recorrido, porque lo que la memoria describe es el modelo tal y como
# queda al cerrar el trabajo; promediar repartos calculados con modelos de 2019 y
# de 2025 mezclaría dos objetos distintos.
N_PLIEGUES_EXPLICACION = 8

# Las limitaciones que acompañan a toda cifra publicada. Van en el manifiesto y de
# ahí a la ficha del modelo, porque una limitación que solo vive en la memoria no
# llega al lector de la aplicación, que es quien puede malinterpretar el número.
LIMITACIONES = [
    "El modelo publicado no elige variables: usa el banco entero, repartido en dos "
    "mitades —el precio del mercado y el juego— que se ajustan por separado y se "
    "combinan en escala logit con un solo peso. Lo que se eligió midiendo es ese "
    "peso, un único número entre veintiuno de una rejilla.",
    "El peso se eligió sobre 2019/20, 2020/21 y 2021/22, de modo que la cifra "
    "agregada que se publica —que cubre de 2017/18 a 2024/25 más la temporada en "
    "curso— incluye los 1140 partidos que participaron en elegirlo y es por tanto "
    "optimista. Las cifras citables son las de las seis temporadas que no "
    "eligieron nada: 2017/18 y 2018/19, anteriores a cualquier elección, y "
    "2022/23, 2023/24 y 2024/25, posteriores, más la temporada en curso. El "
    "desglose por temporada las publica fila a fila. Agregando solo esas seis, la "
    "ventaja sobre el listón es +1.18 pp con intervalo [-0.84, +3.33], que no "
    "excluye el cero. La estimación final sigue siendo la del holdout.",
    "La búsqueda de subconjuntos de variables que precedió a esta decisión "
    "—voraz hacia delante, con los hiperparámetros fijos— encuentra un óptimo "
    "local y no el mejor conjunto posible, y no encontró ningún conjunto que "
    "mejorase al precio fuera del bloque donde se eligió. Que no lo encontrara no "
    "demuestra que no exista.",
    "El retorno económico es la métrica más ruidosa del trabajo: su intervalo es "
    "ancho y una diferencia de signo entre dos políticas puede no estar medida.",
    "El modelo se adoptó por su rentabilidad y no por la calidad de su "
    "probabilidad: su RPS es algo peor que el del modelo de solo precio al que "
    "sustituye. Las dos cifras se publican juntas, y la elección entre ellas es "
    "una decisión declarada del trabajo, no un resultado de la medición.",
    "En RPS y en log-loss el modelo queda por detrás de los tres precios: su RPS "
    "es 0.194494 en el bloque de validación, contra 0.193958 de Pinnacle, "
    "0.194116 del consenso y 0.194237 de Bet365. Lo que gana es el acierto, y a "
    "los tres (53.38 % contra 53.11 %, 53.08 % y 53.01 %). La tabla comparativa "
    "coloca al candidato en la primera fila porque ordena antes los candidatos "
    "que las referencias, no porque gane: no es un ranking. El daño en RPS está "
    "localizado en 2017/18 y 2018/19, las dos temporadas de precio más pobre de "
    "la serie, y en las dos el modelo gana en acierto.",
    "El rendimiento histórico se calcula contra cuotas de cierre, mientras que una "
    "apuesta emitida antes de la jornada iría contra el precio de apertura; se "
    "publican las dos lecturas y la diferencia entre ellas es parte del resultado.",
    "La simulación supone que la apuesta se coloca al precio publicado y por el "
    "importe calculado, sin límite de la casa ni movimiento del mercado.",
    "El bloque de arranque es 2016/17 sola: entra en el entrenamiento y no se "
    "evalúa. 2017/18 y 2018/19 sí se evalúan, y carecen de precio de cierre y de "
    "parte médico, así que solo se liquidan a consenso y sus indicadores de "
    "faltante están encendidos en todos sus partidos.",
    "La cifra del holdout procede de una única pasada y no admite reajuste: "
    "cualquier decisión de diseño tomada después de verla la invalidaría.",
    "La ventana de entrenamiento es siempre expansiva y la semivida del peso de "
    "recencia está declarada en el protocolo, no elegida dentro de él.",
    "El modelo no observa alineaciones confirmadas: trabaja con la información "
    "disponible antes del anuncio oficial, que es la situación de uso real.",
]


# ===========================================================================
# 2. Identidad de la ejecución
# ===========================================================================
def _huella(feat: pd.DataFrame, cfg: pt.ProtocolConfig, regimen: str, *,
            columnas=None) -> str:
    """Código determinista de la ejecución: mismas entradas, mismo código.

    Depende del contenido de la matriz —número de filas y columnas y la última
    fecha—, de la configuración del protocolo, del régimen y de las tres decisiones
    que definen el modelo publicado: qué variables, qué familia y con qué valores.
    Estas tres entran porque cambiarlas cambia el modelo sin cambiar ni el dato ni
    el protocolo, y un código que no las distinguiera haría pasar por la misma dos
    ejecuciones que no lo son. Sirve para que la aplicación pueda decir con qué
    ejecución se emitió una predicción y para detectar que dos artefactos
    publicados no proceden de la misma pasada, que es la avería silenciosa que
    produce una web incoherente.
    """
    cols = tuple(sorted(columnas if columnas is not None else CONJUNTO))
    firma = "|".join([
        str(feat.shape[0]), str(feat.shape[1]),
        str(pd.to_datetime(feat[C.DATE]).max().date()),
        cfg.describe() if hasattr(cfg, "describe") else repr(cfg),
        regimen, FAMILIA, json.dumps(hiper_efectivo(), sort_keys=True, default=str),
        "|".join(cols),
    ])
    return hashlib.sha256(firma.encode("utf-8")).hexdigest()[:12]


# ===========================================================================
# 3. La previa que hay que batir
# ===========================================================================
def previa_favorable(pred: pd.DataFrame) -> dict:
    """La previa más favorable sin modelo: la frecuencia observada de las clases.

    Reparte en todos los partidos la proporción de locales, empates y visitantes
    que de verdad hubo en los partidos evaluados. Es una referencia deliberadamente
    generosa —un pronosticador real tendría que estimar ese reparto y aquí se le
    regala—, y esa generosidad es su valor: si el modelo la bate, la mejora no
    puede atribuirse a haber acertado el reparto global del campeonato.
    """
    y = ev._objetivo(pred)
    m = np.isfinite(y) & ev.evaluable(pred)
    if not m.any():
        raise ValueError("no hay filas evaluables con las que construir la previa")
    y = y[m].astype(int)
    reparto = np.bincount(y, minlength=3) / len(y)
    P = np.tile(reparto, (len(y), 1))
    return {"rps": float(np.mean(ev.rps_vector(y, P))),
            "logloss": float(np.mean(ev.logloss_vector(y, P))),
            "brier": float(np.mean(ev.brier_vector(y, P))),
            "accuracy": float(np.mean(ev.hit_vector(y, P))),
            "n": int(len(y)),
            "reparto": {c: float(p) for c, p in zip(ev.CLASES, reparto)}}


def comprueba_piso(tab: pd.DataFrame, previa: dict, *,
                   verbose: bool = True) -> dict:
    """Aplica la única guarda declarada y devuelve el veredicto por escrito.

    La comprobación se escribe aquí, en el código y antes de ejecutar, y no en la
    lectura posterior de la tabla, porque una condición que se evalúa a ojo después
    de ver el resultado no es una condición.
    """
    fila = tab[tab.tipo == "candidato"].iloc[0]
    cifra = float(fila[METRICA_ELECCION])
    piso = float(previa[METRICA_ELECCION])
    if cifra >= piso:
        raise ValueError(
            f"el conjunto declarado no bate a la previa de frecuencias "
            f"({METRICA_ELECCION}={cifra:.4f} frente a {piso:.4f}); no hay nada "
            f"que publicar. El sitio para volver a intentarlo es el cuaderno 05, "
            f"no este script")
    res = {"modelo": FAMILIA,
           METRICA_ELECCION: cifra,
           "ic": (float(fila["rps_ic_bajo"]), float(fila["rps_ic_alto"])),
           "piso_previa": piso,
           "previa": previa,
           "motivo": (f"{METRICA_ELECCION}={cifra:.4f} en el bloque de validación, "
                      f"por debajo de la previa de frecuencias ({piso:.4f})")}
    if verbose:
        print(f"\nmodelo publicado: «{FAMILIA}» sobre {len(CONJUNTO) or 'las '} "
              f"variables declaradas  —  {res['motivo']}")
    return res


# ===========================================================================
# 4. La tabla de métricas
# ===========================================================================
def tabla_comparativa(pred: pd.DataFrame, *, n_boot: int = 400,
                      seed: int = 7) -> pd.DataFrame:
    """El modelo y sus referencias de mercado, con el intervalo del RPS.

    El modelo y las tres lecturas del precio comparten tabla, y la columna `tipo`
    los distingue. Que el mercado aparezca como una fila más y no como una columna
    aparte no es cosmético: `evaluate.compare` restringe todas las métricas a los
    partidos que todas las referencias cubren, de modo que las filas del modelo y
    las de su mercado están medidas sobre exactamente el mismo conjunto de
    encuentros. Comparar un modelo evaluado sobre todos los partidos con un mercado
    evaluado solo sobre los que tienen precio produciría una ventaja que únicamente
    mide la cobertura, que es el error más fácil de cometer en este cuadro.

    El precio entra por sus tres lecturas —consenso, Pinnacle y Bet365— y no por
    una elegida a conveniencia. Cada una responde a una pregunta distinta: si el
    pronóstico bate a la media de las casas, si bate al dinero informado y si bate
    a la casa minorista que el lector reconoce. Publicarlas juntas es lo que impide
    quedarse con la más favorable después de ver el resultado, y su coste es la
    merma de muestra que la restricción de fila común impone y que la propia tabla
    declara en `n`.

    El intervalo se calcula solo para el modelo, y por remuestreo de jornadas
    completas: los partidos de un mismo fin de semana comparten el estado del mundo
    de esa semana y remuestrearlos sueltos daría un intervalo más estrecho y falso.
    """
    comp = ev.compare(pred)
    boot = ev.bootstrap_metrics(pred, metricas=(METRICA_ELECCION,),
                                n_boot=n_boot, seed=seed)
    filas = []
    for contendiente, r in comp.iterrows():
        fila = {"modelo": FAMILIA, "contendiente": contendiente,
                "tipo": "candidato" if contendiente == "modelo" else "referencia",
                **r.to_dict()}
        if contendiente == "modelo":
            fila["rps_ic_bajo"] = float(boot.loc[METRICA_ELECCION, "inf"])
            fila["rps_ic_alto"] = float(boot.loc[METRICA_ELECCION, "sup"])
            fila["rps_ee"] = float(boot.loc[METRICA_ELECCION, "ee"])
        filas.append(fila)
    tab = pd.DataFrame(filas)
    orden = tab.tipo.eq("referencia").astype(int)
    return (tab.assign(_o=orden)
               .sort_values(["_o", METRICA_ELECCION])
               .drop(columns="_o")
               .reset_index(drop=True))


def tabla_conjunto(columnas, feat: pd.DataFrame) -> pd.DataFrame:
    """Una fila por variable declarada, con el dominio explicativo al que pertenece.

    Es la evidencia de qué se publicó, en una forma que la aplicación puede leer
    sin conocer el catálogo. Sustituye a cualquier lista suelta en un documento: la
    lista de la memoria y esta tabla salen del mismo sitio.

    Lo que cuenta esta tabla es el conjunto PEDIDO, y son 62 filas. El modelo ajusta
    61, y por eso `importancia_bloques` y `registro_bloques` suman 61 y no 62: la
    poda por colinealidad del preprocesado (D3) retira en cada pliegue una de las
    tres probabilidades del modelo de goles, porque las tres suman uno. Medido sobre
    los 255 pliegues de los dos regímenes: cae `strength_p_home` en 208 y
    `strength_p_away` en los 47 restantes. **Por eso se declaran las 62 y no las 61**
    —quitar `strength_p_home` de `CONJUNTO` dejaría 60 columnas en esos 47 pliegues—,
    y por eso `presente` se mide contra la matriz de features y no contra la matriz
    ya preprocesada: dice si la variable existe para que el pliegue pueda elegir, no
    si sobrevivió a la poda de un pliegue concreto. Que falte de la matriz de
    features sí sería una avería, y es lo que esta columna vigila.
    """
    mapa = ex.mapa_bloques(list(columnas))
    de_quien = {c: b for b, cols in mapa.items() for c in cols}
    return pd.DataFrame({
        "orden": range(1, len(columnas) + 1),
        "variable": list(columnas),
        "bloque": [de_quien.get(c, "sin bloque") for c in columnas],
        "presente": [c in feat.columns for c in columnas],
    })


# ===========================================================================
# 5. Interpretabilidad del modelo publicado
# ===========================================================================
def explica_publicado(feat: pd.DataFrame, cfg: pt.ProtocolConfig, fabrica, *,
                      n_pliegues: int = N_PLIEGUES_EXPLICACION, folds=None,
                      cache=None, columnas=None, verbose: bool = True) -> dict:
    """Ajusta los últimos pliegues del modelo declarado y produce los tres relatos.

    La fábrica que entra es exactamente la que produjo las predicciones evaluadas.
    Explicar un objeto distinto del que se midió sería describir un modelo que
    nadie ha evaluado, y la diferencia no se vería en ninguna cifra.

    El mapa de bloques se restringe a las variables declaradas: romper por
    permutación bloques que el estimador ni siquiera mira produciría una tabla
    llena de ceros que se leería como «ese bloque no importa» cuando lo que dice es
    «ese bloque no está».
    """
    folds = folds if folds is not None else pt.make_folds(feat, cfg,
                                                          evaluar="validacion")
    ultimos = folds[-n_pliegues:] if n_pliegues else folds
    cache = cache if cache is not None else pt._StateCache(feat, cfg)
    ajustados = [pt.fit_fold(feat, fabrica, cfg, f, cache=cache) for f in ultimos]
    if verbose:
        print(f"interpretabilidad sobre {len(ajustados)} pliegues "
              f"({ultimos[0].season} j{ultimos[0].jornada} -> "
              f"{ultimos[-1].season} j{ultimos[-1].jornada})")
    return ex.explain_report(ajustados, metrica=METRICA_ELECCION, feat=feat,
                             columnas=columnas, verbose=verbose)


# ===========================================================================
# 6. Publicación
# ===========================================================================
# Nombre del bloque del que sale cada fila publicada —los de `protocol.BLOQUES`—.
# Viaja en los artefactos porque las poblaciones responden preguntas distintas y una
# tabla que las mezcle sin rótulo invita a citar la cifra equivocada. Se declara aquí,
# al principio de la publicación, porque todo lo que sigue lo usa: el reparto entre lo
# que se desglosa y lo que se agrega es la primera decisión de esta sección.
BLOQUE = "bloque"

# El rótulo distingue dos poblaciones, y hay una TERCERA distinción que el dato no
# lleva y que la ficha tiene que declarar para que la aplicación pueda rotularla.
#
# `validacion` es el bloque de desarrollo ENTERO, y eso es lo correcto para la cifra
# agregada: el modelo se mide sobre todo el desarrollo. Pero dentro de ese bloque hay
# tres temporadas que ELIGIERON el peso de la mezcla y tres que no, y la diferencia
# está medida y es de signo: la ventaja sobre el listón a cierre promediado es +3.20,
# −0.47 y +2.76 puntos en 2019/20–2021/22 —las que eligieron, media ≈ +1.83— y −2.79,
# +1.46 y +0.85 en 2022/23–2024/25 —las que no, media ≈ −0.16—. Un desglose por
# temporada que las presente sin distinguir invita exactamente a citar las tres
# primeras, que es lo que la regla 5 prohíbe.
#
# Rotular las filas con esa tercera distinción se estudió y NO se hace, porque sería
# romper para añadir: `bloque == "validacion"` es un filtro en `_solo_validacion`, en
# la invariante de `qa_models` y en la economía, y renombrarlo devolvería tablas vacías
# en dos sitios. El rótulo por fila no hace falta además, porque el papel de cada
# temporada es función SOLO de la temporada: basta publicar qué temporadas eligieron y
# la aplicación une por `season`. Eso es `TEMPORADAS_ROL` y va a la ficha.
#
# Se derivan de los VALORES DECLARADOS del protocolo (`pt.WARMUP_SEASONS`,
# `pt.CORTE_SELECCION`) y no de la `cfg` que se reciba, y es deliberado: qué temporadas
# eligieron el modelo es un hecho histórico de cómo se construyó, no una propiedad de
# la ventana con la que se lo evalúe después. Al bajar el arranque para evaluar 17/18 y
# 18/19, esas dos caen por debajo del corte, y derivar el papel de la `cfg` las
# rotularía `seleccion`: afirmaría que participaron en una búsqueda anterior a ellas.
# Son `confirmacion`, el nombre que el proyecto ya les da
# (`seleccion.WARMUP_CONFIRMACION`), y por eso sus cifras SÍ son citables.


def temporadas_rol(feat: pd.DataFrame) -> dict[str, str]:
    """Qué papel tuvo cada temporada del histórico frente a la elección del modelo.

    `seleccion` son las que eligieron —y cuyas cifras la regla 5 no deja citar—,
    `medicion` las posteriores al corte, `confirmacion` las anteriores a la búsqueda
    que un arranque de una sola temporada abre, y `en_curso` la que se está jugando.

    El `holdout` se reconoce por el propio dato (`C.SPLIT`) y no por la aritmética del
    corte, y esa rama no es defensiva: 2025/26 es posterior al corte, así que sin ella
    la temporada sellada sale rotulada `medicion` y la aplicación la enseñaría como una
    temporada medida más. Es el sello de la regla 7 y la decisión es de Manuel.
    """
    warmup = {str(s) for s in pt.WARMUP_SEASONS}
    curso = {str(s) for s in pt.temporadas_en_curso(feat)}
    sellado = {str(s) for s in
               feat.loc[feat[C.SPLIT] == "holdout", C.SEASON].dropna().unique()}
    corte = str(pt.CORTE_SELECCION)
    papel = {}
    for s in sorted({str(x) for x in feat[C.SEASON].dropna().unique()}):
        if s in curso:
            papel[s] = "en_curso"
        elif s in sellado:
            papel[s] = "holdout"
        elif s in warmup:
            papel[s] = "confirmacion"
        else:
            papel[s] = "seleccion" if s < corte else "medicion"
    return papel


# Las referencias que se le piden a `evaluate.compare`: su mismo valor por
# defecto, escrito aquí porque `_referencias_vivas` necesita recorrerlo para
# saber cuáles tienen precio en cada temporada.
_REFERENCIAS = ("consenso", "pinnacle", "b365", "prior", "local")


def _escribe(df: pd.DataFrame, nombre: str, regimen: str, *, save: bool,
             codigo: str) -> Path:
    """Escribe un artefacto sellado con el código de ejecución que lo produjo."""
    destino = OUT / regimen / f"{nombre}.parquet"
    if not save:
        print(f"  [DRY] {destino.relative_to(_ROOT)}  ({len(df)} filas)")
        return destino
    salida = df.copy()
    # Los `attrs` se vacían antes de escribir. `protocol.walk_forward` deja en
    # `pred.attrs` el objeto `ProtocolConfig` y el diccionario de estados ajustados
    # —un `StandardScaler` y una logística por pliegue—, y `backtest` encadena esos
    # mismos `attrs` a los libros de apuestas. Son útiles en memoria y no son
    # escribibles: pandas vuelca `attrs` a los metadatos del parquet como JSON, y
    # el primer artefacto que hereda la cadena aborta con «Object of type
    # ProtocolConfig is not JSON serializable». Perderlos no cuesta nada aquí,
    # porque lo que `attrs` dice del protocolo ya viaja en `manifiesto.json` y en
    # la columna `run_code`, que es lo que lee el consumidor.
    salida.attrs = {}
    # Un índice con nombre lleva información —la temporada, la métrica— que se
    # perdería al leer el parquet como una tabla plana, de modo que se materializa
    # como columna antes de escribir. El consumidor de estos artefactos es una web,
    # no una sesión de pandas.
    if salida.index.name is not None or isinstance(salida.index, pd.MultiIndex):
        salida = salida.reset_index()
    salida["run_code"] = codigo
    write_df_required(salida, destino, fmt="parquet", etiqueta="build_models")
    print(f"  [OK ] {destino.relative_to(_ROOT)}  ({len(salida)} filas)")
    return destino


def _predicciones_largas(pred: pd.DataFrame) -> pd.DataFrame:
    """Las predicciones con el nombre de la familia como columna.

    La columna `modelo` sobrevive aunque ahora solo haya uno: la aplicación filtra
    por ella y el histórico de artefactos ya publicados la trae, de modo que
    quitarla obligaría a tocar el consumidor sin ganar nada.
    """
    t = pred.copy()
    t.insert(0, "modelo", FAMILIA)
    return t


def _referencias_vivas(sub: pd.DataFrame) -> tuple[str, ...]:
    """Las referencias que tienen precio dentro de ESTA temporada.

    `evaluate.compare` mide a todos los contendientes sobre la intersección de
    los partidos que todos cubren, y eso es lo correcto —comparar dos cifras
    calculadas sobre partidos distintos no compara nada—, pero convierte a
    cualquier referencia sin cobertura en un veto sobre la temporada entera.

    Es exactamente lo que le pasaba a la temporada en curso. **Pinnacle cubre
    3037 de los 3040 partidos de validación y 0 de los 78 de 2026/27**, porque
    football-data dejó de publicar su precio; exigirla vaciaba la intersección,
    `compare` levantaba `ValueError` y 2026/27 se caía de
    `metricas_por_temporada` en silencio —el `except ValueError: continue` de
    abajo— aunque sus 78 partidos estuvieran puntuados y su economía publicada.
    La tabla por temporada terminaba en 2024/25 sin decir por qué.

    Lo que se descarta es la referencia muerta, no la temporada. El precio de
    hacerlo está declarado y es cero donde ya había cobertura: en las seis
    temporadas de validación esta función devuelve las cinco referencias y la
    cifra no se mueve ni un decimal. Y la referencia ausente no se disimula:
    una temporada sin fila `pinnacle` dice por sí misma que ahí no hubo precio
    de Pinnacle, que es más honesto que una fila rellena o que ninguna tabla.
    """
    vivas = []
    for r in _REFERENCIAS:
        P = ev.reference_probs(sub, r)
        if P is None:
            continue
        if bool((np.isfinite(P).all(axis=1) & ev.evaluable(sub)).any()):
            vivas.append(r)
    return tuple(vivas)


def _por_temporada_con_mercado(pred: pd.DataFrame) -> pd.DataFrame:
    """Métricas por temporada del modelo y de sus referencias, en las mismas filas.

    La página de métricas necesita poder dibujar la línea del modelo y la del
    mercado sin cruzar dos ficheros, y la memoria necesita que ambas estén medidas
    sobre el mismo conjunto de partidos dentro de cada temporada. `compare` impone
    esa restricción temporada a temporada; hacerlo una sola vez sobre el total no
    bastaría, porque la cobertura del precio no es la misma en 2019 que en 2024.

    Cada fila sale rotulada con su bloque. Agrupar por temporada ya separa las dos
    poblaciones —ninguna temporada está a la vez en validación y en curso—, pero
    separar no es lo mismo que decirlo: quien lee la tabla ve una serie de
    temporadas seguidas y no tiene cómo saber que la última se midió con el
    holdout dentro de su ventana de entrenamiento. El rótulo es lo que impide
    citar la última fila como si fuera una más de la serie.
    """
    por_bloque = (pred.groupby(BLOQUE, sort=True) if BLOQUE in pred.columns
                  else [("validacion", pred)])
    filas = []
    for bloque, trozo in por_bloque:
        for temporada, sub in trozo.groupby(C.SEASON, sort=True):
            try:
                comp = ev.compare(sub, referencias=_referencias_vivas(sub))
            except ValueError:                  # temporada sin filas evaluables
                continue
            for contendiente, r in comp.iterrows():
                filas.append({C.SEASON: temporada, BLOQUE: bloque,
                              "contendiente": contendiente, **r.to_dict()})
    return pd.DataFrame(filas)


def _por_cuotas(comparaciones: dict, clave: str, *,
                bloque: str = "validacion") -> pd.DataFrame:
    """Las tablas de la comparación de los dos precios, apiladas y etiquetadas.

    Una tabla larga con la columna `cuotas` se filtra sin saber de antemano qué
    momentos del precio se midieron, y la columna `bloque` dice de qué población
    sale cada fila: una serie económica de la temporada en curso y una de
    validación se dibujan igual y no significan lo mismo.
    """
    trozos = []
    for cual, cmp_ in comparaciones.items():
        t = cmp_[clave].copy()
        if t.empty:
            continue
        t.insert(0, "cuotas", cual)
        t.insert(1, "casa", cmp_["casa"])
        t.insert(2, BLOQUE, bloque)
        trozos.append(t)
    return (pd.concat(trozos, ignore_index=True) if trozos
            else pd.DataFrame(columns=["cuotas", "casa", BLOQUE]))


def _apila_economia(validacion: dict, curso: dict, clave: str) -> pd.DataFrame:
    """Las dos economías —la que decide y la que se está jugando— en una tabla.

    Van juntas porque la aplicación las dibuja juntas y cruzar dos ficheros para
    pintar una sola gráfica es una invitación a desincronizarlos. Van rotuladas
    porque son poblaciones distintas: `qa_models.check_economia` cuadra el cierre
    de la serie contra el manifiesto, y el manifiesto resume validación, de modo
    que sin el rótulo la comprobación cuadraría contra la última jornada jugada y
    fallaría cada semana por el motivo equivocado.
    """
    trozos = [t for t in (_por_cuotas(validacion, clave, bloque="validacion"),
                          _por_cuotas(curso, clave, bloque="produccion"))
              if not t.empty]
    return (pd.concat(trozos, ignore_index=True) if trozos
            else pd.DataFrame(columns=["cuotas", "casa", BLOQUE]))


def _economia_manifiesto(cmp_: dict) -> dict:
    """El bloque económico de un momento del precio, en cifras planas.

    Se aplana a propósito. El manifiesto lo leen la aplicación y el cuaderno, y una
    jerarquía anidada obligaría a ambos a saber su forma; con las cifras de la
    referencia como columnas más, la tabla del modelo y la de su listón se leen de
    una sola pasada. El detalle desagregado no cabe aquí y viaja en los artefactos
    `economia_por_temporada` y `economia_por_jornada`.
    """
    escalares = {k: v for k, v in cmp_["modelo"].items()
                 if not isinstance(v, dict)}
    ref = cmp_["referencia"]
    iv = cmp_["intervalo"] or {}
    return {**escalares,
            "casa_referencia": cmp_["casa"],
            "n_apuestas_referencia": ref["n_apuestas"],
            "acierto_referencia": ref["acierto"],
            "cuota_media_referencia": ref["cuota_media"],
            "rendimiento_referencia": ref["rendimiento"],
            "retorno_referencia": ref["retorno"],
            "caida_max_rel_referencia": ref["caida_max_rel"],
            "diferencia": cmp_["diferencia"],
            "diferencia_inf": iv.get("inf", float("nan")),
            "diferencia_sup": iv.get("sup", float("nan")),
            "diferencia_ee": iv.get("ee", float("nan")),
            "diferencia_n_replicas": iv.get("n_replicas", 0)}


def _agregable(pred: pd.DataFrame) -> pd.DataFrame:
    """Las filas que pueden entrar en una cifra de CONJUNTO, y solo esas.

    Validación y temporada en curso conviven en las tablas descriptivas, y para
    las filas por temporada eso es correcto: cada una dice de qué temporada habla.
    La fila agregada no puede hacer lo mismo. El agregado es la cifra que la
    memoria cita como rendimiento del sistema, y la temporada en curso se puntúa
    con el holdout dentro de su ventana de entrenamiento, así que sumarla al total
    contamina justamente la cifra que más se mira, de la forma más difícil de ver:
    no cambia ninguna columna ni rompe ninguna comprobación, solo desplaza el
    número unas décimas en la dirección favorable.

    Que el recorte sea del total y no de la tabla entera es deliberado. La
    temporada en curso es una de las tres cifras que el trabajo necesita y tiene
    que publicarse; lo que no puede es publicarse disuelta.
    """
    if BLOQUE not in pred.columns:
        return pred
    return pred[pred[BLOQUE] == "validacion"]


def _fiabilidad_por_temporada(pred: pd.DataFrame) -> pd.DataFrame:
    """Curvas de fiabilidad por clase y temporada, más la del conjunto.

    Es la evidencia visual de la calibración y solo dice algo desagregada: un
    modelo puede estar bien calibrado en promedio y mal en la clase del empate, que
    es justamente la que interesa, y el promedio lo escondería.

    La fila `total` es de validación (ver `_agregable`); las de temporada traen
    también la que se está jugando, rotulada con su temporada.
    """
    trozos = []
    for etiqueta, sub in ([("total", _agregable(pred))]
                          + list(pred.groupby(C.SEASON, sort=True))):
        y = ev._objetivo(sub)
        P = ev._terna(sub, ev.PROB_COLS)
        m = np.isfinite(P).all(axis=1) & np.isfinite(y)
        if not m.any():
            continue
        for clase in [None, 0, 1, 2]:
            t = ev.reliability_table(y[m], P[m], clase=clase)
            t.insert(0, C.SEASON, etiqueta)
            trozos.append(t)
    return pd.concat(trozos, ignore_index=True)


def _confusion_por_temporada(pred: pd.DataFrame) -> pd.DataFrame:
    """Matrices de confusión en formato largo: temporada, verdad, predicción, n.

    Mismo reparto que en la fiabilidad: `total` es de validación, las filas por
    temporada incluyen la que se está jugando.
    """
    trozos = []
    for etiqueta, sub in ([("total", _agregable(pred))]
                          + list(pred.groupby(C.SEASON, sort=True))):
        y = ev._objetivo(sub)
        P = ev._terna(sub, ev.PROB_COLS)
        m = np.isfinite(P).all(axis=1) & np.isfinite(y)
        if not m.any():
            continue
        cm = ev.confusion_matrix(y[m], P[m])
        largo = (cm.rename_axis("real").reset_index()
                 .melt(id_vars="real", var_name="predicha", value_name="n"))
        largo.insert(0, C.SEASON, etiqueta)
        trozos.append(largo)
    return pd.concat(trozos, ignore_index=True)


def _relatos_largos(rep: dict) -> pd.DataFrame:
    """Los tres relatos globales en una tabla larga, comparables fila a fila.

    Se toma la tabla que `explain.agreement` ya ha alineado —mismo índice de
    bloques, una columna por vía— en lugar de recomponerla aquí desde las tres
    piezas sueltas. Las vías no siempre son tres: un modelo no lineal no publica
    coeficientes, y en ese caso la columna sencillamente no existe. Formato largo
    porque quien lo consume dibuja una barra por vía y bloque y no puede saber de
    antemano cuántas vías habrá.
    """
    val = rep["concordancia"]["valores"]
    largo = (val.rename_axis("bloque").reset_index()
             .melt(id_vars="bloque", var_name="via", value_name="importancia"))
    largo["importancia"] = largo["importancia"].astype("float64")
    return largo[["via", "bloque", "importancia"]].reset_index(drop=True)


# ===========================================================================
# 7. Recorrido completo de un régimen
# ===========================================================================
_COLS_TABLA = ["contendiente", "n", "rps", "logloss", "brier", "accuracy",
               "rps_ic_bajo", "rps_ic_alto", "skill_rps"]


def puntua_en_curso(feat: pd.DataFrame, fab, cfg: pt.ProtocolConfig, *,
                    cache=None, verbose: bool = True) -> pd.DataFrame:
    """Puntúa la temporada EN CURSO, jornada a jornada, con el modelo ya cerrado.

    Es la tercera de las tres cifras que el trabajo necesita —el agregado, el
    desglose por temporada y **cómo va el modelo en la temporada que se está
    jugando**— y la única que un lector de la aplicación puede contrastar él mismo
    el lunes siguiente.

    No decide nada, y eso no es una cautela de estilo sino la condición que la hace
    publicable. Cualquier jornada posterior al holdout se entrena con el holdout
    dentro de la ventana, de modo que si estas predicciones entrasen en la elección
    de familia, de hiperparámetros o de variables, la temporada sellada habría
    participado en la decisión y su cifra dejaría de ser independiente. Por eso el
    bloque vive fuera de `validacion` (ver `protocol._bloque_evaluable`), el banco
    de `laboratorio.py` se niega a tocarlo y aquí se recibe la fábrica ya
    construida: no hay ninguna alternativa que comparar.

    Tampoco lleva sello de pasada única, al contrario que el holdout, porque se
    reejecuta **cada semana** por definición: la jornada que se acaba de disputar
    es una fila nueva de la misma tabla. Repetirla no es volver a mirar un examen;
    es el funcionamiento normal del sistema.
    """
    folds = pt.make_folds(feat, cfg, evaluar="produccion")
    if not folds:
        if verbose:
            print("\n[INFO] temporada en curso sin jornadas puntuables todavía: "
                  "no hay partidos jugados posteriores al holdout, o la ventana de "
                  "entrenamiento aún no llega al mínimo del protocolo.")
        return pd.DataFrame()

    pred = pt.run_protocol(feat, fab, cfg, folds=folds, cache=cache,
                           verbose=verbose)
    if verbose and not pred.empty:
        temporadas = ", ".join(sorted(pred[C.SEASON].dropna().unique()))
        print(f"\nEN CURSO [{temporadas}] {len(folds)} jornadas, {len(pred)} "
              "partidos puntuados con el modelo cerrado (no decide nada)")
        try:
            fila = ev.compare(pred).loc["modelo"]
            print(f"  rps={fila['rps']:.4f}  logloss={fila['logloss']:.4f}  "
                  f"acierto={fila['accuracy']:.1%}")
        except (ValueError, KeyError):
            print("  aún son pocos partidos para una métrica estable")
    return pred


def build_regimen(feat: pd.DataFrame, regimen: str, *, save: bool = True,
                  verbose: bool = True) -> dict:
    """Ejecuta y publica el bloque de validación completo bajo un régimen.

    La `cfg` sale de `PROTOCOLO` y no de `pt.ProtocolConfig()` recién construido, y
    el cambio es una corrección: construirla aquí con los valores por omisión hacía
    que `PROTOCOLO` fuera una declaración que la pasada no usaba. Dos fuentes de
    verdad para el esquema de validación, y la peor pareja posible —el cuaderno
    comprueba `PROTOCOLO` (sección E.2) y los artefactos salían de la otra—, de modo
    que tocar el arranque en `PROTOCOLO` no cambiaba una sola cifra y el desajuste no
    aparecía en ningún sitio: todas las tablas salen con su aspecto normal. Medido al
    bajar el arranque el 21-oct-2026, la pasada siguió dando 255 pliegues desde
    2019/20 con `PROTOCOLO` declarando una sola temporada de arranque.
    """
    cfg = replace(PROTOCOLO, regime=regimen)
    cols = conjunto_efectivo(feat, verbose=verbose)
    codigo = _huella(feat, cfg, regimen, columnas=cols)
    if verbose:
        print("\n" + "#" * 72)
        print(f"# RÉGIMEN: {regimen}   (código de ejecución {codigo})")
        print("#" * 72)
        print(cfg.describe())
        print(f"conjunto: {len(cols)} variables · familia «{FAMILIA}» · "
              f"{'cerrado' if conjunto_cerrado() else 'PROVISIONAL'} "
              f"(procedencia: {PROCEDENCIA})")

    fab = lb.fabrica(FAMILIA, cols, hiper_efectivo())
    folds = pt.make_folds(feat, cfg, evaluar="validacion")
    cache = pt._StateCache(feat, cfg)
    pred = pt.run_protocol(feat, fab, cfg, folds=folds, cache=cache,
                           verbose=verbose)
    if pred.empty:
        raise ValueError(f"el recorrido no ha dejado ninguna predicción ({regimen})")

    previa = previa_favorable(pred)
    tab = tabla_comparativa(pred)
    if verbose:
        print("\nmétricas:")
        print(tab[[c for c in _COLS_TABLA if c in tab.columns]]
              .round(4).to_string(index=False))
        print(f"previa de frecuencias: rps={previa['rps']:.4f}  "
              f"acierto={previa['accuracy']:.1%}  sobre {previa['n']} partidos")

    eleccion = comprueba_piso(tab, previa, verbose=verbose)

    # La temporada en curso se puntúa DESPUÉS de haber decidido, y su resultado no
    # vuelve sobre `tab` ni sobre `comprueba_piso`: todo lo que elige algo —la
    # familia, el piso frente al mercado, la importancia por bloque— se ha medido
    # ya, y solo sobre validación. Lo que la temporada en curso alimenta es la
    # parte DESCRIPTIVA, que es la que lee la aplicación: el desglose por
    # temporada, la economía y el registro de partido. La columna `bloque`
    # acompaña a cada fila para que ninguna tabla publicada mezcle las dos
    # poblaciones en silencio.
    pred_curso = puntua_en_curso(feat, fab, cfg, cache=cache, verbose=verbose)
    descrip = pd.concat(
        [pred.assign(**{BLOQUE: "validacion"})]
        + ([pred_curso.assign(**{BLOQUE: "produccion"})]
           if not pred_curso.empty else []),
        ignore_index=True)

    # Métricas por temporada, desacuerdo y fiabilidad del modelo publicado.
    #
    # Las tablas que se desglosan por temporada reciben `descrip` entera y rotulan
    # cada fila con su bloque; las que producen una cifra de conjunto reciben solo
    # lo agregable (ver `_agregable`). El desacuerdo es de las segundas aunque no
    # lo parezca: sus grupos son cuantiles de distancia al mercado calculados
    # sobre todas las filas a la vez, de modo que meter la temporada en curso no
    # añadiría una fila, redefiniría los cortes de las cinco.
    por_temporada = _por_temporada_con_mercado(descrip)
    desacuerdo = ev.disagreement(_agregable(descrip))
    fiabilidad = _fiabilidad_por_temporada(descrip)
    confusion = _confusion_por_temporada(descrip)

    # Rendimiento económico. Se necesita el precio en escala de cuota, que vive en
    # el conjunto de variables y no en la salida del protocolo.
    # Los dos MOMENTOS del precio se publican juntos porque una apuesta emitida
    # antes de la jornada iría contra la apertura mientras que el histórico se
    # calcula contra el cierre, y reportar solo uno de ellos sobreestima o
    # infravalora el rendimiento de forma sistemática. El eje es distinto del de
    # las tres casas —consenso, Pinnacle y Bet365— con el que se mide la calidad
    # del pronóstico: aquí no se compara contra quién, sino contra cuándo.
    #
    # Cada momento del precio se mide contra el apostante que sigue al favorito de
    # Bet365 y cobra a esas mismas cuotas. El listón no es el cero: perder poco
    # donde el mercado pierde mucho es un resultado y ganar poco donde el mercado
    # gana más no lo es, de modo que el rendimiento a solas no responde a la
    # pregunta que el trabajo hace. La comparación sale temporada a temporada y
    # jornada a jornada porque una ventaja que solo existe en el agregado suele ser
    # una ventaja que solo existe en una temporada.
    #
    # Y sale DOS VECES, no una. El agregado es la respuesta a la pregunta del
    # trabajo y se mide solo sobre validación; la temporada en curso se mide
    # aparte, con la misma política y contra el mismo listón, y sus filas viajan
    # rotuladas. Simularlas juntas no daría «el rendimiento con una temporada
    # más»: daría una sola cifra que no es ninguna de las dos, porque la curva
    # acumulada, la caída máxima y el intervalo por remuestreo son funciones de
    # toda la serie y no sumas de tramos.
    val_desc = _agregable(descrip)
    curso_desc = (descrip[descrip[BLOQUE] == "produccion"]
                  if BLOQUE in descrip.columns else descrip.iloc[:0])
    libros, resumenes, comparaciones, curso_cmp = {}, {}, {}, {}
    for cual in ("cierre_avg", "consenso"):
        con_cuotas = bt.attach_odds(val_desc, feat, cuotas=cual)
        comparaciones[cual] = bt.compara_con_mercado(
            con_cuotas, politica=POLITICA_ECONOMIA, importe="plana",
            n_boot=N_BOOT_ECONOMIA, seed=SEMILLA_ECONOMIA,
            intervalos_temporada=(cual == "cierre_avg"))
        libros[cual] = comparaciones[cual]["libros"]["modelo"]
        resumenes[cual] = comparaciones[cual]["modelo"]
        if verbose:
            r = resumenes[cual]
            print(f"\nrendimiento [{cual}]: {r['n_apuestas']} apuestas sobre "
                  f"{r['n_partidos']} partidos, rendimiento={r['rendimiento']:+.2%}, "
                  f"retorno={r['retorno']:+.2%}, caída máxima="
                  f"{r['caida_max_rel']:.2%}, margen del libro={r['overround']:.2%}")
            bt.imprime_comparacion(comparaciones[cual])
        if not curso_desc.empty:
            try:
                # Sin intervalo: con las jornadas que lleva una temporada a medias,
                # el remuestreo no distingue nada y publicar un intervalo vacío de
                # información invita a leerlo como si dijera algo.
                curso_cmp[cual] = bt.compara_con_mercado(
                    bt.attach_odds(curso_desc, feat, cuotas=cual),
                    politica=POLITICA_ECONOMIA, importe="plana", n_boot=0,
                    seed=SEMILLA_ECONOMIA, intervalos_temporada=False)
            except (ValueError, KeyError, ZeroDivisionError) as e:
                # Que todavía no se pueda formar la comparación en curso —pocas
                # apuestas, una de las dos estrategias sin jugar— no invalida la
                # pasada: lo que se publica es lo que decide, y eso ya está medido.
                if verbose:
                    print(f"[INFO] economía en curso [{cual}] aún no medible: {e}")
    libro = libros["cierre_avg"]
    economia_temporada = _apila_economia(comparaciones, curso_cmp, "por_temporada")
    economia_jornada = _apila_economia(comparaciones, curso_cmp, "por_jornada")

    relato = explica_publicado(feat, cfg, fab, folds=folds, cache=cache,
                               columnas=cols, verbose=verbose)

    if verbose:
        print("\nartefactos:")
    _escribe(_predicciones_largas(pred), "predicciones_validacion", regimen,
             save=save, codigo=codigo)
    if not pred_curso.empty:
        # En su propio fichero y no apiladas con las de validación: el nombre del
        # artefacto es la primera cosa que lee quien lo abre, y «validacion» no
        # puede contener la temporada en curso sin desmentirse.
        _escribe(_predicciones_largas(pred_curso), "predicciones_produccion",
                 regimen, save=save, codigo=codigo)
    _escribe(tab, "metricas_modelos", regimen, save=save, codigo=codigo)
    _escribe(tabla_conjunto(cols, feat), "conjunto_declarado", regimen, save=save,
             codigo=codigo)
    _escribe(por_temporada, "metricas_por_temporada", regimen, save=save,
             codigo=codigo)
    _escribe(desacuerdo, "desacuerdo_mercado", regimen, save=save, codigo=codigo)
    _escribe(fiabilidad, "fiabilidad", regimen, save=save, codigo=codigo)
    _escribe(confusion, "confusion", regimen, save=save, codigo=codigo)
    # El registro de apuesta a apuesta, de los dos bloques y rotulado. Es lo que
    # permite que el lunes siguiente un lector compruebe por su cuenta la jornada
    # que acaba de verse, que es la única cifra del trabajo contrastable sin
    # esperar a la memoria; y el rótulo es lo que impide sumarla al histórico.
    _escribe(pd.concat(
        [lg.assign(cuotas=k, **{BLOQUE: "validacion"})
         for k, lg in libros.items()]
        + [cmp_["libros"]["modelo"].assign(cuotas=k, **{BLOQUE: "produccion"})
           for k, cmp_ in curso_cmp.items()
           if not cmp_["libros"]["modelo"].empty],
        ignore_index=True),
             "matchlogs", regimen, save=save, codigo=codigo)
    _escribe(economia_temporada, "economia_por_temporada", regimen, save=save,
             codigo=codigo)
    _escribe(economia_jornada, "economia_por_jornada", regimen, save=save,
             codigo=codigo)
    _escribe(_relatos_largos(relato), "importancia_bloques", regimen, save=save,
             codigo=codigo)
    _escribe(relato["permutacion"], "permutacion_detalle", regimen, save=save,
             codigo=codigo)
    _escribe(relato["bloques"], "registro_bloques", regimen, save=save,
             codigo=codigo)
    if relato.get("coeficientes") is not None:
        _escribe(relato["coeficientes"], "coeficientes_bloques", regimen,
                 save=save, codigo=codigo)

    manifiesto = {
        "run_code": codigo,
        "generado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "regimen": regimen,
        "protocolo": cfg.describe(),
        # El papel de cada temporada, en estructura y no solo dentro de la prosa de
        # `protocolo`. Es lo que deja a la aplicación rotular el repaso temporada por
        # temporada sin parsear una frase, y lo que impide que presente como
        # resultado las tres que eligieron el peso. El por qué, en `temporadas_rol`.
        "temporadas_rol": temporadas_rol(feat),
        "entrada": {"artefacto": str(FEATURES.relative_to(_ROOT)),
                    "filas": int(feat.shape[0]), "columnas": int(feat.shape[1]),
                    "ultima_fecha": str(pd.to_datetime(feat[C.DATE]).max().date())},
        # El conjunto viaja entero en el manifiesto y no solo en el parquet porque
        # quien emite la predicción de la próxima jornada lo lee de aquí: ajustar
        # la familia sobre otras variables daría una emisión que no corresponde a
        # ninguna cifra publicada.
        "conjunto": {"cerrado": conjunto_cerrado(),
                     "procedencia": PROCEDENCIA,
                     "n_variables": len(cols),
                     "variables": list(cols),
                     "nota": ("" if conjunto_cerrado() else
                              "el conjunto declarado está vacío; esta pasada usa "
                              f"los dominios {list(DOMINIOS_PROVISIONALES)} y sus "
                              "cifras no son publicables")},
        "familia": FAMILIA,
        "hiperparametros": hiper_efectivo(),
        "pliegues": int(len(folds)),
        # La temporada en curso se declara aparte y con su recuento, para que al
        # leer el manifiesto quede a la vista que las tablas por temporada traen
        # una temporada más que los pliegues de validación.
        "en_curso": ({"temporadas": sorted(pred_curso[C.SEASON].unique().tolist()),
                      "jornadas": int(pred_curso["fold"].nunique())
                      if "fold" in pred_curso.columns else None,
                      "partidos": int(len(pred_curso)),
                      # Su economía va AQUÍ y no en `economia`, que es la cifra de
                      # validación que responde a la pregunta del trabajo. Es la
                      # tercera lectura que el proyecto necesita —cómo va el
                      # modelo en la temporada que se juega— y lleva su propio
                      # listón de Bet365 por la misma razón que la otra.
                      "economia": {k: _economia_manifiesto(v)
                                   for k, v in curso_cmp.items()},
                      "nota": "puntuada con el modelo cerrado; no decide nada"}
                     if not pred_curso.empty else
                     {"temporadas": [], "partidos": 0, "economia": {},
                      "nota": "sin jornadas puntuables todavía"}),
        "publicado": eleccion,
        "economia": {k: _economia_manifiesto(v) for k, v in comparaciones.items()},
        "concordancia": relato["concordancia"]["spearman"].to_dict(),
        "limitaciones": LIMITACIONES,
        "holdout": "no puntuado en esta ejecución",
    }
    if save:
        destino = OUT / regimen / "manifiesto.json"
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False,
                                      default=str), encoding="utf-8")
        print(f"  [OK ] {destino.relative_to(_ROOT)}")
    else:
        print("  [DRY] manifiesto.json")

    return {"tabla": tab, "prediccion": pred, "previa": previa,
            "eleccion": eleccion, "conjunto": cols, "libro": libro,
            "comparaciones": comparaciones, "relato": relato,
            "manifiesto": manifiesto}


# ===========================================================================
# 8. La pasada única del holdout
# ===========================================================================
def sello_path(regimen: str) -> Path:
    """Fichero que acredita que el holdout ya se puntuó bajo ese régimen."""
    return OUT / regimen / "holdout_sello.json"


def puntua_holdout(feat: pd.DataFrame, regimen: str, *, save: bool = True,
                   verbose: bool = True) -> pd.DataFrame:
    """Puntúa la temporada sellada una sola vez y deja constancia de haberlo hecho.

    El estado del preprocesado se sigue aprendiendo sobre filas de desarrollo, de
    modo que la transformación que recibe el holdout se ajustó sin mirarlo; lo que
    sí entra en la ventana de entrenamiento son las jornadas del holdout ya
    disputadas, que en el momento de predecir son pasado legítimo y cuya exclusión
    haría la cifra artificialmente peor.

    Lo que aquí no cambia es el modelo: el conjunto de variables, la familia y los
    hiperparámetros son los declarados en este módulo, decididos íntegramente sobre
    el bloque de validación. Que no haya nada que elegir en esta función es
    precisamente lo que hace que su cifra signifique algo.

    Si el sello existe, la función se niega. No es una precaución exagerada: la
    tentación de repetir la pasada «solo para ver» con otra variable más es
    exactamente lo que convierte un holdout en un segundo conjunto de validación, y
    contra esa tentación un fichero en disco es mejor argumento que una intención.
    """
    if not conjunto_cerrado():
        raise PermissionError(
            "la reserva no se abre con un conjunto provisional: escribe primero "
            "CONJUNTO, FAMILIA, HIPER y PROCEDENCIA en build_models.py, que es lo "
            "que la sección E.2 del cuaderno 05 comprueba")

    sello = sello_path(regimen)
    if sello.exists():
        previo = json.loads(sello.read_text(encoding="utf-8"))
        raise RuntimeError(
            f"el holdout ya se puntuó el {previo.get('generado')} con el conjunto "
            f"«{previo.get('procedencia')}»; la cifra publicada es la de aquella "
            f"pasada y no se recalcula. Fichero: {sello}")

    # Misma fuente que `build_regimen`: el holdout se puntúa con el protocolo
    # declarado, no con uno construido aquí. Abrirlo bajo un esquema de validación
    # distinto del que produjo las cifras publicadas daría un número incomparable, y
    # como se abre una sola vez (regla 7) no habría segunda oportunidad de notarlo.
    cfg = replace(PROTOCOLO, regime=regimen)
    cols = conjunto_efectivo(feat, verbose=verbose)
    fab = lb.fabrica(FAMILIA, cols, hiper_efectivo())
    pred = pt.run_protocol(feat, fab, cfg, evaluar="holdout", verbose=verbose)
    comp = ev.compare(pred)
    fila = comp.loc["modelo"]
    previa = previa_favorable(pred)
    # La habilidad frente al consenso viaja como columnas `skill_*` en la fila del
    # modelo, y no como un bloque aparte, precisamente porque está medida sobre las
    # mismas filas que el resto de la fila: separarla invitaría a compararla con
    # cifras calculadas sobre otro conjunto de partidos.
    habilidad = {k[len("skill_"):]: float(v) for k, v in fila.items()
                 if k.startswith("skill_") and pd.notna(v)}
    if verbose:
        print(f"\nHOLDOUT [{regimen}] familia «{FAMILIA}» sobre {len(cols)} "
              f"variables y {len(pred)} partidos")
        print(f"  rps={fila['rps']:.4f}  logloss={fila['logloss']:.4f}  "
              f"acierto={fila['accuracy']:.1%}")
        print(f"  previa de frecuencias: rps={previa['rps']:.4f}")
        for otro in comp.index.drop("modelo"):
            print(f"  {otro:10s} rps={comp.loc[otro, 'rps']:.4f}")
        for met, val in habilidad.items():
            print(f"  habilidad en {met} frente al consenso: {val:+.2%}")
        cob = comp.attrs.get("cobertura", {})
        if cob:
            print(f"  filas comunes a todas las referencias: "
                  f"{cob.get('comunes')}/{cob.get('filas')}")

    codigo = _huella(feat, cfg, regimen, columnas=cols)
    if save:
        _escribe(_predicciones_largas(pred), "predicciones_holdout", regimen,
                 save=True, codigo=codigo)
        sello.parent.mkdir(parents=True, exist_ok=True)
        sello.write_text(json.dumps({
            "generado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "run_code": codigo, "regimen": regimen,
            "familia": FAMILIA, "hiperparametros": hiper_efectivo(),
            "procedencia": PROCEDENCIA,
            "conjunto": list(cols),
            "temporadas": sorted(pred[C.SEASON].unique().tolist()),
            "partidos": int(len(pred)),
            "metricas": {k: float(v) for k, v in fila.items()
                         if pd.notna(v) and not k.startswith("skill_")},
            "previa": previa,
            "habilidad": habilidad,
            "referencias": {i: {k: float(v) for k, v in comp.loc[i].items()
                                if pd.notna(v)}
                            for i in comp.index.drop("modelo")},
            "cobertura": comp.attrs.get("cobertura", {}),
            "limitaciones": LIMITACIONES,
        }, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        print(f"  [OK ] {sello.relative_to(_ROOT)}  (sello de pasada única)")
    else:
        print("  [DRY] no se escribe el sello: la pasada no cuenta como emitida")
    return pred


# ===========================================================================
# 9. Entrada
# ===========================================================================
def main(save: bool = True, regimenes: list[str] | None = None,
         holdout: bool = False) -> dict:
    feat = pd.read_parquet(FEATURES)
    print(f"features_matches: {feat.shape[0]} partidos x {feat.shape[1]} columnas")
    regimenes = regimenes or list(pp.REGIMES)

    salidas = {}
    for regimen in regimenes:
        salidas[regimen] = build_regimen(feat, regimen, save=save)

    if holdout:
        for regimen in regimenes:
            print("\n" + "=" * 72)
            puntua_holdout(feat, regimen, save=save)

    if not save:
        print("\n[DRY] no se ha escrito nada.")
    return salidas


if __name__ == "__main__":
    _reg = None
    if "--regimen" in sys.argv:
        _reg = [sys.argv[sys.argv.index("--regimen") + 1]]
    main(save="--dry" not in sys.argv,
         regimenes=_reg,
         holdout="--holdout" in sys.argv)
