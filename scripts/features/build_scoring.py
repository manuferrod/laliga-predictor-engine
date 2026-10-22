"""Ruta de INFERENCIA (scoring): features de los partidos FUTUROS a predecir.

Simetría con el entrenamiento
-----------------------------
El entrenamiento (`build_features.py`) convierte el máster de partidos JUGADOS en
`features_matches.parquet`. Para PREDECIR una jornada nueva hacen falta EXACTAMENTE
las mismas features, pero calculadas para partidos que aún no se han jugado. La
regla de oro: las features de un partido futuro solo pueden mirar hacia ATRÁS
(partidos ya jugados). Todo el diseño con `shift()` de form.py garantiza esto por
construcción, así que la ruta de inferencia REUTILIZA el mismo código de features.

Idea central: "máster extendido"
---------------------------------
En lugar de duplicar la lógica de features, se construyen filas de partido futuro
compatibles con el esquema del máster (todas las columnas POST-partido = NaN,
porque todavía no existen) y se CONCATENAN al máster jugado:

    máster jugado (3800)      +      filas futuras (jornada N)
        (post-partido real)              (post-partido = NaN)

Sobre esa unión se ejecuta `build_features` sin cambios. Como cada feature de forma
mira solo partidos anteriores del equipo (shift), las filas jugadas NO se alteran y
las filas futuras heredan la forma de su historial. Después se filtran las filas
futuras y se marcan `split='predict'`.

Esa herencia es literal para las dos parametrizaciones de la memoria —la ventana
rectangular y el decaimiento exponencial— y para el acumulado de temporada: se
calculan aquí con el mismo código y los mismos parámetros que en entrenamiento,
porque son el mismo código. La fila futura entra con marcador NaN, de modo que sus
puntos y su diferencia de goles son NaN y ninguna ventana los confunde con un cero.

Qué se conoce ANTES del partido futuro y de dónde sale
------------------------------------------------------
Se engancha TODO lo pre-partido que se pueda conocer en el momento de puntuar:

  - cuotas / mercado : del extractor football-data (`upcoming()`), cuotas de
    APERTURA + prob. implícita -> `footballdata_upcoming.parquet`. Si football-data
    aún NO ha publicado cuotas para algún partido, el usuario las mete a mano por
    una PLANTILLA CSV (ver "Cuotas manuales" abajo). Las cuotas manuales pasan por
    exactamente la MISMA normalización de-margen (`_implied_probs`) que las
    automáticas, así que son indistinguibles aguas abajo.
  - Elo              : `clubelo_elo.parquet` vía merge_asof (último Elo <= fecha),
    tipificado dentro de la jornada contra el censo de la temporada completa, igual
    que en entrenamiento. Sostiene dos cosas distintas: la fuerza relativa del
    partido a predecir y, aguas arriba, la dureza del calendario
    (`form_opp_elo_z_*`), que es la fuerza media de los rivales ya enfrentados. Por
    eso su ausencia se vigila en dos bloques y no en uno.
  - valor plantilla  : `team_squad_value.parquet` por (season_year, team_key).
  - fatiga           : `team_fatigue.parquet` (descanso / congestión multi-comp).
    El calendario sofascore ya trae los fixtures FUTUROS, así que el descanso y la
    congestión de un partido por jugar son conocibles mirando hacia atrás.
  - bajas            : dos sub-bloques, los mismos que trae el máster de
    entrenamiento. (a) RECUENTOS de `team_availability.parquet` (lesionados, dudas,
    sancionados). (b) BAJAS PONDERADAS por valor económico, recalculadas con
    `consolidate/absences.py` desde el crudo `sofascore_missing_players.csv` y
    `transfermarkt_player_values.parquet`, con el mismo lag t-1 que en consolidación.
    Ambos existen solo cuando sofascore ya publicó alineaciones/parte médico del
    partido; si aún no hay alineaciones -> quedan NaN (hueco honesto, no se inventa).
  - rotación y carga : `team_lineups.parquet`. Es el único bloque de sofascore que
    no espera a que la fuente publique nada del partido a predecir: la continuidad
    del once y los minutos acumulados se leen del pasado del equipo, de modo que
    basta con que el extractor esté al día.

Lo que NO se engancha, y por qué
---------------------------------
La calidad de ocasión (`team_shot_quality.parquet`) describe los remates del
partido y por tanto no puede conocerse antes de jugarlo. No se engancha a la fila
futura —queda en NaN, como el marcador— y llega a la predicción por el único
camino legítimo: las ventanas retrospectivas de `features/form.py`, que la leen de
los partidos YA disputados del máster. La vigilancia del bloque, aun así, existe
(`forma (calidad de ocasión)` en la política de bloques), porque lo que sí puede
fallar es que el volcado de tiro no se haya ejecutado y esa forma llegue vacía.

Puente clave: `event_id` -> `match_key` para el FUTURO
------------------------------------------------------
fatiga y bajas se identifican por `event_id` de sofascore. El puente canónico de
consolidación (`reducers.event_to_match_key`) se construye desde la tabla de
partidos JUGADOS, así que NO conoce los event_id futuros. Aquí se construye un
puente FUTURO-consciente desde el crudo `sofascore_team_fixtures.csv` (que sí trae
los fixtures venideros): filas de LaLiga con `is_home`, `match_key = fecha|local|visitante`.

Cuotas manuales (plantilla CSV) + gate estricto
-----------------------------------------------
Como el usuario ejecuta esto varias veces antes de la jornada (mismo día, 1 día
antes, 2 días antes...), puede toparse con que football-data aún no publica cuotas.
En ese caso:
  1. build_scoring detecta los partidos sin cuota y ESCRIBE una plantilla CSV
     (`footballdata_upcoming_manual_odds.csv`) con match_key/fecha/equipos y columnas
     vacías odds_h/odds_d/odds_a.
  2. El usuario rellena las cuotas 1X2 que ve en SU casa de apuestas y re-ejecuta.
  3. build_scoring lee la plantilla, normaliza con `_implied_probs` y las inyecta.
GATE ESTRICTO: si tras el intento automático + manual queda ALGÚN partido sin
cuota, se PARA y NO se escribe `features_upcoming.parquet` (la cuota es una señal
demasiado fuerte como para predecir sin ella).

Gates de frescura y cobertura (por qué no basta con el de cuotas)
-----------------------------------------------------------------
La cuota era, hasta ahora, lo único capaz de interrumpir la ejecución: cualquier
otro bloque podía desvanecerse —artefacto ausente o dañado, reductor que revienta,
merge sin coincidencias— y el script continuaba con un `[WARN]` en consola. El
efecto no era una predicción que fallase, sino una peor: `preprocess.apply`
imputa esas columnas con la mediana aprendida en entrenamiento, así que el modelo
recibía un valor plausible donde no había dato y las probabilidades salían con
aspecto normal. Peor aún, un máster sin consolidar no produce NaN ninguno: genera
forma, clasificación y h2h de la semana pasada, indistinguibles de los buenos.

Se añaden por eso dos guardas declarativas, además del gate de cuotas:

  - FRESCURA (`_freshness_gate`): cruza el calendario de sofascore contra el
    máster y aborta si hay partidos ya disputados sin consolidar. Se mide por
    partidos pendientes, no por días transcurridos, porque un parón de selecciones
    o el intervalo entre temporadas también dejan el máster "viejo" sin que haya
    nada que corregir.
  - COBERTURA (`_coverage_gate`): mide, bloque a bloque y PARTIDO A PARTIDO,
    cuántos llevan el bloque entero en NaN, y lo compara con la tolerancia
    declarada en `_politica_bloques`. Elo, valor de plantilla, forma y fatiga son
    conocibles antes del partido y su ausencia masiva delata un fallo de la
    cadena: abortan. Las bajas no abortan nunca, porque antes de que sofascore
    publique el parte médico el hueco es la respuesta correcta.

La distinción que ordena todo el diseño: la ausencia de un dato que TODAVÍA no
existe es información legítima y se propaga como NaN; la ausencia de un dato que
YA debería estar es un fallo y se propaga como excepción.

¿Qué partidos futuros se predicen? (léelo antes de ejecutar)
------------------------------------------------------------
El pipeline es DIRIGIDO POR FIXTURE, no por número de jornada. La ÚNICA fuente de
"qué partidos vienen" es football-data (`fixtures.csv`), una ventana rodante de
~7 días con TODAS las ligas que el extractor filtra a LaLiga y acumula en
`footballdata_upcoming.parquet`. `build_scoring` puntúa EXACTAMENTE esos fixtures
que aún no están en el máster jugado. Consecuencias, que cubren los casos límite
sin lógica especial:

  - Una jornada NO es "10 partidos" por definición: se predice lo que esté
    programado, sean 10, 8 o 1.
  - Partido APLAZADO  -> sale de la ventana de football-data -> no se predice.
  - Partido CANCELADO -> nunca entra en la ventana -> no se predice.
  - Partido ADELANTADO de otra jornada -> si está en la ventana, se predice igual.
  - Semana REDUCIDA (p.ej. finalistas de Supercopa no juegan liga) -> solo
    aparecen los partidos que football-data programa; nunca se inventan los demás.
  - Fixture YA DISPUTADO pero aún no consolidado en el máster -> queda fuera de la
    VENTANA TEMPORAL (`_future_window`) y no se predice: el parquet de upcoming es
    acumulativo, así que "no estar en el máster" no implica "no jugado todavía".

Como football-data es silencioso (no dice POR QUÉ hay N partidos y no otros),
`build_scoring` imprime un DIAGNÓSTICO DE COMPLETITUD (`_completeness_report`) que
cruza su ventana contra el calendario de sofascore (`sofascore_team_fixtures.csv`,
que sí trae `status`: notstarted / postponed / canceled y llega hasta fin de
temporada). El diagnóstico lista: (1) los partidos que SE predicen y si sofascore
los confirma; (2) los que sofascore PROGRAMA pero football-data aún no trae (hueco
probable: football-data publica con pocos días -> re-ejecuta el extractor o mete
cuota manual); (3) los APLAZADOS/CANCELADOS de la ventana (esperado que falten);
(4) CONFLICTOS football-data↔sofascore. NO se usa el número de jornada (no existe
en los fixtures futuros de ninguna fuente actual); el cross-check por status hace
el mismo trabajo sin depender de un conteo nominal.

Dónde acaba este script (y dónde NO)
------------------------------------
La salida `features_upcoming.parquet` es la matriz CRUDA de inferencia: mismas
columnas y misma escala que `features_matches.parquet`, sin imputar ni escalar. El
modelo NO se alimenta de ella directamente, porque se entrena sobre la salida del
preprocesador. El encadenado (scoring -> `preprocess.apply`) y la verificación de
paridad viven en `build_predict.py`, que es el punto de entrada de la predicción
semanal. Este módulo mantiene una sola responsabilidad: construir las features.

Junto a la matriz se publica `coverage_upcoming.parquet`, con una fila por
partido a predecir que declara qué fuentes llegaron y qué bloques analíticos son
calculables. Comparte definición con la matriz de cobertura del set de
entrenamiento (`features/lineage.py`), de modo que las dos son comparables celda
a celda. Es lo que permite auditar una predicción ya emitida: sin ese registro,
una jornada puntuada con la fatiga a medias resulta indistinguible de una
puntuada con todo el dato, porque el preprocesador imputa el hueco y la
predicción sale sin ningún síntoma visible.

Uso:
    python scripts/features/build_scoring.py          # construye y guarda
    python scripts/features/build_scoring.py --dry    # construye y solo reporta

    # predicción completa (features + preprocesado + paridad):
    python scripts/features/build_predict.py
"""
from __future__ import annotations

import sys
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_CONSOLIDATE = _HERE.parent / "consolidate"
if str(_CONSOLIDATE) not in sys.path:
    sys.path.insert(0, str(_CONSOLIDATE))
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import assert_unique, write_df_required  # noqa: E402

import catalogo  # noqa: E402
import columns as C  # noqa: E402
import footballdata as fd  # noqa: E402
import teamnames as tn  # noqa: E402
from build_features import build_features  # noqa: E402
from form import form_feature_names  # noqa: E402
from spine import add_clubelo, censo_por_temporada  # noqa: E402
from squadvalue import reduce_squad_value  # noqa: E402
from reducers import reduce_fatigue, reduce_availability, reduce_lineups  # noqa: E402
from absences import build_absences  # noqa: E402
from lineage import (  # noqa: E402
    COVERAGE_UPCOMING_OUT, coverage_matrix, feature_lineage)

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
RAW = _ROOT / "data" / "01_raw"
RAW_SOFA = RAW / "sofascore"
RAW_FD = RAW / "footballdata"

MASTER = PROC / "master_matches.parquet"
UPCOMING = PROC / "footballdata_upcoming.parquet"
CLUBELO = PROC / "clubelo_elo.parquet"
SQUADVAL = PROC / "team_squad_value.parquet"
FATIGUE = PROC / "team_fatigue.parquet"
AVAIL = PROC / "team_availability.parquet"
FIXTURES = RAW_SOFA / "sofascore_team_fixtures.csv"
MISSING_RAW = RAW_SOFA / "sofascore_missing_players.csv"
PLAYERVAL = PROC / "transfermarkt_player_values.parquet"
LINEUPS = PROC / "team_lineups.parquet"
PLAYERMIN = PROC / "player_minutes.parquet"
# La calidad de ocasión no se lee aquí —viaja dentro del máster, porque describe
# partidos ya jugados— pero su ruta se declara igualmente: es la fuente cuya
# antigüedad explica que la forma de calidad de ocasión llegue vacía.
TEAMSHOTS = PROC / "team_shot_quality.parquet"
MANUAL_ODDS = RAW_FD / "footballdata_upcoming_manual_odds.csv"
# Registro histórico de lo tecleado, propiedad de `footballdata`: aquí solo se
# ESCRIBE al promover, y es ese módulo el que lo relee para hacerlo prevalecer.
MANUAL_ODDS_HIST = fd.MANUAL_ODDS_HIST
OUT = PROC / "features_upcoming.parquet"

# --------------------------------------------------------------------------- #
# ventana temporal de predicción
# --------------------------------------------------------------------------- #
# `footballdata_upcoming.parquet` es ACUMULATIVO: cada ejecución del extractor
# añade los fixtures de su ventana rodante, pero no borra los antiguos. El filtro
# "no está en el máster jugado" no basta para descartarlos, porque un partido ya
# disputado no entra en el máster hasta que se re-ejecuta la consolidación: entre
# ambos momentos seguiría siendo "futuro" y se predeciría un resultado ya conocido.
# Por eso la ventana se define explícitamente respecto a `today`:
#   [today - _STALE_DAYS, today + _HORIZON_DAYS]
# _STALE_DAYS = 1 tolera el partido de anoche (aún sin consolidar) y descarta todo
# lo anterior; _HORIZON_DAYS = 10 cubre con margen la ventana de ~7 días que
# publica football-data sin llegar a predecir jornadas para las que no hay ni
# alineaciones ni cuotas estables.
_HORIZON_DAYS = 10
_STALE_DAYS = 1


class MissingOddsError(RuntimeError):
    """Se lanza cuando algún partido futuro se queda sin cuota (auto ni manual).

    Interrumpe el scoring ANTES de escribir features_upcoming: la cuota es la
    señal de mercado más fuerte y no se predice sin ella. El mensaje apunta a la
    plantilla CSV que hay que rellenar y re-ejecutar.
    """


class ScoringGateError(RuntimeError):
    """Un bloque pre-partido EXIGIBLE no está en condiciones de puntuar.

    Cubre las tres formas en que la matriz de inferencia puede degradarse sin que
    nada falle: (a) un artefacto de origen ilegible o ausente, (b) un bloque que
    se engancha con excepción y queda entero en NaN, y (c) un bloque presente
    pero vacío en una fracción de los partidos superior a la tolerada.

    Se distingue de `MissingOddsError` —que es el caso particular del mercado, ya
    resuelto con su propia plantilla manual— porque aquí no hay acción de
    subsanación tecleable: lo que procede es regenerar el artefacto que falta.
    """


# --------------------------------------------------------------------------- #
# Política de bloques: qué se exige a cada familia pre-partido para predecir
# --------------------------------------------------------------------------- #
# Exigir el bloque de cuotas y dejar que los demás se cuiden solos sería la forma
# barata de resolver esto, y es justamente la que no sirve: cualquier bloque puede
# desaparecer entero —artefacto ausente, excepción en el enganche, merge sin
# coincidencias— y si eso solo produce un `[WARN]` en consola, la matriz de
# inferencia llega con columnas enteras en NaN que el preprocesador imputa con la
# mediana de entrenamiento. El modelo recibiría entonces un valor plausible en
# lugar de un hueco, y la predicción saldría sin síntoma alguno.
#
# La política se declara aquí, en una tabla, y no repartida por el código, para
# que sea auditable de un vistazo y para que la tolerancia de cada bloque sea una
# decisión explícita y justificada:
#
#   - `max_nan`   fracción máxima de partidos de la jornada que pueden llevar el
#                 bloque ENTERO en NaN. No es un umbral estadístico: refleja
#                 cuántos huecos son esperables por el calendario real.
#   - `exigible`  True  -> superar el umbral ABORTA (el bloque es conocible y su
#                          ausencia indica un fallo de la cadena de datos);
#                 False -> superar el umbral se informa pero no corta (el hueco
#                          es legítimo, p.ej. bajas antes de que se publique el
#                          parte médico).
#   - `max_age`   días de antigüedad del artefacto de origen a partir de los
#                 cuales se avisa. La antigüedad se mide por la fecha de
#                 modificación del fichero, que es una señal DÉBIL (copiar el
#                 repositorio la reinicia), así que nunca aborta por sí sola:
#                 sirve para explicar una cobertura baja, no para provocarla.
#
# Elo y valor de plantilla se exigen con tolerancia pequeña pero no nula: un club
# recién ascendido puede tardar en aparecer en ClubElo o en tener valoración de
# la temporada en curso, y un solo partido de diez no justifica abortar. La
# fatiga admite un cuarto de la jornada (el calendario de un equipo puede quedar
# pendiente de la siguiente pasada, ver `fatigue._crawl`). Las bajas no abortan
# nunca: antes de que sofascore publique alineaciones el hueco es la respuesta
# correcta, y el modelo ya distingue "sin bajas" de "sin dato" con su flag.
@dataclass(frozen=True)
class _Bloque:
    nombre: str
    patrones: tuple[str, ...]   # nombres exactos o prefijos terminados en '*'
    fuente: Path
    max_nan: float
    max_age: float
    exigible: bool
    # subcadenas que descartan una columna capturada por los patrones. Existe
    # porque un bloque no se define por cómo se llaman sus columnas sino por DE
    # QUÉ FUENTE dependen: la forma ajustada al rival comparte el prefijo
    # `home_form_` con el resto de la forma pero exige además que ClubElo publique
    # a los dos equipos, así que se vigila por separado y hay que poder restarla
    # del bloque que se alimenta solo del máster.
    excluye: tuple[str, ...] = ()
    # bloque cuyo fallo de enganche explica el de este. Los motivos se registran
    # con el nombre del enganche que falla (`Elo`, `fatiga`, ...), pero un bloque
    # puede quedarse vacío por culpa de OTRO: sin Elo no hay forma ajustada al
    # rival. Declararlo aquí hace que el informe de incumplimiento apunte a la
    # causa real en lugar de dejar al lector reconstruirla.
    causa_de: str = ""


def _cols_forma(*, prefijos: tuple[str, ...] = (),
                contiene: tuple[str, ...] = (),
                excluye: tuple[str, ...] = ()) -> tuple[str, ...]:
    """Columnas de forma que el catálogo emite, filtradas por atributo BASE.

    El gate vigila bloques, y un bloque se define por la FUENTE de la que
    depende, no por cómo empiezan sus nombres. Vigilarlo con prefijos
    (`home_form_*`) funcionaba mientras cada atributo salía siempre como par;
    desde que el catálogo decide lado por lado, la mayoría sale solo como
    diferencia (`form_ppda_ewm_diff`) y ningún prefijo de lado la captura. Un
    bloque medio invisible es el peor de los casos: el gate seguiría dando
    cobertura verde con la mitad de sus columnas enteras en NaN.

    Por eso se pregunta al catálogo. Se parte de los atributos base que declara
    `form.form_feature_names()`, se filtran —`prefijos` recorta la FAMILIA de
    parametrización (la forma rodante y los acumulados de temporada, que es el
    alcance que el gate venía vigilando), `contiene` y `excluye` recortan por la
    dependencia de fuente: `opp_elo_z` necesita ClubElo, `shot_xg_` el detalle de
    tiro— y se expanden con `catalogo.nombres`, que devuelve exactamente los
    lados que la matriz lleva. Se emiten como nombres exactos, no como patrones,
    para que una columna que desaparezca del catálogo desaparezca también de la
    vigilancia en lugar de dejar un patrón huérfano que no casa con nada.

    Los bloques se mantienen ESTRECHOS a propósito. El gate declara vacío un
    partido cuando le faltan todas las columnas del bloque, de modo que meter en
    él columnas de otra familia solo sirve para que la ausencia de la que
    interesa deje de detectarse.
    """
    nombres = form_feature_names()
    bases = list(nombres["numeric"]) + list(nombres["categorical"])
    out: list[str] = []
    for b in bases:
        if prefijos and not b.startswith(prefijos):
            continue
        if contiene and not any(x in b for x in contiene):
            continue
        if any(x in b for x in excluye):
            continue
        out += catalogo.nombres(b)
    return tuple(dict.fromkeys(out))


def _politica_bloques() -> list[_Bloque]:
    return [
        _Bloque("mercado", tuple(("mkt_p_home", "mkt_p_draw", "mkt_p_away")),
                UPCOMING, max_nan=0.0, max_age=7, exigible=True),
        # La estructura del precio (margen, dispersión entre casas, divergencia con
        # Pinnacle) necesita el desglose por casa, no solo el consenso. Se informa
        # pero no aborta: cuando una cuota se resuelve por la plantilla manual solo
        # se teclea el precio 1X2, así que el desglose falta por diseño y el hueco
        # es la respuesta correcta, no un fallo de la cadena de datos.
        #
        # Las columnas se toman del catálogo en vez de escribirse como patrones.
        # La probabilidad implícita de Pinnacle (`mkt_p_ps_*`) ya no se construye
        # —lo que interesa de esa casa es cuánto se separa del consenso, y eso lo
        # lleva `sharp_div_*`—, de modo que un patrón suyo sería un huérfano que
        # no casa con nada; preguntando al catálogo, ese riesgo no existe.
        _Bloque("estructura del precio", tuple(catalogo.MERCADO_ESTRUCTURA),
                UPCOMING, max_nan=1.0, max_age=7, exigible=False),
        _Bloque("Elo", tuple(C._ELO), CLUBELO,
                max_nan=0.10, max_age=21, exigible=True),
        _Bloque("valor de plantilla", tuple(C._SQUAD), SQUADVAL,
                max_nan=0.10, max_age=400, exigible=True),
        # La forma que se construye sobre el historial propio del equipo (ventana
        # rectangular y decaimiento exponencial, ambas parametrizaciones) depende
        # únicamente de que el máster esté consolidado. Se le restan los dos
        # atributos que, compartiendo el rótulo `form_`, encadenan otra fuente.
        _Bloque("forma (histórico propio)",
                _cols_forma(prefijos=("form_", "cum_"),
                            excluye=("opp_elo_z", "shot_xg_")),
                MASTER, max_nan=0.10, max_age=21, exigible=True),
        # La forma de CALIDAD DE OCASIÓN se vigila aparte por la misma razón que
        # la dureza del calendario: comparte el rótulo `form_` con el resto pero
        # depende de un artefacto propio, el detalle de tiro. Mezclada con el
        # bloque anterior, su ausencia completa quedaría enmascarada por cualquier
        # media de goles que sí estuviera. La subcadena que la separa es
        # `shot_xg_`, que captura `form_shot_xg_mean_3` y no `form_sot_3`: el
        # volumen de remate a puerta lo publica sofascore y viaja con el histórico
        # propio. No se exige, porque el volcado de tiro es largo y la cadena
        # tiene que poder predecir sin él; el flag de faltante del preprocesado
        # deja el hueco a la vista del modelo.
        _Bloque("forma (calidad de ocasión)",
                _cols_forma(contiene=("shot_xg_",)),
                TEAMSHOTS, max_nan=1.0, max_age=21, exigible=False),
        # La dureza del calendario recorrido encadena dos fuentes: el resultado
        # (máster) y el Elo pre-partido del RIVAL de cada jornada (ClubElo). Se
        # vigila aparte porque el gate declara vacío un partido solo cuando le
        # faltan TODAS las columnas del bloque: mezclada con la anterior, su
        # ausencia completa quedaría enmascarada por cualquier media de puntos que
        # sí estuviera, y el partido se puntuaría con un bloque entero imputado
        # sin que nada avisara. Es lo que queda del antiguo bloque «forma vs
        # rival»: el residuo de sorpresa respecto al Elo esperado ya no se
        # construye —medía lo mismo que el resultado ya ponderado por rival—, así
        # que el bloque se reduce al Elo medio de los rivales enfrentados.
        _Bloque("forma vs rival (dureza del calendario)",
                _cols_forma(contiene=("opp_elo_z",)),
                CLUBELO, max_nan=0.10, max_age=21, exigible=True,
                causa_de="Elo"),
        _Bloque("fatiga", tuple(C._FATIGUE), FATIGUE,
                max_nan=0.25, max_age=14, exigible=True),
        # El recuento es lo que queda de las bajas al restarle las dos lecturas
        # que las PESAN, cada una con su fuente propia. Del desglose por motivo
        # —lesión, sanción, duda— no hay nada que restar: el catálogo se queda
        # con el total y no construye las partes.
        _Bloque("bajas (recuento)",
                tuple(c for c in C._ABSENCES if "out_value" not in c
                      and "out_minshare" not in c),
                AVAIL, max_nan=1.0, max_age=7, exigible=False),
        _Bloque("bajas (valor €)",
                tuple(c for c in C._ABSENCES if "out_value" in c),
                MISSING_RAW, max_nan=1.0, max_age=7, exigible=False),
        # El peso por ROL de las bajas comparte el parte médico con el bloque
        # anterior, pero no la fuente que lo pondera: aquí es la tabla de minutos.
        # Se vigila por separado porque puede faltar por su cuenta —parte de bajas
        # publicado y minutos sin refrescar— y atribuir ese hueco a sofascore
        # mandaría a revisar el extractor equivocado. No es exigible: en las
        # primeras jornadas de una temporada la cuota no tiene denominador y el
        # bloque falta legítimamente para todos los partidos.
        _Bloque("bajas (rol: minutos)",
                tuple(c for c in C._ABSENCES if "out_minshare" in c),
                PLAYERMIN, max_nan=1.0, max_age=7, exigible=False),
        # Rotación y carga de minutos. A diferencia de las bajas, este bloque solo
        # necesita el pasado del equipo, así que es conocible con días de
        # antelación para cualquier partido del calendario; si falta, lo que ha
        # fallado es el refresco del extractor y no la publicación de la fuente.
        # Aun así no se exige para no bloquear la predicción de una jornada
        # entera por un bloque secundario, y las primeras jornadas de temporada
        # lo tienen legítimamente vacío.
        _Bloque("rotación y minutos", tuple(C._LINEUPS),
                LINEUPS, max_nan=1.0, max_age=7, exigible=False),
        # El contexto de calendario nace del sello de saque. En el máster jugado
        # lo aporta la tabla de sofascore; en un partido futuro solo existe en el
        # calendario crudo, así que su fuente de vigilancia es esa y no el máster.
        # Se exige porque la hora de una jornada se publica con días de antelación:
        # si falta, lo que ha fallado es el refresco del calendario. La hora exacta
        # ya no viaja como columna —el catálogo se queda con la franja, que es la
        # misma información agrupada—, de modo que el bloque se vigila con las dos
        # lecturas que sí se emiten.
        _Bloque("contexto de calendario",
                tuple(catalogo.CONTEXTO_CATEGORICO) + ("is_weekend",),
                FIXTURES, max_nan=0.10, max_age=7, exigible=True,
                causa_de="sello de saque"),
        # La geografía no depende de ninguna descarga: sale de la tabla de
        # estadios versionada en el repositorio. Su único modo de fallo es un club
        # que no figure en ella, y entonces falta para TODOS sus partidos, así que
        # la tolerancia es cero. `max_age` se fija alto a propósito: la fecha de
        # modificación del módulo no es una señal de caducidad porque las
        # coordenadas de un estadio no se refrescan.
        _Bloque("contexto geográfico", ("away_travel_km", "is_derby"),
                _HERE / "context.py", max_nan=0.0, max_age=36500, exigible=True),
        # La fuerza latente sale del marcador, de modo que su fuente es el máster
        # consolidado y no una descarga. Se vigila aun así, y con una tolerancia
        # DISTINTA de cero, porque tiene un modo de ausencia legítimo que la
        # clasificación y el head-to-head no tienen: un club sin historia
        # suficiente en la categoría —un recién ascendido en sus primeras
        # jornadas— no recibe fuerza, y como el bloque describe un enfrentamiento,
        # el partido entero se queda sin él. Con dos ascendidos estrenándose, dos
        # de los diez partidos de la jornada pueden carecer del bloque sin que
        # nada haya fallado; por encima de eso lo que falla es la consolidación
        # del máster, y entonces sí debe abortar.
        # El prefijo va solo, sin los de lado: el catálogo emite la fuerza como
        # diferencia (`strength_attack_diff`) y como probabilidad del partido
        # (`strength_p_*`), de modo que `home_strength_*` sería un patrón
        # huérfano que no casa con nada y que solo sirve para hacer creer que
        # vigila algo.
        _Bloque("fuerza latente", ("strength_*",),
                MASTER, max_nan=0.30, max_age=21, exigible=True),
    ]


# Un partido ya disputado hace más de este número de días y todavía ausente del
# máster delata que la consolidación no se ha re-ejecutado. No es un detalle
# cosmético: la forma, la clasificación y el h2h de la jornada que se va a
# predecir se calcularían ignorando esos resultados, es decir, con la foto de la
# semana pasada. Se toleran 2 días para no penalizar el partido de anoche.
_CONSOLIDATION_GRACE_DAYS = 2
_CONSOLIDATION_LOOKBACK_DAYS = 45


# --------------------------------------------------------------------------- #
# de-margen de cuotas (COPIA EXACTA de footballdata._implied_probs)
# --------------------------------------------------------------------------- #
# Se replica aquí (en vez de importar footballdata, que arrastra el extractor
# entero) para que las cuotas MANUALES pasen por la MISMA transformación que las
# automáticas: quita el margen de la casa y normaliza a prob. 1X2. Una cuota
# decimal válida es > 1.0 y el overround del trío tiene que caer en el rango de un
# mercado real; basura (0/neg/margen imposible) -> NA en toda la fila. Este control
# es especialmente relevante aquí: las cuotas manuales las teclea una persona.
_MIN_OVERROUND = 0.95
_MAX_OVERROUND = 1.60


def _implied_probs(h: pd.Series, d: pd.Series, a: pd.Series) -> pd.DataFrame:
    h, d, a = (pd.to_numeric(x, errors="coerce").where(lambda v: v > 1.0)
               for x in (h, d, a))
    ih, idr, ia = 1.0 / h, 1.0 / d, 1.0 / a
    s = ih + idr + ia
    valido = s.notna() & (s >= _MIN_OVERROUND) & (s <= _MAX_OVERROUND)
    out = pd.DataFrame({"mkt_p_home": ih / s, "mkt_p_draw": idr / s,
                        "mkt_p_away": ia / s})
    return out.where(valido, other=np.nan)


# --------------------------------------------------------------------------- #
# Puente FUTURO-consciente event_id -> match_key (desde el calendario crudo)
# --------------------------------------------------------------------------- #
def _fixtures_match_key(fx: pd.DataFrame) -> np.ndarray:
    """`match_key = fecha|home_key|away_key` del calendario, SIEMPRE re-derivado.

    Se construye en un único sitio porque el convenio es uno: dos
    reconstrucciones independientes del mismo convenio son dos oportunidades de
    que se desincronicen.

    La clave se deriva del **nombre** y no se lee la columna `team_key` que el
    crudo trae, aunque esté. Es la misma política que `fatigue.process` declara
    para sus propias claves —el crudo guarda nombres y las claves se derivan al
    reprocesar— y aquí hace falta por un motivo concreto: la clave almacenada es
    una copia en caché de algo que depende de la tabla de alias, y esa tabla
    cambia justo cuando aparece un club nuevo. Un ascendido se extrae el primer
    fin de semana de agosto con el alias todavía sin declarar, el crudo congela
    el slug de reserva (`real_racing` en lugar de `racing_santander`), y a partir
    de ahí ninguna re-extracción lo corrige: el CSV es acumulativo y salta por
    `row_id` lo ya guardado. Confiar en la columna almacenada dejaría al club
    ascendido sin puente de `event_id` ni sello de saque —features de calendario
    en NaN para él y solo para él— sin que nada diera error.

    Solo se recurre a la columna almacenada si falta el nombre, que es el único
    caso en que no hay nada mejor.
    """
    d = pd.to_datetime(fx["date"]).dt.strftime("%Y-%m-%d")
    mk = tn.match_key(d, fx["team"], fx["opponent"])
    mk = pd.Series(np.asarray(mk), index=fx.index)
    falta = fx["team"].isna() | fx["opponent"].isna()
    if falta.any() and {"team_key", "opp_key"} <= set(fx.columns):
        alt = (d + "|" + fx["team_key"].astype(str) + "|"
               + fx["opp_key"].astype(str))
        mk = mk.where(~falta, alt)
    return np.asarray(mk)


def _fixtures_local_rows(fixtures: pd.DataFrame) -> pd.DataFrame:
    """Filas del LOCAL de cada partido de LaLiga del calendario, con `match_key`.

    Es la vista base de la que salen los dos puentes con el calendario crudo (el
    de `event_id` y el del sello de saque).
    """
    fx = fixtures[fixtures["competition"].astype(str) == "LaLiga"]
    is_home = fx["is_home"].astype(str).str.strip().str.lower().isin(("true", "1"))
    home = fx[is_home].copy()
    if home.empty:
        return home.assign(match_key=pd.Series(dtype=object))
    home["match_key"] = _fixtures_match_key(home)
    return home


def _fixtures_event2key(fixtures: pd.DataFrame) -> pd.Series:
    """Serie `event_id -> match_key` que SÍ cubre los fixtures futuros.

    Se toma de `sofascore_team_fixtures.csv`, único crudo con event_id + fecha +
    equipos de partidos venideros.
    """
    home = _fixtures_local_rows(fixtures)
    if home.empty:
        return pd.Series(dtype=object)
    s = pd.Series(np.asarray(home["match_key"]), index=np.asarray(home["event_id"]))
    return s[~s.index.duplicated(keep="last")]


def _fixtures_kickoff_ts(fixtures: pd.DataFrame) -> pd.Series:
    """Serie `match_key -> sello de saque (s, UTC)` para los partidos venideros.

    En el máster jugado la hora de saque llega desde la tabla de partido de
    sofascore, que solo existe una vez disputado el encuentro. Para un partido
    futuro la única fuente es el calendario, que publica `start_ts` desde que la
    jornada se programa. Sin este puente, las features de calendario nacerían en
    NaN en TODA la matriz de inferencia mientras en entrenamiento están
    completas, que es exactamente el desajuste silencioso que el régimen de
    información está para evitar.
    """
    home = _fixtures_local_rows(fixtures)
    if home.empty or "start_ts" not in home.columns:
        return pd.Series(dtype="float64")
    s = pd.Series(pd.to_numeric(home["start_ts"], errors="coerce").values,
                  index=np.asarray(home["match_key"]))
    return s[~s.index.duplicated(keep="last")].dropna()


def _merge_block(fut: pd.DataFrame, block: pd.DataFrame) -> pd.DataFrame:
    """Left-merge de un bloque reducido (keyed match_key) sobre las filas futuras.

    Descarta en `fut` las columnas que el bloque va a aportar (evita _x/_y) y hace
    un left-merge: los partidos futuros sin dato en el bloque quedan NaN.
    """
    drop = [c for c in block.columns if c != "match_key" and c in fut.columns]
    return fut.drop(columns=drop).merge(block, on="match_key", how="left")


# --------------------------------------------------------------------------- #
# Cuotas manuales (plantilla CSV) + gate estricto
# --------------------------------------------------------------------------- #
def _apply_manual_odds(up: pd.DataFrame, manual_csv: Path) -> set[str]:
    """Rellena cuotas ausentes en `up` desde la plantilla manual. Muta `up`.

    Devuelve el conjunto de match_key cuya cuota se resolvió MANUALMENTE (para el
    log). Solo toca filas con `mkt_p_home` NaN (football-data no las trajo).
    """
    manual_csv = Path(manual_csv)
    applied: set[str] = set()
    need = up["mkt_p_home"].isna()
    if not need.any() or not manual_csv.exists():
        return applied

    man = pd.read_csv(manual_csv, dtype={"match_key": str})
    for col in ("odds_h", "odds_d", "odds_a"):
        if col not in man.columns:
            man[col] = np.nan
    man = man.dropna(subset=["match_key"])
    man = man[man[["odds_h", "odds_d", "odds_a"]].notna().all(axis=1)]
    if man.empty:
        return applied
    mmap = man.drop_duplicates("match_key", keep="last").set_index("match_key")

    for i in up.index[need]:
        mk = up.at[i, "match_key"]
        if mk not in mmap.index:
            continue
        h, d, a = (pd.to_numeric(mmap.at[mk, c], errors="coerce")
                   for c in ("odds_h", "odds_d", "odds_a"))
        probs = _implied_probs(pd.Series([h]), pd.Series([d]), pd.Series([a]))
        if probs["mkt_p_home"].isna().any():
            # La cuota teclada no es un precio 1X2 plausible (alguna <= 1.0, o un
            # overround imposible). Se rechaza y el partido sigue contando como
            # faltante, pero se dice en voz alta: si no, el usuario reescribiría la
            # misma cuota mala en la plantilla una y otra vez.
            print(f"[WARN] cuota manual rechazada en {mk}: "
                  f"{h} / {d} / {a} no forman un mercado 1X2 plausible "
                  f"(cada cuota > 1.0 y margen conjunto entre "
                  f"{_MIN_OVERROUND:.2f} y {_MAX_OVERROUND:.2f}).")
            continue
        up.at[i, "odds_avg_h"], up.at[i, "odds_avg_d"], up.at[i, "odds_avg_a"] = h, d, a
        up.at[i, "mkt_p_home"] = probs["mkt_p_home"].iloc[0]
        up.at[i, "mkt_p_draw"] = probs["mkt_p_draw"].iloc[0]
        up.at[i, "mkt_p_away"] = probs["mkt_p_away"].iloc[0]
        applied.add(mk)
    return applied


def promote_manual_odds(frame: pd.DataFrame, *,
                        path: Path | str = MANUAL_ODDS_HIST) -> int:
    """Promueve al registro histórico las cuotas que se tecleron para emitir.

    Sin este paso el mecanismo manual sería de un solo uso: la cuota resolvería
    la emisión de esta semana y la semana siguiente, al reconstruir el máster con
    el partido ya jugado, football-data impondría la suya. Teclear una vez basta
    porque lo que se teclea se guarda, y `footballdata.apply_manual_odds` lo
    vuelve a aplicar en cada reproceso.

    Se conserva la entrada MÁS ANTIGUA de cada partido (`keep="first"`), que es lo
    contrario de lo que haría un upsert normal y es deliberado: el sello
    `entered_ts` tiene que seguir siendo el del momento en que la cuota se vio,
    antes del partido. Si se sobreescribiera con el sello de una reejecución
    posterior al pitido final, el propio límite de `apply_manual_odds` rechazaría
    la fila y el override desaparecería en silencio justo cuando ya era historia.
    """
    path = Path(path)
    nuevo = frame.reindex(columns=fd.MANUAL_ODDS_COLS)
    prev = fd.read_manual_odds(path)
    juntos = nuevo if prev.empty else pd.concat([prev, nuevo], ignore_index=True)
    out = (juntos.dropna(subset=["match_key"])
           .drop_duplicates("match_key", keep="first")
           .sort_values("date"))
    path.parent.mkdir(parents=True, exist_ok=True)
    write_df_required(out[fd.MANUAL_ODDS_COLS], path, fmt="csv",
                      etiqueta="promote_manual_odds")
    return len(nuevo)


def _write_odds_template(missing: pd.DataFrame, manual_csv: Path) -> None:
    """Escribe/actualiza la plantilla de cuotas manuales con los partidos que faltan.

    Preserva las filas ya rellenadas por el usuario (merge por match_key) para que
    re-ejecutar no borre su trabajo, y añade filas vacías para los nuevos huecos.
    """
    manual_csv = Path(manual_csv)
    cols = ["match_key", "date", "home_team", "away_team", "odds_h", "odds_d", "odds_a"]
    tpl = missing[["match_key", "date", "home_team", "away_team"]].copy()
    for c in ("odds_h", "odds_d", "odds_a"):
        tpl[c] = np.nan

    if manual_csv.exists():
        prev = pd.read_csv(manual_csv, dtype={"match_key": str})
        filled = prev[prev[["odds_h", "odds_d", "odds_a"]].notna().any(axis=1)] \
            if set(("odds_h", "odds_d", "odds_a")) <= set(prev.columns) else prev.iloc[0:0]
        keep = filled[filled["match_key"].isin(set(tpl["match_key"]))]
        tpl = tpl[~tpl["match_key"].isin(set(keep["match_key"]))]
        tpl = pd.concat([keep.reindex(columns=cols), tpl.reindex(columns=cols)],
                        ignore_index=True)

    manual_csv.parent.mkdir(parents=True, exist_ok=True)
    tpl.reindex(columns=cols).sort_values("date").to_csv(manual_csv, index=False)


# Hueco de calendario que separa dos jornadas consecutivas, el mismo criterio y
# el mismo valor que `models/protocol._GAP_DIAS`: la `round` nominal de sofascore
# no sirve para esto —un aplazado rotulado «jornada 5» y jugado en febrero
# aparecería como jornada próxima— así que la jornada se delimita por el hueco.
_GAP_JORNADA = 3


def _solo_proxima_jornada(sofa: pd.DataFrame) -> pd.DataFrame:
    """Recorta el calendario sembrado a la PRIMERA jornada que venga.

    Hace falta porque las dos fuentes se autolimitan de forma muy distinta.
    `fixtures.csv` de football-data es una ventana de ~7 días, de modo que
    `_HORIZON_DAYS = 10` nunca llegaba a abarcar dos jornadas. El calendario de
    sofascore, en cambio, conoce la temporada entera, y con el mismo horizonte
    siembra la jornada siguiente Y la de después: diecinueve partidos en vez de
    diez, y los nueve de más son partidos para los que ninguna casa ha abierto
    mercado todavía. Teclear sus cuotas sería inventarlas.

    El corte se hace por el hueco de calendario y no por el número de jornada,
    por el mismo motivo que `protocol.effective_matchday`: la `round` nominal es
    una etiqueta y un aplazamiento la desordena.
    """
    if sofa.empty:
        return sofa
    d = pd.to_datetime(sofa["date"], errors="coerce")
    orden = d.sort_values()
    corte = orden.diff().dt.days.gt(_GAP_JORNADA)
    fin = orden[corte].iloc[0] if corte.any() else None
    return sofa if fin is None else sofa[d < fin]


def _seed_from_sofascore(up: pd.DataFrame, fixtures: pd.DataFrame | None,
                         played: pd.DataFrame, today: pd.Timestamp,
                         horizon_days: int, stale_days: int, *,
                         verbose: bool = True) -> pd.DataFrame:
    """Añade a `up` los partidos de la ventana que football-data todavía no lista.

    Resuelve el único caso en que la plantilla de cuotas manuales no servía de
    nada: football-data publica `fixtures.csv` como una ventana de pocos días y
    con el precio ya puesto, de modo que mientras no publica la jornada no hay
    NI fixture NI cuota. El gate de cuotas —que es quien escribe la plantilla—
    vive detrás del recorte de ventana, así que con `up` vacío se abortaba antes
    de llegar a él: no había fila de la que sembrar la plantilla y teclear la
    cuota a mano era imposible por mucho que el mecanismo existiera.

    El calendario de sofascore sí conoce la jornada con semanas de antelación, ya
    es dependencia de la cadena (del paso 1 de la rutina cuelgan fatiga, bajas y
    alineaciones) y ya se lee aquí para el gate de frescura y el informe de
    completitud. Las filas que aporta nacen SIN cuota a propósito: traen el
    fixture y nada más, y es el gate de cuotas el que las pone en la plantilla.

    Qué se siembra, y por qué tan poco:

    - solo `status == notstarted`: un aplazado o un cancelado no es un partido
      que venga, y un `finished` que el máster no tenga es un hueco de
      consolidación que ya denuncia el gate de frescura.
    - solo lo que no está en `played` ni ya en `up`: football-data manda cuando
      tiene el fixture, porque trae el precio real de las casas y la clave la
      derivan los dos del mismo convenio (`_fixtures_match_key`).

    La procedencia queda declarada en `fixture_src` para que el informe pueda
    distinguir un partido con cuota automática de uno que sigue a la espera de
    teclado; sin esa columna los dos se leerían igual en la matriz de salida.
    """
    lo = today - pd.Timedelta(days=stale_days)
    hi = today + pd.Timedelta(days=horizon_days)
    sofa = _sofa_future_window(fixtures, lo, hi)
    if sofa.empty:
        return up
    sofa = sofa[sofa["status"].astype(str) == "notstarted"]
    ya = set(up["match_key"]) | set(played["match_key"])
    sofa = sofa[~sofa["match_key"].isin(ya)]
    if sofa.empty:
        return up
    sofa = _solo_proxima_jornada(sofa)

    d = pd.to_datetime(sofa["date"], errors="coerce")
    yr = d.dt.year
    season_year = yr.where(d.dt.month >= 7, yr - 1).astype("Int64")
    nuevo = pd.DataFrame({
        "match_key": sofa["match_key"].values,
        "season": season_year.map(
            lambda y: f"{y}/{str(int(y) + 1)[2:]}" if pd.notna(y) else pd.NA).values,
        "season_year": season_year.values,
        "date": sofa["date"].values,
        "home_team": sofa["home"].values,
        "away_team": sofa["away"].values,
        "home_key": np.asarray(sofa["home"].map(tn.to_key)),
        "away_key": np.asarray(sofa["away"].map(tn.to_key)),
    })
    for c in up.columns:
        if c not in nuevo.columns:
            nuevo[c] = np.nan
    if "fixture_src" not in up.columns:
        up = up.assign(fixture_src="footballdata")
    nuevo["fixture_src"] = "sofascore"

    if verbose:
        print(f"\n  sembrados desde el calendario de sofascore "
              f"({len(nuevo)}): football-data no los lista todavía, así que "
              f"llegan SIN cuota y pasan a la plantilla manual")
        for _, r in nuevo.sort_values("date").iterrows():
            print(f"      {r['date']}  {r['home_team']} vs {r['away_team']}")

    nuevo = nuevo.reindex(columns=up.columns)
    # Concatenar con un frame vacío cambia los dtypes del resultado y pandas lo
    # avisa; cuando `up` está vacío —el caso que motiva toda esta función— no hay
    # nada que concatenar y la semilla ES el resultado.
    out = nuevo if up.empty else pd.concat([up, nuevo], ignore_index=True)
    return out.drop_duplicates("match_key", keep="first").reset_index(drop=True)


def _future_window(up: pd.DataFrame, today: pd.Timestamp, horizon_days: int,
                   stale_days: int, *, verbose: bool = True) -> pd.DataFrame:
    """Recorta `up` a la ventana [today - stale_days, today + horizon_days].

    Descarta por separado, y lo dice, las dos causas de exclusión: fixtures VIEJOS
    (ya disputados o abandonados en el parquet acumulativo, cuyo resultado podría
    conocerse ya) y fixtures DEMASIADO LEJANOS (sin alineaciones ni cuotas fiables).
    """
    if up.empty:
        return up
    lo = today - pd.Timedelta(days=stale_days)
    hi = today + pd.Timedelta(days=horizon_days)
    d = pd.to_datetime(up["date"], errors="coerce").dt.normalize()

    stale, far, bad = d < lo, d > hi, d.isna()
    keep = ~(stale | far | bad)

    if verbose and not keep.all():
        def _listar(mask, titulo, nota):
            if not mask.any():
                return
            print(f"  {titulo} ({int(mask.sum())}): {nota}")
            for _, r in up.loc[mask].sort_values("date").iterrows():
                print(f"      {r['date']}  {r.get('home_team', '?')} vs "
                      f"{r.get('away_team', '?')}")
        print(f"\n  ventana de predicción: {lo.date()} … {hi.date()} "
              f"(hoy={today.date()})")
        _listar(stale, "descartados por ANTIGUOS",
                "ya disputados o caducados en el parquet acumulativo; "
                "re-ejecuta la consolidación del máster")
        _listar(far, "descartados por LEJANOS",
                f"a más de {horizon_days} días; sin alineaciones ni cuotas estables")
        _listar(bad, "descartados por FECHA INVÁLIDA",
                "el fixture no trae fecha parseable")

    return up.loc[keep].reset_index(drop=True).copy()


# --------------------------------------------------------------------------- #
# construcción de las filas de partido futuro (esquema-máster, post-partido NaN)
# --------------------------------------------------------------------------- #
def _future_master_rows(up: pd.DataFrame, played: pd.DataFrame,
                        elo: pd.DataFrame, tv: pd.DataFrame,
                        fat: pd.DataFrame | None, av: pd.DataFrame | None,
                        fixtures: pd.DataFrame | None,
                        missing: pd.DataFrame | None = None,
                        pv: pd.DataFrame | None = None,
                        lin: pd.DataFrame | None = None,
                        pm: pd.DataFrame | None = None,
                        *,
                        motivos: dict | None = None,
                        strict_blocks: bool = True) -> pd.DataFrame:
    """Filas de partido futuro compatibles con el esquema del máster jugado.

    - Copia claves + cuotas/mercado (auto o manual ya resuelto) desde `up`.
    - Deja TODA columna post-partido en NaN (goles, sofa_*, xG, rating, ftr...).
    - Recupera del calendario el sello de saque `kickoff_ts`, que en el máster
      jugado llega con la tabla de partido y para un futuro no existe todavía.
    - Adjunta bloques pre-partido conocibles: Elo, valor plantilla, fatiga y bajas
      (tanto los RECUENTOS de `reduce_availability` como las bajas PONDERADAS por
      valor económico de `build_absences`). Los dos sub-bloques se enganchan aquí
      porque el máster de entrenamiento los tiene, y una feature presente en el
      entrenamiento y ausente en inferencia degradaría la predicción en silencio.

    Enganches que fallan
    --------------------
    Un enganche que lanza excepción NO es "el dato todavía no está publicado":
    es un reductor que se rompe, un puente `event_id -> match_key` que no casa o
    un esquema que ha cambiado. Absorberlo con un `print` dejaría el bloque en
    NaN, que es justo la degradación silenciosa que el preprocesador convierte
    después en un valor imputado creíble; por eso se propaga como
    `ScoringGateError` (`strict_blocks=False` lo degrada a aviso para
    exploración manual) y, en cualquier caso, la causa se anota en `motivos`
    para que el gate de cobertura pueda citarla.
    """
    motivos = motivos if motivos is not None else {}

    def _fallo_enganche(nombre: str, e: Exception) -> None:
        motivos[nombre] = str(e)
        if strict_blocks:
            raise ScoringGateError(
                f"[{nombre}] el enganche del bloque ha fallado: {e}. No es un "
                "hueco de calendario sino un error de la cadena de datos; se "
                "interrumpe la predicción en vez de dejar el bloque en NaN y "
                "que el preprocesador lo impute con la mediana de entrenamiento."
            ) from e
        print(f"[WARN] {nombre} no enganchado ({e}); queda NaN.")

    # armazón con TODAS las columnas del máster, en NaN
    fut = pd.DataFrame(np.nan, index=range(len(up)), columns=played.columns)
    up = up.reset_index(drop=True)
    for c in up.columns:
        if c in fut.columns:
            fut[c] = up[c].values
    fut["season_year"] = pd.to_numeric(fut["season_year"], errors="coerce").astype("Int64")

    # sello de saque desde el calendario (ver `_fixtures_kickoff_ts`): es lo que
    # alimenta el contexto de calendario, y en el armazón nace en NaN porque la
    # tabla de partido de sofascore aún no existe para un encuentro no disputado.
    if "kickoff_ts" in fut.columns and fixtures is not None:
        try:
            ts = _fixtures_kickoff_ts(fixtures)
            fut["kickoff_ts"] = fut["match_key"].map(ts).astype("float64")
            if fut["kickoff_ts"].isna().all():
                motivos.setdefault(
                    "sello de saque",
                    "el calendario crudo de sofascore no publica `start_ts` para "
                    "ninguno de los partidos de la ventana de predicción")
        except Exception as e:  # noqa: BLE001
            _fallo_enganche("sello de saque", e)

    # Elo vigente el día del partido (merge_asof backward), tipificado dentro de
    # la jornada. La base de tipificación se IMPONE con el censo de la temporada
    # completa —los equipos de los partidos ya jugados más los de la ventana que
    # se va a puntuar— y no se deja deducir de las filas que se pasan: `fut` trae
    # los cuatro o diez partidos de la próxima jornada, de modo que `add_clubelo`
    # tipificaría contra ocho o veinte equipos en lugar de contra los veinte de
    # Primera. La media saldría de otra liga y el modelo recibiría una columna en
    # una escala distinta de la que aprendió, que es el sesgo entrenamiento-
    # servicio en su forma más difícil de ver: los valores serían plausibles.
    if elo is not None:
        try:
            censo: dict[int, list[str]] = {}
            for src in (played, fut):
                for s, ks in censo_por_temporada(src).items():
                    censo[s] = sorted(set(censo.get(s, ())) | set(ks))
            elo_add = add_clubelo(
                fut[["match_key", "home_key", "away_key", "date",
                     "season_year"]].copy(), elo, censo=censo)
            for c in ("home_elo", "away_elo", "elo_diff",
                      "home_elo_z", "away_elo_z", "elo_via", "elo_base_n"):
                if c in fut.columns and c in elo_add.columns:
                    fut[c] = elo_add[c].values
        except Exception as e:  # noqa: BLE001
            _fallo_enganche("Elo", e)

    # valor de plantilla de la temporada en curso (prior de fuerza)
    if tv is not None:
        try:
            meta = fut[["match_key", "season_year", "home_key", "away_key"]].rename(
                columns={"season_year": "season"})
            meta["season"] = pd.to_numeric(meta["season"], errors="coerce").astype("Int64")
            sv = reduce_squad_value(meta, tv)
            fut = _merge_block(fut, sv)
        except Exception as e:  # noqa: BLE001
            _fallo_enganche("valor de plantilla", e)

    # fatiga / bajas vía puente futuro-consciente event_id -> match_key
    e2k = _fixtures_event2key(fixtures) if fixtures is not None else pd.Series(dtype=object)
    if e2k.empty:
        # Sin puente event_id -> match_key no hay forma de pegar fatiga ni bajas:
        # se deja constancia para que el gate de cobertura no atribuya el hueco a
        # la fuente equivocada.
        motivos.setdefault(
            "puente event_id",
            "el calendario crudo de sofascore no aporta fixtures de LaLiga con "
            "clave de local; fatiga y bajas no pueden enlazarse")
    if fat is not None and not e2k.empty:
        try:
            rfat = reduce_fatigue(fat, e2k)
            keep = [c for c in rfat.columns if c == "match_key" or c in C._FATIGUE]
            fut = _merge_block(fut, rfat[keep])
        except Exception as e:  # noqa: BLE001
            _fallo_enganche("fatiga", e)
    if av is not None and not e2k.empty:
        try:
            rav = reduce_availability(av, e2k)
            keep = [c for c in rav.columns if c == "match_key" or c in C._ABSENCES]
            fut = _merge_block(fut, rav[keep])
        except Exception as e:  # noqa: BLE001
            _fallo_enganche("bajas (recuento)", e)

    # bajas PONDERADAS: mismo cálculo que en el máster (`consolidate/absences.py`),
    # con el mismo lag t-1 en los valores de Transfermarkt y el mismo instante de
    # corte (00:00 UTC del día del partido) para la cuota de minutos. Si sofascore
    # aún no ha publicado el parte médico, `build_absences` devuelve el bloque en
    # NaN (hueco honesto, no ceros); si lo que falta es la tabla de minutos, solo
    # queda en NaN el sub-bloque de rol y el de valor se calcula igual.
    if missing is not None and pv is not None and not e2k.empty:
        try:
            abs_meta = meta.dropna(subset=["season"]).copy()
            abs_meta["season"] = abs_meta["season"].astype(int)
            abs_meta["date"] = fut.set_index("match_key")["date"].reindex(
                abs_meta["match_key"]).to_numpy()
            rabs = build_absences(missing, pv, abs_meta, e2k, tv, player_minutes=pm)
            keep = [c for c in rabs.columns if c == "match_key" or c in C._ABSENCES]
            fut = _merge_block(fut, rabs[keep])
        except Exception as e:  # noqa: BLE001
            _fallo_enganche("bajas (valor €)", e)

    # rotación y carga de minutos. Es el único bloque de sofascore que NO depende
    # de que la fuente publique nada del partido a predecir: se calcula mirando
    # solo hacia atrás, de modo que basta con que el extractor esté al día. El
    # puente hasta `match_key` es el mismo calendario que usan fatiga y bajas.
    if lin is not None and not e2k.empty:
        try:
            rlin = reduce_lineups(lin, e2k)
            keep = [c for c in rlin.columns if c == "match_key" or c in C._LINEUPS]
            fut = _merge_block(fut, rlin[keep])
        except Exception as e:  # noqa: BLE001
            _fallo_enganche("rotación y minutos", e)

    return fut


# --------------------------------------------------------------------------- #
# orquestación de la ruta de inferencia
# --------------------------------------------------------------------------- #
def build_scoring(up: pd.DataFrame | None = None,
                  played: pd.DataFrame | None = None,
                  elo: pd.DataFrame | None = None,
                  tv: pd.DataFrame | None = None,
                  fat: pd.DataFrame | None = None,
                  av: pd.DataFrame | None = None,
                  fixtures: pd.DataFrame | None = None,
                  missing: pd.DataFrame | None = None,
                  pv: pd.DataFrame | None = None,
                  lin: pd.DataFrame | None = None,
                  pm: pd.DataFrame | None = None,
                  *,
                  manual_odds_csv: Path | str = MANUAL_ODDS,
                  strict_odds: bool = True,
                  strict_blocks: bool = True,
                  strict_freshness: bool = True,
                  strict_coverage: bool = True,
                  today: pd.Timestamp | None = None,
                  horizon_days: int = _HORIZON_DAYS,
                  stale_days: int = _STALE_DAYS,
                  verbose: bool = True) -> pd.DataFrame:
    """Features de los partidos futuros (split='predict'), keyed por match_key.

    Engancha todo lo pre-partido conocible (cuotas auto/manual, Elo, valor
    plantilla, fatiga, bajas por recuento y bajas ponderadas por valor).

    Gates que pueden interrumpir la construcción, en el orden en que se aplican:
      1. LECTURA   `_leer_fuente`: artefacto exigible ausente, o artefacto
         ilegible (dañado) sea o no exigible -> `ScoringGateError`.
      2. FRESCURA  `_freshness_gate`: hay partidos ya jugados sin consolidar en
         el máster -> `ScoringGateError` (la forma sería la de la semana pasada).
      3. CUOTAS    gate preexistente: algún partido sin cuota -> se escribe la
         plantilla manual y `MissingOddsError`.
      4. ENGANCHE  un reductor que revienta -> `ScoringGateError` (no es un hueco
         de calendario, es la cadena de datos rota).
      5. COBERTURA `_coverage_gate`: un bloque exigible vacío en más partidos de
         los tolerados -> `ScoringGateError`.
    Los cuatro `strict_*` permiten degradar cada gate a aviso para exploración
    manual; la ruta de predicción (`build_predict.py`) los deja todos activos.

    `horizon_days` / `stale_days` delimitan la ventana de predicción alrededor de
    `today` (ver `_future_window`).
    """
    motivos: dict[str, str] = {}
    played = played if played is not None else pd.read_parquet(MASTER)
    up = up if up is not None else pd.read_parquet(UPCOMING)
    # Elo y valor de plantilla son exigibles (ver `_politica_bloques`): si el
    # artefacto falta o está dañado se corta aquí con el motivo, en vez de dejar
    # que reviente más adelante con un `FileNotFoundError` sin contexto.
    elo = elo if elo is not None else _try_read(
        CLUBELO, "Elo", exigible=strict_coverage, motivos=motivos)
    tv = tv if tv is not None else _try_read(
        SQUADVAL, "valor de plantilla", exigible=strict_coverage, motivos=motivos)
    # fatiga y bajas son exigibles en distinto grado (ver `_politica_bloques`),
    # pero su LECTURA se pide siempre igual: lo que decide si la ausencia corta
    # es la política del bloque, no el punto en que se abre el fichero.
    fat = fat if fat is not None else _try_read(
        FATIGUE, "fatiga", exigible=strict_coverage, motivos=motivos)
    av = av if av is not None else _try_read(
        AVAIL, "bajas (recuento)", exigible=False, motivos=motivos)
    fixtures = fixtures if fixtures is not None else _try_read_csv(
        FIXTURES, "calendario sofascore", exigible=False, motivos=motivos)
    missing = missing if missing is not None else _try_read_csv(
        MISSING_RAW, "bajas (valor €)", exigible=False, motivos=motivos)
    pv = pv if pv is not None else _try_read(
        PLAYERVAL, "valores de jugador", exigible=False, motivos=motivos)
    lin = lin if lin is not None else _try_read(
        LINEUPS, "rotación y minutos", exigible=False, motivos=motivos)
    pm = pm if pm is not None else _try_read(
        PLAYERMIN, "bajas (rol: minutos)", exigible=False, motivos=motivos)
    today = pd.Timestamp(today) if today is not None else pd.Timestamp.now().normalize()

    # 0) FRESCURA: antes que nada, ¿el máster refleja lo ya jugado? Si no, todas
    #    las features de forma nacerían viejas y ningún gate posterior lo vería.
    _freshness_gate(played, fixtures, today, strict=strict_freshness,
                    verbose=verbose)

    # 1) quedarnos solo con los fixtures GENUINAMENTE futuros: ni consolidados en el
    #    máster ni fuera de la ventana temporal de predicción.
    n0 = len(up)
    up = up[~up["match_key"].isin(set(played["match_key"]))].reset_index(drop=True).copy()
    up = _future_window(up, today, horizon_days, stale_days, verbose=verbose)
    # 1b) SEMILLA: los partidos que el calendario de sofascore ya conoce y
    #     football-data todavía no lista. Llegan sin cuota y van a la plantilla
    #     manual del paso 3; sin esto, una jornada sin precio publicado abortaba
    #     aquí mismo y no había fila de la que sembrar la plantilla.
    up = _seed_from_sofascore(up, fixtures, played, today, horizon_days,
                              stale_days, verbose=verbose)
    if up.empty:
        raise RuntimeError(
            f"No hay partidos que puntuar en la ventana [{today.date()} -"
            f"{stale_days}d, +{horizon_days}d]: de los {n0} fixtures de "
            "footballdata_upcoming.parquet, ninguno queda pendiente y dentro de "
            "plazo, y el calendario de sofascore no declara ningún partido de "
            "LaLiga `notstarted` en la ventana. Ejecuta `python "
            "scripts/extractors/footballdata.py upcoming` y `python "
            "scripts/extractors/sofascore.py weekly` para refrescar el "
            "calendario.")

    # 2) resolver cuotas: automáticas (ya vienen en `up`) + manuales de plantilla
    for c in ("mkt_p_home", "odds_avg_h", "odds_avg_d", "odds_avg_a"):
        if c not in up.columns:
            up[c] = np.nan
    auto_ok = set(up.loc[up["mkt_p_home"].notna(), "match_key"])
    manual_ok = _apply_manual_odds(up, manual_odds_csv)

    # 2b) DIAGNÓSTICO DE COMPLETITUD: cruza la ventana de football-data (lo que se
    #     VA a predecir) contra el calendario de sofascore (con status) y explica
    #     qué partidos futuros habrá y cuáles no (aplazados / cancelados / no
    #     listados). Es puramente informativo y NO cambia lo que se puntúa.
    if verbose:
        _completeness_report(up, fixtures, played, today)

    # 3) GATE ESTRICTO de cuotas
    still_missing = up[up["mkt_p_home"].isna()]
    if strict_odds and not still_missing.empty:
        _write_odds_template(still_missing, manual_odds_csv)
        lines = "\n".join(
            f"    - {r.date}  {r.home_team} vs {r.away_team}"
            for r in still_missing.itertuples())
        raise MissingOddsError(
            f"{len(still_missing)} partido(s) futuro(s) SIN cuota (football-data no "
            f"las publica todavía):\n{lines}\n\n"
            f"He escrito una plantilla en:\n    {Path(manual_odds_csv)}\n"
            "Rellena las columnas odds_h / odds_d / odds_a con las cuotas 1X2 que "
            "veas en tu casa de apuestas y vuelve a ejecutar. NO se ha escrito "
            "features_upcoming (no se predice sin cuota).")

    # 4) filas futuras esquema-máster con todos los bloques pre-partido
    fut = _future_master_rows(up, played, elo, tv, fat, av, fixtures, missing, pv,
                              lin, pm,
                              motivos=motivos, strict_blocks=strict_blocks)
    future_keys = set(fut["match_key"])

    # 5) máster extendido = jugados + futuros; una sola pasada de features compartida.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        extended = pd.concat([played, fut], ignore_index=True)
    assert_unique(extended, ["match_key"], "scoring.extended")

    feat = build_features(extended)
    feat.loc[feat["match_key"].isin(future_keys), C.SPLIT] = "predict"
    out = feat[feat[C.SPLIT] == "predict"].reset_index(drop=True)
    assert_unique(out, ["match_key"], "scoring.out")

    # 6) GATE DE COBERTURA sobre la matriz ya construida: es el único punto donde
    #    se ve el efecto conjunto de las fuentes, los merges y las features.
    _coverage_gate(out, motivos, strict=strict_coverage, verbose=verbose)

    # 7) MATRIZ DE COBERTURA de la jornada, con la misma definición que la del set
    #    de entrenamiento: las fuentes se miden sobre las filas esquema-máster
    #    —donde un hueco solo puede significar que el extractor no trajo el dato—
    #    y los bloques sobre la matriz de features ya construida. Se adjunta al
    #    resultado en lugar de escribirse aquí porque `build_scoring` es una
    #    función de construcción y no un escritor: quien decide publicar es
    #    `main`, y la ruta de predicción puede reutilizarla sin tocar el disco.
    #
    #    Es el documento que permite responder, semanas después y sobre una
    #    predicción ya emitida, con qué información se hizo. Sin él, una
    #    predicción de una jornada con la fatiga a medias es indistinguible de
    #    una hecha con todo el dato, porque la imputación borra la diferencia.
    out.attrs["cobertura"] = coverage_matrix(fut, out)
    # Las cuotas tecleadas viajan adjuntas para que `main` las promueva al
    # registro histórico: se adjuntan en vez de escribirse aquí por el mismo
    # motivo que la cobertura —esta función construye y no escribe— y el sello
    # de entrada se pone AHORA, que es el instante en que se vieron, no en la
    # reconstrucción de la semana que viene.
    out.attrs["cuotas_manuales"] = (
        up.loc[up["match_key"].isin(manual_ok),
               ["match_key", "date", "home_team", "away_team",
                "odds_avg_h", "odds_avg_d", "odds_avg_a"]]
        .rename(columns={"odds_avg_h": "odds_h", "odds_avg_d": "odds_d",
                         "odds_avg_a": "odds_a"})
        .assign(entered_ts=pd.Timestamp.now().isoformat(timespec="seconds"))
        .reset_index(drop=True))

    if verbose:
        odds_src = {mk: "auto" for mk in auto_ok}
        odds_src.update({mk: "manual" for mk in manual_ok})
        _readiness_report(out, played, odds_src, today)
    return out


def _leer_fuente(path: Path, label: str, *, exigible: bool,
                 motivos: dict | None = None, csv: bool = False
                 ) -> pd.DataFrame | None:
    """Lee un artefacto de origen separando ausencia de corrupción.

    Capturar cualquier excepción y devolver `None` con un aviso —la lectura
    defensiva de manual— haría que un parquet corrupto y un parquet nunca generado
    produjeran exactamente el mismo efecto: el bloque entero en NaN y la ejecución
    adelante. Son dos situaciones distintas y merecen tratamiento distinto:

      - AUSENTE: el fichero no está. Es el estado normal de una fuente que aún no
        se ha extraído; se aborta solo si el bloque es exigible.
      - ILEGIBLE: el fichero está pero no se puede leer (truncado a mitad de una
        escritura, esquema roto, permiso denegado). Eso no es "todavía no hay
        dato", es un artefacto dañado, y se propaga SIEMPRE: continuar equivale a
        publicar una predicción cuya causa de degradación ya se conocía.

    En ambos casos el motivo se anota en `motivos` para que el gate de cobertura
    pueda explicar, aguas abajo, POR QUÉ un bloque salió vacío.
    """
    path = Path(path)
    if not path.exists():
        motivo = f"el artefacto no existe ({path})"
        if motivos is not None:
            motivos[label] = motivo
        if exigible:
            raise ScoringGateError(
                f"[{label}] {motivo}. Es un bloque exigible para predecir: "
                "ejecuta el extractor correspondiente y vuelve a lanzar la "
                "predicción. No se ha construido nada.")
        print(f"[WARN] {label}: {motivo}; su bloque quedará NaN.")
        return None
    try:
        return pd.read_csv(path) if csv else pd.read_parquet(path)
    except Exception as e:  # noqa: BLE001
        raise ScoringGateError(
            f"[{label}] el artefacto existe pero no se puede leer: {e}. "
            "Un fichero dañado no es un hueco de dato: regenéralo (o recupera "
            "el .bak que deja `write_df_safe`) antes de predecir.") from e


def _try_read(path: Path, label: str, *, exigible: bool = False,
              motivos: dict | None = None) -> pd.DataFrame | None:
    return _leer_fuente(path, label, exigible=exigible, motivos=motivos)


def _try_read_csv(path: Path, label: str, *, exigible: bool = False,
                  motivos: dict | None = None) -> pd.DataFrame | None:
    return _leer_fuente(path, label, exigible=exigible, motivos=motivos, csv=True)


# --------------------------------------------------------------------------- #
# Diagnóstico de completitud de la ventana (cross-check football-data vs sofascore)
# --------------------------------------------------------------------------- #
# El pipeline es DIRIGIDO POR FIXTURE: se predice EXACTAMENTE lo que football-data
# lista como próximo (su fixtures.csv es una ventana rodante de ~7 días de TODAS
# las ligas, filtrada a LaLiga). NUNCA se asume "una jornada = 10 partidos": si un
# partido está aplazado sale de la ventana y no se predice; si se adelanta uno de
# otra jornada, aparece y se predice; en una semana reducida (p.ej. Supercopa)
# solo aparecen los que football-data programa. El número de jornada NO existe en
# los fixtures futuros de ninguna fuente actual, así que no se usa lógica por
# jornada; en su lugar se CRUZA la ventana contra el calendario de sofascore, que
# sí trae `status` (notstarted / postponed / canceled), para que el usuario sepa
# qué partidos habrá y cuáles no —sin depender de un conteo nominal.
_SOFA_SCHEDULED = {"notstarted", "inprogress"}
_SOFA_OFF = {"postponed", "canceled", "cancelled", "suspended", "abandoned"}


def _sofa_future_window(fixtures: pd.DataFrame | None,
                        win_min: pd.Timestamp, win_max: pd.Timestamp) -> pd.DataFrame:
    """Fixtures LaLiga de sofascore en [win_min, win_max], keyed como el máster.

    Devuelve match_key (fecha|home_key|away_key), fecha, equipos y `status`
    normalizado. Es el calendario "verdad" independiente para el cross-check.
    """
    empty = pd.DataFrame(columns=["match_key", "date", "home", "away", "status"])
    if fixtures is None or len(fixtures) == 0:
        return empty
    fx = fixtures.copy()
    fx = fx[fx["competition"].astype(str) == "LaLiga"]
    is_home = fx["is_home"].astype(str).str.strip().str.lower().isin(("true", "1"))
    fx = fx[is_home].copy()
    if fx.empty:
        return empty
    dnorm = pd.to_datetime(fx["date"], errors="coerce").dt.normalize()
    fx = fx[(dnorm >= win_min) & (dnorm <= win_max)]
    if fx.empty:
        return empty
    ds = pd.to_datetime(fx["date"]).dt.strftime("%Y-%m-%d")
    mk = _fixtures_match_key(fx)
    status = (fx["status"].astype(str).str.strip().str.lower()
              if "status" in fx.columns else pd.Series("", index=fx.index))
    out = pd.DataFrame({
        "match_key": np.asarray(mk),
        "date": ds.values,
        "home": fx["team"].values,
        "away": fx["opponent"].values,
        "status": np.asarray(status),
    })
    return out.drop_duplicates("match_key", keep="last").reset_index(drop=True)


def _completeness_report(up: pd.DataFrame, fixtures: pd.DataFrame | None,
                         played: pd.DataFrame, today: pd.Timestamp) -> None:
    """Explica qué partidos futuros SE van a predecir y cuáles no, y por qué.

    Cruza la ventana de football-data (lo que se puntúa) contra el calendario de
    sofascore (con status). Informa de: partidos que se predicen (confirmados o
    con status raro), partidos que sofascore programa pero football-data aún no
    trae (posible hueco), aplazados/cancelados de la ventana (esperado que falten)
    y conflictos football-data↔sofascore. NO usa número de jornada.
    """
    print("=" * 78)
    print("SCORING · COMPLETITUD DE LA VENTANA (football-data vs sofascore)")
    print("=" * 78)
    if up.empty:
        print("  (no hay partidos futuros en football-data)")
        return

    dates = pd.to_datetime(up["date"], errors="coerce").dt.normalize()
    win_min, win_max = dates.min(), dates.max()
    # No escanear sofascore más atrás de una semana antes de hoy: si quedara una
    # fila vieja en la ventana de football-data, evita arrastrar TODO el histórico
    # de aplazamientos/cancelaciones al cross-check.
    win_min = max(win_min, today - pd.Timedelta(days=7))
    fd_keys = set(up["match_key"])
    print(f"  ventana football-data: {win_min.date()} … {win_max.date()}  "
          f"({len(fd_keys)} partido(s) a predecir)")
    print("  regla: se predice lo que football-data programa; NO se asume nº fijo "
          "de partidos por jornada.")

    sofa = _sofa_future_window(fixtures, win_min, win_max)
    if sofa.empty:
        print("\n  sofascore: sin fixtures LaLiga en esta ventana -> cross-check no "
              "disponible.")
        print("  Se predicen los partidos de football-data tal cual (sin verificación "
              "cruzada de status).")
        return

    sofa_by_key = sofa.set_index("match_key")
    sched_keys = set(sofa.loc[sofa["status"].isin(_SOFA_SCHEDULED), "match_key"])
    off = sofa[sofa["status"].isin(_SOFA_OFF)]
    off_keys = set(off["match_key"])

    # 1) partidos que SÍ se predicen (están en football-data)
    print(f"\n  [OK] SE PREDICEN ({len(fd_keys)}):")
    for _, r in up.sort_values("date").iterrows():
        mk = r["match_key"]
        if mk in sofa_by_key.index:
            st = sofa_by_key.at[mk, "status"]
            note = ("confirmado (sofascore=notstarted)" if st in _SOFA_SCHEDULED
                    else f"[!] sofascore='{st}'")
        else:
            note = "no listado en sofascore (revisar clave/fecha)"
        print(f"      {r['date']}  {r.get('home_team', '?')} vs "
              f"{r.get('away_team', '?')}   [{note}]")

    # 2) sofascore los programa pero football-data NO los trae → posible hueco
    miss = sched_keys - fd_keys
    if miss:
        print(f"\n  [!] SOFASCORE LOS PROGRAMA PERO FOOTBALL-DATA NO LOS TRAE ({len(miss)}):")
        for mk in sorted(miss, key=lambda k: sofa_by_key.at[k, "date"]):
            r = sofa_by_key.loc[mk]
            print(f"      {r['date']}  {r['home']} vs {r['away']}")
        print("      -> football-data suele publicar con ~7 días de antelación; puede que")
        print("        aún no los liste. Re-ejecuta el extractor `footballdata upcoming`")
        print("        más cerca del partido, o mete cuota manual si quieres predecirlos.")

    # 3) aplazados / cancelados en la ventana → NO se predicen (esperado)
    if not off.empty:
        print(f"\n  [i] APLAZADOS / CANCELADOS EN LA VENTANA ({len(off)}) — no se predicen:")
        for _, r in off.sort_values("date").iterrows():
            print(f"      {r['date']}  {r['home']} vs {r['away']}   [{r['status']}]")

    # 4) football-data lo lista pero sofascore lo da por aplazado/cancelado → conflicto
    conflict = fd_keys & off_keys
    if conflict:
        print(f"\n  [!] CONFLICTO: football-data los lista pero sofascore los da por "
              f"aplazado/cancelado ({len(conflict)}):")
        for mk in sorted(conflict, key=lambda k: sofa_by_key.at[k, "date"]):
            r = sofa_by_key.loc[mk]
            print(f"      {r['date']}  {r['home']} vs {r['away']}   "
                  f"[sofascore={r['status']}]")
        print("      -> verifica manualmente antes de fiarte de la predicción.")

    print(f"\n  Resumen: football-data={len(fd_keys)}  "
          f"sofascore_programados={len(sched_keys)}  "
          f"coincidentes={len(fd_keys & sched_keys)}  "
          f"solo_sofascore={len(miss)}  aplaz/canc={len(off_keys)}")


# --------------------------------------------------------------------------- #
# Gate de FRESCURA: ¿el máster refleja los partidos que ya se han jugado?
# --------------------------------------------------------------------------- #
def _edad_dias(path: Path, today: pd.Timestamp) -> float | None:
    """Días entre la última modificación del artefacto y `today` (None si falta)."""
    path = Path(path)
    if not path.exists():
        return None
    mtime = pd.Timestamp(path.stat().st_mtime, unit="s").normalize()
    return float((today.normalize() - mtime).days)


def _pendientes_de_consolidar(played: pd.DataFrame, fixtures: pd.DataFrame | None,
                              today: pd.Timestamp,
                              grace_days: int = _CONSOLIDATION_GRACE_DAYS,
                              lookback_days: int = _CONSOLIDATION_LOOKBACK_DAYS
                              ) -> pd.DataFrame:
    """Partidos de LaLiga ya disputados que todavía NO están en el máster.

    Es la medida honesta de la frescura del máster. Medirla como "días desde el
    último partido consolidado" no serviría: un parón por selecciones o el
    intervalo entre temporadas produce huecos de dos semanas o de dos meses que
    son perfectamente normales. Lo que sí delata un máster viejo es que el
    calendario de sofascore dé por disputados partidos que el máster no tiene.

    Se ignoran los aplazados/cancelados (nunca se jugaron) y los programados (aún
    no se han jugado), y se conceden `grace_days` para no señalar el partido de
    anoche, todavía sin consolidar por diseño.
    """
    vacío = pd.DataFrame(columns=["match_key", "date", "home", "away", "status"])
    if fixtures is None or len(fixtures) == 0:
        return vacío
    hi = today.normalize() - pd.Timedelta(days=grace_days)
    lo = today.normalize() - pd.Timedelta(days=lookback_days)
    sofa = _sofa_future_window(fixtures, lo, hi)
    if sofa.empty:
        return vacío
    jugados = sofa[~sofa["status"].isin(_SOFA_OFF | _SOFA_SCHEDULED)]
    if jugados.empty:
        return vacío
    pend = jugados[~jugados["match_key"].isin(set(played["match_key"]))]
    return pend.sort_values("date").reset_index(drop=True)


def _freshness_gate(played: pd.DataFrame, fixtures: pd.DataFrame | None,
                    today: pd.Timestamp, *, strict: bool = True,
                    verbose: bool = True) -> None:
    """Corta si el máster ignora partidos ya jugados; informa de la edad del resto.

    Un máster desactualizado no produce ningún NaN —todas las features de forma,
    clasificación y h2h salen con valor— pero ese valor es el de la semana
    pasada. Es la degradación más difícil de detectar aguas abajo y la que más
    directamente contamina la predicción, así que es el único criterio de
    frescura que ABORTA.

    La antigüedad de los ficheros de origen se reporta pero no corta: la fecha de
    modificación es una señal débil (clonar el repositorio o restaurar una copia
    la reinicia) y sirve para explicar una cobertura baja, no para provocar un
    fallo por sí misma.
    """
    pend = _pendientes_de_consolidar(played, fixtures, today)
    if verbose:
        print("=" * 78)
        print("SCORING · FRESCURA DE LOS ARTEFACTOS")
        print("=" * 78)
        if fixtures is None or len(fixtures) == 0:
            print("  sin calendario de sofascore: no se puede verificar si el "
                  "máster está al día (cross-check no disponible).")
        for b in _politica_bloques():
            edad = _edad_dias(b.fuente, today)
            if edad is None:
                print(f"  {b.nombre:22s}: artefacto ausente ({b.fuente.name})")
            else:
                marca = "  ⚠ por encima del umbral" if edad > b.max_age else ""
                print(f"  {b.nombre:22s}: {edad:>4.0f} d desde su última "
                      f"escritura (umbral {b.max_age:.0f} d){marca}")

    if pend.empty:
        return
    lineas = "\n".join(f"    - {r.date}  {r.home} vs {r.away}"
                       for r in pend.itertuples())
    msg = (f"MÁSTER DESACTUALIZADO: {len(pend)} partido(s) ya disputados no están "
           f"consolidados:\n{lineas}\n\n"
           "La forma, la clasificación y el head-to-head de la jornada a predecir "
           "se calcularían ignorándolos: no saldría ningún NaN, saldrían valores "
           "de la semana pasada. Re-ejecuta la cadena de consolidación "
           "(`python scripts/consolidate/build_master.py`) antes de predecir.")
    if strict:
        raise ScoringGateError(msg)
    print("[WARN] " + msg)


# --------------------------------------------------------------------------- #
# Gate de COBERTURA: ¿cuántos partidos se quedan sin cada bloque pre-partido?
# --------------------------------------------------------------------------- #
def _resolver_cols(out: pd.DataFrame, patrones, excluye=()) -> list[str]:
    """Columnas presentes que casan con nombres exactos o prefijos `pre*`.

    `excluye` se aplica por subcadena y DESPUÉS de resolver los patrones, de modo
    que un bloque pueda quedarse con el prefijo ancho y restarle las columnas que
    pertenecen a otro bloque por depender de otra fuente.
    """
    cols: list[str] = []
    for p in patrones:
        if p.endswith("*"):
            cols += [c for c in out.columns if c.startswith(p[:-1])]
        elif p in out.columns:
            cols.append(p)
    cols = [c for c in cols if not any(x in c for x in excluye)]
    return list(dict.fromkeys(cols))


def _coverage_gate(out: pd.DataFrame, motivos: dict | None = None, *,
                   strict: bool = True, verbose: bool = True) -> None:
    """Comprueba, bloque a bloque, cuántos partidos lo llevan ENTERO en NaN.

    Se mide por partido y no por celda a propósito: la media de NaN de un bloque
    diluye el caso que importa (un partido concreto al que no se le ha podido
    pegar la fatiga) entre columnas que sí están. Lo relevante para predecir es
    si a ESTE partido le falta ESTE bloque, no cuánto NaN hay en promedio.

    Un bloque cuyas columnas no existen siquiera en la matriz es un caso aparte y
    más grave: significa que la matriz de inferencia no tiene el mismo esquema
    que la de entrenamiento. Se trata como incumplimiento total.
    """
    motivos = motivos or {}
    infracciones: list[str] = []
    if verbose:
        print("=" * 78)
        print("SCORING · COBERTURA POR BLOQUE (partidos con el bloque completo)")
        print("=" * 78)

    for b in _politica_bloques():
        cols = _resolver_cols(out, b.patrones, b.excluye)
        if not cols:
            frac, detalle = 1.0, pd.DataFrame()
            estado = "SIN COLUMNAS en la matriz de inferencia"
        else:
            vacio = out[cols].isna().all(axis=1)
            frac = float(vacio.mean()) if len(out) else 0.0
            detalle = out.loc[vacio]
            estado = (f"{1 - frac:6.1%} de los partidos con dato "
                      f"({len(cols)} cols)")
        marca = "" if frac <= b.max_nan else (
            "   ✗ ABORTA" if b.exigible else "   ⓘ hueco tolerado")
        if verbose:
            print(f"  {b.nombre:22s}: {estado}"
                  f"   umbral vacío <={b.max_nan:.0%}{marca}")

        if frac <= b.max_nan or not b.exigible:
            continue
        lineas = "\n".join(
            f"      - {r.get('date', '?')}  {r.get('home_team', r.get('home_key', '?'))}"
            f" vs {r.get('away_team', r.get('away_key', '?'))}"
            for _, r in detalle.head(12).iterrows()) or "      (todos)"
        causa = (motivos.get(b.nombre)
                 or (motivos.get(b.causa_de) if b.causa_de else None)
                 or motivos.get("puente event_id"))
        infracciones.append(
            f"  · {b.nombre}: vacío en {frac:.0%} de los partidos "
            f"(máximo tolerado {b.max_nan:.0%}); fuente `{b.fuente.name}`"
            + (f"\n    causa registrada: {causa}" if causa else "")
            + f"\n{lineas}")

    if not infracciones:
        return
    msg = ("COBERTURA INSUFICIENTE en bloques exigibles para predecir:\n"
           + "\n".join(infracciones)
           + "\n\nEstos bloques SÍ son conocibles antes del partido, así que un "
             "hueco de este tamaño indica que la fuente no se ha refrescado o "
             "que el enlace con el fixture ha fallado; no es la ausencia legítima "
             "de un dato aún no publicado. Si se continuara, el preprocesador "
             "imputaría esas columnas con la mediana de entrenamiento y la "
             "predicción saldría sin ningún síntoma visible.")
    if strict:
        raise ScoringGateError(msg)
    print("[WARN] " + msg)


# --------------------------------------------------------------------------- #
# Log de readiness por partido (qué dato hay y qué no, en el momento de puntuar)
# --------------------------------------------------------------------------- #
def _readiness_report(out: pd.DataFrame, played: pd.DataFrame,
                      odds_src: dict, today: pd.Timestamp) -> None:
    print("=" * 78)
    print("SCORING · READINESS POR PARTIDO —", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 78)
    print(f"partidos a predecir={len(out)}  columnas={out.shape[1]}  hoy={today.date()}")
    print(f"último partido en el máster jugado: {pd.to_datetime(played['date']).max().date()}\n")

    def _has(row, cols):
        cols = [c for c in cols if c in out.columns]
        return bool(cols) and row[cols].notna().all()

    def _any(row, cols):
        cols = [c for c in cols if c in out.columns]
        return bool(cols) and row[cols].notna().any()

    out = out.sort_values("date").reset_index(drop=True)
    for _, row in out.iterrows():
        d2k = (pd.to_datetime(row["date"]) - today).days
        home = row.get("home_team", row.get("home_key", "?"))
        away = row.get("away_team", row.get("away_key", "?"))
        src = odds_src.get(row["match_key"], "MISSING")

        elo = "ok" if _has(row, C._ELO) else "NaN"
        sv = "ok" if _has(row, ["home_squad_value", "away_squad_value"]) else "NaN"
        fat_h = _any(row, [c for c in C._FATIGUE if c.startswith("home_")])
        fat_a = _any(row, [c for c in C._FATIGUE if c.startswith("away_")])
        fatiga = "ok" if (fat_h and fat_a) else ("parcial" if (fat_h or fat_a) else "NaN")

        nh = [c for c in ("home_n_missing", "home_n_out") if c in out.columns]
        na = [c for c in ("away_n_missing", "away_n_out") if c in out.columns]
        pub_h = bool(nh) and row[nh].notna().any()
        pub_a = bool(na) and row[na].notna().any()
        if pub_h or pub_a:
            hh = int(row["home_n_missing"]) if ("home_n_missing" in out.columns
                                                and pd.notna(row["home_n_missing"])) else "?"
            mm = int(row["away_n_missing"]) if ("away_n_missing" in out.columns
                                                and pd.notna(row["away_n_missing"])) else "?"
            bajas = f"publicadas ({hh} local / {mm} visit.)"
            if not (pub_h and pub_a):
                bajas += " [un solo lado]"
            if _has(row, ["home_out_value", "away_out_value"]):
                bajas += (f" · valor {row['home_out_value']:.0f}/"
                          f"{row['away_out_value']:.0f} M€")
            else:
                bajas += " · sin ponderar por valor"
        else:
            bajas = "sin publicar (NaN)"

        flag = "" if src != "MISSING" else "  <-- SIN CUOTA"
        print(f"  [{d2k:+3d}d] {row['date']}  {home} vs {away}{flag}")
        print(f"          cuota={src:<7} elo={elo:<3} valor_plantilla={sv:<3} "
              f"fatiga={fatiga:<7} bajas={bajas}")

    # Cobertura agregada por BLOQUE ANALÍTICO y por PROVEEDOR. Ninguna de las dos
    # se recompone aquí con patrones sobre los nombres: las dos se leen de la
    # tabla de procedencia, igual que en el QA del set de entrenamiento. Así el
    # informe de la jornada que se va a predecir usa exactamente la misma
    # partición de columnas que el informe del set con el que se entrenó, y una
    # comparación entre ambos significa algo.
    #
    # El desglose por proveedor es el que interesa en el momento de puntuar: si
    # un extractor no se ha ejecutado esta semana, el síntoma no se reparte por
    # bloques analíticos sino que se concentra en todas las columnas que dependen
    # de esa fuente, que es justo lo que esta segunda tabla agrupa.
    lin = feature_lineage()
    presentes = set(out.columns)

    def _cov(cols: list[str], label: str) -> None:
        cols = [c for c in cols if c in presentes]
        if not cols:
            print(f"  {label:26s}: (sin columnas)")
            return
        print(f"  {label:26s}: {out[cols].notna().mean().mean():6.1%}  "
              f"({len(cols)} cols)")

    print("\nCobertura agregada por bloque analítico:")
    for bloque, grupo in lin.groupby("bloque", sort=True):
        _cov(list(grupo["columna"]), bloque)

    print("\nCobertura agregada por proveedor de datos:")
    proveedores = sorted({p for fila in lin["proveedores"]
                          for p in fila.split(", ")})
    for prov in proveedores:
        cols = list(lin.loc[lin["proveedores"].str.contains(prov, regex=False),
                            "columna"])
        _cov(cols, prov)


def main(save: bool = True) -> pd.DataFrame:
    out = build_scoring()
    if save:
        cov = out.attrs.get("cobertura")
        manual = out.attrs.get("cuotas_manuales")
        # `out.attrs` lleva adjuntos dos DataFrame —la matriz de cobertura y las
        # cuotas tecleadas— y pandas vuelca `attrs` a los metadatos del parquet
        # como JSON, de modo que escribir `out` con ellos dentro aborta con
        # «Object of type DataFrame is not JSON serializable». Es el mismo fallo
        # que `models/build_models._escribe` ya evita, y aquí no se había visto
        # porque la emisión nunca había llegado a escribirse: el gate de cuotas
        # corta antes mientras football-data no publique la jornada. Se vacían
        # DESPUÉS de recogerlos y antes de la primera escritura.
        out.attrs.clear()
        write_df_required(out, OUT, fmt="parquet", etiqueta="build_scoring")
        print(f"\n[OK] scoring -> {OUT} ({len(out)} filas, {out.shape[1]} cols)")
        if cov is not None:
            write_df_required(cov, COVERAGE_UPCOMING_OUT, fmt="parquet",
                              etiqueta="build_scoring")
            print(f"[OK] cobertura de la jornada -> {COVERAGE_UPCOMING_OUT} "
                  f"({len(cov)} filas, {cov.shape[1]} cols)")
        if manual is not None and not manual.empty:
            n = promote_manual_odds(manual)
            print(f"[OK] cuotas manuales promovidas al histórico -> "
                  f"{MANUAL_ODDS_HIST} ({n} partido(s))")
    else:
        print("\n[DRY] no se ha escrito nada.")
    return out


if __name__ == "__main__":
    main(save="--dry" not in sys.argv)
