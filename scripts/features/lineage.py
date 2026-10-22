"""PROCEDENCIA y COBERTURA: de qué fuente sale cada columna y desde cuándo existe.

Un conjunto de features construido sobre seis proveedores distintos plantea dos
preguntas que la memoria tiene que poder responder por escrito y sin ambigüedad:

  1. ¿DE DÓNDE SALE cada columna? No basta con decir "de SofaScore" a nivel de
     bloque, porque dentro de una misma familia conviven procedencias distintas:
     `form_npxg_ewm_diff` viene de Understat, `form_rating_ewm_diff` de
     SofaScore, `home_form_points_ewm` de football-data y `form_opp_elo_z_ewm_diff`
     de ClubElo, y las cuatro son "forma". La trazabilidad, por tanto, se
     resuelve a nivel de ATRIBUTO —la magnitud que se mide— y no de módulo ni de
     familia.

  2. ¿DESDE CUÁNDO existe? Las fuentes no arrancan a la vez: la disponibilidad y
     el valor económico de las bajas empiezan en 2017/18 y las cuotas de cierre
     no aparecen hasta 2019/20. Un modelo que ignore ese hecho entrena sobre
     temporadas en las que media docena de columnas son, en realidad, el valor
     que les puso la imputación.

Este módulo responde a las dos con artefactos reproducibles, no con una tabla
escrita a mano que se desincroniza al primer cambio:

  * `feature_lineage()`  -> una fila por columna del set de features, con su
    bloque analítico, el módulo que la emite, el atributo que mide, cómo está
    parametrizada, de qué lado del partido habla, su naturaleza, sus fuentes y
    el régimen de información al que pertenece.
  * `coverage_matrix()`  -> una fila por partido, con un indicador por bloque de
    fuente (medido sobre el MÁSTER) y otro por bloque analítico (medido sobre el
    set de features).

EL CONTRATO SE GENERA, NO SE TRANSCRIBE. Las filas de `feature_lineage` se
construyen a partir de las mismas constantes que usan los módulos de features
para emitir sus columnas, y `_verificar_contrato()` comprueba que los nombres
generados coinciden EXACTAMENTE con los que cada módulo declara en su
`*_feature_names()`. La consecuencia es la deseada: añadir un atributo a
`form.py` sin declarar su procedencia aquí no produce una tabla incompleta sino
un error inmediato. Lo único escrito a mano es el conocimiento irreducible —qué
proveedor mide cada magnitud—, que no se puede deducir de ningún sitio.

POR QUÉ LA COBERTURA DE FUENTE SE MIDE SOBRE EL MÁSTER Y NO SOBRE LAS FEATURES.
Una columna de forma está vacía en la primera jornada de un equipo porque no hay
partidos anteriores que promediar, no porque la fuente haya fallado. Si la
cobertura se midiera sobre el set de features, ese arranque en frío —que es una
propiedad deseada del diseño leak-safe— se contaría como un hueco de datos. Los
indicadores `fuente_*` se calculan por eso sobre las columnas crudas del máster,
donde un hueco solo puede significar que el dato no llegó. Los indicadores
`bloque_*`, medidos sobre las features, responden a la pregunta complementaria:
si el bloque analítico es calculable para ese partido concreto.

Uso:
    from lineage import feature_lineage, coverage_matrix, coverage_by_season
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import catalogo  # noqa: E402
import columns as C  # noqa: E402
from form import (  # noqa: E402
    _CUM_ATTRS, _ROLL_ATTRS, _VENUE_ATTRS, _VENUE_WINDOW,
    WINDOWS, form_feature_names)
from market_form import market_form_feature_names  # noqa: E402
from h2h import h2h_feature_names  # noqa: E402
from standings import standings_feature_names  # noqa: E402
from prematch_derived import prematch_derived_feature_names  # noqa: E402
from context import context_feature_names, stadium_table  # noqa: E402
from strength import strength_feature_names  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
LINEAGE_OUT = PROC / "feature_lineage.parquet"
COVERAGE_OUT = PROC / "coverage_matrix.parquet"
COVERAGE_UPCOMING_OUT = PROC / "coverage_upcoming.parquet"

# Columnas de la matriz de cobertura que identifican el partido.
_COV_KEYS = ["match_key", "season", "date", "home_team", "away_team"]


# --------------------------------------------------------------------------- #
# 1. Proveedores
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Proveedor:
    """Quién publica el dato original, antes de que el proyecto lo toque."""
    id: str
    nombre: str
    origen: str
    nota: str


PROVEEDORES: dict[str, Proveedor] = {
    "football_data": Proveedor(
        "football_data", "football-data.co.uk", "football-data.co.uk/spainm.php",
        "Resultado oficial y cuotas 1X2 de una decena de casas, en apertura y "
        "cierre. Es la fuente del objetivo y la única que cubre todo el periodo."),
    "understat": Proveedor(
        "understat", "Understat", "understat.com/league/La_liga",
        "Modelo de goles esperados propietario, más presión (PPDA) y entradas "
        "en zona de remate (deep). Cubre desde 2014/15."),
    "sofascore": Proveedor(
        "sofascore", "SofaScore", "api.sofascore.com",
        "Nota por equipo, estadística de partido, jornada y sello de saque. La "
        "estadística detallada se estabiliza a partir de 2017/18."),
    "clubelo": Proveedor(
        "clubelo", "ClubElo", "clubelo.com",
        "Rating Elo diario por club, comparable entre ligas y continuo en el "
        "tiempo, de modo que no se reinicia en agosto."),
    "transfermarkt": Proveedor(
        "transfermarkt", "Transfermarkt", "transfermarkt.es",
        "Valor de mercado por jugador y por plantilla. Es el único indicador de "
        "fuerza previo a que se juegue un solo partido de la temporada."),
    "repositorio": Proveedor(
        "repositorio", "tabla versionada en el repositorio", "features/context.py",
        "Sede, coordenadas y comunidad autónoma de los 31 clubes del periodo. "
        "Dato estable, de tamaño fijo y sin dependencia de red."),
}


# --------------------------------------------------------------------------- #
# 2. Bloques de fuente (unidad de cobertura)
# --------------------------------------------------------------------------- #
# Un bloque de fuente es el conjunto mínimo de columnas del máster que llega o
# no llega JUNTO. La distinción entre bloques de un mismo proveedor no es
# cosmética: football-data publica el resultado desde el primer día y las cuotas
# de cierre solo desde 2019/20, de modo que tratarlos como un bloque único
# escondería precisamente el hecho que justifica el diseño en dos regímenes.
@dataclass(frozen=True)
class BloqueFuente:
    id: str
    proveedor: str
    extractor: str
    artefacto: str
    granularidad: str
    columnas: tuple[str, ...]
    nota: str
    sin_master: bool = False      # el bloque no viaja como columna del máster


BLOQUES_FUENTE: tuple[BloqueFuente, ...] = (
    BloqueFuente(
        "resultado", "football_data", "extractors/footballdata.py",
        "footballdata_match.parquet", "partido",
        ("ftr", "fthg", "ftag"),
        "Resultado final y goles de cada equipo. De aquí sale el objetivo 1X2 y "
        "toda la forma basada en puntos y goles."),
    BloqueFuente(
        "cuotas_apertura", "football_data", "extractors/footballdata.py",
        "footballdata_match.parquet", "partido",
        ("odds_avg_h", "odds_avg_d", "odds_avg_a"),
        "Precio 1X2 en el momento en que el mercado abre. Es la única lectura "
        "del mercado conocible cuando se emite la predicción semanal."),
    BloqueFuente(
        "cuotas_cierre", "football_data", "extractors/footballdata.py",
        "footballdata_match.parquet", "partido",
        ("odds_close_avg_h", "odds_close_avg_d", "odds_close_avg_a"),
        "Último precio antes del saque. Solo existe a partir de 2019/20 y solo "
        "es conocible minutos antes del partido: bloque retrospectivo."),
    BloqueFuente(
        "xg", "understat", "extractors/understat.py",
        "understat_match.parquet", "partido",
        ("home_xg", "away_xg", "home_npxg", "away_npxg",
         "home_xpts", "away_xpts", "home_ppda", "away_ppda",
         "home_deep", "away_deep"),
        "Goles esperados, puntos esperados, presión y entradas en zona de "
        "remate. Mide el proceso del partido, no su desenlace."),
    BloqueFuente(
        "calidad_ocasion", "understat", "extractors/understat_shots.py",
        "team_shot_quality.parquet", "equipo-partido",
        ("home_shot_xg_mean", "away_shot_xg_mean",
         "home_shot_dist_mean", "away_shot_dist_mean",
         "home_shot_setpiece_xg_pct", "away_shot_setpiece_xg_pct",
         "home_shot_xg_max", "away_shot_xg_max"),
        "Forma de la distribución de remates: cuánto vale el disparo corriente, "
        "desde dónde se dispara, qué parte del peligro nace de estrategia y cuál "
        "fue la ocasión más clara. Es el detalle que el xG agregado esconde, "
        "porque un mismo total admite muchos remates malos o pocos buenos."),
    BloqueFuente(
        "elo", "clubelo", "extractors/clubelo.py",
        "clubelo_elo.parquet", "club-fecha",
        ("home_elo_z", "away_elo_z"),
        "Fuerza de club previa al partido, tipificada dentro de la jornada: "
        "cuántas desviaciones separan al equipo de la media de Primera ese día. "
        "Al ser continua entre temporadas, resuelve el arranque en frío que sufre "
        "cualquier acumulado de agosto. Viaja tipificada y no en nivel porque la "
        "serie publicada empalma las dos escalas de ClubElo y solo la posición "
        "relativa por fecha es comparable en todo el histórico."),
    BloqueFuente(
        "rating", "sofascore", "extractors/sofascore.py",
        "sofascore_team_match.parquet", "equipo-partido",
        ("home_sofa_rating", "away_sofa_rating"),
        "Nota agregada del equipo en el partido: una síntesis del rendimiento "
        "individual que ninguna otra fuente del proyecto ofrece."),
    BloqueFuente(
        "estadistica_partido", "sofascore", "extractors/sofascore.py",
        "sofascore_team_match.parquet", "equipo-partido",
        ("sofa_shots_total_shots_home", "sofa_shots_total_shots_away"),
        "Estadística detallada del encuentro (tiros, córners, ocasiones "
        "claras). Materia prima de la forma, nunca feature cruda.",),
    BloqueFuente(
        "calendario", "sofascore", "extractors/sofascore.py",
        "sofascore_team_match.parquet", "partido",
        ("kickoff_ts",),
        "Sello de saque y jornada. De él salen la hora local, la franja de "
        "emisión y el fin de semana."),
    BloqueFuente(
        "fatiga", "sofascore", "extractors/fatigue.py",
        "team_fatigue.parquet", "equipo-partido",
        ("home_fat_rest_days", "away_fat_rest_days"),
        "Descanso y congestión calculados sobre el calendario multicompetición "
        "del club, no solo sobre sus partidos de Liga."),
    BloqueFuente(
        "disponibilidad", "sofascore", "extractors/availability.py",
        "team_availability.parquet", "equipo-partido",
        ("home_n_missing", "away_n_missing"),
        "Recuento de ausencias por lesión, sanción y duda. Empieza en 2017/18."),
    BloqueFuente(
        "valor_plantilla", "transfermarkt", "extractors/transfermarkt.py",
        "team_squad_value.parquet", "club-temporada",
        ("home_squad_value", "away_squad_value"),
        "Valor agregado, edad media y tamaño de la plantilla. Prior de fuerza "
        "disponible antes de la primera jornada."),
    BloqueFuente(
        "bajas_euros", "transfermarkt", "extractors/transfermarkt.py",
        "transfermarkt_player_values.parquet", "jugador-temporada",
        ("home_out_value", "away_out_value"),
        "Valor económico de los ausentes, cruzando la lista de bajas de "
        "SofaScore con el valor por jugador. Pondera la baja por su importancia."),
    BloqueFuente(
        "rotacion_minutos", "sofascore", "extractors/lineups.py",
        "team_lineups.parquet", "equipo-partido",
        ("home_xi_continuity", "away_xi_continuity"),
        "Continuidad del once y minutos acumulados por los titulares, siempre "
        "sobre partidos anteriores del equipo. Dice si la congestión de "
        "calendario se ha repartido o se ha cargado sobre los mismos once."),
    BloqueFuente(
        "bajas_rol", "sofascore", "extractors/lineups.py",
        "player_minutes.parquet", "jugador-partido",
        ("home_out_minshare", "away_out_minshare"),
        "Cuota de minutos que el ausente venía disputando con su equipo. Pesa la "
        "baja por el papel que tenía, no por su precio de mercado."),
    BloqueFuente(
        "geografia", "repositorio", "features/context.py",
        "(tabla versionada en el módulo)", "club",
        ("home_key", "away_key"),
        "Sede, coordenadas y comunidad autónoma. Sostiene el desplazamiento del "
        "visitante y el derbi.",
        sin_master=True),
)

_FUENTE_POR_ID = {b.id: b for b in BLOQUES_FUENTE}


# --------------------------------------------------------------------------- #
# 3. Atributos: el conocimiento irreducible
# --------------------------------------------------------------------------- #
# Aquí, y solo aquí, se escribe a mano qué mide cada magnitud y de qué bloque de
# fuente procede. Todo lo demás —los cientos de nombres de columna— se genera.
@dataclass(frozen=True)
class Atributo:
    bloque: str
    modulo: str
    fuentes: tuple[str, ...]
    naturaleza: str          # observada | derivada | estimada
    descripcion: str


_OBS, _DER, _EST = "observada", "derivada", "estimada"
_M_FORM = "features/form.py"

# --- atributos de forma (form.py) --------------------------------------------
_ATTR_FORMA: dict[str, Atributo] = {
    "points": Atributo("forma", _M_FORM, ("resultado",), _DER,
                       "Puntos obtenidos en el partido (3 victoria, 1 empate, 0 derrota)."),
    "gf": Atributo("forma", _M_FORM, ("resultado",), _OBS, "Goles a favor."),
    "ga": Atributo("forma", _M_FORM, ("resultado",), _OBS, "Goles en contra."),
    "gd": Atributo("forma", _M_FORM, ("resultado",), _DER, "Diferencia de goles."),
    "xgd": Atributo("forma", _M_FORM, ("xg",), _DER, "Diferencia de goles esperados."),
    "npxg": Atributo("forma", _M_FORM, ("xg",), _OBS,
                     "Goles esperados a favor sin contar penaltis."),
    "npxga": Atributo("forma", _M_FORM, ("xg",), _OBS,
                      "Goles esperados en contra sin contar penaltis."),
    "xpts": Atributo("forma", _M_FORM, ("xg",), _OBS,
                     "Puntos esperados del partido según el modelo de Understat."),
    "ppda": Atributo("forma", _M_FORM, ("xg",), _OBS,
                     "Pases del rival permitidos por acción defensiva: a menor "
                     "valor, presión más alta."),
    "deep": Atributo("forma", _M_FORM, ("xg",), _OBS,
                     "Entradas de balón en los últimos veinte metros."),
    "rating": Atributo("forma", _M_FORM, ("rating",), _OBS,
                       "Nota agregada del equipo en el partido."),
    "sot": Atributo("forma", _M_FORM, ("estadistica_partido",), _OBS,
                    "Tiros a puerta: el volumen de presencia ofensiva."),
    "big_chances": Atributo("forma", _M_FORM, ("estadistica_partido",), _OBS,
                            "Ocasiones claras generadas: el volumen de peligro real."),
    "shot_xg_mean": Atributo("forma", _M_FORM, ("calidad_ocasion",), _DER,
                             "Gol esperado del remate medio: separa al equipo que "
                             "dispara mucho y mal del que dispara poco y bien."),
    "shot_xg_mean_against": Atributo("forma", _M_FORM, ("calidad_ocasion",), _DER,
                                     "Gol esperado del remate medio concedido: "
                                     "distingue al bloque que permite muchos "
                                     "disparos inocuos del que permite pocos y claros."),
    "opp_elo_z": Atributo("forma", _M_FORM, ("elo",), _OBS,
                          "Fuerza tipificada del rival al que se enfrentó, para "
                          "leer la forma en su contexto de dificultad."),
    "matches_prev": Atributo("forma", _M_FORM, ("resultado",), _DER,
                             "Partidos ya disputados en la temporada: da la "
                             "madurez del acumulado y detecta el arranque en frío."),
    "effectiveness": Atributo("forma", _M_FORM, ("resultado", "estadistica_partido"),
                              _DER,
                              "Puntos acumulados por tiro a puerta: eficiencia "
                              "en convertir presencia ofensiva en resultado."),
    "elo_mom_5_z": Atributo("forma", _M_FORM, ("elo",), _DER,
                            "Variación de la fuerza tipificada en los cinco "
                            "últimos partidos: tendencia, no nivel."),
    "playstyle": Atributo("forma", _M_FORM, ("resultado",), _DER,
                          "Perfil del equipo derivado del híbrido de diferencia "
                          "de goles.  [categórica]"),
}

# --- atributos de mercado y arrastre psicológico (market_form.py) -------------
_M_MKTFORM = "features/market_form.py"
_ATTR_MERCADO: dict[str, Atributo] = {
    "relative_perf": Atributo("psicologicas_mercado", _M_MKTFORM,
                              ("resultado", "cuotas_apertura"), _DER,
                              "Victorias reales sobre victorias esperadas por la "
                              "cuota: rendimiento relativo a la opinión del mercado."),
    "post_big_win": Atributo("psicologicas_mercado", _M_MKTFORM,
                             ("resultado", "cuotas_apertura"), _DER,
                             "El partido anterior fue una victoria inesperada. "
                             "Hipótesis de relajación posterior."),
}

# --- atributos de enfrentamiento directo (h2h.py) ----------------------------
_M_H2H = "features/h2h.py"
_ATTR_H2H: dict[str, Atributo] = {
    "win_rate": Atributo("head_to_head", _M_H2H, ("resultado",), _DER,
                         "Proporción de victorias en el historial contra ese rival."),
    "draw_rate": Atributo("head_to_head", _M_H2H, ("resultado",), _DER,
                          "Proporción de empates en el historial contra ese rival."),
    "gd": Atributo("head_to_head", _M_H2H, ("resultado",), _DER,
                   "Diferencia de goles media en el historial contra ese rival."),
}

# --- atributos de clasificación y trascendencia (standings.py) ---------------
_M_STAND = "features/standings.py"
_ATTR_STANDINGS: dict[str, Atributo] = {
    "prev_position": Atributo("clasificacion", _M_STAND, ("resultado",), _DER,
                              "Posición en la tabla antes de la jornada."),
    "position_zone": Atributo("clasificacion", _M_STAND, ("resultado",), _DER,
                              "Zona de la tabla: título, Europa, media o "
                              "descenso.  [categórica]"),
    "points_above_drop": Atributo("clasificacion", _M_STAND, ("resultado",), _DER,
                                  "Puntos de margen sobre el primer puesto de descenso."),
    "dynamic_pos_change": Atributo("clasificacion", _M_STAND, ("resultado",), _DER,
                                   "Puestos ganados o perdidos en las últimas jornadas."),
    "points_to_leader": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                                 "Distancia en puntos al líder."),
    "points_to_champions": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                                    "Distancia en puntos al último puesto de Champions."),
    "points_to_europa": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                                 "Distancia en puntos al último puesto europeo."),
    "matches_left": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                             "Partidos que restan por disputar."),
    "safe_from_drop": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                               "La salvación es ya matemática."),
    # `europe_unreachable` y `stakes_none` son INSUMOS: `standings.py` los calcula
    # para poder multiplicarlos en `stakes_none_match`, pero el catálogo no los
    # declara y por tanto `catalogo.lados` devuelve `()` para ambos, de modo que
    # `_desdoblar` no genera ninguna fila con su nombre. Se dejan aquí escritos
    # porque la tabla de procedencia también documenta de dónde sale lo que se
    # emite, y lo que se emite del par es su producto.
    "europe_unreachable": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                                   "Europa es ya matemáticamente inalcanzable."),
    "stakes_none": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                            "El equipo no se juega nada: ni desciende ni alcanza Europa."),
    "stakes_none_match": Atributo("trascendencia", _M_STAND, ("resultado",), _DER,
                                  "Ninguno de los dos equipos se juega nada. Es el "
                                  "producto de los dos indicadores, no su diferencia."),
}

# --- atributos derivados pre-partido (prematch_derived.py) -------------------
_M_DERIV = "features/prematch_derived.py"
_ATTR_DERIVADAS: dict[str, Atributo] = {
    "mkt_overround_open": Atributo("estructura_precio", _M_DERIV,
                                   ("cuotas_apertura",), _DER,
                                   "Margen de la casa en la apertura: cuánto suman "
                                   "las tres probabilidades implícitas por encima de uno."),
    "mkt_spread_mean": Atributo("estructura_precio", _M_DERIV,
                                ("cuotas_apertura",), _DER,
                                "Dispersión media de los tres signos: incertidumbre "
                                "agregada del mercado sobre el partido."),
    "sharp_div_home": Atributo("estructura_precio", _M_DERIV,
                               ("cuotas_apertura",), _DER,
                               "Divergencia entre Pinnacle y la media del mercado en "
                               "el signo local: dónde discrepa el dinero informado."),
    "sharp_div_draw": Atributo("estructura_precio", _M_DERIV,
                               ("cuotas_apertura",), _DER,
                               "Divergencia entre Pinnacle y la media en el empate."),
    "sharp_div_away": Atributo("estructura_precio", _M_DERIV,
                               ("cuotas_apertura",), _DER,
                               "Divergencia entre Pinnacle y la media en el signo visitante."),
    # Las tres probabilidades de Bet365 llevan bloque `servicio` y no
    # `estructura_precio` aunque las calcule el mismo módulo: son el patrón de
    # comparación contra el que se mide el modelo, no una entrada suya
    # (`columns.SERVICIO_DERIVADO`). Contarlas aparte es lo que permite que el
    # recuento de features del anexo diga cuántas variables ve el estimador.
    "mkt_p_b365_home": Atributo("servicio", _M_DERIV,
                                ("cuotas_apertura",), _DER,
                                "Probabilidad de victoria local según Bet365, ya sin "
                                "margen. Es referencia de evaluación, no variable del "
                                "modelo: el consenso ya incorpora este precio."),
    "mkt_p_b365_draw": Atributo("servicio", _M_DERIV,
                                ("cuotas_apertura",), _DER,
                                "Probabilidad de empate según Bet365, ya sin margen."),
    "mkt_p_b365_away": Atributo("servicio", _M_DERIV,
                                ("cuotas_apertura",), _DER,
                                "Probabilidad de victoria visitante según Bet365, sin "
                                "margen."),
    "close_move_fav": Atributo("movimiento_linea", _M_DERIV,
                               ("cuotas_apertura", "cuotas_cierre"), _DER,
                               "Movimiento del precio del favorito: la lectura "
                               "sintética de hacia dónde se ha ido el dinero."),
}

# --- atributos de contexto (context.py) --------------------------------------
_M_CTX = "features/context.py"
_ATTR_CONTEXTO: dict[str, Atributo] = {
    "kickoff_slot": Atributo("contexto_calendario", _M_CTX, ("calendario",), _DER,
                             "Franja de emisión: mediodía, tarde o noche.  [categórica]"),
    "is_weekend": Atributo("contexto_calendario", _M_CTX, ("calendario",), _DER,
                           "El saque cae en sábado o domingo."),
    "away_travel_km": Atributo("contexto_geografia", _M_CTX, ("geografia",), _DER,
                               "Distancia ortodrómica que recorre el visitante."),
    "is_derby": Atributo("contexto_geografia", _M_CTX, ("geografia",), _DER,
                         "Ambos clubes pertenecen a la misma comunidad autónoma."),
}

# --- atributos de fuerza latente (strength.py) -------------------------------
_M_STR = "features/strength.py"
_ATTR_FUERZA: dict[str, Atributo] = {
    "strength_attack": Atributo("fuerza_latente", _M_STR, ("resultado",), _EST,
                                "Capacidad de marcar estimada por el modelo de "
                                "goles, en escala logarítmica."),
    "strength_defence": Atributo("fuerza_latente", _M_STR, ("resultado",), _EST,
                                 "Capacidad de evitar goles estimada por el modelo, "
                                 "orientada para que más sea mejor."),
    "strength_p_home": Atributo("modelo_goles", _M_STR, ("resultado",), _EST,
                                "Probabilidad de victoria local que se deriva del "
                                "modelo de goles."),
    "strength_p_draw": Atributo("modelo_goles", _M_STR, ("resultado",), _EST,
                                "Probabilidad de empate que se deriva del modelo de goles."),
    "strength_p_away": Atributo("modelo_goles", _M_STR, ("resultado",), _EST,
                                "Probabilidad de victoria visitante que se deriva "
                                "del modelo de goles."),
}


# --------------------------------------------------------------------------- #
# 4. Bloque pre-partido de pasarela (columns.py)
# --------------------------------------------------------------------------- #
# Las columnas que el máster entrega ya listas y que el set de features
# transporta sin transformar, sean features (pasarela) o columnas de servicio
# (las cuotas crudas). Su descripción se GENERA a partir del nombre para las
# familias regulares —las cuotas 1X2 siguen una rejilla de casa por signo— y se
# escribe a mano solo donde la rejilla no aplica.
_M_PASS = "features/columns.py (pasarela)"

_CASA_CUOTA = {
    "b365": "Bet365, apertura",
    "ps": "Pinnacle, apertura",
    "max": "máxima del mercado, apertura",
    "avg": "media del mercado, apertura",
    "close_avg": "media del mercado, cierre",
    "close_max": "máxima del mercado, cierre",
    "b365_close": "Bet365, cierre",
}
_SIGNO_CUOTA = {"h": "victoria local", "d": "empate", "a": "victoria visitante"}

_DESC_PASS_EXTRA: dict[str, str] = {
    "odds_over25": "Cuota de más de 2,5 goles (media del mercado, apertura).",
    "odds_under25": "Cuota de menos de 2,5 goles (media del mercado, apertura).",
    "odds_b365_over25": "Cuota de más de 2,5 goles en Bet365, apertura.",
    "odds_b365_under25": "Cuota de menos de 2,5 goles en Bet365, apertura.",
    "odds_close_over25": "Cuota de más de 2,5 goles al cierre.",
    "odds_close_under25": "Cuota de menos de 2,5 goles al cierre.",
    "ah_line": "Línea del hándicap asiático en la apertura: la ventaja en goles "
               "que el mercado concede para igualar el partido.",
    "odds_ah_home": "Cuota del local con hándicap asiático, apertura.",
    "odds_ah_away": "Cuota del visitante con hándicap asiático, apertura.",
    "ah_close_line": "Línea del hándicap asiático al cierre.",
    "odds_ah_close_home": "Cuota del local con hándicap asiático, cierre.",
    "odds_ah_close_away": "Cuota del visitante con hándicap asiático, cierre.",
    "mkt_p_home": "Probabilidad de victoria local implícita en la cuota media, "
                  "ya normalizada para eliminar el margen de la casa.",
    "mkt_p_draw": "Probabilidad de empate implícita, ya sin margen.",
    "mkt_p_away": "Probabilidad de victoria visitante implícita, ya sin margen.",
}

# Priores de fuerza de club. La glosa se escribe del ATRIBUTO y no del lado,
# porque qué lado viaja —el par o la resta— lo decide el catálogo y la tabla ya
# lo registra en su propia columna `lado`: describir aquí «del local» obligaría
# a reescribir el diccionario cada vez que una declaración cambiase, que es
# exactamente la duplicación que este módulo existe para evitar.
_DESC_PRIOR = {
    "elo_z": "Fuerza de club antes del partido, en desviaciones respecto a la "
             "media de Primera ese día.",
    "squad_value": "Valor de mercado de la plantilla.",
    "age": "Edad media de la plantilla.",
}

_DESC_FATIGA = {
    "rest_days": "Días de descanso desde el partido anterior, contando todas "
                 "las competiciones.",
    "games_last_14": "Partidos disputados en los catorce días previos.",
    "euro_trip_recent": "Hubo desplazamiento europeo reciente.",
}

_DESC_BAJAS = {
    "n_missing": "Total de jugadores no disponibles.",
    "out_value_pct": "Porcentaje del valor de la plantilla que está ausente: "
                     "pondera la baja por su importancia y no por su número.",
    "out_minshare": "Suma de las cuotas de minutos que los ausentes venían "
                    "disputando: cuánto once le falta al equipo.",
    "out_minshare_max": "Cuota de minutos del ausente más importante: el tamaño "
                        "de la peor baja, que la suma diluye cuando hay varias.",
}

_DESC_ROTACION = {
    "xi_continuity": "Titulares que el equipo repitió respecto a su partido "
                     "anterior, sobre once.",
    "starters_minutes_14": "Minutos que los últimos titulares acumularon en los "
                           "catorce días previos.",
    "minutes_hhi": "Concentración de los minutos de la temporada en pocos "
                   "futbolistas (índice de Herfindahl).",
}


def _despoja(col: str) -> str:
    """Nombre de columna de pasarela -> atributo de equipo que mide.

    Le quita al nombre la decoración del lado: el prefijo `home_`/`away_` si
    viaja como par y el sufijo `_diff` si viaja como resta. Existe porque el
    lado es una decisión del CATÁLOGO y la procedencia es una propiedad del
    atributo: `elo_z` mide lo mismo se emita como par o como diferencia, y
    reconocer el nombre con una batería de expresiones regulares por familia
    —una rama para el par y otra para la resta— significaría que cambiar una
    declaración del catálogo rompe este módulo, cuando la fuente, el proveedor
    y la glosa no han cambiado.
    """
    base = col
    for pre in ("home_", "away_"):
        if base.startswith(pre):
            base = base[len(pre):]
            break
    if base.endswith("_diff"):
        base = base[: -len("_diff")]
    return base


def _atributo_pasarela(col: str) -> Atributo:
    """Procedencia y glosa de una columna pre-partido de pasarela."""
    # cuotas 1X2: rejilla casa x signo
    m = re.fullmatch(r"odds_(.+)_([hda])", col)
    if m and m.group(1) in _CASA_CUOTA:
        fuente = ("cuotas_cierre" if C.weekly_unavailable([col]) else "cuotas_apertura")
        return Atributo("cuotas", _M_PASS, (fuente,), _OBS,
                        f"Cuota de {_SIGNO_CUOTA[m.group(2)]} "
                        f"({_CASA_CUOTA[m.group(1)]}).")
    # precio del mercado: todo lo que queda aquí es magnitud DEL partido, así que
    # se reconoce por el nombre completo y no pasa por `_despoja`.
    if col in _DESC_PASS_EXTRA:
        fuente = ("cuotas_cierre" if C.weekly_unavailable([col])
                  else "cuotas_apertura")
        nat = _DER if col.startswith("mkt_p_") else _OBS
        return Atributo("cuotas", _M_PASS, (fuente,), nat, _DESC_PASS_EXTRA[col])

    # magnitudes de EQUIPO: el atributo manda, el lado es decoración.
    base = _despoja(col)
    if base in _DESC_PRIOR:
        if base == "elo_z":
            # La columna que viaja es la TIPIFICADA, y tipificar es un cálculo
            # sobre el rating observado: la naturaleza es derivada aunque el
            # nombre no lo delate. Quien la produce es
            # `consolidate/spine.add_clubelo`, no el extractor.
            return Atributo("elo", _M_PASS, ("elo",), _DER, _DESC_PRIOR[base])
        return Atributo("valor_plantilla", _M_PASS, ("valor_plantilla",),
                        _DER if col.endswith("_diff") else _OBS, _DESC_PRIOR[base])
    # fatiga: el máster rotula estas columnas con el infijo `fat_`
    if base.startswith("fat_") and base[len("fat_"):] in _DESC_FATIGA:
        return Atributo("fatiga", _M_PASS, ("fatiga",), _DER,
                        _DESC_FATIGA[base[len("fat_"):]])
    # bajas: conteo (disponibilidad), valor económico (bajas €) y rol (minutos)
    if base in _DESC_BAJAS:
        fuente = ("disponibilidad" if base.startswith("n_")
                  else "bajas_rol" if base.startswith("out_minshare")
                  else "bajas_euros")
        return Atributo("bajas", _M_PASS, (fuente,), _DER, _DESC_BAJAS[base])
    # rotación y carga de minutos
    if base in _DESC_ROTACION:
        return Atributo("rotacion", _M_PASS, ("rotacion_minutos",), _DER,
                        _DESC_ROTACION[base])
    raise KeyError(f"lineage: columna de pasarela sin procedencia declarada: {col!r}")


# --------------------------------------------------------------------------- #
# 5. Generación de la tabla de procedencia
# --------------------------------------------------------------------------- #
_LADO = {"home": "local", "away": "visitante"}


def _fila(columna: str, atr: Atributo, parametrizacion: str, lado: str) -> dict:
    fuentes = atr.fuentes
    proveedores = sorted({_FUENTE_POR_ID[f].proveedor for f in fuentes})
    return {
        "columna": columna,
        "bloque": atr.bloque,
        "modulo": atr.modulo,
        "atributo": None,          # lo rellena el generador que conoce el atributo
        "parametrizacion": parametrizacion,
        "lado": lado,
        "naturaleza": atr.naturaleza,
        "fuentes": ", ".join(fuentes),
        "proveedores": ", ".join(proveedores),
        "regimen": ("retrospectivo" if C.weekly_unavailable([columna]) else "semanal"),
        "descripcion": atr.descripcion,
    }


def _desdoblar(base: str, atr: Atributo, param: str, *,
               prefijo: str = "") -> list[dict]:
    """Una magnitud de equipo -> las columnas que el CATÁLOGO declara para ella.

    Qué lados se emiten no se decide aquí: se pregunta a `catalogo.nombres`, que
    es la misma función que consultan los módulos generadores al construir sus
    columnas. Así la procedencia no puede describir un conjunto distinto del que
    se produce —ni de más ni de menos—, y un atributo que el catálogo no declara
    devuelve la lista vacía y no aparece en ninguno de los dos sitios.
    """
    filas = []
    for lado in catalogo.lados(base):
        if lado == "diff":
            f = _fila(f"{prefijo}{base}_diff", atr, param, "diferencia")
        elif lado == "partido":
            f = _fila(f"{prefijo}{base}", atr, param, "partido")
        else:
            f = _fila(f"{lado}_{prefijo}{base}", atr, param, _LADO[lado])
        f["atributo"] = base
        filas.append(f)
    return filas


def _filas_forma() -> list[dict]:
    """Forma: acumulados, ventana corta, exponencial y escenario."""
    filas: list[dict] = []
    filas += _desdoblar("matches_prev", _ATTR_FORMA["matches_prev"], "acumulado")
    for a in _CUM_ATTRS:
        filas += _desdoblar(f"cum_{a}_pg", _ATTR_FORMA[a], "acumulado_por_partido")
    for a in _ROLL_ATTRS:
        for w in WINDOWS:
            filas += _desdoblar(f"form_{a}_{w}", _ATTR_FORMA[a], f"ventana_{w}")
    for a in _ROLL_ATTRS:
        filas += _desdoblar(f"form_{a}_ewm", _ATTR_FORMA[a], "exponencial")
    for a in _VENUE_ATTRS:
        filas += _desdoblar(f"venue_form_{a}_{_VENUE_WINDOW}", _ATTR_FORMA[a],
                            f"escenario_{_VENUE_WINDOW}")
    for a in ("effectiveness", "elo_mom_5_z"):
        filas += _desdoblar(a, _ATTR_FORMA[a], "directa")
    filas += _desdoblar("playstyle", _ATTR_FORMA["playstyle"], "directa")
    return filas


def _filas_mercado() -> list[dict]:
    filas: list[dict] = []
    for a in market_form_feature_names():
        filas += _desdoblar(a, _ATTR_MERCADO[a],
                            "exponencial" if a == "relative_perf" else "retardo_1")
    return filas


def _filas_h2h() -> list[dict]:
    filas: list[dict] = []
    for nombre in h2h_feature_names():
        base = nombre[: -len("_ewm")]
        filas += _desdoblar(nombre, _ATTR_H2H[base], "exponencial", prefijo="h2h_")
    return filas


def _filas_standings() -> list[dict]:
    nombres = standings_feature_names()
    filas: list[dict] = []
    for a in nombres["numeric"] + nombres["categorical"]:
        filas += _desdoblar(a, _ATTR_STANDINGS[a], "directa")
    for a in nombres["match_level"]:
        f = _fila(a, _ATTR_STANDINGS[a], "directa", "partido")
        f["atributo"] = a
        filas.append(f)
    return filas


def _filas_derivadas() -> list[dict]:
    filas = []
    for a in prematch_derived_feature_names():
        f = _fila(a, _ATTR_DERIVADAS[a], "directa", "partido")
        f["atributo"] = a
        filas.append(f)
    return filas


def _filas_contexto() -> list[dict]:
    nombres = context_feature_names()
    filas = []
    for a in nombres["numeric"] + nombres["categorical"]:
        f = _fila(a, _ATTR_CONTEXTO[a], "directa", "partido")
        f["atributo"] = a
        filas.append(f)
    return filas


def _filas_fuerza() -> list[dict]:
    nombres = strength_feature_names()
    filas: list[dict] = []
    for a in nombres["numeric"]:
        filas += _desdoblar(a, _ATTR_FUERZA[a], "directa")
    for a in nombres["match_level"]:
        f = _fila(a, _ATTR_FUERZA[a], "directa", "partido")
        f["atributo"] = a
        filas.append(f)
    return filas


def _filas_pasarela() -> list[dict]:
    """Pasarela pre-partido y columnas de SERVICIO.

    Las cuotas crudas de cada casa no son variables del modelo —el consenso sin
    margen ya lleva esa información y `columns.SERVICIO` las mantiene fuera del
    conjunto—, pero viajan en la misma matriz porque el backtest económico y el
    patrón de comparación de Bet365 se liquidan sobre exactamente las mismas
    filas que el pronóstico. Se documentan aquí, con bloque propio, por la misma
    razón por la que se documenta todo lo demás: una columna presente en el
    artefacto sin procedencia declarada es una columna que nadie puede defender.
    Marcarlas con un bloque aparte permite además contarlas por separado en el
    anexo, en vez de inflar el recuento de features.
    """
    filas = []
    for col in list(C.PREMATCH_PASSTHROUGH) + list(C.SERVICIO_MASTER):
        atr = _atributo_pasarela(col)
        if col in C.SERVICIO_MASTER:
            atr = Atributo("servicio", atr.modulo, atr.fuentes,
                           atr.naturaleza, atr.descripcion)
        f = _fila(col, atr, "servicio" if col in C.SERVICIO_MASTER else "pasarela",
                  "local" if col.startswith("home_") else
                  "visitante" if col.startswith("away_") else
                  "diferencia" if col.endswith("_diff") else "partido")
        f["atributo"] = col
        filas.append(f)
    return filas


_GENERADORES = (_filas_pasarela, _filas_forma, _filas_mercado, _filas_h2h,
                _filas_standings, _filas_derivadas, _filas_contexto, _filas_fuerza)


def _verificar_contrato(tabla: pd.DataFrame) -> None:
    """La tabla generada tiene que coincidir con lo que los módulos declaran.

    Es la comprobación que convierte esta tabla en un contrato y no en una
    descripción aproximada: si `form.py` incorpora un atributo nuevo, el
    conjunto de nombres que declara deja de coincidir con el que se genera aquí
    y el error aparece al construir el artefacto, no al leer la memoria.
    """
    generadas = set(tabla["columna"])

    # Los nombres esperados NO se reconstruyen aquí a base de expandir
    # `{home_, away_, _diff}`: eso sería una segunda opinión sobre qué lados
    # emite cada atributo, y el día que discrepase del catálogo la comprobación
    # daría por buena una tabla equivocada. Se le pregunta a `catalogo.nombres`,
    # que es la misma función que usan los módulos al construir sus columnas.
    esperadas: set[str] = set(C.PREMATCH_PASSTHROUGH) | set(C.SERVICIO_MASTER)
    nf = form_feature_names()
    for c in nf["numeric"] + nf["categorical"]:
        esperadas |= set(catalogo.nombres(c))
    for c in market_form_feature_names():
        esperadas |= set(catalogo.nombres(c))
    for c in h2h_feature_names():
        esperadas |= set(catalogo.nombres(c, "h2h_"))
    ns = standings_feature_names()
    for c in ns["numeric"] + ns["categorical"]:
        esperadas |= set(catalogo.nombres(c))
    esperadas |= set(ns["match_level"])
    esperadas |= set(prematch_derived_feature_names())
    nc = context_feature_names()
    esperadas |= set(nc["numeric"]) | set(nc["categorical"])
    nst = strength_feature_names()
    for c in nst["numeric"]:
        esperadas |= set(catalogo.nombres(c))
    esperadas |= set(nst["match_level"])

    faltan = sorted(esperadas - generadas)
    sobran = sorted(generadas - esperadas)
    if faltan or sobran:
        raise AssertionError(
            "lineage: la tabla de procedencia no coincide con lo que declaran "
            f"los módulos.\n  sin declarar aquí ({len(faltan)}): {faltan[:12]}\n"
            f"  declaradas de más ({len(sobran)}): {sobran[:12]}")

    dup = tabla.loc[tabla["columna"].duplicated(), "columna"].tolist()
    if dup:
        raise AssertionError(f"lineage: columnas con procedencia duplicada: {dup[:12]}")


def feature_lineage() -> pd.DataFrame:
    """Tabla de procedencia: una fila por columna del set de features.

    No necesita leer ningún artefacto: se genera enteramente de las
    declaraciones de los módulos, de modo que puede construirse y revisarse sin
    haber ejecutado la cadena de datos.
    """
    filas: list[dict] = []
    for gen in _GENERADORES:
        filas += gen()
    tabla = pd.DataFrame(filas)
    _verificar_contrato(tabla)
    orden = ["columna", "bloque", "modulo", "atributo", "parametrizacion", "lado",
             "naturaleza", "fuentes", "proveedores", "regimen", "descripcion"]
    return tabla[orden].sort_values(["bloque", "atributo", "columna"]) \
                       .reset_index(drop=True)


def source_registry() -> pd.DataFrame:
    """Los bloques de fuente, con su proveedor, extractor y artefacto publicado."""
    return pd.DataFrame([{
        "fuente": b.id,
        "proveedor": PROVEEDORES[b.proveedor].nombre,
        "origen": PROVEEDORES[b.proveedor].origen,
        "extractor": b.extractor,
        "artefacto": b.artefacto,
        "granularidad": b.granularidad,
        "nota": b.nota,
    } for b in BLOQUES_FUENTE])


# --------------------------------------------------------------------------- #
# 6. Comprobación de completitud contra un set real
# --------------------------------------------------------------------------- #
_NO_FEATURE = set(C.KEYS) | set(C.MASTER_METADATA) | {
    C.TARGET, C.TARGET_NUM, C.TARGET_SOURCE, C.SPLIT}


def feature_columns(feat: pd.DataFrame) -> list[str]:
    """Columnas de `feat` que son features (ni clave, ni objetivo, ni partición)."""
    return [c for c in feat.columns if c not in _NO_FEATURE]


def check_lineage(feat: pd.DataFrame) -> dict[str, list[str]]:
    """Compara el set real con la tabla de procedencia, en los dos sentidos.

    Devuelve dos listas, y las dos deben quedar vacías:

      * `sin_procedencia` : columnas del set que nadie declara. Es el fallo
        grave —una columna sin origen documentado no puede defenderse ante un
        tribunal ni justificarse en el anexo de la memoria.
      * `declaradas_ausentes` : columnas declaradas que no están en el set. No
        es un fallo de documentación sino de datos: normalmente significa que
        una fuente no llegó al máster y su bloque entero se ha caído en
        silencio, que es exactamente lo que esta comprobación evita.
    """
    reales = set(feature_columns(feat))
    declaradas = set(feature_lineage()["columna"])
    return {"sin_procedencia": sorted(reales - declaradas),
            "declaradas_ausentes": sorted(declaradas - reales)}


# --------------------------------------------------------------------------- #
# 7. Matriz de cobertura
# --------------------------------------------------------------------------- #
def _cobertura_fuente(master: pd.DataFrame, b: BloqueFuente) -> pd.Series:
    """Indicador por partido de que el bloque de fuente llegó.

    El listón son las columnas TESTIGO que el bloque declara, no todas las que
    el proveedor trae. La diferencia importa en el panel de estadística de
    Sofascore, que son ciento seis columnas cuya composición varía de un partido
    a otro: exigirlas todas a la vez convierte el indicador en un cero constante
    —ningún partido de los 3800 las tiene— y un indicador que no puede variar es
    peor que ninguno, porque la rutina semanal lo imprime y el ojo aprende a
    saltárselo. Los testigos se eligen justamente como las columnas que están
    siempre que el panel llegó, de modo que el indicador responde a la pregunta
    que se le hace: si la fuente contestó para este partido.
    """
    if b.id == "geografia":
        conocidos = set(stadium_table()["team_key"])
        return (master["home_key"].isin(conocidos)
                & master["away_key"].isin(conocidos))
    presentes = [c for c in b.columnas if c in master.columns]
    if not presentes:
        return pd.Series(False, index=master.index)
    return master[presentes].notna().all(axis=1)


def coverage_matrix(master: pd.DataFrame,
                    feat: pd.DataFrame | None = None) -> pd.DataFrame:
    """Una fila por partido con qué fuentes llegaron y qué bloques son calculables.

    Los indicadores `fuente_*` se miden sobre el máster —donde un hueco solo
    puede significar que el dato no llegó— y los `bloque_*` sobre el set de
    features, donde un hueco puede deberse además al arranque en frío de una
    ventana móvil. Leídos juntos distinguen el fallo de la fuente del efecto
    esperado del diseño leak-safe.
    """
    keys = [c for c in _COV_KEYS if c in master.columns]
    cov = master[keys].copy().reset_index(drop=True)

    for b in BLOQUES_FUENTE:
        cov[f"fuente_{b.id}"] = _cobertura_fuente(master, b).values
    fuentes = [f"fuente_{b.id}" for b in BLOQUES_FUENTE]
    cov["n_fuentes"] = cov[fuentes].sum(axis=1).astype("int64")

    if feat is not None:
        lin = feature_lineage()
        presentes = set(feat.columns)
        emparejado = feat.set_index("match_key")
        idx = cov["match_key"]
        bloques = []
        for bloque, grupo in lin.groupby("bloque", sort=True):
            cols = [c for c in grupo["columna"] if c in presentes]
            if not cols:
                continue
            ok = emparejado[cols].notna().all(axis=1).reindex(idx).fillna(False)
            cov[f"bloque_{bloque}"] = ok.values
            bloques.append(f"bloque_{bloque}")
        cov["n_bloques"] = cov[bloques].sum(axis=1).astype("int64")

    return cov


def coverage_by_season(cov: pd.DataFrame) -> pd.DataFrame:
    """Resume la matriz por temporada: la tabla que va al anexo de la memoria."""
    ind = [c for c in cov.columns if c.startswith(("fuente_", "bloque_"))]
    res = cov.groupby("season", dropna=False)[ind].mean()
    res.insert(0, "partidos", cov.groupby("season", dropna=False).size())
    return res.reset_index()


def coverage_report(cov: pd.DataFrame) -> None:
    """Imprime la cobertura por temporada y el primer año activo de cada fuente."""
    por_temp = coverage_by_season(cov)
    fuentes = [c for c in cov.columns if c.startswith("fuente_")]
    print("\nCobertura de fuente por temporada (proporción de partidos completos):")
    cab = "  season    partidos" + "".join(f"{c[7:][:9]:>11}" for c in fuentes)
    print(cab)
    for _, r in por_temp.iterrows():
        celdas = "".join(f"{r[c]:>10.0%} " for c in fuentes)
        print(f"  {str(r['season']):<9} {int(r['partidos']):>8}" + celdas)

    print("\nPrimera temporada con dato de cada fuente:")
    for c in fuentes:
        activo = por_temp.loc[por_temp[c] > 0, "season"]
        inicio = activo.iloc[0] if len(activo) else "—"
        media = por_temp.loc[por_temp[c] > 0, c].mean() if len(activo) else float("nan")
        print(f"  {c[7:]:22s}: desde {str(inicio):<8} "
              f"cobertura en su rango {media:.1%}")


# --------------------------------------------------------------------------- #
# 8. Informe de la tabla de procedencia
# --------------------------------------------------------------------------- #
def lineage_report(lin: pd.DataFrame | None = None) -> None:
    lin = feature_lineage() if lin is None else lin
    print("=" * 72)
    print("PROCEDENCIA DE LAS FEATURES")
    print("=" * 72)
    print(f"columnas declaradas={len(lin)}  bloques={lin['bloque'].nunique()}  "
          f"atributos={lin['atributo'].nunique()}")

    print("\nPor bloque analítico:")
    for b, g in lin.groupby("bloque", sort=True):
        print(f"  {b:24s}: {len(g):>3} cols  ({g['modulo'].iloc[0]})")

    print("\nPor proveedor (una columna puede depender de varios):")
    conteo: dict[str, int] = {}
    for p in lin["proveedores"]:
        for x in p.split(", "):
            conteo[x] = conteo.get(x, 0) + 1
    for k in sorted(conteo, key=lambda x: -conteo[x]):
        print(f"  {k:28s}: {conteo[k]:>3} cols")

    print("\nPor naturaleza del dato:")
    for k, v in lin["naturaleza"].value_counts().items():
        print(f"  {k:12s}: {v:>3} cols")

    print("\nPor régimen de información:")
    for k, v in lin["regimen"].value_counts().items():
        print(f"  {k:16s}: {v:>3} cols")


if __name__ == "__main__":
    lineage_report()
