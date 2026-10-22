"""Features de CONTEXTO del encuentro: calendario y geografía.

Dos condicionantes del partido que no dependen de cómo llegan los equipos sino
de DÓNDE y CUÁNDO se juega. Ambos se conocen con semanas de antelación —el
calendario lo publica la competición y la geografía no cambia—, así que son
pre-partido en el sentido fuerte: existen antes del pitido inicial y también en
el momento de emitir la predicción.

CALENDARIO (fuente: `kickoff_ts`, el sello de tiempo de saque de SofaScore, que
`consolidate/reducers.reduce_sofascore` traslada al máster como metadato).

  * kickoff_slot   franja de emisión: 'mediodia' (< 15 h) < 'tarde' (15-19 h) <
                   'noche' (>= 19 h).  [categórica ordinal]
  * is_weekend     1 si el saque cae en sábado o domingo.

  La hora local de saque se calcula como paso intermedio pero no sale a la
  matriz: la hipótesis de dominio no es lineal en la hora —el calor de un saque
  de mediodía de agosto, el ritmo circadiano de uno de las diez de la noche y la
  audiencia de cada franja son efectos de escalón, no de pendiente—, de modo que
  la franja es la lectura correcta de esa magnitud y la hora en bruto sería la
  misma información en una escala que el modelo no puede aprovechar.

  El día de la semana entra SOLO como `is_weekend`, y esa es toda la lectura del
  calendario semanal que llega a la matriz. El saque entre semana se sigue
  calculando en `fatigue.py` como insumo del bloque de congestión, pero ya no se
  emite: era la misma partición del calendario leída al revés —`is_weekend` y
  `is_midweek` se reparten casi todas las fechas— y su información mutua con el
  resultado era la más baja de todo el banco. El desplazamiento a viernes o lunes
  es el complemento de `is_weekend` y cualquier estimador puede reconstruirlo.

GEOGRAFÍA (fuente: la tabla de estadios declarada más abajo, versionada en el
repositorio).

  * away_travel_km  distancia ortodrómica entre el estadio del visitante y el
                    del local. Es el desplazamiento que soporta el visitante y
                    cero para el local, de ahí el prefijo.
  * is_derby        1 si ambos clubes pertenecen a la misma comunidad autónoma.

  El derbi NO se deriva de la distancia, y esa es una decisión deliberada. Un
  umbral kilométrico sería una función determinista de `away_travel_km` y por
  tanto no aportaría información nueva; además clasificaría mal los dos casos
  que definen el fenómeno: el derbi gallego (Celta-Deportivo, unos 150 km) es un
  derbi y el trayecto Vitoria-Pamplona (unos 90 km) no lo es. La rivalidad local
  es un hecho administrativo y social, no métrico, así que se opera con el hecho
  administrativo —la comunidad autónoma del club— que es objetivo, verificable y
  ajeno a cualquier juicio sobre qué rivalidades "cuentan".

EL ÁRBITRO QUEDA FUERA DEL ALCANCE. Es el tercer condicionante clásico del
contexto y no se incluye porque el dato no existe en la cadena: el fichero de
LaLiga de football-data no publica la columna `Referee` —solo la publican sus
ficheros ingleses—, de modo que ni el crudo ni el máster la contienen.
Obtenerla exigiría descargar el detalle de evento de SofaScore, una extracción
distinta de la que alimenta hoy la estadística de partido. Se prefiere declarar
el hueco a rellenarlo con un identificador aproximado.

Todas las columnas son de PARTIDO (no se desdoblan en local/visitante) y
ninguna admite el cero como sustituto del hueco: sin sello de saque las
columnas de calendario quedan NaN, y un club ausente de la tabla de estadios
deja las de geografía en NaN. `qa_features` comprueba que ese segundo caso no
ocurra.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import assert_unique  # noqa: E402

import catalogo  # noqa: E402

_NEED = ["match_key", "home_key", "away_key"]

# Huso de la competición. El sello de saque viaja en UTC; leer la hora y el día
# de la semana en UTC desplazaría a la víspera todos los saques nocturnos de
# invierno y a la jornada anterior los de verano.
_TZ_LOCAL = "Europe/Madrid"

# Cortes de franja horaria, en hora local. Coinciden con las tres ventanas de
# emisión que la competición usa desde 2016/17.
_SLOT_MEDIODIA_MAX = 15.0   # saques por debajo de las 15:00
_SLOT_TARDE_MAX = 19.0      # saques por debajo de las 19:00
SLOT_ORDER = ("mediodia", "tarde", "noche")

_WEEKEND_WD = (5, 6)        # sábado, domingo

# Radio medio de la Tierra en kilómetros (esfera de referencia IUGG).
_EARTH_RADIUS_KM = 6371.0088

# --------------------------------------------------------------------------- #
# Tabla de estadios: clave canónica -> (estadio, latitud, longitud, comunidad)
# --------------------------------------------------------------------------- #
# Cubre los 32 clubes que han disputado al menos un partido de Primera desde
# 2016/17. Se escribe a mano una sola vez porque es un dato estable y de tamaño
# fijo: montar un scraping para treinta filas que no cambian añadiría una
# dependencia de red y un modo de fallo a cambio de nada.
#
# Las coordenadas son las del estadio en que el club ejerce de local en el
# periodo del estudio; se toma un único emplazamiento por club, de modo que la
# distancia entre dos clubes es una constante del par. Los traslados temporales
# de sede (obras, sanciones) afectan a un puñado de partidos y su efecto sobre
# un trayecto de cientos de kilómetros es de segundo orden frente al coste de
# mantener un calendario de sedes.
#
# La comunidad autónoma es el criterio de derbi (ver el encabezado del módulo).
_ESTADIOS: dict[str, tuple[str, float, float, str]] = {
    "alaves":              ("Mendizorroza",             42.8371, -2.6882, "pais_vasco"),
    "almeria":             ("Power Horse Stadium",      36.8400, -2.4356, "andalucia"),
    "athletic_bilbao":     ("San Mamés",                43.2642, -2.9494, "pais_vasco"),
    "atletico_madrid":     ("Metropolitano",            40.4362, -3.5995, "madrid"),
    "barcelona":           ("Camp Nou",                 41.3809,  2.1228, "cataluna"),
    "cadiz":               ("Nuevo Mirandilla",         36.5020, -6.2731, "andalucia"),
    "celta_vigo":          ("Balaídos",                 42.2118, -8.7396, "galicia"),
    "deportivo_la_coruna": ("Riazor",                   43.3692, -8.4188, "galicia"),
    "eibar":               ("Ipurua",                   43.1836, -2.4756, "pais_vasco"),
    "elche":               ("Martínez Valero",          38.2665, -0.6656, "valencia"),
    "espanyol":            ("RCDE Stadium",             41.3475,  2.0755, "cataluna"),
    "getafe":              ("Coliseum",                 40.3259, -3.7148, "madrid"),
    "girona":              ("Montilivi",                41.9614,  2.8283, "cataluna"),
    "granada":             ("Nuevo Los Cármenes",       37.1531, -3.5959, "andalucia"),
    "huesca":              ("El Alcoraz",               42.1319, -0.4084, "aragon"),
    "las_palmas":          ("Gran Canaria",             28.1000, -15.4566, "canarias"),
    "leganes":             ("Butarque",                 40.3403, -3.7597, "madrid"),
    "levante":             ("Ciutat de València",       39.4947, -0.3644, "valencia"),
    "malaga":              ("La Rosaleda",              36.7399, -4.4262, "andalucia"),
    "mallorca":            ("Son Moix",                 39.5896,  2.6303, "baleares"),
    "osasuna":             ("El Sadar",                 42.7963, -1.6370, "navarra"),
    "racing_santander":    ("El Sardinero",             43.4274, -3.8206, "cantabria"),
    "rayo_vallecano":      ("Vallecas",                 40.3919, -3.6586, "madrid"),
    "real_betis":          ("Benito Villamarín",        37.3564, -5.9819, "andalucia"),
    "real_madrid":         ("Santiago Bernabéu",        40.4531, -3.6883, "madrid"),
    "real_oviedo":         ("Carlos Tartiere",          43.3600, -5.8697, "asturias"),
    "real_sociedad":       ("Reale Arena",              43.3013, -1.9736, "pais_vasco"),
    "sevilla":             ("Ramón Sánchez-Pizjuán",    37.3841, -5.9705, "andalucia"),
    "sporting_gijon":      ("El Molinón",               43.5364, -5.6376, "asturias"),
    "valencia":            ("Mestalla",                 39.4747, -0.3583, "valencia"),
    "valladolid":          ("José Zorrilla",            41.6446, -4.7614, "castilla_leon"),
    "villarreal":          ("La Cerámica",              39.9440, -0.1039, "valencia"),
}

_NUMERIC = list(catalogo.CONTEXTO)
_CATEGORICAL = list(catalogo.CONTEXTO_CATEGORICO)


# --------------------------------------------------------------------------- #
# geometría
# --------------------------------------------------------------------------- #
def haversine_km(lat1, lon1, lat2, lon2):
    """Distancia ortodrómica en km sobre una esfera de radio medio terrestre.

    Se usa la fórmula del semiverseno y no la ley de cosenos esférica porque
    esta última pierde precisión justamente en el caso que más abunda aquí: dos
    estadios muy próximos (Sevilla-Betis están a cuatro kilómetros), donde el
    coseno del ángulo es indistinguible de uno en coma flotante.
    """
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(x, dtype="float64"))
                              for x in (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2.0 * _EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def stadium_table() -> pd.DataFrame:
    """La tabla de estadios como DataFrame (clave canónica, sede, coords, CCAA)."""
    return pd.DataFrame(
        [(k, v[0], v[1], v[2], v[3]) for k, v in sorted(_ESTADIOS.items())],
        columns=["team_key", "stadium", "lat", "lon", "region"])


def unknown_stadiums(master: pd.DataFrame) -> list[str]:
    """Clubes del máster que no tienen entrada en la tabla de estadios.

    Es el único modo de fallo de la geografía: un club recién ascendido deja las
    dos columnas en NaN y el preprocesado le imputaría después una distancia
    mediana, que es un valor perfectamente creíble y por completo inventado. La
    comprobación se expone aquí y la ejecuta `qa_features`, para que el aviso
    llegue al construir el artefacto y no al leer una predicción rara.
    """
    vistos = set(master["home_key"].dropna()) | set(master["away_key"].dropna())
    return sorted(vistos - set(_ESTADIOS))


# --------------------------------------------------------------------------- #
# calendario
# --------------------------------------------------------------------------- #
def _classify_slot(hour) -> object:
    if pd.isna(hour):
        return pd.NA
    if hour < _SLOT_MEDIODIA_MAX:
        return "mediodia"
    if hour < _SLOT_TARDE_MAX:
        return "tarde"
    return "noche"


def _local_kickoff(master: pd.DataFrame) -> pd.Series:
    """Sello de saque convertido a hora local, o todo NaT si no viaja en el máster."""
    if "kickoff_ts" not in master.columns:
        return pd.Series(pd.NaT, index=master.index, dtype="datetime64[ns]")
    ts = pd.to_numeric(master["kickoff_ts"], errors="coerce")
    return (pd.to_datetime(ts, unit="s", utc=True)
              .dt.tz_convert(_TZ_LOCAL).dt.tz_localize(None))


# --------------------------------------------------------------------------- #
# ensamblado
# --------------------------------------------------------------------------- #
def build_context_features(master: pd.DataFrame) -> pd.DataFrame:
    """Máster -> features de contexto (calendario y geografía), keyed por match_key."""
    faltan = [c for c in _NEED if c not in master.columns]
    if faltan:
        raise KeyError(f"context: faltan columnas en el máster: {faltan}")

    out = pd.DataFrame({"match_key": master["match_key"].values})

    # --- calendario ---------------------------------------------------------
    # La hora local es una variable de esta función y no una columna del frame:
    # el catálogo no la emite, así que escribirla en `out` para borrarla tres
    # líneas después al seleccionar sería dejar constancia de una feature que no
    # existe. Quien necesite auditarla —el banco de pruebas— la reconstruye desde
    # `_local_kickoff`, que es donde la conversión de huso realmente ocurre.
    loc = _local_kickoff(master)
    hora = loc.dt.hour + loc.dt.minute / 60.0
    out["is_weekend"] = (loc.dt.weekday.isin(_WEEKEND_WD)
                            .where(loc.notna()).astype("float64").values)
    out["kickoff_slot"] = pd.Series(hora.map(_classify_slot).values, dtype="string")

    # --- geografía ----------------------------------------------------------
    coords = {k: (v[1], v[2]) for k, v in _ESTADIOS.items()}
    region = {k: v[3] for k, v in _ESTADIOS.items()}
    hk, ak = master["home_key"].astype("string"), master["away_key"].astype("string")

    h_lat = hk.map(lambda k: coords.get(k, (np.nan, np.nan))[0]).astype("float64")
    h_lon = hk.map(lambda k: coords.get(k, (np.nan, np.nan))[1]).astype("float64")
    a_lat = ak.map(lambda k: coords.get(k, (np.nan, np.nan))[0]).astype("float64")
    a_lon = ak.map(lambda k: coords.get(k, (np.nan, np.nan))[1]).astype("float64")

    out["away_travel_km"] = haversine_km(h_lat, h_lon, a_lat, a_lon)

    h_reg = hk.map(lambda k: region.get(k)).astype("string")
    a_reg = ak.map(lambda k: region.get(k)).astype("string")
    conocidos = h_reg.notna() & a_reg.notna()
    out["is_derby"] = (h_reg == a_reg).where(conocidos.values).astype("float64").values

    out = out.reset_index(drop=True)
    assert_unique(out, ["match_key"], "context")
    return out[["match_key"] + _NUMERIC + _CATEGORICAL]


def context_feature_names() -> dict[str, list[str]]:
    """Contrato del módulo: las columnas que devuelve, por tipo.

    La lista declarada es el contrato y no una descripción aproximada de lo que
    ocurre dentro: `build_context_features` selecciona por ella al devolver, de
    modo que una columna intermedia que se olvidara de borrar no puede colarse
    en el set de features sin pasar antes por aquí.
    """
    return {"numeric": list(_NUMERIC), "categorical": list(_CATEGORICAL)}
