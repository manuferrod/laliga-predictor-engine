"""
Capa compartida de normalizacion de nombres de equipo y clave de partido.

Objetivo
--------
Todas las fuentes (football-data.co.uk, FBref, Understat, ClubElo, Transfermarkt)
nombran a los mismos clubes de forma distinta:

    "Athletic Club" / "Bilbao" / "athletic-club"        -> athletic_bilbao
    "Atletico Madrid" / "Atletico" / "Club Atletico..." -> atletico_madrid
    "Real Betis Balompie" / "Betis"                     -> real_betis

Este modulo reduce cualquiera de esas variantes a una **clave canonica** estable
(p.ej. `atletico_madrid`), de forma que los merges entre fuentes sean fiables.

El mapa se construyo a partir de los nombres REALES que devuelven las fuentes
del proyecto (football-data, Sofascore, Understat, ClubElo, Transfermarkt), de
modo que cubre todos los clubes de Primera de la ventana 2016/17 en adelante.

Diseno
------
1. `clean(name)`  -> forma normalizada en MAYUSCULAS, sin acentos, sin sufijos de
   tipo de club (CF, FC, UD, SD, CA, RCD, "de Futbol", "Balompie", ...).
2. `ALIASES`      -> dict {forma_limpia: clave_canonica}. Varias formas limpias
   pueden apuntar a la misma clave.
3. `to_key(name)` -> clave canonica. Si un nombre nuevo no esta en el mapa,
   degrada con gracia a un slug (minusculas_con_guiones_bajos) y se puede detectar
   con `unknown_names()` para anadirlo al mapa.

Uso tipico
----------
    from extractors import teamnames as tn
    df["home_key"] = df["HomeTeam"].map(tn.to_key)
    df["away_key"] = df["AwayTeam"].map(tn.to_key)
    df["match_key"] = tn.match_key(df["Date"], df["HomeTeam"], df["AwayTeam"])
"""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable

import pandas as pd

__all__ = [
    "clean",
    "to_key",
    "to_display",
    "match_key",
    "add_keys",
    "unknown_names",
    "assert_known",
    "CANONICAL",
    "ALIASES",
]

# ---------------------------------------------------------------------------
# 1) Limpieza basica
# ---------------------------------------------------------------------------

# Tokens de "tipo de club" y coletillas geograficas que no aportan identidad.
# Se eliminan tras pasar a MAYUSCULAS y quitar acentos.
_STOP = re.compile(
    r"\b("
    r"CF|FC|SAD|CD|UD|SD|CA|RCDE|RCD|RC|AD|CDE|"
    r"BALOMPIE|DE FUTBOL|DE BARCELONA|DE VIGO|DE MADRID|DE LA|"
    r"CLUB ATLETICO DE|CLUB ATLETICO|ATLETICO CLUB|CLUB|"
    r"REAL CLUB DEPORTIVO|REAL CLUB"
    r")\b"
)


def _deaccent(s: str) -> str:
    t = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in t if not unicodedata.combining(c))


def clean(name) -> str:
    """Normaliza un nombre de equipo a MAYUSCULAS sin acentos ni sufijos de club.

    Ejemplos:
        "Atlético Madrid"           -> "ATLETICO MADRID"
        "Club Atlético de Madrid"   -> "MADRID"
        "RCD Espanyol de Barcelona" -> "ESPANYOL"
        "sporting-gijon"            -> "SPORTING GIJON"
    """
    if name is None:
        return ""
    t = _deaccent(name).upper()
    t = t.replace("-", " ")
    t = re.sub(r"[^A-Z0-9 ]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = _STOP.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ---------------------------------------------------------------------------
# 2) Mapa canonico  (clave -> nombre "bonito" para mostrar)
# ---------------------------------------------------------------------------

CANONICAL: dict[str, str] = {
    "alaves": "Deportivo Alavés",
    "almeria": "UD Almería",
    "athletic_bilbao": "Athletic Club",
    "atletico_madrid": "Atlético de Madrid",
    "barcelona": "FC Barcelona",
    "cadiz": "Cádiz CF",
    "celta_vigo": "RC Celta de Vigo",
    "cordoba": "Córdoba CF",
    "deportivo_la_coruna": "Deportivo de La Coruña",
    "eibar": "SD Eibar",
    "elche": "Elche CF",
    "espanyol": "RCD Espanyol",
    "getafe": "Getafe CF",
    "gimnastic_tarragona": "Gimnàstic de Tarragona",
    "girona": "Girona FC",
    "granada": "Granada CF",
    "hercules": "Hércules CF",
    "huesca": "SD Huesca",
    "las_palmas": "UD Las Palmas",
    "leganes": "CD Leganés",
    "levante": "Levante UD",
    "malaga": "Málaga CF",
    "mallorca": "RCD Mallorca",
    "numancia": "CD Numancia",
    "osasuna": "CA Osasuna",
    "rayo_vallecano": "Rayo Vallecano",
    "real_betis": "Real Betis",
    "real_madrid": "Real Madrid",
    "real_murcia": "Real Murcia",
    "real_oviedo": "Real Oviedo",
    "real_sociedad": "Real Sociedad",
    "racing_santander": "Racing de Santander",
    "recreativo_huelva": "Recreativo de Huelva",
    "sevilla": "Sevilla FC",
    "sporting_gijon": "Sporting de Gijón",
    "tenerife": "CD Tenerife",
    "valencia": "Valencia CF",
    "valladolid": "Real Valladolid",
    "villarreal": "Villarreal CF",
    "xerez": "Xerez CD",
    "zaragoza": "Real Zaragoza",
}

# ---------------------------------------------------------------------------
# 3) Alias: {forma_limpia -> clave_canonica}
#    Construido a partir de las formas limpias REALES observadas en las fuentes
#    del proyecto. Cada linea documenta de que fuente(s) viene la variante.
# ---------------------------------------------------------------------------

ALIASES: dict[str, str] = {
    # alaves            (US/CE "ALAVES", WK/TM "DEPORTIVO ALAVES")
    "ALAVES": "alaves",
    "DEPORTIVO ALAVES": "alaves",
    # almeria
    "ALMERIA": "almeria",
    # athletic bilbao   (US/WK/TM "ATHLETIC", CE "BILBAO", FD "ATH BILBAO")
    "ATHLETIC": "athletic_bilbao",
    "ATHLETIC BILBAO": "athletic_bilbao",
    "ATHLETIC DE BILBAO": "athletic_bilbao",   # "Athletic Club de Bilbao"
    "BILBAO": "athletic_bilbao",
    "ATH BILBAO": "athletic_bilbao",
    # atletico madrid   (US/WK/TM "ATLETICO MADRID", CE "ATLETICO", WK "MADRID", FD "ATH MADRID")
    "ATLETICO MADRID": "atletico_madrid",
    "ATLETICO": "atletico_madrid",
    # PELIGRO: "MADRID" viene de "Club Atletico de Madrid" tras pasar por _STOP
    # (que elimina "CLUB ATLETICO DE"). Si alguna fuente futura emite "Madrid" a
    # secas refiriendose al REAL Madrid, este alias lo asignara al Atletico en
    # silencio. Si aparece una fuente asi, elimina este alias y anade la forma
    # completa de esa fuente en su lugar.
    "MADRID": "atletico_madrid",
    "ATLETICO DE MADRID": "atletico_madrid",
    "ATH MADRID": "atletico_madrid",
    # barcelona
    "BARCELONA": "barcelona",
    # cadiz
    "CADIZ": "cadiz",
    # celta vigo        (US "CELTA VIGO", WK/TM/CE "CELTA")
    "CELTA VIGO": "celta_vigo",
    "CELTA": "celta_vigo",
    # cordoba
    "CORDOBA": "cordoba",
    # deportivo la coruna (US/TM "DEPORTIVO LA CORUNA", WK "LA CORUNA", CE "DEPOR",
    #                      SofaScore usa forma gallega "Deportivo de A Coruña")
    "DEPORTIVO LA CORUNA": "deportivo_la_coruna",
    "LA CORUNA": "deportivo_la_coruna",
    "DEPOR": "deportivo_la_coruna",
    "DEPORTIVO": "deportivo_la_coruna",
    "DEPORTIVO DE A CORUNA": "deportivo_la_coruna",
    "DEPORTIVO A CORUNA": "deportivo_la_coruna",
    "DEPORTIVO CORUNA": "deportivo_la_coruna",   # TM slug "deportivo-de-la-coruna" -> _STOP quita "DE LA"
    "CORUNA": "deportivo_la_coruna",             # "Real Club Deportivo de La Coruna" -> _STOP come todo lo demas
    "A CORUNA": "deportivo_la_coruna",
    # eibar
    "EIBAR": "eibar",
    # elche
    "ELCHE": "elche",
    # espanyol
    "ESPANYOL": "espanyol",
    "ESPANOL": "espanyol",
    "ESPANYOL BARCELONA": "espanyol",   # TM slug "espanyol-barcelona"
    "ESPANOL BARCELONA": "espanyol",
    "REIAL DEPORTIU ESPANYOL": "espanyol",       # nombre oficial en catalan
    # getafe
    "GETAFE": "getafe",
    # gimnastic tarragona (WK "GIMNASTIC", CE "TARRAGONA", TM "GIMNASTIC TARRAGONA")
    "GIMNASTIC": "gimnastic_tarragona",
    "GIMNASTIC TARRAGONA": "gimnastic_tarragona",
    "TARRAGONA": "gimnastic_tarragona",
    "NASTIC": "gimnastic_tarragona",
    # girona
    "GIRONA": "girona",
    # granada
    "GRANADA": "granada",
    # hercules
    "HERCULES": "hercules",
    # huesca
    "HUESCA": "huesca",
    # las palmas
    "LAS PALMAS": "las_palmas",
    "UNION DEPORTIVA LAS PALMAS": "las_palmas",  # nombre oficial completo
    # leganes
    "LEGANES": "leganes",
    "DEPORTIVO LEGANES": "leganes",              # "Club Deportivo Leganes"
    # levante
    "LEVANTE": "levante",
    # malaga
    "MALAGA": "malaga",
    # mallorca
    "MALLORCA": "mallorca",
    # numancia
    "NUMANCIA": "numancia",
    # osasuna
    "OSASUNA": "osasuna",
    # rayo vallecano
    "RAYO VALLECANO": "rayo_vallecano",
    "VALLECANO": "rayo_vallecano",
    # real betis        (US/TM "REAL BETIS", WK/CE "BETIS")
    "REAL BETIS": "real_betis",
    "BETIS": "real_betis",
    # real madrid
    "REAL MADRID": "real_madrid",
    # real murcia       (WK/TM "REAL MURCIA", CE "MURCIA")
    "REAL MURCIA": "real_murcia",
    "MURCIA": "real_murcia",
    # real oviedo       (US "REAL OVIEDO", CE "OVIEDO")
    "REAL OVIEDO": "real_oviedo",
    "OVIEDO": "real_oviedo",
    # real sociedad     ("REAL SOCIEDAD", CE "SOCIEDAD")
    "REAL SOCIEDAD": "real_sociedad",
    "SOCIEDAD": "real_sociedad",
    # racing santander  (WK "RACING SANT", CE "SANTANDER", TM "RACING SANTANDER",
    #                    SofaScore "Real Racing Club" -> el filtro come "CLUB")
    "RACING SANTANDER": "racing_santander",
    "RACING SANT": "racing_santander",
    "RACING DE SANTANDER": "racing_santander",   # "Real Racing Club de Santander"
    "REAL RACING DE SANTANDER": "racing_santander",
    "REAL RACING": "racing_santander",
    "RACING": "racing_santander",
    "SANTANDER": "racing_santander",
    # recreativo huelva (WK/CE "RECREATIVO", TM "RECREATIVO HUELVA")
    "RECREATIVO": "recreativo_huelva",
    "RECREATIVO HUELVA": "recreativo_huelva",
    # sevilla
    "SEVILLA": "sevilla",
    # sporting gijon    (US/WK/TM "SPORTING GIJON", CE "GIJON", FD "SP GIJON")
    "SPORTING GIJON": "sporting_gijon",
    "SPORTING": "sporting_gijon",
    "GIJON": "sporting_gijon",
    "SP GIJON": "sporting_gijon",
    "SPORTING DE GIJON": "sporting_gijon",       # "Real Sporting de Gijon"
    "REAL SPORTING DE GIJON": "sporting_gijon",
    "REAL SPORTING": "sporting_gijon",
    # tenerife
    "TENERIFE": "tenerife",
    # valencia
    "VALENCIA": "valencia",
    # valladolid        (US/TM "REAL VALLADOLID", WK/CE "VALLADOLID")
    "REAL VALLADOLID": "valladolid",
    "VALLADOLID": "valladolid",
    # villarreal
    "VILLARREAL": "villarreal",
    # xerez
    "XEREZ": "xerez",
    # zaragoza          (WK/CE "ZARAGOZA", TM "REAL ZARAGOZA")
    "ZARAGOZA": "zaragoza",
    "REAL ZARAGOZA": "zaragoza",
}


# ---------------------------------------------------------------------------
# 4) API publica
# ---------------------------------------------------------------------------

def _slug(cleaned: str) -> str:
    """Convierte una forma limpia en un slug de reserva (para nombres nuevos)."""
    return re.sub(r"\s+", "_", cleaned.strip().lower())


def to_key(name) -> str:
    """Devuelve la clave canonica de un equipo.

    Si el nombre no esta en el mapa de alias, devuelve un slug de reserva
    (minusculas_con_guiones_bajos) para no romper el pipeline; usa
    `unknown_names()` para detectar estos casos y anadirlos a ALIASES.

    El slug de reserva comparte espacio de nombres con las claves canonicas, de
    modo que un nombre desconocido cuyo slug coincida con una clave existente
    cruzaria sin dejar rastro en el resultado. La deteccion no se delega, por
    tanto, a la forma de la clave: `unknown_names()` y `assert_known()` comprueban
    la pertenencia a ALIASES, no el aspecto del slug, y por eso siguen viendo el
    caso aunque la coincidencia sea exacta. En la capa de normalizacion los
    extractores llaman a `assert_known` con `strict=True`, que es donde esa
    comprobacion se vuelve obligatoria.
    """
    c = clean(name)
    if not c:
        return ""
    return ALIASES.get(c, _slug(c))


def to_display(name) -> str:
    """Devuelve el nombre 'bonito' de un equipo (para tablas y la app)."""
    key = to_key(name)
    return CANONICAL.get(key, str(name))


def _iso_date(s) -> pd.Series:
    # Dos formatos coexisten: football-data usa dd/mm/yy; FBref/Understat usan
    # ISO (YYYY-MM-DD). Parseamos cada grupo por separado para evitar la
    # ambiguedad (y el warning) de la inferencia automatica de pandas.
    ser = pd.Series(s).reset_index(drop=True).astype("string")
    out = pd.Series(pd.NaT, index=ser.index, dtype="datetime64[ns]")

    def _fill(mask, **kw):
        if mask.any():
            parsed = pd.to_datetime(ser[mask], errors="coerce", **kw)
            out.loc[mask] = parsed

    # Parseamos cada formato con un patron EXPLICITO (sin inferencia de pandas,
    # asi no hay avisos y el resultado es determinista):
    # Los patrones se anclan por COMPLETO (`fullmatch`): un patron de prefijo
    # aceptaria una marca de tiempo ISO ('2024-05-01 20:00:00') en la rama de fecha
    # pura y despues fallaria al parsearla con `format="%Y-%m-%d"`, dejando un NaT
    # que se propaga hasta `match_key` y desconecta ese partido de las demas
    # fuentes. Por eso la variante con hora tiene su propia rama explicita.
    done = ser.isna()
    iso = ~done & ser.str.fullmatch(r"\d{4}-\d{2}-\d{2}")        # FBref / Understat
    _fill(iso, format="%Y-%m-%d"); done |= iso
    iso_dt = ~done & ser.str.match(r"^\d{4}-\d{2}-\d{2}[T ]")    # ISO con hora
    if iso_dt.any():
        out.loc[iso_dt] = pd.to_datetime(ser[iso_dt].str.slice(0, 10),
                                         errors="coerce", format="%Y-%m-%d")
    done |= iso_dt
    dmy4 = ~done & ser.str.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}")   # football-data dd/mm/yyyy
    _fill(dmy4, format="%d/%m/%Y"); done |= dmy4
    dmy2 = ~done & ser.str.fullmatch(r"\d{1,2}/\d{1,2}/\d{2}")   # football-data dd/mm/yy
    _fill(dmy2, format="%d/%m/%y"); done |= dmy2
    _fill(~done, dayfirst=True)                                  # resto: best-effort
    return out.dt.strftime("%Y-%m-%d")


def match_key(date, home, away) -> pd.Series:
    """Clave de partido estable y comparable entre fuentes.

    Formato: 'YYYY-MM-DD|home_key|away_key'. Acepta escalares o Series.
    """
    date = pd.Series(date) if not isinstance(date, pd.Series) else date
    home = pd.Series(home) if not isinstance(home, pd.Series) else home
    away = pd.Series(away) if not isinstance(away, pd.Series) else away
    d = _iso_date(date).reset_index(drop=True)
    h = home.reset_index(drop=True).map(to_key)
    a = away.reset_index(drop=True).map(to_key)
    return d.str.cat([h, a], sep="|")


def add_keys(
    df: pd.DataFrame,
    home_col: str = "HomeTeam",
    away_col: str = "AwayTeam",
    date_col: str = "Date",
) -> pd.DataFrame:
    """Anade columnas home_key / away_key / match_key a un DataFrame de partidos."""
    out = df.copy()
    out["home_key"] = out[home_col].map(to_key)
    out["away_key"] = out[away_col].map(to_key)
    if date_col in out.columns:
        out["match_key"] = match_key(out[date_col], out[home_col], out[away_col])
    return out


def unknown_names(names: Iterable) -> list[str]:
    """Devuelve la lista de nombres cuya forma limpia NO esta en ALIASES.

    Util para revisar cada temporada nueva o cada fuente nueva (p.ej. FBref):
    si aparece un nombre desconocido, se anade a ALIASES antes de fiarse del merge.
    """
    out = []
    for n in names:
        c = clean(n)
        if c and c not in ALIASES:
            out.append(n)
    return sorted(set(out))


def assert_known(names: Iterable, *, source: str = "teamnames") -> None:
    """Falla RUIDOSAMENTE si algun nombre no esta en el mapa de alias.

    `to_key` degrada con gracia a un slug de reserva para no romper el scraping,
    pero en la capa de NORMALIZACION un slug no canonico significa que ese equipo
    NO va a cruzar con las demas fuentes: mejor parar el pipeline y anadir el
    alias que publicar un parquet con huecos silenciosos. El mensaje lista los
    nombres exactos y donde arreglarlo, para que la correccion sea de un minuto.
    """
    bad = unknown_names(names)
    if bad:
        raise ValueError(
            f"[{source}] {len(bad)} nombre(s) de equipo SIN clave canonica: {bad}. "
            f"Formas limpias: {[clean(b) for b in bad]}. "
            "Anadelos a teamnames.ALIASES (apuntando a su clave de CANONICAL; "
            "crea la entrada en CANONICAL si es un club nuevo en Primera) y "
            "re-ejecuta 'process'. El crudo descargado esta intacto."
        )
