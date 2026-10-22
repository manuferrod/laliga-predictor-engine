"""Estado de salud de las fuentes, ANTES de empezar la jornada.

Motivacion
----------
La rutina semanal se detiene en el primer paso que falla, y eso es correcto: un
artefacto a medias contamina todo lo que viene detras. Pero tiene un efecto
secundario incomodo. Si Sofascore no responde un domingo, la cadena muere en el
paso 1 y del resto no se sabe nada: puede que football-data tambien este caido,
puede que Understat haya vuelto a cambiar su endpoint, o puede que todo lo demas
este perfecto y solo haya que esperar diez minutos. Diagnosticar eso a base de
reejecutar la cadena cuesta una tarde.

Esta sonda recorre las OCHO puertas de red de las que cuelgan los diez pasos de
extraccion, una peticion barata por puerta, sin escribir una sola fila en el
almacen crudo, y entrega un parte: que fuente esta bien, cual va por su via de
respaldo y cual esta caida, con el motivo y con el remedio previsto. Es el paso
0 de la rutina.

Que distingue un estado de otro
-------------------------------
* OK         : la via principal responde y lo que devuelve tiene la forma
               esperada.
* DEGRADADA  : hay dato, pero no por donde deberia -el espejo de Sofascore en
               vez del host canonico, el HTML de Understat en vez de su JSON-, o
               el dato llega mas viejo de lo que deberia. La cadena puede correr;
               alguien tiene que mirarlo.
* CAIDA      : no hay dato utilizable.

El estado se decide por la FORMA de la respuesta, no solo por el codigo HTTP. El
fallo que de verdad hace dano no es el 502, que se ve: es el 200 que devuelve un
JSON al que le han cambiado las claves, porque ese se consolida en silencio y
aparece tres pasos despues como columnas vacias sin motivo aparente.

Que aborta la jornada y que no
------------------------------
Degradar con aviso es aceptable; lo que no es aceptable es fallar sin saber por
que. Asi que la sonda solo devuelve codigo de error cuando cae una fuente
CRITICA, es decir, una sin la cual el resto de la cadena no tiene nada que
hacer: el calendario de Sofascore, del que cuelgan fatiga, bajas y alineaciones;
el resultado de football-data, que ES el objetivo del modelo; y su fichero de
cuotas, sin el cual no hay jornada por jugar que predecir. Que ClubElo o
Transfermarkt esten caidos degrada unas columnas, y eso se avisa y se sigue.

Uso
---
    python scripts/extractors/sonda.py                 # las ocho puertas
    python scripts/extractors/sonda.py --solo clubelo  # una sola
    python scripts/extractors/sonda.py --exigente      # cualquier aviso aborta
    python scripts/extractors/sonda.py --permisivo     # informa y nunca aborta

Deja el parte en `outputs/sonda/fuentes.json` y una linea por ejecucion en
`outputs/sonda/historico.jsonl`. El historico es el que contesta la pregunta que
importa cuando algo falla por tercera semana seguida: esto es de hoy o lleva
roto un mes.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_RAW = _REPO / "data" / "01_raw"
_OUT = _REPO / "outputs" / "sonda"
_INFORME = _OUT / "fuentes.json"
_HISTORICO = _OUT / "historico.jsonl"

OK, DEGRADADA, CAIDA = "OK", "DEGRADADA", "CAIDA"

# Cuantos dias puede ir atrasado el ultimo dato de una fuente antes de que su
# frescura deje de ser creible. La cadencia es semanal, asi que diez dias es una
# jornada perdida mas margen: por debajo de eso no hay nada que avisar.
_DIAS_FRESCURA = 10


@dataclass
class Diagnostico:
    """El parte de UNA puerta de red: estado, por donde ha entrado y que hacer."""

    fuente: str
    estado: str
    via: str = ""
    detalle: str = ""
    remedio: str = ""
    pasos: tuple[str, ...] = ()
    critica: bool = False
    segundos: float = 0.0

    @property
    def bloquea(self) -> bool:
        return self.critica and self.estado == CAIDA


# =========================================================================== #
#  Sondas. Ninguna lanza: cualquier excepcion se traduce a CAIDA con su motivo,
#  porque una sonda que se cae a mitad deja de informar justo cuando hace falta.
# =========================================================================== #
def _ultimo_id(csv: Path, columna: str) -> int | None:
    """Un id reciente del almacen crudo, para sondear un endpoint por partido.

    Se lee de disco a proposito: descubrir un id en vivo costaria recorrer el
    calendario entero, que es precisamente lo que la sonda no debe hacer.
    """
    import pandas as pd
    try:
        df = pd.read_csv(csv, usecols=[columna])
    except (OSError, ValueError):
        return None
    ids = pd.to_numeric(df[columna], errors="coerce").dropna()
    return int(ids.max()) if len(ids) else None


def _sonda_sofascore() -> Diagnostico:
    """Calendario de LaLiga: la puerta de la que cuelga media cadena."""
    import sofascore as sofa
    ses = sofa._new_session()
    try:
        temporadas = sofa._fetch_seasons(ses)
        via = ses.strategies[ses.strategy_idx][0]
        anios = {sofa._season_year(t) for t in temporadas}
        vigente = date.today().year if date.today().month >= 7 else date.today().year - 1
        if vigente not in anios:
            return Diagnostico(
                "sofascore", DEGRADADA, via,
                f"responde con {len(temporadas)} temporadas pero la vigente "
                f"({vigente}) no esta entre ellas: {sorted(anios)[-4:]}",
                "mira si Sofascore ha movido el id del torneo "
                "(LALIGA_TOURNAMENT_ID en sofascore.py)")
        if ses.strategy_idx > 0:
            return Diagnostico(
                "sofascore", DEGRADADA, via,
                f"{len(temporadas)} temporadas, pero el host canonico no ha "
                f"servido: la cascada ha escalado a '{via}'",
                "no hay que hacer nada hoy; si se vuelve permanente, el espejo "
                "es oficioso y conviene no depender de el")
        return Diagnostico("sofascore", OK, via,
                           f"{len(temporadas)} temporadas, la vigente incluida")
    except Exception as e:  # noqa: BLE001
        return Diagnostico(
            "sofascore", CAIDA, "",
            f"{type(e).__name__}: {e}",
            "las tres estrategias han fallado. Comprueba la red y luego prueba "
            "la ultima a mano (Selenium necesita Chrome instalado)")
    finally:
        ses.close()


def _sonda_sofascore_lineups() -> Diagnostico:
    """El endpoint `/event/{id}/lineups`: de el salen bajas y alineaciones."""
    import sofascore as sofa
    mid = _ultimo_id(_RAW / "sofascore" / "sofascore_laliga_match_stats.csv",
                     "match_id")
    if mid is None:
        return Diagnostico(
            "sofascore_lineups", DEGRADADA, "",
            "no hay ningun match_id en el almacen crudo con el que sondear",
            "es un hueco de la sonda, no de la fuente: ejecuta antes "
            "`python scripts/extractors/sofascore.py weekly`")
    ses = sofa._new_session()
    try:
        res = sofa._fetch_json(ses, f"/api/v1/event/{mid}/lineups")
        via = ses.strategies[ses.strategy_idx][0]
        if not res.get("ok"):
            return Diagnostico(
                "sofascore_lineups", CAIDA, via,
                f"evento {mid}: {res.get('err')}",
                "sin este endpoint fallan 'bajas' y 'alineaciones'. Si el "
                "calendario si responde, el problema es del endpoint, no del host")
        data = res.get("data") or {}
        faltan = [k for k in ("home", "away") if k not in data]
        if faltan:
            return Diagnostico(
                "sofascore_lineups", CAIDA, via,
                f"evento {mid}: 200 sin {faltan} (claves: {sorted(data)[:8]})",
                "Sofascore ha cambiado la forma del payload: hay que revisar "
                "`availability._parse_*` y `lineups._parse_*`")
        bajas = data.get("home", {}).get("missingPlayers")
        if bajas is None:
            return Diagnostico(
                "sofascore_lineups", DEGRADADA, via,
                f"evento {mid}: alineaciones si, pero sin 'missingPlayers'",
                "puede ser normal en un partido antiguo. Si pasa tambien con un "
                "partido de esta semana, 'bajas' se quedara a cero y hay que "
                "revisar availability.py")
        return Diagnostico("sofascore_lineups", OK, via,
                           f"evento {mid}: alineaciones y bajas presentes")
    except Exception as e:  # noqa: BLE001
        return Diagnostico("sofascore_lineups", CAIDA, "",
                           f"{type(e).__name__}: {e}",
                           "sin este endpoint fallan 'bajas' y 'alineaciones'")
    finally:
        ses.close()


def _sonda_footballdata() -> Diagnostico:
    """El CSV de la temporada: resultado (el objetivo) y cuotas de las casas."""
    import footballdata as fd
    anio = fd._current_season_year()
    url = f"{fd.BASE_URL}/{fd._season_code(anio)}/{fd.LEAGUE_CODE}.csv"
    try:
        status, body = fd._http_get_bytes(url, headers=fd._HEADERS)
    except Exception as e:  # noqa: BLE001
        return Diagnostico("footballdata", CAIDA, url, f"{type(e).__name__}: {e}",
                           "sin resultado no hay objetivo que modelar")
    if status != 200:
        return Diagnostico(
            "footballdata", CAIDA, url, f"HTTP {status}",
            "un 404 en agosto es normal (la temporada no ha abierto); en "
            "mitad de temporada significa que han movido la ruta del CSV")
    try:
        df = fd.parse_season_csv(body, anio)
    except Exception as e:  # noqa: BLE001
        return Diagnostico("footballdata", CAIDA, url,
                           f"200 pero el CSV no se deja parsear: {e}",
                           "han cambiado la cabecera: revisa `_clean_season_frame`")
    jugados = int(df["FTR"].notna().sum()) if "FTR" in df.columns else 0
    if not jugados:
        return Diagnostico(
            "footballdata", DEGRADADA, url,
            f"{len(df)} filas, ninguna con resultado",
            "si la jornada 1 ya se jugo, la columna FTR ha cambiado de nombre")
    return Diagnostico("footballdata", OK, url,
                       f"temporada {fd._season_label(anio)}: {len(df)} partidos, "
                       f"{jugados} con resultado")


def _sonda_footballdata_fixtures() -> Diagnostico:
    """`fixtures.csv`: el precio de la jornada POR JUGAR, que es la que se predice."""
    import footballdata as fd
    try:
        status, body = fd._http_get_bytes(fd.FIXTURES_URL, headers=fd._HEADERS)
    except Exception as e:  # noqa: BLE001
        return Diagnostico("footballdata_fixtures", CAIDA, fd.FIXTURES_URL,
                           f"{type(e).__name__}: {e}",
                           "sin cuotas no hay jornada que predecir: 'predict' falla")
    if status != 200:
        return Diagnostico("footballdata_fixtures", CAIDA, fd.FIXTURES_URL,
                           f"HTTP {status}",
                           "football-data publica este fichero de jueves a domingo; "
                           "fuera de esa ventana puede no estar")
    try:
        df = fd.parse_fixtures_csv(body)
    except Exception as e:  # noqa: BLE001
        return Diagnostico("footballdata_fixtures", CAIDA, fd.FIXTURES_URL,
                           f"200 pero no se deja parsear: {e}",
                           "revisa `_clean_fixtures_frame`")
    if df.empty:
        return Diagnostico(
            "footballdata_fixtures", DEGRADADA, fd.FIXTURES_URL,
            "el fichero existe pero no trae ningun partido de LaLiga",
            "normal a mitad de semana o en paron de selecciones. Si la jornada "
            "es este fin de semana, 'predict' no tendra partidos que predecir")
    return Diagnostico("footballdata_fixtures", OK, fd.FIXTURES_URL,
                       f"{len(df)} partidos de LaLiga con precio")


def _sonda_understat() -> Diagnostico:
    """xG de la temporada vigente. Dos vias: el JSON nuevo y el HTML clasico."""
    import understat as us
    anio = us._current_season_year()
    api = f"{us._API_URL}/{us.LEAGUE_SLUG}/{anio}"
    try:
        dates, teams = us._fetch_season_payload(anio)
    except Exception as e:  # noqa: BLE001
        return Diagnostico(
            "understat", CAIDA, api, f"{type(e).__name__}: {e}",
            "Understat rehizo su web en dic-2025 y puede haberlo vuelto a hacer: "
            "abre la pagina de la liga y mira en DevTools (Red/XHR) de donde "
            "carga ahora los datos")
    if not dates or not teams:
        return Diagnostico("understat", CAIDA, api,
                           f"respuesta vacia: {len(dates)} partidos, {len(teams)} equipos",
                           "temporada sin datos todavia, o cambio de formato")
    # Cual de las dos vias ha contestado: solo la principal es OK del todo.
    try:
        cab = {**us._HEADERS, "Accept": "application/json",
               "X-Requested-With": "XMLHttpRequest",
               "Referer": f"{us._BASE_URL}/{us.LEAGUE_SLUG}/{anio}"}
        status, _ = us._http_get(api, headers=cab)
    except Exception:  # noqa: BLE001
        status = 0
    if status != 200:
        return Diagnostico(
            "understat", DEGRADADA, f"HTML {us._BASE_URL}/{us.LEAGUE_SLUG}/{anio}",
            f"hay dato ({len(dates)} partidos) pero el endpoint JSON responde "
            f"{status}: ha entrado por el HTML de respaldo",
            "la cadena corre igual. Conviene mirar si el JSON ha cambiado de "
            "ruta antes de que tambien se caiga el HTML")
    jugados = sum(1 for d in dates if d.get("isResult"))
    return Diagnostico("understat", OK, api,
                       f"temporada {anio}: {len(dates)} partidos ({jugados} "
                       f"jugados), {len(teams)} equipos")


def _sonda_understat_shots() -> Diagnostico:
    """Remate a remate de un partido: la calidad de ocasion."""
    import understat_shots as ush
    mid = _ultimo_id(_RAW / "understat" / "understat_laliga_match_stats.csv",
                     "match_id")
    if mid is None:
        return Diagnostico(
            "understat_shots", DEGRADADA, "",
            "no hay ningun match_id de Understat con el que sondear",
            "hueco de la sonda, no de la fuente: ejecuta antes "
            "`python scripts/extractors/understat.py weekly`")
    try:
        payload = ush._fetch_match_shots(mid)
        filas = ush.parse_shots(payload, mid)
    except Exception as e:  # noqa: BLE001
        return Diagnostico(
            "understat_shots", CAIDA, f"{ush._BASE_MATCH_URL}/{mid}",
            f"{type(e).__name__}: {e}",
            "'remates' es opcional: la cadena se completa con --sin-opcionales "
            "y sus columnas llegan a nulo y se podan")
    if not filas:
        return Diagnostico(
            "understat_shots", DEGRADADA, f"{ush._BASE_MATCH_URL}/{mid}",
            f"partido {mid}: responde pero sin ningun remate",
            "puede ser un partido sin datos. Si pasa con varios, han cambiado "
            "la forma de `shotsData`")
    return Diagnostico("understat_shots", OK, f"{ush._BASE_MATCH_URL}/{mid}",
                       f"partido {mid}: {len(filas)} remates")


def _sonda_clubelo() -> Diagnostico:
    """Rating de club, por la web. Se comprueba IDENTIDAD, censo y FRESCURA.

    La API (`api.clubelo.com`) esta jubilada: devuelve 502 en todas sus rutas y
    el sitio nuevo no publica CSV ni la menciona. La via viva es la web, asi que
    es la web lo que hay que sondear.

    Tres cosas se miran, y ninguna es el codigo HTTP:

    * IDENTIDAD. Esta web responde 200 a cualquier ruta desconocida, sirviendo
      la portada. Un 200 no prueba nada, asi que la sonda pide el censo de /ESP
      y deja que `parse_censo_web` aplique sus guardianes (<h1> no vacio y
      FedURL de la federacion); si /ESP dejase de existir, lo que llegaria seria
      el ranking mundial con 200 y buena cara.
    * CENSO. Que los 20 de Primera esten y con slug resuelto, porque de ahi sale
      el descubrimiento de clubes: un censo corto deja sin Elo a los recien
      ascendidos justo en las jornadas que se predicen.
    * FRESCURA, medida por la fecha que el SITIO se atribuye en el enlace de su
      titulo, no por la del ultimo partido del club. Es la unica fuente que se
      une al maestro por `merge_asof`, o sea cogiendo el ultimo Elo anterior a
      la fecha del partido, asi que una fuente congelada pega dato viejo a
      partidos nuevos sin hacer ruido. Pero la fecha del ultimo partido NO mide
      eso: medido, en octubre de 2026 toda Primera lleva tres semanas sin punto
      (del 20-sep al 10-oct) mientras Segunda sigue jugando, y la fuente esta
      perfectamente al dia. Tomar ese hueco por atraso daria un aviso falso cada
      paron de selecciones. El arrastre de dato viejo hasta un partido ya
      jugado lo caza `qa_master` [6], que es donde se puede ver de verdad.
    """
    import clubelo as ce
    import pandas as pd
    url = f"{ce.CLUB_ELO_WEB}/{ce.WEB_FED}"
    remedio_caida = (
        "sin Elo, la cadena corre y las columnas derivadas llegan a nulo; lo que "
        "NO se puede hacer es tirar de `--via api`, que esta jubilada (502 en "
        "todas sus rutas). Abre la URL en el navegador: si la pagina esta en pie "
        "pero la sonda la rechaza, ha cambiado de forma y hay que ajustar "
        "`parse_censo_web`. Mientras, el Elo del maestro se queda en el ultimo "
        "conocido y el arrastre lo caza `qa_master` [6]")
    try:
        censo = ce.parse_censo_web(ce._fetch_web(ce.WEB_FED))
    except Exception as e:  # noqa: BLE001
        return Diagnostico("clubelo", CAIDA, url, f"{type(e).__name__}: {e}",
                           remedio_caida)

    top = censo[censo["level"] == ce.TOP_LEVEL]
    sin_slug = top["slug"].isna().sum()
    if len(top) < ce._WEB_MIN_CENSO or sin_slug:
        return Diagnostico(
            "clubelo", DEGRADADA, url,
            f"censo con {len(top)} clubes de Primera y {sin_slug} sin slug",
            "el descubrimiento intentara resolver los slugs que falten por "
            "candidatos; si alguno no sale, ese club puntuara con Elo nulo. "
            "Revisa la cobertura al final de `clubelo.py weekly`")

    # Un club concreto para medir frescura y comprobar que la grafica sigue
    # parseandose: el primero del censo por Elo, el que menos semanas se pasa sin
    # jugar. La pagina de club es ademas la que trae la fecha del sitio; la de
    # federacion no la lleva.
    fila = top.sort_values("elo", ascending=False).iloc[0]
    url_club = f"{ce.CLUB_ELO_WEB}/{fila.slug}"
    try:
        html = ce._fetch_web(fila.slug)
        hist = ce.parse_historia_web(html, team=fila.team, level=fila.level)
    except Exception as e:  # noqa: BLE001
        return Diagnostico(
            "clubelo", DEGRADADA, url_club,
            f"el censo responde, pero la serie de {fila.team} no: "
            f"{type(e).__name__}: {e}",
            "si pasa con un solo club puede ser su pagina; si pasa con varios, "
            "ha cambiado la grafica y hay que ajustar `parse_historia_web`")

    if hist.empty or not ce.es_pagina_de_club(html, clave=fila.team_key):
        return Diagnostico(
            "clubelo", DEGRADADA, url_club,
            f"/{fila.slug} responde 200 pero no es la serie de {fila.team}: "
            f"titulo {ce._web_titulo(html)!r}, {len(hist)} tramos",
            "o ha cambiado el slug en la tabla del Ranking de /" + ce.WEB_FED +
            " o la ruta ha caido a la portada. Borra "
            "`data/01_raw/clubelo/clubelo_slugs_web.json` para que el "
            "descubrimiento vuelva a resolver los slugs desde cero")

    # `from` es el dia SIGUIENTE al partido (el Elo que la web fecha en D es el
    # posterior a D), asi que el partido es un dia antes del tramo que abre.
    ultimo = pd.to_datetime(hist["from"], errors="coerce").max()
    ultimo_partido = ("?" if pd.isna(ultimo)
                      else str((ultimo - pd.Timedelta(days=1)).date()))

    fecha_sitio = ce._web_fecha_sitio(html)
    corte = pd.to_datetime(fecha_sitio, errors="coerce")
    if pd.isna(corte):
        return Diagnostico(
            "clubelo", DEGRADADA, url_club,
            f"{len(hist)} tramos (ultimo partido el {ultimo_partido}), pero el "
            "titulo no trae la fecha del sitio",
            "sin esa fecha no se puede distinguir una fuente congelada de un "
            "paron de calendario. Revisa `_web_fecha_sitio`; entretanto, "
            "apoyate en la invariante 6 de `qa_master`")

    atraso = (pd.Timestamp(date.today()) - corte.normalize()).days
    if atraso > _DIAS_FRESCURA:
        return Diagnostico(
            "clubelo", DEGRADADA, url_club,
            f"responde, pero el sitio tiene el Elo calculado hasta el "
            f"{corte.date()} ({atraso} dias de atraso)",
            "la fuente esta congelada: el Elo se une por `merge_asof`, asi que "
            "un dato viejo se pega sin ruido a los partidos nuevos. "
            "`qa_master` [6] lo deberia cazar; si no, no publiques esta "
            "jornada con Elo")
    return Diagnostico(
        "clubelo", OK, url,
        f"censo con {len(top)} de Primera; sitio al dia {corte.date()}; "
        f"{fila.team}: {len(hist)} tramos, ultimo partido el {ultimo_partido}")


def _sonda_transfermarkt() -> Diagnostico:
    """Valor de plantilla. Entra al maestro con retardo de una temporada."""
    import transfermarkt as tm
    anio = tm.sofa_current_year()
    url = tm._comp_url(anio)
    try:
        status, body = tm._http_get(url)
    except Exception as e:  # noqa: BLE001
        return Diagnostico("transfermarkt", CAIDA, url, f"{type(e).__name__}: {e}",
                           "el valor de plantilla entra con retardo t-1, asi que "
                           "una semana sin el no cambia nada del maestro")
    if status != 200:
        return Diagnostico(
            "transfermarkt", CAIDA, url, f"HTTP {status}",
            "Transfermarkt bloquea por fingerprint: si es 403, comprueba que "
            "curl_cffi esta instalado (es el que impersona Chrome)")
    try:
        clubs = tm.parse_competition_clubs(body, anio)
    except Exception as e:  # noqa: BLE001
        return Diagnostico("transfermarkt", CAIDA, url,
                           f"200 pero el HTML no se deja parsear: {e}",
                           "han rehecho la tabla de la competicion: revisa "
                           "`parse_competition_clubs`")
    if len(clubs) < 18:
        return Diagnostico(
            "transfermarkt", DEGRADADA, url,
            f"solo {len(clubs)} clubes detectados (LaLiga tiene 20)",
            "el selector de la tabla ya no coge todas las filas: con clubes de "
            "menos, los que falten se quedan sin valor de plantilla")
    return Diagnostico("transfermarkt", OK, url,
                       f"temporada {anio}: {len(clubs)} clubes en la competicion")


# =========================================================================== #
#  Registro: que puerta alimenta que pasos de la rutina, y cual es critica
# =========================================================================== #
@dataclass(frozen=True)
class Puerta:
    nombre: str
    sonda: object
    pasos: tuple[str, ...]
    critica: bool = False


PUERTAS: tuple[Puerta, ...] = (
    Puerta("sofascore", _sonda_sofascore, ("sofascore", "fatiga"), critica=True),
    Puerta("sofascore_lineups", _sonda_sofascore_lineups,
           ("bajas", "alineaciones"), critica=True),
    Puerta("footballdata", _sonda_footballdata, ("footballdata",), critica=True),
    Puerta("footballdata_fixtures", _sonda_footballdata_fixtures, ("cuotas",),
           critica=True),
    Puerta("understat", _sonda_understat, ("understat",)),
    Puerta("understat_shots", _sonda_understat_shots, ("remates",)),
    Puerta("clubelo", _sonda_clubelo, ("clubelo",)),
    Puerta("transfermarkt", _sonda_transfermarkt, ("transfermarkt",)),
)

_NOMBRES = tuple(p.nombre for p in PUERTAS)


# =========================================================================== #
#  Ejecucion e informe
# =========================================================================== #
def sondea(puertas: tuple[Puerta, ...] = PUERTAS) -> list[Diagnostico]:
    """Recorre las puertas dadas. No se detiene en ningun fallo: eso es el punto."""
    partes: list[Diagnostico] = []
    for puerta in puertas:
        print(f"  sondeando {puerta.nombre} ...", flush=True)
        t0 = time.perf_counter()
        try:
            d = puerta.sonda()
        except Exception as e:  # noqa: BLE001  una sonda rota no calla las demas
            d = Diagnostico(puerta.nombre, CAIDA, "",
                            f"la sonda misma ha fallado: {type(e).__name__}: {e}",
                            "esto es un fallo del codigo de la sonda, no "
                            "necesariamente de la fuente")
        d.segundos = time.perf_counter() - t0
        d.pasos, d.critica = puerta.pasos, puerta.critica
        partes.append(d)
    return partes


def informa(partes: list[Diagnostico]) -> None:
    """El parte, en una tabla que se lee de un vistazo."""
    print("\n" + "=" * 78)
    print(f"{'fuente':<24}{'estado':<12}{'pasos que alimenta':<26}{'tiempo':>8}")
    print("-" * 78)
    for d in partes:
        marca = "!" if d.bloquea else " "
        print(f"{marca}{d.fuente:<23}{d.estado:<12}"
              f"{', '.join(d.pasos):<26}{d.segundos:>7.1f}s")
    print("=" * 78)

    for d in partes:
        if d.estado == OK:
            continue
        cabecera = "CAIDA" if d.estado == CAIDA else "AVISO"
        if d.bloquea:
            cabecera = "CAIDA CRITICA"
        print(f"\n[{cabecera}] {d.fuente}")
        if d.via:
            print(f"  via     : {d.via}")
        print(f"  motivo  : {d.detalle}")
        if d.remedio:
            print(f"  remedio : {d.remedio}")


def _corta(ruta: Path) -> str:
    """Ruta relativa al repo si cuelga de el, y absoluta si no.

    La bateria desvia el parte a un temporal, que no cuelga del repo: avisar de
    donde quedo el parte no puede ser lo que tumbe el sondeo.
    """
    try:
        return str(ruta.relative_to(_REPO))
    except ValueError:
        return str(ruta)


def guarda(partes: list[Diagnostico]) -> None:
    """Deja el parte de hoy y una linea en el historico. No es critico que falle."""
    informe = {
        "momento": datetime.now().isoformat(timespec="seconds"),
        "resumen": {e: sum(1 for d in partes if d.estado == e)
                    for e in (OK, DEGRADADA, CAIDA)},
        "bloquea": [d.fuente for d in partes if d.bloquea],
        "fuentes": [asdict(d) | {"pasos": list(d.pasos)} for d in partes],
    }
    try:
        _OUT.mkdir(parents=True, exist_ok=True)
        _INFORME.write_text(json.dumps(informe, ensure_ascii=False, indent=2),
                            encoding="utf-8")
        with _HISTORICO.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(
                {"momento": informe["momento"],
                 "estados": {d.fuente: d.estado for d in partes}},
                ensure_ascii=False) + "\n")
        print(f"\nparte: {_corta(_INFORME)}  |  historico: {_corta(_HISTORICO)}")
    except OSError as e:
        print(f"\n[WARN] no he podido guardar el parte: {e}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Estado de salud de las fuentes, antes de ejecutar la jornada.")
    p.add_argument("--solo", metavar="FUENTE", nargs="+",
                   help=f"sondea solo esas fuentes ({', '.join(_NOMBRES)})")
    p.add_argument("--exigente", action="store_true",
                   help="aborta tambien con fuentes degradadas, no solo caidas")
    p.add_argument("--permisivo", action="store_true",
                   help="informa y devuelve 0 siempre: diagnostico sin frenar nada")
    args = p.parse_args(argv)

    puertas = PUERTAS
    if args.solo:
        desconocidas = [f for f in args.solo if f not in _NOMBRES]
        if desconocidas:
            raise SystemExit(f"no conozco {desconocidas}. Las fuentes son: "
                             + ", ".join(_NOMBRES))
        puertas = tuple(q for q in PUERTAS if q.nombre in set(args.solo))

    print("=" * 78)
    print(f"SONDA DE FUENTES  |  {len(puertas)} de {len(PUERTAS)} puertas de red")
    print("=" * 78)
    partes = sondea(puertas)
    informa(partes)
    guarda(partes)

    bloqueantes = [d for d in partes if d.bloquea]
    avisos = [d for d in partes if d.estado == DEGRADADA]
    if args.permisivo:
        print("\n--permisivo: no freno la cadena.")
        return 0
    if bloqueantes:
        print("\nLa jornada NO puede ejecutarse: "
              + ", ".join(d.fuente for d in bloqueantes)
              + ". Sin estas fuentes los pasos siguientes no tendrian dato nuevo "
                "que consolidar y el maestro saldria con la jornada anterior.")
        return 1
    if avisos and args.exigente:
        print("\n--exigente: " + ", ".join(d.fuente for d in avisos)
              + " van degradadas y freno aqui.")
        return 1
    if avisos:
        print("\nSe puede ejecutar la jornada, con "
              + ", ".join(d.fuente for d in avisos) + " degradada(s).")
    else:
        print("\nLas ocho puertas en orden. Adelante con la jornada.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
