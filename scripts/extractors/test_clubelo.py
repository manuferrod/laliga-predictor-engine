"""Validacion offline de clubelo.py (parseo, sondeo y cobertura). Sin red.

Cubre las capas puras del extractor por sus dos vias:

  * via API (jubilada): parseo del CSV de ClubElo y filtrado de la foto por
    fecha;
  * via web (la viva): extraccion del especificador Vega-Lite incrustado,
    guardian de identidad de pagina, lectura del censo de /ESP y traduccion de
    la serie a tramos [from, to] SIN fuga de informacion;
  * comun a las dos: fecha de sondeo del descubrimiento de clubes, asercion de
    cobertura de la temporada vigente (el fallo que dejaba a los recien
    ascendidos sin Elo en las primeras jornadas, justo las que se predicen) y
    guardian de escala unica.

Los HTML de la via web son sinteticos pero con la FORMA del real: `datasets`
incrustado dentro del especificador, el literal `const eloData` con los enlaces
de slug, y la portada que la web sirve con 200 ante cualquier ruta invalida.
Sinteticos a proposito: las paginas reales pesan ~720 KB cada una y lo que hay
que fijar aqui es el contrato de parseo, no una cosecha concreta.
"""
import sys, pathlib
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "clubelo.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import clubelo as ce
import _common

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


# --------------------------------------------------------------------------- #
print("== _parse_clubelo_csv / _laliga_clubs_from_snapshot ==")
SNAP = (
    "Rank,Club,Country,Level,Elo,From,To\n"
    "1,Real Madrid,ESP,1,2050.1,2026-09-01,2026-09-08\n"
    "2,Barcelona,ESP,1,2010.5,2026-09-01,None\n"
    "40,Levante,ESP,2,1600.0,2026-09-01,None\n"
    "5,Arsenal,ENG,1,1950.0,2026-09-01,None\n"
)
snap = ce._parse_clubelo_csv(SNAP)
check("columnas normalizadas a minusculas",
      list(snap.columns) == ["rank", "club", "country", "level", "elo", "from", "to"],
      list(snap.columns))
check("'None' textual -> NaT en 'to'", pd.isna(snap.loc[1, "to"]), snap.loc[1, "to"])
check("elo numerico", snap["elo"].dtype.kind == "f")
check("foto -> solo Primera espanola",
      ce._laliga_clubs_from_snapshot(SNAP) == ["Barcelona", "Real Madrid"],
      ce._laliga_clubs_from_snapshot(SNAP))
check("CSV con solo cabecera -> vacio con columnas",
      ce._parse_clubelo_csv("Rank,Club,Country,Level,Elo,From,To\n").empty)
check("CSV vacio -> vacio", ce._parse_clubelo_csv("").empty)
check("_url_name deacentua y compacta",
      ce._url_name("Atlético Madrid") == "AtleticoMadrid", ce._url_name("Atlético Madrid"))


# --------------------------------------------------------------------------- #
print("\n== fecha de sondeo: la temporada VIGENTE tambien se sondea ==")
# (temporada, hoy, esperado). Cerradas -> 1-oct; vigente arrancada -> hoy;
# temporada sin empezar -> None (nada que sondear).
for y, hoy, esperado in [
    (2016, "2026-07-27", "2016-10-01"),
    (2025, "2026-07-27", "2025-10-01"),
    (2026, "2026-07-27", None),
    (2026, "2026-08-01", "2026-08-01"),
    (2026, "2026-08-20", "2026-08-20"),
    (2026, "2026-09-30", "2026-09-30"),
    (2026, "2026-10-01", "2026-10-01"),
    (2026, "2027-03-01", "2026-10-01"),
    (2027, "2026-08-20", None),
]:
    got = ce._season_probe_date(y, today=hoy)
    check(f"temporada {y} sondeada el {hoy} -> {esperado!r}", got == esperado, repr(got))

check("agosto de la temporada vigente SI se sondea (arranque de liga)",
      ce._season_probe_date(2026, today="2026-08-20") is not None)


# --------------------------------------------------------------------------- #
print("\n== cobertura de la temporada vigente ==")

def master(n_primera, *, hasta=None, extra=()):
    """Master sintetico: `n_primera` clubes de Primera con tramo desde agosto."""
    filas = [{"team_key": f"club{i:02d}", "country": "ESP", "level": 1,
              "from": "2026-08-05", "to": hasta} for i in range(n_primera)]
    return pd.DataFrame(filas + list(extra))

HOY = "2026-09-15"
check("20 clubes con tramo abierto -> 20 vigentes",
      len(ce.current_season_clubs(master(20), today=HOY)) == 20)
check("tramo cerrado antes de hoy -> no vigente",
      ce.current_season_clubs(master(20, hasta="2026-09-01"), today=HOY) == [])
check("tramo cerrado despues de hoy -> vigente",
      len(ce.current_season_clubs(master(20, hasta="2026-09-20"), today=HOY)) == 20)

ruido = [{"team_key": "segunda", "country": "ESP", "level": 2,
          "from": "2026-08-05", "to": None},
         {"team_key": "premier", "country": "ENG", "level": 1,
          "from": "2026-08-05", "to": None}]
check("Segunda y otras ligas no cuentan",
      len(ce.current_season_clubs(master(20, extra=ruido), today=HOY)) == 20)
check("tramo que empieza en el futuro no cuenta",
      len(ce.current_season_clubs(
          master(19, extra=[{"team_key": "asc", "country": "ESP", "level": 1,
                             "from": "2026-10-01", "to": None}]), today=HOY)) == 19)

# ventana de exigencia: (n, hoy, strict, debe_fallar)
for n, hoy, strict, debe in [
    (20, "2026-09-15", True, False),
    (18, "2026-09-15", True, False),   # justo el minimo tolerado
    (17, "2026-09-15", True, True),    # falta un ascendido -> error ruidoso
    (17, "2026-09-15", False, False),  # strict=False -> solo aviso
    (17, "2026-08-20", True, False),   # antes del 1-sep -> solo aviso
    (0,  "2026-07-27", True, False),   # temporada sin empezar -> no se exige
    (0,  "2026-06-30", True, False),   # temporada terminada -> no se exige
]:
    try:
        ce.assert_current_season_coverage(master(n), strict=strict, today=hoy)
        fallo = False
    except _common.SchemaError:
        fallo = True
    check(f"n={n} hoy={hoy} strict={strict} -> error={debe}", fallo == debe)

try:
    ce.assert_current_season_coverage(pd.DataFrame(), today="2026-11-01")
    vacio_falla = False
except _common.SchemaError:
    vacio_falla = True
check("master vacio en plena temporada -> error", vacio_falla)


# --------------------------------------------------------------------------- #
# VIA WEB -- fixtures con la forma del HTML real
# --------------------------------------------------------------------------- #

def html_club(titulo, puntos):
    """Pagina de club: <h1> con enlace dentro y serie en `datasets`."""
    filas = ",\n".join(
        '      {"Date": "%sT00:00:00", "Elo": %s, "Golo": 1.09, "segment_id": 0}'
        % (d, e) for d, e in puntos)
    return (
        "<html><body>\n"
        f'<h1><a href="/2026-10-08/{titulo}">{titulo}</a></h1>\n'
        "<script>\n{\n"
        '  "mark": {"type": "line", "point": {"size": 30}},\n'
        '  "data": {"name": "data-abc"},\n'
        '  "datasets": {\n    "data-abc": [\n' + filas + "\n    ]\n  },\n"
        '  "title": {"text": "Elo \\"historico\\" {no JSON}"}\n'
        "}\n</script>\n</body></html>")


def html_censo(primera, segunda=(), *, con_tabla=True, con_datasets=True):
    """Pagina de federacion: censo en `datasets` + literal `const eloData`."""
    def fila(nombre, level, elo, fed="ESP"):
        return ('      {"Colour": "#A4234B", "Elo": %s, "FedURL": "%s", '
                '"Level": %d, "Name": "%s", "TLC": "%s"}'
                % (elo, fed, level, nombre, nombre[:3].upper()))
    filas = ",\n".join(
        [fila(n, 1, 1700 + i) for i, (n, _) in enumerate(primera)] +
        [fila(n, 2, 1600 + i) for i, (n, _) in enumerate(segunda)])
    ds = ('  "datasets": {\n    "data-vacio": [],\n    "data-censo": [\n'
          + filas + "\n    ]\n  },\n") if con_datasets else '  "datasets": {},\n'
    enlaces = ", ".join(
        "['<td class=\"l\"><a href=\"/ESP\"><img src=\"esp.png\"></a> "
        '<small> %d </small><a href="/%s">%s<span class="min481"></span></a>'
        "</td>', '2024', '+0.00', '2.30']" % (i + 1, slug, nombre)
        for i, (nombre, slug) in enumerate(list(primera) + list(segunda)))
    tabla = ("<script>const eloData = [" + enlaces + "];</script>\n"
             if con_tabla else "")
    # El <h1> del censo real tambien va fechado y con nombre de pagina detras de
    # la barra —`/2026-10-08/ESP`—, igual que el de un club: es lo que permite
    # medir la frescura del censo con la misma regla y no con una excepcion.
    return ("<html><body>\n<h1><a href=\"/2026-10-08/ESP\">Spain</a></h1>\n"
            "<script>\n{\n" + ds + '  "title": {"text": "Spain"}\n}\n</script>\n'
            + tabla + "</body></html>")


# La portada: lo que la web sirve con 200 ante CUALQUIER ruta desconocida.
# Reproduce lo medido en el sitio real, incluido lo incomodo:
#   - el <h1> no esta vacio de etiquetas, sino de TEXTO: trae un enlace fechado
#     `<h1><a href="/2026-10-09/"></a></h1>`, con la fecha del ranking mundial y
#     sin nombre de pagina detras de la barra. Copiarlo importa porque la version
#     comoda —un `<h1></h1>` pelado— dejaria sin probar justo el caso que puede
#     colarse por `_web_fecha_sitio`;
#   - 50 filas de ranking mundial con 'Name' y 'Level', todas de nivel 1,
#     repartidas entre federaciones y sin 'Date'.
# Es el caso que mas importa: por numero de filas pasaria el minimo de censo sin
# inmutarse, y solo el texto del <h1> y el FedURL la delatan.
_FEDS = ["ENG"] * 16 + ["ITA"] * 7 + ["GER"] * 6 + ["ESP"] * 5 + ["FRA"] * 4 + \
        ["POR"] * 3 + ["NED"] * 9
HTML_PORTADA = (
    '<html><body>\n<h1><a href="/2026-10-09/"></a></h1>\n<script>\n{\n'
    '  "datasets": {\n    "data-rank": [\n' + ",\n".join(
        '      {"Name": "Club%02d", "Level": 1, "Elo": %d, "FedURL": "%s", '
        '"TLC": "C%02d"}' % (i, 2000 - i, fed, i)
        for i, fed in enumerate(_FEDS)) + "\n    ]\n  }\n}\n</script>\n"
    "</body></html>")

CENSO_1 = [("Barcelona", "Barcelona"), ("Real Madrid", "realmadrid"),
           ("Atletico", "atletico"), ("Athletic Club", "athletic-club"),
           ("Real Sociedad", "Sociedad"), ("Celta", "rc-celta"),
           ("Betis", "Betis"), ("Villarreal", "Villarreal"),
           ("Sevilla", "Sevilla"), ("Valencia", "Valencia"),
           ("Osasuna", "Osasuna"), ("Getafe", "Getafe"), ("Alaves", "Alaves"),
           ("Rayo Vallecano", "RayoVallecano"), ("Espanyol", "Espanyol"),
           ("Mallorca", "Mallorca"), ("Elche", "Elche"), ("Levante", "Levante"),
           ("Girona", "Girona"), ("Depor", "Depor")]
CENSO_2 = [("Almeria", "Almeria"), ("Eibar", "Eibar")]


# --------------------------------------------------------------------------- #
print("\n== via web: bloque JSON con llaves balanceadas ==")
check("objeto balanceado, no hasta la ultima llave del documento",
      ce._bloque_json('x "d": {"a": {"b": 1}} , "otro": 2', '"d"')
      == '{"a": {"b": 1}}',
      ce._bloque_json('x "d": {"a": {"b": 1}} , "otro": 2', '"d"'))
check("llave dentro de una cadena no cuenta",
      ce._bloque_json('"d": {"t": "}}}"} resto', '"d"') == '{"t": "}}}"}',
      ce._bloque_json('"d": {"t": "}}}"} resto', '"d"'))
check("comilla escapada no abre cadena",
      ce._bloque_json('"d": {"t": "a\\"b"} resto', '"d"') == '{"t": "a\\"b"}')
check("ancla con '=' (literal de JavaScript) tambien vale",
      ce._bloque_json("const eloData = [[1],[2]]; mas", "const eloData")
      == "[[1],[2]]")
check("ancla ausente -> None", ce._bloque_json("nada", '"d"') is None)
check("bloque sin cerrar -> None", ce._bloque_json('"d": {"a": 1', '"d"') is None)
check("texto vacio -> None", ce._bloque_json("", '"d"') is None)


# --------------------------------------------------------------------------- #
print("\n== via web: titulo y guardian de identidad de pagina ==")
H_GIRONA = html_club("Girona", [("2026-09-27", 1719.0), ("2026-10-04", 1721.5)])
check("titulo del <h1> sin etiquetas", ce._web_titulo(H_GIRONA) == "Girona",
      ce._web_titulo(H_GIRONA))
check("portada -> titulo vacio", ce._web_titulo(HTML_PORTADA) == "")
check("sin <h1> -> titulo vacio", ce._web_titulo("<html></html>") == "")
check("pagina de club valida -> True", ce.es_pagina_de_club(H_GIRONA))
check("portada servida con 200 -> False (no hay 404 en esta web)",
      not ce.es_pagina_de_club(HTML_PORTADA))
check("clave correcta -> True", ce.es_pagina_de_club(H_GIRONA, clave="girona"))
check("clave de OTRO club -> False (candidato a ciegas rechazado)",
      not ce.es_pagina_de_club(H_GIRONA, clave="real_madrid"))
check("censo no es pagina de club (sus filas no traen 'Date')",
      not ce.es_pagina_de_club(html_censo(CENSO_1)))
check("HTML vacio -> False", not ce.es_pagina_de_club(""))

# La frescura se mide por la fecha que el SITIO se atribuye, no por la del
# ultimo partido del club: un club de Primera puede pasarse tres semanas sin
# punto por un paron de calendario con la fuente perfectamente al dia.
check("fecha del sitio leida del enlace del titulo",
      ce._web_fecha_sitio(H_GIRONA) == "2026-10-08", ce._web_fecha_sitio(H_GIRONA))
check("portada -> sin fecha de sitio, aunque SU <h1> tambien venga fechado",
      ce._web_fecha_sitio(HTML_PORTADA) == "", ce._web_fecha_sitio(HTML_PORTADA))
check("lo que la descarta es la barra sin nombre detras de la fecha",
      ce._web_fecha_sitio('<h1><a href="/2026-10-09/"></a></h1>') == "")
check("titulo sin enlace fechado -> ''",
      ce._web_fecha_sitio("<h1><a href=\"/ESP\">Spain</a></h1>") == "")
check("el censo declara su fecha con el mismo patron que un club",
      ce._web_fecha_sitio(html_censo(CENSO_1)) == "2026-10-08",
      ce._web_fecha_sitio(html_censo(CENSO_1)))
check("se lee la del <h1>, no cualquier fecha de la pagina",
      ce._web_fecha_sitio('<a href="/2020-01-01/Otro">x</a>'
                          '<h1><a href="/2026-10-08/Girona">Girona</a></h1>')
      == "2026-10-08")


# --------------------------------------------------------------------------- #
print("\n== via web: censo de /ESP y tabla de slugs ==")
H_ESP = html_censo(CENSO_1, CENSO_2)
tabla = ce._web_tabla_slugs(H_ESP)
check("la tabla del Ranking cosecha los 22 enlaces", len(tabla) == 22, len(tabla))
check("slugs irregulares leidos, no deducidos",
      [tabla.get(n) for n in ("Real Madrid", "Celta", "Athletic Club",
                              "Real Sociedad")]
      == ["realmadrid", "rc-celta", "athletic-club", "Sociedad"],
      [tabla.get(n) for n in ("Real Madrid", "Celta", "Athletic Club",
                              "Real Sociedad")])
check("sin literal del Ranking -> tabla vacia",
      ce._web_tabla_slugs(html_censo(CENSO_1, con_tabla=False)) == {})

censo = ce.parse_censo_web(H_ESP)
check("censo: 22 clubes, 20 en Primera",
      (len(censo) == 22) and int((censo["level"] == 1).sum()) == 20,
      f"{len(censo)} / {int((censo['level'] == 1).sum())}")
check("censo: columnas del contrato",
      list(censo.columns) == ["team", "team_key", "slug", "level", "tlc", "elo"],
      list(censo.columns))
check("censo: slug pegado a cada club",
      censo["slug"].notna().all() and
      censo.loc[censo["team"] == "Celta", "slug"].iat[0] == "rc-celta")
check("censo: team_key canonico",
      censo.loc[censo["team"] == "Atletico", "team_key"].iat[0] == "atletico_madrid",
      censo.loc[censo["team"] == "Atletico", "team_key"].iat[0])
check("censo: level entero anulable", str(censo["level"].dtype) == "Int64",
      str(censo["level"].dtype))

check("la portada traeria 50 filas de censo: por numero NO se detecta",
      len([r for v in ce._web_datasets(HTML_PORTADA).values()
           if isinstance(v, list) for r in v]) >= ce._WEB_MIN_CENSO)

for nombre, html in [
    ("portada (ruta invalida con 200): <h1> vacio", HTML_PORTADA),
    ("pagina sin bloque datasets", "<html><h1>x</h1></html>"),
    ("censo corto: 19 clubes en Primera", html_censo(CENSO_1[:19], CENSO_2)),
    ("censo de OTRA federacion",
     html_censo(CENSO_1, CENSO_2).replace('"FedURL": "ESP"', '"FedURL": "ENG"')),
]:
    try:
        ce.parse_censo_web(html)
        fallo = False
    except _common.SchemaError:
        fallo = True
    check(f"censo rechazado: {nombre}", fallo)

check("strict=False sobre la portada -> vacio con columnas, sin excepcion",
      ce.parse_censo_web(HTML_PORTADA, strict=False).empty)
check("filas de otras federaciones descartadas, no contadas",
      len(ce.parse_censo_web(
          html_censo(CENSO_1, CENSO_2) + "", strict=True)) == 22)


# --------------------------------------------------------------------------- #
print("\n== via web: serie -> tramos SIN fuga (el Elo de D es POSTERIOR a D) ==")
PTS = [("2026-09-20", 1700.0), ("2026-09-27", 1710.0), ("2026-10-04", 1721.5)]
h = ce.parse_historia_web(html_club("Girona", PTS), team="Girona", level=1)
check("tramos: columnas del contrato",
      list(h.columns) == ["team", "team_key", "country", "level", "rank", "elo",
                          "from", "to", "fuente"], list(h.columns))
check("un tramo por punto", len(h) == 3, len(h))
check("'from' = fecha del punto + 1 dia (la clave anti-fuga)",
      list(h["from"]) == ["2026-09-21", "2026-09-28", "2026-10-05"],
      list(h["from"]))
check("'to' = fecha del punto siguiente",
      list(h["to"][:2]) == ["2026-09-27", "2026-10-04"], list(h["to"][:2]))
check("ultimo tramo abierto (es el que se mueve cada semana)",
      pd.isna(h["to"].iat[-1]), h["to"].iat[-1])
check("el Elo del punto viaja al tramo que ABRE, no al que cierra",
      list(h["elo"]) == [1700.0, 1710.0, 1721.5], list(h["elo"]))
check("team_key canonico y fuente marcada",
      (h["team_key"] == "girona").all() and (h["fuente"] == "web").all())
check("level del club, no del tramo", list(h["level"].unique()) == [1],
      list(h["level"].unique()))
check("country por defecto", (h["country"] == ce.COUNTRY).all())

# el tramo que abre no puede alcanzar al partido que lo genero
_f = pd.to_datetime(h["from"])
check("ningun tramo empieza el dia de su propio partido",
      bool((_f > pd.to_datetime([p[0] for p in PTS])).all()))

desorden = ce.parse_historia_web(
    html_club("Girona", [PTS[2], PTS[0], PTS[1]]), team="Girona")
check("puntos desordenados -> tramos ordenados igual",
      list(desorden["from"]) == list(h["from"]), list(desorden["from"]))

doble = ce.parse_historia_web(
    html_club("Girona", [("2026-09-20", 1700.0), ("2026-09-20", 1705.0),
                         ("2026-09-27", 1710.0)]), team="Girona")
check("dos puntos el mismo dia -> manda el ultimo (Elo de salida)",
      len(doble) == 2 and doble["elo"].iat[0] == 1705.0,
      f"{len(doble)} / {doble['elo'].iat[0]}")

sucio = ce.parse_historia_web(
    html_club("Girona", [("2026-09-20", 1700.0), ("sin-fecha", 1702.0)]),
    team="Girona")
check("punto con fecha ilegible -> descartado, no NaT en el maestro",
      len(sucio) == 1 and pd.isna(sucio["to"].iat[0]), len(sucio))
check("portada -> historia vacia con columnas (no filas fantasma)",
      ce.parse_historia_web(HTML_PORTADA, team="Girona").empty)


# --------------------------------------------------------------------------- #
print("\n== via web: candidatos de slug para clubes fuera del censo ==")
# Los descendidos desaparecen de /ESP pero conservan pagina. Aqui no se acierta,
# se PROPONE; acepta o descarta el guardian de identidad.
for nombre, esperado in [
    ("Cadiz", "Cadiz"),
    ("Real Valladolid", "Valladolid"),
    ("UD Las Palmas", "LasPalmas"),
    ("Real Oviedo", "Oviedo"),
    ("Sporting Gijon", "Gijon"),
    ("Racing Santander", "Santander"),
    ("Athletic Club", "athletic-club"),
    ("Real Madrid", "realmadrid"),
]:
    cands = ce._slug_candidatos(nombre)
    check(f"{nombre!r} propone {esperado!r}", esperado in cands, cands)

check("acentos deacentuados", "Alaves" in ce._slug_candidatos("Deportivo Alaves"),
      ce._slug_candidatos("Deportivo Alaves"))
check("sin duplicados en los candidatos",
      len(set(ce._slug_candidatos("Granada"))) == len(ce._slug_candidatos("Granada")))
check("nombre vacio -> sin candidatos", ce._slug_candidatos("") == [])


# --------------------------------------------------------------------------- #
print("\n== guardian de escala: una sola escala vigente en cada fecha ==")
# El salto web-API es de +85 de media, dependiente del club y de la fecha, y
# ademas comprime el rango un 11%: en nivel las dos series no son conciliables.
# Se admite UNA frontera —archivo hasta el corte, web desde el corte— porque la
# tipificacion dentro de la jornada que aplica `spine` es invariante a ese cambio
# de escala. Lo que no se admite es el solape, que a las aserciones de esquema le
# pasaria inadvertido: columnas que casan, fechas que casan y un Elo plausible.
def tramos(filas):
    """`filas` = [(fuente, from, to)] -> maestro minimo de un solo club."""
    return pd.DataFrame({"team_key": ["girona"] * len(filas),
                         "elo": [1700.0] * len(filas),
                         "fuente": [f for f, _, _ in filas],
                         "from": [d for _, d, _ in filas],
                         "to": [h for _, _, h in filas]})

una_via = [("web", "2022-10-18", "2022-10-25"), ("web", "2022-10-26", None)]
check("maestro solo web -> 'web'", ce.assert_una_escala(tramos(una_via)) == "web")
check("maestro sin columna 'fuente' -> 'api' (maestro anterior a la columna)",
      ce.assert_una_escala(pd.DataFrame({"team_key": ["x"], "elo": [1.0]})) == "api")
check("'fuente' vacia cuenta como api",
      ce.assert_una_escala(tramos([("", "2016-08-20", None)])) == "api")

limpio = [("api", "2016-08-20", "2022-10-17"),
          ("web", "2022-10-18", "2022-10-25"),
          ("web", "2022-10-26", None)]
check("empalme con frontera unica -> 'empalme:<corte>'",
      ce.assert_una_escala(tramos(limpio)) == "empalme:2022-10-18",
      ce.assert_una_escala(tramos(limpio)))

# (nombre, filas) de los empalmes que NO se admiten
for nombre, filas in [
    ("solape: el archivo entra en territorio web",
     [("api", "2016-08-20", "2022-11-30"), ("web", "2022-10-18", None)]),
    ("tramo del archivo que abre despues del corte",
     [("api", "2022-11-01", "2022-11-30"), ("web", "2022-10-18", None)]),
    ("tramo del archivo ABIERTO: arrastraria la escala vieja para siempre",
     [("api", "2016-08-20", None), ("web", "2022-10-18", None)]),
    ("via desconocida",
     [("calculado", "2016-08-20", "2022-10-17"), ("web", "2022-10-18", None)]),
]:
    try:
        ce.assert_una_escala(tramos(filas))
        fallo = False
    except _common.RegressionError:
        fallo = True
    check(f"{nombre} -> error ruidoso", fallo)


# --------------------------------------------------------------------------- #
print("\n== serie empalmada: el archivo hasta el corte, la web desde el corte ==")
CORTE = "2022-10-18"

def via(fuente, filas):
    """`filas` = [(team_key, from, to)] -> maestro minimo de una via."""
    return pd.DataFrame({
        "team": [k for k, _, _ in filas],
        "team_key": [k for k, _, _ in filas],
        "country": "ESP", "level": 1, "rank": pd.NA,
        "elo": [1700.0 + i for i in range(len(filas))],
        "from": [d for _, d, _ in filas],
        "to": [h for _, _, h in filas],
        "fuente": fuente})

API = via("api", [("girona", "2016-08-20", "2022-09-30"),
                  ("girona", "2022-10-01", "2022-10-31"),   # cruza el corte
                  ("girona", "2022-11-01", None),           # posterior al corte
                  ("betis",  "2016-08-20", None)])          # abierto en el archivo
WEB = via("web", [("girona", "2022-10-09", "2022-10-16"),   # anterior al corte
                  ("girona", "2022-10-17", "2022-10-24"),   # cruza el corte
                  ("girona", "2022-10-25", None),
                  ("betis",  "2022-10-10", None)])

emp = ce.serie_empalmada(API, WEB, corte=CORTE)
g = emp[emp["team_key"] == "girona"].reset_index(drop=True)

check("la via queda determinada por la fecha, sin excepciones",
      ((emp["fuente"] == "api") == (emp["from"] < CORTE)).all())
check("el archivo se cierra el dia anterior al corte",
      emp.loc[emp["fuente"] == "api", "to"].max() == "2022-10-17",
      emp.loc[emp["fuente"] == "api", "to"].max())
check("el tramo del archivo abierto tambien se cierra (no arrastra la escala vieja)",
      emp.loc[(emp["team_key"] == "betis") & (emp["fuente"] == "api"), "to"].tolist()
      == ["2022-10-17"])
check("los tramos del archivo posteriores al corte se tiran, no se promedian",
      (emp.loc[emp["fuente"] == "api", "from"] < CORTE).all())
check("el tramo web que cruza el corte se levanta hasta el",
      g.loc[g["fuente"] == "web", "from"].min() == CORTE,
      g.loc[g["fuente"] == "web", "from"].min())
check("el tramo web cerrado antes del corte se descarta",
      "2022-10-09" not in emp["from"].tolist())
check("ningun club tiene dos escalas vigentes el mismo dia",
      not emp.duplicated(subset=["team_key", "from"]).any())
check("`date` se recalcula desde `from` (clave del merge_asof)",
      (emp["date"] == emp["from"]).all())
check("la serie empalmada se declara como tal",
      ce.assert_una_escala(emp) == f"empalme:{CORTE}")

# agujero real: el club tiene pagina en la web pero su serie no alcanza el corte,
# asi que entre el corte y su primer punto se quedaria sin escala vigente.
TARDE = via("web", [("girona", "2022-10-17", None),
                    ("betis",  "2022-11-05", None)])
try:
    ce.serie_empalmada(API, TARDE, corte=CORTE)
    fallo = False
except _common.RegressionError:
    fallo = True
check("club con web que arranca despues del corte -> error ruidoso", fallo)

try:
    ce.serie_empalmada(API, WEB, corte="2016-01-01")
    fallo = False
except _common.RegressionError:
    fallo = True
check("corte que deja un lado vacio -> error ruidoso", fallo)

check("el corte declarado es el del modulo", ce.ELO_CORTE == CORTE, ce.ELO_CORTE)
check("sin strict no valida, pero sigue empalmando",
      len(ce.serie_empalmada(API, TARDE, corte=CORTE, strict=False)) > 0)


# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_clubelo():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest; cortar el proceso queda reservado al uso como guion,
    porque un `sys.exit` durante la importacion abortaria la recoleccion.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
