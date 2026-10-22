"""Validacion de los detectores silenciosos de `qa_master.py`. Sin red ni disco.

Las invariantes 1 a 5 del QA se validan solas: si fallan, algo visible esta roto.
La 6 y la 7 no, y por eso tienen bateria propia.

La invariante 6 persigue las dos unicas averias de la consolidacion que publican
una jornada entera sin dar la cara:

  (a) ARRASTRE DE ELO. El Elo entra por `merge_asof` backward, que no tiene
      techo: si ClubElo deja de publicar, el ultimo tramo conocido se pega a
      todos los partidos nuevos. La columna sigue llena, el valor sigue siendo
      plausible y nadie se enteraria. El desfase se mide en PARTIDOS del propio
      club que el rating se deja atras, no en dias, y esta bateria existe sobre
      todo para fijar esa eleccion: el desfase en dias tiene maximos legitimos de
      tres meses --paron de verano, parada de la pandemia-- asi que cualquier
      tolerancia que los admita se come tambien un mes de fuente caida en plena
      temporada; en partidos el paron de verano vale cero y el mes de fuente caida
      vale tres. Sobre esa medida, dos partidos sin recoger abortan siempre, y uno
      solo se tolera por debajo de un techo declarado: ClubElo publica un tramo por
      partido pero no con la regularidad de un reloj, y el rating que llega
      entonces es el de la vispera del partido anterior, informacion pre-partido
      legitima aunque mas pobre. Se cuenta y se informa; no se corrige inventando
      un valor que la fuente no dio.

  (b) COLA VACIA. El resto de bloques entra por `match_key`, asi que un extractor
      que no corrio no ensucia nada: deja vacias solo las filas nuevas. Un bloque
      al que le falten las dos ultimas jornadas de las treinta y ocho declara un
      95 % de cobertura y pasa todos los suelos por temporada. Mirando la cola
      pesa un tercio y salta.

La invariante 7 vigila el empalme de ClubElo. La serie publicada une dos escalas
de la fuente, separadas por un desplazamiento y una compresion, y lo que hace
legitima esa union es que el modelo consume la fuerza TIPIFICADA dentro de la
jornada, invariante a cualquier transformacion afin. Eso es un argumento, no un
dato, y la invariante 7 es su comprobacion: el salto que los equipos acusan al
cruzar la union tiene que caber dentro de lo que mueve una semana corriente de
competicion. Si cae, no falla un parquet: falla el argumento, y habria que volver
a decidir el empalme. De ahi que se compruebe aqui pieza por pieza.

Se comprueba, en este orden:

  1. ELO LIMPIO   un calendario con Elo al dia no produce ni un falso positivo,
                  incluidos los dos casos que mas se parecen a una averia: el
                  paron de verano y un equipo que debuta.
  2. ELO SUCIO    una fuente que deja de publicar a mitad de temporada se caza,
                  y se caza desde el primer partido sin rating nuevo.
  3. COLA         la cobertura de la cola ve la jornada que falta donde el
                  promedio por temporada no la ve.
  4. SUELOS       la tabla de suelos cubre todos los bloques que el QA declara y
                  nadie ha rebajado uno por descuido.
  5. ARRASTRE     `veredicto_arrastre` separa el refresco que llego tarde --una
                  fila a la que falta un partido, que es lo que el maestro real
                  trae-- de la fuente que dejo de publicar, que se salta dos.
  6. EMPALME      `salto_elo_z` empareja cada partido con el anterior del MISMO
                  equipo y temporada, y marca como cruce solo el par que atraviesa
                  el corte; y `veredicto_empalme` distingue un empalme limpio de
                  un escalon de escala, incluido el caso en que el escalon es
                  pequeno pero afecta a los veinte clubes a la vez.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import qa_master as qa  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str, extra: str = "") -> None:
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}"
          + (f"  -> {extra}" if extra and not cond else ""))


# --------------------------------------------------------------------------- #
# dos fixtures minimos: un calendario y un historico de Elo
# --------------------------------------------------------------------------- #
def calendario(partidos) -> pd.DataFrame:
    """`partidos` es una lista de (fecha, local, visitante) -> maestro minimo.

    Solo las columnas que leen los detectores: `elo_pegado` necesita las claves
    de equipo y la fecha, y `cobertura_cola` necesita la fecha y los bloques.
    """
    return pd.DataFrame([
        {"match_key": f"{d}_{h}_{a}", "season": "2025/26", "date": d,
         "home_key": h, "away_key": a}
        for d, h, a in partidos])


def elos(tramos) -> pd.DataFrame:
    """`tramos` es una lista de (fecha, equipo) -> `clubelo_elo.parquet` minimo.

    El valor del rating es irrelevante para el detector --es justo lo que no
    delata el arrastre-- asi que va fijo.
    """
    return pd.DataFrame([{"team_key": t, "date": d, "elo": 1500.0}
                         for d, t in tramos])


def arrastradas(m: pd.DataFrame, elo: pd.DataFrame) -> pd.DataFrame:
    return qa.elo_pegado(m, elo).query("_arrastre")


def calendario_z(partidos, season: str = "2022/23") -> pd.DataFrame:
    """`partidos` es (fecha, local, visitante, z_local, z_visitante) -> maestro.

    Los valores son DESVIACIONES respecto a la media de Primera ese dia, que es
    como viaja el Elo a partir de la consolidacion. Un 1700 en este fixture seria
    la senal de que alguien ha vuelto a publicar el nivel crudo con el nombre
    tipificado.
    """
    return pd.DataFrame([
        {"match_key": f"{d}_{h}_{a}", "season": season, "date": d,
         "home_key": h, "away_key": a, "home_elo_z": zh, "away_elo_z": za}
        for d, h, a, zh, za in partidos])


def saltos(cruces, normales) -> pd.DataFrame:
    """Dos listas de magnitudes -> el largo que `veredicto_empalme` consume.

    El veredicto solo lee `_salto` y `_cruza`, asi que se construye la tabla
    directamente en vez de fabricar un maestro que la produzca: cada caso queda
    descrito por las dos distribuciones que se comparan, que es de lo que habla la
    invariante, y no por un calendario del que haya que deducirlas.
    """
    filas = [{"team_key": f"c{i}", "_d": pd.Timestamp(qa.ELO_CORTE),
              "_salto": float(v), "_cruza": True} for i, v in enumerate(cruces)]
    filas += [{"team_key": f"n{i}", "_d": pd.Timestamp("2024-01-13"),
               "_salto": float(v), "_cruza": False} for i, v in enumerate(normales)]
    return pd.DataFrame(
        filas, columns=["team_key", "_d", "_salto", "_cruza"]
    ).astype({"_salto": float, "_cruza": bool})


# --------------------------------------------------------------------------- #
def caso_elo_limpio() -> None:
    print("\n[1] un Elo al dia no inventa averias")

    # Dos equipos, una jornada por semana, y ClubElo republicando el lunes
    # siguiente a cada jornada: el caso normal.
    m = calendario([("2025-08-16", "rma", "bar"),
                    ("2025-08-23", "bar", "rma"),
                    ("2025-08-30", "rma", "bar")])
    e = elos([("2025-08-10", "rma"), ("2025-08-10", "bar"),
              ("2025-08-18", "rma"), ("2025-08-18", "bar"),
              ("2025-08-25", "rma"), ("2025-08-25", "bar")])
    pegado = qa.elo_pegado(m, e)
    check(len(pegado) == 6, "una fila por equipo y partido", str(len(pegado)))
    check(not pegado["_arrastre"].any(),
          "con rating nuevo entre jornadas no se senala nada")
    check(pegado["_previo"].isna().sum() == 2,
          "el primer partido de cada equipo no tiene partido previo que comparar")

    # El paron de verano: el ultimo Elo de la temporada pasada es de junio y el
    # primer partido de la nueva es de agosto. Ochenta dias de desfase, y es
    # correcto: el rating es POSTERIOR al ultimo partido jugado.
    m = calendario([("2025-05-24", "rma", "bar"), ("2025-08-16", "rma", "bar")])
    e = elos([("2025-05-20", "rma"), ("2025-05-20", "bar"),
              ("2025-05-26", "rma"), ("2025-05-26", "bar")])
    pegado = qa.elo_pegado(m, e)
    desfase = (pegado["_d"] - pegado["_elo_d"]).dt.days.max()
    check(not pegado["_arrastre"].any() and desfase > 60,
          f"el paron de verano no es arrastre aunque el desfase sea de {desfase} d")
    # Y esta es la razon de medir en partidos: en dias el paron de verano es el
    # peor caso de todo el historico, y en partidos vale exactamente cero, igual
    # que una semana corriente. Un liston en dias tendria que admitir ochenta para
    # no cazarlo, y con esa tolerancia tampoco cazaria un mes de fuente caida.
    check(int(pegado["_saltados"].max()) == 0,
          "en partidos, el paron de verano no deja ninguno sin recoger",
          str(pegado["_saltados"].tolist()))

    # Un recien ascendido: su primer partido en los datos no tiene previo, asi
    # que no hay liston y no se puede afirmar nada. Callarse es lo correcto.
    m = calendario([("2025-08-16", "rma", "elche")])
    e = elos([("2024-11-01", "rma"), ("2024-11-01", "elche")])
    check(not arrastradas(m, e).shape[0],
          "un equipo que debuta no se senala: sin partido previo no hay liston")


def caso_elo_sucio() -> None:
    print("\n[2] un Elo que se queda parado se caza")

    # ClubElo publica hasta el 18 de agosto y deja de hacerlo. Las jornadas del
    # 23 y del 30 reciben el rating del 18, que es anterior a partidos que esos
    # equipos ya han jugado.
    m = calendario([("2025-08-16", "rma", "bar"),
                    ("2025-08-23", "bar", "rma"),
                    ("2025-08-30", "rma", "bar")])
    e = elos([("2025-08-10", "rma"), ("2025-08-10", "bar"),
              ("2025-08-18", "rma"), ("2025-08-18", "bar")])
    mal = arrastradas(m, e)
    check(len(mal) == 2,
          "se senalan los dos equipos del primer partido sin rating nuevo",
          str(len(mal)))
    check(set(mal["_d"].dt.strftime("%Y-%m-%d")) == {"2025-08-30"},
          "y se senala la jornada del 30, no la del 23",
          sorted(mal["_d"].dt.strftime("%Y-%m-%d")))

    # El detalle que importa: la jornada del 23 NO se senala. Su Elo es del 18,
    # posterior al partido del 16, asi que es legitimo —ClubElo publica el lunes
    # y la jornada es el sabado—. El detector no se dispara por un desfase de
    # cinco dias; se dispara cuando el rating ignora un partido ya jugado.
    check((qa.elo_pegado(m, e)
           .query("_d == '2025-08-23'")["_arrastre"] == False).all(),  # noqa: E712
          "y no se senala la del 23, cuyo rating es posterior a su partido previo")

    # Una fuente caida un mes entero: todas las jornadas posteriores a la ultima
    # publicacion quedan senaladas, no solo la primera.
    m = calendario([(d, "rma", "bar") for d in
                    ("2025-09-06", "2025-09-13", "2025-09-20", "2025-09-27")])
    e = elos([("2025-09-01", "rma"), ("2025-09-01", "bar")])
    mal = arrastradas(m, e)
    check(len(mal) == 6,
          "un mes sin publicar senala las tres jornadas posteriores, por los dos "
          "equipos de cada una", str(len(mal)))

    # Y la gravedad crece con los partidos que el rating se deja atras, no con los
    # dias: es lo que separa el refresco que llego tarde de la fuente caida. Aqui
    # las tres jornadas sin publicar valen 1, 2 y 3 partidos sin recoger.
    saltados = (qa.elo_pegado(m, e)
                .groupby(qa.elo_pegado(m, e)["_d"].dt.strftime("%m-%d"))["_saltados"]
                .max().to_dict())
    check(saltados == {"09-06": 0, "09-13": 1, "09-20": 2, "09-27": 3},
          "el desfase se cuenta en partidos del propio club, y crece jornada a "
          "jornada mientras la fuente no publica", str(saltados))

    # Y si ClubElo nunca publico nada para un equipo, el Elo llega vacio: el
    # merge no pega un dato viejo, pega NaN, que la invariante 4b ya persigue.
    # Aqui no es arrastre y no debe contarse como tal. El Elo del rival va al
    # dia a proposito, para que lo unico que pueda saltar sea el NaN.
    m = calendario([("2025-08-16", "rma", "bar"), ("2025-08-23", "rma", "bar")])
    pegado = qa.elo_pegado(m, elos([("2025-08-10", "rma"), ("2025-08-18", "rma")]))
    check(pegado["_elo_d"].isna().sum() == 2 and not pegado["_arrastre"].any(),
          "un equipo sin ningun Elo publicado deja NaN, que no es arrastre",
          str(pegado[["team_key", "_d", "_previo", "_elo_d", "_arrastre"]].to_dict("records")))


def caso_cola() -> None:
    print("\n[3] la cola ve la jornada que falta")

    # Una temporada entera, treinta y ocho jornadas de un solo partido, con
    # Sofascore cubriendolas todas menos las dos ultimas: el extractor no corrio.
    fechas = pd.date_range("2025-08-16", periods=38, freq="7D")
    m = calendario([(d.strftime("%Y-%m-%d"), "rma", "bar") for d in fechas])
    m["home_sofa_rating"] = [7.0] * 36 + [None] * 2
    m["away_sofa_rating"] = [7.0] * 36 + [None] * 2
    bloques = {"sofa_rating": ["home_sofa_rating", "away_sofa_rating"]}

    temporada = 1 - m[bloques["sofa_rating"]].isna().mean().mean()
    check(temporada > 0.94,
          f"por temporada el bloque declara un {temporada:.0%} y pasaria el suelo")

    _, cobertura = qa.cobertura_cola(m, bloques, n=4)
    check(cobertura["sofa_rating"] == 0.5,
          f"mirando las cuatro ultimas cae al {cobertura['sofa_rating']:.0%} y salta",
          str(cobertura))
    check(cobertura["sofa_rating"] < qa._SUELO["sofa_rating"],
          "por debajo del suelo del bloque, que es lo que hace fallar el QA")

    # La cola se toma por FECHA, no por el orden en que venga el maestro: un
    # parquet reordenado por match_key no debe cambiar el veredicto.
    revuelto = m.sort_values("match_key").reset_index(drop=True)
    _, c2 = qa.cobertura_cola(revuelto, bloques, n=4)
    check(c2 == cobertura, "y se toma por fecha, no por el orden de las filas")

    cola, _ = qa.cobertura_cola(m, bloques, n=4)
    check(cola["_d"].max() == fechas[-1],
          "la cola termina en el ultimo partido del maestro")

    # Una cola llena no se queja.
    m["home_sofa_rating"] = m["away_sofa_rating"] = 7.0
    _, c3 = qa.cobertura_cola(m, bloques, n=4)
    check(c3["sofa_rating"] == 1.0, "y una cola completa da el 100 %")

    # Un bloque cuyas columnas no estan en el maestro no se inventa un cero: lo
    # declara ausente la invariante 4b, que para eso existe.
    _, c4 = qa.cobertura_cola(m, {"inexistente": ["no_esta"]}, n=4)
    check(c4 == {}, "un bloque sin columnas en el maestro no entra en la cola")


def caso_suelos() -> None:
    print("\n[4] la tabla de suelos")
    declarados = set(qa._BLOCKS) | set(qa._BLOCKS_OPCIONALES)
    sobran = set(qa._SUELO) - declarados
    check(not sobran, f"ningun suelo apunta a un bloque que ya no existe: {sobran}")
    check(all(0.5 <= v <= 1.0 for v in qa._SUELO.values()),
          "ningun suelo esta rebajado hasta volverse inofensivo",
          str(qa._SUELO))
    techo_ascendidos = 17 / 20          # tres ascendidos de veinte clubes
    for b in ("valor_plantilla", "bajas_euros"):
        check(qa._SUELO.get(b, 1.0) < techo_ascendidos,
              f"el suelo de '{b}' queda por debajo de su techo estructural "
              f"({techo_ascendidos:.0%}): el recien ascendido no jugo la "
              "temporada s-1 de la que se toma el valor, asi que un suelo "
              "igual al techo mediria el redondeo y no la cobertura",
              str(qa._SUELO.get(b)))
    check(qa._SUELO_DEFECTO >= 0.98,
          "y el suelo por defecto sigue siendo exigente", str(qa._SUELO_DEFECTO))


def desfases(graves=(), unos=0, aldia=0) -> pd.DataFrame:
    """Numeros de partidos sin recoger -> el largo que `veredicto_arrastre` lee.

    El veredicto solo mira `_saltados` y las fechas, asi que el caso se describe
    por la distribucion del desfase y no por un calendario del que haya que
    deducirla: `graves` son las filas que se dejan dos partidos o mas, `unos` las
    que se dejan uno y `aldia` las que van al dia.
    """
    filas = ([{"team_key": f"g{i}", "_saltados": int(v)} for i, v in enumerate(graves)]
             + [{"team_key": f"u{i}", "_saltados": 1} for i in range(unos)]
             + [{"team_key": f"a{i}", "_saltados": 0} for i in range(aldia)])
    d = pd.DataFrame(filas, columns=["team_key", "_saltados"]).astype({"_saltados": int})
    d["_d"] = pd.Timestamp("2025-08-23")
    d["_elo_d"] = d["_d"] - pd.to_timedelta(2 + 7 * d["_saltados"], unit="D")
    return d


def caso_arrastre() -> None:
    print("\n[5] el refresco que llega tarde no es una fuente caida")

    # El caso limpio reproduce lo que se mide sobre el maestro real: 16 filas de
    # 7600 heredan un rating que no incorpora el ultimo partido del club, y ninguna
    # se salta dos. Son las cifras que la memoria cita; si algun dia dejan de pasar
    # la invariante, lo que ha cambiado es la fiabilidad de la fuente.
    motivo, mt = qa.veredicto_arrastre(desfases(unos=16, aldia=7584))
    check(motivo is None, "0.21 % de filas a las que falta un partido pasa", str(motivo))
    check(mt["n"] == 7600 and mt["n_uno"] == 16 and mt["n_graves"] == 0
          and abs(mt["share_uno"] - 0.0021) < 1e-4,
          "y el veredicto deja las cifras del desfase a mano para citarlas", str(mt))

    # Faltar DOS partidos no tiene techo: una sola fila basta. Es el caso que la
    # invariante existe para cazar —la fuente dejo de publicar y el merge repite el
    # ultimo tramo— y no se diluye por mucho maestro que haya alrededor.
    motivo, _ = qa.veredicto_arrastre(desfases(graves=(2,), aldia=7599))
    check(motivo is not None and "dos" in motivo and "repitiendo" in motivo,
          "una sola fila que se salte dos partidos aborta, sin importar la escala",
          str(motivo))

    # Y el techo caza la degradacion que no llega a saltarse dos partidos en
    # ninguna fila pero deja de ser un caso aislado: el refresco de ClubElo se ha
    # vuelto sistematicamente tardio.
    motivo, mt = qa.veredicto_arrastre(desfases(unos=200, aldia=7400))
    check(motivo is not None and "techo declarado" in motivo,
          "pero un 2.6 % de filas con un partido sin recoger tambien aborta",
          str(motivo))
    check(mt["share_uno"] > qa._SALTADOS_TECHO > mt["techo"] / 2,
          "y el techo es el declarado en el modulo, no uno escrito aqui",
          f"{mt['share_uno']} vs {qa._SALTADOS_TECHO}")

    # Un maestro sin Elo en ninguna fila no es un maestro limpio: es la invariante
    # sin nada que mirar, y decirlo es distinto de decir OK.
    sin = desfases(aldia=3).assign(_elo_d=pd.NaT)
    motivo, mt = qa.veredicto_arrastre(sin)
    check(motivo is not None and not mt,
          "ninguna fila con Elo se declara como tal, no como OK", str(motivo))
    check(qa.veredicto_arrastre(desfases().iloc[:0]) == (None, {}),
          "y un largo vacio se calla: no hay maestro del que opinar")


def caso_empalme() -> None:
    print("\n[6] el empalme de ClubElo no puede parecer un acontecimiento deportivo")

    # Dos equipos, jornadas semanales a caballo del corte: dos partidos antes y
    # uno despues. El Elo tipificado se mueve poco, como se mueve de verdad.
    m = calendario_z([("2022-10-08", "rma", "bar", 1.0, -0.5),
                      ("2022-10-15", "bar", "rma", -0.4, 1.1),
                      ("2022-10-22", "rma", "bar", 1.4, -0.2)])
    s = qa.salto_elo_z(m)
    check(len(s) == 4,
          "el primer partido de cada equipo no entra: no hay anterior con que "
          "comparar", str(len(s)))
    rma = s[(s["team_key"] == "rma") & (s["_d"] == pd.Timestamp("2022-10-15"))]
    check(abs(float(rma["_salto"].iloc[0]) - 0.1) < 1e-9,
          "`_salto` es el cambio absoluto de fuerza respecto al partido anterior",
          str(rma["_salto"].tolist()))
    check(set(s.loc[s["_cruza"], "_d"].dt.strftime("%Y-%m-%d")) == {"2022-10-22"},
          "y se marca como cruce solo el par de partidos que atraviesa el corte",
          sorted(s.loc[s["_cruza"], "_d"].dt.strftime("%Y-%m-%d")))

    # La decision que esta bateria fija: el emparejamiento es DENTRO de temporada.
    # Un club que vuelve de Segunda tiene su partido anterior dos temporadas atras,
    # con otra plantilla y otra base de tipificacion; si contase como cruce, su
    # cambio de division se sumaria al presupuesto del empalme y lo reventaria por
    # un motivo que no tiene nada que ver con la escala de la fuente.
    ascendido = pd.concat([
        calendario_z([("2022-02-05", "leg", "bar", -1.8, -0.5)], season="2021/22"),
        calendario_z([("2022-10-22", "leg", "rma", -0.9, 1.4)])], ignore_index=True)
    s2 = qa.salto_elo_z(ascendido)
    check(not s2["_cruza"].any(),
          "un club que vuelve de Segunda no cuenta como cruce del empalme",
          str(s2[["team_key", "season", "_salto", "_cruza"]].to_dict("records")))

    # Sin las dos columnas tipificadas no se puede medir nada, y lo correcto es
    # devolver el largo vacio con su esquema: el QA lo lee como "nada que medir",
    # no como un cero que pasaria la invariante por la puerta de atras.
    vacio = qa.salto_elo_z(m.drop(columns=["away_elo_z"]))
    check(vacio.empty and {"_salto", "_cruza"} <= set(vacio.columns),
          "sin fuerza tipificada el detector se calla, pero conserva su esquema",
          str(list(vacio.columns)))

    # --- el veredicto ------------------------------------------------------- #
    # Las dos distribuciones del caso limpio reproducen las que se miden sobre el
    # maestro real: veinte clubes cruzando la union con una mediana de 0.075
    # desviaciones y un peor caso de 0.19, contra una semana corriente de
    # competicion con mediana 0.06, p90 0.11 y extremos de hasta 0.34. Se escriben
    # a mano y no se generan al azar porque son las cifras que la memoria cita: si
    # alguna vez dejan de pasar la invariante, lo que ha cambiado es el argumento.
    cruces = [0.03, 0.04, 0.05, 0.05, 0.06, 0.06, 0.07, 0.07, 0.07, 0.07,
              0.08, 0.08, 0.09, 0.09, 0.10, 0.11, 0.13, 0.15, 0.17, 0.19]
    normal = ([0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.11] * 6
              + [0.18, 0.25, 0.34])
    motivo, mt = qa.veredicto_empalme(saltos(cruces, normal))
    check(motivo is None,
          "un empalme que cabe en una semana de competicion no se denuncia",
          str(motivo))
    check(mt["n_cruces"] == 20 and abs(mt["max_c"] - 0.19) < 1e-9
          and abs(mt["p50_c"] - 0.075) < 1e-9,
          "y el veredicto devuelve las cifras para citarlas aunque no falle",
          str(mt))

    # Escalon de escala: el peor cruce pasa del peor movimiento normal.
    motivo, _ = qa.veredicto_empalme(saltos([0.40, 0.45], normal))
    check(motivo is not None and "como si fuera futbol" in motivo,
          "un salto mayor que el extremo de una semana normal si se denuncia",
          str(motivo))

    # Suelo de cordura. La cola normal de este caso es absurda a proposito --0.90
    # desviaciones en una semana-- para que el liston relativo admita el cruce y lo
    # unico que pueda cortar sea el tope: media desviacion no la mueve una semana
    # de futbol por mucho que la distribucion se ensanche.
    motivo, _ = qa.veredicto_empalme(saltos([0.60], normal + [0.90]))
    check(motivo is not None and "tope declarado" in motivo,
          "y el tope absoluto corta aunque el liston relativo lo admitiera",
          str(motivo))

    # El caso sutil, y el que de verdad delata un cambio de escala: ningun cruce
    # es extremo --0.30 cabe de sobra en la cola de lo normal y debajo del tope--
    # pero los veinte clubes se mueven eso a la vez, que es la firma de un
    # desplazamiento de la escala y no de una jornada de liga.
    motivo, _ = qa.veredicto_empalme(saltos([0.30] * 20, normal))
    check(motivo is not None and "sistematico" in motivo,
          "un desplazamiento pequeno pero simultaneo de los veinte clubes salta",
          str(motivo))

    # Un maestro que no llega al corte no tiene nada que medir, y callarse es lo
    # correcto: es la situacion de un historico que empiece despues del empalme.
    motivo, mt = qa.veredicto_empalme(saltos([], [0.05, 0.10]))
    check(motivo is None and mt == {},
          "sin partidos que crucen el corte no hay invariante que comprobar",
          str((motivo, mt)))

    # Y al contrario: cruces sin semanas normales con las que compararlos deja la
    # invariante sin liston, que es un fallo y no un aprobado por defecto.
    motivo, _ = qa.veredicto_empalme(saltos([0.07], []))
    check(motivo is not None and "sin semanas corrientes" in motivo,
          "sin liston con el que comparar, la invariante no se aprueba sola",
          str(motivo))


# --------------------------------------------------------------------------- #
def main() -> int:
    print("=" * 74)
    print("BANCO DE PRUEBAS — qa_master.py (invariantes 6 y 7)")
    print("=" * 74)
    caso_elo_limpio()
    caso_elo_sucio()
    caso_cola()
    caso_suelos()
    caso_arrastre()
    caso_empalme()
    print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
    if FAIL:
        print("FALLOS:")
        for f in FAIL:
            print("  -", f)
    else:
        print("Todo correcto.")
    return 1 if FAIL else 0


def test_qa_master() -> None:
    """Punto de entrada para `pytest`."""
    assert main() == 0, FAIL


if __name__ == "__main__":
    raise SystemExit(main())
