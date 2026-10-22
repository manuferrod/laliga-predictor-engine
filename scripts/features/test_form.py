"""Validacion offline del bloque de forma. Sin red y sin datos reales.

La forma es el bloque mas expuesto a la fuga de informacion de todo el proyecto:
son medias de resultados pasados calculadas sobre la misma tabla que contiene el
resultado presente, y un desplazamiento mal puesto no produce ningun error —
produce un modelo que acierta mucho y no sirve para nada. Las dos
parametrizaciones (ventana rectangular y ponderacion exponencial) y la lectura de
la dureza del calendario multiplican las oportunidades de cometer ese fallo, asi
que cada una se comprueba contra un caso sintetico cuyo valor correcto esta
calculado a mano.

La aritmetica se comprueba sobre el TEAM-LONG, no sobre la fila de partido: el
catalogo decide para cada atributo que lados llegan a la matriz, y la mayoria
llega solo como diferencia, de la que no se puede recuperar el valor de un
equipo. Que la proyeccion a fila de partido emita exactamente los lados
declarados es una pregunta distinta, y la responde [6] CONTRATO.

Se comprueban, sobre secuencias construidas con el resultado conocido:

  1. ANTIFUGA     ninguna columna de forma del partido t depende de lo ocurrido
                  en t; el primer partido de cada equipo y temporada no tiene
                  forma, y alterar el marcador de t deja intactas sus features.
  2. ARITMETICA   la media movil y la media exponencial de semivida declarada
                  valen exactamente lo que dice su definicion.
  3. AGRUPACION   la forma se reinicia por temporada y no cruza equipos.
  4. RIVAL        opp_elo_z lleva a la fila de cada equipo la fuerza tipificada
                  del rival —el mismo partido visto por los dos lados— y al
                  promediarse mide la dureza del calendario YA recorrido, no la
                  del que viene.
  5. NAN-SAFE     una fila de partido futuro no entra en las ventanas como si
                  fuera un rendimiento nulo.
  6. CONTRATO     `form_feature_names` describe exactamente lo que
                  `build_form_features` produce, y todo el bloque pertenece al
                  regimen semanal (ninguna columna lleva el token de cierre).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import catalogo  # noqa: E402
import columns as C  # noqa: E402
import form as F  # noqa: E402
import prematch_derived as pdrv  # noqa: E402
from teamlong import to_team_long  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []
_TOL = 1e-9

# La ventana rectangular la fija el catalogo, no esta bateria. Los valores
# esperados a mano de [2] ARITMETICA estan calculados para la ventana declarada,
# asi que si alguien la cambia el test tiene que fallar diciendolo, no seguir
# comprobando una columna que ya no existe con otro nombre.
_W = F.WINDOWS[0]


def check(cond: bool, msg: str, detalle: object = None) -> None:
    """Registra una comprobacion; `detalle` solo se imprime cuando falla.

    Un fallo de conjunto («lo producido no coincide con lo declarado») no se
    diagnostica con un booleano: hace falta ver QUE columna sobra o falta. Se
    imprime solo en el fallo para no ensuciar la salida de la bateria verde.
    """
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")
    if not cond and detalle is not None:
        print(f"         -> {detalle}")


# --------------------------------------------------------------------------- #
# generador de masters sinteticos
# --------------------------------------------------------------------------- #
def _master(filas: list[dict]) -> pd.DataFrame:
    """Master minimo con lo que teamlong exige, mas los Elo pre-partido.

    Cada fila declara temporada, fecha, los dos equipos y el marcador. El resto
    de columnas del master real (xG, tiros, cuotas...) se dejan ausentes a
    proposito: `_col` debe devolver NaN y el modulo tiene que seguir produciendo
    su esquema completo, que es justo la situacion de una fuente que aun no
    cubre una temporada.

    El Elo entra TIPIFICADO dentro de la jornada (`home_elo_z`, `away_elo_z`):
    desviaciones respecto a la media de Primera ese dia, que es la unica lectura
    comparable a traves del empalme de las dos escalas de ClubElo (ver
    `consolidate/spine.add_clubelo`). Los fixtures de abajo usan valores del
    orden de +-2 por eso, y no por comodidad: un 1700 en estas comprobaciones
    seria la senal de que la consolidacion ha vuelto a publicar el nivel crudo
    con el nombre tipificado.
    """
    df = pd.DataFrame(filas)
    df["date"] = pd.to_datetime(df["date"])
    df["match_key"] = (df["date"].dt.strftime("%Y-%m-%d") + "|"
                       + df["home_key"] + "|" + df["away_key"])
    if "round" not in df.columns:
        df["round"] = np.nan
    return df[["match_key", "season_year", "date", "round",
               "home_key", "away_key", "fthg", "ftag"]
              + [c for c in df.columns if c.endswith("_elo_z")]]


def _liga(marcadores: list[tuple[str, str, float, float]],
          season: int = 2020, elos: dict[str, float] | None = None) -> pd.DataFrame:
    """Secuencia de partidos en jornadas consecutivas (una por semana)."""
    filas = []
    for i, (h, a, gh, ga) in enumerate(marcadores):
        fila = {"season_year": season,
                "date": pd.Timestamp("2020-08-15") + pd.Timedelta(days=7 * i),
                "home_key": h, "away_key": a, "fthg": gh, "ftag": ga}
        if elos:
            fila["home_elo_z"] = elos.get(h, np.nan)
            fila["away_elo_z"] = elos.get(a, np.nan)
        filas.append(fila)
    return _master(filas)


def _forma_larga(master: pd.DataFrame) -> pd.DataFrame:
    """Master -> team-long con las features de forma ya calculadas.

    La aritmetica de la forma vive en el team-long: una fila por equipo y
    partido, donde cada atributo existe SIEMPRE para los dos contendientes. La
    fila de partido es una proyeccion posterior de esa tabla, y desde que el
    catalogo decide lado por lado la mayoria de los atributos se proyecta solo
    como diferencia (`form_points_3_diff`), sin que el valor del local ni el del
    visitante lleguen a la matriz.

    Comprobar la aritmetica contra la proyeccion obligaria a reconstruir el valor
    de un equipo a partir de una resta, que es imposible, o a limitar las pruebas
    al puñado de atributos que siguen saliendo por pares, que dejaria sin vigilar
    justo a la mayoria. Se comprueba, por tanto, donde se calcula. Que la
    proyeccion emita exactamente los lados declarados es una pregunta distinta y
    tiene su propia comprobacion en [6] CONTRATO.
    """
    long, _, _ = F._add_team_features(to_team_long(master))
    return long


def _serie_equipo(long: pd.DataFrame, equipo: str, col: str) -> list[float]:
    """Valor de `col` para `equipo` en cada uno de SUS partidos, en orden."""
    s = long.loc[long["team_key"] == equipo].sort_values(["date", "match_key"])
    return list(s[col])


# --------------------------------------------------------------------------- #
# 1. ANTIFUGA
# --------------------------------------------------------------------------- #
def test_antifuga() -> None:
    print("\n[1] ANTIFUGA — la fila del partido t no ve el partido t")
    m = _liga([("aaa", "bbb", 3, 0), ("aaa", "ccc", 2, 1),
               ("ddd", "aaa", 0, 4), ("aaa", "eee", 1, 1)])
    w = F.build_form_features(m)
    long = _forma_larga(m)

    pts = _serie_equipo(long, "aaa", f"form_points_{_W}")
    check(pd.isna(pts[0]), "el primer partido del equipo no tiene forma (NaN)")
    check(abs(pts[1] - 3.0) < _TOL, "el segundo partido ve solo el primero (3 puntos)")
    check(abs(pts[2] - 3.0) < _TOL, "el tercero promedia las dos victorias previas")

    ewm = _serie_equipo(long, "aaa", "form_points_ewm")
    check(pd.isna(ewm[0]), "la forma exponencial tampoco existe en el primer partido")
    check(abs(ewm[1] - 3.0) < _TOL, "con un solo previo, exponencial y media coinciden")

    # prueba de intervencion: cambiar el marcador de un partido no puede alterar
    # las features de ESE partido, solo las de los posteriores. Se barren TODAS
    # las columnas de forma que el catalogo emite —no las que empiezan por
    # `home_form_`, que desde la curacion son una minoria—, porque la fuga puede
    # entrar por cualquiera de ellas y la diferencia es tan capaz de filtrar el
    # resultado como el par del que sale.
    m2 = m.copy()
    m2.loc[m2.index[2], ["fthg", "ftag"]] = [9, 0]     # ddd 9-0 aaa
    w2 = F.build_form_features(m2)
    forma_cols = [c for c in w.columns
                  if c != "match_key" and pd.api.types.is_numeric_dtype(w[c])]
    fila, fila2 = (w.set_index("match_key").loc[m.loc[m.index[2], "match_key"]],
                   w2.set_index("match_key").loc[m.loc[m.index[2], "match_key"]])
    igual = all(
        (pd.isna(fila[c]) and pd.isna(fila2[c])) or abs(float(fila[c]) - float(fila2[c])) < _TOL
        for c in forma_cols)
    check(igual, "alterar el marcador de t deja intactas TODAS las features de forma de t",
          [c for c in forma_cols
           if not ((pd.isna(fila[c]) and pd.isna(fila2[c]))
                   or abs(float(fila[c]) - float(fila2[c])) < _TOL)][:8])

    # ...pero SI tiene que alterar las del partido siguiente, o el desplazamiento
    # estaria borrando informacion en vez de ordenarla en el tiempo. Se mira en el
    # team-long y no en la fila de partido porque el rival de esa jornada es un
    # equipo debutante: su forma es NaN, y la diferencia local-visitante —que es
    # como `form_points_3` viaja a la matriz— seria NaN en los dos escenarios, de
    # modo que la comparacion no distinguiria nada.
    antes = _serie_equipo(long, "aaa", f"form_points_{_W}")[3]
    despues = _serie_equipo(_forma_larga(m2), "aaa", f"form_points_{_W}")[3]
    check(despues < antes, "y si altera las del partido siguiente (la forma empeora)")


# --------------------------------------------------------------------------- #
# 2. ARITMETICA
# --------------------------------------------------------------------------- #
def test_aritmetica() -> None:
    print("\n[2] ARITMETICA — media movil y media exponencial valen su definicion")
    # aaa juega 5 partidos con diferencias de gol conocidas: +3, -1, +2, 0, +1
    # aaa juega 5 partidos y marca 3, 0, 2, 1, 1 goles (la diferencia de goles ya
    # no es un atributo de forma: viaja descompuesta en `gf` y `ga`, que dicen lo
    # mismo y ademas distinguen al equipo que gana marcando del que gana sin
    # encajar).
    m = _liga([("aaa", "b1", 3, 0), ("b2", "aaa", 1, 0), ("aaa", "b3", 2, 0),
               ("b4", "aaa", 1, 1), ("aaa", "b5", 1, 0)])
    check(_W == 3, "los valores esperados a mano suponen ventana 3 "
                   f"(el catalogo declara {_W}: hay que rehacerlos)")
    long = _forma_larga(m)
    gf = [3.0, 0.0, 2.0, 1.0, 1.0]

    obs3 = _serie_equipo(long, "aaa", f"form_gf_{_W}")
    esp3 = [np.nan, 3.0, 1.5, (3 + 0 + 2) / 3, (0 + 2 + 1) / 3]
    ok = all((pd.isna(o) and pd.isna(e)) or abs(o - e) < _TOL for o, e in zip(obs3, esp3))
    check(ok, f"form_gf_{_W} = media de los {_W} gf anteriores (min_periods=1)")

    # media exponencial de pandas con halflife: alpha = 1 - 0.5**(1/H), pesos
    # (1-alpha)^k sobre las observaciones ya desplazadas, normalizados.
    alpha = 1.0 - 0.5 ** (1.0 / F.EWM_HALFLIFE)
    obse = _serie_equipo(long, "aaa", "form_gf_ewm")
    espe = [np.nan]
    for i in range(1, 5):
        prev = gf[:i][::-1]                       # del mas reciente al mas antiguo
        pesos = [(1 - alpha) ** k for k in range(len(prev))]
        espe.append(float(np.dot(prev, pesos) / sum(pesos)))
    ok = all((pd.isna(o) and pd.isna(e)) or abs(o - e) < 1e-8 for o, e in zip(obse, espe))
    check(ok, f"form_gf_ewm = media exponencial de semivida {F.EWM_HALFLIFE:g} sobre lo anterior")

    check(abs(obse[4] - obs3[4]) > 1e-3,
          "las dos parametrizaciones no son la misma columna con otro nombre")
    # el ultimo resultado pesa mas en la exponencial que en la rectangular
    peso_ult_ewm = 1.0 / sum((1 - alpha) ** k for k in range(4))
    check(peso_ult_ewm > 1 / _W,
          f"en la exponencial el partido mas reciente pesa mas que en la ventana de {_W}")

    # el acumulado de temporada tiene otra memoria: no olvida, promedia todo lo
    # jugado. aaa suma 3+0+3+1 puntos en sus cuatro primeros partidos.
    cum = _serie_equipo(long, "aaa", "cum_points_pg")
    check(abs(cum[4] - (3 + 0 + 3 + 1) / 4) < _TOL,
          "cum_points_pg promedia TODOS los puntos previos de la temporada, no los 3 ultimos")


# --------------------------------------------------------------------------- #
# 3. AGRUPACION
# --------------------------------------------------------------------------- #
def test_agrupacion() -> None:
    print("\n[3] AGRUPACION — la forma se reinicia por temporada y no cruza equipos")
    filas = []
    for i, (h, a, gh, ga) in enumerate([("aaa", "bbb", 3, 0), ("aaa", "ccc", 3, 0)]):
        filas.append({"season_year": 2020, "date": pd.Timestamp("2020-08-15")
                      + pd.Timedelta(days=7 * i), "home_key": h, "away_key": a,
                      "fthg": gh, "ftag": ga})
    filas.append({"season_year": 2021, "date": pd.Timestamp("2021-08-15"),
                  "home_key": "aaa", "away_key": "bbb", "fthg": 0, "ftag": 0})
    m = _master(filas)
    long = _forma_larga(m)
    ultimo = long.set_index(["match_key", "team_key"]).loc[
        (m["match_key"].iloc[-1], "aaa")]
    check(pd.isna(ultimo[f"form_points_{_W}"]),
          "el primer partido de una temporada nueva no arrastra la forma de la anterior")
    check(pd.isna(ultimo["form_points_ewm"]),
          "y la exponencial se reinicia igual que la rectangular")
    check(ultimo["matches_prev"] == 0,
          "matches_prev vuelve a cero al cambiar de temporada")

    # bbb solo ha jugado su primer partido de 2020 antes del de 2021: su forma
    # de 2021 no puede contener nada, aunque en la tabla haya filas intermedias
    # de otros equipos.
    rival = long.set_index(["match_key", "team_key"]).loc[
        (m["match_key"].iloc[-1], "bbb")]
    check(pd.isna(rival[f"form_gf_{_W}"]),
          "la forma de un equipo no se contamina con partidos de otros")


# --------------------------------------------------------------------------- #
# 4. FORMA AJUSTADA AL RIVAL
# --------------------------------------------------------------------------- #
def test_rival() -> None:
    print("\n[4] RIVAL — opp_elo_z mide la dificultad del calendario ya recorrido")
    # Desviaciones respecto a la media de Primera, no puntos de rating: el Elo
    # viaja tipificado dentro de la jornada porque la serie publicada empalma las
    # dos escalas de ClubElo. Un equipo a +1.8 es uno de los dos o tres mejores
    # del dia; uno a -1.2, candidato al descenso.
    elos = {"fuerte": 1.8, "flojo": -1.2}
    m = _liga([("fuerte", "flojo", 1, 0), ("flojo", "fuerte", 1, 0),
               ("fuerte", "flojo", 0, 0)], elos=elos)
    long = to_team_long(m)
    check("opp_elo_z" in long.columns,
          "teamlong publica el Elo del rival en la fila del equipo")
    fila_h = long[(long["match_key"] == m["match_key"].iloc[0]) & (long["is_home"] == 1)]
    check(float(fila_h["opp_elo_z"].iloc[0]) == -1.2,
          "opp_elo_z del local es el Elo del visitante")
    fila_a = long[(long["match_key"] == m["match_key"].iloc[0]) & (long["is_home"] == 0)]
    check(float(fila_a["opp_elo_z"].iloc[0]) == 1.8,
          "y el del visitante es el del local: el mismo partido visto por los dos lados")

    # El residuo de sorpresa respecto al Elo esperado (`elo_surprise`) YA NO SE
    # CONSTRUYE. Medía el rendimiento ponderado por la dificultad del rival, que
    # es la misma pregunta que responden juntos el resultado (`form_points_*`) y
    # la dureza del calendario (`form_opp_elo_z_*`), ambos ya en la matriz. Se
    # comprueba que no ha vuelto por la puerta de atras: un atributo que un
    # generador produzca sin que el catalogo lo declare no llega a la matriz,
    # pero si llega al long y ahi se paga en tiempo de computo sin usarse.
    long2, num, _ = F._add_team_features(long.copy())
    check(not any("elo_surprise" in c for c in long2.columns),
          "el residuo de sorpresa no se calcula ni siquiera como insumo del long")
    check(not any("elo_surprise" in c for c in num),
          "ni figura entre las features numericas que el modulo declara")

    w = F.build_form_features(m)
    check(not any("elo_surprise" in c for c in w.columns),
          "y no queda rastro suyo en la fila de partido")
    check(any("form_opp_elo_z" in c for c in w.columns),
          "la dificultad de calendario si se promedia (fuerza de los rivales)")
    dif = _serie_equipo(long2, "flojo", f"form_opp_elo_z_{_W}")
    check(abs(dif[1] - 1.8) < _TOL,
          "form_opp_elo_z mide el Elo medio de los rivales YA enfrentados")

    # La logistica Elo sobrevive como funcion de analisis aunque ya no alimente
    # ninguna columna: se comprueba la propiedad que la hace utilizable para leer
    # el Elo en escala de probabilidad, que es la complementariedad de los lados.
    check(abs(pdrv.elo_expected_score(0.0, pdrv.HOME_ADV_ELO)
              + pdrv.elo_expected_score(0.0, -pdrv.HOME_ADV_ELO) - 1.0) < 1e-12,
          "las expectativas Elo de los dos lados son complementarias (suman 1)")


# --------------------------------------------------------------------------- #
# 5. NAN-SAFE (ruta de inferencia)
# --------------------------------------------------------------------------- #
def test_nan_safe() -> None:
    print("\n[5] NAN-SAFE — un partido futuro no cuenta como rendimiento nulo")
    m = _liga([("aaa", "b1", 3, 0), ("aaa", "b2", np.nan, np.nan), ("aaa", "b3", 3, 0),
               ("aaa", "b4", np.nan, np.nan)], elos={"aaa": 0.4})
    long2 = _forma_larga(m)
    pts = _serie_equipo(long2, "aaa", f"form_points_{_W}")
    check(abs(pts[2] - 3.0) < _TOL,
          "la jornada sin marcador se ignora en la ventana en vez de entrar como 0 puntos")
    check(abs(pts[3] - 3.0) < _TOL, "y sigue ignorandose en la jornada siguiente")

    # El hueco tiene que propagarse como hueco tambien en el team-long, que es
    # donde se calculan las ventanas: si el partido sin marcador entrase con un
    # rendimiento imputado a cero, la media rodante lo leeria como una derrota y
    # el equipo apareceria en horas bajas sin haber jugado.
    futuros = long2.loc[long2["gf"].isna()]
    check(futuros["points"].isna().all(),
          "sin marcador no hay puntos que contabilizar (NaN, no cero)")
    check(futuros["gd"].isna().all(),
          "ni diferencia de goles (NaN, no empate a cero)")

    ewm = _serie_equipo(long2, "aaa", "form_points_ewm")
    check(not pd.isna(ewm[2]) and abs(ewm[2] - 3.0) < _TOL,
          "la exponencial tampoco interpreta el hueco como un mal resultado")


# --------------------------------------------------------------------------- #
# 6. CONTRATO Y REGIMEN
# --------------------------------------------------------------------------- #
def test_contrato() -> None:
    print("\n[6] CONTRATO — lo declarado es exactamente lo producido")
    m = _liga([("aaa", "bbb", 1, 0), ("bbb", "aaa", 2, 2), ("aaa", "ccc", 0, 1)],
              elos={"aaa": 0.4, "bbb": 0.1, "ccc": -0.3})
    w = F.build_form_features(m)
    nombres = F.form_feature_names()
    base = nombres["numeric"] + nombres["categorical"]

    check(len(base) == len(set(base)), "la lista declarada no tiene nombres repetidos")

    # El contrato ya no es «cada atributo sale como par y como diferencia». De un
    # atributo de equipo caben tres columnas en la fila de partido —local,
    # visitante y resta— y las tres son linealmente dependientes, asi que el
    # catalogo elige para cada uno cuales salen y la mayoria viaja solo como
    # diferencia. Lo que se comprueba, entonces, es que lo producido y lo
    # declarado sean el MISMO conjunto en las dos direcciones: que no falte
    # ninguna columna que el catalogo emita (un atributo calculado y tirado a la
    # basura es tiempo de computo perdido y una feature que el modelo no vera) y
    # que no sobre ninguna que no emita (una columna que llega a la matriz sin
    # estar declarada se salta la curacion entera, que es justo lo que el
    # catalogo existe para impedir).
    producidas = {c for c in w.columns if c != "match_key"}
    declaradas = {c for b in base for c in catalogo.nombres(b)}
    check(producidas == declaradas,
          "lo producido coincide exactamente con lo que el catalogo declara",
          sorted(producidas ^ declaradas)[:8])

    check(all(f"{c}_diff" not in w.columns for c in nombres["categorical"]),
          "las categoricas NO generan diferencia (restar etiquetas no significa nada)")

    # las dos parametrizaciones cubren el mismo conjunto de atributos
    ewm_attrs = {n[len("form_"):-len("_ewm")] for n in nombres["numeric"]
                 if n.startswith("form_") and n.endswith("_ewm")}
    check(ewm_attrs == set(F._ROLL_ATTRS),
          "la forma exponencial cubre exactamente los mismos atributos que la rectangular")

    todas = [c for c in w.columns if c != "match_key"]
    fuera = C.weekly_unavailable(todas)
    check(not fuera,
          "ninguna columna de forma queda fuera del regimen semanal (no hay token de cierre)")

    check(w["match_key"].is_unique and len(w) == len(m),
          "la salida es una fila por partido, con match_key unica")


def _run() -> None:
    print("=" * 72)
    print("TEST — bloque de forma (ventana, exponencial y dureza del calendario)")
    print("=" * 72)
    test_antifuga()
    test_aritmetica()
    test_agrupacion()
    test_rival()
    test_nan_safe()
    test_contrato()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_form() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
