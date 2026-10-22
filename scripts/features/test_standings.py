"""Validacion offline de las features de clasificacion y trascendencia.

La clasificacion es el bloque con mayor riesgo de fuga silenciosa de todo el
conjunto: la tabla del dia siguiente ya incorpora el resultado del partido que
se quiere predecir, y una tabla tomada con un dia de desfase produce features
perfectamente creibles que contienen la respuesta. El primer grupo de
comprobaciones existe para descartar exactamente eso.

El segundo mide la aritmetica de la trascendencia, que tiene una propiedad
igualmente incomoda: los tres indicadores son binarios y por tanto cualquier
error los deja pareciendo correctos. Se comprueban contra una tabla sintetica
con los puntos elegidos a mano, de modo que el valor esperado de cada indicador
se conoce antes de ejecutar nada, incluidos los dos casos donde se decide si la
cota es conservadora o se pasa de largo: un rival que llega JUSTO a los puntos
del equipo evaluado, y un rival con partidos aplazados, que puede sumar mas de
lo que al propio equipo le resta por disputar.

  1. FUGA         la tabla se lee ANTES del partido: en la jornada inaugural no
                  hay puesto, y el puesto de cada fecha refleja solo lo jugado
                  con fecha estrictamente anterior.
  2. ORDEN        la tabla ordena por puntos y desempata por diferencia y por
                  goles a favor, en ese orden.
  3. CORTES       las distancias al lider, a Champions y a Europa se miden
                  contra los puntos del equipo que ocupa el corte, y valen NaN
                  mientras la tabla no tenga equipos suficientes.
  4. TRASCENDENCIA los tres indicadores son cotas conservadoras: se obtienen
                  CONTANDO rivales cuyo techo o cuyos puntos actuales excluyen el
                  cruce, con comparaciones estrictas, y se anulan sin tabla previa.
  5. CONTRATO     lo producido coincide exactamente con lo que el catalogo
                  declara, con su columna de partido.

Una advertencia sobre DONDE se comprueba cada cosa. El catalogo decide para cada
atributo de equipo que lados llegan a la matriz, y a casi todos los de este
bloque les asigna solo la diferencia: `prev_position_diff` viaja a la fila de
partido, pero `home_prev_position` no. De una resta no se recupera ningun
sumando, asi que la aritmetica por equipo se comprueba sobre las dos vistas
donde esa aritmetica vive —`_snapshots`, que es la foto de la tabla, y
`standings_history`, que es la trayectoria por equipo con los indicadores ya
calculados—. La proyeccion a fila de partido, que la diferencia sea la resta de
los dos lados y que el conjunto de columnas sea el declarado, se comprueba
aparte contra el catalogo en [5] CONTRATO.
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
import standings as st  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str, detalle: object = None) -> None:
    """Registra una comprobacion; `detalle` solo se imprime cuando falla.

    Un fallo de conjunto —«las columnas producidas no son las declaradas»— no se
    diagnostica con un booleano: hace falta ver CUALES sobran y cuales faltan. Se
    imprime solo al fallar para no ensuciar la salida de la bateria verde.
    """
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")
    if not cond and detalle is not None:
        print(f"         -> {detalle}")


def _por_equipo(m: pd.DataFrame) -> pd.DataFrame:
    """Trayectoria por (match_key, equipo) con los indicadores ya calculados."""
    return st.standings_history(m).set_index(["match_key", "team_key"])


# --------------------------------------------------------------------------- #
# utilidades de construccion
# --------------------------------------------------------------------------- #
def _master(filas: list[tuple[str, str, str, str, int | None, int | None]],
            season: int = 2020) -> pd.DataFrame:
    """Master sintetico: (fecha, local, visitante, ftr, fthg, ftag).

    Un partido con `ftr` a None es un partido FUTURO: existe en el calendario,
    no aporta puntos a la tabla y es el caso que la ruta de inferencia recibe.
    """
    df = pd.DataFrame(filas, columns=["date", "home_key", "away_key",
                                      "ftr", "fthg", "ftag"])
    df.insert(0, "match_key",
              [f"{r.date}|{r.home_key}|{r.away_key}" for r in df.itertuples()])
    df["season_year"] = season
    df["ftr"] = df["ftr"].astype("string")
    return df


def _foto(snap: pd.DataFrame, fecha: str) -> pd.DataFrame:
    """La fila de la tabla correspondiente a una fecha, indexada por equipo."""
    return (snap[snap["date"] == pd.Timestamp(fecha)]
            .set_index("team_key"))


def _long_sintetico(puntos: list[float], jugados: int | list[int], fecha: str,
                    gd: list[float] | None = None,
                    gf: list[float] | None = None) -> pd.DataFrame:
    """Frame largo con una tabla previa de puntos ELEGIDOS a mano.

    Reparte los puntos de cada equipo en `jugados` filas anteriores a `fecha` y
    anade una fila sin disputar en `fecha`, que es la que provoca la foto. No se
    respeta el emparejamiento real —los puntos de un equipo no salen de rivales
    concretos— porque la foto de la tabla no lo necesita: suma por equipo lo
    anterior a la fecha. Renunciar al emparejamiento es lo que permite fijar la
    tabla exacta que se quiere probar en lugar de la que salga de un calendario.

    `jugados` admite una lista para dar a cada equipo un numero distinto de
    partidos disputados, que es como se representa un partido aplazado.
    """
    n = len(puntos)
    jug = [jugados] * n if isinstance(jugados, int) else list(jugados)
    gd = gd if gd is not None else [0.0] * n
    gf = gf if gf is not None else [0.0] * n
    D = pd.Timestamp(fecha)
    filas = []
    for i in range(n):
        equipo = f"e{i:02d}"
        for j in range(jug[i]):
            filas.append({
                "match_key": f"m{i}-{j}", "season_year": 2020,
                "date": D - pd.Timedelta(days=jug[i] - j),
                "is_home": j % 2, "team_key": equipo,
                # los puntos y el gol se concentran en la primera fila de cada
                # equipo: la foto suma, asi que el reparto interno es indiferente
                "pts": puntos[i] if j == 0 else 0.0,
                "gd": gd[i] if j == 0 else 0.0,
                "gf": gf[i] if j == 0 else 0.0,
                "played": True})
        filas.append({"match_key": f"f{i}", "season_year": 2020, "date": D,
                      "is_home": 1, "team_key": equipo, "pts": np.nan,
                      "gd": np.nan, "gf": np.nan, "played": False})
    return pd.DataFrame(filas)


# ---------------------------------------------------------------------------
# 1. ausencia de fuga
# ---------------------------------------------------------------------------
def test_fuga() -> None:
    print("\n1. FUGA — la tabla que se lee es la de ANTES del partido")
    m = _master([
        ("2020-09-12", "aa", "bb", "H", 2, 0),
        ("2020-09-12", "cc", "dd", "A", 0, 1),
        ("2020-09-19", "bb", "cc", "D", 1, 1),
        ("2020-09-19", "dd", "aa", "H", 3, 0),
        ("2020-09-26", "aa", "cc", None, None, None),
    ])
    out = st.build_standings_features(m).set_index("match_key")
    # El puesto de cada equipo ya no llega a la fila de partido —el catalogo le
    # da solo la diferencia— y de una resta no se recupera ninguno de los dos
    # puestos, asi que la lectura por equipo se hace sobre la trayectoria, que es
    # donde se calcula. Lo que si se comprueba sobre la fila de partido es lo que
    # la fila de partido de verdad contiene.
    eq = _por_equipo(m)

    check(pd.isna(eq.loc[("2020-09-12|aa|bb", "aa"), "prev_position"])
          and pd.isna(eq.loc[("2020-09-12|aa|bb", "bb"), "prev_position"]),
          "en la jornada inaugural no hay puesto: sin partidos previos no hay tabla")
    check(pd.isna(out.loc["2020-09-12|aa|bb", "home_matches_left"]),
          "y tampoco partidos restantes: el recuento sale de la tabla previa")
    check(pd.isna(out.loc["2020-09-12|aa|bb", "prev_position_diff"]),
          "y la diferencia hereda el hueco en vez de valer cero, que seria "
          "afirmar que los dos equipos estan igualados")

    # tras la 1a jornada: aa y dd ganaron (3 pts), bb y cc perdieron (0).
    check(eq.loc[("2020-09-19|bb|cc", "bb"), "prev_position"] in (3.0, 4.0)
          and eq.loc[("2020-09-19|bb|cc", "cc"), "prev_position"] in (3.0, 4.0),
          "en la 2a jornada los dos perdedores de la 1a ocupan los ultimos puestos")
    check(out.loc["2020-09-19|bb|cc", "home_matches_left"] == 37.0,
          "con un partido disputado restan 37 de los 38 del calendario")

    # el partido del 19 lo empataron bb y cc; si la foto del 26 usara la tabla
    # del dia siguiente al partido en vez de la anterior, aa —que perdio el 19—
    # apareceria con los puntos ya descontados de un partido que no ha jugado.
    check(out.loc["2020-09-26|aa|cc", "home_matches_left"] == 36.0,
          "en la 3a jornada restan 36: la foto cuenta dos jugados, no tres")
    check(out.loc["2020-09-26|aa|cc", "prev_position_diff"] <= 0.0,
          "aa (3 pts) va por delante de cc (1 pt) al entrar al partido, y el "
          "signo de la diferencia lo dice sin necesidad de los dos puestos")

    # un partido futuro no aporta puntos: si lo hiciera, la tabla del 26 tendria
    # un tercer partido contado para aa.
    check(out.loc["2020-09-26|aa|cc"]["home_matches_left"]
          > out.loc["2020-09-19|dd|aa"]["home_matches_left"] - 2,
          "el partido sin resultado no suma a la tabla (no se cuenta como jugado)")


# ---------------------------------------------------------------------------
# 2. orden y desempate
# ---------------------------------------------------------------------------
def test_orden() -> None:
    print("\n2. ORDEN — puntos, luego diferencia, luego goles a favor")
    # tres equipos con los mismos puntos y desempates escalonados
    long = _long_sintetico(puntos=[9.0, 9.0, 9.0, 3.0], jugados=3,
                           fecha="2020-11-01",
                           gd=[5.0, 5.0, 9.0, 0.0], gf=[10.0, 12.0, 9.0, 3.0])
    foto = _foto(st._snapshots(long), "2020-11-01")["prev_position"]
    check(foto["e02"] == 1.0,
          "con puntos iguales manda la diferencia de goles (e02, +9, es primero)")
    check(foto["e01"] == 2.0 and foto["e00"] == 3.0,
          "a igualdad de puntos y diferencia desempatan los goles a favor")
    check(foto["e03"] == 4.0, "el ultimo por puntos cierra la tabla")


# ---------------------------------------------------------------------------
# 3. cortes de la tabla
# ---------------------------------------------------------------------------
def test_cortes() -> None:
    print("\n3. CORTES — distancias medidas contra el equipo que ocupa el puesto")
    puntos = [80.0, 78.0, 76.0, 74.0, 72.0, 70.0,
              45.0, 44.0, 43.0, 42.0, 41.0, 40.0, 39.0, 38.0, 37.0, 36.0, 35.0,
              12.0, 10.0, 8.0]
    long = _long_sintetico(puntos, jugados=30, fecha="2021-04-11")
    snap = _foto(st._snapshots(long), "2021-04-11")

    check(snap.loc["e00", "points_to_leader"] == 0.0,
          "el lider esta a cero puntos de si mismo")
    check(snap.loc["e19", "points_to_leader"] == 72.0,
          "el colista esta a 80-8 = 72 puntos del lider")
    check(snap.loc["e05", "points_to_europa"] == 0.0,
          "quien ocupa el 6o puesto esta a cero de Europa")
    check(snap.loc["e00", "points_to_champions"] == -6.0,
          "por encima del corte la distancia es negativa (74-80 = -6), que es "
          "el margen con el que se ocupa la plaza")
    check(snap.loc["e19", "points_above_drop"] == -4.0,
          "el colista esta 4 puntos por debajo del primer puesto de descenso")

    # tabla incompleta: con menos de 18 equipos el corte del descenso no existe
    corto = _long_sintetico([9.0, 6.0, 3.0], jugados=3, fecha="2020-11-01")
    s2 = _foto(st._snapshots(corto), "2020-11-01")
    check(pd.isna(s2.loc["e00", "points_above_drop"]),
          "con menos de 18 equipos el corte del descenso es NaN y no los puntos "
          "del ultimo clasificado")
    check(s2.loc["e00", "points_to_leader"] == 0.0,
          "el corte del lider, que si existe, se sigue calculando")


# ---------------------------------------------------------------------------
# 4. trascendencia
# ---------------------------------------------------------------------------
def test_trascendencia() -> None:
    print("\n4. TRASCENDENCIA — cotas conservadoras sobre lo aun recuperable")
    # 30 jugados => restan 8 => 24 puntos como maximo recuperable.
    # corte de descenso (18o) = 12 pts ; corte de Europa (6o) = 70 pts.
    puntos = [80.0, 78.0, 76.0, 74.0, 72.0, 70.0,
              45.0, 44.0, 43.0, 42.0, 41.0, 40.0, 39.0, 38.0, 37.0, 36.0, 35.0,
              12.0, 10.0, 8.0]
    long = _long_sintetico(puntos, jugados=30, fecha="2021-04-11")
    snap = _foto(st._snapshots(long), "2021-04-11")

    ml = 38 - 30
    check((snap["jugados"] == 30).all(),
          "la foto cuenta 30 partidos disputados por equipo")
    check(st._TOTAL_MATCHES - 30 == ml,
          f"restan {ml} partidos, es decir {3 * ml} puntos como maximo recuperable")

    # Los recuentos son los que emite el modulo; aqui solo se aplica el umbral,
    # que es la lectura de esos recuentos (tantos rivales inalcanzables como
    # plazas de descenso, seis rivales inalcanzables para Europa).
    plazas = len(puntos) - st._DROP_POS + 1
    safe = snap["rivals_unreachable_below"] >= plazas
    unre = snap["rivals_out_of_reach_above"] >= st._EUROPA_POS
    check(plazas == 3, "en una liga de 20 equipos hay tres plazas de descenso")

    check(bool(safe["e00"]) and not bool(unre["e00"]),
          "el lider esta salvado y sigue con Europa a tiro: se juega algo")
    check(bool(safe["e06"]) and bool(unre["e06"]),
          "e06 (45 pts, techo 69) no se juega nada: por debajo hay tres equipos "
          "que no le alcanzan y por encima seis a los que el no alcanza")
    check(not bool(safe["e16"]) and bool(unre["e16"]),
          "e16 (35 pts) aun puede descender —solo dos rivales tienen el techo "
          "por debajo— aunque Europa le sea inalcanzable")
    check(not bool(safe["e19"]) and bool(unre["e19"]),
          "el colista se juega la permanencia")

    # frontera: el 18o llega JUSTO a los puntos de e15, y eso no basta.
    check(snap.loc["e15", "points_above_drop"] == float(3 * ml),
          "e15 esta exactamente a los puntos recuperables del descenso")
    check(int(snap.loc["e15", "rivals_unreachable_below"]) == 2,
          "el techo del 18o iguala los puntos de e15, asi que solo dos rivales "
          "quedan por debajo de forma demostrable")
    check(not bool(safe["e15"]),
          "y no se declara salvado: el recuento es estricto, porque un rival que "
          "llega justo a sus puntos puede superarle en el desempate")
    check(bool(safe["e14"]),
          "un punto mas (e14) deja los tres techos por debajo y si basta")

    # --- partido aplazado: el rival de abajo tiene un partido de mas -----------
    # Misma tabla, pero el 18o llega con cuatro partidos aplazados. Su techo pasa
    # de 36 a 48, de modo que puede superar a e14 pese a que el colchon de e14
    # (25 puntos) siga excediendo los 24 que a el mismo le quedan por disputar.
    jug = [30] * 20
    jug[17] = 26
    snap_apl = _foto(st._snapshots(_long_sintetico(puntos, jug, "2021-04-11")),
                     "2021-04-11")
    safe_apl = snap_apl["rivals_unreachable_below"] >= plazas
    check(snap_apl.loc["e14", "points_above_drop"] == 25.0,
          "el colchon de e14 sobre el 18o no cambia: son los mismos puntos")
    check(not bool(safe_apl["e14"]),
          "pero ya no se declara salvado: con cuatro partidos aplazados el 18o "
          "puede sumar mas de lo que a e14 le resta, y una regla apoyada en los "
          "partidos pendientes PROPIOS habria afirmado una salvacion indemostrable")
    check(bool(safe["e14"]) and not bool(safe_apl["e14"]),
          "la diferencia entre los dos casos es solo el calendario del rival, que "
          "es exactamente lo que el recuento por equipos incorpora")

    # --- competicion sin zona de descenso -------------------------------------
    m_corto = _master([
        ("2020-09-12", "aa", "bb", "H", 2, 0),
        ("2020-09-19", "bb", "aa", "H", 1, 0),
        ("2020-09-26", "aa", "bb", "D", 1, 1),
    ])
    o = st.build_standings_features(m_corto).set_index("match_key")
    eq = _por_equipo(m_corto)
    check(pd.isna(eq.loc[("2020-09-26|aa|bb", "aa"), "safe_from_drop"]),
          "con menos equipos que puestos de referencia el indicador es NaN y no "
          "un cero: no se afirma nada sobre una zona que no existe")
    check(pd.isna(o.loc["2020-09-26|aa|bb", "safe_from_drop_diff"]),
          "y el hueco se propaga a la diferencia, que es lo unico que de este "
          "indicador llega a la matriz")
    check(o.loc["2020-09-26|aa|bb", "home_matches_left"] == 36.0,
          "lo que si se sigue contando son los partidos restantes, que no "
          "dependen del tamano de la competicion")


# ---------------------------------------------------------------------------
# 5. contrato del modulo
# ---------------------------------------------------------------------------
def test_contrato() -> None:
    print("\n5. CONTRATO — lo declarado, las diferencias y la columna de partido")
    # El contrato ya no es «cada atributo sale como par y como diferencia». De un
    # atributo de equipo caben tres columnas en la fila de partido y las tres son
    # linealmente dependientes, asi que el catalogo elige cuales salen: a casi
    # todos los de este bloque les da solo la diferencia, a `matches_left` solo el
    # lado local —porque es el mismo numero para los dos— y a la zona el par, que
    # es categorica y no se resta. Lo que se comprueba es que lo producido y lo
    # declarado sean el MISMO conjunto en las dos direcciones: que no falte nada
    # que el catalogo emita —una feature calculada y tirada a la basura— y que no
    # sobre nada que no emita, porque una columna que llega a la matriz sin estar
    # declarada se salta la curacion entera.
    nombres = st.standings_feature_names()
    esperadas = (["match_key"]
                 + [c for b in nombres["numeric"] + nombres["categorical"]
                    for c in catalogo.nombres(b)]
                 + nombres["match_level"])

    m = _master([
        ("2019-09-12", "aa", "bb", "H", 2, 0),
        ("2019-09-19", "bb", "aa", "H", 1, 0),
        ("2020-09-12", "aa", "bb", "H", 2, 0),
        ("2020-09-19", "bb", "aa", "A", 0, 1),
    ])
    m.loc[m["date"].str.startswith("2019"), "season_year"] = 2019
    out = st.build_standings_features(m)

    check(set(out.columns) == set(esperadas),
          "lo producido coincide exactamente con lo que el catalogo declara",
          sorted(set(out.columns) ^ set(esperadas)))
    check(out["match_key"].is_unique and len(out) == len(m),
          "una fila por partido y match_key unico")
    check("stakes_none_match" in out.columns and "stakes_none_match_diff" not in out.columns,
          "`stakes_none_match` es columna de partido y no se desdobla ni se resta")
    check("home_stakes_none" not in out.columns
          and "europe_unreachable_diff" not in out.columns,
          "los dos insumos se calculan pero no salen: `europe_unreachable` por "
          "redundar con `points_to_europa`, y `stakes_none` por lado porque la "
          "pregunta es si el partido no importa a NINGUNO de los dos")

    # La resta se comprueba contra la trayectoria por equipo y no contra dos
    # columnas de la propia fila, porque esas dos columnas ya no existen: es
    # precisamente el paso que la diferencia hace irreversible, y el unico sitio
    # desde el que se puede verificar es el frame donde ambos lados aun conviven.
    eq = st.standings_history(m).set_index(["match_key", "is_home"])
    sub = out.dropna(subset=["prev_position_diff"]).set_index("match_key")
    h = eq.xs(1, level="is_home").loc[sub.index, "prev_position"]
    a = eq.xs(0, level="is_home").loc[sub.index, "prev_position"]
    check(np.allclose(sub["prev_position_diff"].to_numpy(),
                      (h - a).to_numpy()),
          "cada diferencia es la resta de los dos lados de la trayectoria")

    # cambio dinamico: aa acabo 2019 por delante de bb (una victoria cada uno,
    # aa con mejor diferencia), asi que en 2020 arrastra final_position = 1.
    k2020 = [k for k in out["match_key"] if k.startswith("2020-09-19")][0]
    check(not pd.isna(eq.loc[(k2020, 1), "dynamic_pos_change"]),
          "`dynamic_pos_change` existe en la 2a temporada: compara el puesto "
          "actual con el final de la anterior")
    k2019 = [k for k in out["match_key"] if k.startswith("2019")][0]
    check(pd.isna(eq.loc[(k2019, 1), "dynamic_pos_change"]),
          "y no existe en la primera temporada del historico, que no tiene previa")

    check(set(nombres["numeric"]) & set(nombres["categorical"]) == set()
          and set(nombres["numeric"]) & set(nombres["match_level"]) == set(),
          "ninguna columna se declara en dos categorias a la vez")


def _run() -> None:
    print("=" * 72)
    print("TEST — features de clasificacion y trascendencia")
    print("=" * 72)
    test_fuga()
    test_orden()
    test_cortes()
    test_trascendencia()
    test_contrato()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_standings() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
