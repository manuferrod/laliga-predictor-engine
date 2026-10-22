"""Validacion offline de las fuerzas latentes de ataque y defensa. Sin red.

El bloque de fuerza tiene el problema opuesto al de contexto: no sale de una
tabla que se pueda leer a ojo, sino de un ajuste iterativo cuyo resultado nadie
sabe de memoria. Un signo invertido, una normalizacion mal cerrada o un corte
temporal mal puesto producen numeros perfectamente razonables --logaritmos
pequenos alrededor de cero, goles esperados de orden uno, probabilidades que
suman uno-- y ninguna de esas averias asoma en un histograma.

La estrategia es por tanto generar el dato con el mismo modelo que se ajusta.
Se fijan unas fuerzas verdaderas, se simulan diez temporadas de marcadores de
Poisson con ellas y se comprueba que el modulo las reencuentra. Lo que valida
esa prueba no es solo el optimizador: si el ataque y la defensa estuvieran
cruzados, o la ventaja de campo aplicada al lado equivocado, la correlacion con
la verdad se hundiria.

Se comprueba, en este orden:

  1. RECUPERACION  sobre una liga simulada con parametros conocidos, las dos
                   fuerzas estimadas correlacionan con las verdaderas, la
                   ventaja de campo y el nivel medio se reencuentran, y la
                   normalizacion de identificabilidad se cumple exactamente.
  2. ANTI-FUGA     el resultado de un partido no influye en las features de su
                   propia fecha --ni en las suyas ni en las de otro partido del
                   mismo dia-- y si influye en las de las fechas siguientes, que
                   es la prueba de que el corte esta puesto y no de mas.
  3. HISTORIA      un club sin recorrido suficiente no recibe fuerza, y el
                   partido entero queda en NaN incluido su pronostico, en vez de
                   heredar la media de la liga como si fuera una medicion.
  4. SIGNOS        positivo es mejor en las dos columnas y cada diferencia
                   publicada es exactamente la resta de las dos trayectorias.
  5. PRONOSTICO    la terna 1X2 suma uno, vive en [0, 1], ordena a favor del
                   mejor equipo y recoge la ventaja de campo entre iguales.
  6. FUTURO        una jornada sin resultado no entra en el ajuste pero si
                   recibe features, que es lo que hace utilizable el modulo en
                   la ruta de inferencia sin ninguna regla adicional.
  7. CONTRATO      lo producido coincide exactamente con lo que el catalogo
                   declara para los atributos que este modulo firma.

Una advertencia sobre DONDE se comprueba cada cosa. El catalogo decide para cada
atributo de equipo que lados llegan a la matriz, y a las dos fuerzas les asigna
solo la diferencia: `strength_attack_diff` viaja a la fila de partido, pero
`home_strength_attack` no. De una resta no se recupera ningun sumando, asi que la
aritmetica por equipo se comprueba sobre `strength_history`, que es la
trayectoria por (fecha, equipo) y el sitio donde esa aritmetica realmente vive.
La proyeccion a fila de partido —que la diferencia sea la resta de las dos
trayectorias del dia, y que el conjunto de columnas sea el declarado— se
comprueba aparte, contra el catalogo, en [4] SIGNOS y [7] CONTRATO.
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
import strength as sg  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str, detalle: object = None) -> None:
    """Registra una comprobacion; `detalle` solo se imprime cuando falla.

    Un fallo de conjunto —«lo producido no es lo declarado»— no se diagnostica
    con un booleano: hace falta ver QUE columna sobra o falta.
    """
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")
    if not cond and detalle is not None:
        print(f"         -> {detalle}")


def _fuerza_por_equipo(m: pd.DataFrame) -> pd.DataFrame:
    """Trayectoria de fuerzas indexada por (fecha, equipo).

    Es la tabla contra la que se comprueba todo lo que el catalogo ya no publica
    por lados. Las dos fuerzas se emiten solo como diferencia, de modo que la
    fila de partido no dice cuanto ataque tiene el local; esta si, y ademas es
    donde el modulo las calcula.
    """
    h = sg.strength_history(m).copy()
    h["date"] = pd.to_datetime(h["date"])
    return h.set_index(["date", "team_key"])


# ---------------------------------------------------------------------------
# generador de ligas sinteticas
# ---------------------------------------------------------------------------
_MU_TRUE = 1.35
_H_TRUE = 1.25


def _liga(n_equipos: int = 20, temporadas: int = 10, semilla: int = 7,
          sigma_att: float = 0.30, sigma_def: float = 0.25) -> tuple:
    """Simula el calendario con el propio modelo de goles y devuelve la verdad.

    Los parametros verdaderos se normalizan a media geometrica 1 antes de
    simular, que es la misma convencion que impone el ajuste: sin ese paso la
    comparacion entre lo estimado y lo verdadero estaria desplazada por una
    constante y no mediria nada.

    Las fechas imitan el reparto real de la competicion --una jornada repartida
    entre viernes y lunes-- porque el reajuste del modulo es diario y una
    simulacion con una sola fecha por jornada no ejercitaria el caso de dos
    partidos que comparten dia sin informarse.
    """
    rng = np.random.default_rng(semilla)
    equipos = [f"eq{i:02d}" for i in range(n_equipos)]
    att = np.exp(rng.normal(0, sigma_att, n_equipos))
    dfn = np.exp(rng.normal(0, sigma_def, n_equipos))
    att /= np.exp(np.mean(np.log(att)))
    dfn /= np.exp(np.mean(np.log(dfn)))

    filas = []
    fecha = pd.Timestamp("2016-08-19")
    for _ in range(temporadas):
        for _ in range(2 * (n_equipos - 1)):
            orden = rng.permutation(n_equipos)
            for k in range(0, n_equipos, 2):
                i, j = int(orden[k]), int(orden[k + 1])
                dia = fecha + pd.Timedelta(days=int(rng.integers(0, 4)))
                filas.append((f"{dia.date()}-{equipos[i]}-{equipos[j]}",
                              str(dia.date()), equipos[i], equipos[j],
                              int(rng.poisson(_MU_TRUE * att[i] * dfn[j] * _H_TRUE)),
                              int(rng.poisson(_MU_TRUE * att[j] * dfn[i]))))
            fecha += pd.Timedelta(days=7)
        fecha += pd.Timedelta(days=70)          # parada entre temporadas
    m = pd.DataFrame(filas, columns=["match_key", "date", "home_key",
                                     "away_key", "fthg", "ftag"])
    return m.drop_duplicates("match_key").reset_index(drop=True), equipos, att, dfn


def _master(filas: list[tuple]) -> pd.DataFrame:
    """Master minimo a mano: (fecha, local, visitante, goles local, goles visit)."""
    return pd.DataFrame({
        "match_key": [f"{f[0]}-{f[1]}-{f[2]}" for f in filas],
        "date": [f[0] for f in filas],
        "home_key": [f[1] for f in filas],
        "away_key": [f[2] for f in filas],
        "fthg": [f[3] for f in filas],
        "ftag": [f[4] for f in filas],
    })


# ---------------------------------------------------------------------------
# 1. recuperacion de parametros conocidos
# ---------------------------------------------------------------------------
def test_recuperacion() -> None:
    print("\n1. RECUPERACION - reencontrar unas fuerzas que se conocen")
    m, equipos, att, dfn = _liga()
    hist = sg.strength_history(m)
    ult = hist[hist["date"] == hist["date"].max()].set_index("team_key")
    idx = [equipos.index(t) for t in ult.index]

    check(len(ult) == len(equipos),
          "al final del historico los veinte clubes tienen fuerza estimada")

    r_att = float(np.corrcoef(ult["strength_attack"], np.log(att[idx]))[0, 1])
    r_def = float(np.corrcoef(ult["strength_defence"], -np.log(dfn[idx]))[0, 1])
    check(r_att > 0.80, f"el ataque estimado sigue al verdadero (r = {r_att:.3f})")
    check(r_def > 0.80, f"la defensa estimada sigue a la verdadera (r = {r_def:.3f})")

    r_cruz = float(np.corrcoef(ult["strength_attack"], -np.log(dfn[idx]))[0, 1])
    check(abs(r_cruz) < abs(r_att),
          "y cada una sigue a la suya: el ataque no esta leyendo la defensa")

    # La ventaja de campo y el nivel medio se comprueban sobre la MEDIANA de sus
    # trayectorias, no sobre su valor en una fecha concreta. La estimacion de una
    # fecha sale de una muestra con memoria exponencial, cuyo tamano efectivo es
    # una fraccion del historico, y fluctua en consecuencia varias centesimas de
    # una fecha a la siguiente; lo que la prueba quiere verificar es que el
    # parametro esta centrado donde se genero, no que cada ajuste diario acierte.
    h = float(hist["home_advantage"].median())
    mu = float(hist["league_mean_goals"].median())
    check(abs(h - _H_TRUE) < 0.08,
          f"la ventaja de campo se reencuentra (h = {h:.3f}, verdadera {_H_TRUE})")
    check(abs(mu - _MU_TRUE) < 0.10,
          f"el nivel medio de la liga tambien (mu = {mu:.3f}, verdadero {_MU_TRUE})")
    check(bool((hist["home_advantage"] > 1).all()),
          "y en ninguna fecha del historico la ventaja de campo se pierde")

    g_att = float(np.exp(np.mean(ult["strength_attack"])))
    g_def = float(np.exp(np.mean(-ult["strength_defence"])))
    check(abs(g_att - 1) < 1e-6 and abs(g_def - 1) < 1e-6,
          "la normalizacion de identificabilidad se cumple: media geometrica 1 "
          "en ataques y en defensas, de modo que el cero es la media de la liga")

    check(float(ult["effective_matches"].min()) >= sg.MIN_EFFECTIVE_MATCHES,
          "solo se publica la fuerza de quien supera la historia minima")


# ---------------------------------------------------------------------------
# 2. corte temporal
# ---------------------------------------------------------------------------
def test_anti_fuga() -> None:
    print("\n2. ANTI-FUGA - lo que un partido puede saber es lo anterior a el")
    m, _, _, _ = _liga(temporadas=3, semilla=11)
    base = sg.build_strength_features(m).set_index("match_key")

    # se altera el marcador de un partido de mitad del historico
    corte = len(m) // 2
    dia = m.loc[corte, "date"]
    alt = m.copy()
    alt.loc[corte, "fthg"] = 9
    alt.loc[corte, "ftag"] = 0
    nuevo = sg.build_strength_features(alt).set_index("match_key")

    # Se barren TODAS las columnas publicadas, no solo las que empiezan por
    # `home_`/`away_`: desde la curacion del catalogo las fuerzas viajan como
    # diferencia y la terna 1X2 sin rotulo de lado, de modo que un barrido por
    # prefijo no miraria ni una sola columna y el test pasaria sin comprobar nada.
    cols = [c for c in base.columns if c != "match_key"]
    mismo_dia = m.loc[m["date"] == dia, "match_key"]
    igual = np.allclose(base.loc[mismo_dia, cols].to_numpy(dtype="float64"),
                        nuevo.loc[mismo_dia, cols].to_numpy(dtype="float64"),
                        equal_nan=True)
    check(igual,
          "cambiar un resultado no altera las features de su propia fecha: ni "
          "las del partido, que no se informa a si mismo, ni las de los demas "
          "partidos del mismo dia, que no se informan entre ellos")
    check(len(mismo_dia) > 1,
          "y el caso probado es real, porque esa fecha tiene mas de un partido")

    posteriores = m.loc[m["date"] > dia, "match_key"]
    distinto = not np.allclose(
        base.loc[posteriores, cols].to_numpy(dtype="float64"),
        nuevo.loc[posteriores, cols].to_numpy(dtype="float64"), equal_nan=True)
    check(distinto,
          "pero si altera las de las fechas siguientes, que es la prueba de que "
          "el corte esta puesto donde toca y no un paso mas atras")

    anteriores = m.loc[m["date"] < dia, "match_key"]
    check(np.allclose(base.loc[anteriores, cols].to_numpy(dtype="float64"),
                      nuevo.loc[anteriores, cols].to_numpy(dtype="float64"),
                      equal_nan=True),
          "y nada de lo anterior a esa fecha se mueve")


# ---------------------------------------------------------------------------
# 3. historia minima
# ---------------------------------------------------------------------------
def test_historia_minima() -> None:
    print("\n3. HISTORIA MINIMA - el hueco se declara, no se rellena")
    m, equipos, _, _ = _liga(temporadas=2, semilla=5)
    ultimo = pd.Timestamp(m["date"].max())

    # un club recien llegado juega su primer partido al final del historico
    nuevo = _master([
        (str((ultimo + pd.Timedelta(days=7)).date()), "ascendido", equipos[0], 1, 1),
        (str((ultimo + pd.Timedelta(days=7)).date()), equipos[1], equipos[2], 2, 0),
    ])
    out = sg.build_strength_features(pd.concat([m, nuevo], ignore_index=True))
    out = out.set_index("match_key")

    debut = out.loc[nuevo["match_key"].iloc[0]]
    check(bool(pd.isna(debut["strength_attack_diff"])),
          "el debutante no recibe fuerza: no hay muestra de la que salga")
    check(bool(pd.isna(debut["strength_p_home"])),
          "el pronostico del partido queda vacio en lugar de apoyarse en la "
          "media de la liga, que seria inventar una prevision creible")

    par = out.loc[nuevo["match_key"].iloc[1]]
    check(not bool(pd.isna(par["strength_p_home"])),
          "el otro partido de esa misma fecha no sufre el contagio")

    todo = pd.concat([m, nuevo], ignore_index=True)
    hist = sg.strength_history(todo)
    check("ascendido" not in set(hist["team_key"]),
          "y la trayectoria tampoco publica una fuerza que no existe")

    # El hueco del debut no es del debutante, sino del PARTIDO: su rival lleva
    # temporadas jugando y su fuerza de ese dia esta perfectamente estimada en la
    # trayectoria. Que aun asi la fila salga vacia es lo que se quiere —una
    # feature de partido describe un enfrentamiento, y media descripcion no lo
    # es—, y solo se puede comprobar mirando fuera de la fila: la diferencia es
    # NaN tanto si falta un lado como si faltan los dos.
    fecha = pd.to_datetime(nuevo["date"].iloc[0])
    porequipo = _fuerza_por_equipo(todo)
    check((fecha, equipos[0]) in porequipo.index
          and not bool(pd.isna(porequipo.loc[(fecha, equipos[0]),
                                             "strength_attack"])),
          "y el rival del debutante SI tiene fuerza estimada ese dia: el hueco "
          "es del enfrentamiento, no suyo")


# ---------------------------------------------------------------------------
# 4. signos, escala y diferencias
# ---------------------------------------------------------------------------
def test_signos() -> None:
    print("\n4. SIGNOS - positivo es mejor en las dos columnas")
    m, equipos, att, dfn = _liga(temporadas=6, semilla=21)
    out = sg.build_strength_features(m).set_index("match_key")
    hist = sg.strength_history(m)
    ult = hist[hist["date"] == hist["date"].max()].set_index("team_key")

    mejor_att = equipos[int(np.argmax(att))]
    peor_att = equipos[int(np.argmin(att))]
    check(ult.loc[mejor_att, "strength_attack"] > 0
          > ult.loc[peor_att, "strength_attack"],
          "el equipo que mas marca tiene `strength_attack` positivo y el que "
          "menos, negativo: el cero es el ataque medio de la liga")

    mejor_def = equipos[int(np.argmin(dfn))]     # concede menos = mejor defensa
    peor_def = equipos[int(np.argmax(dfn))]
    check(ult.loc[mejor_def, "strength_defence"] > 0
          > ult.loc[peor_def, "strength_defence"],
          "y quien menos encaja tiene `strength_defence` positivo, de modo que "
          "las dos columnas se leen en la misma direccion")

    # La diferencia es lo unico que el catalogo publica de estas dos fuerzas, asi
    # que comprobar que «es la resta de sus lados» exige traerse los lados de la
    # trayectoria y emparejarlos por (fecha, equipo). El emparejamiento no es
    # decorativo: es justo donde se cometeria el error que este bloque teme —usar
    # la fuerza de una fecha distinta de la del partido, o la del rival en lugar
    # de la propia— y ninguna comprobacion de rango lo detectaria.
    porequipo = _fuerza_por_equipo(m)
    mm = m.copy()
    mm["date"] = pd.to_datetime(mm["date"])
    sub = out.dropna(subset=["strength_attack_diff"]).join(
        mm.set_index("match_key")[["date", "home_key", "away_key"]])
    for c in ("strength_attack", "strength_defence"):
        h = porequipo.loc[list(zip(sub["date"], sub["home_key"])), c].to_numpy()
        a = porequipo.loc[list(zip(sub["date"], sub["away_key"])), c].to_numpy()
        check(np.allclose(sub[f"{c}_diff"].to_numpy(), h - a),
              f"`{c}_diff` es exactamente la resta de las dos trayectorias del "
              f"dia del partido")

    # Los goles esperados de cada lado ya no se publican —la terna 1X2 es su
    # resumen y entregar ambos seria decir dos veces lo mismo—, de modo que la
    # ventaja de campo se lee donde ahora asoma: en el pronostico.
    p = out.dropna(subset=["strength_p_home"])
    check(p["strength_p_home"].mean() > p["strength_p_away"].mean(),
          "el local gana mas a menudo que el visitante en el pronostico medio, "
          "que es la ventaja de campo llegando hasta la columna publicada")


# ---------------------------------------------------------------------------
# 5. pronostico 1X2
# ---------------------------------------------------------------------------
def test_pronostico() -> None:
    print("\n5. PRONOSTICO - la terna 1X2 como distribucion legitima")
    m, equipos, att, dfn = _liga(temporadas=6, semilla=33)
    out = sg.build_strength_features(m)
    cols = ["strength_p_home", "strength_p_draw", "strength_p_away"]
    p = out.dropna(subset=cols)

    check(float(np.abs(p[cols].sum(axis=1) - 1).max()) < 1e-9,
          "las tres probabilidades suman uno en todos los partidos")
    check(bool((p[cols] >= 0).all().all() and (p[cols] <= 1).all().all()),
          "y ninguna se sale del intervalo [0, 1]")

    # fuerte contra debil, en las dos direcciones, sobre el mismo historico
    fuerte = equipos[int(np.argmax(att / dfn))]
    debil = equipos[int(np.argmin(att / dfn))]
    ultimo = pd.Timestamp(m["date"].max()) + pd.Timedelta(days=7)
    extra = _master([
        (str(ultimo.date()), fuerte, debil, 0, 0),
        (str(ultimo.date()), debil, fuerte, 0, 0),
    ])
    o2 = sg.build_strength_features(
        pd.concat([m, extra], ignore_index=True)).set_index("match_key")
    a = o2.loc[extra["match_key"].iloc[0]]
    b = o2.loc[extra["match_key"].iloc[1]]
    check(a["strength_p_home"] > a["strength_p_away"]
          and b["strength_p_away"] > b["strength_p_home"],
          "el mejor equipo es favorito juegue donde juegue, y el pronostico se "
          "da la vuelta al intercambiar los campos")
    check(a["strength_p_home"] > b["strength_p_away"],
          "aunque no es simetrico: el mismo favorito lo es mas en su campo")

    # dos equipos identicos: solo queda la ventaja de campo
    espejo = pd.DataFrame({"match_key": ["esp1"], "date": [str(ultimo.date())],
                           "home_key": [equipos[0]], "away_key": [equipos[0]],
                           "fthg": [0], "ftag": [0]})
    e = sg.build_strength_features(
        pd.concat([m, espejo], ignore_index=True)).set_index("match_key")
    check(e.loc["esp1", "strength_p_home"] > e.loc["esp1", "strength_p_away"],
          "entre dos equipos de identica fuerza gana probabilidad el local, que "
          "es lo unico que los separa")

    # Los goles esperados de cada lado ya no se publican —la terna 1X2 es su
    # resumen y entregar ambos seria decir dos veces lo mismo—, de modo que el
    # rango en que se movian se vigila ahora en la columna que si sale. La averia
    # que se busca es la de escala: un μ mal puesto produce medias de diez goles
    # o de dos decimas, y en los dos casos el empate se desploma y el favorito se
    # va a la certeza.
    #
    # Se mira la MEDIANA y no el extremo. Esta liga simulada reparte las fuerzas
    # con una dispersion generosa sobre veinte equipos, asi que algun
    # enfrentamiento entre el mejor y el peor llega legitimamente al 95 % y un
    # umbral sobre el maximo estaria midiendo la cola de la simulacion en vez de
    # la escala del modelo. La mediana, en cambio, describe al partido tipico, que
    # es lo que tiene que parecerse al futbol.
    todas = o2.dropna(subset=["strength_p_home"])
    emp = float(todas["strength_p_draw"].median())
    check(0.15 < emp < 0.35,
          f"el empate del partido tipico se mantiene en rango futbolistico "
          f"({emp:.2f}), que es la senal de que los goles esperados que lo "
          f"generan no se han ido de escala")
    fav = float(np.median(
        todas[["strength_p_home", "strength_p_away"]].to_numpy().max(axis=1)))
    check(0.35 < fav < 0.75,
          f"y el favorito tipico se pronostica con conviccion, no con certeza "
          f"({fav:.2f})")


# ---------------------------------------------------------------------------
# 6. partidos por jugar
# ---------------------------------------------------------------------------
def test_futuro() -> None:
    print("\n6. FUTURO - la jornada por venir recibe features y no las contamina")
    m, equipos, _, _ = _liga(temporadas=3, semilla=13)
    base = sg.build_strength_features(m).set_index("match_key")

    ultimo = pd.Timestamp(m["date"].max()) + pd.Timedelta(days=7)
    futuro = pd.DataFrame({
        "match_key": ["fut1"], "date": [str(ultimo.date())],
        "home_key": [equipos[3]], "away_key": [equipos[4]],
        "fthg": [np.nan], "ftag": [np.nan]})
    ext = sg.build_strength_features(
        pd.concat([m, futuro], ignore_index=True)).set_index("match_key")

    cols = [c for c in base.columns if c != "match_key"]
    check(np.allclose(base[cols].to_numpy(dtype="float64"),
                      ext.loc[base.index, cols].to_numpy(dtype="float64"),
                      equal_nan=True),
          "anadir la jornada por venir no cambia ni una cifra de lo ya jugado: "
          "un partido sin marcador no entra en el ajuste")
    check(not bool(pd.isna(ext.loc["fut1", "strength_p_home"])),
          "y ese partido si recibe su pronostico, estimado con lo anterior, que "
          "es lo que hace que la ruta de inferencia no necesite regla aparte")


# ---------------------------------------------------------------------------
# 7. contrato del modulo
# ---------------------------------------------------------------------------
def test_contrato() -> None:
    print("\n7. CONTRATO - lo declarado es exactamente lo producido")
    # El contrato ya no es «cada atributo sale como par y como diferencia». De un
    # atributo de equipo caben tres columnas en la fila de partido y las tres son
    # linealmente dependientes, asi que el catalogo elige cuales salen; a estas
    # dos les da solo la diferencia. Lo que se comprueba es que lo producido y lo
    # declarado sean el MISMO conjunto en las dos direcciones: que no falte nada
    # que el catalogo emita —una feature calculada y tirada a la basura— y que no
    # sobre nada que no emita, porque una columna que llega a la matriz sin estar
    # declarada se salta la curacion entera, que es justo lo que el catalogo
    # existe para impedir.
    nombres = sg.strength_feature_names()
    esperadas = (["match_key"]
                 + [c for b in nombres["numeric"] + nombres["categorical"]
                    for c in catalogo.nombres(b)]
                 + nombres["match_level"])

    m, _, _, _ = _liga(temporadas=2, semilla=3)
    out = sg.build_strength_features(m)

    check(set(out.columns) == set(esperadas),
          "lo producido coincide exactamente con lo que el catalogo declara",
          sorted(set(out.columns) ^ set(esperadas)))
    check(out["match_key"].is_unique and len(out) == len(m),
          "una fila por partido y `match_key` unico")
    check(nombres["categorical"] == [],
          "el bloque no declara categoricas: todo lo que produce es numerico o "
          "una probabilidad de partido")
    check(all(c.startswith("strength_p_") for c in nombres["match_level"]),
          "y lo declarado a nivel de partido son las tres probabilidades, que "
          "no se desdoblan por lado ni se restan entre si")

    faltan = [c for c in sg._NEED if c != "match_key"]
    for c in faltan[:1]:
        try:
            sg.build_strength_features(m.drop(columns=[c]))
            ok = False
        except KeyError:
            ok = True
        check(ok, f"la ausencia de `{c}` en el master se detiene con un error "
                  "explicito en lugar de producir columnas vacias")


def _run() -> None:
    print("=" * 72)
    print("TEST - fuerzas latentes de ataque y defensa")
    print("=" * 72)
    test_recuperacion()
    test_anti_fuga()
    test_historia_minima()
    test_signos()
    test_pronostico()
    test_futuro()
    test_contrato()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_strength() -> None:
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
