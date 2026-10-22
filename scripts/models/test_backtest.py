"""Banco de pruebas de backtest.py: la simulacion economica, sin red ni artefactos.

Todo lo que se comprueba aqui se calcula a mano o se deduce de la aritmetica de
la cuota decimal, de modo que el banco no depende de ningun parquet y puede
ejecutarse en cualquier maquina en segundos.

Secciones
---------
1. CUOTAS            incorporacion, validacion y margen del conjunto elegido
2. VALOR Y KELLY     valor esperado y fraccion optima, con casos de referencia
3. SELECCION         a que se apuesta, y por que a una sola cosa por partido
4. IMPORTE PLANO     libro calculado a mano, apuesta a apuesta, y lo que no se
                     puede liquidar: sin precio o sin acta
5. KELLY             tamano por jornada, tope y dependencia del banco
6. LECTURA           caida maxima, resumen, corte por temporada y trayectoria
7. PROPIEDADES       lo que la aritmetica del mercado obliga a que ocurra
8. SENSIBILIDAD      barrido de umbral e intervalo por remuestreo
9. COMPARACION       el apostante de referencia y la ventaja sobre el mercado
10. GUARDAS          lo que el modulo se niega a hacer
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import backtest as bt  # noqa: E402
import evaluate as ev  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    if bool(cond):
        PASS.append(msg)
        print(f"  [OK ] {msg}")
    else:
        FAIL.append(msg)
        print(f"  [FAIL] {msg}")


def _cerca(a, b, tol=1e-9) -> bool:
    return bool(np.allclose(np.asarray(a, dtype="float64"),
                            np.asarray(b, dtype="float64"),
                            rtol=0, atol=tol, equal_nan=True))


# ---------------------------------------------------------------------------
# Utilidades de construccion
# ---------------------------------------------------------------------------
def _pred(P, y, *, fechas=None, jornadas=None, season="2019/20",
          claves=None) -> pd.DataFrame:
    """Marco de predicciones con el contrato que publica el protocolo."""
    P = np.asarray(P, dtype="float64")
    n = len(P)
    if fechas is None:
        fechas = pd.to_datetime("2019-08-17") + pd.to_timedelta(
            np.repeat(np.arange((n + 4) // 5), 5)[:n] * 7, unit="D")
    if jornadas is None:
        jornadas = np.repeat(np.arange(1, (n + 4) // 5 + 1), 5)[:n]
    if claves is None:
        claves = [f"p{i:04d}" for i in range(n)]
    return pd.DataFrame({
        "match_key": claves, "season": np.repeat(season, n),
        "jornada": jornadas, "date": fechas, "y_num": np.asarray(y, dtype=int),
        "p_home": P[:, 0], "p_draw": P[:, 1], "p_away": P[:, 2],
    })


def _feat(claves, O, conjunto="cierre_avg") -> pd.DataFrame:
    """Marco de variables con solo lo que el estudio economico necesita."""
    O = np.asarray(O, dtype="float64")
    cols = bt.CUOTAS[conjunto]
    return pd.DataFrame({"match_key": list(claves), cols[0]: O[:, 0],
                         cols[1]: O[:, 1], cols[2]: O[:, 2]})


def _mercado(Q, margen=0.05) -> np.ndarray:
    """Cuotas decimales de un mercado con probabilidad `Q` y margen `margen`."""
    Q = np.asarray(Q, dtype="float64")
    return 1.0 / (Q * (1.0 + margen))


def _liga(n=1200, margen=0.06, semilla=5, ventaja=0.0):
    """Liga sintetica: mercado con margen, modelo con la ventaja que se le plante.

    Con `ventaja=0` el modelo publica exactamente la probabilidad verdadera y el
    mercado tambien; entonces no hay nada que ganar y el margen decide. Con
    ventaja positiva el modelo conoce un desplazamiento hacia el empate que el
    precio ignora, que es la unica forma de que una apuesta tenga sentido.
    """
    rng = np.random.default_rng(semilla)
    Qv = rng.dirichlet([6.0, 4.0, 4.0], size=n)          # verdad
    Qm = Qv.copy()
    if ventaja:
        Qm = Qv * np.array([1.0, 1.0 - ventaja, 1.0])    # el precio infravalora
        Qm = Qm / Qm.sum(axis=1, keepdims=True)          # el empate
    y = np.array([rng.choice(3, p=p) for p in Qv])
    jor = np.repeat(np.arange(1, n // 10 + 2), 10)[:n]
    fechas = pd.to_datetime("2019-08-17") + pd.to_timedelta(jor * 7, unit="D")
    pred = _pred(Qv, y, fechas=fechas, jornadas=jor)
    feat = _feat(pred.match_key, _mercado(Qm, margen))
    return bt.attach_odds(pred, feat), Qv, Qm


def _con_casa(pred: pd.DataFrame, Q) -> pd.DataFrame:
    """Le adjunta al marco la terna de Bet365, que es la opinion de la casa.

    Es lo que el preprocesado deja en el artefacto: la probabilidad implicita de
    las cuotas de apertura de la casa, ya sin margen y sumando uno. El precio al
    que se cobra es otra cosa y viaja en las columnas de cuota.
    """
    out = pred.copy()
    Q = np.asarray(Q, dtype="float64")
    for i, col in enumerate(ev.REFERENCIAS["b365"]):
        out[col] = Q[:, i]
    out.attrs = dict(pred.attrs)
    return out


# ===========================================================================
# 1. Cuotas
# ===========================================================================
def test_cuotas() -> None:
    print("\n1. CUOTAS")

    P = np.array([[.5, .3, .2], [.4, .3, .3], [.2, .3, .5]])
    y = [0, 1, 2]
    O = np.array([[2.0, 3.4, 4.5], [2.5, 3.2, 3.0], [5.0, 3.5, 1.8]])
    pred = _pred(P, y)
    feat = _feat(pred.match_key, O)

    p2 = bt.attach_odds(pred, feat)
    check(len(p2) == len(pred), "incorporar las cuotas no anade ni pierde partidos")
    check(list(p2.match_key) == list(pred.match_key),
          "y conserva el orden, porque la union es por clave unica")
    check(_cerca(p2[bt.CUOTA_COLS].to_numpy(), O),
          "las tres cuotas llegan a sus columnas, en el orden local-empate-visitante")
    check(p2.attrs["cuotas"] == "cierre_avg",
          "el marco declara contra que precio se va a medir")

    # Un partido que el conjunto de cuotas no cubre no es una apuesta perdida.
    feat_hueco = feat.copy()
    feat_hueco.loc[1, list(bt.CUOTAS["cierre_avg"])] = np.nan
    p3 = bt.attach_odds(pred, feat_hueco)
    check(bool(p3[bt.CUOTA_COLS].iloc[1].isna().all()),
          "un partido sin precio queda con hueco, no con un valor inventado")
    check(len(p3) == 3, "y sigue en el marco, para poder contar la cobertura")

    # Una cuota decimal menor o igual que 1 no paga ni la puesta: es dato corrupto.
    feat_mala = feat.copy()
    feat_mala.loc[0, bt.CUOTAS["cierre_avg"][0]] = 0.85
    p4 = bt.attach_odds(pred, feat_mala)
    check(bool(np.isnan(p4.cuota_home.iloc[0])),
          "una cuota que no llega a 1 se trata como ausencia, no como ganga")

    # Margen: con cuotas construidas al 5 % el margen medido es exactamente 5 %.
    Q = np.array([[.5, .3, .2], [.45, .27, .28], [.2, .25, .55]])
    pm = bt.attach_odds(_pred(Q, [0, 1, 2]),
                        _feat([f"p{i:04d}" for i in range(3)], _mercado(Q, 0.05)))
    check(_cerca(bt.overround(pm), 0.05, tol=1e-12),
          "el margen medido es el que se planto en las cuotas")
    check(bt.overround(pm) > 0,
          "y es positivo: la casa cobra por hacer de contrapartida")


# ===========================================================================
# 2. Valor esperado y Kelly
# ===========================================================================
def test_valor_kelly() -> None:
    print("\n2. VALOR Y KELLY")

    P = np.array([[0.5, 0.3, 0.2]])
    O = np.array([[2.5, 3.0, 4.0]])
    VE = bt.expected_value(P, O)
    check(_cerca(VE, [[0.25, -0.10, -0.20]]),
          "el valor esperado es p por cuota menos uno, calculado a mano")

    # Apuesta justa: cuota igual al inverso de la probabilidad.
    check(_cerca(bt.expected_value(np.array([[0.4]]), np.array([[2.5]])), 0.0),
          "una apuesta justa tiene valor esperado exactamente cero")

    # Kelly: (p·c − 1)/(c − 1). Con p=0.5 y c=2.5 la fraccion es 1/6.
    f = bt.kelly_fraction(np.array([0.5]), np.array([2.5]))
    check(_cerca(f, 1.0 / 6.0), "la fraccion de Kelly sale de la formula cerrada")
    check(_cerca(bt.kelly_fraction(np.array([0.4]), np.array([2.5])), 0.0),
          "en una apuesta justa Kelly no arriesga nada")
    check(_cerca(bt.kelly_fraction(np.array([0.2]), np.array([2.5])), 0.0),
          "y con desventaja tampoco: no se puede vender un resultado")
    check(_cerca(bt.kelly_fraction(np.array([1.0]), np.array([2.0])), 1.0),
          "con certeza absoluta Kelly arriesga el banco entero, que es su limite")

    # Monotonia: mas ventaja, mas fraccion.
    f1 = bt.kelly_fraction(np.array([0.45]), np.array([2.5]))
    f2 = bt.kelly_fraction(np.array([0.55]), np.array([2.5]))
    check(float(f2) > float(f1), "mas probabilidad a la misma cuota, mas importe")

    # Con el mismo valor esperado, la cuota alta recibe menos importe: es la
    # traduccion cuantitativa de que la apuesta larga es mas volatil.
    fa = bt.kelly_fraction(np.array([0.55]), np.array([2.0]))      # VE = 0.10
    fb = bt.kelly_fraction(np.array([0.22]), np.array([5.0]))      # VE = 0.10
    check(float(fa) > float(fb),
          "a igual ventaja, la cuota alta recibe menos importe por ser mas volatil")


# ===========================================================================
# 3. Seleccion
# ===========================================================================
def test_seleccion() -> None:
    print("\n3. SELECCION")

    # Partido construido para que favorito y valor discrepen: el local es el mas
    # probable, pero el visitante es el que esta mal pagado.
    P = np.array([[0.50, 0.28, 0.22]])
    O = np.array([[1.90, 3.20, 5.50]])
    pred = bt.attach_odds(_pred(P, [0]), _feat(["p0000"], O))

    fav = bt.select_bets(pred, politica="favorito")
    val = bt.select_bets(pred, politica="valor", umbral=0.0)
    check(fav.seleccion.iloc[0] == "H", "el favorito es la clase mas probable")
    check(val.seleccion.iloc[0] == "A",
          "y el valor es otra cosa: la clase peor pagada por el mercado")
    check(_cerca(val.ve_sel.iloc[0], 0.22 * 5.5 - 1.0),
          "el valor esperado publicado es el de la seleccion, no el maximo de otra")

    # Una sola apuesta por partido, siempre.
    P2 = np.array([[0.40, 0.35, 0.25], [0.30, 0.30, 0.40]])
    O2 = np.array([[3.0, 3.2, 4.5], [4.0, 3.5, 2.8]])
    pred2 = bt.attach_odds(_pred(P2, [0, 2]), _feat(["p0000", "p0001"], O2))
    s2 = bt.select_bets(pred2, umbral=0.0)
    check(len(s2) == len(pred2) and s2.seleccion.notna().all(),
          "hay como mucho una seleccion por partido, nunca dos")

    # El umbral filtra, y solo actua sobre la politica de valor.
    alto = bt.select_bets(pred2, politica="valor", umbral=0.50)
    check(int(alto.apuesta.sum()) < int(s2.apuesta.sum()),
          "subir el umbral deja fuera apuestas, que es para lo que existe")
    fav2 = bt.select_bets(pred2, politica="favorito", umbral=0.50)
    check(bool(fav2.apuesta.all()),
          "al favorito no se le exige ventaja: seria otra politica de valor")

    # Sin cuota no hay seleccion posible.
    sin = pred2.copy()
    sin.loc[0, bt.CUOTA_COLS] = np.nan
    s3 = bt.select_bets(sin, umbral=0.0)
    check(s3.seleccion.iloc[0] is None and not bool(s3.apuesta.iloc[0]),
          "un partido sin precio no genera seleccion")
    check(bool(s3.apuesta.iloc[1]), "y no contamina al resto de la jornada")


# ===========================================================================
# 4. Importe plano
# ===========================================================================
def test_plana() -> None:
    print("\n4. IMPORTE PLANO")

    # Cuatro partidos, dos aciertos, todo calculado a mano.
    P = np.array([[0.60, 0.25, 0.15], [0.20, 0.30, 0.50],
                  [0.55, 0.25, 0.20], [0.25, 0.35, 0.40]])
    O = np.array([[2.00, 4.00, 6.00], [5.00, 3.50, 2.20],
                  [2.10, 3.80, 5.00], [4.00, 3.00, 2.60]])
    y = [0, 2, 1, 0]                       # acierta el 1 y el 2, falla el 3 y el 4
    pred = bt.attach_odds(_pred(P, y), _feat([f"p{i:04d}" for i in range(4)], O))

    lib = bt.simulate(pred, politica="favorito", importe="plana",
                      unidad=1.0, banca=100.0)
    check(len(lib) == 4, "con importe plano se apuesta en todos los partidos con precio")
    check(_cerca(lib.importe.to_numpy(), 1.0),
          "y cada apuesta arriesga exactamente una unidad")

    # Ganancias: +1.00, +1.20, −1, −1  →  beneficio 0.20
    check(_cerca(lib.ganancia.to_numpy(), [1.00, 1.20, -1.0, -1.0]),
          "acertar paga la cuota menos uno; fallar pierde la puesta")
    check(_cerca(lib.banca.to_numpy(), [101.0, 102.2, 101.2, 100.2]),
          "el banco es el acumulado, apuesta a apuesta")

    s = bt.summary(lib)
    check(_cerca(s["beneficio"], 0.20), "el beneficio total es la suma de ganancias")
    check(_cerca(s["rendimiento"], 0.20 / 4.0),
          "el rendimiento es beneficio sobre importe arriesgado")
    check(_cerca(s["retorno"], 0.20 / 100.0),
          "y el retorno es lo que le paso al banco, que es otra pregunta")
    check(_cerca(s["acierto"], 0.5), "la tasa de acierto cuenta apuestas, no partidos")

    # El importe plano no depende del orden: es lo que lo hace comparable.
    revuelto = pred.iloc[::-1].reset_index(drop=True)
    revuelto.attrs = dict(pred.attrs)
    s_rev = bt.summary(bt.simulate(revuelto, politica="favorito", importe="plana"))
    check(_cerca(s_rev["beneficio"], s["beneficio"]),
          "reordenar los partidos no cambia el beneficio con importe plano")

    # Cobertura: los partidos sin precio no son apuestas perdidas.
    hueco = pred.copy()
    hueco.loc[2, bt.CUOTA_COLS] = np.nan
    s_h = bt.summary(bt.simulate(hueco, politica="favorito", importe="plana"))
    check(s_h["n_apuestas"] == 3 and s_h["n_partidos"] == 4,
          "el partido sin precio se descuenta de la cobertura, no del beneficio")
    check(_cerca(s_h["cobertura"], 0.75), "y la cobertura lo dice con un numero")

    # La otra forma de no poder liquidar una apuesta: hay precio, pero no hay
    # acta. El filtro por bloque se hace por temporada y no por objetivo, de modo
    # que un encuentro anulado o aplazado llega hasta la simulacion. Contarlo como
    # apuesta perdida falsearia el rendimiento a la baja —seria inventar una
    # derrota— y detener la simulacion entera seria peor: se omite igual que el
    # partido sin precio, y la cobertura publicada lo refleja.
    sin_acta = pred.copy()
    sin_acta["y_num"] = sin_acta["y_num"].astype("float64")
    sin_acta.loc[3, "y_num"] = np.nan          # el cuarto, que era una derrota
    sin_acta.attrs = dict(pred.attrs)
    lib_sa = bt.simulate(sin_acta, politica="favorito", importe="plana",
                         unidad=1.0, banca=100.0)
    check(len(lib_sa) == 3, "un partido sin resultado conocido no se liquida")
    check(_cerca(lib_sa.ganancia.to_numpy(), [1.00, 1.20, -1.0]),
          "y las tres apuestas que si tienen acta se liquidan como antes")
    s_sa = bt.summary(lib_sa)
    check(_cerca(s_sa["beneficio"], 1.20),
          "el partido sin acta no entra como derrota: no se inventa una perdida")
    check(s_sa["n_apuestas"] == 3 and _cerca(s_sa["cobertura"], 0.75),
          "se descuenta de la cobertura, igual que el partido sin precio")
    check(not lib_sa["y_num"].isna().any(),
          "en el libro no queda ninguna fila con el resultado en blanco")

    # Y el caso extremo: ninguna apuesta liquidable. Lo que no puede pasar es que
    # la simulacion lance; un bloque sin actas es un bloque sin cifras, no un error.
    ninguna = pred.copy()
    ninguna["y_num"] = np.nan
    ninguna.attrs = dict(pred.attrs)
    lib_0 = bt.simulate(ninguna, politica="favorito", importe="plana")
    check(lib_0.empty, "un bloque entero sin actas devuelve un libro vacio, no un fallo")
    check(bt.summary(lib_0)["n_apuestas"] == 0,
          "y su resumen se lee sin tropezar con la division por cero")


# ===========================================================================
# 5. Kelly
# ===========================================================================
def test_kelly() -> None:
    print("\n5. KELLY")

    # Dos partidos en la MISMA jornada y uno en la siguiente. Los dos primeros
    # deben dimensionarse con el mismo banco aunque el primero se gane.
    P = np.array([[0.60, 0.25, 0.15], [0.60, 0.25, 0.15], [0.60, 0.25, 0.15]])
    O = np.array([[2.00, 4.00, 6.00]] * 3)
    y = [0, 0, 0]
    fechas = pd.to_datetime(["2019-08-17", "2019-08-18", "2019-08-24"])
    pred = bt.attach_odds(
        _pred(P, y, fechas=fechas, jornadas=[1, 1, 2]),
        _feat([f"p{i:04d}" for i in range(3)], O))

    lib = bt.simulate(pred, politica="favorito", importe="kelly",
                      fraccion=1.0, tope=1.0, banca=100.0)
    # Kelly = (0.6·2 − 1)/(2 − 1) = 0.20 del banco.
    check(_cerca(lib.importe.iloc[0], 20.0), "el importe es la fraccion de Kelly del banco")
    check(_cerca(lib.importe.iloc[1], 20.0),
          "y el segundo partido de la MISMA jornada usa el banco de su inicio")
    check(_cerca(lib.banca.iloc[1], 140.0), "aunque el banco ya haya crecido")
    check(_cerca(lib.importe.iloc[2], 28.0),
          "la jornada siguiente si parte del banco actualizado")

    # Fraccion y tope.
    cuarto = bt.simulate(pred, politica="favorito", importe="kelly",
                         fraccion=0.25, tope=1.0, banca=100.0)
    check(_cerca(cuarto.importe.iloc[0], 5.0),
          "el cuarto de Kelly arriesga la cuarta parte de lo que dice el criterio")
    topado = bt.simulate(pred, politica="favorito", importe="kelly",
                         fraccion=1.0, tope=0.02, banca=100.0)
    check(_cerca(topado.importe.iloc[0], 2.0),
          "y el tope corta la apuesta por muy buena que parezca")

    # Una seleccion sin ventaja recibe importe cero y, por tanto, no se apuesta.
    P0 = np.array([[0.50, 0.30, 0.20]])
    O0 = np.array([[1.80, 3.00, 4.00]])          # VE del favorito = −0.10
    p0 = bt.attach_odds(_pred(P0, [0]), _feat(["p0000"], O0))
    lib0 = bt.simulate(p0, politica="favorito", importe="kelly")
    check(lib0.empty,
          "Kelly no arriesga en una apuesta de valor negativo, aunque sea la favorita")
    check(bt.summary(lib0)["n_apuestas"] == 0,
          "y el resumen de un libro vacio se publica sin romperse")

    # Crecimiento multiplicativo: con aciertos encadenados el banco compone.
    check(_cerca(lib.banca.iloc[2], 140.0 + 28.0),
          "con Kelly el banco compone, que es exactamente su proposito")

    # --- una jornada aplazada no es dos jornadas ---------------------------
    # El segundo partido de la jornada 1 se juega DESPUES del de la jornada 2,
    # que es lo que ocurre con un aplazamiento. La unidad de liquidacion sigue
    # siendo la jornada, asi que los dos partidos de la 1 se dimensionan con el
    # mismo banco por mucho que el calendario los separe. Ordenar por fecha y
    # cortar donde cambia la etiqueta partiria la jornada 1 en dos y dejaria que
    # el aplazado se financiase con lo ganado entre medias.
    aplazado = bt.attach_odds(
        _pred(P, y, fechas=pd.to_datetime(["2019-08-17", "2019-09-25",
                                           "2019-08-24"]),
              jornadas=[1, 1, 2]),
        _feat([f"p{i:04d}" for i in range(3)], O))
    liq = bt.simulate(aplazado, politica="favorito", importe="kelly",
                      fraccion=1.0, tope=1.0, banca=100.0)
    check(list(liq.jornada) == [1, 1, 2],
          "el libro liquida la jornada entera junta, aunque un partido se aplace")
    check(_cerca(liq.importe.to_numpy()[:2], [20.0, 20.0]),
          "y el aplazado se dimensiona con el banco del inicio de SU jornada")
    check(_cerca(liq.importe.iloc[2], 28.0),
          "la jornada siguiente parte del banco con las dos ya liquidadas")

    # Con importe plano el aplazamiento no puede cambiar nada: el orden de los
    # partidos no altera la suma. Es la comprobacion que justifica que la cifra
    # publicada sea la plana y no la de Kelly.
    pl_a = bt.simulate(aplazado, politica="favorito", importe="plana")
    pl_b = bt.simulate(pred, politica="favorito", importe="plana")
    check(_cerca(pl_a.ganancia.sum(), pl_b.ganancia.sum()),
          "con importe plano el calendario no mueve el beneficio")


# ===========================================================================
# 6. Lectura del resultado
# ===========================================================================
def test_lectura() -> None:
    print("\n6. LECTURA")

    # Caida maxima sobre una trayectoria conocida: 100 → 120 → 90 → 110.
    dd = bt.max_drawdown(np.array([120.0, 90.0, 110.0]), inicial=100.0)
    check(_cerca(dd["caida_max"], 30.0),
          "la caida maxima se mide desde el pico, no desde el inicio")
    check(_cerca(dd["caida_max_rel"], 30.0 / 120.0),
          "y en proporcion se mide contra ese mismo pico")
    check(_cerca(bt.max_drawdown(np.array([110.0, 120.0]), 100.0)["caida_max"], 0.0),
          "una trayectoria que solo sube no tiene caida")
    check(_cerca(bt.max_drawdown(np.array([80.0]), 100.0)["caida_max"], 20.0),
          "perder de salida cuenta como caida desde el banco inicial")

    pred, _, _ = _liga(n=400, margen=0.05, ventaja=0.10)
    lib = bt.simulate(pred, politica="valor", umbral=0.0, importe="plana")
    s = bt.summary(lib)
    check(s["caida_max_rel"] >= 0.0, "la caida publicada nunca es negativa")
    check(_cerca(s["banca_final"] - bt.BANCA_INICIAL, s["beneficio"], tol=1e-9),
          "el banco final y el beneficio cuentan lo mismo de dos maneras")

    tab = bt.by_season(lib)
    check(len(tab) == 1, "el corte por temporada tiene una fila por temporada")
    check(_cerca(float(tab.beneficio.sum()), s["beneficio"]),
          "y sus beneficios suman el total, sin partidos perdidos por el camino")

    curva = bt.equity_curve(lib)
    check(len(curva) == len(lib), "la trayectoria tiene un punto por apuesta")
    check(bool((np.diff(curva.pico.to_numpy()) >= -1e-12).all()),
          "el maximo alcanzado no puede bajar, por definicion")
    check(bool((curva.caida.to_numpy() >= -1e-12).all()),
          "y la caida viva nunca es negativa")


# ===========================================================================
# 7. Propiedades que impone la aritmetica del mercado
# ===========================================================================
def test_propiedades() -> None:
    print("\n7. PROPIEDADES")

    # Un modelo que reproduce el precio no encuentra ninguna ventaja: con margen
    # m, el valor esperado de CUALQUIER clase es exactamente −m/(1+m).
    Q = np.array([[.5, .3, .2], [.45, .27, .28], [.2, .25, .55]])
    m = 0.06
    espejo = bt.attach_odds(_pred(Q, [0, 1, 2]),
                            _feat([f"p{i:04d}" for i in range(3)], _mercado(Q, m)))
    VE = bt.expected_value(ev._terna(espejo, ev.PROB_COLS),
                           espejo[bt.CUOTA_COLS].to_numpy())
    check(_cerca(VE, -m / (1 + m), tol=1e-12),
          "apostar al propio precio da el mismo valor esperado en las tres clases")
    check(_cerca(VE.max(), -m / (1 + m), tol=1e-12),
          "y ese valor es exactamente el margen, con signo negativo")
    vacio = bt.simulate(espejo, politica="valor", umbral=0.0)
    check(vacio.empty,
          "por eso la politica de valor se planta y no apuesta a nada")

    # Y si se le obliga a apostar, pierde el margen: es el liston del bloque.
    pred, Qv, _ = _liga(n=3000, margen=0.05, ventaja=0.0, semilla=8)
    forzado = bt.simulate(pred, politica="favorito", importe="plana")
    s = bt.summary(forzado)
    esperado = -0.05 / 1.05
    check(abs(s["rendimiento"] - esperado) < 0.03,
          "apostar contra un precio que ya lo sabe todo pierde el margen")
    check(s["rendimiento"] < 0,
          "y el signo no admite discusion: sin ventaja no hay negocio")

    # Con ventaja real el rendimiento se vuelve positivo. El empate esta
    # infravalorado por el precio y el modelo lo sabe.
    con, _, _ = _liga(n=3000, margen=0.05, ventaja=0.20, semilla=8)
    s2 = bt.summary(bt.simulate(con, politica="valor", umbral=0.0, importe="plana"))
    check(s2["rendimiento"] > 0,
          "con una ventaja real sobre el precio el rendimiento se vuelve positivo")
    check(s2["n_apuestas"] > 0, "y la politica encuentra en que apostar")

    # La politica de valor apuesta a cuotas mas largas que la de favorito: busca
    # precios equivocados, no resultados probables.
    s_fav = bt.summary(bt.simulate(con, politica="favorito", importe="plana"))
    check(s2["cuota_media"] > s_fav["cuota_media"],
          "la politica de valor apuesta mas largo que la de favorito")
    check(s2["acierto"] < s_fav["acierto"],
          "acierta menos veces, que es el precio de buscar valor y no certeza")


# ===========================================================================
# 8. Sensibilidad e incertidumbre
# ===========================================================================
def test_sensibilidad() -> None:
    print("\n8. SENSIBILIDAD")

    pred, _, Qm = _liga(n=1500, margen=0.05, ventaja=0.20, semilla=12)
    barrido = bt.sweep_umbral(pred, umbrales=(0.0, 0.02, 0.05, 0.10))
    check(len(barrido) == 4, "el barrido devuelve una fila por umbral evaluado")
    n = barrido.n_apuestas.to_numpy()
    check(bool((np.diff(n) <= 0).all()),
          "exigir mas ventaja no puede aumentar el numero de apuestas")
    check(bool((barrido.cuota_media.to_numpy()[-1]
                >= barrido.cuota_media.to_numpy()[0] - 1e-9)),
          "y las que sobreviven al filtro son, si acaso, mas largas")

    inter = bt.bootstrap_rendimiento(pred, n_boot=120, seed=3,
                                     politica="valor", umbral=0.0,
                                     importe="plana")
    check(inter["inf"] <= inter["estimacion"] <= inter["sup"],
          "el intervalo del rendimiento contiene su estimacion")
    check(inter["sup"] > inter["inf"],
          "y tiene anchura: el retorno es la cifra mas ruidosa del trabajo")
    check(inter["n_replicas"] > 0, "el remuestreo produce replicas evaluables")

    # El remuestreo es por jornada: el modulo lo hereda de evaluate y no lo
    # reimplementa, de modo que la unidad de dependencia es la misma en todo el
    # trabajo.
    bloques = ev._bloques(pred)
    check(len(pd.unique(bloques)) == pred.jornada.nunique(),
          "los bloques del remuestreo son jornadas, no partidos sueltos")

    # El informe incluye la comparacion con el apostante de referencia, y esa
    # comparacion no es opcional, de modo que el marco tiene que traer la terna de
    # la casa igual que la trae el artefacto real. Aqui la casa opina como el
    # precio; que pueda opinar otra cosa se comprueba en la seccion 9.
    informe = bt.backtest_report(_con_casa(pred, Qm), n_boot=60, verbose=False)
    check(set(informe["tabla"].politica) == {"favorito", "valor"},
          "el informe recorre las dos politicas")
    check(set(informe["tabla"].importe) == {"plana", "kelly"},
          "y los dos modos de importe")
    check(informe["intervalo"] is not None,
          "y acompana la combinacion principal de su intervalo")
    check(_cerca(informe["overround"], bt.overround(pred)),
          "el informe publica el margen contra el que se ha jugado")


# ===========================================================================
# 9. Comparacion con el apostante de referencia
# ===========================================================================
def test_comparacion() -> None:
    print("\n9. COMPARACION")

    # La casa opina distinto del consenso al que se paga: solo asi se puede
    # comprobar que la referencia sigue a la casa y no al precio del libro.
    pred, Qv, Qm = _liga(n=300, margen=0.05, ventaja=0.20, semilla=17)
    rng = np.random.default_rng(4)
    Qb = Qm + rng.dirichlet([20.0, 20.0, 20.0], size=len(Qm)) * 0.25
    Qb = Qb / Qb.sum(axis=1, keepdims=True)
    p = _con_casa(pred, Qb)

    ref = bt.referencia_mercado(p)
    check(_cerca(ref[ev.PROB_COLS].to_numpy(), Qb),
          "la referencia sustituye la terna del modelo por la de la casa")
    check(_cerca(ref[bt.CUOTA_COLS].to_numpy(), p[bt.CUOTA_COLS].to_numpy()),
          "y no toca el precio: las dos estrategias cobran lo mismo")

    lib_r = bt.simula_referencia(p)
    elegida = np.array([list(ev.CLASES).index(s) for s in lib_r.seleccion])
    orden = {k: i for i, k in enumerate(p.match_key)}
    filas = np.array([orden[k] for k in lib_r.match_key])
    check(bool((elegida == Qb[filas].argmax(axis=1)).all()),
          "apuesta siempre al favorito de la casa, que es su cuota mas baja")
    check(len(lib_r) == int(p[bt.CUOTA_COLS].notna().all(axis=1).sum()),
          "y apuesta en todos los partidos con precio, sin umbral que la filtre")
    check(lib_r.attrs.get("referencia") == "b365",
          "el libro declara de quien es la opinion que ha seguido")

    # El corte por jornada y su acumulado.
    jor = bt.by_matchday(lib_r)
    check(len(jor) == lib_r.jornada.nunique(),
          "hay una fila por jornada con apuestas")
    check(_cerca(float(jor.beneficio.sum()), float(lib_r.ganancia.sum())),
          "no se pierde ni se duplica ninguna apuesta en el corte por jornada")
    check(_cerca(float(jor.rendimiento_acum.iloc[-1]), bt.rendimiento(lib_r)),
          "y el acumulado cierra exactamente en la cifra global")
    check(bool(jor.fecha.is_monotonic_increasing),
          "el orden es el temporal, no el alfabetico de la etiqueta")

    cmp_ = bt.compara_con_mercado(p, n_boot=60, seed=3)
    check(_cerca(cmp_["diferencia"],
                 cmp_["modelo"]["rendimiento"] - cmp_["referencia"]["rendimiento"]),
          "la diferencia publicada es la resta de los dos rendimientos")
    check(cmp_["intervalo"]["inf"] <= cmp_["diferencia"] <= cmp_["intervalo"]["sup"],
          "el intervalo pareado contiene la diferencia observada")
    temp = cmp_["por_temporada"]
    check(_cerca(temp["rendimiento_modelo"] - temp["rendimiento_ref"],
                 temp["diferencia"]),
          "la tabla por temporada resta esos mismos dos rendimientos")
    serie = cmp_["por_jornada"]
    check(_cerca(float(serie.rendimiento_acum_ref.iloc[-1]),
                 cmp_["referencia"]["rendimiento"]),
          "la serie por jornada de la referencia cierra en su cifra global")
    check(_cerca(float(serie.diferencia.iloc[-1]), cmp_["diferencia"]),
          "y la distancia final entre las dos curvas es la diferencia publicada")

    # Un modelo que opina exactamente como la casa no puede ganarle ni perder
    # con ella: es la prueba de que el remuestreo esta pareado y de que las dos
    # estrategias ven los mismos partidos en cada replica.
    igual = p.copy()
    igual[ev.PROB_COLS] = Qb
    igual.attrs = dict(p.attrs)
    espejo = bt.compara_con_mercado(igual, politica="favorito", n_boot=40, seed=3,
                                    intervalos_temporada=False)
    check(abs(espejo["diferencia"]) < 1e-12,
          "quien opina como la casa empata con ella, apuesta a apuesta")
    check(abs(espejo["intervalo"]["inf"]) < 1e-12
          and abs(espejo["intervalo"]["sup"]) < 1e-12,
          "y su intervalo pareado es exactamente cero, no un ruido alrededor")

    informe = bt.backtest_report(p, n_boot=40, verbose=False)
    check("comparacion" in informe and "por_jornada" in informe,
          "el informe no publica el rendimiento sin su liston")
    check(informe["casa"] == bt.REFERENCIA
          and "rendimiento" in informe["referencia"],
          "y declara contra quien se ha medido, con su resumen al lado")


# ===========================================================================
# 10. Guardas
# ===========================================================================
def test_guardas() -> None:
    print("\n10. GUARDAS")

    P = np.array([[0.5, 0.3, 0.2]])
    pred = _pred(P, [0])
    feat = _feat(["p0000"], np.array([[2.0, 3.5, 5.0]]))

    try:
        bt.attach_odds(pred, feat, cuotas="inventado")
        check(False, "un conjunto de cuotas desconocido se rechaza")
    except ValueError as e:
        check("desconocido" in str(e), "un conjunto de cuotas desconocido se rechaza")

    try:
        bt.attach_odds(pred, feat.drop(columns=[bt.CUOTAS["cierre_avg"][0]]))
        check(False, "si el marco no trae las cuotas pedidas, se dice")
    except ValueError as e:
        check("no trae" in str(e), "si el marco no trae las cuotas pedidas, se dice")

    try:
        bt.simulate(pred)
        check(False, "simular sin haber incorporado cuotas se detiene")
    except ValueError as e:
        check("attach_odds" in str(e), "simular sin haber incorporado cuotas se detiene")

    p2 = bt.attach_odds(pred, feat)
    try:
        bt.simulate(p2, importe="martingala")
        check(False, "un modo de importe no contemplado se rechaza")
    except ValueError as e:
        check("importe" in str(e), "un modo de importe no contemplado se rechaza")

    try:
        bt.simulate(p2, importe="kelly", fraccion=1.5)
        check(False, "una fraccion de Kelly fuera de (0, 1] se rechaza")
    except ValueError as e:
        check("Kelly" in str(e), "una fraccion de Kelly fuera de (0, 1] se rechaza")

    try:
        bt.select_bets(p2, politica="corazonada")
        check(False, "una politica de seleccion inexistente se rechaza")
    except ValueError as e:
        check("politica" in str(e) or "pol" in str(e).lower(),
              "una politica de seleccion inexistente se rechaza")

    try:
        bt.simulate(p2.drop(columns=["date"]))
        check(False, "sin fecha no hay simulacion posible")
    except ValueError as e:
        check("fecha" in str(e), "sin fecha no hay simulacion posible")

    try:
        bt.expected_value(np.zeros((3, 3)), np.zeros((4, 3)))
        check(False, "probabilidades y cuotas descuadradas se rechazan")
    except ValueError as e:
        check("misma forma" in str(e),
              "probabilidades y cuotas descuadradas se rechazan")

    sin_clave = pred.drop(columns=["match_key"])
    try:
        bt.attach_odds(sin_clave, feat)
        check(False, "sin clave de partido no se puede unir el precio")
    except ValueError as e:
        check("match_key" in str(e), "sin clave de partido no se puede unir el precio")


# ===========================================================================
def _run() -> None:
    print("=" * 74)
    print("BANCO DE PRUEBAS — backtest.py")
    print("=" * 74)
    test_cuotas()
    test_valor_kelly()
    test_seleccion()
    test_plana()
    test_kelly()
    test_lectura()
    test_propiedades()
    test_sensibilidad()
    test_comparacion()
    test_guardas()
    print("\n" + "=" * 74)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for m in FAIL:
        print(f"   - {m}")
    print("=" * 74)


def test_backtest() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
