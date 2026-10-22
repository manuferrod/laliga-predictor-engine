"""Banco de pruebas de publish_web.py (proyeccion de lo evaluado al contrato web).

La capa de publicacion es la unica que el lector ve, y su modo de fallo tipico no
es la excepcion sino el numero plausible: una confianza que no case con sus
probabilidades, un radar que se dibuja contra el minimo observado y cambia de
forma cada semana, una seleccion de apuesta heredada del argumento maximo de una
fila vacia. Ninguno de esos defectos rompe nada; todos mienten. Por eso este
banco no comprueba que las funciones devuelvan algo, sino que devuelvan
EXACTAMENTE lo que se puede calcular a mano, y construye a proposito la fila
degenerada que cada una debe tratar aparte.

Todo es sintetico y offline: ni red, ni artefactos, ni parquets del proyecto. El
modelo de las pruebas de emision es un estimador de juguete cuya probabilidad es
una funcion cerrada de dos columnas, de modo que el reparto de Shapley y la
paridad de esquema se verifican contra valores conocidos y no contra la salida de
un estimador real, que solo permitiria comprobar que no ha cambiado.

  1. Magnitudes derivadas: clase, confianza, entropia normalizada por log 3 y
     margen entre las dos clases mas probables; terna uniforme, terna concentrada
     y matriz con numero de columnas equivocado.
  2. Normalizacion de ejes: recorte al rango DECLARADO por arriba y por abajo, y
     eje invertido, donde el minimo del rango tiene que puntuar uno.
  3. Declaracion de ejes: un eje por eje, rangos no degenerados, identificadores
     unicos y una sola version en el artefacto.
  4. Cobertura por partido: un bloque incompleto rebaja la cobertura del partido
     afectado y solo la de ese.
  5. Radar: filas = 2 x ejes x partidos, normalizado en [0,1], el eje invertido
     ordena al reves y la version viaja en cada fila.
  6. Proxima jornada: valor esperado igual a p*c-1, Kelly bajo el tope, seleccion
     por VALOR distinta de la clase predicha, y partido sin precio sin seleccion.
  7. Perfiles de equipo: dos filas por partido y traduccion H/D/A a V/E/P segun
     el lado, incluido el partido sin resultado.
  8. Diagnosticos: informe por clase y ROC, por temporada y en total.
  9. Ficha del modelo: metricas del contendiente 'modelo', referencias del mismo
     modelo, rendimiento acompanado del favorito de Bet365, limitaciones del
     manifiesto y publicacion sin jornada pendiente.
 10. Emision: la paridad de esquema se comprueba ANTES de puntuar —una permuta
     de columnas aborta la emision— y las explicaciones de la jornada suman
     exactamente la distancia al partido de referencia.
 11. Configuracion de emision: se emite el conjunto declarado en build_models,
     el sello se comprueba antes que el manifiesto, el orden de la lista no
     cuenta, un conjunto distinto del medido aborta la emision y sin conjunto
     declarado no hay modelo que emitir.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
_EXTRACTORS = _HERE.parent / "extractors"
for _p in (_HERE, _FEATURES, _EXTRACTORS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import build_models as bm  # noqa: E402
import columns as C  # noqa: E402
import lineage as lg  # noqa: E402
import publish_web as pw  # noqa: E402
import qa_models as qa  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str, detalle: object = None) -> None:
    """Registra una comprobacion; `detalle` solo se imprime cuando falla.

    Un fallo de conjunto —«hay ejes que el catalogo no emite»— no se diagnostica
    con un booleano: hace falta ver CUALES. Se imprime solo al fallar para no
    ensuciar la salida de la bateria verde.
    """
    if cond:
        PASS.append(msg)
        print(f"  [OK ] {msg}")
    else:
        FAIL.append(msg)
        print(f"  [FAIL] {msg}")
        if detalle is not None:
            print(f"         -> {detalle}")


# ===========================================================================
# Utilidades de construccion
# ===========================================================================
def _upcoming(n: int = 4) -> pd.DataFrame:
    """Cuatro partidos por jugar con todas las columnas del contrato de emision.

    Los ejes se rellenan con una rampa de minimo a maximo del rango declarado
    para que el normalizado de cada eje sea exactamente 0, 1/3, 2/3 y 1, que es
    un valor comprobable a mano en lugar de un numero que solo se puede aceptar.
    """
    up = pd.DataFrame({
        "match_key": [f"m{i}" for i in range(n)],
        "date": pd.to_datetime("2026-08-15") + pd.to_timedelta(range(n), "D"),
        "season": "2026/27",
        "round": 1,
        "home_key": ["rma", "fcb", "atm", "sev"][:n],
        "away_key": ["vil", "bet", "val", "ath"][:n],
        "home_team": ["Real", "Barsa", "Atleti", "Sevilla"][:n],
        "away_team": ["Villarreal", "Betis", "Valencia", "Athletic"][:n],
    })
    for lado in ("home", "away"):
        for eje in pw.EJES_RADAR:
            up[f"{lado}_{eje.base}"] = np.linspace(eje.minimo, eje.maximo, n)
    return up


def _con_cuotas(up: pd.DataFrame, cuotas, sin_precio=()) -> pd.DataFrame:
    """Anade las tres cuotas del consenso, dejando sin precio las filas indicadas."""
    cols = list(pw.bt.CUOTAS[pw.CUOTAS_EMISION])
    for j, c in enumerate(cols):
        up[c] = [fila[j] for fila in cuotas]
    if len(sin_precio):
        up.loc[list(sin_precio), cols] = np.nan
    return up


class _ModeloJuguete:
    """Estimador cerrado: la probabilidad local crece con la primera columna.

    No aprende nada —`fit` es un no-operativo— porque lo que se prueba no es la
    calidad del ajuste sino el circuito que rodea al ajuste: el orden en que se
    comprueba el esquema, la forma de la salida y la aditividad del reparto. Un
    estimador real haria las tres cosas indistinguibles de sus propios errores.
    """

    classes_ = np.array([0, 1, 2])

    def __init__(self, cols):
        self.cols = list(cols)

    def fit(self, X, y, sample_weight=None):     # pragma: no cover - trivial
        return self

    def predict_proba(self, X):
        a = pd.to_numeric(X[self.cols[0]], errors="coerce").to_numpy(dtype="float64")
        b = pd.to_numeric(X[self.cols[1]], errors="coerce").to_numpy(dtype="float64")
        z = np.column_stack([1.0 + a, 1.0 + 0.0 * a, 1.0 + b])
        return z / z.sum(axis=1, keepdims=True)


class _EstadoJuguete:
    """El minimo de `PreprocessState` que la emision consulta: sus columnas."""

    def __init__(self, cols):
        self.feature_cols = list(cols)
        self.flag_cols = {}
        self.regime = pw.REGIMEN


# ===========================================================================
# 1. Magnitudes derivadas de una terna
# ===========================================================================
def test_1_derivadas() -> None:
    print("\n[1] magnitudes derivadas de la terna")
    P = np.array([[1 / 3, 1 / 3, 1 / 3],
                  [0.90, 0.05, 0.05],
                  [0.40, 0.35, 0.25],
                  [0.20, 0.30, 0.50]])
    d = pw.derivadas_terna(P)

    check(list(d["clase"]) == ["H", "H", "H", "A"],
          f"la clase es el argumento maximo en el orden H/D/A: {list(d['clase'])}")
    check(abs(float(d.entropia[0]) - 1.0) < 1e-12,
          f"la terna uniforme tiene entropia 1 tras normalizar por log 3 "
          f"({d.entropia[0]:.9f})")
    check(float(d.entropia[1]) < 0.40,
          f"la terna concentrada tiene entropia baja ({d.entropia[1]:.3f})")
    check(abs(float(d.margen[2]) - 0.05) < 1e-12,
          "el margen es la distancia entre las dos clases mas probables (0.40-0.35)")
    check(abs(float(d.confianza[1]) - 0.90) < 1e-12,
          "la confianza es la probabilidad de la clase predicha")
    check(float(d.entropia.min()) >= 0.0 and float(d.entropia.max()) <= 1.0,
          "la entropia normalizada nunca sale de [0,1]")

    # Una probabilidad nula no debe producir un NaN por el logaritmo: el termino
    # p*log(p) tiende a cero y asi tiene que calcularse, porque una entropia NaN
    # se propagaria a la interfaz como una casilla vacia sin explicacion.
    cero = pw.derivadas_terna(np.array([[1.0, 0.0, 0.0]]))
    check(abs(float(cero.entropia[0])) < 1e-12,
          "la certeza absoluta da entropia 0 y no NaN pese al log(0)")

    for mala in (np.zeros((2, 2)), np.zeros((2, 4)), np.zeros(3)):
        try:
            pw.derivadas_terna(mala)
            check(False, f"una matriz {getattr(mala, 'shape', '?')} deberia fallar")
        except ValueError:
            check(True, f"rechazada una matriz de forma {mala.shape}")


# ===========================================================================
# 2. Normalizacion de los ejes
# ===========================================================================
def test_2_normalizacion() -> None:
    print("\n[2] normalizacion contra el rango declarado")
    directo = pw.EjeRadar("x", "X", "x", 0.0, 10.0)
    inverso = pw.EjeRadar("y", "Y", "y", 1.0, 20.0, True)

    z = pw._normaliza(pd.Series([-5.0, 0.0, 5.0, 10.0, 99.0]), directo)
    check(list(z) == [0.0, 0.0, 0.5, 1.0, 1.0],
          f"se recorta a [0,1] por los dos extremos del rango declarado: {list(z)}")

    zi = pw._normaliza(pd.Series([1.0, 20.0, 10.5]), inverso)
    check(list(zi[:2]) == [1.0, 0.0],
          "en un eje invertido el minimo del rango puntua 1 y el maximo 0")
    check(abs(float(zi[2]) - 0.5) < 1e-12,
          "el punto medio de un eje invertido sigue siendo 0.5")

    # La normalizacion NO puede depender de lo observado: dos series distintas con
    # el mismo valor tienen que dar el mismo normalizado, que es justo lo que se
    # pierde al escalar contra el minimo y el maximo de la muestra.
    a = pw._normaliza(pd.Series([2.0, 8.0]), directo)
    b = pw._normaliza(pd.Series([2.0, 3.0, 4.0]), directo)
    check(abs(float(a[0]) - float(b[0])) < 1e-12,
          "el mismo valor crudo da el mismo normalizado en dos muestras distintas")

    nan = pw._normaliza(pd.Series([np.nan]), directo)
    check(bool(pd.isna(nan[0])),
          "un valor ausente sigue ausente y no se convierte en cero")


# ===========================================================================
# 3. Declaracion de ejes
# ===========================================================================
def test_3_ejes_declarados() -> None:
    print("\n[3] declaracion publicada de los ejes")
    ej = pw.ejes_declarados()
    check(len(ej) == len(pw.EJES_RADAR), "una fila por eje declarado")
    check(bool(ej.eje.is_unique), "los identificadores de eje son unicos")
    check(bool((ej.rango_max > ej.rango_min).all()),
          "ningun rango es degenerado ni esta invertido por error de escritura")
    check(ej.version_normalizacion.nunique() == 1,
          "el artefacto lleva una sola version de normalizacion")
    check(set(ej.columns) >= {"eje", "titulo", "columna_base", "rango_min",
                              "rango_max", "invertido", "version_normalizacion"},
          "la declaracion incluye todo lo necesario para dibujar la escala fuera")

    # El radar y la linea temporal dibujan UN EQUIPO, asi que solo pueden leer
    # magnitudes que el catalogo emita separadas para el local y el visitante. La
    # mayoria de los atributos de equipo viaja solo como diferencia, y declarar
    # uno de ellos aqui no romperia nada visible: la columna `home_...` no
    # existiria, el rescate del modulo la sustituiria por NaN y el grafico se
    # publicaria vacio sin que ningun error lo dijese. Por eso el modulo contrasta
    # su declaracion con el catalogo al importarse, y por eso se comprueba que ese
    # contraste esta VIVO: un guardian que no salta cuando debe es peor que no
    # tenerlo, porque ademas tranquiliza.
    declaradas = pw._columnas_del_catalogo()
    faltan = [b for b in ej.columna_base
              if not {f"home_{b}", f"away_{b}"} <= declaradas]
    check(not faltan, "todos los ejes se leen de columnas que el catalogo emite "
                      "por lados", faltan)
    try:
        pw._exige_por_lado(["form_points_3"], "prueba")
        salto = False
    except ValueError:
        salto = True
    check(salto, "y una base que solo viaja como diferencia aborta la declaracion")


# ===========================================================================
# 4. Cobertura por partido
# ===========================================================================
def test_4_cobertura() -> None:
    print("\n[4] cobertura de bloques por partido emitido")
    up = _upcoming()
    lin = lg.feature_lineage()
    reales = [c for c in lin.columna if c not in up.columns][:12]
    for c in reales:
        up[c] = np.arange(len(up), dtype="float64")

    cov = pw.cobertura_por_partido(up)
    check(len(cov) == len(up), "una fila de cobertura por partido")
    check(bool(cov.cobertura.between(0.0, 1.0).all()), "la cobertura vive en [0,1]")
    check(bool((cov.cobertura == cov.cobertura.iloc[0]).all()),
          "sin huecos todos los partidos tienen la misma cobertura")

    hueco = up.copy()
    hueco.loc[2, reales[0]] = np.nan
    cov2 = pw.cobertura_por_partido(hueco)
    check(float(cov2.loc[2, "cobertura"]) < float(cov.loc[2, "cobertura"]),
          "un solo valor ausente rebaja la cobertura del partido afectado")
    otros = [i for i in cov2.index if i != 2]
    check(bool((cov2.loc[otros, "cobertura"].to_numpy()
                == cov.loc[otros, "cobertura"].to_numpy()).all()),
          "y no rebaja la de ningun otro partido de la misma jornada")


# ===========================================================================
# 5. Radar
# ===========================================================================
def test_5_radar() -> None:
    print("\n[5] ejes del radar en formato largo")
    up = _upcoming()
    r = pw.radar(up)

    check(len(r) == 2 * len(pw.EJES_RADAR) * len(up),
          f"filas = 2 lados x {len(pw.EJES_RADAR)} ejes x {len(up)} partidos "
          f"({len(r)})")
    check(bool(r.normalizado.between(0.0, 1.0).all()),
          "todo lo normalizado esta en [0,1]")
    check(set(r.lado) == {"local", "visitante"}, "los dos lados estan presentes")
    check(bool((r.version_normalizacion == pw.VERSION_NORMALIZACION).all()),
          "la version de normalizacion viaja en cada fila")
    check("valor" in r.columns and r.valor.notna().any(),
          "el valor crudo acompana al normalizado para poder etiquetar el grafico")

    inv = r[r.eje == "defensa"]
    check(float(inv.loc[inv.valor.idxmin(), "normalizado"])
          > float(inv.loc[inv.valor.idxmax(), "normalizado"]),
          "en el eje invertido, menos es mas area")

    # Un eje cuya columna no exista se omite en lugar de publicarse vacio: una
    # arista de radar con valor cero es indistinguible de un rendimiento pesimo.
    sin_bajas = up.drop(columns=["home_out_value_pct", "away_out_value_pct"])
    r2 = pw.radar(sin_bajas)
    check("bajas" not in set(r2.eje),
          "un eje sin columna en la matriz no se dibuja como cero")
    check(len(r2) == len(r) - 2 * len(up),
          "y desaparecen exactamente sus filas, no mas")


# ===========================================================================
# 6. Tabla de la proxima jornada
# ===========================================================================
def test_6_proxima_jornada() -> None:
    print("\n[6] contrato de la proxima jornada")
    up = _con_cuotas(_upcoming(),
                     [(2.00, 3.00, 3.20),
                      (2.00, 3.00, 3.20),
                      (2.00, 3.00, 3.20),
                      (2.00, 3.00, 3.20)],
                     sin_precio=[3])
    P = np.array([[0.55, 0.25, 0.20],
                  [0.30, 0.30, 0.40],
                  [0.34, 0.33, 0.33],
                  [0.60, 0.20, 0.20]])
    jor = pw.proxima_jornada(up, P)

    check(len(jor) == len(up), "una fila por partido emitido")
    check(bool(jor.date.is_monotonic_increasing), "ordenada por fecha")
    check("cobertura" in jor.columns,
          "la cobertura por bloques viaja con la prediccion, no en otro fichero")

    f0 = jor[jor.match_key == "m0"].iloc[0]
    check(abs(float(f0.valor_esperado) - (0.55 * 2.00 - 1.0)) < 1e-12,
          f"el valor esperado es p*c-1 sobre la clase PREDICHA ({f0.valor_esperado:.4f})")
    check(abs(float(f0.cuota) - 2.00) < 1e-12,
          "y la cuota publicada es la de esa misma clase")

    # El caso que justifica publicar dos columnas: en m1 la clase mas probable es
    # la visitante, pero el empate a 3.00 con probabilidad 0.30 tiene VE -0.10 y
    # la visitante a 3.20 con 0.40 tiene VE +0.28. Ambas afirmaciones conviven y
    # la tabla tiene que distinguirlas.
    f1 = jor[jor.match_key == "m1"].iloc[0]
    check(f1.clase == "A" and f1.seleccion == "A",
          "cuando coinciden, clase predicha y seleccion de valor coinciden")
    f2 = jor[jor.match_key == "m2"].iloc[0]
    check(f2.clase == "H" and f2.seleccion == "A",
          f"la seleccion de valor puede no ser la clase predicha "
          f"({f2.clase} predicha, {f2.seleccion} por valor)")
    check(abs(float(f2.seleccion_ve) - (0.33 * 3.20 - 1.0)) < 1e-12,
          "y su VE es el de la clase de mayor valor esperado")

    # Y el caso que separa la recomendacion de la lectura de valor. En m2 la clase
    # predicha es la local, cuyo valor esperado es 0.34*2.00-1 = -0.32: la politica
    # publicada —`build_models.POLITICA_ECONOMIA = "favorito"`, una unidad plana al
    # signo que el modelo señala en todos los partidos con precio— la apuesta igual.
    # Si `apostable` volviera a colgar del umbral de valor, esta fila saldria en
    # falso y la aplicacion mostraria una rentabilidad historica que mide una
    # politica distinta de la que recomienda.
    check(float(f2.valor_esperado) < 0.0 and bool(f2.apostable) is True,
          f"un partido con precio se apuesta aunque su VE sea negativo "
          f"({f2.valor_esperado:+.4f})")

    check(bool((jor.kelly.dropna() <= pw.bt.TOPE_KELLY + 1e-12).all()),
          f"ninguna fraccion de Kelly supera el tope ({pw.bt.TOPE_KELLY:.0%})")
    check(bool((jor.kelly.dropna() >= 0.0).all()),
          "y ninguna es negativa: no se apuesta en contra")

    f3 = jor[jor.match_key == "m3"].iloc[0]
    check(bool(pd.isna(f3.seleccion)) or f3.seleccion is None,
          "un partido sin precio no lleva seleccion heredada del argumento maximo")
    check(bool(f3.apostable) is False, "y por tanto no es apostable")
    check(bool(pd.isna(f3.cuota)), "ni publica una cuota inventada")

    sin = pw.proxima_jornada(_upcoming(), P)
    check(str(sin.cuotas_origen.iloc[0]) == "sin precio",
          "sin ninguna cuota en la matriz la tabla lo declara en vez de omitirlo")
    check(bool(sin.apostable.eq(False).all()),
          "y ningun partido resulta apostable")


# ===========================================================================
# 7. Perfiles de equipo
# ===========================================================================
def test_7_perfiles() -> None:
    print("\n[7] serie por equipo y jornada")
    feat = _upcoming()
    feat[C.TARGET] = ["H", "D", "A", None]
    for lado in ("home", "away"):
        for _, col in pw._PERFIL:
            feat[f"{lado}_{col}"] = np.arange(len(feat), dtype="float64")

    pf = pw.perfiles_equipo(feat)
    check(len(pf) == 2 * len(feat), "dos filas por partido, una por equipo")
    check(bool(pf.sort_values(["equipo", "date"]).index.equals(pf.index)),
          "ordenada por equipo y fecha, que es como se lee una trayectoria")

    def _res(clave, lado):
        return pf[(pf.match_key == clave) & (pf.lado == lado)].iloc[0]["resultado"]

    check(_res("m0", "local") == "V" and _res("m0", "visitante") == "P",
          "H se traduce a V para el local y P para el visitante")
    check(_res("m1", "local") == "E" and _res("m1", "visitante") == "E",
          "D se traduce a E para los dos")
    check(_res("m2", "local") == "P" and _res("m2", "visitante") == "V",
          "A se traduce a P para el local y V para el visitante")
    check(_res("m3", "local") is None and _res("m3", "visitante") is None,
          "un partido sin resultado no inventa uno: la serie queda abierta")

    rival = pf[(pf.match_key == "m0") & (pf.lado == "local")].iloc[0]["rival"]
    check(rival == "vil", "cada fila lleva el rival de ese partido")
    check(set(dict(pw._PERFIL)) <= set(pf.columns),
          "estan todas las magnitudes del perfil declarado")


# ===========================================================================
# 8. Diagnosticos de clasificacion
# ===========================================================================
def test_8_diagnosticos() -> None:
    print("\n[8] diagnosticos por clase y por temporada")
    rng = np.random.default_rng(11)
    n = 240
    P = rng.dirichlet([4.0, 3.0, 3.0], size=n)
    pred = pd.DataFrame(P, columns=pw.ev.PROB_COLS)
    pred["season"] = np.where(np.arange(n) < 120, "2023/24", "2024/25")
    pred[C.TARGET_NUM] = rng.integers(0, 3, n)

    dg = pw.diagnosticos(pred)
    check(set(dg) == {"informe", "roc"}, "devuelve informe por clase y ROC")
    check(set(dg["informe"].season) == {"total", "2023/24", "2024/25"},
          "el total y cada temporada, porque el agregado esconde el empate")
    check(bool((dg["informe"].groupby("season").size() == 5).all()),
          "cinco filas por corte: tres clases mas macro y ponderado")
    check(len(dg["roc"]) == 3, "una fila de ROC por corte")
    check(bool(dg["roc"].auc_macro.between(0.0, 1.0).all()),
          "las AUC estan en [0,1]")
    check("auc_micro" in dg["roc"].columns,
          "se publican macro y micro, que difieren con clases desbalanceadas")

    # Un modelo aleatorio tiene que dar AUC cercana a 0.5: si saliera muy alta,
    # el diagnostico estaria midiendo otra cosa.
    check(abs(float(dg["roc"].loc[dg["roc"].season == "total", "auc_macro"].iloc[0])
              - 0.5) < 0.12,
          "un pronostico sin senal da AUC macro cercana a 0.5")


# ===========================================================================
# 9. Ficha del modelo
# ===========================================================================
def test_9_ficha() -> None:
    print("\n[9] ficha del modelo publicado")
    met = pd.DataFrame([
        {"modelo": "logistica", "contendiente": "modelo", "n": 100,
         "rps": 0.190, "logloss": 0.98, "accuracy": 0.52},
        {"modelo": "logistica", "contendiente": "consenso", "n": 100,
         "rps": 0.185, "logloss": 0.97, "accuracy": 0.53},
        {"modelo": "previa", "contendiente": "modelo", "n": 100,
         "rps": 0.210, "logloss": 1.05, "accuracy": 0.46},
        {"modelo": "previa", "contendiente": "consenso", "n": 100,
         "rps": 0.185, "logloss": 0.97, "accuracy": 0.53},
    ])
    man = {"publicado": {"modelo": "logistica", "motivo": "bate a la previa en RPS"},
           "run_code": "abc123def456",
           "protocolo": {"regimen": "semanal"},
           "familia": "logistica", "hiperparametros": {"C": 1.0},
           "conjunto": {"cerrado": True, "procedencia": "C.4 · mercado + fuerza",
                        "n_variables": 3, "variables": ["a", "b", "c"]},
           "economia": {"cierre_avg": {"rendimiento": 0.031,
                                       "casa_referencia": "b365",
                                       "rendimiento_referencia": -0.052,
                                       "diferencia": 0.083}},
           "limitaciones": ["el mercado incorpora informacion no disponible aqui",
                            "el retorno simulado no descuenta la liquidez"]}
    st = _EstadoJuguete(["a", "b", "c"])

    fi = pw.ficha_modelo(man, st, met, {"partidos": 10})
    check(fi["modelo"] == "logistica", "publica el modelo que eligio el protocolo")
    check(fi["motivo_de_eleccion"].startswith("bate a la previa"),
          "y el motivo con el que se justifica publicarlo")
    check(abs(fi["metricas_validacion"]["rps"] - 0.190) < 1e-12,
          "las metricas son las de la fila del contendiente 'modelo'")
    check(set(fi["referencias"]) == {"consenso"},
          "las referencias son las del modelo publicado y no las de otro candidato")
    check(fi["familia"] == "logistica" and fi["conjunto"]["cerrado"] and
          fi["conjunto"]["variables"] == ["a", "b", "c"],
          "la ficha declara con que familia y con que variables se midio")
    check(fi["conjunto"]["procedencia"].startswith("C.4"),
          "y de que seccion del cuaderno salio la eleccion")
    check(abs(fi["referencias"]["consenso"]["rps"] - 0.185) < 1e-12,
          "con su cifra intacta, para que la comparacion sea auditable")
    check(fi["variables"]["n_features"] == 3,
          "cuenta las variables del estado congelado")
    eco = fi["economia"]["cierre_avg"]
    check(eco["casa_referencia"] == "b365"
          and abs(eco["diferencia"] - (eco["rendimiento"]
                                       - eco["rendimiento_referencia"])) < 1e-12,
          "la ficha lleva el rendimiento con el del favorito de Bet365 al lado")
    check(fi["limitaciones"] == man["limitaciones"],
          "las limitaciones vienen del manifiesto y estan al nivel de las metricas")
    check(fi["normalizacion_graficos"]["version"] == pw.VERSION_NORMALIZACION,
          "la version de normalizacion queda registrada en la ficha")
    check(fi["run_code"] == "abc123def456",
          "y el codigo de ejecucion, que ata la ficha a unos artefactos concretos")

    # Sin jornada pendiente la ficha se publica igual, pero el recuento de la
    # matriz de emision va a `null`, no a cero. La distincion importa porque la
    # ficha lleva en el mismo fichero `conjunto.n_variables`: un cero afirmaria
    # que el modelo no usa ninguna variable tres lineas por encima de la lista
    # de las tres que usa, y es exactamente lo que escribio la primera ficha
    # publicada de verdad.
    vacia = pw.ficha_modelo(man, None, met, None)
    check(vacia["variables"]["n_features"] is None
          and vacia["variables"]["n_indicadores_faltante"] is None,
          "sin estado cargado el recuento de la matriz es nulo, no cero")
    check(vacia["conjunto"]["n_variables"] == 3,
          "y el conjunto del modelo sigue ahi, que es lo que no es nulo")
    check(vacia["emision"] == {"estado": "sin jornada pendiente"},
          "declarando explicitamente que no hay emision")


# ===========================================================================
# 9.bis. El papel de cada temporada, que es lo que deja rotular el repaso
# ===========================================================================
def test_9bis_temporadas_rol() -> None:
    """El rotulo por temporada, y en particular que la sellada no sea `medicion`.

    La aplicacion tiene que repasar temporada por temporada, y tres de las que
    ensena eligieron el peso de la mezcla: presentarlas sin distinguir es citar la
    cifra que la regla 5 no deja citar. El rotulo es funcion solo de la temporada,
    asi que viaja en la ficha y no fila a fila.

    El caso que de verdad se comprueba aqui es el holdout. 2025/26 es POSTERIOR al
    corte de seleccion, de modo que la aritmetica del corte sola la rotula
    `medicion` y la aplicacion la ensenaria como una temporada medida mas. No
    rompe nada y miente, que es el modo de fallo de esta capa.
    """
    print("\n[9.bis] papel de cada temporada frente a la eleccion")
    filas = []
    for s, split in (("2016/17", "dev"), ("2017/18", "dev"), ("2018/19", "dev"),
                     ("2019/20", "dev"), ("2021/22", "dev"), ("2022/23", "dev"),
                     ("2024/25", "dev"), ("2025/26", "holdout"),
                     ("2026/27", "dev")):
        filas.append({C.SEASON: s, C.SPLIT: split,
                      C.DATE: pd.Timestamp("2027-01-01" if s == "2026/27"
                                           else "2020-01-01")})
    feat = pd.DataFrame(filas)
    rol = bm.temporadas_rol(feat)

    check(rol["2025/26"] == "holdout",
          "la temporada sellada se reconoce por el dato, no por el corte")
    check(rol["2019/20"] == "seleccion" and rol["2021/22"] == "seleccion",
          "las que eligieron el peso quedan rotuladas como tales")
    check(rol["2022/23"] == "medicion" and rol["2024/25"] == "medicion",
          "y las posteriores al corte son las citables")
    check(all(rol[s] == "confirmacion"
              for s in ("2016/17", "2017/18", "2018/19")),
          "el arranque declarado es confirmacion: anterior a la busqueda")
    check(set(rol) == {f[C.SEASON] for f in filas},
          "una temporada del historico, un rotulo, sin inventar ni perder")


# ===========================================================================
# 10. Emision: paridad de esquema y aditividad de la explicacion
# ===========================================================================
def test_10_emision() -> None:
    print("\n[10] emision de la jornada")
    lin = lg.feature_lineage()
    # Dos bloques distintos para que el reparto tenga algo que repartir.
    por_bloque = (lin.groupby("bloque")["columna"].first().head(3).tolist())
    cols = por_bloque
    n = 5
    X = pd.DataFrame({c: np.linspace(0.0, 1.0, n) * (j + 1)
                      for j, c in enumerate(cols)})
    X["match_key"] = [f"m{i}" for i in range(n)]
    X["date"] = pd.to_datetime("2026-08-15") + pd.to_timedelta(range(n), "D")
    up = pd.DataFrame({"match_key": X.match_key, "date": X.date})

    modelo = _ModeloJuguete(cols)
    st = _EstadoJuguete(cols)
    Xtr = X[cols].copy()

    P = pw.puntua_jornada(modelo, st, X, up, Xtr)
    check(P.shape == (n, 3), f"la puntuacion devuelve una matriz N x 3 ({P.shape})")
    check(bool(np.allclose(P.sum(axis=1), 1.0)), "y sus ternas suman uno")

    # El defecto silencioso que la comprobacion previa existe para cazar: el mismo
    # conjunto de columnas en otro orden. El estimador no fallaria; aplicaria cada
    # coeficiente a la columna equivocada y devolveria numeros plausibles.
    permutado = _EstadoJuguete([cols[1], cols[0]] + cols[2:])
    try:
        pw.puntua_jornada(modelo, permutado, X, up, Xtr)
        check(False, "una permutacion de columnas deberia abortar la emision")
    except qa.PublicacionError as exc:
        check("orden" in str(exc) or "posici" in str(exc),
              "una permutacion de columnas aborta la emision ANTES de puntuar")

    faltante = _EstadoJuguete(cols[:-1])
    try:
        pw.puntua_jornada(modelo, faltante, X, up, Xtr)
        check(False, "un esquema mas estrecho deberia abortar la emision")
    except qa.PublicacionError:
        check(True, "un esquema mas estrecho que el de entrenamiento tambien aborta")

    base = pw.ex.baseline_median(Xtr)
    aporta, frases = pw.explicaciones_jornada(modelo, X[cols], None, base,
                                              up["match_key"])
    check(len(frases) == n, "una frase por partido emitido")
    check(bool(frases.narrativa.str.len().gt(20).all()),
          "y ninguna frase vacia")
    check(set(aporta.match_key) == set(up.match_key),
          "la descomposicion cubre todos los partidos")
    check(bool(aporta.groupby("match_key").dominante.sum().le(4).all()),
          "se marcan a lo sumo cuatro bloques dominantes por partido")

    # Eficiencia: lo que se dibuja suma lo que se afirma. El error se publica en
    # los atributos de la tabla para que la interfaz pueda citarlo sin recalcularlo.
    err = aporta.attrs.get("error_eficiencia")
    check(err is not None and float(err) < 1e-9,
          f"las aportaciones suman la distancia al partido de referencia "
          f"(error {float(err):.2e})")


# ===========================================================================
# 11. Configuracion con la que se emite
# ===========================================================================
def test_11_configuracion_emision() -> None:
    """Emitir no elige: comprueba que lo declarado es lo que se midio.

    La averia que esta funcion existe para cazar es silenciosa. Si alguien toca
    `CONJUNTO` en build_models.py despues de haber ejecutado el recorrido, en
    disco quedan unas metricas que describen a un modelo y una emision que
    ejecuta a otro; como el nombre de la familia no cambia, nada en la aplicacion
    lo delataria.
    """
    print("\n[11] configuracion con la que se emite")
    import build_models as bm

    declarado = ("mercado_p_home", "elo_diff", "forma_h")
    original = bm.CONJUNTO
    try:
        bm.CONJUNTO = declarado
        man = {"conjunto": {"cerrado": True, "n_variables": 3,
                            "variables": list(declarado)}}

        cols, motivo = pw.configuracion_emision(man, "2025/26")
        check(list(cols) == list(declarado),
              "emite con el conjunto declarado en build_models.py")
        check("manifiesto" in motivo and "2025/26" in motivo,
              "y dice contra que artefacto lo ha verificado")

        # El sello del holdout es mas reciente que el manifiesto y se mira antes.
        sello = {"conjunto": list(declarado)}
        cols, motivo = pw.configuracion_emision(man, "2025/26", sello=sello)
        check(list(cols) == list(declarado) and "sello" in motivo,
              "el sello del holdout se comprueba antes que el manifiesto")

        # El orden no es parte del conjunto: la matriz manda sobre la lista.
        revuelto = {"conjunto": {"variables": list(reversed(declarado))}}
        cols, _ = pw.configuracion_emision(revuelto, "2025/26")
        check(list(cols) == list(declarado),
              "el orden de la lista no cuenta, solo que sean las mismas variables")

        # La averia: el artefacto se midio con otro conjunto.
        try:
            pw.configuracion_emision(
                {"conjunto": {"variables": ["elo_diff", "forma_h"]}}, "2025/26")
            check(False, "un conjunto distinto del medido no se emite en silencio")
        except ValueError as e:
            check("no es el que produjo las cifras" in str(e),
                  "un conjunto distinto del medido no se emite en silencio")

        # Un artefacto que no trae la lista no bloquea: se emite lo declarado y
        # el motivo dice que no ha habido con que compararlo.
        cols, motivo = pw.configuracion_emision({}, "2025/26")
        check(list(cols) == list(declarado) and "no" in motivo,
              "sin lista en el artefacto se emite lo declarado, diciendolo")

        bm.CONJUNTO = ()
        try:
            pw.configuracion_emision({}, "2025/26")
            check(False, "sin conjunto declarado no hay modelo que emitir")
        except ValueError:
            check(True, "sin conjunto declarado no hay modelo que emitir")
    finally:
        bm.CONJUNTO = original


# ===========================================================================
# 12. La terna por partido, y que el agregado no se coma la temporada en curso
# ===========================================================================
def test_12_predicciones_por_partido() -> None:
    """La tabla de partido, y el reparto entre filas por temporada y fila total.

    Lo que de verdad se vigila aqui es el reparto. Las filas por temporada tienen
    que traer la temporada en curso —si no, la web no puede repasarla— y la fila
    `total` NO, porque es la cifra que se cita. Las dos cosas a la vez, y el modo
    de fallo es silencioso en las dos direcciones: sin la temporada en curso la
    pagina sale vacia, y con ella dentro del total el agregado se desplaza unas
    decimas sin que nada se rompa.
    """
    print("\n[12] terna por partido y reparto del agregado")
    rng = np.random.default_rng(1220)
    n_val, n_cur = 200, 40
    n = n_val + n_cur
    P = rng.dirichlet([4.0, 3.0, 3.0], size=n)
    pred = pd.DataFrame(P, columns=pw.ev.PROB_COLS)
    pred["season"] = (["2023/24"] * 100 + ["2024/25"] * 100 + ["2026/27"] * n_cur)
    pred[bm.BLOQUE] = ["validacion"] * n_val + ["produccion"] * n_cur
    pred[C.TARGET_NUM] = rng.integers(0, 3, n)
    pred["jornada"] = 1
    pred["date"] = pd.date_range("2024-01-01", periods=n, freq="D")
    pred["match_key"] = [f"p{i}" for i in range(n)]
    pred["home_key"] = "local"
    pred["away_key"] = "visitante"
    for lado in ("home", "draw", "away"):
        pred[f"mkt_p_b365_{lado}_raw"] = P[:, ("home", "draw", "away").index(lado)]

    dg = pw.diagnosticos(pred)
    check("2026/27" in set(dg["roc"].season),
          "la temporada en curso tiene su propia fila de ROC")
    soporte_total = float(
        dg["informe"].loc[(dg["informe"].season == "total")
                          & (dg["informe"].clase.isin(["H", "D", "A"])),
                          "soporte"].sum())
    check(soporte_total == float(n_val),
          "el total se mide sobre validacion sola, no sobre todo lo puntuado",
          f"soporte del total = {soporte_total}, esperado {n_val}")

    d = pw.predicciones_por_partido(pred)
    check(len(d) == n and d.match_key.nunique() == n,
          "una fila por partido puntuado, sin duplicar por fuente de cuota")
    # Se reindexa por `match_key` en el orden en que se construyo el sintetico,
    # porque la tabla se publica ordenada por fecha y no por clave.
    claves = [f"p{i}" for i in range(n)]
    vista = d.set_index("match_key").loc[claves]
    check(bool((vista.signo.to_numpy()
                == np.array(pw.ev.CLASES)[np.argmax(P, axis=1)]).all()),
          "el signo es el argumento maximo de la terna del modelo")
    check(bool(np.allclose(d[["p_home", "p_draw", "p_away"]].sum(axis=1), 1.0)),
          "la terna publicada suma uno")
    check(bool((vista.acierto.to_numpy().astype(bool)
                == (np.argmax(P, axis=1) == pred[C.TARGET_NUM].to_numpy())).all()),
          "el acierto compara el signo con el resultado observado")
    check(set(d.bloque) == {"validacion", "produccion"},
          "el bloque viaja en la tabla: es lo que deja filtrar el agregado")
    # Pinnacle no se ha sintetizado: la columna tiene que existir y ser nula, no
    # desaparecer, para que el esquema no cambie segun la temporada.
    check("pinnacle_p_home" in d.columns and d.pinnacle_p_home.isna().all(),
          "una referencia sin cobertura sale como columna nula, no se omite")


# ===========================================================================
def _run() -> None:
    """Las pruebas en orden.

    Hay que añadir aquí toda prueba nueva, y el olvido no se delata solo: pytest
    recoge por su cuenta cualquier función `test_*` del módulo, así que una prueba
    sin registrar sigue saliendo en verde bajo `pytest` y en cambio **no se ejecuta**
    por la vía `__main__`, que es la que se usa para depurar. Pasó con
    `test_9bis_temporadas_rol`: definida, recogida por pytest, nunca llamada aquí.
    """
    print("=" * 72)
    print("BANCO DE PRUEBAS · publish_web.py")
    print("=" * 72)
    test_1_derivadas()
    test_2_normalizacion()
    test_3_ejes_declarados()
    test_4_cobertura()
    test_5_radar()
    test_6_proxima_jornada()
    test_7_perfiles()
    test_8_diagnosticos()
    test_9_ficha()
    test_9bis_temporadas_rol()
    test_10_emision()
    test_11_configuracion_emision()
    test_12_predicciones_por_partido()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    if FAIL:
        for m in FAIL:
            print(f"  - {m}")
    print("=" * 72)


def test_publish_web() -> None:
    """Entrada para pytest."""
    _run()
    assert not FAIL, FAIL


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
