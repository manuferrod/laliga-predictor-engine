"""Banco de pruebas de `laboratorio.py`, sin red y sin artefactos.

El banco manual es la pieza con la que se decide qué variables entran en el
modelo, y por eso lo que hay que comprobar de él no es que dé cifras sino que las
cifras signifiquen lo que se cree. Un banco roto no lanza una excepción: ordena la
tabla al revés, rotula un coeficiente con la variable de al lado, mide dos
conjuntos distintos con el mismo sello, o —lo más caro de todo— deja que una
sesión de tanteo mire el bloque de 2025/26 y contamine el número que se defiende.
Ninguna de esas averías se ve en la salida.

Este banco ataca precisamente eso. Los datos son una liga sintética de seis
temporadas y diez equipos, con nombres de columna REALES —los que la tabla de
procedencia documenta— para que el catálogo por dominios, que es lo que traduce
«una idea» en «un conjunto de columnas», se ejercite tal cual se usa. Las
respuestas correctas se conocen antes de ejecutar nada: están plantadas en el
propio marco o se derivan de la definición de la función.

    1. FAMILIAS Y FABRICA     las cuatro del estudio y el invocable sin argumentos
    2. MODELO CON COLUMNAS    la envoltura que restringe la matriz
    3. CATALOGO DE VARIABLES  columnas disponibles, dominios y atajos
    4. CACHE Y PLIEGUES       lo caro se calcula una vez y se reutiliza
    5. EL HOLDOUT             la reserva no se abre desde el banco
    6. EL ENSAYO              cifras, sello y tablas de un ensayo medido
    7. COMPARAR Y RONDA       ordenar sin decidir; la ronda no adopta nada
    8. REGISTRO               la memoria del estudio entre sesiones
"""
from __future__ import annotations

import sys
import tempfile
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import columns as C  # noqa: E402
import estimators as es  # noqa: E402
import explain as ex  # noqa: E402
import laboratorio as lb  # noqa: E402
import preprocess as pp  # noqa: E402
import protocol as pt  # noqa: E402

warnings.filterwarnings("ignore")

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    if cond:
        PASS.append(msg)
        print(f"  [OK ] {msg}")
    else:
        FAIL.append(msg)
        print(f"  [FAIL] {msg}")


# ===========================================================================
# La liga sintetica
# ===========================================================================
# Los nombres de columna no son inventados: `elo_z_diff`, `form_points_ewm_diff` y los
# precios los emite el catalogo y por tanto figuran en la tabla de procedencia, de
# modo que el catalogo por dominios los clasifica igual que clasificaria a los de
# verdad. Aqui no vale rotularlos a voluntad como en `test_explain.py`, donde la
# tabla se inyecta: `laboratorio.dominios` delega en `explain.bloque_de`, que lee la
# procedencia REAL y levanta `KeyError` ante una columna que el catalogo no emite.
# Ese acoplamiento es deliberado (el banco tiene que romperse si alguien prueba una
# variable que la matriz ya no construye) y obliga a que estos nombres se revisen
# cuando el catalogo cambie. Una liga con columnas `x1, x2, x3` habria dejado sin
# probar justo la parte del banco que traduce una idea en un conjunto de variables.
#
# La fuerza entra por `elo_z_diff` y no por la lectura de un lado: el catalogo declara
# este atributo como DIFERENCIA, porque el nivel —la suma de los dos lados— no lleva
# senal y la resta si (regla 2). El par `home_elo_z`/`away_elo_z` existe en la matriz,
# pero como columna de PRESENTACION: la dibuja el radar de la aplicacion y
# `columns.PRESENTACION` la saca del conjunto de candidatas, de modo que pedirla aqui
# no probaria el banco sino su manejo de una columna ausente. El sufijo `_z` tampoco es
# opcional: el Elo que llega al modelo viene tipificado dentro de la jornada y el nivel
# crudo se queda en el maestro, de modo que una columna `elo_diff` a secas seria una que
# la matriz no construye. Cuando este banco se ponga rojo con un `KeyError` de
# procedencia, lo que hay que corregir es el nombre de aqui; relajar `bloque_de` para
# que acepte lo que no esta declarado apagaria justo el aviso por el que existe.
_EQUIPOS = [f"e{i:02d}" for i in range(10)]
_SEASONS = ["2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2025/26"]

# Un conjunto pequeño que atraviesa tres dominios distintos. Se usa en casi todas
# las secciones para que el coste de la cache se pague una sola vez.
_TRIO = ["mkt_p_home", "elo_z_diff", "form_points_ewm_diff"]


def _calendario(equipos: list[str]) -> list[list[tuple[int, int]]]:
    """Liga a doble vuelta por el metodo del circulo: cada equipo juega una vez."""
    n = len(equipos)
    idx = list(range(n))
    ida = []
    for _ in range(n - 1):
        ida.append([(idx[i], idx[n - 1 - i]) for i in range(n // 2)])
        idx = [idx[0]] + [idx[-1]] + idx[1:-1]
    return ida + [[(b, a) for a, b in jornada] for jornada in ida]


def _liga(semilla: int = 11) -> pd.DataFrame:
    """Seis temporadas con precio de mercado, cuotas de cierre y resultado.

    El resultado se sortea CON la probabilidad que publica el mercado, así que el
    precio es informativo por construccion y una logistica sobre él tiene que
    quedar en el entorno de la referencia. No se busca aquí un modelo bueno: se
    busca que las cifras sean calculables y que el bloque economico tenga apuestas
    que contar, porque un ROI sobre cero apuestas no distingue un banco sano de uno
    que no selecciona nada.
    """
    rng = np.random.default_rng(semilla)
    jornadas = _calendario(_EQUIPOS)
    filas = []
    for t, temporada in enumerate(_SEASONS):
        inicio = pd.Timestamp(f"{int(temporada[:4])}-08-20")
        for j, jornada in enumerate(jornadas):
            base = inicio + pd.Timedelta(days=7 * j)
            for k, (a, b) in enumerate(jornada):
                dia = base + pd.Timedelta(days=int(k % 2))
                p = rng.dirichlet([4.0, 3.0, 3.0])
                # Bet365 desvia un poco del consenso: si publicase exactamente lo
                # mismo, la comparacion con la referencia seria una tautologia.
                b365 = np.clip(p + rng.normal(0.0, 0.012, 3), 0.02, None)
                b365 = b365 / b365.sum()
                filas.append({
                    "match_key": f"{dia.date()}-{_EQUIPOS[a]}-{_EQUIPOS[b]}",
                    "season": temporada,
                    "date": dia,
                    "round": j + 1,
                    "home_key": _EQUIPOS[a],
                    "away_key": _EQUIPOS[b],
                    "home_team": _EQUIPOS[a],
                    "away_team": _EQUIPOS[b],
                    C.TARGET_NUM: int(rng.choice(3, p=p)),
                    C.SPLIT: ("holdout" if temporada in C.HOLDOUT_SEASONS
                              else "dev"),
                    "mkt_p_home": float(p[0]),
                    "mkt_p_draw": float(p[1]),
                    "mkt_p_away": float(p[2]),
                    "mkt_p_b365_home": float(b365[0]),
                    "mkt_p_b365_draw": float(b365[1]),
                    "mkt_p_b365_away": float(b365[2]),
                    # Elo TIPIFICADO dentro de la jornada, como lo declara el
                    # catalogo: desviaciones respecto a la media de Primera ese
                    # dia, del orden de +-2, y no puntos de rating. La escala
                    # importa poco para lo que mide el banco, pero una columna con
                    # el nombre de una real y valores imposibles invita a leer mal
                    # cualquier coeficiente.
                    #
                    # La resta es la variable; el par viaja para que la seccion 3
                    # pueda comprobar que una columna de PRESENTACION esta en la
                    # matriz y aun asi no se ofrece como candidata.
                    "elo_z_diff": float(rng.normal(0.0, 1.0)),
                    "home_elo_z": float(rng.normal(0.0, 1.0)),
                    "away_elo_z": float(rng.normal(0.0, 1.0)),
                    "form_points_ewm_diff": float(rng.normal(0.0, 1.0)),
                    "form_gf_ewm_diff": float(rng.normal(0.0, 1.0)),
                    "home_squad_value": float(rng.normal(100.0 * (t + 1), 8.0)),
                    "away_squad_value": float(rng.normal(100.0 * (t + 1), 8.0)),
                    "home_n_missing": float(rng.integers(0, 4)),
                    "away_n_missing": float(rng.integers(0, 4)),
                    # Margen de casa del 3 %: sin él no habría valor esperado que
                    # superase ningún umbral y la seccion economica no mediria nada.
                    "odds_close_avg_h": float(0.97 / max(p[0], 0.05)),
                    "odds_close_avg_d": float(0.97 / max(p[1], 0.05)),
                    "odds_close_avg_a": float(0.97 / max(p[2], 0.05)),
                })
    return pd.DataFrame(filas)


LIGA = _liga()
# `desde` recorta el bloque evaluado a la ultima temporada de desarrollo. Las
# ventanas de entrenamiento siguen siendo completas, de modo que el recorte no
# cambia el significado de ninguna cifra: solo abarata la bateria.
DESDE = "2020/21"
COMUN = dict(feat=LIGA, desde=DESDE, n_boot=20, registrar=False, verbose=False)


class _Tonto:
    """Estimador minimo: reparte la probabilidad a partes iguales.

    Sirve para probar la envoltura sin arrastrar el coste de una familia real y,
    sobre todo, para probar lo que la envoltura hace cuando el estimador base NO
    admite peso de recencia ni contexto.
    """

    def __init__(self) -> None:
        self.visto_: list[str] = []

    def fit(self, X, y):
        self.visto_ = list(map(str, X.columns))
        self.classes_ = np.array([0, 1, 2])
        return self

    def predict_proba(self, X):
        return np.full((len(X), 3), 1.0 / 3.0)


class _Pesado(_Tonto):
    """Como el anterior, pero declara `sample_weight` y `contexto`."""

    def fit(self, X, y, sample_weight=None, contexto=None):
        self.peso_ = None if sample_weight is None else np.asarray(sample_weight)
        self.contexto_ = contexto
        return super().fit(X, y)


# ===========================================================================
# 1. FAMILIAS Y FABRICA
# ===========================================================================
def test_familias() -> None:
    """La escalera son cuatro familias y la fabrica es un invocable pelado.

    El protocolo pide una fabrica sin argumentos y construye un estimador nuevo por
    pliegue. Si `fabrica` devolviese la instancia en lugar de la receta, el ajuste de
    una jornada se arrastraria a la siguiente y el walk-forward seria una ficcion sin
    que ninguna metrica lo delatase.
    """
    print("\n1. las familias y la fabrica")
    check(tuple(lb.ESCALERA) == ("logistica", "bosque", "refuerzo_xgb", "red"),
          "la escalera del estudio son exactamente las cuatro hipotesis")
    check(tuple(lb.FAMILIAS) == lb.ESCALERA + ("mezcla",),
          "y lo construible son esas cuatro mas la mezcla que publica")
    check(sorted(es.CONSTRUCTORES) == sorted(lb.FAMILIAS),
          "y son las mismas que estimators.py sabe construir")
    check(sorted(lb.HIPER) == sorted(lb.FAMILIAS),
          "cada familia tiene sus valores fijos declarados")
    # La mezcla no entra en la escalera por omision, y el motivo no es estetico:
    # necesita las dos mitades de la matriz y se detendria con cualquier conjunto
    # que no las traiga, convirtiendo un recorrido legitimo en un fallo.
    check("mezcla" not in lb.ESCALERA,
          "y la mezcla no se cuela en el recorrido por familias")

    for fam, params in lb.HIPER.items():
        sobran = [p for p in params if p not in es.ESPACIOS.get(fam, {})]
        check(not sobran, f"los valores fijos de {fam} estan todos declarados "
                          f"en es.ESPACIOS{'' if not sobran else f' ({sobran})'}")

    fab = lb.fabrica("logistica", _TRIO)
    check(callable(fab) and fab.familia == "logistica",
          "la fabrica es invocable y dice de que familia es")
    check(fab.params == lb.HIPER["logistica"],
          "sin hiperparametros propios, la fabrica lleva los fijos de la familia")

    afinada = lb.fabrica("logistica", _TRIO, {"C": 7.0})
    check(afinada.params["C"] == 7.0 and afinada.params["l1_ratio"] ==
          lb.HIPER["logistica"]["l1_ratio"],
          "lo que se pasa sobrescribe, y lo que no se pasa se conserva")
    check(lb.HIPER["logistica"]["C"] != 7.0,
          "y afinar una fabrica no toca la tabla de valores fijos")

    a, b = fab(), fab()
    check(a is not b, "cada llamada construye un estimador nuevo, no el mismo")
    check(isinstance(a, lb.ModeloConColumnas),
          "y lo que construye es la envoltura restringida a las columnas")

    try:
        lb.fabrica("escalera", _TRIO)
        check(False, "una familia fuera del estudio se rechaza al construirla")
    except ValueError as e:
        check("familia desconocida" in str(e),
              "una familia fuera del estudio se rechaza al construirla")


# ===========================================================================
# 2. MODELO CON COLUMNAS
# ===========================================================================
def test_modelo_con_columnas() -> None:
    """La envoltura entrega al estimador las columnas elegidas, y solo esas.

    Lo delicado no es filtrar sino el ORDEN y las AUSENCIAS. El orden se toma de la
    matriz y no de la lista pedida, para que dos conjuntos iguales escritos en
    distinto orden ajusten el mismo modelo. Las ausencias —una columna que la poda
    por colinealidad se llevo en esta ventana— no interrumpen: se anotan, porque un
    conjunto de tres del que una nunca entra es en realidad un conjunto de dos y el
    resultado tiene que poder decirlo.
    """
    print("\n2. la envoltura que restringe la matriz")
    X = pd.DataFrame({"a": [0.0, 1.0, 2.0, 3.0], "b": [1.0, 0.0, 1.0, 0.0],
                      "c": [3.0, 2.0, 1.0, 0.0]})
    y = np.array([0, 1, 2, 0])

    m = lb.ModeloConColumnas(_Tonto(), ["c", "a"]).fit(X, y)
    check(m.columnas_ == ["a", "c"],
          "el orden lo pone la matriz, no la lista pedida")
    check(m.base.visto_ == ["a", "c"],
          "y el estimador base recibe exactamente esas dos columnas")
    check(m.columnas_ausentes_ == (),
          "sin ausencias, la anotacion queda vacia en lugar de no existir")

    m2 = lb.ModeloConColumnas(_Tonto(), ["a", "z"]).fit(X, y)
    check(m2.columnas_ausentes_ == ("z",),
          "una columna que no sobrevive se anota y el ajuste continua")
    check(m2.columnas_ == ["a"], "con las que quedan")

    try:
        lb.ModeloConColumnas(_Tonto(), ["z", "w"]).fit(X, y)
        check(False, "si no queda ninguna columna, no hay matriz y se interrumpe")
    except ValueError as e:
        check("ninguna" in str(e),
              "si no queda ninguna columna, no hay matriz y se interrumpe")

    try:
        lb.ModeloConColumnas(_Tonto(), [])
        check(False, "un conjunto vacio no llega ni a construirse")
    except ValueError:
        check(True, "un conjunto vacio no llega ni a construirse")

    check(lb.ModeloConColumnas(_Tonto(), ["a", "a", "b"]).columnas == ("a", "b"),
          "una columna repetida se cuenta una vez")

    # Peso de recencia y contexto: se reenvian solo si el base los admite, y
    # pedirle peso a quien no lo admite tiene que fallar en voz alta.
    pes = lb.ModeloConColumnas(_Pesado(), ["a", "b"])
    pes.fit(X, y, sample_weight=np.array([1.0, 2.0, 3.0, 4.0]), contexto={"k": 1})
    check(pes.base.peso_ is not None and pes.base.contexto_ == {"k": 1},
          "el peso y el contexto llegan al estimador que los admite")
    try:
        lb.ModeloConColumnas(_Tonto(), ["a", "b"]).fit(X, y,
                                                       sample_weight=np.ones(4))
        check(False, "pedir peso a un estimador que no lo admite falla en voz alta")
    except TypeError as e:
        check("sample_weight" in str(e),
              "pedir peso a un estimador que no lo admite falla en voz alta")
    sin_peso = lb.ModeloConColumnas(_Tonto(), ["a", "b"]).fit(X, y, contexto={"k": 1})
    check(sin_peso.base.visto_ == ["a", "b"],
          "y un contexto que el base no admite se descarta sin romper nada")

    check(lb.ModeloConColumnas(_Tonto(), ["a", "b"]).fit(X, y)
          .predict_proba(X).shape == (4, 3),
          "la prediccion sale con una fila por partido y tres columnas")

    # Los coeficientes se rotulan con lo que el estimador vio de verdad. Rotular
    # con la lista completa cuando falta una columna desplazaria cada coeficiente
    # a la variable de al lado y el grafico se dibujaria igual diciendo otra cosa.
    Xl = pd.DataFrame({"a": np.linspace(-1, 1, 60), "b": np.linspace(1, -1, 60),
                       "z": np.zeros(60)})
    yl = np.tile([0, 1, 2], 20)
    lin = lb.fabrica("logistica", ["a", "b", "ausente"])()
    lin.fit(Xl[["a", "b", "z"]], yl)
    tabla = lin.coeficientes()
    nombres = set(tabla["variable"]) if "variable" in tabla.columns else set(tabla.index)
    check(nombres == {"a", "b"},
          "los coeficientes se rotulan con las columnas que el modelo recibio")
    check("ausente" not in nombres,
          "y la que nunca entro no aparece rotulando el coeficiente de otra")


# ===========================================================================
# 3. CATALOGO DE VARIABLES
# ===========================================================================
def test_catalogo() -> None:
    """De la matriz salen candidatas; de las candidatas, dominios; y de ahi, atajos.

    Los dominios no son una taxonomia nueva: son los bloques explicativos de
    `explain`. Que sean los mismos importa porque lo que el lector reconoce como una
    idea al leer la memoria tiene que ser el mismo conjunto de columnas que se probo
    junto; dos taxonomias obligarian a traducir entre ellas cada vez que se cuenta un
    resultado, y las traducciones se equivocan.
    """
    print("\n3. el catalogo de variables por dominio")
    disp = lb.columnas_disponibles(LIGA)
    check(C.TARGET_NUM not in disp and C.SPLIT not in disp,
          "ni el objetivo ni la marca de reserva son candidatas a variable")
    check("home_team" not in disp and "date" not in disp,
          "los identificadores y la fecha tampoco: no son numericos")
    check("mkt_p_home" in disp and "elo_z_diff" in disp,
          "las variables numericas si lo son")
    # El par de presentacion esta en el marco —el radar de la aplicacion lo
    # dibuja— y aun asi no se ofrece. Es la mitad del mecanismo que hace
    # defendible tener las dos lecturas en la matriz: si el banco las ofreciese,
    # un conjunto podria llevar a la vez la resta y sus dos sumandos, y el ajuste
    # quedaria indeterminado sin que nada lo denunciase.
    check(not ({"home_elo_z", "away_elo_z"} & set(disp)),
          "las columnas de presentacion viajan en la matriz pero no son candidatas")

    # Las cuotas en escala cruda viajan junto a la prediccion para la evaluacion
    # economica, pero no entran en ninguna matriz: una probabilidad estandarizada
    # ya no es una probabilidad y el numero a batir no seria calculable.
    con_crudas = LIGA.assign(**{c: 0.5 for c in pp.PRICE_RAW_COLS})
    check(not (set(pp.PRICE_RAW_COLS) & set(lb.columnas_disponibles(con_crudas))),
          "el precio en escala cruda no es candidato a variable")

    # La probabilidad implicita de Bet365 si esta en la matriz —viaja con cada
    # partido porque la evaluacion economica la necesita— pero no puede ser
    # candidata: es la referencia contra la que se mide el modelo, y una referencia
    # que ademas es una de las variables medidas convierte la comparacion en una
    # tautologia. Se comprueba sobre la liga tal cual, que ya las trae, y no sobre
    # un marco preparado a proposito: asi la comprobacion falla si alguien las
    # anade al conjunto de candidatas, que es el descuido que hay que atrapar.
    check(all(c in LIGA.columns for c in pp.B365_PROB_COLS),
          "la liga sintetica trae la probabilidad de Bet365, como la matriz real")
    check(not (set(pp.B365_PROB_COLS) & set(disp)),
          "y ninguna de sus tres columnas es candidata: es lo que hay que batir")

    # Que una columna no sea numerica no la excluye. Las categoricas ordenadas —la
    # franja de saque, la zona de clasificacion, el estilo— son candidatas y es el
    # preprocesado quien las codifica; excluirlas aqui por su tipo dejaria fuera un
    # bloque entero de la matriz sin que ninguna cifra lo delatase.
    con_categ = LIGA.assign(kickoff_slot="tarde")
    check("kickoff_slot" in lb.columnas_disponibles(con_categ),
          "una categorica ordenada si es candidata: la codifica el preprocesado")

    # Y nada de lo anterior se comprueba contra una lista que el banco reconstruya,
    # sino contra la unica definicion de candidata del proyecto. La igualdad es la
    # propiedad que importa: el dia que `feature_candidates` cambie, el banco cambia
    # con ella sin tocar una linea. Dos definiciones que empiezan iguales acaban
    # discrepando, y la discrepancia se descubre cuando el modelo ya se ajusto.
    check(disp == pp.feature_candidates(LIGA, regime=None),
          "la lista es la de preprocess.feature_candidates, no una copia paralela")

    mapa = lb.dominios(_TRIO)
    check(mapa == {"mercado": ["mkt_p_home"], "fuerza": ["elo_z_diff"],
                   "forma": ["form_points_ewm_diff"]},
          "cada columna cae en su dominio y el orden es el canonico")
    check(all(d in {b.id for b in ex.BLOQUES_EXPLICATIVOS} for d in mapa),
          "y los dominios son los bloques explicativos de explain, no otros")

    # El catalogo enumera los ocho dominios y no solo los que tienen columnas: un
    # dominio a cero es informacion —dice que esa idea no se esta usando— mientras
    # que uno omitido se confunde con uno que no existe.
    cat = lb.catalogo(_TRIO)
    cuenta = dict(zip(cat["bloque"], cat["n_columnas"]))
    check(len(cat) == len(ex.BLOQUES_EXPLICATIVOS),
          "el catalogo enumera los ocho dominios, tambien los que quedan vacios")
    check(int(cat["n_columnas"].sum()) == 3 and
          all(cuenta[d] == 1 for d in mapa),
          "y cuenta cuantas columnas del conjunto cae en cada uno")
    check("titulo" in cat.columns,
          "con su glosa, que es lo que hace legible un grafico de importancia")

    unas = lb.variables("mercado", "fuerza", columnas=_TRIO)
    check(unas == ["mkt_p_home", "elo_z_diff"],
          "el atajo concatena dominios en el orden pedido")
    check(lb.variables("mercado", "mercado", columnas=_TRIO) == ["mkt_p_home"],
          "y no repite una columna porque se pida su dominio dos veces")
    try:
        lb.variables("astrologia", columnas=_TRIO)
        check(False, "un dominio inexistente se rechaza en lugar de devolver vacio")
    except KeyError as e:
        check("astrologia" in str(e),
              "un dominio inexistente se rechaza en lugar de devolver vacio")


# ===========================================================================
# 4. CACHE Y PLIEGUES
# ===========================================================================
def test_cache() -> None:
    """Lo caro de un ensayo es congelar el preprocesado, y eso se hace una vez.

    El estado de preprocesado no depende del estimador ni de las columnas elegidas:
    lo aprenden las mismas filas y lo definen las mismas reglas. Compartirlo entre
    ensayos con la misma configuracion no cambia ninguna cifra —esto es lo que hay
    que comprobar— y es la diferencia entre una sesion de trabajo y una tarde
    perdida.
    """
    print("\n4. la cache de pliegues y estados")
    lb.olvida()
    cfg = pt.ProtocolConfig()

    fs1 = lb.pliegues(cfg, feat=LIGA, desde=DESDE)
    fs2 = lb.pliegues(cfg, feat=LIGA, desde=DESDE)
    check(fs1 is fs2, "pedir dos veces los mismos pliegues no los recalcula")
    check(len(fs1) > 0 and all(str(f.season) >= DESDE for f in fs1),
          f"`desde` recorta el bloque evaluado ({len(fs1)} pliegues desde {DESDE})")

    # Con la ventana minima de serie, esta liga sintetica solo deja evaluable la
    # ultima temporada de desarrollo, de modo que el recorte no quitaria nada y la
    # comprobacion no diria nada. Se baja la ventana minima —solo aqui, y solo para
    # construir pliegues— para que haya dos temporadas evaluables y el recorte tenga
    # algo que recortar.
    ancho = pt.ProtocolConfig(min_train_matches=200)
    completos = lb.pliegues(ancho, feat=LIGA)
    recortados = lb.pliegues(ancho, feat=LIGA, desde=DESDE)
    check(len(completos) > len(recortados),
          f"sin recorte hay mas pliegues que con el ({len(completos)} y "
          f"{len(recortados)}), y no al reves")
    check({f.season for f in recortados} < {f.season for f in completos},
          "el recorte quita temporadas enteras del bloque evaluado")
    check(all(f.season not in C.HOLDOUT_SEASONS for f in completos),
          "y en ninguno de ellos aparece la temporada de reserva")

    c1 = lb._cache(cfg, "validacion", DESDE, LIGA)
    c2 = lb._cache(cfg, "validacion", DESDE, LIGA)
    check(c1 is c2, "y la cache de estados es la misma para la misma clave")
    check(lb._clave(cfg, "validacion", DESDE) != lb._clave(cfg, "validacion", None),
          "dos recortes distintos son dos claves distintas, no una compartida")

    otra = pt.ProtocolConfig(halflife_days=180)
    check(lb._clave(cfg, "validacion", DESDE) != lb._clave(otra, "validacion", DESDE),
          "y cambiar el protocolo cambia la clave, que es lo que evita mezclar")

    # Hay tres formas de equivocarse con `desde` y las tres tienen que decirse, pero
    # no con el mismo mensaje: quien lee cada aviso tiene que arreglar otra cosa.
    # La primera es un rotulo que no existe ni se le parece.
    try:
        lb.pliegues(cfg, feat=LIGA, desde="2099/00")
        check(False, "un rotulo que no es una temporada de la matriz se rechaza")
    except ValueError as e:
        check("2099/00" in str(e),
              "un rotulo que no es una temporada de la matriz se rechaza")

    # La segunda es la peligrosa, y por eso se comprueba con el protocolo ancho, que
    # es el que deja mas de una temporada evaluable. `2019-20` esta bien escrito para
    # cualquiera que lo lea, y como el guion va antes que la barra en la tabla de
    # caracteres, la comparacion de cadenas no falla: sin la guarda devuelve
    # EXACTAMENTE los mismos pliegues que `2019/20`, de modo que nada en la salida
    # delata la errata. El dia que el rotulo mal escrito caiga en otro sitio, el
    # recorte se movera solo y la cifra cambiara sin que nadie sepa por que.
    try:
        lb.pliegues(ancho, feat=LIGA, desde="2019-20")
        check(False, "un rotulo con el separador cambiado no cuela como recorte")
    except ValueError as e:
        check("2016/17" in str(e),
              "un rotulo con el separador cambiado no cuela, y el aviso ensena el "
              "formato que si existe")

    # La tercera es un rotulo que si existe pero deja el bloque evaluado vacio: la
    # temporada de reserva nunca esta entre los pliegues de validacion. El recorte
    # es correcto y lo que sobra es la expectativa, de modo que el mensaje es otro.
    try:
        lb.pliegues(cfg, feat=LIGA, desde=C.HOLDOUT_SEASONS[0])
        check(False, "un recorte que no deja pliegues se dice, no se devuelve vacio")
    except ValueError as e:
        check("pliegue" in str(e),
              "un recorte que no deja pliegues se dice, no se devuelve vacio")

    lb.olvida()
    check(not lb._folds and not lb._caches,
          "olvidar vacia las dos caches, que es lo unico que hace falta tras "
          "cambiar el parquet")


# ===========================================================================
# 5. EL HOLDOUT
# ===========================================================================
def test_holdout() -> None:
    """La reserva no se abre desde el banco. Ni para evaluar, ni para ajustar.

    Es la guarda mas importante del modulo y la unica cuyo incumplimiento no deja
    rastro: un ensayo sobre 2025/26 devuelve cifras perfectamente plausibles, y la
    memoria defenderia como fuera de muestra un numero que ya se ha mirado. La
    reserva se abre una sola vez, desde `build_models.py --holdout`, con el conjunto
    ya cerrado.
    """
    print("\n5. la reserva no se abre desde el banco")
    try:
        lb.evalua(_TRIO, "logistica", feat=LIGA, evaluar="holdout", verbose=False)
        check(False, "evaluar sobre el holdout esta prohibido")
    except PermissionError as e:
        check(True, "evaluar sobre el holdout esta prohibido")
        check("build_models" in str(e),
              "y el mensaje dice desde donde se abre de verdad")
    try:
        lb.ajusta(_TRIO, "logistica", feat=LIGA, evaluar="holdout")
        check(False, "ajustar sobre el holdout tambien")
    except PermissionError:
        check(True, "ajustar sobre el holdout tambien")

    # La temporada EN CURSO esta cerrada al banco por el mismo motivo de fondo,
    # aunque no lleve sello: cualquier jornada posterior a la sellada se entrena
    # con ella dentro de la ventana, de modo que comparar familias ahi seria
    # elegir mirando el holdout por un camino indirecto.
    for fn, que in ((lb.evalua, "evaluar"), (lb.ajusta, "ajustar")):
        try:
            fn(_TRIO, "logistica", feat=LIGA, evaluar="produccion")
            check(False, f"{que} sobre la temporada en curso esta prohibido")
        except PermissionError as e:
            check("holdout" in str(e),
                  f"{que} sobre la temporada en curso esta prohibido, y el "
                  "mensaje nombra el holdout, que es la razon de fondo")

    # La guarda de arriba es explicita; esta comprueba que ademas el camino normal
    # nunca llega alli, que es lo que la hace innecesaria en el uso corriente.
    e = lb.evalua(_TRIO, "logistica", nombre="sin-reserva", **COMUN)
    check(not set(e.pred["season"]) & set(C.HOLDOUT_SEASONS),
          "y una evaluacion corriente no toca ni una fila de 2025/26")


# ===========================================================================
# 6. EL ENSAYO
# ===========================================================================
def test_ensayo() -> None:
    """Un ensayo medido: probabilidades, sello, y las dos tablas que se leen.

    El sello es la pieza que permite reconocer entre sesiones que una combinacion ya
    se probo aunque se le pusiera otro nombre. Tiene que depender de lo que cambia
    las cifras —familia, columnas, hiperparametros, protocolo, recorte— y NO del
    rotulo, o el registro acumularia el mismo ensayo varias veces y la comparacion
    entre sesiones enganaria.
    """
    print("\n6. el ensayo y su sello")
    e = lb.evalua(_TRIO, "logistica", nombre="trio", **COMUN)

    probs = e.pred[["p_home", "p_draw", "p_away"]].to_numpy()
    check(np.allclose(probs.sum(axis=1), 1.0, atol=1e-9),
          "la terna de probabilidades suma uno en todos los partidos")
    check(float(probs.min()) >= 0.0 and float(probs.max()) <= 1.0,
          "y ninguna cae fuera de [0, 1]")
    check(int(e.metricas["n"]) == len(e.pred) > 0,
          f"el ensayo mide {len(e.pred)} partidos fuera de muestra")
    check(0.0 <= e.accuracy <= 1.0 and e.rps > 0.0,
          "acierto y RPS salen en su escala")
    check(abs((e.roi - e.roi_referencia) - e.ventaja) < 1e-12,
          "la ventaja es exactamente el rendimiento menos el de la referencia")

    fila = e.fila()
    for clave in ("nombre", "familia", "n_variables", "accuracy", "rps", "roi",
                  "roi_referencia", "ventaja", "sello", "variables", "dominios"):
        check(clave in fila, f"la fila registrable trae `{clave}`")
    check(fila["n_variables"] == 3 and fila["variables"] == " + ".join(_TRIO),
          "y guarda el conjunto entero, que es lo que permite retomarlo")
    check(set(fila["dominios"].split(", ")) == {"mercado", "fuerza", "forma"},
          "junto a los dominios que atraviesa")

    tabla = e.tabla()
    check(len(tabla) == e.pred["season"].nunique(),
          "la tabla por temporada trae una fila por temporada evaluada")
    check({"accuracy", "rps"} <= set(tabla.columns),
          "con el acierto y el RPS de cada una")
    check(isinstance(e.resumen(), str) and e.nombre in e.resumen(),
          "y el resumen de una linea nombra el ensayo")

    # El sello: mismo contenido, mismo sello; distinto contenido, distinto sello.
    igual = lb.evalua(_TRIO, "logistica", nombre="otro rotulo", **COMUN)
    check(igual.sello == e.sello,
          "el mismo ensayo con otro rotulo tiene el mismo sello")
    check(np.isclose(igual.accuracy, e.accuracy),
          "y las mismas cifras, que es lo que el sello promete")
    revuelto = lb.evalua(list(reversed(_TRIO)), "logistica", **COMUN)
    check(revuelto.sello == e.sello,
          "el orden en que se escriben las columnas no cambia el ensayo")
    distinto = lb.evalua(_TRIO + ["form_gf_ewm_diff"], "logistica", **COMUN)
    check(distinto.sello != e.sello, "anadir una variable si lo cambia")
    afinado = lb.evalua(_TRIO, "logistica", hiper={"C": 5.0}, **COMUN)
    check(afinado.sello != e.sello, "y cambiar un hiperparametro tambien")
    check(afinado.hiper["C"] == 5.0,
          "el ensayo guarda los hiperparametros efectivos, no los pedidos")

    try:
        lb.evalua([], "logistica", feat=LIGA)
        check(False, "un ensayo sin columnas no es un ensayo")
    except ValueError:
        check(True, "un ensayo sin columnas no es un ensayo")

    # Una columna que no existe en la matriz se anota como ausente en lugar de
    # tumbar el ensayo: es informacion sobre el conjunto, y se publica con el.
    con_fantasma = lb.evalua(_TRIO + ["columna_que_no_existe"], "logistica", **COMUN)
    check("columna_que_no_existe" in con_fantasma.ausentes,
          "una columna que nunca entra se anota entre las ausentes")
    check(np.isclose(con_fantasma.accuracy, e.accuracy),
          "y el ensayo mide lo mismo que el conjunto sin ella")

    # La procedencia se lee sobre lo que entro, no sobre lo que se pidio. Una columna
    # ausente puede llevar un nombre que la tabla de procedencia no conoce —un error de
    # tecleo, sin ir mas lejos—, y preguntar por su dominio detendria el ensayo entero
    # en el momento de resumirlo, que es justo cuando ya no hay nada que salvar.
    fila_fantasma = con_fantasma.fila()
    check(fila_fantasma["dominios"] == e.fila()["dominios"],
          "los dominios de la fila salen de las columnas efectivas, no de las pedidas")
    check(fila_fantasma["n_variables"] == 4 and fila_fantasma["n_ausentes"] == 1,
          "y la fila dice cuantas se pidieron y cuantas de ellas no entraron")

    # Ajustar devuelve modelos entrenados, que es lo que la interpretabilidad pide
    # y lo que la evaluacion tira a proposito.
    ajustes = lb.ajusta(_TRIO, "logistica", feat=LIGA, desde=DESDE, n_pliegues=2)
    check(len(ajustes) == 2, "ajusta devuelve los ultimos pliegues reentrenados")
    check(hasattr(ajustes[-1], "model") or hasattr(ajustes[-1], "modelo"),
          "y cada uno lleva su estimador, no solo sus predicciones")


# ===========================================================================
# 7. COMPARAR Y RONDA
# ===========================================================================
def test_compara_y_ronda() -> None:
    """Ordenar sin decidir. La ronda puntua candidatas y no adopta ninguna.

    Esta es la seccion que expresa el criterio del estudio: el banco MIDE y quien
    lee DECIDE. Una `ronda` que devolviese un ganador —o que iterase adoptando el
    mejor— seria una seleccion automatica disfrazada, y la cifra publicada estaria
    contaminada por las decenas de comparaciones que la produjeron sin que nadie las
    contase.
    """
    print("\n7. comparar y la ronda que no decide")
    e1 = lb.evalua(["mkt_p_home"], "logistica", nombre="uno", **COMUN)
    e2 = lb.evalua(_TRIO, "logistica", nombre="tres", **COMUN)

    t = lb.compara(e1, e2)
    check(len(t) == 2 and set(t["nombre"]) == {"uno", "tres"},
          "comparar pone una fila por ensayo")
    check(list(t["ventaja"]) == sorted(t["ventaja"], reverse=True),
          "y por defecto ordena por ventaja de mayor a menor")
    check(list(lb.compara(e1, e2, ordenar="rps")["rps"]) ==
          sorted(t["rps"]),
          "el RPS, en cambio, ordena al reves: cuanto menos, mejor")
    check(list(lb.compara(e1, e2, ordenar="n_variables")["n_variables"]) == [1, 3],
          "igual que el numero de variables, donde lo escueto va primero")
    try:
        lb.compara(e1, ordenar="carisma")
        check(False, "ordenar por algo que no se mide se rechaza")
    except KeyError:
        check(True, "ordenar por algo que no se mide se rechaza")
    try:
        lb.compara()
        check(False, "comparar nada no es comparar")
    except ValueError:
        check(True, "comparar nada no es comparar")

    r = lb.ronda(["mkt_p_home"], ["elo_z_diff", "form_points_ewm_diff"], "logistica",
                 etiqueta="prueba", feat=LIGA, desde=DESDE, n_boot=20,
                 registrar=False)
    check(list(r["candidata"])[0] == "(base)",
          "la primera fila de la ronda es el conjunto base, sin anadir nada")
    check(float(r.iloc[0]["d_accuracy"]) == 0.0 and
          float(r.iloc[0]["d_ventaja"]) == 0.0,
          "y sus diferencias son cero, que es contra lo que se leen las demas")
    check(set(r["candidata"]) == {"(base)", "elo_z_diff", "form_points_ewm_diff"},
          "hay una fila por candidata y ninguna se queda fuera")
    check(list(r.columns)[:3] == ["candidata", "accuracy", "d_accuracy"],
          "la tabla se abre por lo que se viene a leer")
    check(all(r.iloc[i]["n_variables"] == 2 for i in range(1, len(r))),
          "cada candidata se mide sobre el base mas ella, no sobre las anteriores")
    check("adopta" not in dir(lb.ronda) and isinstance(r, pd.DataFrame),
          "la ronda devuelve una tabla y no un ganador: decide quien la lee")

    try:
        lb.ronda(["mkt_p_home"], ["mkt_p_home"], "logistica", feat=LIGA)
        check(False, "una candidata que ya esta en el base no es candidata")
    except ValueError:
        check(True, "una candidata que ya esta en el base no es candidata")

    # Una candidata que no se puede ajustar es un resultado, no un corte.
    r2 = lb.ronda(["mkt_p_home"], ["elo_z_diff", "no_existe_esta"], "logistica",
                  etiqueta="fallo", feat=LIGA, desde=DESDE, n_boot=20,
                  registrar=False)
    check(len(r2) == 3, "una candidata inexistente se mide como ausente y no corta")

    # `por_familia` recorre las cuatro y anota la que no puede probarse —el refuerzo
    # si falta XGBoost— en lugar de detenerse: que una familia no pudiera probarse es
    # un resultado del trabajo y tiene que quedar escrito.
    pf = lb.por_familia(["mkt_p_home", "elo_z_diff"],
                        familias=("logistica", "refuerzo_xgb"),
                        hiper=None, **COMUN)
    fallidas = {f["familia"] for f in pf.attrs.get("no_aplicables", [])}
    check(len(pf) + len(fallidas) == 2,
          "por_familia da cuenta de las dos familias pedidas, medidas o no")
    check("logistica" in set(pf["familia"]),
          "la logistica, que no depende de nada externo, siempre se mide")
    if fallidas:
        check("refuerzo_xgb" in fallidas,
              "y la familia que no puede instanciarse queda anotada con su motivo")

    # `por_dominio` reparte un conjunto que YA viene recortado. El recorte por
    # regimen lo hace el cuaderno, a la vista, y tiene que llegar hasta aqui: si
    # esta funcion repartiese la matriz entera, el reconocimiento inicial se haria
    # sobre columnas que el modelo publicable no puede usar y ordenaria el trabajo
    # posterior en falso. Que el conjunto entre por un parametro propio y no por
    # `**kw` no es cosmetico: en `evalua`, `columnas` son las variables de UN ensayo
    # y el reenvio chocaria con su primer parametro.
    reparto = lb.por_dominio("logistica", columnas=["mkt_p_home", "elo_z_diff"],
                             hiper=None, **COMUN)
    check(set(reparto["dominio"]) == {"mercado", "fuerza"},
          "por_dominio reparte el conjunto recortado que se le pasa y no la matriz")
    check(all(int(n) == 1 for n in reparto["n_variables"]),
          "y mide cada dominio con sus columnas y con ninguna otra")

    # Un dominio pedido que se queda sin columnas tras el recorte es informacion —esa
    # idea no se esta usando— y se anota en lugar de interrumpir el recorrido.
    parcial = lb.por_dominio("logistica", dominios_pedidos=["mercado", "forma"],
                             columnas=["mkt_p_home"], hiper=None, **COMUN)
    faltos = {f["dominio"] for f in parcial.attrs.get("no_evaluables", [])}
    check(list(parcial["dominio"]) == ["mercado"] and faltos == {"forma"},
          "un dominio sin columnas en el conjunto recortado se anota y no corta")


# ===========================================================================
# 8. REGISTRO
# ===========================================================================
def test_registro() -> None:
    """La memoria del estudio entre sesiones.

    Sin registro, cada sesion empezaria de cero y la tentacion de repetir lo ya
    descartado seria constante. Con registro, lo que importa es que el mismo ensayo
    no se acumule dos veces: si el sello ya esta, la fila se sustituye, porque volver
    a medir la misma combinacion tiene que dar lo mismo y si diese otra cosa lo que
    hay que arreglar no es el registro.
    """
    print("\n8. el registro entre sesiones")
    with tempfile.TemporaryDirectory() as tmp:
        ruta = Path(tmp) / "registro.parquet"
        e = lb.evalua(_TRIO, "logistica", nombre="registrado", **COMUN)

        lb.registra(e, desde=DESDE, ruta=ruta)
        check(ruta.exists(), "registrar escribe el parquet donde se le dice")
        h1 = lb.historial(ruta)
        check(len(h1) == 1 and h1.iloc[0]["nombre"] == "registrado",
              "y el historial lo devuelve")
        check(h1.iloc[0]["desde"] == DESDE and h1.iloc[0]["protocolo"],
              "con el recorte y el protocolo con que se midio, no solo las cifras")
        check(bool(h1.iloc[0]["momento"]),
              "y el momento en que se midio")

        lb.registra(e, desde=DESDE, ruta=ruta)
        check(len(lb.historial(ruta)) == 1,
              "volver a registrar el mismo sello sustituye la fila, no la duplica")

        otro = lb.evalua(_TRIO + ["form_gf_ewm_diff"], "logistica",
                         nombre="cuatro", **COMUN)
        lb.registra(otro, desde=DESDE, ruta=ruta)
        check(len(lb.historial(ruta)) == 2, "un ensayo distinto si anade fila")

        h = lb.historial(ruta, ordenar="ventaja")
        check(list(h["ventaja"]) == sorted(h["ventaja"], reverse=True),
              "el historial se ordena por lo que se le pida")
        check(len(lb.historial(ruta, familia="bosque")) == 0,
              "y se puede filtrar por familia")

        check(lb.conjunto("registrado", ruta) == list(_TRIO),
              "el conjunto de un ensayo se recupera por su nombre")
        try:
            lb.conjunto("no registrado", ruta)
            check(False, "y pedir uno que no esta se dice en lugar de devolver vacio")
        except KeyError:
            check(True, "y pedir uno que no esta se dice en lugar de devolver vacio")

        vacio = lb.historial(Path(tmp) / "no_existe.parquet")
        check(vacio.empty,
              "un registro que aun no existe es un historial vacio, no un error")


# ===========================================================================
def _run() -> None:
    print("=" * 74)
    print("BANCO DE PRUEBAS — laboratorio.py")
    print("=" * 74)
    test_familias()
    test_modelo_con_columnas()
    test_catalogo()
    test_cache()
    test_holdout()
    test_ensayo()
    test_compara_y_ronda()
    test_registro()
    print("\n" + "=" * 74)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for m in FAIL:
        print(f"   - {m}")
    print("=" * 74)


def test_laboratorio() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
