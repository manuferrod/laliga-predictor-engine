"""Validacion offline de la SELECCION HACIA DELANTE (seleccion.py). Sin red.

Este modulo elige las variables del sistema, y una seleccion mal hecha no produce
ningun error: produce una tabla de aspecto impecable con una cifra que no significa
lo que dice. Las tres averias posibles son de ese tipo, invisibles las tres:

  (a) que la busqueda mire el bloque de medicion. La voraz elegiria la variable que
      mas sube la cifra sobre los mismos partidos con los que despues se publica, y
      la cifra publicada seria el maximo de un ruido con cien oportunidades de
      parecer senal. No habria ningun sintoma: la tabla saldria mejor.
  (b) que los dos bloques se solapen o dejen huecos. Un partido que cae en los dos
      es un partido con el que se eligio y se midio; uno que no cae en ninguno es
      informacion tirada sin decirlo.
  (c) que la regla de parada no pare. Una voraz que adopta la mejor candidata del
      paso pase lo que pase termina siempre con `max_vars` variables, y la mitad
      finales entraron por ruido.

Por eso el banco no comprueba que la busqueda "encuentre algo" —eso lo hace
cualquier voraz— sino que se rompa cuando debe: que pedirle el bloque equivocado sea
un error explicito, que la particion sea exacta y que la parada ocurra donde la
tolerancia dice.

La medicion se sustituye por una funcion conocida en las comprobaciones de busqueda.
No es un atajo: `seleccion.py` no mide nada —eso es de `laboratorio.py` y de
`protocol.py`, que tienen sus propios bancos— sino que decide con lo que la medicion
le devuelve. Sustituirla por una regla conocida es lo que permite comprobar la
decision contra una verdad en lugar de contra una corazonada, que es justo lo que un
dato real no deja hacer.

Se comprueba, en este orden:

  1. BLOQUES     seleccion y medicion particionan la validacion, no tocan el
                 arranque ni el holdout, y un corte imposible es un error.
  2. GUARDAS     elegir sobre el bloque de medicion, y medir sobre el de seleccion,
                 se detienen con un error explicito.
  3. BUSQUEDA    la voraz adopta las variables informativas en orden de aportacion,
                 no repite ninguna y respeta el pool.
  4. PARADA      se detiene cuando la mejor mejora no llega a la tolerancia, y con
                 una tolerancia enorme no adopta nada.
  5. AISLAMIENTO ninguna llamada de la busqueda pide un bloque distinto del de
                 seleccion.
  6. TRAZA       cada paso queda anotado, el guardado y la lectura son inversos, y
                 el conjunto viaja con su corte y su protocolo.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import columns as C        # noqa: E402
import laboratorio as lb   # noqa: E402
import protocol as pt      # noqa: E402
import seleccion as sl     # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")


# ---------------------------------------------------------------------------
# la liga sintetica y la medicion de juguete
# ---------------------------------------------------------------------------
_EQUIPOS = [f"eq{i:02d}" for i in range(10)]
_SEASONS = ["2016/17", "2017/18", "2018/19", "2019/20", "2020/21",
            "2021/22", "2022/23", "2023/24", "2024/25", "2025/26"]
_CORTE = "2022/23"


def _liga(semilla: int = 3) -> pd.DataFrame:
    """Diez temporadas, dos partidos por jornada, calendario sin aplazamientos.

    No hace falta un calendario realista: lo unico que este banco le pide a la liga
    es que las temporadas caigan en los bloques que les tocan, de modo que la
    particion se pueda contrastar contra una verdad conocida.
    """
    rng = np.random.default_rng(semilla)
    filas = []
    for temporada in _SEASONS:
        inicio = pd.Timestamp(f"{int(temporada[:4])}-08-20")
        for j in range(20):
            base = inicio + pd.Timedelta(days=7 * j)
            for k in range(0, len(_EQUIPOS), 2):
                a, b = _EQUIPOS[k], _EQUIPOS[(k + 1) % len(_EQUIPOS)]
                p = rng.dirichlet([4.0, 3.0, 3.0])
                filas.append({
                    "match_key": f"{base.date()}-{a}-{b}",
                    "season": temporada, "date": base, "round": j + 1,
                    "home_key": a, "away_key": b,
                    "y_num": int(rng.choice(3, p=p)),
                    "split": ("holdout" if temporada in C.HOLDOUT_SEASONS
                              else "dev"),
                })
    return pd.DataFrame(filas)


# Ganancia declarada de cada variable, en unidades de RPS. Es la verdad contra la
# que se contrasta la busqueda: `buena_1` aporta mucho, `buena_3` aporta justo por
# encima de la tolerancia y `buena_4` justo por debajo, que es donde la regla de
# parada tiene que actuar.
GANANCIA = {
    "buena_1": 0.0120,
    "buena_2": 0.0040,
    "buena_3": 0.0006,
    "buena_4": 0.0001,
}
INERTES = [f"inerte_{i:02d}" for i in range(8)]
POOL = list(GANANCIA) + INERTES
SEMILLA = ("mkt_p_home", "mkt_p_draw", "mkt_p_away")

_LLAMADAS: list[dict] = []


class _EnsayoFalso:
    """Lo que la busqueda lee de un ensayo, y nada mas.

    Deliberadamente NO imita el `Ensayo` del banco al completo: si un dia la
    busqueda empezase a leer algo nuevo, este objeto tiene que romperse en vez de
    devolver algo plausible. Lo que hay aqui es exactamente la superficie que
    `seleccion.Cifras.de` toca, y ni un atributo mas.
    """

    def __init__(self, cols):
        self.columnas = tuple(cols)
        # Aditiva por construccion, de modo que la ganancia de una variable no
        # depende del orden en que entro. Anadir una variable inerte EMPEORA en una
        # milmillonesima: un empate exacto dejaria la parada a merced del orden del
        # pool, y aqui se quiere que pare por la regla y no por como se ordeno.
        ganado = sum(GANANCIA.get(c, 0.0) for c in cols)
        self.rps = 0.2100 - ganado + 1e-9 * len(cols)
        self.accuracy = 0.4500 + ganado
        self.roi = -0.030 + 2.0 * ganado
        self.roi_referencia = -0.045
        self.ventaja = self.roi - self.roi_referencia
        self.ausentes = ()
        self.metricas = {"accuracy": self.accuracy, "rps": self.rps,
                         "logloss": 1.0200 - ganado, "brier": 0.6200 - ganado,
                         "n": 900}
        # Tabla por temporada vacia a proposito: comprueba que `Cifras.de` no se
        # apoya en que exista y que la exigencia de consistencia no se puede aplicar
        # sin ella y por tanto no se aplica —un bloque de una sola temporada no es un
        # caso que haya que tapar—. La exigencia se prueba en la seccion 10, con un
        # ensayo que si trae la tabla.
        self.por_temporada = pd.DataFrame(
            {"accuracy": [], "rps": []},
            index=pd.Index([], name="season"))
        self.economia = {
            "modelo": {"rendimiento": self.roi, "n_apuestas": 300},
            "referencia": {"rendimiento": self.roi_referencia, "acierto": 0.52},
            "diferencia": self.ventaja,
            "por_temporada": pd.DataFrame(),
        }


class _SustituyeEvalua:
    """Cambia `laboratorio.evalua` mientras dure el bloque, y lo repone despues."""

    def __init__(self, fabrica=_EnsayoFalso):
        self._fabrica = fabrica

    def __enter__(self):
        self._real = lb.evalua

        def evalua(columnas, familia="logistica", **kw):
            _LLAMADAS.append({"evaluar": kw.get("evaluar"),
                              "cols": tuple(columnas), "familia": familia,
                              "politica": kw.get("politica")})
            return self._fabrica(columnas)

        lb.evalua = evalua
        _LLAMADAS.clear()
        return _LLAMADAS

    def __exit__(self, *exc):
        lb.evalua = self._real
        return False


def _busca(**kw):
    """La voraz con los valores de trabajo del banco, sin tocar el disco."""
    base = dict(pool=POOL, semilla=SEMILLA, anchura=len(POOL), max_vars=12,
                tolerancia=sl.TOLERANCIA, guardar=False, verbose=False,
                feat=pd.DataFrame(), cfg=pt.ProtocolConfig())
    base.update(kw)
    return sl.adelante(**base)


# ---------------------------------------------------------------------------
# 1. los bloques
# ---------------------------------------------------------------------------
def test_bloques() -> None:
    print("\n1. BLOQUES - elegir y medir ocurren en temporadas distintas")
    liga = _liga()
    cfg = pt.ProtocolConfig(corte_seleccion=_CORTE)

    sel = pt._bloque_evaluable(liga, cfg, "seleccion")
    med = pt._bloque_evaluable(liga, cfg, "medicion")
    val = pt._bloque_evaluable(liga, cfg, "validacion")

    check(bool(((sel & med) == False).all()),  # noqa: E712
          "ningun partido cae a la vez en seleccion y en medicion")
    check(bool(((sel | med) == val).all()),
          "los dos bloques cubren la validacion entera, sin huecos")

    t_sel = set(liga.loc[sel, "season"])
    t_med = set(liga.loc[med, "season"])
    check(t_sel == {"2019/20", "2020/21", "2021/22"},
          f"el bloque de seleccion son las anteriores al corte: {sorted(t_sel)}")
    check(t_med == {"2022/23", "2023/24", "2024/25"},
          f"el bloque de medicion son las del corte en adelante: {sorted(t_med)}")
    check(not ((t_sel | t_med) & set(pt.WARMUP_SEASONS)),
          "el arranque no se evalua en ninguno de los dos")
    check(not ((t_sel | t_med) & set(C.HOLDOUT_SEASONS)),
          "el holdout no se evalua en ninguno de los dos")

    # El corte es un parametro del protocolo y viaja con el: dos cifras obtenidas
    # con cortes distintos no son comparables y la descripcion tiene que decirlo.
    check(_CORTE in cfg.describe(),
          "el corte figura en la descripcion del protocolo")

    try:
        pt.ProtocolConfig(corte_seleccion="2017/18")
        check(False, "un corte dentro del arranque es un error explicito")
    except ValueError as e:
        check("arranque" in str(e),
              "un corte dentro del arranque es un error explicito")

    try:
        pt._bloque_evaluable(liga, cfg, "seleccionn")
        check(False, "un bloque mal escrito es un error explicito")
    except ValueError as e:
        check("desconocido" in str(e), "un bloque mal escrito es un error explicito")

    # Los pliegues heredan la particion: es la comprobacion que de verdad importa,
    # porque es `make_folds` y no la mascara lo que la busqueda acaba recorriendo.
    c2 = pt.ProtocolConfig(corte_seleccion=_CORTE, min_train_matches=40)
    f_sel = pt.make_folds(liga, c2, evaluar="seleccion")
    f_med = pt.make_folds(liga, c2, evaluar="medicion")
    ev_sel = set().union(*[set(f.eval_idx) for f in f_sel]) if f_sel else set()
    ev_med = set().union(*[set(f.eval_idx) for f in f_med]) if f_med else set()
    check(bool(f_sel) and bool(f_med),
          f"las dos mitades producen pliegues ({len(f_sel)} y {len(f_med)})")
    check(not (ev_sel & ev_med),
          "ningun partido se evalua en los pliegues de los dos bloques")
    check(all(f.train_idx.isin(liga.index[liga["date"] < f.fecha_ini]).all()
              for f in f_med),
          "los pliegues de medicion siguen entrenando solo con el pasado")


# ---------------------------------------------------------------------------
# 2. las guardas
# ---------------------------------------------------------------------------
def test_guardas() -> None:
    print("\n2. GUARDAS - el bloque no es un parametro que se pueda equivocar")

    for fn, kw, quien in (
        (sl.adelante, dict(pool=POOL, semilla=SEMILLA, evaluar="medicion",
                           guardar=False, verbose=False), "la busqueda"),
        (sl.criba, dict(cols=POOL, evaluar="medicion", guardar=False), "la criba"),
        (sl.mide, dict(conjunto=POOL[:3], evaluar="seleccion"), "la medicion"),
    ):
        try:
            fn(**kw)
            check(False, f"{quien} rechaza el bloque que no le toca")
        except PermissionError as e:
            check("bloque" in str(e), f"{quien} rechaza el bloque que no le toca")
        except Exception as e:                       # noqa: BLE001
            check(False, f"{quien} rechaza el bloque que no le toca ({e!r})")

    # El holdout no se alcanza ni por accidente: la guarda de mas arriba lo para
    # antes, y si alguien la sorteara, la del banco lo pararia despues.
    try:
        sl.mide(POOL[:3], evaluar="holdout")
        check(False, "el holdout es inalcanzable desde este modulo")
    except PermissionError:
        check(True, "el holdout es inalcanzable desde este modulo")

    try:
        _busca(criterio="beneficio")
        check(False, "un criterio no declarado es un error explicito")
    except ValueError as e:
        check("desconocido" in str(e), "un criterio no declarado es un error explicito")

    check(set(sl.CRITERIOS) == {"rps", "accuracy", "ventaja"}
          and sl.CRITERIOS["rps"] == -1,
          "los criterios estan declarados en un solo sitio, con su sentido")


# ---------------------------------------------------------------------------
# 3. la busqueda
# ---------------------------------------------------------------------------
def test_busqueda() -> None:
    print("\n3. BUSQUEDA - adopta lo que aporta, en el orden en que aporta")

    with _SustituyeEvalua():
        sel = _busca()

    elegidas = [p.elegida for p in sel.pasos if p.adoptada]
    check(elegidas[:3] == ["buena_1", "buena_2", "buena_3"],
          f"adopta las informativas por orden de aportacion: {elegidas}")
    check(set(sel.conjunto) == set(SEMILLA) | {"buena_1", "buena_2", "buena_3"},
          f"el conjunto final es la semilla mas las tres que aportan: "
          f"{sorted(set(sel.conjunto) - set(SEMILLA))}")
    check(len(set(sel.conjunto)) == len(sel.conjunto),
          "ninguna variable entra dos veces")
    check(set(sel.conjunto) <= set(POOL) | set(SEMILLA),
          "no se inventa ninguna variable fuera del pool")
    check(all(p.n_variables == len(p.conjunto) for p in sel.pasos),
          "cada paso anota cuantas variables tenia el conjunto que midio")
    check(sel.semilla == tuple(SEMILLA),
          "la semilla queda registrada tal como se paso")

    # Cada paso mide `conjunto + una candidata` para CADA candidata que queda: es lo
    # que distingue una voraz de un ranking, y su coste declarado.
    n0 = sel.pasos[0].n_probadas
    check(n0 == len(POOL),
          f"el primer paso prueba todas las candidatas del pool ({n0})")
    check(sel.pasos[1].n_probadas == len(POOL) - 1,
          "cada paso siguiente prueba una menos: la adoptada sale del pool")

    # `anchura` es el presupuesto de computo, y tiene que morder.
    with _SustituyeEvalua():
        corto = _busca(anchura=3)
    check(corto.pasos[0].n_probadas == 3,
          "la anchura recorta cuantas candidatas mira cada paso")
    check(set(corto.conjunto) - set(SEMILLA) <= set(POOL[:3]),
          "y la voraz solo puede adoptar de entre las que miro")

    # Sin semilla, el primer paso no tiene contra que compararse y adopta la mejor.
    with _SustituyeEvalua():
        desnuda = _busca(semilla=())
    check(desnuda.pasos[0].elegida == "buena_1" and desnuda.pasos[0].adoptada,
          "sin semilla, el primer paso adopta la mejor candidata absoluta")


# ---------------------------------------------------------------------------
# 4. la parada
# ---------------------------------------------------------------------------
def test_parada() -> None:
    print("\n4. PARADA - dejar de anadir es una decision, no un agotamiento")

    with _SustituyeEvalua():
        sel = _busca()

    check("buena_4" not in sel.conjunto,
          "una candidata que mejora por debajo de la tolerancia no entra")
    ultimo = sel.pasos[-1]
    check(not ultimo.adoptada,
          "el paso que detiene la busqueda se anota, pero no se adopta")
    check(ultimo.mejora < sl.TOLERANCIA,
          f"y se anota cuanto mejoraba: {ultimo.mejora:+.6f}")
    check("tolerancia" in sel.motivo_parada,
          f"el motivo de la parada queda escrito: {sel.motivo_parada!r}")
    check(len(sel.conjunto) < 12,
          "la busqueda para antes de llegar al tope de variables")

    # Con una tolerancia imposible no entra nada: la parada no depende de que las
    # ganancias sean pequenas sino de compararlas con el umbral.
    with _SustituyeEvalua():
        nada = _busca(tolerancia=1.0)
    check(tuple(nada.conjunto) == tuple(SEMILLA),
          "con una tolerancia imposible el conjunto se queda en la semilla")
    check(len(nada.pasos) == 1 and not nada.pasos[0].adoptada,
          "y la busqueda gasta un solo paso en descubrirlo")

    # Con tolerancia negativa entra todo hasta el tope: es la voraz sin regla de
    # parada, y sirve para demostrar que el tope tambien sujeta.
    with _SustituyeEvalua():
        tope = _busca(tolerancia=-1.0, max_vars=len(SEMILLA) + 4)
    check(len(tope.conjunto) == len(SEMILLA) + 4,
          f"sin regla de parada, manda el tope de variables: {len(tope.conjunto)}")
    check("lleno" in tope.motivo_parada,
          f"y el motivo lo dice: {tope.motivo_parada!r}")

    # Un pool agotado antes que el tope es tambien una parada legitima.
    with _SustituyeEvalua():
        agotado = _busca(pool=["buena_1", "buena_2"], anchura=2,
                         tolerancia=-1.0, max_vars=12)
    check("agotado" in agotado.motivo_parada,
          f"agotar el pool para la busqueda y queda dicho: {agotado.motivo_parada!r}")


# ---------------------------------------------------------------------------
# 5. el aislamiento del bloque de medicion
# ---------------------------------------------------------------------------
def test_aislamiento() -> None:
    print("\n5. AISLAMIENTO - la busqueda no mira el bloque que publica")

    with _SustituyeEvalua() as llamadas:
        _busca()
        bloques = {c["evaluar"] for c in llamadas}
        n = len(llamadas)

    check(bloques == {"seleccion"},
          f"las {n} mediciones de la busqueda pidieron solo el bloque de "
          f"seleccion: {sorted(bloques)}")
    check(sl.BLOQUE_SELECCION != sl.BLOQUE_MEDICION,
          "los dos bloques son dos nombres distintos, no un alias")
    check("medicion" not in bloques and "holdout" not in bloques,
          "ni una sola llamada toco medicion ni holdout")

    # La criba comparte la misma propiedad, y por el mismo motivo: ordena candidatas
    # y por tanto es parte de la eleccion.
    with _SustituyeEvalua() as llamadas:
        try:
            sl.criba(POOL, feat=pd.DataFrame(), cfg=pt.ProtocolConfig(),
                     guardar=False)
        except Exception:                            # noqa: BLE001
            pass                                     # `ronda` imprime y agrega
        bloques_criba = {c["evaluar"] for c in llamadas}
    check(bloques_criba <= {"seleccion"},
          f"la criba tambien mide solo sobre seleccion: {sorted(bloques_criba)}")


# ---------------------------------------------------------------------------
# 6. la traza y lo que se guarda
# ---------------------------------------------------------------------------
def test_traza() -> None:
    print("\n6. TRAZA - la busqueda se puede reconstruir sin repetirla")

    with _SustituyeEvalua():
        sel = _busca()

    t = sel.traza
    check(len(t) == len(sel.pasos) and list(t["paso"]) == list(range(1, len(t) + 1)),
          "hay una fila por paso, numeradas en orden")
    for col in ("elegida", "n_probadas", "mejora", "accuracy", "rps", "roi",
                "roi_referencia", "ventaja", "adoptada", "conjunto"):
        if col not in t.columns:
            check(False, f"la traza anota {col}")
            break
    else:
        check(True, "la traza anota que entro, cuanto mejoro y con que cifras")
    check(t["rps"].is_monotonic_decreasing,
          "el RPS del conjunto no empeora paso a paso")
    check(all(isinstance(v, str) and " + " in v or len(v.split(" + ")) >= 1
              for v in t["conjunto"]),
          "cada paso guarda el conjunto entero, no solo la variable que entro")

    # Guardar y leer tienen que ser inversos, y el conjunto tiene que viajar con el
    # corte y el protocolo: sin ellos no es reproducible y `build_models` no puede
    # comprobar que esta consumiendo lo que cree.
    with tempfile.TemporaryDirectory() as tmp:
        raiz = Path(tmp)
        traza_real, conj_real = sl.TRAZA, sl.CONJUNTO
        sl.TRAZA, sl.CONJUNTO = raiz / "traza.parquet", raiz / "conjunto.json"
        try:
            sl.guarda(sel, sello="abc123")
            leido = sl.carga()
            check(tuple(leido["conjunto"]) == tuple(sel.conjunto),
                  "el conjunto se lee tal como se guardo")
            check(leido["corte_seleccion"] == str(sel.corte)
                  and leido["protocolo"] == sel.protocolo,
                  "y viaja con el corte y el protocolo con que se obtuvo")
            check(leido["motivo_parada"] == sel.motivo_parada
                  and leido["n_pasos"] == len(sel.pasos),
                  "junto al motivo de la parada y cuantos pasos costo")
            check(json.loads(sl.CONJUNTO.read_text(encoding="utf-8"))["sello"]
                  == "abc123", "el sello identifica la busqueda que lo produjo")
            leida = sl.traza()
            check(len(leida) == len(t), "la traza vuelve entera del parquet")
            sl.CONJUNTO.unlink()
            try:
                sl.carga()
                check(False, "leer una seleccion que no existe es un error claro")
            except FileNotFoundError as e:
                check("seleccion" in str(e).lower() or "ejecute" in str(e),
                      "leer una seleccion que no existe es un error claro")
        finally:
            sl.TRAZA, sl.CONJUNTO = traza_real, conj_real

    check(sl.SEMILLA_MERCADO and all(c.startswith("mkt_p_")
                                     for c in sl.SEMILLA_MERCADO),
          f"la semilla por defecto es el precio del mercado: "
          f"{list(sl.SEMILLA_MERCADO)}")


# ---------------------------------------------------------------------------
# 7. las mudas: una candidata que la poda descarta en todos los pliegues
# ---------------------------------------------------------------------------
def test_mudas() -> None:
    print("\n7. MUDAS - una columna que no llega al ajuste no puede ganar un paso")

    # `muda` declara una ganancia altisima que NO va a cobrar: la poda por
    # colinealidad la descarta, de modo que el ensayo mide el conjunto anterior otra
    # vez y devuelve sus mismas cifras. Es el empate degenerado que detuvo la primera
    # prueba de `adelante` en el banco real, con `ah_line`.
    def _evalua_con_muda(columnas, familia="logistica", **kw):
        cols = tuple(c for c in columnas if c != "muda")
        e = _EnsayoFalso(cols)
        if len(cols) != len(tuple(columnas)):
            e.ausentes = ("muda",)
            e.columnas = tuple(columnas)
        return e

    real = lb.evalua
    lb.evalua = _evalua_con_muda
    try:
        sel = _busca(pool=["muda"] + POOL)
    finally:
        lb.evalua = real

    check("muda" not in sel.conjunto,
          "la muda no entra en el conjunto elegido")
    check(not any(p.elegida == "muda" for p in sel.pasos),
          "y no gana ningun paso, ni para ser rechazada despues")
    check("muda" not in (sel.motivo_parada or ""),
          f"luego la busqueda no para por su culpa: {sel.motivo_parada!r}")
    # Lo que de verdad se comprueba: la busqueda llego igual a las buenas. Si la muda
    # hubiese ganado el paso 1 con su empate, la parada habria sido inmediata y el
    # conjunto se habria quedado en la semilla.
    check(all(c in sel.conjunto for c in ("buena_1", "buena_2")),
          "y encuentra las variables que si pagan su sitio")


# ---------------------------------------------------------------------------
# 8. el resumen nombra un conjunto: tiene que dar SUS cifras
# ---------------------------------------------------------------------------
def test_resumen() -> None:
    print("\n8. RESUMEN - las cifras son las del conjunto elegido, no las del ultimo")

    with _SustituyeEvalua():
        sel = _busca()

    ult_anotado = sel.pasos[-1]
    adoptados = [p for p in sel.pasos if p.adoptada]
    check(not ult_anotado.adoptada,
          "la busqueda paro anotando el paso que rechazo (es la evidencia)")

    txt = sel.resumen()
    check(f"{adoptados[-1].rps:.5f}" in txt,
          f"el resumen da el RPS del conjunto adoptado ({adoptados[-1].rps:.5f})")
    check(f"{ult_anotado.rps:.5f}" not in txt,
          f"y no el del conjunto que rechazo ({ult_anotado.rps:.5f}), que es mejor "
          f"por construccion y haria parecer al elegido mejor de lo que es")
    check(str(len(sel.conjunto)) in txt,
          "y el tamano que nombra es el del conjunto que devuelve")


# ---------------------------------------------------------------------------
# 9. los argumentos que el banco fija el mismo no se pueden reenviar
# ---------------------------------------------------------------------------
def test_reservados() -> None:
    print("\n9. RESERVADOS - un argumento reenviado por error no pasa por resultado")

    # `criba`, `adelante` y `flotante` reenvian a ciegas en `**kw` lo que no entienden.
    # Sin esta guarda, un `verbose=False` de mas chocaba con el que `_mide_uno` fija,
    # la llamada moria con TypeError una vez por conjunto, y la busqueda lo contaba
    # como «ninguna candidata resulto evaluable»: un error de tecleo disfrazado de
    # medicion. Que falle aqui, una vez y por su nombre.
    for arg in ("verbose", "feat", "evaluar", "registrar"):
        try:
            sl._lote([list(SEMILLA)], "logistica", pt.ProtocolConfig(),
                     kw={arg: False}, feat=pd.DataFrame())
            check(False, f"reenviar {arg} en kw tiene que ser un error")
        except TypeError as e:
            check(arg in str(e),
                  f"reenviar {arg} en kw falla nombrandolo: {str(e)[:60]}...")

    # Y el camino bueno sigue abierto: `criba` tiene su propio `verbose`, que es lo
    # que hay que usar para callar la traza.
    import inspect
    for fn in (sl.criba, sl.adelante, sl.flotante, sl.atras):
        check("verbose" in inspect.signature(fn).parameters,
              f"{fn.__name__} declara su propio verbose")


# ---------------------------------------------------------------------------
# 10. la consistencia: una mejora que es de una temporada sola no compite
# ---------------------------------------------------------------------------
TEMPORADAS = ("2019/20", "2020/21", "2021/22")

# Ganancia de RPS temporada a temporada, en el mismo signo que la traza: positiva
# cuando el RPS baja. `suertuda` gana MAS de media que `constante` y sin embargo
# empeora dos de las tres temporadas; es el caso que la exigencia existe para
# rechazar, y es el que la voraz adoptaria si decidiese solo con la media.
GANANCIA_TEMP = {
    "constante": (0.0010, 0.0010, 0.0010),      # media 0.0010, jackknife +0.0010
    "suertuda": (-0.0004, -0.0002, 0.0051),     # media 0.0015, jackknife -0.0003
    "mediocre": (0.0003, 0.0003, 0.0003),       # media 0.0003, jackknife +0.0003
}
POOL_TEMP = list(GANANCIA_TEMP) + INERTES[:3]


class _EnsayoTemporadas(_EnsayoFalso):
    """Como `_EnsayoFalso`, pero con la tabla por temporada rellena.

    El agregado es la media de las tres temporadas porque las tres traen el mismo
    numero de partidos, que es tambien el caso del bloque real —380 por temporada—.
    Si no lo fuese, la media de temporadas y el agregado no serian el mismo numero y
    la comprobacion de abajo no podria leer una cosa en la otra.
    """

    def __init__(self, cols):
        super().__init__(cols)
        por = {t: sum(GANANCIA_TEMP.get(c, (0.0, 0.0, 0.0))[i]
                      for c in cols)
               for i, t in enumerate(TEMPORADAS)}
        media = sum(por.values()) / len(TEMPORADAS)
        self.rps = 0.2100 - media + 1e-9 * len(cols)
        self.accuracy = 0.4500 + media
        self.roi = -0.030 + 2.0 * media
        self.ventaja = self.roi - self.roi_referencia
        self.metricas = dict(self.metricas, accuracy=self.accuracy, rps=self.rps)
        self.por_temporada = pd.DataFrame(
            {"accuracy": [0.4500 + g for g in por.values()],
             "rps": [0.2100 - g + 1e-9 * len(cols) for g in por.values()]},
            index=pd.Index(list(TEMPORADAS), name="season"))
        self.economia = dict(self.economia, por_temporada=pd.DataFrame({
            "season": list(TEMPORADAS),
            "n_modelo": [100, 100, 100],
            "rendimiento_modelo": [-0.030 + 2.0 * g for g in por.values()],
            "rendimiento_ref": [-0.045, -0.045, -0.045],
        }))


def test_consistencia() -> None:
    print("\n10. CONSISTENCIA - la mejora tiene que sobrevivir a su mejor temporada")

    base = {"cfg": pt.ProtocolConfig(), "feat": pd.DataFrame(), "guardar": False,
            "verbose": False, "pool": POOL_TEMP, "semilla": SEMILLA,
            "anchura": len(POOL_TEMP), "max_vars": 10,
            "tolerancia": sl.TOLERANCIA}

    # Primero, que el caso este bien armado: `suertuda` TIENE que ganar por media,
    # porque si no la comprobacion pasaria sin comprobar nada.
    with _SustituyeEvalua(_EnsayoTemporadas):
        semilla = sl.Cifras.de(_EnsayoTemporadas(list(SEMILLA)))
        medias = {c: sl.Cifras.de(_EnsayoTemporadas(list(SEMILLA) + [c])).rps
                  for c in GANANCIA_TEMP}
    check(medias["suertuda"] < medias["constante"],
          f"el caso esta armado: suertuda gana por media ({medias['suertuda']:.5f} "
          f"frente a {medias['constante']:.5f})")

    jacks = {}
    for c in GANANCIA_TEMP:
        cif = sl.Cifras.de(_EnsayoTemporadas(list(SEMILLA) + [c]))
        jacks[c] = sl._jackknife(cif, sl._por_temporada(semilla, "rps"),
                                 "rps", sl.CRITERIOS["rps"])
    check(jacks["suertuda"] < 0 < jacks["constante"],
          f"el jackknife separa los dos casos ({jacks['suertuda']:+.6f} frente a "
          f"{jacks['constante']:+.6f})")

    with _SustituyeEvalua(_EnsayoTemporadas):
        sel = sl.adelante(**base)

    check("suertuda" not in sel.conjunto,
          f"la que le debe la media a una temporada no entra: {sel.conjunto}")
    check("constante" in sel.conjunto,
          "la que mejora en las tres si entra")
    primero = next(p for p in sel.pasos if p.adoptada)
    check(primero.elegida == "constante",
          f"el primer paso adopta la consistente, no la de mejor media: "
          f"{primero.elegida}")
    check(primero.n_inconsistentes == 1 and primero.jackknife > 0,
          f"la traza anota el descarte y el jackknife de la elegida "
          f"({primero.n_inconsistentes}, {primero.jackknife:+.6f})")

    # Y la exigencia no se come lo que si paga: `mediocre` gana tres veces menos que
    # `constante` pero gana en las tres temporadas y esta por encima de la tolerancia,
    # asi que entra detras.
    check("mediocre" in sel.conjunto,
          f"una ganancia pequena pero de las tres temporadas entra: {sel.conjunto}")
    check(not any(c.startswith("inerte") for c in sel.conjunto),
          f"ninguna inerte entra: {sel.conjunto}")

    # Sin tabla por temporada la exigencia no se puede aplicar, y entonces no cambia
    # nada: es lo que hace que un bloque de una sola temporada siga funcionando.
    with _SustituyeEvalua(_EnsayoFalso):
        libre = sl.adelante(**dict(base, pool=POOL_TEMP))
    check(all(p.n_inconsistentes == 0 for p in libre.pasos),
          "sin tabla por temporada no se descarta a nadie por consistencia")


# ---------------------------------------------------------------------------
# 11. el criterio manda en las tres cosas que decidia el RPS
# ---------------------------------------------------------------------------
def test_criterio() -> None:
    print("\n11. CRITERIO - la tolerancia, el orden y la liquidacion lo siguen")

    # La tolerancia es la del criterio, no la del RPS. Que son escalas distintas se
    # ve en el orden de magnitud: la del acierto es doce veces la del RPS.
    check(set(sl.TOLERANCIAS) == set(sl.CRITERIOS),
          f"hay una tolerancia medida por criterio declarado: "
          f"{sorted(sl.TOLERANCIAS)}")
    check(sl.TOLERANCIAS["rps"] == sl.TOLERANCIA,
          "la del RPS sigue siendo la constante historica, sin tocar")
    check(sl.TOLERANCIAS["accuracy"] > 10 * sl.TOLERANCIAS["rps"],
          f"la del acierto es de otro orden: {sl.TOLERANCIAS['accuracy']:.6f} "
          f"contra {sl.TOLERANCIAS['rps']:.6f}")
    check(sl.TOLERANCIAS["ventaja"] > sl.TOLERANCIAS["accuracy"],
          f"y la de la ventaja, la mayor de las tres: "
          f"{sl.TOLERANCIAS['ventaja']:.6f}")

    # Y baja con la raiz del tamano del bloque, que es lo que la mantiene valida
    # cuando la eleccion se ensancha de 1140 partidos a 3040.
    uno = sl.tolerancia_de("accuracy", sl.N_TOLERANCIA)
    cuatro = sl.tolerancia_de("accuracy", 4 * sl.N_TOLERANCIA)
    check(abs(uno - sl.TOLERANCIAS["accuracy"]) < 1e-12,
          "en la ventana en la que se midio devuelve la cifra medida")
    check(abs(cuatro - uno / 2.0) < 1e-12,
          f"y con cuatro veces los partidos, la mitad: {cuatro:.6f}")
    check(sl.tolerancia_de("accuracy") == sl.TOLERANCIAS["accuracy"],
          "sin tamano no escala, en lugar de inventarse uno")
    try:
        sl.tolerancia_de("roi")
        check(False, "un criterio sin tolerancia medida tendria que fallar")
    except KeyError:
        check(True, "un criterio sin tolerancia medida falla y no cae al RPS")

    # El orden del pool sigue al criterio, con el signo de `CRITERIOS`. Es lo que
    # decide que candidatas se llegan a mirar cuando `anchura > 0`.
    tabla = pd.DataFrame({"candidata": ["a", "b", "c"],
                          "rps": [0.10, 0.20, 0.30],
                          "accuracy": [0.50, 0.55, 0.60],
                          "ventaja": [0.01, 0.03, 0.02]})
    check(sl._ordena(tabla, "rps") == ["a", "b", "c"],
          "el RPS se minimiza: primero el menor")
    check(sl._ordena(tabla, "accuracy") == ["c", "b", "a"],
          "el acierto se maximiza: primero el mayor, que es el orden inverso")
    check(sl._ordena(tabla, "ventaja") == ["b", "c", "a"],
          "y la ventaja da un tercer orden, distinto de los dos")
    try:
        sl._ordena(tabla, "logloss")
        check(False, "ordenar por un criterio no declarado tendria que fallar")
    except KeyError:
        check(True, "ordenar por un criterio no declarado falla")

    # La busqueda liquida con la unidad plana del trabajo y no con la politica de
    # valor del banco: con politica de valor el subconjunto apostado cambia con cada
    # columna que entra y dos conjuntos no se compararian sobre los mismos partidos.
    with _SustituyeEvalua() as llamadas:
        _busca()
        politicas = {c["politica"] for c in llamadas}
    check(politicas == {sl.POLITICA_SELECCION},
          f"las {len(llamadas)} mediciones liquidaron con "
          f"{sl.POLITICA_SELECCION!r}: {sorted(map(str, politicas))}")
    check(sl.POLITICA_SELECCION == "favorito",
          "que es una unidad al signo del modelo en todos los partidos")

    # Y quien llame puede pasar otra, que gana sobre la de la busqueda.
    with _SustituyeEvalua() as llamadas:
        _busca(politica="valor")
        otras = {c["politica"] for c in llamadas}
    check(otras == {"valor"},
          f"una politica explicita manda sobre la de la busqueda: {sorted(otras)}")


# ---------------------------------------------------------------------------
def main() -> int:
    test_bloques()
    test_guardas()
    test_busqueda()
    test_parada()
    test_aislamiento()
    test_traza()
    test_mudas()
    test_resumen()
    test_reservados()
    test_consistencia()
    test_criterio()
    print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
    if FAIL:
        print("FALLOS:")
        for f in FAIL:
            print("  -", f)
    else:
        print("Todo correcto.")
    return 1 if FAIL else 0


def test_seleccion() -> None:
    """Punto de entrada para `pytest` (ver test_scoring_gates.py)."""
    assert main() == 0, FAIL


if __name__ == "__main__":
    raise SystemExit(main())
