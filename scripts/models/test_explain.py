"""Banco de pruebas de explain.py: la interpretabilidad, sin red ni artefactos.

Los tres relatos de importancia se comprueban contra propiedades que se pueden
escribir a mano. El reparto de Shapley admite la comprobacion mas fuerte de
todas: sobre un modelo ADITIVO en los bloques, la aportacion de cada bloque tiene
que ser exactamente su termino, y sobre un modelo con una interaccion simetrica,
exactamente la mitad de la interaccion para cada uno de los dos. Un banco que
solo comprobara que la suma cierra dejaria pasar cualquier reparto arbitrario que
sumase lo mismo.

La tabla de procedencia se sustituye por un diccionario sintetico, de modo que el
banco no depende de que la cadena de features se pueda importar ni de que sus
artefactos existan.

Secciones
---------
1. BLOQUES        taxonomia, herencia de los indicadores de faltante y mapa
2. COEFICIENTES   reordenacion por clase, magnitud y agregacion por bloque
3. PERMUTACION    deterioro medido sobre la evaluacion, con bloques ignorados
4. SHAPLEY        reparto exacto: aditividad, simetria, nulidad y eficiencia
5. LOCAL          descomposicion de un partido y la frase que se publica
6. CONCORDANCIA   comparacion de los tres ordenes
7. GUARDAS        lo que el modulo se niega a hacer
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import explain as ex  # noqa: E402
import protocol as pt  # noqa: E402

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
# Procedencia sintetica: nombre de columna -> bloque analitico de lineage. Se
# inyecta en la cache del modulo para que el banco no arrastre la cadena de
# features entera.
#
# Los nombres son ETIQUETAS SINTETICAS, no la nomenclatura de la matriz real, y
# conviene decirlo porque varias de ellas ya no existen: el catalogo emite hoy la
# mayoria de los atributos de equipo como diferencia, de modo que `home_form_gd_5`
# o `home_relative_perf` describen una matriz anterior a la curacion. Aqui eso no
# es un fallo: `explain.bloque_de` resuelve por la TABLA de procedencia y no por
# patrones sobre el nombre, asi que la cadena concreta es indiferente y lo unico
# que el banco necesita es que la tabla y la matriz sintetica usen la misma. Lo
# que si es real —y es lo que se esta comprobando— son los BLOQUES analiticos de
# la derecha, que tienen que estar todos asignados a un bloque explicativo.
_LINEAGE = {
    "mkt_p_home": "cuotas", "mkt_overround_open": "estructura_precio",
    "close_move_fav": "movimiento_linea",
    "home_elo": "elo", "squad_value_diff": "valor_plantilla",
    "home_strength_attack": "fuerza_latente", "strength_p_home": "modelo_goles",
    "home_form_gd_5": "forma", "away_form_gd_5": "forma",
    "home_relative_perf": "psicologicas_mercado",
    "home_h2h_gd_ewm": "head_to_head",
    "home_prev_position": "clasificacion", "stakes_none_match": "trascendencia",
    "home_fat_rest_days": "fatiga",
    "home_out_value": "bajas", "home_n_missing": "bajas",
    "kickoff_hour": "contexto_calendario", "away_travel_km": "contexto_geografia",
}

_COLS = list(_LINEAGE)

# Columnas a las que apuntan los indicadores de faltante del preprocesado. Tienen
# que constar en la procedencia para que cada indicador herede el bloque de la
# variable cuya ausencia declara, pero no forman parte de la matriz sintetica:
# ninguna prueba las usa como entrada del modelo.
#
# Estas SI son las columnas reales, y no etiquetas: son las que `preprocess`
# nombra en `FLAG_BLOCKS`, de modo que renombrar una fuente y no tocar aqui haria
# que `bloque_de` no pudiera resolver el indicador. Por eso hay ademas una
# comprobacion —abajo, en las pruebas de taxonomia— de que esta tabla cubre todas
# las fuentes declaradas por el preprocesado.
_LINEAGE_REFERIDAS = {
    "odds_close_avg_h": "cuotas",
    "out_value_pct_diff": "bajas",
    "form_points_ewm_diff": "forma",
    "h2h_gd_ewm_diff": "head_to_head",
    "age_diff": "valor_plantilla",
    "strength_attack_diff": "fuerza_latente",
}


def _instala_lineage() -> None:
    ex._lineage_cache = {**_LINEAGE, **_LINEAGE_REFERIDAS}


try:                                                        # pragma: no cover
    import pytest

    @pytest.fixture(autouse=True)
    def _procedencia_aislada():
        """Devuelve la cache de procedencia a su estado real tras CADA prueba.

        `_instala_lineage` escribe en una variable de modulo de `explain`, que
        pertenece al proceso y no a este banco. Bajo `pytest` cada funcion de aqui
        se recolecta por separado, de modo que sin esta restauracion la ultima que
        se ejecute deja la tabla de juguete puesta para el resto de la sesion: el
        banco de `publish_web`, que pide el bloque de columnas REALES, recibiria
        una tabla que solo contiene las sinteticas y fallaria por una causa que no
        esta en el codigo que prueba. Poner la cache a `None` no instala nada: la
        reconstruye `explain` a partir de `lineage.feature_lineage()` cuando
        alguien vuelve a preguntarle.
        """
        yield
        ex._lineage_cache = None
except ImportError:                                         # pragma: no cover
    pass                    # el banco tambien corre como guion suelto, sin pytest


class _Modelo:
    """Estimador de juguete: recibe la matriz y devuelve la terna que se le diga.

    `fn` toma el marco y devuelve una matriz (n, 3) que sume uno por filas. Al
    declarar `classes_` en el orden canonico, el protocolo no reordena nada y lo
    que se mide es exactamente lo que la funcion calcula.
    """

    def __init__(self, fn):
        self.fn = fn
        self.classes_ = np.array([0, 1, 2])

    def fit(self, X, y, sample_weight=None):
        return self

    def predict_proba(self, X):
        return np.asarray(self.fn(X), dtype="float64")


def _aditivo(X) -> np.ndarray:
    """p = (1/3 + a·mercado + b·forma, 1/3 − a·mercado, 1/3 − b·forma).

    Es aditivo en los bloques por construccion: el termino de mercado y el de
    forma entran por separado y ninguno interviene en el del otro. La suma por
    filas vale uno para cualquier valor de las variables.
    """
    m = 0.10 * X["mkt_p_home"].to_numpy(dtype="float64")
    f = 0.05 * X["home_form_gd_5"].to_numpy(dtype="float64")
    return np.column_stack([1 / 3 + m + f, 1 / 3 - m, 1 / 3 - f])


def _interaccion(X) -> np.ndarray:
    """p = (1/3 + c·mercado·forma, 1/3 − c·mercado·forma, 1/3).

    Ningun bloque hace nada por si solo y juntos mueven la probabilidad: es el
    caso en que el reparto no es obvio y Shapley tiene que decidirlo.
    """
    t = 0.10 * (X["mkt_p_home"].to_numpy(dtype="float64")
                * X["home_form_gd_5"].to_numpy(dtype="float64"))
    return np.column_stack([1 / 3 + t, 1 / 3 - t, np.full(len(X), 1 / 3)])


def _solo_mercado(X) -> np.ndarray:
    """Terna que depende UNICAMENTE del precio: los demas bloques son mudos."""
    z = 2.0 * X["mkt_p_home"].to_numpy(dtype="float64")
    E = np.exp(np.column_stack([z, np.zeros(len(X)), -z]))
    return E / E.sum(axis=1, keepdims=True)


def _matriz(n: int, seed: int = 3) -> pd.DataFrame:
    """Matriz sintetica con todas las columnas declaradas en la procedencia."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({c: rng.normal(size=n) for c in _COLS})


def _pliegue(modelo, Xtr: pd.DataFrame, Xev: pd.DataFrame,
             yev: np.ndarray, idx: int = 0) -> pt.PliegueAjustado:
    """Un `PliegueAjustado` sintetico con el contrato que publica el protocolo."""
    ev_marco = Xev.copy()
    ev_marco["y_num"] = np.asarray(yev, dtype="float64")
    fold = pt.Fold(idx=idx, season="2019/20", jornada=idx + 1,
                   fecha_ini=pd.Timestamp("2019-09-01"),
                   fecha_fin=pd.Timestamp("2019-09-02"),
                   train_idx=Xtr.index, eval_idx=Xev.index)
    ctx_tr = pt.Contexto(fechas=pd.Series(
        pd.to_datetime("2019-08-01") + pd.to_timedelta(np.arange(len(Xtr)), "D")))
    ctx_ev = pt.Contexto(fechas=pd.Series(
        pd.to_datetime("2019-09-01") + pd.to_timedelta(np.arange(len(Xev)), "D")))
    return pt.PliegueAjustado(fold=fold, modelo=modelo, estado=None,
                              tr=Xtr, ev=ev_marco, Xtr=Xtr, Xev=Xev,
                              ytr=pd.Series(np.zeros(len(Xtr), dtype="int64")),
                              ctx_tr=ctx_tr, ctx_ev=ctx_ev)


# ===========================================================================
# 1. BLOQUES
# ===========================================================================
def test_1_bloques() -> None:
    print("\n1. BLOQUES EXPLICATIVOS")
    _instala_lineage()

    ids = [b.id for b in ex.BLOQUES_EXPLICATIVOS]
    check(len(ids) == len(set(ids)), "los bloques explicativos no se repiten")
    origenes = [o for b in ex.BLOQUES_EXPLICATIVOS for o in b.bloques_lineage]
    check(len(origenes) == len(set(origenes)),
          "ningun bloque analitico se asigna a dos bloques explicativos")

    # La taxonomia tiene que cubrir TODOS los bloques que emite lineage: un bloque
    # sin destino seria una familia entera de columnas que la explicacion ignora.
    check(set(_LINEAGE.values()) <= set(origenes),
          "la taxonomia cubre todos los bloques analiticos de la procedencia")

    # Y la procedencia sintetica tiene que cubrir todas las fuentes que el
    # preprocesado declara para sus indicadores de faltante. Sin esta linea, un
    # renombrado en `FLAG_BLOCKS` —como el que trajo la curacion del catalogo, que
    # llevo tres de las seis fuentes de la lectura del local a la diferencia— no
    # fallaria aqui sino en la primera prueba que pidiera el bloque de un flag, y
    # el mensaje hablaria de una columna que no aparece en la tabla en lugar de
    # decir que la tabla se ha quedado vieja.
    import preprocess as _pp
    _huerfanas = sorted(set(_pp.FLAG_BLOCKS.values()) - set(_LINEAGE_REFERIDAS))
    check(not _huerfanas,
          f"la procedencia sintetica cubre las fuentes de todos los indicadores "
          f"{'(faltan: ' + ', '.join(_huerfanas) + ')' if _huerfanas else ''}")

    check(ex.bloque_de("mkt_p_home") == "mercado",
          "la cuota se explica como mercado")
    check(ex.bloque_de("close_move_fav") == "mercado",
          "y el movimiento de la linea tambien, aunque lineage los separe")
    check(ex.bloque_de("home_elo") == "fuerza"
          and ex.bloque_de("strength_p_home") == "fuerza",
          "el Elo y el modelo de goles caen juntos en fuerza")
    check(ex.bloque_de("home_relative_perf") == "forma",
          "el rendimiento relativo al precio es forma, no mercado: mide al equipo")

    # Los indicadores de faltante heredan el bloque de la columna cuya ausencia
    # declaran, de modo que "no hay parte medico" y "el parte medico dice esto"
    # se leen en la misma barra.
    check(ex.bloque_de("flag_no_absences") == "bajas",
          "el indicador de bajas no recogidas se explica dentro de bajas")
    check(ex.bloque_de("flag_no_closing_odds") == "mercado",
          "el de cuota de cierre ausente, dentro de mercado")
    check(ex.bloque_de("flag_coldstart") == "forma",
          "y el de arranque en frio, dentro de forma")

    mapa = ex.mapa_bloques(_COLS)
    check(list(mapa) == [b for b in ex._ORDEN_BLOQUES if b in mapa],
          "el mapa respeta el orden canonico de los bloques")
    check(sum(len(v) for v in mapa.values()) == len(_COLS),
          "y reparte todas las columnas sin perder ninguna")
    check(sorted(mapa["mercado"]) == ["close_move_fav", "mkt_overround_open",
                                      "mkt_p_home"],
          "el bloque de mercado agrupa cuota, estructura y movimiento")

    # Un bloque sin columnas no aparece: una casilla a cero se leeria como "no
    # aporta" cuando lo cierto es "la poda lo ha retirado".
    parcial = ex.mapa_bloques(["mkt_p_home", "home_form_gd_5"])
    check(set(parcial) == {"mercado", "forma"},
          "un bloque sin columnas no aparece en el mapa")

    reg = ex.block_registry(_COLS)
    check(len(reg) == len(ex.BLOQUES_EXPLICATIVOS)
          and int(reg.n_columnas.sum()) == len(_COLS),
          "el registro publica un bloque por fila con su recuento de columnas")
    check(reg.nota.str.len().min() > 40,
          "y cada bloque lleva una glosa que el lector puede citar")


# ===========================================================================
# 2. COEFICIENTES
# ===========================================================================
class _Lineal:
    def __init__(self, coef, clases):
        self.coef_ = np.asarray(coef, dtype="float64")
        self.classes_ = np.asarray(clases)


def test_2_coeficientes() -> None:
    print("\n2. COEFICIENTES ESTANDARIZADOS")
    _instala_lineage()

    nombres = ["mkt_p_home", "home_form_gd_5", "home_elo"]
    # `classes_` en orden 2, 0, 1: el estimador no tiene por que devolver H, D, A.
    modelo = _Lineal([[0.9, 0.0, 0.0],       # fila de la clase 2 (visitante)
                      [0.1, 0.4, 0.0],       # fila de la clase 0 (local)
                      [0.0, 0.0, 0.2]],      # fila de la clase 1 (empate)
                     [2, 0, 1])
    tab = ex.linear_coefficients(modelo, nombres)
    check(list(tab.columns) == ["home", "draw", "away", "magnitud"],
          "la tabla publica las tres clases en el orden canonico")
    check(_cerca(tab.loc["mkt_p_home", "away"], 0.9)
          and _cerca(tab.loc["mkt_p_home", "home"], 0.1),
          "y reordena las filas segun classes_, no segun como vinieran")
    check(list(tab.index) == ["mkt_p_home", "home_form_gd_5", "home_elo"],
          "las variables se ordenan por magnitud decreciente")
    check(_cerca(tab["magnitud"].to_numpy(), [0.9, 0.4, 0.2]),
          "y la magnitud es el maximo valor absoluto entre clases")

    # Una ventana sin empates deja al estimador con dos clases: la tercera no es
    # un error, es un coeficiente nulo que hay que publicar como tal.
    corto = _Lineal([[0.5, 0.0, 0.0], [-0.5, 0.0, 0.0]], [0, 2])
    tab2 = ex.linear_coefficients(corto, nombres)
    check(_cerca(tab2["draw"].to_numpy(), 0.0),
          "una clase ausente de la ventana recibe coeficiente cero, no un hueco")

    porbloque = ex.coefficients_by_block(tab)
    check(set(porbloque.bloque) == {"mercado", "forma", "fuerza"},
          "la agregacion reparte las variables entre sus bloques")
    check(_cerca(float(porbloque.loc[porbloque.bloque == "mercado", "suma_abs"].iloc[0]), 0.9),
          "la suma de magnitudes del bloque es la suma de las de sus columnas")
    check(porbloque.iloc[0].bloque == "mercado",
          "y la tabla se ordena por peso lineal total")
    check(str(porbloque.loc[porbloque.bloque == "forma",
                            "variable_dominante"].iloc[0]) == "home_form_gd_5",
          "cada bloque declara cual es su variable mas fuerte")

    # Suma y maximo miden cosas distintas: un bloque numeroso gana la primera sin
    # tener ninguna variable fuerte. El banco lo fija con un caso explicito.
    muchos = pd.DataFrame({"home": [0.1, 0.1, 0.1, 0.5],
                           "draw": 0.0, "away": 0.0,
                           "magnitud": [0.1, 0.1, 0.1, 0.5]},
                          index=["mkt_p_home", "mkt_overround_open",
                                 "close_move_fav", "home_form_gd_5"])
    agg = ex.coefficients_by_block(muchos).set_index("bloque")
    check(_cerca(agg.loc["mercado", "suma_abs"], 0.3)
          and _cerca(agg.loc["forma", "suma_abs"], 0.5),
          "por suma gana el bloque con la variable fuerte, no el numeroso")
    check(_cerca(agg.loc["mercado", "max_abs"], 0.1)
          and _cerca(agg.loc["forma", "max_abs"], 0.5),
          "y por maximo la distancia entre ambos es aun mayor")


# ===========================================================================
# 3. PERMUTACION
# ===========================================================================
def test_3_permutacion() -> None:
    print("\n3. IMPORTANCIA POR PERMUTACION")
    _instala_lineage()

    Xtr = _matriz(400, seed=11)
    Xev = _matriz(120, seed=12)
    # El objetivo depende SOLO del precio, y el modelo lo sabe. Es el caso de
    # laboratorio en el que la respuesta correcta se conoce de antemano.
    m = Xev["mkt_p_home"].to_numpy()
    yev = np.where(m > 0.3, 0, np.where(m < -0.3, 2, 1))
    aj = _pliegue(_Modelo(_solo_mercado), Xtr, Xev, yev)

    imp = ex.permutation_importance_blocks([aj], n_repeticiones=3, seed=5)
    check(imp.attrs["metrica"] == "rps" and imp.attrs["n_filas"] == 120,
          "la tabla declara la perdida usada y sobre cuantos partidos se midio")
    check(imp.iloc[0].grupo == "mercado",
          "romper el precio es lo que mas deteriora el pronostico")
    check(float(imp.iloc[0].importancia) > 0.05,
          "y el deterioro es grande, no marginal")

    otros = imp[imp.grupo != "mercado"]
    check(_cerca(otros.importancia.to_numpy(), 0.0),
          "romper un bloque que el modelo no mira no cambia NADA: importancia cero exacta")
    check(_cerca(otros.ee.to_numpy(), 0.0),
          "y su error entre repeticiones es tambien exactamente cero")

    # La perdida de partida es una sola cifra compartida por todas las filas: si
    # cada grupo tuviese la suya, las importancias no serian comparables.
    check(imp.perdida_base.nunique() == 1,
          "todas las importancias se miden contra la misma perdida de partida")
    check(_cerca(imp.perdida_base.iloc[0],
                 imp.perdida_rota.iloc[0] - imp.importancia.iloc[0]),
          "y la importancia es exactamente la diferencia entre las dos perdidas")

    # El reemplazo es POR FILAS: dos columnas perfectamente ligadas en la ventana
    # tienen que seguir ligadas despues de permutar, o el modelo estaria opinando
    # sobre partidos imposibles.
    Xtr2, Xev2 = _matriz(300, seed=21), _matriz(60, seed=22)
    for X in (Xtr2, Xev2):
        X["mkt_overround_open"] = X["mkt_p_home"]
    visto = {"max_dif": 0.0}

    def _espia(X):
        d = float(np.abs(X["mkt_p_home"] - X["mkt_overround_open"]).max())
        visto["max_dif"] = max(visto["max_dif"], d)
        return _solo_mercado(X)

    aj2 = _pliegue(_Modelo(_espia), Xtr2, Xev2, np.zeros(60, dtype=int))
    ex.permutation_importance_blocks([aj2], n_repeticiones=2, seed=5)
    check(_cerca(visto["max_dif"], 0.0),
          "el bloque se sustituye entero desde la misma fila: no se rompe su correlacion interna")

    # Por columna, en cambio, la misma senal repartida entre dos copias identicas
    # da importancia nula: es la redundancia, y hay que poder verla.
    porcol = ex.permutation_importance(
        [aj2], columnas=["mkt_p_home", "mkt_overround_open"],
        n_repeticiones=2, seed=5)
    check(len(porcol) == 2 and (porcol.n_columnas == 1).all(),
          "la lectura por columna trata cada variable como su propio grupo")

    columnas = ex.permutation_importance([aj], columnas=["home_elo"],
                                         n_repeticiones=2, seed=5)
    check(_cerca(float(columnas.importancia.iloc[0]), 0.0),
          "y una columna que el modelo ignora tampoco deteriora nada")

    # Con log-loss el orden se mantiene: la conclusion no depende de la metrica.
    imp_ll = ex.permutation_importance_blocks([aj], metrica="logloss",
                                              n_repeticiones=2, seed=5)
    check(imp_ll.iloc[0].grupo == "mercado",
          "cambiar de perdida no cambia que el precio sea lo que importa")

    # Dos pliegues agrupan sus filas en una sola cifra, no en dos.
    dos = ex.permutation_importance_blocks([aj, aj], n_repeticiones=2, seed=5)
    check(dos.attrs["n_filas"] == 240 and dos.attrs["n_pliegues"] == 2,
          "varios pliegues se agrupan en una unica perdida sobre todas sus filas")


# ===========================================================================
# 4. SHAPLEY
# ===========================================================================
def test_4_shapley() -> None:
    print("\n4. VALORES DE SHAPLEY EXACTOS")
    _instala_lineage()

    pesos = ex._pesos_shapley(3)
    check(_cerca(pesos, [1 / 3, 1 / 6, 1 / 3]),
          "los pesos de coalicion son s!(B-s-1)!/B!")
    for B in (2, 3, 5, 8):
        w = ex._pesos_shapley(B)
        total = sum(w[s] * math.comb(B - 1, s) for s in range(B))
        check(_cerca(total, 1.0),
              f"y con B={B} los pesos de las coaliciones sin el bloque suman uno")

    X = _matriz(40, seed=31)
    mapa = {"mercado": ["mkt_p_home"], "forma": ["home_form_gd_5"],
            "fuerza": ["home_elo"]}
    base = pd.Series(0.0, index=X.columns)

    # --- caso aditivo: el reparto correcto se conoce en cerrado ---------------
    sh = ex.shapley_blocks(_Modelo(_aditivo), X, None, base, mapa)
    check(sh["bloques"] == ["mercado", "forma", "fuerza"],
          "el reparto conserva el orden canonico de los bloques")
    check(sh["phi"].shape == (40, 3, 3),
          "y publica una aportacion por partido, bloque y clase")
    check(_cerca(sh["base"], np.full((40, 3), 1 / 3)),
          "la coalicion vacia es el pronostico del partido de referencia")

    m = 0.10 * X["mkt_p_home"].to_numpy()
    f = 0.05 * X["home_form_gd_5"].to_numpy()
    check(_cerca(sh["phi"][:, 0, :], np.column_stack([m, -m, np.zeros(40)])),
          "en un modelo aditivo el mercado aporta exactamente su propio termino")
    check(_cerca(sh["phi"][:, 1, :], np.column_stack([f, np.zeros(40), -f])),
          "y la forma exactamente el suyo, sin contaminarse el uno del otro")
    check(_cerca(sh["phi"][:, 2, :], 0.0),
          "un bloque que el modelo no usa recibe cero: es la propiedad de nulidad")
    check(_cerca(ex.verifica_eficiencia(sh), 0.0, tol=1e-12),
          "y las aportaciones suman el pronostico menos la referencia")

    # --- caso con interaccion: el reparto lo decide el metodo ----------------
    sh2 = ex.shapley_blocks(_Modelo(_interaccion), X, None, base, mapa)
    t = 0.10 * (X["mkt_p_home"].to_numpy() * X["home_form_gd_5"].to_numpy())
    check(_cerca(sh2["phi"][:, 0, 0], t / 2) and _cerca(sh2["phi"][:, 1, 0], t / 2),
          "una interaccion simetrica se reparte a partes iguales entre los dos bloques")
    check(_cerca(sh2["phi"][:, 0, :], sh2["phi"][:, 1, :]),
          "y los dos bloques reciben la misma aportacion en las tres clases")
    check(_cerca(sh2["phi"][:, 2, :], 0.0),
          "el bloque ajeno a la interaccion sigue recibiendo cero")
    check(_cerca(ex.verifica_eficiencia(sh2), 0.0, tol=1e-12),
          "la eficiencia se cumple tambien cuando el modelo no es aditivo")

    # El reparto no puede depender de como se escriban los bloques en el mapa.
    mapa_r = {"forma": ["home_form_gd_5"], "mercado": ["mkt_p_home"],
              "fuerza": ["home_elo"]}
    sh3 = ex.shapley_blocks(_Modelo(_interaccion), X, None, base, mapa_r)
    j = sh3["bloques"].index("mercado")
    check(_cerca(sh3["phi"][:, j, :], sh2["phi"][:, 0, :]),
          "y es indiferente al orden en que se declaren los bloques")

    # Un partido en la referencia no tiene nada que explicar.
    Xb = pd.DataFrame([{c: 0.0 for c in X.columns}])
    sh4 = ex.shapley_blocks(_Modelo(_aditivo), Xb, None, base, mapa)
    check(_cerca(sh4["phi"], 0.0),
          "un partido identico a la referencia reparte cero entre todos los bloques")

    glob = ex.shapley_global(sh)
    check(list(glob.bloque) == ["mercado", "forma", "fuerza"],
          "la importancia global ordena los bloques por aportacion absoluta media")
    check(_cerca(float(glob.loc[glob.bloque == "fuerza", "importancia"]), 0.0),
          "y el bloque nulo aparece con importancia cero")
    check(_cerca(float(glob.loc[glob.bloque == "mercado", "abs_home"]),
                 float(np.abs(m).mean())),
          "la aportacion absoluta media por clase es la del reparto, sin reescalar")
    check(abs(float(glob.loc[glob.bloque == "mercado", "medio_home"])) <
          float(glob.loc[glob.bloque == "mercado", "abs_home"]),
          "y el promedio con signo es menor que el absoluto: el bloque empuja en ambos sentidos")

    base_mediana = ex.baseline_median(X)
    check(_cerca(base_mediana["mkt_p_home"], float(X["mkt_p_home"].median())),
          "la referencia por defecto es la mediana de la ventana, columna a columna")


# ===========================================================================
# 5. LOCAL
# ===========================================================================
def test_5_local() -> None:
    print("\n5. LECTURA LOCAL")
    _instala_lineage()

    X = pd.DataFrame({c: [0.0] for c in _COLS})
    X.loc[0, "mkt_p_home"] = 2.0        # aporta +0.20 al local
    X.loc[0, "home_form_gd_5"] = 1.0    # aporta +0.05 al local
    mapa = {"mercado": ["mkt_p_home"], "forma": ["home_form_gd_5"],
            "fuerza": ["home_elo"]}
    base = pd.Series(0.0, index=X.columns)
    sh = ex.shapley_blocks(_Modelo(_aditivo), X, None, base, mapa)

    tab = ex.explain_match(sh, 0)
    check(list(tab.index)[:2] == ["mercado", "forma"],
          "la descomposicion ordena los bloques por lo que mueven")
    check(int(tab.dominante.sum()) == 3,
          "y marca como dominantes los que la frase debe mencionar")
    check(len(tab) == 3,
          "sin recortar la tabla: el anexo de la memoria la necesita entera")
    check(_cerca(tab.loc["mercado", "home"], 0.20)
          and _cerca(tab.loc["forma", "home"], 0.05),
          "las aportaciones se publican en puntos de probabilidad")
    check(_cerca(tab[list(ex._CLASES)].sum(axis=0).to_numpy(),
                 sh["completo"][0] - sh["base"][0]),
          "y la columna de cada clase suma la diferencia con la referencia")
    check(tab.loc["mercado", "titulo"] == "lo que dice el mercado",
          "cada fila lleva el rotulo con el que se publica el bloque")

    dos = ex.explain_match(sh, 0, top=2)
    check(int(dos.dominante.sum()) == 2, "el numero de dominantes es configurable")

    # Ordenar por una clase concreta es lo que se quiere cuando la explicacion
    # acompana a una apuesta: importa lo que mueve ESE signo.
    porclase = ex.explain_match(sh, 0, clase="away")
    check(list(porclase.index)[0] == "forma",
          "al ordenar por el signo visitante manda el bloque que lo mueve")

    frase = ex.narrate(sh, 0)
    check(frase.startswith("Victoria local con probabilidad"),
          "la frase explica el resultado mas probable")
    check("58.3%" in frase or "58,3" in frase or "%" in frase,
          "y publica la probabilidad y la referencia en porcentaje")
    check("lo que dice el mercado a favor (+20.0%)" in frase,
          "menciona el bloque dominante con su signo y su magnitud")
    check("la fuerza de las plantillas" not in frase,
          "y no menciona un bloque cuya aportacion es nula")

    # Un partido sin nada que destacar tiene que decirlo, no inventar matices.
    Xb = pd.DataFrame({c: [0.0] for c in _COLS})
    shb = ex.shapley_blocks(_Modelo(_aditivo), Xb, None, base, mapa)
    check("Ningun bloque" in ex.narrate(shb, 0).replace("Ningún", "Ningun"),
          "un partido en la referencia se explica diciendo que no hay nada que destacar")

    # El umbral evita enumerar aportaciones de dos milesimas como si importasen.
    Xc = pd.DataFrame({c: [0.0] for c in _COLS})
    Xc.loc[0, "mkt_p_home"] = 2.0
    Xc.loc[0, "home_form_gd_5"] = 0.05      # aporta 0.0025: por debajo del umbral
    shc = ex.shapley_blocks(_Modelo(_aditivo), Xc, None, base, mapa)
    check("el momento de forma" not in ex.narrate(shc, 0),
          "una aportacion por debajo del umbral no se menciona")
    check("el momento de forma" in ex.narrate(shc, 0, umbral=0.001),
          "y bajar el umbral la recupera")


# ===========================================================================
# 6. CONCORDANCIA
# ===========================================================================
def test_6_concordancia() -> None:
    print("\n6. CONCORDANCIA ENTRE RELATOS")

    a = pd.Series({"mercado": 0.30, "forma": 0.20, "fuerza": 0.10, "bajas": 0.05})
    conc = ex.agreement({"uno": a, "otro": a * 3})
    check(_cerca(conc["spearman"].loc["uno", "otro"], 1.0),
          "dos relatos con el mismo orden concuerdan perfectamente")
    check(conc["orden"]["uno"] == ["mercado", "forma", "fuerza", "bajas"],
          "y el orden publicado va de mas a menos importante")
    check(_cerca(conc["rangos"]["rango_uno"].loc["mercado"], 1.0),
          "el rango 1 es el del bloque mas importante")

    conc2 = ex.agreement({"uno": a, "inverso": -a})
    check(_cerca(conc2["spearman"].loc["uno", "inverso"], -1.0),
          "y un relato que invierte el orden da concordancia -1")

    # Que discrepen no es un fallo: los relatos responden a preguntas distintas y
    # el modulo tiene que poder decirlo con un numero.
    b = pd.Series({"mercado": 0.05, "forma": 0.30, "fuerza": 0.20, "bajas": 0.10})
    conc3 = ex.agreement({"coeficientes": a, "permutacion": b})
    check(-1.0 <= float(conc3["spearman"].loc["coeficientes", "permutacion"]) < 1.0,
          "una discrepancia real se publica como concordancia parcial, no como error")

    # Un bloque que solo aparece en un relato se descarta: comparar rangos sobre
    # conjuntos distintos de bloques no significa nada.
    c = pd.Series({"mercado": 0.1, "forma": 0.2})
    conc4 = ex.agreement({"uno": a, "corto": c})
    check(list(conc4["valores"].index) == ["mercado", "forma"],
          "la comparacion se restringe a los bloques que los dos relatos cubren")


# ===========================================================================
# 7. GUARDAS
# ===========================================================================
def _falla(fn, tipos=Exception) -> bool:
    try:
        fn()
    except tipos:
        return True
    return False


def test_7_guardas() -> None:
    print("\n7. GUARDAS")
    _instala_lineage()

    check(_falla(lambda: ex.bloque_de("columna_inventada"), KeyError),
          "una columna sin procedencia documentada no se puede explicar")

    X = _matriz(10, seed=41)
    base = pd.Series(0.0, index=X.columns)
    mapa = {"mercado": ["mkt_p_home"]}

    check(_falla(lambda: ex.shapley_blocks(
        _Modelo(_aditivo), X, None, base, {"mercado": ["no_existe"]}), KeyError),
        "el mapa no puede citar columnas que no estan en la matriz")
    check(_falla(lambda: ex.shapley_blocks(
        _Modelo(_aditivo), X, None, pd.Series(dtype="float64"), mapa), KeyError),
        "ni pedir un reparto sin valor de referencia para cada columna")
    check(_falla(lambda: ex.shapley_blocks(
        _Modelo(_aditivo), X, None, base, {}), ValueError),
        "un mapa vacio no produce un reparto vacio, produce un error")

    muchos = {f"b{i}": ["mkt_p_home"] for i in range(ex.MAX_BLOQUES_SHAPLEY + 1)}
    check(_falla(lambda: ex.shapley_blocks(_Modelo(_aditivo), X, None, base, muchos),
                 ValueError),
          "pasar columnas sueltas como si fueran bloques se rechaza antes de enumerar")

    # La eficiencia es la guarda que protege al lector: si el reparto no cierra,
    # la explicacion publicada no cuadraria con el numero publicado.
    sh = ex.shapley_blocks(_Modelo(_aditivo), X, None, base,
                           {"mercado": ["mkt_p_home"], "forma": ["home_form_gd_5"]})
    roto = dict(sh)
    roto["phi"] = sh["phi"] + 0.01
    check(_falla(lambda: ex.verifica_eficiencia(roto), AssertionError),
          "un reparto manipulado no pasa la comprobacion de eficiencia")

    check(_falla(lambda: ex.permutation_importance_blocks([]), ValueError),
          "no se mide importancia sin pliegues ajustados")
    aj = _pliegue(_Modelo(_solo_mercado), _matriz(50, seed=51),
                  _matriz(20, seed=52), np.zeros(20, dtype=int))
    check(_falla(lambda: ex.permutation_importance_blocks(
        [aj], metrica="accuracy"), ValueError),
        "el acierto se maximiza y no sirve como perdida a deteriorar")
    check(_falla(lambda: ex.permutation_importance_blocks(
        [aj], metrica="inventada"), ValueError),
        "ni se admite una perdida que el modulo de evaluacion no define")

    check(_falla(lambda: ex.linear_coefficients(_Modelo(_aditivo), _COLS), TypeError),
          "un modelo sin coeficientes lo dice en lugar de fabricarlos")
    check(_falla(lambda: ex.linear_coefficients(
        _Lineal([[0.1, 0.2]], [0]), ["a", "b", "c"]), ValueError),
        "y el numero de nombres tiene que coincidir con el de coeficientes")

    check(_falla(lambda: ex.explain_match(sh, 0, clase="empate"), ValueError),
          "las clases se nombran en el vocabulario del modulo, no en castellano")
    check(_falla(lambda: ex.agreement({"uno": pd.Series({"a": 1.0})}), ValueError),
          "la concordancia necesita al menos dos relatos que comparar")

    # Un bloque analitico sin destino declarado tiene que romper, no caer en un
    # cajon de sastre: un cajon de sastre es una familia entera sin explicar.
    ex._lineage_cache = {"rara": "bloque_inexistente"}
    check(_falla(lambda: ex.bloque_de("rara"), KeyError),
          "un bloque analitico sin bloque explicativo asignado se rechaza")
    _instala_lineage()


# ===========================================================================
def _run() -> None:
    print("=" * 72)
    print("BANCO DE PRUEBAS - explain.py")
    print("=" * 72)
    try:
        test_1_bloques()
        test_2_coeficientes()
        test_3_permutacion()
        test_4_shapley()
        test_5_local()
        test_6_concordancia()
        test_7_guardas()
    finally:
        # La procedencia sintetica vive en una variable de MODULO de `explain`, de
        # modo que sobrevive a este banco: si se dejara instalada, el siguiente
        # banco de la misma sesion —`test_publish_web.py`— pediria el bloque de una
        # columna real y `explain` le responderia con esta tabla de juguete, que no
        # la contiene. El fallo aparecería allí y la causa estaría aquí. Devolver la
        # cache a `None` obliga a reconstruirla de `lineage.feature_lineage()`.
        ex._lineage_cache = None
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    if FAIL:
        for m in FAIL:
            print(f"  FALLO: {m}")
    print("=" * 72)


def test_explain() -> None:
    """Entrada para pytest: el banco entero como una sola prueba."""
    PASS.clear()
    FAIL.clear()
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL[:5]}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
