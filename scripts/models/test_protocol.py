"""Validacion offline del PROTOCOLO DE VALIDACION TEMPORAL (protocol.py). Sin red.

El protocolo es la pieza del trabajo cuya averia no se ve. Un modelo mal
especificado produce metricas malas y se detecta solo; un protocolo mal
especificado produce metricas BUENAS, que es justo lo que nadie sospecha. Basta
con que la ventana de entrenamiento roce la jornada que se evalua, con que dos
partidos consecutivos del mismo equipo caigan en el mismo pliegue, o con que el
preprocesado se ajuste una sola vez sobre todo el desarrollo, para que la cifra
publicada describa un sistema que en produccion no existe.

Ninguna de esas tres averias asoma en un histograma ni en una curva de
calibracion. Por eso el banco no comprueba que el protocolo "funcione" —eso lo
demuestra cualquier ejecucion— sino que se rompa cuando debe romperse: las
comprobaciones que importan son las que fabrican la averia a proposito y exigen
un error explicito.

El dato es sintetico y esta construido con un calendario de doble vuelta por el
metodo del circulo, de modo que ninguna jornada repite equipo POR CONSTRUCCION.
Eso permite comparar la jornada reconstruida contra una verdad conocida en lugar
de contra la intuicion.

Se comprueba, en este orden:

  1. JORNADA        la reconstruccion corta por hueco de calendario y por
                    repeticion de equipo, reinicia la numeracion en cada
                    temporada y coincide con la jornada nominal cuando el
                    calendario no tiene aplazamientos.
  2. PLIEGUES       orden cronologico, ventana estrictamente anterior al
                    pliegue, particion sin solape del bloque evaluado, arranque
                    nunca evaluado, holdout fuera de la validacion, ventana
                    deslizante acotada y guarda de tamano minimo.
  3. PESOS          el peso de recencia se parte por la mitad exactamente en la
                    semivida, y su ausencia es None y no un vector de unos.
  4. CADENCIA       una cadencia = una clave de reutilizacion. Se mide ademas la
                    diferencia entre reajustar por temporada y ajustar una sola
                    vez, que es la fuga de distribucion que el protocolo corrige.
  5. ANTIFUGA       una ventana contaminada, una columna de cierre bajo regimen
                    semanal y una matriz con valores no finitos se detienen con
                    un error explicito.
  6. PROBABILIDADES el orden canonico H, D, A se reconstruye desde `classes_`
                    venga como venga, incluso sin empates en el entrenamiento, y
                    una terna que no suma 1 es un error.
  7. CONTRATO       la salida tiene una fila por partido evaluado, con clave
                    unica, probabilidades validas, el precio crudo intacto y la
                    configuracion adjunta.
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

import columns as C     # noqa: E402
import protocol as pt   # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")


# ---------------------------------------------------------------------------
# generador de la liga sintetica
# ---------------------------------------------------------------------------
_EQUIPOS = [f"eq{i:02d}" for i in range(10)]
_SEASONS = ["2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2025/26"]
_VALIDACION = ["2019/20", "2020/21"]

# Configuracion de trabajo del banco. La ventana minima se rebaja porque la liga
# sintetica tiene diez equipos y no veinte: mantener las 380 del valor real
# dejaria el banco sin ningun pliegue y la comprobacion de la guarda se hace
# aparte, subiendola a proposito.
_MIN_TRAIN = 45


def _calendario(equipos: list[str]) -> list[list[tuple[int, int]]]:
    """Doble vuelta por el metodo del circulo.

    Devuelve una lista de jornadas, cada una con los emparejamientos de la
    jornada como pares de indices. La propiedad que interesa es estructural: en
    una jornada del circulo cada equipo aparece exactamente una vez, de modo que
    la verdad contra la que se contrasta `effective_matchday` no depende de
    ninguna eleccion del banco.
    """
    n = len(equipos)
    idx = list(range(n))
    ida = []
    for _ in range(n - 1):
        ida.append([(idx[k], idx[n - 1 - k]) for k in range(n // 2)])
        idx = [idx[0]] + [idx[-1]] + idx[1:-1]
    vuelta = [[(b, a) for a, b in jornada] for jornada in ida]
    return ida + vuelta


def _liga(semilla: int = 11) -> pd.DataFrame:
    """Seis temporadas completas con features numericas y precio de mercado.

    El calendario reparte cada jornada entre dos dias consecutivos y separa las
    jornadas siete dias, que es el patron que el corte por hueco espera
    encontrar. `x_nivel` crece de temporada en temporada a proposito: es la
    columna con la que se mide la deriva del valor de imputacion neutral.
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
                filas.append({
                    "match_key": f"{dia.date()}-{_EQUIPOS[a]}-{_EQUIPOS[b]}",
                    "season": temporada,
                    "date": dia,
                    "round": j + 1,
                    "home_key": _EQUIPOS[a],
                    "away_key": _EQUIPOS[b],
                    "home_team": _EQUIPOS[a],
                    "away_team": _EQUIPOS[b],
                    "y_num": int(rng.choice(3, p=p)),
                    "split": ("holdout" if temporada in C.HOLDOUT_SEASONS
                              else "dev"),
                    "x_forma": float(rng.normal(0.0, 1.0)),
                    "x_elo": float(rng.normal(1500.0, 60.0)),
                    "x_ritmo": float(rng.normal(11.0, 2.5)),
                    "x_nivel": float(rng.normal(100.0 * (t + 1), 5.0)),
                    "mkt_p_home": float(p[0]),
                    "mkt_p_draw": float(p[1]),
                    "mkt_p_away": float(p[2]),
                    "odds_close_avg_h": float(1.0 / max(p[0], 0.05)),
                    # El movimiento de la linea entre apertura y cierre es la
                    # variable que separa a los dos regimenes: lleva el token
                    # `close`, de modo que el semanal la excluye por convenio y
                    # el retrospectivo la admite. La cuota de cierre de arriba
                    # no sirve para eso porque no es una variable —es precio,
                    # y el precio entra al modelo por `mkt_p_*`—, asi que el
                    # preprocesado no la considera candidata bajo ningun regimen.
                    # Se deriva del propio favoritismo en lugar de sortearse:
                    # una extraccion mas del generador correria el resto del
                    # flujo y cambiaria valores de columnas que otras pruebas ya
                    # dan por fijos. Y se centra lejos del cero porque la tabla
                    # de deriva mide variacion RELATIVA, de modo que una columna
                    # cuya media ronda el cero la encabeza por division.
                    "close_move_fav": float(0.20 + 0.05 * (p[0] - p[2])),
                })
    return pd.DataFrame(filas)


def _frame(filas: list[tuple]) -> pd.DataFrame:
    """Frame minimo a mano: (temporada, fecha, local, visitante)."""
    return pd.DataFrame({
        "season": [f[0] for f in filas],
        "date": [pd.Timestamp(f[1]) for f in filas],
        "home_key": [f[2] for f in filas],
        "away_key": [f[3] for f in filas],
    })


# ---------------------------------------------------------------------------
# estimadores de juguete
# ---------------------------------------------------------------------------
class _Frecuencias:
    """Estimador trivial: la frecuencia de clase de la ventana de entrenamiento.

    No aprende nada de las features, y eso es exactamente lo que se quiere aqui:
    cualquier cosa que el banco observe en la salida es atribuible al protocolo y
    no al modelo.
    """

    def fit(self, X, y):
        y = pd.Series(np.asarray(y))
        self.classes_ = np.array(sorted(y.unique()))
        frec = y.value_counts(normalize=True)
        self._p = np.array([frec.get(c, 0.0) for c in self.classes_], dtype="float64")
        self._p = self._p / self._p.sum()
        self.n_fit_ = len(y)
        return self

    def predict_proba(self, X):
        return np.tile(self._p, (len(X), 1))


class _FrecuenciasConPesos(_Frecuencias):
    """El mismo estimador, con `sample_weight` en la firma de `fit`."""

    def fit(self, X, y, sample_weight=None):
        y = pd.Series(np.asarray(y))
        w = np.ones(len(y)) if sample_weight is None else np.asarray(sample_weight)
        self.classes_ = np.array(sorted(y.unique()))
        self._p = np.array([w[(y == c).to_numpy()].sum() for c in self.classes_],
                           dtype="float64")
        self._p = self._p / self._p.sum()
        self.peso_visto_ = w
        return self


class _Fijo:
    """Devuelve una terna fija en el orden que declare `classes_`."""

    def __init__(self, clases, probas):
        self.classes_ = np.asarray(clases)
        self._p = np.asarray(probas, dtype="float64")

    def predict_proba(self, X):
        return np.tile(self._p, (len(X), 1))


# ---------------------------------------------------------------------------
# 1. la jornada efectiva
# ---------------------------------------------------------------------------
def test_jornada() -> None:
    print("\n1. JORNADA - reconstruir el calendario en lugar de creer al rotulo")

    # (a) corte por hueco: mismos equipos no, fechas separadas si.
    f = _frame([("2019/20", "2019-08-17", "a", "b"),
                ("2019/20", "2019-08-18", "c", "d"),
                ("2019/20", "2019-08-24", "e", "f"),
                ("2019/20", "2019-08-25", "g", "h")])
    j = pt.effective_matchday(f).tolist()
    check(j == [1, 1, 2, 2],
          "un hueco de calendario mayor que el umbral abre jornada nueva")

    # (b) corte por repeticion de equipo: fechas contiguas, pero 'a' vuelve a jugar.
    f = _frame([("2019/20", "2019-08-17", "a", "b"),
                ("2019/20", "2019-08-18", "c", "d"),
                ("2019/20", "2019-08-19", "a", "c")])
    j = pt.effective_matchday(f).tolist()
    check(j == [1, 1, 2],
          "un equipo que se repite abre jornada nueva aunque no medie hueco")

    # Es la propiedad de la que depende todo lo demas: si 'a' cayera en el mismo
    # pliegue que su partido anterior, la forma del segundo se habria calculado
    # con un resultado que en produccion todavia no existe.
    f = _frame([("2019/20", "2019-08-17", "a", "b"),
                ("2019/20", "2019-08-18", "a", "c")])
    check(pt.effective_matchday(f).tolist() == [1, 2],
          "dos partidos consecutivos del mismo equipo nunca comparten pliegue")

    # (c) la numeracion se reinicia en cada temporada.
    f = _frame([("2019/20", "2019-08-17", "a", "b"),
                ("2019/20", "2019-08-24", "c", "d"),
                ("2020/21", "2020-08-15", "e", "f")])
    check(pt.effective_matchday(f).tolist() == [1, 2, 1],
          "la numeracion arranca de nuevo en cada temporada")

    # (d) sobre la liga completa: la reconstruccion coincide con la verdad.
    liga = _liga()
    jj = pt.effective_matchday(liga)
    check(bool((jj.to_numpy() == liga["round"].to_numpy()).all()),
          "sin aplazamientos, la jornada reconstruida es la jornada nominal")

    tam = liga.assign(_j=jj).groupby(["season", "_j"]).size()
    check(bool((tam == len(_EQUIPOS) // 2).all()),
          "cada jornada reconstruida tiene los partidos que le corresponden")

    dup = (liga.assign(_j=jj)
           .melt(id_vars=["season", "_j"], value_vars=["home_key", "away_key"],
                 value_name="equipo")
           .groupby(["season", "_j", "equipo"]).size().max())
    check(int(dup) == 1,
          "ningun equipo aparece dos veces dentro de la misma jornada")

    # (e) informe descriptivo.
    rep = pt.matchday_report(liga)
    check(list(rep.columns) == ["season", "partidos", "jornadas_efectivas",
                                "jornadas_nominales", "coincidencia",
                                "partidos_por_jornada"],
          "matchday_report publica el contraste efectiva vs nominal")
    check(len(rep) == len(_SEASONS) and bool((rep["coincidencia"] == 1.0).all()),
          "y declara coincidencia total sobre un calendario sin aplazamientos")

    sin_round = pt.matchday_report(liga.drop(columns=["round"]))
    check(bool(sin_round["coincidencia"].isna().all())
          and bool((sin_round["jornadas_efectivas"] > 0).all()),
          "sin jornada nominal el contraste queda vacio pero la reconstruccion no")

    # (f) el frame incompleto es un error, no una columna de ceros.
    try:
        pt.effective_matchday(liga.drop(columns=["away_key"]))
        check(False, "un frame sin las columnas necesarias se detiene con error")
    except ValueError as e:
        check("away_key" in str(e),
              "un frame sin las columnas necesarias se detiene con error")


# ---------------------------------------------------------------------------
# 2. los pliegues
# ---------------------------------------------------------------------------
def test_pliegues() -> None:
    print("\n2. PLIEGUES - la ventana termina donde empieza lo que se predice")
    liga = _liga()
    cfg = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False)
    folds = pt.make_folds(liga, cfg)
    fechas = pd.to_datetime(liga["date"])

    check(len(folds) > 0, "el bloque de validacion produce pliegues")
    check([f.idx for f in folds] == list(range(len(folds))),
          "los pliegues se numeran de forma correlativa")
    inicios = [f.fecha_ini for f in folds]
    check(inicios == sorted(inicios), "y se emiten en orden cronologico")

    # La comprobacion central: nada de la ventana alcanza el pliegue.
    roza = [f.idx for f in folds
            if fechas.loc[f.train_idx].max() >= f.fecha_ini]
    check(not roza, "ninguna ventana de entrenamiento alcanza su propio pliegue")

    solapa = [f.idx for f in folds
              if len(set(f.train_idx) & set(f.eval_idx))]
    check(not solapa, "ningun partido esta a la vez en la ventana y en el pliegue")

    # Particion del bloque evaluado.
    evaluados = [i for f in folds for i in f.eval_idx]
    check(len(evaluados) == len(set(evaluados)),
          "los pliegues no se solapan entre si")
    bloque = liga.index[(liga["split"] == "dev")
                        & (liga["season"].isin(_VALIDACION))]
    check(set(evaluados) == set(bloque),
          "y cubren el bloque de validacion entero")

    temporadas = {f.season for f in folds}
    check(temporadas == set(_VALIDACION),
          "solo se evaluan las temporadas de validacion")
    check(not (set(liga.loc[evaluados, "season"]) & set(pt.WARMUP_SEASONS)),
          "el bloque de arranque alimenta la ventana pero nunca se evalua")
    entrenados = {i for f in folds for i in f.train_idx}
    check(bool(set(liga.loc[sorted(entrenados), "season"])
               & set(pt.WARMUP_SEASONS)),
          "y si alimenta la ventana, que es la razon de conservarlo")
    check(not (set(liga.loc[evaluados, "split"]) - {"dev"}),
          "el holdout no aparece en ningun pliegue de validacion")

    # El holdout, cuando se pide expresamente.
    hold = pt.make_folds(liga, cfg, evaluar="holdout")
    ev_h = [i for f in hold for i in f.eval_idx]
    check(bool(ev_h) and set(liga.loc[ev_h, "split"]) == {"holdout"},
          "la pasada del holdout evalua exactamente las temporadas selladas")
    # En esa pasada la ventana SI incorpora las jornadas ya disputadas del propio
    # holdout: al predecir la jornada 20 de la temporada sellada, el resultado de
    # las diecinueve anteriores es pasado y no mirarlo seria simular un sistema
    # peor que el real. Lo que se mantiene sellado es el ESTADO del preprocesado,
    # que se aprende solo sobre desarrollo (seccion 4).
    ultimo = hold[-1]
    check(bool(set(liga.loc[ultimo.train_idx, "split"]) == {"dev", "holdout"}),
          "y su ventana incorpora las jornadas ya disputadas de la propia temporada")

    try:
        pt.make_folds(liga, cfg, evaluar="ninguno")
        check(False, "un bloque mal escrito se detiene con error explicito")
    except ValueError as e:
        check("desconocido" in str(e),
              "un bloque mal escrito se detiene con error explicito")

    # Las dos mitades de la validacion. El corte parte el bloque de desarrollo en
    # una mitad donde se ELIGEN variables y otra donde se MIDE el conjunto ya
    # cerrado; que sean disjuntas y que juntas cubran la validacion entera es lo
    # que hace que la cifra publicada no se haya usado para elegir nada. Aqui el
    # corte va en la segunda temporada evaluable porque la liga de prueba solo
    # tiene dos; en el proyecto son tres y tres a un lado y otro de 2022/23.
    cfg_p = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False,
                              corte_seleccion=_VALIDACION[1])
    sel = pt.make_folds(liga, cfg_p, evaluar="seleccion")
    med = pt.make_folds(liga, cfg_p, evaluar="medicion")
    ev_s = {i for f in sel for i in f.eval_idx}
    ev_m = {i for f in med for i in f.eval_idx}
    check(bool(ev_s) and bool(ev_m) and not (ev_s & ev_m),
          "seleccion y medicion no comparten ni un partido")
    check(ev_s | ev_m == set(evaluados),
          "y juntas cubren la validacion entera, sin huecos")
    check(set(liga.loc[sorted(ev_s), "season"]) == {_VALIDACION[0]}
          and set(liga.loc[sorted(ev_m), "season"]) == {_VALIDACION[1]},
          "el corte reparte por temporadas, no por partidos sueltos")
    check(not (set(liga.loc[sorted(ev_m), "split"]) - {"dev"}),
          "la mitad que publica sigue siendo desarrollo: el holdout no se toca")
    check(str(cfg_p.corte_seleccion) in cfg_p.describe(),
          "el corte viaja en la descripcion del protocolo, y por tanto en el sello")

    try:
        pt.ProtocolConfig(corte_seleccion=pt.WARMUP_SEASONS[0])
        check(False, "un corte dentro del arranque se detiene con error explicito")
    except ValueError as e:
        check("arranque" in str(e),
              "un corte dentro del arranque se detiene con error explicito")

    # La TEMPORADA EN CURSO. Se fabrica anadiendo a la liga una temporada
    # posterior a la sellada --que es lo que ocurre cada agosto en el proyecto
    # real-- con `split` a "dev", porque esa columna solo materializa el sello del
    # holdout y no sabe nada de la temporada que se juega. La prueba es que el
    # protocolo la descuente de todos los bloques de desarrollo pese a ello.
    en_curso = "2026/27"
    extra = liga[liga["season"] == C.HOLDOUT_SEASONS[-1]].copy()
    extra["season"] = en_curso
    extra["split"] = "dev"
    extra["date"] = extra["date"] + pd.DateOffset(years=1)
    extra["match_key"] = (extra["date"].dt.date.astype(str) + "-"
                          + extra["home_key"] + "-" + extra["away_key"])
    viva = pd.concat([liga, extra], ignore_index=True)

    check(pt.temporadas_en_curso(viva) == [en_curso],
          "la temporada en curso se deduce de ir despues del holdout, sin lista")
    check(pt.temporadas_en_curso(liga) == [],
          "y sin temporada posterior a la sellada no hay ninguna en curso")

    for bloque in ("validacion", "seleccion", "medicion"):
        ev_b = {i for f in pt.make_folds(viva, cfg_p, evaluar=bloque)
                for i in f.eval_idx}
        check(en_curso not in set(viva.loc[sorted(ev_b), "season"]),
              f"la temporada en curso NO se evalua en el bloque de {bloque}: "
              "si lo hiciera, la decision que sale de ahi se habria tomado con "
              "predicciones de un modelo entrenado sobre el holdout")

    prod = pt.make_folds(viva, cfg_p, evaluar="produccion")
    ev_p = [i for f in prod for i in f.eval_idx]
    check(bool(ev_p) and set(viva.loc[ev_p, "season"]) == {en_curso},
          "el bloque de produccion evalua exactamente la temporada en curso")
    check(set(viva.loc[ev_p, "y_num"].notna()) == {True},
          "y solo partidos con resultado: la jornada que viene no es un pliegue")
    # Que la ventana contenga el holdout no es un fallo que se tolere, es la
    # situacion inevitable de cualquier temporada posterior a la sellada, y la
    # razon por la que el bloque existe. Se comprueba para que quede escrito.
    check("holdout" in set(viva.loc[prod[0].train_idx, "split"]),
          "y se entrena CON el holdout dentro de la ventana, que es precisamente "
          "lo que obliga a sacar este bloque de la validacion")

    sin_resultado = viva.copy()
    sin_resultado.loc[sin_resultado["season"] == en_curso, "y_num"] = np.nan
    check(pt.make_folds(sin_resultado, cfg_p, evaluar="produccion") == [],
          "una temporada en curso sin ningun partido jugado no da pliegues")

    # Ventana deslizante: acotada por arriba y por abajo.
    cfg_desl = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False,
                                 window="deslizante", sliding_seasons=1)
    desl = pt.make_folds(liga, cfg_desl)
    fuera = [f.idx for f in desl
             if fechas.loc[f.train_idx].min()
             < f.fecha_ini - pd.DateOffset(years=1)]
    check(not fuera, "la ventana deslizante no mira mas atras de lo declarado")
    check(all(f.n_train <= g.n_train for f, g in zip(desl, folds)),
          "y nunca es mayor que la expansiva del mismo pliegue")

    # Guarda de tamano minimo.
    exigente = pt.make_folds(liga, pt.ProtocolConfig(min_train_matches=400,
                                                     transform=False))
    check(len(exigente) < len(folds) and all(f.n_train >= 400 for f in exigente),
          "los pliegues con ventana insuficiente se descartan")

    rep = pt.folds_report(folds)
    check(list(rep.columns) == ["fold", "season", "jornada", "fecha_ini",
                                "fecha_fin", "n_train", "n_eval", "dias"],
          "folds_report describe cada pliegue con su ventana y su duracion")
    check(len(rep) == len(folds) and int(rep["n_eval"].sum()) == len(evaluados),
          "y cuadra con los pliegues que describe")


# ---------------------------------------------------------------------------
# 3. el peso de recencia
# ---------------------------------------------------------------------------
def test_pesos() -> None:
    print("\n3. PESOS - la semivida significa exactamente lo que dice")
    ref = pd.Timestamp("2020-01-01")
    fechas = pd.to_datetime(["2020-01-01", "2019-12-02", "2019-11-02",
                             "2019-01-01"])

    check(pt.recency_weights(fechas, ref, None) is None,
          "sin semivida no hay vector de pesos, hay ausencia de pesos")

    w = pt.recency_weights(fechas, ref, 30.0)
    check(abs(w[0] - 1.0) < 1e-12, "el dia de referencia pesa 1")
    check(abs(w[1] - 0.5) < 1e-9, "treinta dias antes pesa exactamente la mitad")
    check(abs(w[2] - 0.25) < 1e-9, "sesenta dias antes, la cuarta parte")
    check(w[3] < 1e-3 and w[3] > 0,
          "un ano antes el peso es despreciable pero nunca nulo")
    check(bool((np.diff(w) < 0).all()), "el peso decrece monotonamente al pasado")

    # Un partido posterior a la referencia no puede pesar mas que uno de ese dia:
    # el peso no es una via para amplificar el futuro.
    futuro = pt.recency_weights(pd.to_datetime(["2020-06-01"]), ref, 30.0)
    check(abs(futuro[0] - 1.0) < 1e-12,
          "una fecha posterior a la referencia se satura en 1")

    try:
        pt.ProtocolConfig(halflife_days=0)
        check(False, "una semivida no positiva se detiene con error explicito")
    except ValueError:
        check(True, "una semivida no positiva se detiene con error explicito")

    # El espacio declarado. Se escribe aqui, junto a la funcion que lo aplica y
    # antes de buscar: un rango que se retoca despues de ver resultados deja de ser
    # una busqueda. `None` es un valor del espacio —«todo el pasado pesa igual»— y
    # no la ausencia de el, de modo que ponderar se adopta solo donde mejora.
    check(pt.SEMIVIDAS[0] is None,
          "«sin peso» es el primer valor del espacio, no su ausencia")
    check(all(s is None or (isinstance(s, float) and s > 0) for s in pt.SEMIVIDAS)
          and len(set(pt.SEMIVIDAS)) == len(pt.SEMIVIDAS),
          "los demas valores son semividas positivas y no se repiten")
    check(min(s for s in pt.SEMIVIDAS if s) <= 200
          and max(s for s in pt.SEMIVIDAS if s) >= 1800,
          "el espacio cubre de media temporada a cinco anos")

    # El punto de enganche. La semivida por defecto vive en la configuracion, pero
    # una sola configuracion recorre todas las temporadas y familias mientras que
    # la elegida cambia con ambas: quien la conoce es la fabrica.
    fold = pt.Fold(idx=0, season="2023/24", jornada=1,
                   fecha_ini=pd.Timestamp("2024-01-01"),
                   fecha_fin=pd.Timestamp("2024-01-02"),
                   train_idx=pd.Index([]), eval_idx=pd.Index([]))
    cfg_p = pt.ProtocolConfig(halflife_days=365.0)

    check(pt.semivida_de(lambda: None, fold, cfg_p) == 365.0,
          "una fabrica que no declara semivida deja mandar a la configuracion")

    class _ConSemivida:
        def __init__(self, valor):
            self.valor = valor

        def __call__(self):
            return None

        def semivida(self, fold=None):
            return self.valor

    check(pt.semivida_de(_ConSemivida(730.0), fold, cfg_p) == 730.0,
          "una fabrica que declara semivida manda sobre la configuracion")
    check(pt.semivida_de(_ConSemivida(None), fold, cfg_p) is None,
          "y puede declarar que NO se pondera, que no es lo mismo que no declarar")
    check(pt.semivida_de(_ConSemivida(pt.HEREDADA), fold, cfg_p) == 365.0,
          "la marca de heredada devuelve el mando a la configuracion")
    try:
        pt.semivida_de(_ConSemivida(-1.0), fold, cfg_p)
        check(False, "una semivida no positiva en un pliegue se detiene")
    except ValueError as e:
        check("2023/24" in str(e),
              "una semivida no positiva en un pliegue se detiene y dice donde")


# ---------------------------------------------------------------------------
# 4. la cadencia de reajuste del preprocesado
# ---------------------------------------------------------------------------
def _espiar_fit(fn):
    """Ejecuta `fn` registrando los frames con los que se ajusta el preprocesado."""
    original = pt.pp.fit
    vistos: list[pd.DataFrame] = []

    def espia(dev, *a, **k):
        vistos.append(dev)
        return original(dev, *a, **k)

    pt.pp.fit = espia
    try:
        fn()
    finally:
        pt.pp.fit = original
    return vistos


def test_cadencia() -> None:
    print("\n4. CADENCIA - una clave de reutilizacion por cadencia declarada")
    liga = _liga()

    def _corre(cadencia, **kw):
        cfg = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False,
                                refit_cadence=cadencia, **kw)
        salida = {}
        vistos = _espiar_fit(
            lambda: salida.update(pred=pt.run_protocol(liga, _Frecuencias, cfg)))
        return salida["pred"], vistos

    pred_t, vistos_t = _corre("temporada")
    check(len(vistos_t) == len(_VALIDACION),
          "la cadencia por temporada ajusta un estado por temporada evaluada")
    check(len(pred_t.attrs["states"]) == len(_VALIDACION),
          "y los conserva todos para poder auditarlos despues")

    pred_j, vistos_j = _corre("jornada")
    check(len(vistos_j) == pred_j.attrs["n_folds"],
          "la cadencia por jornada ajusta un estado por pliegue")

    pred_u, vistos_u = _corre("unico")
    check(len(vistos_u) == 1,
          "la cadencia unica ajusta un solo estado para toda la ejecucion")

    # La medida de la fuga que el protocolo corrige: el ajuste unico ve el bloque
    # de desarrollo entero, incluido el futuro del pliegue; el ajuste por
    # temporada ve solo lo anterior. Es un hecho verificable, no una afirmacion.
    n_dev = int((liga["split"] == "dev").sum())
    check(len(vistos_u[0]) == n_dev,
          "el ajuste unico mira el bloque de desarrollo entero (por eso no es valido)")
    check(len(vistos_t[0]) < n_dev,
          "el ajuste por temporada mira solo lo anterior al pliegue")
    check(len(vistos_t[0]) < len(vistos_t[-1]),
          "y su ventana crece conforme avanza la validacion")

    # El estado se aprende SIEMPRE sobre desarrollo, tambien en la pasada del
    # holdout: es lo que mantiene el sello mientras la ventana del modelo avanza.
    cfg_h = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False)
    vistos_h = _espiar_fit(
        lambda: pt.run_protocol(liga, _Frecuencias, cfg_h, evaluar="holdout"))
    check(all(set(v["split"]) == {"dev"} for v in vistos_h),
          "en la pasada del holdout el estado sigue aprendiendose solo en desarrollo")

    # Deriva del valor de imputacion neutral (la comprobacion empirica de M3).
    deriva = pt.imputation_drift(pred_t.attrs["states"])
    check(set(deriva.columns) == {"columna", "primero", "ultimo", "deriva_rel"},
          "imputation_drift publica el primer y el ultimo valor aprendido")
    fila = deriva[deriva["columna"] == "x_nivel"]
    check(len(fila) == 1 and float(fila["deriva_rel"].iloc[0]) > 0.05,
          "y detecta la deriva de una columna cuyo nivel cambia entre temporadas")
    check(deriva["columna"].iloc[0] == "x_nivel",
          "que ademas encabeza la tabla, ordenada por magnitud de la deriva")
    estables = deriva[deriva["columna"].isin(["x_forma", "x_elo", "x_ritmo"])]
    check(bool((estables["deriva_rel"].abs() < 1.0).all()),
          "mientras que las columnas estacionarias no derivan")
    check(pt.imputation_drift({}).empty,
          "sin estados, la tabla de deriva esta vacia y no falla")


# ---------------------------------------------------------------------------
# 5. las guardas antifuga
# ---------------------------------------------------------------------------
def test_antifuga() -> None:
    print("\n5. ANTIFUGA - las averias silenciosas se convierten en errores")
    liga = _liga()
    cfg = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False)
    folds = pt.make_folds(liga, cfg)

    # (a) ventana contaminada con un partido del propio pliegue.
    sucio = folds[len(folds) // 2]
    contaminado = pt.Fold(idx=sucio.idx, season=sucio.season,
                          jornada=sucio.jornada, fecha_ini=sucio.fecha_ini,
                          fecha_fin=sucio.fecha_fin,
                          train_idx=sucio.train_idx.append(sucio.eval_idx[:1]),
                          eval_idx=sucio.eval_idx)
    try:
        pt.run_protocol(liga, _Frecuencias, cfg, folds=[contaminado])
        check(False, "una ventana que alcanza el pliegue se detiene con error")
    except AssertionError as e:
        check("ventana de entrenamiento" in str(e),
              "una ventana que alcanza el pliegue se detiene con error")

    # (b) columna de cierre bajo regimen semanal.
    X = pd.DataFrame({"x_forma": [0.1, 0.2], "odds_close_avg_h": [1.9, 2.1]})
    try:
        pt._guardas_matriz(X, pt.ProtocolConfig(regime="semanal"))
        check(False, "una columna de cierre bajo regimen semanal es un error")
    except AssertionError as e:
        check("odds_close_avg_h" in str(e),
              "una columna de cierre bajo regimen semanal es un error")
    pt._guardas_matriz(X, pt.ProtocolConfig(regime="retrospectivo"))
    check(True, "y la misma columna es legitima bajo regimen retrospectivo")

    # (c) valores no finitos: el error nombra la columna culpable.
    Y = pd.DataFrame({"x_forma": [0.1, 0.2], "x_roto": [np.nan, 1.0]})
    try:
        pt._guardas_matriz(Y, pt.ProtocolConfig(regime="semanal"))
        check(False, "un hueco superviviente al preprocesado es un error")
    except AssertionError as e:
        check("x_roto" in str(e) and "x_forma" not in str(e),
              "un hueco superviviente al preprocesado es un error que se localiza")

    # (d) la matriz real que entra al estimador respeta el regimen.
    pred = pt.run_protocol(liga, _Frecuencias, cfg)
    estado = list(pred.attrs["states"].values())[0]
    check(not C.weekly_unavailable(estado.feature_cols),
          "bajo regimen semanal ninguna feature del estado sale del cierre")
    check("close_move_fav" in liga.columns,
          "aunque el artefacto de partida si la traiga (control de la prueba)")

    retro = pt.run_protocol(liga, _Frecuencias,
                            pt.ProtocolConfig(min_train_matches=_MIN_TRAIN,
                                              transform=False,
                                              regime="retrospectivo"))
    estado_r = list(retro.attrs["states"].values())[0]
    check("close_move_fav" in estado_r.feature_cols,
          "y bajo regimen retrospectivo si entra, que es lo que separa a los dos")

    # (e) configuraciones imposibles.
    for kw, etiqueta in [({"regime": "semanales"}, "un regimen mal escrito"),
                         ({"window": "expansible"}, "una ventana mal escrita"),
                         ({"refit_cadence": "diaria"}, "una cadencia mal escrita")]:
        try:
            pt.ProtocolConfig(**kw)
            check(False, f"{etiqueta} se detiene con error explicito")
        except ValueError as e:
            check("desconocid" in str(e),
                  f"{etiqueta} se detiene con error explicito")


# ---------------------------------------------------------------------------
# 6. el orden de las probabilidades
# ---------------------------------------------------------------------------
def test_probabilidades() -> None:
    print("\n6. PROBABILIDADES - el orden canonico no se supone, se reconstruye")
    X = pd.DataFrame({"x": [0.0, 1.0, 2.0]})

    natural = _Fijo([0, 1, 2], [0.5, 0.3, 0.2])
    p = pt._proba_ordenada(natural, X)
    check(np.allclose(p[0], [0.5, 0.3, 0.2]),
          "con classes_ en orden natural la terna pasa tal cual")

    permutado = _Fijo([2, 0, 1], [0.5, 0.3, 0.2])
    p = pt._proba_ordenada(permutado, X)
    check(np.allclose(p[0], [0.3, 0.2, 0.5]),
          "con classes_ permutado la terna se recoloca en H, D, A")

    # El caso que motiva la funcion: una ventana de entrenamiento sin empates.
    sin_empate = _Fijo([0, 2], [0.6, 0.4])
    p = pt._proba_ordenada(sin_empate, X)
    check(np.allclose(p[0], [0.6, 0.0, 0.4]),
          "una ventana sin empates produce p_draw = 0, no una columna desplazada")
    check(np.allclose(p.sum(axis=1), 1.0),
          "y la terna sigue sumando 1")

    mal = _Fijo([0, 1, 2], [0.5, 0.3, 0.1])
    try:
        pt._proba_ordenada(mal, X)
        check(False, "una terna que no suma 1 se detiene con error")
    except ValueError as e:
        check("suman 1" in str(e), "una terna que no suma 1 se detiene con error")


# ---------------------------------------------------------------------------
# 7. el contrato de salida
# ---------------------------------------------------------------------------
def test_contrato() -> None:
    print("\n7. CONTRATO - una fila por partido evaluado, con su identidad")
    liga = _liga()
    cfg = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False)
    folds = pt.make_folds(liga, cfg)
    pred = pt.run_protocol(liga, _Frecuencias, cfg, folds=folds)

    esperadas = ["fold", "season", "jornada", "date", "match_key", "home_key",
                 "away_key", "split", "y_num", "n_train",
                 "p_home", "p_draw", "p_away"]
    check(all(c in pred.columns for c in esperadas),
          "la salida trae la identidad del partido y las tres probabilidades")

    evaluados = [i for f in folds for i in f.eval_idx]
    check(len(pred) == len(evaluados),
          "una fila por partido evaluado, ni una mas ni una menos")
    check(not pred["match_key"].duplicated().any(),
          "ningun partido lo predice mas de un pliegue")
    check(set(pred["match_key"]) == set(liga.loc[evaluados, "match_key"]),
          "y son exactamente los partidos del bloque evaluado")

    P = pred[["p_home", "p_draw", "p_away"]].to_numpy()
    check(bool(((P >= 0) & (P <= 1)).all()), "las probabilidades viven en [0, 1]")
    check(np.allclose(P.sum(axis=1), 1.0), "y suman 1 en todas las filas")
    check(set(pred["y_num"].unique()) <= {0, 1, 2},
          "el objetivo llega codificado como H, D, A")

    # El precio crudo viaja intacto: es lo que hace comparable el modelo con el
    # mercado sobre exactamente el mismo conjunto de partidos.
    check("mkt_p_home_raw" in pred.columns,
          "la probabilidad implicita del mercado se adjunta sin transformar")
    orden = liga.set_index("match_key").loc[pred["match_key"], "mkt_p_home"]
    check(np.allclose(pred["mkt_p_home_raw"].to_numpy(), orden.to_numpy()),
          "y coincide con la del artefacto de partida, partido a partido")

    n_train = pred.groupby("fold")["n_train"].first()
    check(bool((n_train.to_numpy() == np.array([f.n_train for f in folds])).all()),
          "cada fila declara el tamano de la ventana con la que se entreno")
    check(bool(n_train.is_monotonic_increasing),
          "que con ventana expansiva crece pliegue a pliegue")

    check(pred.attrs["n_folds"] == len(folds)
          and pred.attrs["config"] is cfg
          and bool(pred.attrs["states"]),
          "la configuracion y los estados viajan adjuntos a las predicciones")

    rep = pt.protocol_report(pred, verbose=False)
    check(set(rep) == {"protocolo", "pliegues", "partidos", "temporadas",
                       "ventana_media", "estados_ajustados"},
          "protocol_report resume la ejecucion sin recalcular nada")
    check(rep["partidos"] == len(pred) and rep["temporadas"] == _VALIDACION,
          "y su resumen cuadra con la salida que describe")
    check("expansiva" in cfg.describe() and "semanal" in cfg.describe(),
          "la configuracion se describe en una linea legible")

    # Peso de recencia: el estimador debe admitirlo, y si no, se dice.
    cfg_w = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False,
                              halflife_days=180.0)
    try:
        pt.run_protocol(liga, _Frecuencias, cfg_w, folds=folds[:1])
        check(False, "pedir peso a un estimador que no lo admite es un error")
    except TypeError as e:
        check("sample_weight" in str(e),
              "pedir peso a un estimador que no lo admite es un error")
    con_peso = pt.run_protocol(liga, _FrecuenciasConPesos, cfg_w, folds=folds[:1])
    check(len(con_peso) == folds[0].n_eval and np.allclose(
        con_peso[["p_home", "p_draw", "p_away"]].sum(axis=1), 1.0),
        "y con un estimador que si lo admite la ejecucion es normal")

    # Sin pliegues no hay ejecucion silenciosa.
    try:
        pt.run_protocol(liga, _Frecuencias, cfg, folds=[])
        check(False, "una ejecucion sin pliegues se detiene con error explicito")
    except ValueError as e:
        check("no ha generado" in str(e),
              "una ejecucion sin pliegues se detiene con error explicito")


def test_fabrica_por_pliegue() -> None:
    print("\n8. FABRICA DEPENDIENTE DEL PLIEGUE")
    liga = _liga()
    cfg = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False)
    folds = pt.make_folds(liga, cfg)[:3]

    # Una fabrica que declara `fold` por firma lo recibe; la que no, no.
    vistos = []

    def por_pliegue(fold):
        vistos.append((fold.idx, fold.season, fold.jornada))
        return _Frecuencias()

    pred = pt.run_protocol(liga, por_pliegue, cfg, folds=folds)
    check(len(vistos) == len(folds),
          "la fabrica que declara `fold` se llama una vez por pliegue")
    check([v[0] for v in vistos] == [f.idx for f in folds],
          "y recibe el pliegue que le toca, en su orden")
    check(len(pred) == sum(f.n_eval for f in folds),
          "la ejecucion con fabrica por pliegue produce la misma salida de siempre")

    llana = pt.run_protocol(liga, _Frecuencias, cfg, folds=folds)
    check(np.allclose(pred[["p_home", "p_draw", "p_away"]].to_numpy(),
                      llana[["p_home", "p_draw", "p_away"]].to_numpy()),
          "y si la configuracion no cambia, el resultado es identico al de la fabrica simple")

    # Lo que la fabrica recibe es la IDENTIDAD del pliegue, no sus partidos: el
    # objeto `Fold` trae indices de ventana y de evaluacion, y el contrato es que
    # se use para elegir configuracion ya decidida, no para mirar el futuro.
    f0 = folds[0]
    check(max(pd.to_datetime(liga.loc[f0.train_idx, "date"])) < f0.fecha_ini,
          "el pliegue que recibe la fabrica separa ventana y evaluacion por fecha")

    # Declaracion explicita: un invocable envuelto no tiene firma informativa y
    # anuncia lo que acepta con `ACEPTA`.
    class _Envuelta:
        ACEPTA = ("fold",)

        def __init__(self):
            self.n = 0

        def __call__(self, **kw):
            self.n += 1
            self.ultimo = kw.get("fold")
            return _Frecuencias()

    env = _Envuelta()
    check(pt._acepta(env, "fold"),
          "un invocable puede DECLARAR que acepta el pliegue con `ACEPTA`")
    pt.run_protocol(liga, env, cfg, folds=folds[:1])
    check(env.n == 1 and env.ultimo is folds[0],
          "y el protocolo se lo pasa aunque su firma no lo diga")

    sin_declarar = lambda **kw: _Frecuencias()          # noqa: E731
    check(not pt._acepta(sin_declarar, "fold"),
          "un `**kwargs` sin declaracion no se interpreta como aceptacion")

    # `ACEPTA` es una declaracion, no una suposicion: lo que no figura, no entra.
    class _Parcial:
        ACEPTA = ("fold",)

    check(not pt._acepta(_Parcial(), "contexto"),
          "y lo que `ACEPTA` no nombra se considera no aceptado")


# ---------------------------------------------------------------------------
# 9. el memo del preprocesado
# ---------------------------------------------------------------------------
def _espiar_apply(fn):
    """Ejecuta `fn` registrando cuantas filas recibe cada llamada a `pp.apply`."""
    original = pt.pp.apply
    tamanos: list[int] = []

    def espia(df, *a, **k):
        tamanos.append(len(df))
        return original(df, *a, **k)

    pt.pp.apply = espia
    try:
        fn()
    finally:
        pt.pp.apply = original
    return tamanos


class _MiraLasX(_Frecuencias):
    """Frecuencia de clase inclinada por la primera columna de la matriz.

    Para comparar dos caminos de transformacion hace falta un estimador que de
    verdad LEA las features: el de frecuencias daria la misma salida con
    cualquier matriz, incluso con una mal transformada, de modo que con el la
    comparacion saldria bien sin demostrar nada.
    """

    def fit(self, X, y):
        super().fit(X, y)
        self._col = X.columns[0]
        return self

    def predict_proba(self, X):
        z = np.tanh(X[self._col].to_numpy(dtype="float64"))
        p = np.tile(self._p, (len(X), 1))
        p[:, 0] = p[:, 0] + 0.1 * z
        p[:, 2] = p[:, 2] - 0.1 * z
        return p / p.sum(axis=1, keepdims=True)


def test_memo_preprocesado() -> None:
    print("\n9. MEMO DEL PREPROCESADO - transformar una vez por estado, no por pliegue")
    liga = _liga()
    # Con `transform=True` para que el memo tenga que respetar tambien las lambda
    # de Yeo-Johnson, que es el paso del preprocesado mas facil de romper.
    cfg = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=True,
                            refit_cadence="temporada")
    folds = pt.make_folds(liga, cfg)

    # Lo que hay que demostrar es la equivalencia, y se demuestra pliegue a
    # pliegue: la matriz que el memo entrega es la misma, valor a valor, que la de
    # transformar unicamente la ventana. Lo es porque `preprocess.apply` trabaja
    # fila a fila con parametros congelados en el estado, asi que transformar de
    # mas no contamina lo que el pliegue usa.
    cache = pt._StateCache(liga, cfg)
    for fold in (folds[0], folds[1], folds[len(folds) // 2], folds[-1]):
        aj = pt.fit_fold(liga, _MiraLasX, cfg, fold, cache=cache)
        st = cache.get(fold)
        tr = pt.pp.apply(liga.loc[fold.train_idx], st, keep_meta=True)
        ev = pt.pp.apply(liga.loc[fold.eval_idx], st, keep_meta=True)
        check(list(aj.Xtr.index) == list(tr.index)
              and list(aj.Xev.index) == list(ev.index),
              f"pliegue {fold.idx}: el memo entrega las mismas filas, en su orden")
        check(np.array_equal(aj.Xtr.to_numpy(dtype="float64"),
                             tr[st.feature_cols].to_numpy(dtype="float64")),
              f"pliegue {fold.idx}: con los mismos valores exactos en la ventana")
        check(np.array_equal(aj.Xev.to_numpy(dtype="float64"),
                             ev[st.feature_cols].to_numpy(dtype="float64")),
              f"pliegue {fold.idx}: y en la evaluacion")

    # Y de punta a punta: la ejecucion completa con memo y la misma sin el dan la
    # misma prediccion para los mismos partidos.
    class _SinMemo(pt._StateCache):
        def aplicado(self, fold, feat):
            return None

    cols = ["p_home", "p_draw", "p_away"]
    con = pt.run_protocol(liga, _MiraLasX, cfg)
    sin = pt.run_protocol(liga, _MiraLasX, cfg, cache=_SinMemo(liga, cfg))
    check(list(con["match_key"]) == list(sin["match_key"]),
          "con memo y sin memo se evaluan los mismos partidos")
    check(np.array_equal(con[cols].to_numpy(dtype="float64"),
                         sin[cols].to_numpy(dtype="float64")),
          "y la prediccion es identica bit a bit")

    # El ahorro, medido: una transformacion por estado en lugar de dos por pliegue.
    tam_memo = _espiar_apply(lambda: pt.run_protocol(liga, _Frecuencias, cfg))
    check(len(tam_memo) == len(_VALIDACION),
          "la cadencia por temporada transforma una vez por temporada evaluada")
    check(all(n == len(liga) for n in tam_memo),
          "y transforma el marco entero, del que cada pliegue toma sus dos trozos")

    # Con cadencia por jornada el memo se abstiene: cada pliegue estrena estado, de
    # modo que compartir no ahorra nada y transformar el marco entero costaria mas
    # que transformar la ventana.
    cfg_j = pt.ProtocolConfig(min_train_matches=_MIN_TRAIN, transform=False,
                              refit_cadence="jornada")
    pred_j = {}
    tam_j = _espiar_apply(
        lambda: pred_j.update(p=pt.run_protocol(liga, _Frecuencias, cfg_j)))
    check(len(tam_j) == 2 * pred_j["p"].attrs["n_folds"],
          "con cadencia por jornada cada pliegue transforma su ventana y su jornada")
    check(all(n < len(liga) for n in tam_j),
          "y ninguna de esas llamadas toca el marco completo")

    # El memo afirma algo sobre el contenido de UN marco. Si le llega otro, se
    # abstiene en lugar de devolver filas transformadas que no son las suyas.
    check(pt._StateCache(liga, cfg).aplicado(folds[0], liga.copy()) is None,
          "con un marco distinto del suyo el memo se abstiene")


def _run() -> None:
    print("=" * 72)
    print("TEST - protocolo de validacion temporal")
    print("=" * 72)
    test_jornada()
    test_pliegues()
    test_pesos()
    test_cadencia()
    test_antifuga()
    test_probabilidades()
    test_contrato()
    test_fabrica_por_pliegue()
    test_memo_preprocesado()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_protocol() -> None:
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
