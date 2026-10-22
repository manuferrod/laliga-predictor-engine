"""Validacion offline de las features derivadas pre-partido. Sin red y sin datos.

El bloque de estructura del precio se construye sobre columnas que la fuente publica
con dos decimales y con dos nomenclaturas historicas distintas, de modo que la clase
de fallo que importa no es una excepcion sino una columna que sale con un valor
plausible y equivocado: un overround imposible que se cuela como si fuera el margen
de la casa, una dispersion negativa nacida de dos columnas cruzadas en el origen, o
una divergencia con Pinnacle calculada sobre un trio de cuotas corrupto. Nada de eso
levanta un error por si solo; el modelo simplemente aprende de ruido.

Se comprueban, sobre casos sinteticos construidos a mano con el resultado conocido:

  1. ARITMETICA   el margen, la dispersion, la probabilidad de Pinnacle y la
                  distancia de variacion total valen lo que deben valer.
  2. GUARDAS      cuota <= 1, overround fuera de rango, `max` por debajo de `avg` y
                  dispersion desmedida se anulan en lugar de propagarse.
  3. ACOPLAMIENTO un trio corrupto anula las tres salidas que dependen de el, no
                  solo la que contiene la cuota mala.
  4. REGIMEN      `columns.weekly_unavailable` reconoce todo el bloque de cierre y,
                  por un segundo motivo que no es el momento sino el proveedor, la
                  parte de la estructura del precio que se calcula contra la casa
                  afilada. El bloque queda partido entre los dos regimenes, y eso es
                  lo que mantiene al modelo publicado libre de datos que no
                  existiran al predecir.
  5. CONTRATO     `prematch_derived_feature_names` describe exactamente lo que
                  `build_prematch_derived_features` produce.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import columns as C  # noqa: E402
import prematch_derived as pdrv  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []
_TOL = 1e-9


def check(cond: bool, msg: str, detalle: object = None) -> None:
    """Registra una comprobacion; `detalle` solo se imprime cuando falla."""
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")
    if not cond and detalle is not None:
        print(f"         -> {detalle}")


def _master(filas: list[dict]) -> pd.DataFrame:
    """Master sintetico minimo: solo lo que el modulo lee, con match_key correlativo."""
    df = pd.DataFrame(filas)
    df.insert(0, "match_key", [f"2020-01-0{i + 1}|casa|fuera" for i in range(len(df))])
    for c in ("home_elo", "away_elo", "elo_diff"):
        if c not in df.columns:
            df[c] = np.nan
    return df


def _fila(**kw) -> dict:
    """Fila con las 12 cuotas de apertura y las 3 de cierre a NaN por defecto."""
    base = {c: np.nan for c in (
        "odds_avg_h", "odds_avg_d", "odds_avg_a",
        "odds_max_h", "odds_max_d", "odds_max_a",
        "odds_ps_h", "odds_ps_d", "odds_ps_a",
        "odds_b365_h", "odds_b365_d", "odds_b365_a",
        "odds_close_avg_h", "odds_close_avg_d", "odds_close_avg_a",
        "odds_b365_close_h", "odds_b365_close_d", "odds_b365_close_a")}
    base.update(kw)
    return base


def _estructura(m: pd.DataFrame) -> pd.DataFrame:
    """Bloque de estructura del precio ANTES de la proyeccion del catalogo.

    La aritmetica se comprueba sobre el bloque interno y no sobre la salida del
    modulo porque el catalogo publica solo el resumen: de los tres diferenciales
    por signo sale una dispersion media, y de la probabilidad de Pinnacle sale la
    divergencia contra el consenso. Los pasos intermedios son calculo, no lectura
    —decir tres veces el mismo margen visto desde cada signo es la clase de
    redundancia que la curacion existe para eliminar—, pero siguen siendo el sitio
    donde un error de formula aparece primero. Que la salida publique exactamente
    lo declarado se comprueba aparte, en [5] CONTRATO.
    """
    return pdrv._price_structure(m)


def _movimiento(m: pd.DataFrame) -> pd.DataFrame:
    """Bloque de movimiento de linea antes de proyectar (ver `_estructura`)."""
    return pdrv._line_movement(m)


# ---------------------------------------------------------------------------
# 1. aritmetica del bloque
# ---------------------------------------------------------------------------
def test_aritmetica() -> None:
    print("\n1. ARITMETICA — el numero que sale es el que debe salir")
    # Trio de consenso con margen conocido: 1/2 + 1/4 + 1/4 = 1.00 exacto.
    # Pinnacle mas caro en el local (2.20) => probabilidad local menor.
    m = _master([_fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0,
                       odds_max_h=2.2, odds_max_d=4.0, odds_max_a=4.4,
                       odds_ps_h=2.2, odds_ps_d=4.0, odds_ps_a=4.0,
                       odds_b365_h=1.9, odds_b365_d=3.8, odds_b365_a=4.2)])
    out = _estructura(m).iloc[0]

    check(abs(out["mkt_overround_open"] - 1.0) < _TOL,
          f"overround de apertura = 1/2+1/4+1/4 = 1.00 (obtenido {out['mkt_overround_open']:.6f})")
    # dispersion relativa: (2.2-2.0)/2.0 = 0.10 ; empate sin dispersion ; (4.4-4.0)/4.0 = 0.10
    check(abs(out["mkt_spread_home"] - 0.10) < _TOL, "dispersion local = (2.20-2.00)/2.00 = 0.10")
    check(abs(out["mkt_spread_draw"]) < _TOL, "dispersion del empate = 0 cuando max == avg")
    check(abs(out["mkt_spread_mean"] - (0.10 + 0.0 + 0.10) / 3) < _TOL,
          "dispersion media = media de las tres")

    ps = 1 / 2.2 + 1 / 4.0 + 1 / 4.0
    check(abs(out["mkt_overround_ps"] - ps) < _TOL,
          f"overround de Pinnacle = suma de inversas sin normalizar ({ps:.6f})")
    check(abs(out["mkt_p_ps_home"] - (1 / 2.2) / ps) < _TOL,
          "probabilidad local de Pinnacle = inversa normalizada por su propio margen")
    check(abs(out["mkt_p_ps_home"] + out["mkt_p_ps_draw"] + out["mkt_p_ps_away"] - 1.0) < _TOL,
          "las tres probabilidades de Pinnacle suman 1 (des-margen)")

    # Bet365 se lee con el MISMO procedimiento. Es lo que hace comparables las tres
    # referencias: si cada una se des-marginara de otra forma, la diferencia entre
    # ellas mediria el metodo y no el precio.
    b3 = 1 / 1.9 + 1 / 3.8 + 1 / 4.2
    check(abs(out["mkt_p_b365_home"] - (1 / 1.9) / b3) < _TOL,
          "probabilidad local de Bet365 = inversa normalizada por su propio margen")
    check(abs(out["mkt_p_b365_home"] + out["mkt_p_b365_draw"]
              + out["mkt_p_b365_away"] - 1.0) < _TOL,
          "las tres probabilidades de Bet365 suman 1 (des-margen)")

    # divergencia: Pinnacle paga mas al local, luego le da MENOS probabilidad.
    check(out["sharp_div_home"] < 0, "divergencia local negativa: Pinnacle paga mas al local")
    tv = np.mean([abs(out[f"sharp_div_{k}"]) for k in ("home", "draw", "away")])
    check(abs(out["sharp_div_tv"] - tv) < _TOL,
          "sharp_div_tv = media de los valores absolutos (los signos no se cancelan)")

    # movimiento de linea: el cierre acorta al local, la deriva tiene que ser positiva
    m2 = _master([_fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0,
                        odds_close_avg_h=1.8, odds_close_avg_d=4.0,
                        odds_close_avg_a=4.6)])
    o2 = _movimiento(m2).iloc[0]
    check(o2["close_move_home"] > 0,
          "close_move_home > 0 cuando la cuota local se acorta entre apertura y cierre")
    check(abs(o2["close_move_fav"] - o2["close_move_home"]) < _TOL,
          "close_move_fav sigue al favorito de apertura (aqui el local)")
    check(abs(o2["close_move_home"] + o2["close_move_draw"]
              + o2["close_move_away"]) < 1e-12,
          "los tres desplazamientos suman 0: es una redistribucion de probabilidad")


# ---------------------------------------------------------------------------
# 2. guardas de validez
# ---------------------------------------------------------------------------
def test_guardas() -> None:
    print("\n2. GUARDAS — lo implausible se anula, no se propaga")
    casos = [
        ("cuota <= 1 (no es un precio)",
         _fila(odds_avg_h=1.0, odds_avg_d=4.0, odds_avg_a=4.0)),
        ("cuota 0 (inversa infinita)",
         _fila(odds_avg_h=0.0, odds_avg_d=4.0, odds_avg_a=4.0)),
        ("overround < 0.95 (la casa perderia dinero seguro)",
         _fila(odds_avg_h=5.0, odds_avg_d=5.0, odds_avg_a=5.0)),
        ("overround > 1.60 (margen ajeno a un mercado 1X2)",
         _fila(odds_avg_h=1.2, odds_avg_d=1.2, odds_avg_a=1.2)),
    ]
    m = _master([c[1] for c in casos])
    out = pdrv.build_prematch_derived_features(m)
    for i, (nombre, _) in enumerate(casos):
        check(pd.isna(out.at[i, "mkt_overround_open"]),
              f"overround anulado — {nombre}")

    # dispersion: max por debajo de avg delata columnas cruzadas en el origen
    m2 = _master([
        _fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0, odds_max_h=1.5,
              odds_max_d=4.0, odds_max_a=4.0),                       # max < avg
        _fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0, odds_max_h=5.0,
              odds_max_d=4.0, odds_max_a=4.0),                       # +150%: precio erroneo
        _fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0, odds_max_h=2.000001,
              odds_max_d=4.0, odds_max_a=4.0),                       # redondeo, se acepta
    ])
    o2 = _estructura(m2)
    check(pd.isna(o2.at[0, "mkt_spread_home"]),
          "dispersion anulada cuando la mejor cuota queda por debajo del consenso")
    check(pd.isna(o2.at[1, "mkt_spread_home"]),
          "dispersion anulada por encima del techo de plausibilidad (max > 2x consenso)")
    check(o2.at[2, "mkt_spread_home"] >= 0.0,
          "dispersion nunca negativa: el redondeo de la fuente se absorbe a 0")
    check(pd.isna(o2.at[0, "mkt_spread_mean"]),
          "la dispersion media es NaN si falta alguna de las tres (no se promedia a medias)")


# ---------------------------------------------------------------------------
# 3. acoplamiento del trio
# ---------------------------------------------------------------------------
def test_acoplamiento() -> None:
    print("\n3. ACOPLAMIENTO — una cuota corrupta invalida el trio entero")
    m = _master([_fila(odds_ps_h=2.0, odds_ps_d=4.0, odds_ps_a=0.0,
                       odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0)])
    out = _estructura(m).iloc[0]
    anuladas = [c for c in ("mkt_p_ps_home", "mkt_p_ps_draw", "mkt_p_ps_away")
                if pd.isna(out[c])]
    check(len(anuladas) == 3,
          "las tres probabilidades de Pinnacle se anulan, no solo la de la cuota mala "
          "(la normalizacion acopla los tres resultados)")
    check(all(pd.isna(out[f"sharp_div_{k}"]) for k in ("home", "draw", "away")),
          "la divergencia hereda la anulacion en lugar de restar contra un NaN parcial")
    check(pd.isna(out["sharp_div_tv"]), "el resumen escalar tambien queda indefinido")
    check(not pd.isna(out["mkt_overround_open"]),
          "el consenso, que esta sano, sobrevive: la anulacion no contagia a otro trio")


# ---------------------------------------------------------------------------
# 4. regimen de informacion
# ---------------------------------------------------------------------------
def test_regimen() -> None:
    print("\n4. REGIMEN — dos motivos de indisponibilidad, no uno")
    fuera = set(C.weekly_unavailable(pdrv.prematch_derived_feature_names()))
    check(set(pdrv.LINE_MOVEMENT) <= fuera,
          "el bloque de cierre queda fuera del regimen semanal, entero")
    check(all(C._CLOSING_TOKEN in c for c in pdrv.LINE_MOVEMENT),
          f"todo el bloque retrospectivo lleva el token '{C._CLOSING_TOKEN}' en el nombre")
    check(not any(C._CLOSING_TOKEN in c for c in pdrv.PRICE_STRUCTURE),
          "ninguna columna de estructura del precio lleva ese token: no es cierre")

    # Y aun asi tres de las cinco quedan fuera, por el SEGUNDO motivo. La estructura
    # del precio es de apertura entera, pero la que se calcula contra Pinnacle no es
    # servible: el feed de la jornada por jugar no publica esa casa. Que el bloque
    # salga partido entre los dos regimenes es lo correcto, y es la afirmacion que
    # este banco tiene que sostener para que nadie lo lea como homogeneo.
    afiladas = {c for c in pdrv.PRICE_STRUCTURE if c in set(C.SHARP_ODDS)}
    check(afiladas <= fuera,
          "la divergencia con la casa afilada tampoco entra al semanal, aun siendo apertura",
          sorted(afiladas - fuera))
    check(set(pdrv.PRICE_STRUCTURE) - afiladas and
          not ((set(pdrv.PRICE_STRUCTURE) - afiladas) & fuera),
          "lo que sale del consenso y de la minorista si entra: el bloque queda partido",
          sorted((set(pdrv.PRICE_STRUCTURE) - afiladas) & fuera))
    check(fuera == set(pdrv.LINE_MOVEMENT) | afiladas,
          "y no se excluye nada mas: cierre y casa afilada, sin sobras",
          sorted(fuera ^ (set(pdrv.LINE_MOVEMENT) | afiladas)))

    # la estructura del precio se computa SIN cuotas de cierre: es la prueba de que
    # lo que el regimen semanal se queda de este bloque sale con valor y no vacio.
    m = _master([_fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0,
                       odds_max_h=2.2, odds_max_d=4.2, odds_max_a=4.2,
                       odds_ps_h=2.1, odds_ps_d=4.0, odds_ps_a=4.1,
                       odds_b365_h=2.05, odds_b365_d=3.9, odds_b365_a=4.0)])
    out = pdrv.build_prematch_derived_features(m).iloc[0]
    semanales = [c for c in pdrv.PRICE_STRUCTURE if c not in fuera]
    vacias = [c for c in semanales if pd.isna(out[c])]
    check(not vacias,
          f"sin ninguna cuota de cierre, las {len(semanales)} columnas semanales "
          f"salen con valor (vacias: {vacias})")
    check(all(pd.isna(out[c]) for c in pdrv.LINE_MOVEMENT),
          "y las retrospectivas salen vacias, que es la respuesta correcta al predecir")

    # Sin Pinnacle —el caso REAL de la jornada por jugar— el modulo no se rompe: las
    # dos columnas que el semanal usa siguen saliendo con valor y las afiladas salen
    # vacias. Es lo que hace que excluirlas sea suficiente y no haga falta tocar el
    # calculo.
    sin_ps = _master([_fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0,
                            odds_max_h=2.2, odds_max_d=4.2, odds_max_a=4.2,
                            odds_b365_h=2.05, odds_b365_d=3.9, odds_b365_a=4.0)])
    out2 = pdrv.build_prematch_derived_features(sin_ps).iloc[0]
    check(not [c for c in semanales if pd.isna(out2[c])],
          "sin Pinnacle, las columnas del semanal siguen calculandose")
    check(all(pd.isna(out2[c]) for c in afiladas),
          "y las afiladas salen vacias, que es la averia que el regimen ya previene")


# ---------------------------------------------------------------------------
# 5. contrato del modulo
# ---------------------------------------------------------------------------
def test_contrato() -> None:
    print("\n5. CONTRATO — lo declarado y lo producido coinciden")
    m = _master([_fila(odds_avg_h=2.0, odds_avg_d=4.0, odds_avg_a=4.0)] * 3)
    m["home_elo"], m["away_elo"] = [1700, 1500, 1500], [1500, 1700, 1500]
    m["elo_diff"] = m["home_elo"] - m["away_elo"]
    out = pdrv.build_prematch_derived_features(m)

    declaradas = pdrv.prematch_derived_feature_names()
    check(list(out.columns) == ["match_key"] + declaradas,
          "las columnas producidas son exactamente las declaradas, en el mismo orden")
    check(len(out) == len(m) and out["match_key"].is_unique,
          "una fila por partido y match_key unico")

    # La logistica Elo ya NO produce columna. `elo_exp_home` era una
    # transformacion monotona de la diferencia de fuerza, que el par
    # `home_elo_z`/`away_elo_z` ya entrega: dar las dos es decir dos veces lo mismo
    # y regalarle al modelo una colinealidad perfecta. La funcion sobrevive porque
    # el analisis la necesita para leer el Elo en escala de probabilidad y
    # comprobar su calibracion, asi que sus propiedades se comprueban donde ahora
    # viven —sobre la funcion, en NIVEL de rating, que es la escala en la que los
    # 400 puntos de la logistica significan algo— y aqui solo se vigila que la
    # columna no haya vuelto.
    check(not any("elo" in c for c in declaradas),
          "el modulo no publica ninguna columna derivada del Elo (la diferencia ya esta)",
          [c for c in declaradas if "elo" in c])
    e = [pdrv.elo_expected_score(d, pdrv.HOME_ADV_ELO) for d in (200.0, -200.0, 0.0)]
    check(all(0 < x < 1 for x in e), "la expectativa Elo es una probabilidad en (0,1)")
    check(e[0] > e[2] > e[1],
          "y ordena por diferencia de Elo (+200 > empatados > -200)")
    check(e[2] > 0.5,
          "con Elo empatado la expectativa local supera 0.5: la ventaja de campo esta dentro")

    # un master sin ninguna columna de cuotas no puede reventar: los partidos
    # futuros llegan asi antes de que la fuente publique el precio.
    vacio = pd.DataFrame({"match_key": ["k1", "k2"], "elo_diff": [0.0, 100.0]})
    o = pdrv.build_prematch_derived_features(vacio)
    check(list(o.columns) == ["match_key"] + declaradas,
          "sin ninguna columna de cuotas el esquema de salida se mantiene (todo NaN)")


def _run() -> None:
    print("=" * 72)
    print("TEST — features derivadas pre-partido (estructura del precio y linea)")
    print("=" * 72)
    test_aritmetica()
    test_guardas()
    test_acoplamiento()
    test_regimen()
    test_contrato()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_prematch_derived() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
