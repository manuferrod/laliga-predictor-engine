"""Validacion offline de la POLITICA DE IMPUTACION POR CAUSA (preprocess.py, D2).

Un valor ausente no significa lo mismo en todas las columnas, y rellenarlos todos
con la mediana equivale a afirmar que el partido es promedio en aquello que
precisamente no se sabe. El modulo distingue por eso cuatro causas y solo una de
ellas —`neutral`— estima; las otras tres deducen el relleno del significado de la
variable.

La causa `known` es la mas delicada de las cuatro porque el hueco que trata NO es
ignorancia. El bloque de trascendencia se calcula sobre la clasificacion PREVIA al
partido, de modo que en la primera jornada llega vacio; pero esa situacion esta
perfectamente determinada: con la liga a cero, toda distancia a un puesto de corte
vale 0, ningun objetivo esta decidido y restan los 38 partidos del calendario.
Sustituir esas cifras por la mediana de la temporada introduciria una jornada 1 en
la que los equipos arrancan, por ejemplo, a doce puntos del liderato.

Lo que estas comprobaciones fijan es esa frontera: que cada columna del bloque
recibe su valor cierto, que las dos columnas del mismo bloque que si son
genuinamente desconocidas en la jornada 1 —posicion previa y su variacion— siguen
estimandose, y que la precedencia entre causas no reasigna ninguna columna de las
otras tres familias.

Cierra el banco una comprobacion estructural sobre `apply`: lo que la funcion
adjunta al artefacto —los flags de faltante y la copia cruda del precio— no puede
duplicarse si el marco de entrada ya paso por ella.
"""
import pathlib
import sys

import numpy as np
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "features",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "features",
]
FEAT = next((p for p in _CANDS if (p / "preprocess.py").exists()), _CANDS[0])
sys.path.insert(0, str(FEAT))
import catalogo                   # noqa: E402
import preprocess as P            # noqa: E402
import standings as S             # noqa: E402

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


def _muestra(n=200, loc=7.0, escala=3.0) -> pd.Series:
    """Serie continua cualquiera: sirve para exigir que un relleno NO la mire."""
    rng = np.random.default_rng(0)
    return pd.Series(rng.normal(loc, escala, n))


# Nombres reales que llegan a la matriz. La expansion no se escribe aqui: se pide
# al catalogo, que es quien decide para cada atributo si sale como par, como
# diferencia o si no sale. Escribirla a mano —las tres lecturas de cada atributo—
# metia en el banco columnas que el modulo calcula pero no publica (`stakes_none`,
# `europe_unreachable`: insumos del indicador de partido), y exigirle a la politica
# de imputacion una respuesta sobre una columna que nadie va a imputar es pedirle
# que se pronuncie sobre lo que no existe.
_NOMBRES = S.standings_feature_names()
COLS_TRASCENDENCIA = []
for _c in _NOMBRES["numeric"]:
    COLS_TRASCENDENCIA += catalogo.nombres(_c)
COLS_TRASCENDENCIA += _NOMBRES["match_level"]

# Las dos del bloque que en la jornada 1 no admiten lectura exacta: sin
# clasificacion previa no hay posicion de la que partir ni variacion que medir.
DESCONOCIDAS = ["prev_position", "dynamic_pos_change"]

# --------------------------------------------------------------------------- #
print("== la distancia a un corte vale exactamente cero al arrancar ==")
for base in ("points_above_drop",):
    for c in catalogo.nombres(base):
        est = P._impute_strategy(c)
        val = P._impute_fill_value(c, _muestra())
        check(f"{c}: causa 'known' con relleno 0",
              est == "known" and val == 0.0, f"{est} / {val}")
check("y hay al menos una columna que ejercita la regla",
      len(catalogo.nombres("points_above_drop")) > 0)
# Las otras tres distancias —lider, cuarto, sexto— no se emiten: al restar local
# menos visitante los puntos del corte se cancelan y las tres son la de arriba
# salvo el signo. El catalogo es quien lo decide, y la politica de imputacion no
# tiene por que seguir reconociendo nombres que ya no llegan a la matriz.
for base in ("points_to_leader", "points_to_champions", "points_to_europa"):
    check(f"{base}: el catalogo no lo emite", catalogo.nombres(base) == [],
          catalogo.nombres(base))

# --------------------------------------------------------------------------- #
print("\n== ningun objetivo esta decidido tras cero jornadas ==")
# De los tres indicadores que `standings` calcula solo dos llegan a la matriz: la
# salvacion matematica como diferencia y la falta de trascendencia como columna de
# partido. `europe_unreachable` se queda dentro del modulo por ser el complemento
# casi determinista de la distancia a Europa, asi que no se le exige relleno.
for base in ("safe_from_drop",):
    for c in catalogo.nombres(base):
        val = P._impute_fill_value(c, _muestra(loc=0.5, escala=0.1))
        check(f"{c}: indicador apagado (0)",
              P._impute_strategy(c) == "known" and val == 0.0, val)
check("stakes_none_match tambien queda apagado",
      P._impute_strategy("stakes_none_match") == "known"
      and P._impute_fill_value("stakes_none_match", _muestra()) == 0.0)

# --------------------------------------------------------------------------- #
print("\n== restan los 38 partidos del calendario ==")
check("home_matches_left = 38",
      P._impute_fill_value("home_matches_left", _muestra()) == 38.0)
# La diferencia no llega a la matriz —salvo aplazamiento, los dos equipos tienen el
# mismo calendario pendiente y la resta es cero casi siempre—, pero la regla la
# cubre igualmente porque el mismo relleno tiene que valer si algun dia sale.
check("matches_left_diff = 0 (ambos lados arrancan igual)",
      P._impute_fill_value("matches_left_diff", _muestra()) == 0.0)
check("la cifra sale de la constante del modulo, no de un literal suelto",
      P._TOTAL_MATCHES == 38)

# --------------------------------------------------------------------------- #
print("\n== lo genuinamente desconocido se sigue estimando ==")
for base in DESCONOCIDAS:
    for c in catalogo.nombres(base):
        check(f"{c}: causa 'neutral'", P._impute_strategy(c) == "neutral",
              P._impute_strategy(c))
s = _muestra(loc=10.0)
check("y su relleno es la mediana de dev",
      P._impute_fill_value("prev_position_diff", s) == float(s.median()))
check("la zona de la tabla tampoco se deduce en la jornada 1",
      P._impute_strategy("home_position_zone") == "neutral")

# --------------------------------------------------------------------------- #
print("\n== la cobertura del bloque de trascendencia es completa y exacta ==")
cubiertas = [c for c in COLS_TRASCENDENCIA if P._impute_strategy(c) == "known"]
esperadas = [c for c in COLS_TRASCENDENCIA
             if not any(d in c for d in DESCONOCIDAS)]
check("toda columna del bloque con lectura exacta la recibe",
      set(cubiertas) == set(esperadas),
      sorted(set(esperadas) ^ set(cubiertas)))
check("y las dos desconocidas quedan fuera de 'known'",
      not [c for c in COLS_TRASCENDENCIA
           if any(d in c for d in DESCONOCIDAS)
           and P._impute_strategy(c) == "known"])
check("_known_cold_start devuelve None fuera del bloque",
      P._known_cold_start("home_form_points_ewm") is None
      and P._known_cold_start("home_elo_z") is None
      and P._known_cold_start("mkt_p_home") is None)

# --------------------------------------------------------------------------- #
print("\n== la precedencia entre causas no reasigna las otras familias ==")
for c in sorted(P._WEAK_PRIOR_COLS):
    check(f"{c}: sigue en 'weak_prior'", P._impute_strategy(c) == "weak_prior",
          P._impute_strategy(c))
for c in ("h2h_gd_ewm_diff", "h2h_win_rate_ewm_diff", "venue_form_gd_5_diff"):
    check(f"{c}: sigue en 'zero'", P._impute_strategy(c) == "zero",
          P._impute_strategy(c))
for c in ("form_points_ewm_diff", "away_out_value_pct", "home_elo_z", "age_diff"):
    check(f"{c}: sigue en 'neutral'", P._impute_strategy(c) == "neutral",
          P._impute_strategy(c))
# El Elo es el caso en que 'neutral' no es un reparto por defecto sino la lectura
# exacta: la columna viaja tipificada dentro de la jornada, asi que su mediana de
# dev es cero por construccion y rellenar con ella dice "equipo en la media de
# Primera ese dia", que es precisamente lo que no se sabe cuando falta el rating.
# Tampoco tiene arranque en frio, y esa es la razon por la que el Elo esta en el
# catalogo: es continuo entre temporadas, de modo que un hueco suyo solo puede
# significar que la fuente no publico, nunca que la temporada acaba de empezar.
check("el Elo tipificado no tiene arranque en frio: es continuo entre temporadas",
      P._known_cold_start("home_elo_z") is None
      and P._known_cold_start("away_elo_z") is None)
check("el cuantil de los ascendidos es una cola, no la mediana",
      P._WEAK_PRIOR_Q < 0.5)
s2 = _muestra(loc=300.0, escala=120.0)
check("y se lee de dev",
      P._impute_fill_value("home_squad_value", s2)
      == float(s2.quantile(P._WEAK_PRIOR_Q)))

# --------------------------------------------------------------------------- #
print("\n== los rellenos deducidos no dependen de la muestra ==")
# Es la propiedad que los hace inmunes a la deriva entre dev, holdout e
# inferencia: dos muestras muy distintas producen el mismo valor, mientras que la
# mediana se mueve con ellas.
a, b = _muestra(loc=1.0, escala=0.2), _muestra(loc=90.0, escala=40.0)
for c in ("points_above_drop_diff", "home_matches_left", "stakes_none_match",
          "h2h_gd_ewm_diff"):
    check(f"{c}: mismo relleno con muestras dispares",
          P._impute_fill_value(c, a) == P._impute_fill_value(c, b),
          f"{P._impute_fill_value(c, a)} vs {P._impute_fill_value(c, b)}")
check("la mediana, en cambio, si se mueve (control del test)",
      P._impute_fill_value("form_points_ewm_diff", a)
      != P._impute_fill_value("form_points_ewm_diff", b))

# --------------------------------------------------------------------------- #
print("\n== las cuatro causas son un particion, sin quinta categoria ==")
universo = (COLS_TRASCENDENCIA + sorted(P._WEAK_PRIOR_COLS)
            + ["h2h_gd_ewm_diff", "venue_form_gd_5_diff", "form_points_ewm_diff",
               "home_elo_z", "mkt_p_home", "home_position_zone"])
causas = {P._impute_strategy(c) for c in universo}
check("no aparece ninguna causa fuera de las declaradas",
      causas <= {"weak_prior", "zero", "known", "neutral"}, sorted(causas))
check("las cuatro se ejercitan en esta muestra",
      causas == {"weak_prior", "zero", "known", "neutral"}, sorted(causas))

# --------------------------------------------------------------------------- #
print("\n== volver a preprocesar un artefacto no duplica lo que se le adjunta ==")
# `apply` adjunta al artefacto dos cosas que no venian en el marco: los flags de
# faltante y una copia CRUDA de la probabilidad implicita del mercado. La copia se
# toma antes de imputar y escalar porque despues esas columnas ya no son
# probabilidades y el numero a batir no seria calculable. Un marco que ya paso por
# aqui trae por tanto ambas puestas, y volver a pasarlo —al reconstruir un
# artefacto, o al encadenar la ruta de inferencia sobre uno ya preprocesado— las
# adjuntaria por segunda vez. El sintoma no aparece aqui sino mucho despues: quien
# escriba `df["mkt_p_home_raw"]` recibiria una matriz de dos columnas donde espera
# una serie, y la comparacion contra el mercado fallaria sin decir por que.
_st = P.PreprocessState(
    flag_cols={"flag_prueba": "home_form_points_ewm"},
    impute_value={"home_form_points_ewm": 0.0},
    scale_mean={"home_form_points_ewm": 0.0, "mkt_p_home": 0.0},
    scale_std={"home_form_points_ewm": 1.0, "mkt_p_home": 1.0},
    feature_cols=["home_form_points_ewm", "mkt_p_home", "mkt_p_draw", "mkt_p_away",
                  "flag_prueba"],
)
_n = 6
_crudo = pd.DataFrame({
    "match_key": [f"k{i}" for i in range(_n)],
    "season": "2019/20",
    "date": pd.to_datetime("2019-08-17") + pd.to_timedelta(np.arange(_n), unit="D"),
    "home_team": "A", "away_team": "B",
    P.TARGET_NUM: np.arange(_n) % 3,
    "home_form_points_ewm": np.linspace(-2.0, 2.0, _n),
    "mkt_p_home": np.full(_n, 0.50), "mkt_p_draw": np.full(_n, 0.25),
    "mkt_p_away": np.full(_n, 0.25),
})
_una = P.apply(_crudo, _st)
check("la primera pasada adjunta la copia cruda del precio",
      "mkt_p_home_raw" in _una.columns
      and float(_una["mkt_p_home_raw"].iloc[0]) == 0.50)

_dos = P.apply(_una, _st)
_repes = [c for c in _dos.columns if list(_dos.columns).count(c) > 1]
check("volver a aplicar no repite ninguna columna", not _repes, sorted(set(_repes)))
check("y la copia cruda sigue siendo una serie, no una matriz de dos columnas",
      _dos["mkt_p_home_raw"].ndim == 1)
check("las columnas de la segunda pasada son exactamente las de la primera",
      list(_dos.columns) == list(_una.columns),
      f"{list(_una.columns)} vs {list(_dos.columns)}")

# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_imputacion():
    """Punto de entrada para `pytest` (ver test_scoring_gates.py)."""
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
