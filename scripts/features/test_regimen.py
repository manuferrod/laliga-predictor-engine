"""Validacion offline del REGIMEN DE INFORMACION (columns.py). Sin red y sin datos.

Ser pre-partido y ser conocible en el momento de predecir son propiedades
distintas. La prediccion de una jornada se emite con dias de antelacion; el precio
de cierre solo existe minutos antes del saque. Una columna de cierre entre las
features del modelo publicado no produce ningun error visible: `apply()` la imputa
con la mediana de dev y las probabilidades salen con aspecto normal, de modo que la
metrica historica describiria un sistema distinto del que se ejecuta cada semana.

El bloque de cierre se identifica por un CONVENIO DE NOMBRES —toda columna derivada
del cierre lleva el token `close`— respaldado por una lista explicita. Estas
comprobaciones fijan las dos propiedades de las que depende ese diseno: que el
predicado captura todo el bloque sin arrastrar la apertura, y que una columna nueva
que respete el convenio queda cubierta sin tocar ninguna lista.

Hay un SEGUNDO motivo de indisponibilidad, que no se resuelve por convenio porque no
tiene token utilizable: la casa afilada. `odds_ps_*` es cuota de APERTURA y por tanto
pre-partido legitima, pero el feed de la jornada por jugar no publica Pinnacle, asi
que al emitir llega vacia. Es la misma averia que el cierre por un camino distinto, y
distinguir «de apertura» de «servible» es justo el error de categoria que este modulo
existe para no cometer. Se fija aqui que las dos vias conviven sin confundirse.
"""
import pathlib
import sys

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "features",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "features",
]
FEAT = next((p for p in _CANDS if (p / "columns.py").exists()), _CANDS[0])
sys.path.insert(0, str(FEAT))
import columns as C  # noqa: E402

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


# Muestra representativa del master: identidad, apertura, cierre, mercado y el
# resto de bloques pre-partido.
MASTER = [
    "match_key", "date", "home_team", "away_team", "y", "y_num", "split",
    "odds_avg_h", "odds_avg_d", "odds_avg_a",
    "odds_ps_h", "odds_ps_d", "odds_ps_a",
    "odds_b365_h", "odds_max_h", "odds_over25", "ah_line", "odds_ah_home",
    "odds_close_avg_h", "odds_close_avg_d", "odds_close_avg_a",
    "odds_close_max_h", "odds_b365_close_h", "odds_close_over25",
    "ah_close_line", "odds_ah_close_home",
    "mkt_p_home", "mkt_p_draw", "mkt_p_away",
    "home_elo", "away_elo", "elo_diff", "home_elo_z", "away_elo_z",
    "form_points_ewm_diff", "h2h_gd_ewm_diff", "home_squad_value",
    "home_out_value_pct",
]

# --------------------------------------------------------------------------- #
print("== el predicado separa cierre de apertura ==")
fuera = C.weekly_unavailable(MASTER)
CIERRE = {"odds_close_avg_h", "odds_close_avg_d", "odds_close_avg_a",
          "odds_close_max_h", "odds_b365_close_h", "odds_close_over25",
          "ah_close_line", "odds_ah_close_home"}
AFILADA = {"odds_ps_h", "odds_ps_d", "odds_ps_a"}
check("captura todas las columnas con el token 'close'",
      {c for c in MASTER if "close" in c} <= set(fuera), sorted(fuera))
check("y el bloque de cierre esta entero",
      CIERRE <= set(fuera), sorted(CIERRE - set(fuera)))
check("lo que queda fuera son el cierre y la casa afilada, y nada mas",
      set(fuera) == CIERRE | AFILADA, sorted(set(fuera) ^ (CIERRE | AFILADA)))
check("la apertura del consenso y de la minorista NO se arrastra",
      not ({"odds_avg_h", "odds_b365_h", "odds_max_h",
            "odds_over25", "ah_line", "odds_ah_home"} & set(fuera)))
check("la probabilidad implicita del mercado tampoco (sale del consenso)",
      not ({"mkt_p_home", "mkt_p_draw", "mkt_p_away"} & set(fuera)))
check("CLOSING_ODDS es coherente con el convenio",
      all("close" in c for c in C.CLOSING_ODDS) and len(C.CLOSING_ODDS) > 0,
      C.CLOSING_ODDS)

# --------------------------------------------------------------------------- #
print("\n== la casa afilada queda fuera del semanal por el segundo motivo ==")
# Pinnacle no lleva el token `close` y es cuota de apertura: por el convenio
# pasaria. Queda fuera por declaracion, y hace falta que sea asi porque el feed
# de la jornada por jugar no trae esa casa (solo Bet365, maximo y media).
check("ninguna columna de SHARP_ODDS lleva el token de cierre",
      not any("close" in c for c in C.SHARP_ODDS),
      [c for c in C.SHARP_ODDS if "close" in c])
check("las cuotas de Pinnacle del master quedan fuera del semanal",
      AFILADA <= set(fuera), sorted(AFILADA - set(fuera)))
derivadas_ps = ["mkt_p_ps_home", "mkt_overround_ps",
                "sharp_div_home", "sharp_div_draw", "sharp_div_away"]
fuera_ps = set(C.weekly_unavailable(MASTER + derivadas_ps))
check("y tambien lo derivado de ella, que es lo que entraria al modelo",
      set(derivadas_ps) <= fuera_ps, sorted(set(derivadas_ps) - fuera_ps))
check("pero siguen disponibles en el retrospectivo",
      set(derivadas_ps) | AFILADA
      <= set(C.regime_columns(MASTER + derivadas_ps, "retrospectivo")))
# El token 'ps' es demasiado comun para usarlo como convenio: la comprobacion de
# que NO se usa asi es lo que protege a una columna inocente de caerse sola.
inocentes = ["points_to_leader_diff", "prev_position_home", "home_ppda",
             "stakes_none_match", "position_zone_home"]
check("un token tan comun como 'ps' no se aplica por subcadena",
      not (set(inocentes) & set(C.weekly_unavailable(MASTER + inocentes))),
      sorted(set(inocentes) & set(C.weekly_unavailable(MASTER + inocentes))))

# --------------------------------------------------------------------------- #
print("\n== los dos regimenes particionan el master ==")
sem = C.regime_columns(MASTER, "semanal")
ret = C.regime_columns(MASTER, "retrospectivo")
check("el semanal conserva el orden original", sem == [c for c in MASTER if c not in set(fuera)])
check("el retrospectivo es el master entero", ret == MASTER)
check("particion sin solape ni perdida",
      set(sem) | set(fuera) == set(MASTER) and not (set(sem) & set(fuera)))
check("aplicar el regimen dos veces no cambia nada",
      C.regime_columns(sem, "semanal") == sem)
check("weekly_available es el complemento de weekly_unavailable",
      C.weekly_available(MASTER) == sem)
check("los nombres de regimen estan declarados en un solo sitio",
      C.REGIMES == ("semanal", "retrospectivo"))

try:
    C.regime_columns(MASTER, "semanales")
    check("un regimen mal escrito es un error explicito", False)
except ValueError as e:
    check("un regimen mal escrito es un error explicito", "desconocido" in str(e), str(e))

# --------------------------------------------------------------------------- #
print("\n== el convenio de nombres cubre lo que se anada manana ==")
# Variables derivadas del movimiento de precio (apertura -> cierre): informativas
# en el estudio retrospectivo e imposibles de calcular al emitir la prediccion.
derivadas = ["mkt_close_drift_home", "odds_close_movement_abs", "close_line_move_signed"]
fuera2 = C.weekly_unavailable(MASTER + derivadas)
check("quedan fuera del regimen semanal sin editar ninguna lista",
      set(derivadas) <= set(fuera2), sorted(set(derivadas) - set(fuera2)))
check("y siguen disponibles en el retrospectivo",
      set(derivadas) <= set(C.regime_columns(MASTER + derivadas, "retrospectivo")))

# --------------------------------------------------------------------------- #
print("\n== los dos ejes son independientes ==")
# Una columna de FUGA lo es en los dos regimenes: el eje temporal no la absuelve.
con_fuga = MASTER + ["fthg", "ftag", "home_xg", "sofa_shots_home"]
fuga = set(C.leakage_columns(con_fuga))
check("la fuga se detecta igual bajo el regimen semanal",
      {"fthg", "ftag", "home_xg", "sofa_shots_home"} <= fuga, sorted(fuga))
check("y ninguna columna de cierre se confunde con fuga",
      not (set(C.weekly_unavailable(con_fuga)) & fuga))

# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_regimen():
    """Punto de entrada para `pytest` (ver test_scoring_gates.py)."""
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
