"""Validacion offline del circuito de cuotas tecleadas a mano. Sin red.

El circuito tiene tres tramos y cada uno resuelve un fallo distinto. Se prueban
los tres y, sobre todo, la juntura: una cuota que resuelve la emision de esta
semana y desaparece del historico la semana siguiente no resuelve nada, porque
deja la prediccion publicada y el partido reentrenado con dos precios distintos.

  1. SEMILLA. football-data publica `fixtures.csv` como una ventana de pocos
     dias y con el precio ya puesto: mientras no publica la jornada no hay NI
     fixture NI cuota. El gate de cuotas -- que es quien escribe la plantilla --
     vive detras del recorte de ventana, asi que con la ventana vacia se abortaba
     antes de llegar a el y no habia fila de la que sembrar la plantilla. La
     semilla la trae del calendario de sofascore, que conoce la jornada con
     semanas de antelacion, y la acota a la PROXIMA jornada por el hueco de
     calendario: con el horizonte de diez dias, el calendario entero de sofascore
     sembraria dos jornadas y la segunda no tiene mercado abierto en ninguna casa.

  2. APLICACION. La cuota teclada entra por `_implied_probs`, igual que la
     automatica, de modo que aguas abajo son indistinguibles. Lo que no se acepta
     es un trio que no sea un precio 1X2 real: se rechaza en voz alta, porque en
     silencio el usuario reescribiria la misma cuota mala una y otra vez.

  3. PREVALENCIA. Lo teclado se promueve a un registro propio del crudo y
     `footballdata.apply_manual_odds` lo vuelve a imponer en cada reproceso. Dos
     invariantes la sostienen, y son las dos que mas importa que no se rompan:

       - el override toca SOLO el canal de entrada del modelo (`odds_avg_*`, de
         donde salen las tres `mkt_p_*`) y deja intactas las columnas de Bet365 y
         de cierre, que son el LISTON de toda cifra economica del trabajo
         (regla 3). Si una cuota escrita a mano pudiera entrar ahi, la pregunta
         del TFM se contestaria contra un precio que no publico ninguna casa.
       - solo se aplica si el sello de entrada es ANTERIOR al partido. Es la
         regla 6 en codigo: sin esa comprobacion nada impediria "corregir" el
         precio de un partido de 2019 y mejorar el backtest de un bloque cerrado.
"""
import pathlib
import sys
import tempfile

import numpy as np
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "features",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "features",
]
FEAT = next((p for p in _CANDS if (p / "build_scoring.py").exists()), _CANDS[0])
sys.path.insert(0, str(FEAT))
sys.path.insert(0, str(FEAT.parent / "extractors"))
import build_scoring as bs
import footballdata as fd

TMP = pathlib.Path(tempfile.mkdtemp())
HOY = pd.Timestamp("2026-10-22")

PASS, FAIL = [], []


def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


def _fixtures(filas) -> pd.DataFrame:
    """Calendario crudo de sofascore: una fila POR EQUIPO, como el real."""
    out = []
    for i, (fecha, local, visitante, estado, comp) in enumerate(filas):
        for es_local in (True, False):
            out.append({
                "row_id": f"r{i}{int(es_local)}", "event_id": 1000 + i,
                "team": local if es_local else visitante,
                "opponent": visitante if es_local else local,
                "is_home": es_local, "date": fecha, "start_ts": np.nan,
                "competition": comp, "status": estado,
            })
    return pd.DataFrame(out)


_JORNADA = [
    ("2026-10-23", "Deportivo Alaves", "Malaga CF", "notstarted", "LaLiga"),
    ("2026-10-24", "Valencia", "Villarreal", "notstarted", "LaLiga"),
    ("2026-10-25", "FC Barcelona", "Real Madrid", "notstarted", "LaLiga"),
]
_SIGUIENTE = [
    ("2026-10-31", "Real Betis", "Malaga CF", "notstarted", "LaLiga"),
    ("2026-11-01", "Getafe", "Sevilla", "notstarted", "LaLiga"),
]

_UP_COLS = ["match_key", "season", "season_year", "date", "home_team",
            "away_team", "home_key", "away_key", "odds_avg_h", "odds_avg_d",
            "odds_avg_a", "mkt_p_home", "mkt_p_draw", "mkt_p_away"]
VACIO_UP = pd.DataFrame(columns=_UP_COLS)
VACIO_PLAYED = pd.DataFrame(columns=["match_key"])


# --------------------------------------------------------------------------- #
print("== 1. semilla: lo que sofascore sabe y football-data todavia no ==")
fx = _fixtures(_JORNADA)
sem = bs._seed_from_sofascore(VACIO_UP, fx, VACIO_PLAYED, HOY, 10, 1,
                              verbose=False)
check("con la ventana de football-data vacia, la semilla trae la jornada",
      len(sem) == 3, f"trajo {len(sem)}")
check("las filas sembradas nacen SIN cuota (van a la plantilla manual)",
      bool(sem["mkt_p_home"].isna().all()))
check("la procedencia del fixture queda declarada",
      set(sem["fixture_src"]) == {"sofascore"}, str(set(sem.get("fixture_src", []))))
check("la clave sigue el convenio fecha|home_key|away_key",
      "2026-10-25|barcelona|real_madrid" in set(sem["match_key"]),
      str(sorted(sem["match_key"])))
check("season y season_year se derivan de la fecha (LaLiga arranca en agosto)",
      set(sem["season"]) == {"2026/27"} and set(sem["season_year"]) == {2026})

sem2 = bs._seed_from_sofascore(VACIO_UP, _fixtures(_JORNADA + _SIGUIENTE),
                               VACIO_PLAYED, HOY, 10, 1, verbose=False)
check("el horizonte abarca dos jornadas y la semilla solo siembra la primera",
      len(sem2) == 3, f"sembro {len(sem2)}")

mezcla = _fixtures(_JORNADA + [
    ("2026-10-24", "Levante UD", "Real Union", "notstarted", "Copa del Rey"),
    ("2026-10-24", "Elche", "Cadiz", "notstarted", "LaLiga 2"),
    ("2026-10-25", "Celta Vigo", "Real Betis", "canceled", "LaLiga"),
    ("2026-10-25", "Sevilla", "Osasuna", "postponed", "LaLiga"),
])
sem3 = bs._seed_from_sofascore(VACIO_UP, mezcla, VACIO_PLAYED, HOY, 10, 1,
                               verbose=False)
check("el calendario es multicompeticion y solo se siembra LaLiga",
      len(sem3) == 3, f"sembro {len(sem3)}: {sorted(sem3['match_key'])}")
check("un aplazado o un cancelado no es un partido que venga",
      not any("celta" in k or "sevilla" in k for k in sem3["match_key"]))

ya_jugado = pd.DataFrame({"match_key": ["2026-10-23|alaves|malaga"]})
sem4 = bs._seed_from_sofascore(VACIO_UP, fx, ya_jugado, HOY, 10, 1, verbose=False)
check("no se siembra lo que el master ya tiene consolidado", len(sem4) == 2,
      f"sembro {len(sem4)}")

con_fd = pd.DataFrame([{
    "match_key": "2026-10-25|barcelona|real_madrid", "season": "2026/27",
    "season_year": 2026, "date": "2026-10-25", "home_team": "Barcelona",
    "away_team": "Real Madrid", "home_key": "barcelona",
    "away_key": "real_madrid", "odds_avg_h": 1.9, "odds_avg_d": 3.6,
    "odds_avg_a": 3.8, "mkt_p_home": 0.5, "mkt_p_draw": 0.26,
    "mkt_p_away": 0.24}]).reindex(columns=_UP_COLS)
sem5 = bs._seed_from_sofascore(con_fd, fx, VACIO_PLAYED, HOY, 10, 1, verbose=False)
fila = sem5[sem5["match_key"] == "2026-10-25|barcelona|real_madrid"].iloc[0]
check("football-data manda cuando ya tiene el fixture (trae precio real)",
      len(sem5) == 3 and fila["mkt_p_home"] == 0.5 and
      fila["fixture_src"] == "footballdata", str(fila.to_dict()))

check("sin calendario no se inventa nada",
      bs._seed_from_sofascore(VACIO_UP, None, VACIO_PLAYED, HOY, 10, 1,
                              verbose=False).empty)


# --------------------------------------------------------------------------- #
print("\n== 2. aplicacion: la cuota teclada entra como la automatica ==")
plantilla = TMP / "manual.csv"
up = bs._seed_from_sofascore(VACIO_UP, fx, VACIO_PLAYED, HOY, 10, 1,
                             verbose=False)
bs._write_odds_template(up, plantilla)
tpl = pd.read_csv(plantilla)
check("la plantilla se escribe con una fila por partido y las cuotas vacias",
      len(tpl) == 3 and bool(tpl[["odds_h", "odds_d", "odds_a"]].isna().all().all()))
check("la plantilla lleva la clave y los nombres para poder teclear a ciegas",
      {"match_key", "date", "home_team", "away_team"} <= set(tpl.columns))

tpl.loc[tpl["match_key"] == "2026-10-25|barcelona|real_madrid",
        ["odds_h", "odds_d", "odds_a"]] = [2.10, 3.40, 3.30]
tpl.loc[tpl["match_key"] == "2026-10-23|alaves|malaga",
        ["odds_h", "odds_d", "odds_a"]] = [0.5, 3.40, 3.30]   # cuota imposible
tpl.to_csv(plantilla, index=False)

aplicadas = bs._apply_manual_odds(up, plantilla)
check("se aplica la cuota plausible", aplicadas == {"2026-10-25|barcelona|real_madrid"},
      str(aplicadas))
fila = up[up["match_key"] == "2026-10-25|barcelona|real_madrid"].iloc[0]
check("la cuota teclada se convierte en probabilidad desmarginada",
      abs(fila["mkt_p_home"] - (1 / 2.10) / (1 / 2.10 + 1 / 3.40 + 1 / 3.30)) < 1e-9,
      str(fila["mkt_p_home"]))
check("las tres probabilidades suman uno, igual que las automaticas",
      abs(fila[["mkt_p_home", "mkt_p_draw", "mkt_p_away"]].sum() - 1) < 1e-9)
check("una cuota imposible se rechaza y el partido sigue contando como faltante",
      bool(up[up["match_key"] == "2026-10-23|alaves|malaga"]["mkt_p_home"].isna().all()))

bs._write_odds_template(up[up["mkt_p_home"].isna()], plantilla)
tpl2 = pd.read_csv(plantilla)
rellena = tpl2[tpl2["match_key"] == "2026-10-23|alaves|malaga"]
check("reejecutar no borra lo ya teclado, aunque se haya rechazado",
      len(rellena) == 1 and float(rellena["odds_h"].iloc[0]) == 0.5)


# --------------------------------------------------------------------------- #
print("\n== 3. prevalencia: lo teclado manda en el historico ==")
hist = TMP / "odds_manual_hist.csv"
promovido = pd.DataFrame([{
    "match_key": "2026-10-25|barcelona|real_madrid", "date": "2026-10-25",
    "home_team": "FC Barcelona", "away_team": "Real Madrid",
    "odds_h": 2.10, "odds_d": 3.40, "odds_a": 3.30,
    "entered_ts": "2026-10-22T19:00:00"}])
bs.promote_manual_odds(promovido, path=hist)
check("el registro historico se escribe con el esquema declarado",
      list(fd.read_manual_odds(hist).columns) == fd.MANUAL_ODDS_COLS)

# lo que football-data publicara la semana siguiente, con SU precio
historico = pd.DataFrame([{
    "match_key": "2026-10-25|barcelona|real_madrid", "date": "2026-10-25",
    "season_year": 2026, "odds_avg_h": 1.95, "odds_avg_d": 3.55,
    "odds_avg_a": 3.70, "odds_b365_h": 1.90, "odds_b365_d": 3.60,
    "odds_b365_a": 3.75, "odds_close_avg_h": 1.88, "odds_close_avg_d": 3.62,
    "odds_close_avg_a": 3.80},
    {"match_key": "2019-05-01|getafe|sevilla", "date": "2019-05-01",
     "season_year": 2018, "odds_avg_h": 2.50, "odds_avg_d": 3.20,
     "odds_avg_a": 2.90, "odds_b365_h": 2.45, "odds_b365_d": 3.25,
     "odds_b365_a": 2.95, "odds_close_avg_h": 2.48, "odds_close_avg_d": 3.21,
     "odds_close_avg_a": 2.92}])

res = fd.apply_manual_odds(historico, path=hist, verbose=False)
nueva = res[res["match_key"] == "2026-10-25|barcelona|real_madrid"].iloc[0]
check("la cuota teclada PREVALECE sobre el consenso de football-data",
      float(nueva["odds_avg_h"]) == 2.10 and float(nueva["odds_avg_d"]) == 3.40
      and float(nueva["odds_avg_a"]) == 3.30, str(nueva["odds_avg_h"]))
check("la procedencia queda declarada en odds_src",
      nueva["odds_src"] == "manual")
check("las columnas de Bet365 NO se tocan: son el liston de la regla 3",
      float(nueva["odds_b365_h"]) == 1.90 and float(nueva["odds_b365_a"]) == 3.75)
check("las cuotas de CIERRE tampoco: son mejor dato que la apertura teclada",
      float(nueva["odds_close_avg_h"]) == 1.88)
vieja = res[res["match_key"] == "2019-05-01|getafe|sevilla"].iloc[0]
check("un partido sin cuota teclada queda intacto y marcado como de la fuente",
      float(vieja["odds_avg_h"]) == 2.50 and vieja["odds_src"] == "footballdata")

# el limite: un sello posterior al partido es informacion del futuro
tarde = TMP / "odds_tarde.csv"
pd.DataFrame([{
    "match_key": "2019-05-01|getafe|sevilla", "date": "2019-05-01",
    "home_team": "Getafe", "away_team": "Sevilla",
    "odds_h": 1.01, "odds_d": 50.0, "odds_a": 50.0,
    "entered_ts": "2026-10-22T19:00:00"}]).to_csv(tarde, index=False)
res2 = fd.apply_manual_odds(historico, path=tarde, verbose=False)
v2 = res2[res2["match_key"] == "2019-05-01|getafe|sevilla"].iloc[0]
check("una cuota teclada DESPUES del partido se rechaza (regla 6)",
      float(v2["odds_avg_h"]) == 2.50 and v2["odds_src"] == "footballdata",
      str(v2["odds_avg_h"]))

sin_sello = TMP / "odds_sin_sello.csv"
pd.DataFrame([{
    "match_key": "2026-10-25|barcelona|real_madrid", "date": "2026-10-25",
    "home_team": "FC Barcelona", "away_team": "Real Madrid",
    "odds_h": 2.10, "odds_d": 3.40, "odds_a": 3.30,
    "entered_ts": ""}]).to_csv(sin_sello, index=False)
res3 = fd.apply_manual_odds(historico, path=sin_sello, verbose=False)
check("sin sello de entrada no se aplica: no se puede fechar lo que se vio",
      float(res3[res3["match_key"] ==
                 "2026-10-25|barcelona|real_madrid"].iloc[0]["odds_avg_h"]) == 1.95)

check("sin registro, el historico sale igual pero con la procedencia declarada",
      fd.apply_manual_odds(historico, path=TMP / "no_existe.csv",
                           verbose=False)["odds_src"].eq("footballdata").all())

# promover dos veces conserva el sello MAS ANTIGUO
bs.promote_manual_odds(promovido.assign(
    entered_ts="2026-10-27T10:00:00"), path=hist)
guardado = fd.read_manual_odds(hist)
check("promover de nuevo no pisa el sello original (si lo hiciera, el limite "
      "de arriba borraria el override justo al volverse historia)",
      len(guardado) == 1 and
      guardado["entered_ts"].iloc[0] == "2026-10-22T19:00:00",
      str(guardado["entered_ts"].tolist()))


# --------------------------------------------------------------------------- #
print("\n== 4. la juntura: attrs no puede impedir escribir la emision ==")
out = pd.DataFrame({"match_key": ["k"], "x": [1.0]})
out.attrs["cobertura"] = pd.DataFrame({"bloque": ["xg"]})
out.attrs["cuotas_manuales"] = promovido
destino = TMP / "features_upcoming.parquet"
out_limpio = out.copy()
out_limpio.attrs.clear()
ok = True
try:
    out_limpio.to_parquet(destino, index=False)
except Exception:  # noqa: BLE001
    ok = False
check("vaciar attrs antes de escribir salva la emision (pandas vuelca attrs "
      "a los metadatos del parquet como JSON y un DataFrame no es serializable)",
      ok)
rompe = False
try:
    out.to_parquet(TMP / "roto.parquet", index=False)
except TypeError:
    rompe = True
check("y sin vaciarlos aborta, que es el fallo que tenia la ruta de emision",
      rompe)


# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_manual_odds():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
