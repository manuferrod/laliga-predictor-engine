"""Validacion offline de lineups.py (parseo + reduccion estrictamente pasada). Sin red."""
import sys, pathlib, tempfile, time
import pandas as pd

# Localiza scripts/extractors sin depender del directorio de trabajo:
# 1) subiendo desde este propio archivo si esta dentro del repo;
# 2) rutas conocidas como fallback.
_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "lineups.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import lineups as lin

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


D = 86400.0
LIGA = lin.sofa.LALIGA_TOURNAMENT_ID


def it(slug, *, sub=False, mins=None, sin_stats=False, name=None):
    """Entrada de `players` tal como la publica /event/{id}/lineups."""
    d = {"player": {"name": name or slug, "slug": slug, "position": "M"},
         "substitute": sub, "position": "M"}
    if not sin_stats:
        d["statistics"] = {"minutesPlayed": mins} if mins is not None else {}
    return d


# --------------------------------------------------------------------------- #
print("== _minutos: cero real vs dato ausente ==")
check("suplente sin bloque de estadisticas -> 0 minutos (no jugo)",
      lin._minutos(it("x", sub=True, sin_stats=True)) == 0.0)
check("suplente con bloque pero sin 'minutesPlayed' -> 0 minutos",
      lin._minutos(it("x", sub=True)) == 0.0)
check("titular sin estadisticas -> None (de el no se sabe nada)",
      lin._minutos(it("x", sin_stats=True)) is None)
check("titular con minutos -> float", lin._minutos(it("x", mins=90)) == 90.0)
check("suplente que entra -> sus minutos, no cero",
      lin._minutos(it("x", sub=True, mins=17)) == 17.0)
check("minutos no numericos -> None (no se inventa un cero)",
      lin._minutos({"statistics": {"minutesPlayed": "n/d"}, "substitute": False}) is None)

# --------------------------------------------------------------------------- #
print("\n== _player_rows ==")
payload = {
    "home": {"players": [it("p-a", mins=90), it("p-b", mins=62),
                         it("p-c", sub=True, mins=28), it("p-d", sub=True, sin_stats=True)]},
    "away": {"players": [it("p-e", mins=90),
                         {"player": {"name": None, "slug": None}, "substitute": True}]},
}
rows = lin._player_rows(777, payload)
check("una fila por jugador identificable (4 home + 1 away)", len(rows) == 5, len(rows))
check("row_id = event|side|slug", rows[0]["row_id"] == "777|home|p-a", rows[0]["row_id"])
check("titularidad derivada de 'substitute'",
      [r["is_starter"] for r in rows[:4]] == [True, True, False, False])
check("el suplente que no entra queda con 0, no con NaN",
      rows[3]["minutes_played"] == 0.0, rows[3]["minutes_played"])
check("jugador sin identificador se descarta (no hay forma de seguirlo)",
      all(r["player_slug"] for r in rows))
check("payload que no es un dict -> sin filas", lin._player_rows(1, None) == [])

# --------------------------------------------------------------------------- #
print("\n== scrape_lineup: [] (no publicada) vs None (fallo de transporte) ==")

class FakeSofa:
    """Sustituye a sofa._fetch_json para probar las tres ramas sin red."""
    def __init__(self, res):
        self.res, self.calls = res, 0
    def __call__(self, session, path, **kw):
        self.calls += 1
        return self.res

_orig = lin.sofa._fetch_json
try:
    lin.sofa._fetch_json = FakeSofa({"ok": False, "err": "404", "data": None})
    check("404 -> [] (la fuente no publica alineacion: hecho definitivo)",
          lin.scrape_lineup(None, 1) == [])
    lin.sofa._fetch_json = FakeSofa({"ok": False, "err": "HTTP 403", "data": None})
    check("403 -> None (fallo transitorio, se reintentara)",
          lin.scrape_lineup(None, 2) is None)
    lin.sofa._fetch_json = FakeSofa({"ok": True, "data": {"home": {"players": []},
                                                          "away": {"players": []}}})
    check("respuesta correcta y vacia -> [] (no None)", lin.scrape_lineup(None, 3) == [])
    lin.sofa._fetch_json = FakeSofa({"ok": True, "data": payload})
    got = lin.scrape_lineup(None, 777)
    check("respuesta con alineacion -> 5 filas",
          isinstance(got, list) and len(got) == 5, got if not isinstance(got, list) else len(got))
finally:
    lin.sofa._fetch_json = _orig

# --------------------------------------------------------------------------- #
print("\n== _calendario: lado, temporada y competitividad re-derivada ==")
with tempfile.TemporaryDirectory() as td:
    fx = pathlib.Path(td) / "fixtures.csv"
    t_ago = pd.Timestamp("2024-09-15", tz="UTC").timestamp()
    t_may = pd.Timestamp("2024-05-15", tz="UTC").timestamp()
    pd.DataFrame([
        {"event_id": 11, "team_id": 1, "team": "Real Madrid", "is_home": True,
         "start_ts": t_ago, "competition": "LaLiga", "comp_id": LIGA, "status": "finished"},
        {"event_id": 11, "team_id": 2, "team": "Barcelona", "is_home": False,
         "start_ts": t_ago, "competition": "LaLiga", "comp_id": LIGA, "status": "finished"},
        {"event_id": 12, "team_id": 1, "team": "Real Madrid", "is_home": True,
         "start_ts": t_ago, "competition": "Club Friendly Games", "comp_id": 99,
         "status": "finished"},
        {"event_id": 13, "team_id": 1, "team": "Real Madrid", "is_home": False,
         "start_ts": t_ago, "competition": "LaLiga", "comp_id": LIGA, "status": "postponed"},
        {"event_id": 14, "team_id": 1, "team": "Real Madrid", "is_home": True,
         "start_ts": t_may, "competition": "LaLiga", "comp_id": LIGA, "status": "finished"},
    ]).to_csv(fx, index=False)
    cal = lin._calendario(fx).set_index(["event_id", "side"])
    check("is_home -> side", set(cal.index) >= {(11, "home"), (11, "away")}, set(cal.index))
    check("temporada por corte de julio: septiembre-2024 -> 2024",
          cal.loc[(11, "home"), "season"] == 2024.0, cal.loc[(11, "home"), "season"])
    check("temporada por corte de julio: mayo-2024 -> 2023",
          cal.loc[(14, "home"), "season"] == 2023.0, cal.loc[(14, "home"), "season"])
    check("amistoso no es competitivo", not bool(cal.loc[(12, "home"), "is_competitive"]))
    check("aplazado no es competitivo", not bool(cal.loc[(13, "away"), "is_competitive"]))
    check("clave de equipo canonica", cal.loc[(11, "home"), "team_key"] ==
          lin.tn.to_key("Real Madrid"), cal.loc[(11, "home"), "team_key"])

# --------------------------------------------------------------------------- #
print("\n== to_player_level: el rival extranjero de la eliminatoria europea cae ==")
raw = pd.DataFrame([
    {"event_id": 900, "side": "home", "player_slug": "p1", "player": "P1",
     "position": "M", "is_starter": True, "minutes_played": 90},
    {"event_id": 900, "side": "away", "player_slug": "q1", "player": "Q1",
     "position": "M", "is_starter": True, "minutes_played": 90},
    {"event_id": 901, "side": "home", "player_slug": "p1", "player": "P1",
     "position": "M", "is_starter": True, "minutes_played": 90},
])
cal_min = pd.DataFrame([
    # el equipo espanol juega FUERA en Europa: solo su lado esta en el calendario
    {"event_id": 900, "side": "home", "team_key": "rival_ingles", "season": 2024.0,
     "start_ts": 1.0, "comp_id": 7, "is_competitive": True},
    {"event_id": 901, "side": "home", "team_key": "a", "season": 2024.0,
     "start_ts": 2.0, "comp_id": 99, "is_competitive": False},
])
pl = lin.to_player_level(raw, cal_min)
check("el lado no rastreado se descarta sin ruido", set(pl["event_id"]) == {900},
      set(pl["event_id"]))
check("el partido no competitivo se descarta", 901 not in set(pl["event_id"]))
check("cada fila hereda equipo, temporada y sello de saque",
      {"team_key", "season", "start_ts"} <= set(pl.columns))
check("raw vacio -> tabla vacia con esquema",
      lin.to_player_level(pd.DataFrame(), cal_min).empty)

# --------------------------------------------------------------------------- #
print("\n== compute_team_features: MIRA SOLO HACIA ATRAS ==")

def _once_rows(eid, ts, team, season, starters, minutos=None, suplentes=()):
    """Filas jugador-partido de un equipo en un partido."""
    m = minutos or {}
    out = [{"event_id": eid, "side": "home", "team_key": team, "season": season,
            "start_ts": ts, "player_key": p, "is_starter": True,
            "minutes_played": float(m.get(p, 90.0))} for p in starters]
    out += [{"event_id": eid, "side": "home", "team_key": team, "season": season,
             "start_ts": ts, "player_key": p, "is_starter": False,
             "minutes_played": float(m.get(p, 0.0))} for p in suplentes]
    return out

def _cal_rows(eids_ts, team, season, comp=LIGA):
    return [{"event_id": e, "side": "home", "team_key": team, "season": season,
             "start_ts": ts, "comp_id": comp, "is_competitive": True}
            for e, ts in eids_ts]

T0 = 1.0e9
XI_A = [f"p{i}" for i in range(1, 12)]                 # once base
XI_B = [f"p{i}" for i in range(1, 7)] + [f"q{i}" for i in range(1, 6)]  # 6 repetidos

jug, cal_t = [], []
# e1 y e2: mismo once (continuidad 1). e3: once muy distinto (6/11).
jug += _once_rows(1, T0 + 0 * D, "a", 2024.0, XI_A, minutos={p: 90.0 for p in XI_A})
jug += _once_rows(2, T0 + 10 * D, "a", 2024.0, XI_A, minutos={p: 90.0 for p in XI_A})
jug += _once_rows(3, T0 + 17 * D, "a", 2024.0, XI_B)
cal_t += _cal_rows([(1, T0 + 0 * D), (2, T0 + 10 * D), (3, T0 + 17 * D),
                    (4, T0 + 24 * D)], "a", 2024.0)      # 4 = partido futuro
# temporada anterior del mismo equipo: no debe filtrarse
jug += _once_rows(90, T0 - 200 * D, "a", 2023.0, XI_A)
cal_t += _cal_rows([(90, T0 - 200 * D)], "a", 2023.0)

eq = lin.compute_team_features(pd.DataFrame(jug), pd.DataFrame(cal_t)).set_index("event_id")

check("primer partido de la temporada: todo NaN (arranque en frio honesto)",
      eq.loc[1, lin.STATS].isna().all(), eq.loc[1].to_dict())
check("segundo partido: aun no hay DOS onces anteriores -> continuidad NaN",
      pd.isna(eq.loc[2, "xi_continuity"]), eq.loc[2, "xi_continuity"])
check("en e3 la continuidad es la de e2<-e1 (1.0), NO la del propio e3",
      eq.loc[3, "xi_continuity"] == 1.0, eq.loc[3, "xi_continuity"])
check("el once de e3 solo aparece cuando ya es pasado (en e4: 6/11)",
      abs(eq.loc[4, "xi_continuity"] - 6 / 11) < 1e-9, eq.loc[4, "xi_continuity"])
check("media de tres transiciones en e4 = (6/11 + 1)/2",
      abs(eq.loc[4, "xi_continuity_mean3"] - (6 / 11 + 1.0) / 2) < 1e-9,
      eq.loc[4, "xi_continuity_mean3"])
check("la temporada anterior no aporta continuidad al primer partido de la nueva",
      pd.isna(eq.loc[1, "xi_continuity_mean3"]))
check("el partido FUTURO (4) se publica: el bloque solo necesita el pasado",
      4 in eq.index and eq.loc[4, lin.STATS].notna().any())

print("\n-- ventanas de carga y profundidad --")
# en e3 (t = T0+17d): e2 esta a 7 dias y e1 a 17. La ventana de 7 dias solo ve e2.
check("starters_minutes_7 en e3 = 90 (solo el partido de hace 7 dias)",
      abs(eq.loc[3, "starters_minutes_7"] - 90.0) < 1e-9, eq.loc[3, "starters_minutes_7"])
check("starters_minutes_14 en e3 = 90 (e1 quedo fuera de la ventana)",
      abs(eq.loc[3, "starters_minutes_14"] - 90.0) < 1e-9, eq.loc[3, "starters_minutes_14"])
check("minutes_hhi con once fijo y minutos iguales = 1/11",
      abs(eq.loc[3, "minutes_hhi"] - 1 / 11) < 1e-9, eq.loc[3, "minutes_hhi"])
check("n_players_used cuenta solo a quien ha jugado algun minuto",
      eq.loc[3, "n_players_used"] == 11, eq.loc[3, "n_players_used"])

# un titular que descansa cuenta como CERO, no como dato ausente
jug_z = (_once_rows(1, T0, "z", 2024.0, XI_A, minutos={p: 90.0 for p in XI_A}) +
         _once_rows(2, T0 + 3 * D, "z", 2024.0, XI_A,
                    minutos={**{p: 90.0 for p in XI_A}, "p11": 0.0}))
cal_z = _cal_rows([(1, T0), (2, T0 + 3 * D), (3, T0 + 6 * D)], "z", 2024.0)
eq_z = lin.compute_team_features(pd.DataFrame(jug_z), pd.DataFrame(cal_z)).set_index("event_id")
check("el titular sin minutos entra como 0 en la media (10*90+0+... )/11",
      abs(eq_z.loc[3, "starters_minutes_7"] - (10 * 180 + 90) / 11) < 1e-6,
      eq_z.loc[3, "starters_minutes_7"])

# un once incompleto no es una rotacion
jug_i = (_once_rows(1, T0, "i", 2024.0, XI_A) +
         _once_rows(2, T0 + 3 * D, "i", 2024.0, XI_A[:9]) +      # solo 9 titulares
         _once_rows(3, T0 + 6 * D, "i", 2024.0, XI_A))
cal_i = _cal_rows([(1, T0), (2, T0 + 3 * D), (3, T0 + 6 * D), (4, T0 + 9 * D)],
                  "i", 2024.0)
eq_i = lin.compute_team_features(pd.DataFrame(jug_i), pd.DataFrame(cal_i)).set_index("event_id")
check("una alineacion incompleta se salta en vez de inventar rotacion",
      pd.isna(eq_i.loc[3, "xi_continuity"]), eq_i.loc[3, "xi_continuity"])
check("el once incompleto invalida las DOS transiciones que toca, no una",
      pd.isna(eq_i.loc[4, "xi_continuity"]), eq_i.loc[4, "xi_continuity"])
# el salto no interrumpe la busqueda: con una transicion completa mas atras, se usa
jug_i2 = _once_rows(0, T0 - 3 * D, "i", 2024.0, XI_A) + jug_i
eq_i2 = lin.compute_team_features(pd.DataFrame(jug_i2),
                                  pd.DataFrame(_cal_rows([(0, T0 - 3 * D)], "i", 2024.0)
                                               + cal_i)).set_index("event_id")
check("saltar un once incompleto no detiene la busqueda hacia atras",
      eq_i2.loc[3, "xi_continuity"] == 1.0, eq_i2.loc[3, "xi_continuity"])

print("\n-- universo de salida --")
cal_mix = pd.DataFrame(_cal_rows([(1, T0)], "a", 2024.0) +
                       _cal_rows([(2, T0 + D)], "a", 2024.0, comp=99))
eq_mix = lin.compute_team_features(pd.DataFrame(jug), cal_mix)
check("solo se publican partidos de LaLiga (la Copa es materia prima, no salida)",
      set(eq_mix["event_id"]) == {1}, set(eq_mix["event_id"]))
check("sin calendario -> tabla vacia con esquema",
      lin.compute_team_features(pd.DataFrame(jug), pd.DataFrame()).empty)
check("sin historial -> filas del calendario con todo NaN",
      lin.compute_team_features(pd.DataFrame(), pd.DataFrame(cal_t))[lin.STATS]
      .isna().all().all())

# --------------------------------------------------------------------------- #
print("\n== season_minutes_share: la cuota NO incluye el partido que acompana ==")
js = []
for eid, ts, m1, m2, m3 in [(1, T0, 90.0, 0.0, 120.0),
                            (2, T0 + 7 * D, 90.0, 45.0, 0.0),
                            (3, T0 + 14 * D, 90.0, 90.0, 0.0)]:
    for p, mn in (("p1", m1), ("p2", m2), ("p3", m3)):
        js.append({"event_id": eid, "side": "home", "team_key": "a", "season": 2024.0,
                   "start_ts": ts, "player_key": p, "is_starter": mn > 0,
                   "minutes_played": mn})
sh = lin.season_minutes_share(pd.DataFrame(js)).set_index(["event_id", "player_key"])
check("primer partido: sin partidos previos, la cuota es NaN, no 0",
      pd.isna(sh.loc[(1, "p1"), "minutes_share"]), sh.loc[(1, "p1"), "minutes_share"])
check("titular fijo -> cuota 1 tras un partido completo",
      sh.loc[(2, "p1"), "minutes_share"] == 1.0, sh.loc[(2, "p1"), "minutes_share"])
check("suplente: en e2 su cuota es 0 (no jugo en e1)",
      sh.loc[(2, "p2"), "minutes_share"] == 0.0, sh.loc[(2, "p2"), "minutes_share"])
check("en e3 la cuota de p2 es 45/180, sin contar sus 90 minutos de e3",
      abs(sh.loc[(3, "p2"), "minutes_share"] - 45 / 180) < 1e-12,
      sh.loc[(3, "p2"), "minutes_share"])
check("la prorroga no lleva la cuota por encima de 1",
      sh.loc[(2, "p3"), "minutes_share"] == 1.0, sh.loc[(2, "p3"), "minutes_share"])
check("el denominador cuenta partidos del EQUIPO",
      sh.loc[(3, "p1"), "team_matches_before"] == 2.0,
      sh.loc[(3, "p1"), "team_matches_before"])
check("sin jugadores -> tabla vacia con esquema",
      lin.season_minutes_share(pd.DataFrame()).empty)

print("\n-- minutes_share_at: consulta en un instante arbitrario (peso de una baja) --")
JS = pd.DataFrame(js)
def _q(*filas):
    return pd.DataFrame(filas, columns=["team_key", "season", "player_key", "start_ts"])

q = _q(("a", 2024.0, "p1", T0 + 21 * D),
       ("a", 2024.0, "p2", T0 + 21 * D),
       ("a", 2024.0, "p3", T0 + 21 * D),
       ("a", 2024.0, "desconocido", T0 + 21 * D),
       ("a", 2024.0, "p1", T0 - D),
       ("b", 2024.0, "p1", T0 + 21 * D))
r = lin.minutes_share_at(JS, q)
check("se puede consultar a un jugador que NO figura en esa alineacion",
      r["minutes_share"].notna().sum() >= 4, r["minutes_share"].tolist())
check("titular fijo tras tres partidos -> 1.0", r.loc[0, "minutes_share"] == 1.0,
      r.loc[0, "minutes_share"])
check("suplente: 135 minutos sobre 270 posibles = 0.5",
      abs(r.loc[1, "minutes_share"] - 0.5) < 1e-12, r.loc[1, "minutes_share"])
check("jugador del que no consta ni un minuto -> 0 (es lo que se sabe de el)",
      r.loc[3, "minutes_share"] == 0.0, r.loc[3, "minutes_share"])
check("antes del primer partido del equipo -> NaN, no 0",
      pd.isna(r.loc[4, "minutes_share"]), r.loc[4, "minutes_share"])
check("equipo sin historial en la temporada -> NaN",
      pd.isna(r.loc[5, "minutes_share"]), r.loc[5, "minutes_share"])
check("team_matches_before es el calendario del equipo, no el del jugador",
      r.loc[2, "team_matches_before"] == 3.0, r.loc[2, "team_matches_before"])

# p3 juega 120 minutos en el primer partido y despues nada: su peso decae solo
dec = lin.minutes_share_at(JS, _q(("a", 2024.0, "p3", T0 + 3 * D),
                                  ("a", 2024.0, "p3", T0 + 10 * D),
                                  ("a", 2024.0, "p3", T0 + 21 * D)))["minutes_share"]
check("el peso de una baja larga decae al crecer el denominador",
      dec.is_monotonic_decreasing and abs(dec.iloc[2] - 120 / 270) < 1e-12,
      dec.tolist())
check("la prorroga tampoco escapa del tope de 1 en la consulta libre",
      dec.iloc[0] == 1.0, dec.iloc[0])
check("consulta vacia -> serie vacia, sin excepcion",
      lin.minutes_share_at(JS, _q()).empty)
check("sin historial de minutos -> todo NaN",
      lin.minutes_share_at(pd.DataFrame(), q)["minutes_share"].isna().all())

# --------------------------------------------------------------------------- #
print("\n== sentinela de 'consultado sin alineacion' (idempotencia real) ==")
with tempfile.TemporaryDirectory() as td:
    master = pathlib.Path(td) / "sofascore_lineups.csv"
    df_c, done = lin._load_checked(master)
    check("sin sentinela previo -> vacio", len(df_c) == 0 and done == set())
    lin._save_checked(df_c, [101, 102], master)
    _, done2 = lin._load_checked(master)
    check("el sentinela persiste los partidos ya consultados",
          done2 == {"101", "102"}, done2)
    check("el fichero sentinela vive junto al maestro",
          lin._checked_path(master).name == "_checked_sofascore_lineups.csv",
          lin._checked_path(master).name)

# --------------------------------------------------------------------------- #
print("\n== _played_ids: solo lo jugado, y con margen tras el saque ==")
NOW = time.time()
cal_ids = pd.DataFrame([
    {"event_id": 1, "start_ts": NOW - 30 * D, "is_competitive": True},
    {"event_id": 2, "start_ts": NOW - 2 * D, "is_competitive": True},
    {"event_id": 3, "start_ts": NOW - 3600, "is_competitive": True},   # en curso
    {"event_id": 4, "start_ts": NOW + 3 * D, "is_competitive": True},  # por jugar
    {"event_id": 5, "start_ts": NOW - 10 * D, "is_competitive": False},
    {"event_id": 6, "start_ts": lin._SINCE_TS - D, "is_competitive": True},
])
ids = lin._played_ids(cal_ids)
check("se piden los partidos ya jugados", ids == [1, 2], ids)
check("el partido en curso espera al margen de 4 horas", 3 not in ids, ids)
check("el partido futuro no se pide: el bloque no lo necesita", 4 not in ids)
check("lo no competitivo y lo anterior al proyecto quedan fuera",
      5 not in ids and 6 not in ids)
check("la pasada semanal NO recorta por dias: el partido de hace 30 dias sigue "
      "en el universo, para poder recuperar un hueco antiguo", 1 in ids, ids)
check("sin calendario -> sin peticiones", lin._played_ids(pd.DataFrame()) == [])

# --------------------------------------------------------------------------- #
print("\n== process: no publica sobre nada ==")
with tempfile.TemporaryDirectory() as td:
    try:
        lin.process(pathlib.Path(td) / "no_existe.csv",
                    fixtures_csv=pathlib.Path(td) / "tampoco.csv", verbose=False)
        check("sin crudo ni sentinela, process se niega a publicar", False, "no lanzo")
    except lin._common.RegressionError:
        check("sin crudo ni sentinela, process se niega a publicar", True)
    except Exception as e:  # noqa: BLE001
        check("sin crudo ni sentinela, process se niega a publicar", False, repr(e))

# --------------------------------------------------------------------------- #
print("\n" + "=" * 40)
print(f"TOTAL: {len(PASS)} OK, {len(FAIL)} FAIL")


def test_lineups():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)