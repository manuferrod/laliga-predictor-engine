"""Validacion offline de availability.py (parseo + agregacion). Sin red."""
import sys, pathlib
import pandas as pd

# Localiza scripts/extractors sin depender del directorio de trabajo:
# 1) subiendo desde este propio archivo si esta dentro del repo;
# 2) rutas conocidas como fallback.
_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "availability.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import availability as av

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))

def mp(name, slug, desc=None, ext=None, reason=None, typ=None, end=None):
    return {"player": {"name": name, "slug": slug, "position": "M"},
            "type": typ, "reason": reason, "description": desc,
            "externalType": ext, "expectedEndDate": end}

# shape realista de /event/{id}/lineups.
# type: 'missing' = baja segura (is_out); 'doubtful' = duda (no out).
lineups = {
    "confirmed": True,
    "home": {"missingPlayers": [
        mp("A Lesionado", "a-les", typ="missing", desc="Knee injury", end="2024-12-01"),
        mp("B Sancionado", "b-san", typ="missing", reason=13, desc="Suspension"),
        mp("C Duda", "c-dud", typ="doubtful", reason=1, desc="Overload"),
    ]},
    "away": {"missingPlayers": [
        mp("D Roja", "d-roj", typ="missing", reason=13, ext="Red card suspension"),
        mp("E Baja", "e-baj", typ="missing", reason=1),  # missing sin texto -> injury (fallback type)
    ]},
}

print("== _classify (type-driven is_out) ==")
check("lesion por type=missing (sin texto)",
      av._classify(mp("x","x",typ="missing")) == ("injury", True))
check("lesion por texto 'injury'", av._classify(mp("x","x",desc="Knee injury"))[0] == "injury")
check("sancion por reason==13",
      av._classify(mp("x","x",typ="missing",reason=13)) == ("suspension", True))
check("sancion por 'Suspension' en desc",
      av._classify(mp("x","x",desc="Suspension"))[0] == "suspension")
check("sancion por 'Red card' en externalType",
      av._classify(mp("x","x",ext="Red card suspension"))[0] == "suspension")
check("duda por type=doubtful y NO out",
      av._classify(mp("x","x",typ="doubtful",reason=1)) == ("doubtful", False))
check("sin type ni texto -> unknown y NO out",
      av._classify(mp("x","x")) == ("unknown", False))

print("\n== _missing_rows ==")
rows = av._missing_rows(555, lineups)
check("5 filas (3 home + 2 away)", len(rows) == 5, len(rows))
check("row_id = event|side|slug", rows[0]["row_id"] == "555|home|a-les", rows[0]["row_id"])
check("side asignado", {r["side"] for r in rows} == {"home", "away"})
check("confirmed propagado", all(r["confirmed"] is True for r in rows))

print("\n== aggregate (equipo-partido) ==")
agg = av.aggregate(pd.DataFrame(rows)).set_index("side")
check("home: 3 bajas", agg.loc["home", "n_missing"] == 3, agg.loc["home", "n_missing"])
check("home: 1 lesion, 1 sancion, 1 duda",
      agg.loc["home", "n_injury"] == 1 and agg.loc["home", "n_suspension"] == 1
      and agg.loc["home", "n_doubtful"] == 1)
check("home: n_out = 2 (duda no cuenta como out)", agg.loc["home", "n_out"] == 2,
      agg.loc["home", "n_out"])
check("away: 2 bajas, 1 sancion", agg.loc["away", "n_missing"] == 2
      and agg.loc["away", "n_suspension"] == 1)

print("\n== aggregate: 'sin bajas' NO es 'sin dato' ==")
# un partido con bajas solo en el local tambien tiene dato del visitante: es 0
solo_home = pd.DataFrame([r for r in rows if r["side"] == "home"])
a_solo = av.aggregate(solo_home)
check("se emiten los dos lados (home y away)",
      set(a_solo["side"]) == {"home", "away"}, set(a_solo["side"]))
check("el lado sin bajas entra como 0, no como ausente",
      a_solo.loc[a_solo["side"] == "away", "n_missing"].iloc[0] == 0)
check("columna 'queried' explicita", bool(a_solo["queried"].all()))
# los partidos consultados sin ninguna baja llegan por el sentinela
a_sent = av.aggregate(pd.DataFrame(rows), queried_ids={"901", "902", "no-es-id"})
check("el sentinela aporta partidos con recuentos 0",
      set(a_sent["event_id"]) == {555, 901, 902}, set(a_sent["event_id"]))
check("ids invalidos del sentinela se descartan", len(a_sent) == 6, len(a_sent))
check("recuentos enteros (0 real, no NaN)",
      a_sent[["n_missing", "n_out"]].dtypes.astype(str).eq("int64").all())
a_vac = av.aggregate(pd.DataFrame(), queried_ids=[901])
check("raw vacio + sentinela -> solo ceros", len(a_vac) == 2
      and a_vac["n_missing"].sum() == 0, len(a_vac))
a_nada = av.aggregate(pd.DataFrame())
check("sin raw ni sentinela -> tabla vacia con esquema",
      a_nada.empty and "queried" in a_nada.columns)

print("\n== temporada SIN feed: el cero seria una afirmacion falsa ==")
import tempfile
with tempfile.TemporaryDirectory() as td:
    stats = pathlib.Path(td) / "stats.csv"
    pd.DataFrame({
        "match_id": [1, 2, 3, 11, 12, 13],
        "season_name": ["LaLiga 16/17"] * 3 + ["LaLiga 17/18"] * 3,
    }).to_csv(stats, index=False)
    # el feed arranca en 17/18: en 16/17 no hay ni un registro en toda la liga
    raw = pd.DataFrame([{"event_id": 11, "side": "home", "player_slug": "x",
                         "category": "injury", "is_out": True}])
    sin_feed = av.temporadas_sin_feed(raw, stats)
    check("la temporada sin ni un registro en toda la liga se detecta",
          set(sin_feed) == {"LaLiga 16/17"}, sin_feed)
    check("la temporada con feed vivo no se denuncia por sus huecos sueltos "
          "(12 y 13 son ceros de verdad)", "LaLiga 17/18" not in sin_feed)
    universo = av._sin_temporadas_sin_feed(raw, [1, 2, 3, 12, 13], stats_csv=stats,
                                           verbose=False)
    check("sus partidos salen del universo -> llegaran como 'sin dato'",
          universo == {12, 13}, sorted(universo))
    a = av.aggregate(raw, queried_ids=universo)
    check("y la tabla publicada no contiene la temporada sin feed",
          set(a["event_id"]) == {11, 12, 13}, set(a["event_id"]))
    # sin ninguna temporada vacia, el universo pasa intacto
    raw2 = pd.concat([raw, pd.DataFrame([{"event_id": 1, "side": "home",
                                          "player_slug": "y",
                                          "category": "injury", "is_out": True}])])
    check("si las dos temporadas tienen feed, no se quita nada",
          av._sin_temporadas_sin_feed(raw2, [1, 2, 3, 12, 13], stats_csv=stats,
                                      verbose=False) == {1, 2, 3, 12, 13})
    check("sin fichero de temporadas no se adivina: el universo pasa intacto",
          av._sin_temporadas_sin_feed(raw, [1, 2], stats_csv=stats.parent / "no.csv",
                                      verbose=False) == {1, 2})

print("\n== to_match_level (diffs) ==")
ml = av.to_match_level(agg.reset_index(), master_csv="___no_existe___.csv")
check("una fila de partido", len(ml) == 1, len(ml))
r = ml.iloc[0]
check("home_n_missing=3, away_n_missing=2",
      r["home_n_missing"] == 3 and r["away_n_missing"] == 2)
check("n_missing_diff = 3-2 = 1", r["n_missing_diff"] == 1, r.get("n_missing_diff"))
# home tiene 3 ausentes pero solo 2 son baja SEGURA (la duda no cuenta como out);
# away tiene 2 ausentes, ambos out -> el diff de bajas seguras es 0
check("n_out_diff = 2-2 = 0 (la duda no cuenta como out)",
      r["n_out_diff"] == 0, r.get("n_out_diff"))

# --------------------------------------------------------------------------- #
print("\n== scrape_missing: [] (sin bajas) vs None (fallo) [M1] ==")

class FakeSofa:
    """Sustituye a sofa._fetch_json para probar las tres ramas sin red."""
    def __init__(self, res):
        self.res = res
        self.calls = 0
    def __call__(self, session, path, **kw):
        self.calls += 1
        return self.res

_orig_fetch = av.sofa._fetch_json
try:
    av.sofa._fetch_json = FakeSofa({"ok": True, "data": {"confirmed": True,
        "home": {"missingPlayers": []}, "away": {"missingPlayers": []}}})
    check("lineups vacias -> [] (sin bajas, consulta correcta)",
          av.scrape_missing(None, 1) == [])

    av.sofa._fetch_json = FakeSofa({"ok": False, "err": "404", "data": None})
    check("404 -> [] (no hay lineups publicadas)", av.scrape_missing(None, 2) == [])

    av.sofa._fetch_json = FakeSofa({"ok": False, "err": "HTTP 403", "data": None})
    check("403 -> None (fallo transitorio, NO 'sin bajas')",
          av.scrape_missing(None, 3) is None)

    av.sofa._fetch_json = FakeSofa({"ok": True, "data": lineups})
    got = av.scrape_missing(None, 555)
    check("lineups con bajas -> 5 filas", isinstance(got, list) and len(got) == 5,
          got if not isinstance(got, list) else len(got))
finally:
    av.sofa._fetch_json = _orig_fetch

# --------------------------------------------------------------------------- #
print("\n== sentinel de 'consultados sin bajas' (idempotencia real) ==")
import tempfile
with tempfile.TemporaryDirectory() as td:
    master = pathlib.Path(td) / "bajas.csv"
    df_checked, done = av._load_checked(master)
    check("sin sentinel previo -> vacio", len(df_checked) == 0 and done == set())
    av._save_checked(df_checked, [101, 102], master)
    df2, done2 = av._load_checked(master)
    check("sentinel persiste los event_id consultados",
          done2 == {"101", "102"}, done2)
    check("el fichero sentinel vive junto al master",
          av._checked_path(master).name == "_checked_bajas.csv",
          av._checked_path(master).name)

# --------------------------------------------------------------------------- #
print("\n== refresco del partido por jugar: la foto nueva PISA la vieja ==")
with tempfile.TemporaryDirectory() as td:
    master = pathlib.Path(td) / "bajas.csv"
    viejas = [{"row_id": "9|home|lesionado-que-se-recupera", "event_id": 9,
               "side": "home", "player_slug": "lesionado-que-se-recupera"},
              {"row_id": "9|away|sigue-lesionado", "event_id": 9,
               "side": "away", "player_slug": "sigue-lesionado"},
              {"row_id": "8|home|otro-partido", "event_id": 8,
               "side": "home", "player_slug": "otro-partido"}]
    df = av._finalize_master(pd.DataFrame(), viejas, master)
    nuevas = [{"row_id": "9|away|sigue-lesionado", "event_id": 9,
               "side": "away", "player_slug": "sigue-lesionado"}]
    df = av._finalize_master(df, nuevas, master, refrescados=[9])
    slugs = set(df["player_slug"])
    check("el jugador recuperado desaparece de la foto nueva",
          "lesionado-que-se-recupera" not in slugs, sorted(slugs))
    check("el que sigue lesionado se mantiene", "sigue-lesionado" in slugs)
    check("el refresco no toca los demas partidos", "otro-partido" in slugs)
    df = av._finalize_master(df, [], master, refrescados=[9])
    check("un partido que se queda sin bajas queda vacio, no congelado",
          (df["event_id"] == 9).sum() == 0, df["event_id"].tolist())

# --------------------------------------------------------------------------- #
print("\n== universo de ids: jugados + CALENDARIO (partidos por jugar) ==")
import time
_NOW, _D = time.time(), 86400
with tempfile.TemporaryDirectory() as td:
    stats = pathlib.Path(td) / "stats.csv"
    fixt = pathlib.Path(td) / "fixtures.csv"
    pd.DataFrame({"match_id": [1, 2],
                  "timestamp": [_NOW - 400 * _D, _NOW - 5 * _D]}).to_csv(stats, index=False)
    filas = []
    for eid, off, comp in [(2, -5 * _D, "LaLiga"),        # ya en el master
                           (3, -1 * _D, "LaLiga"),        # jugado, sin consolidar
                           (4, +2 * _D, "LaLiga"),        # proxima jornada
                           (5, +40 * _D, "LaLiga"),       # fuera de horizonte
                           (6, +2 * _D, "Copa del Rey"),  # otra competicion
                           (7, +3600, "LaLiga")]:         # empieza en 1 hora
        for es_local in (True, False):                     # 2 filas por partido
            filas.append({"event_id": eid, "start_ts": _NOW + off,
                          "competition": comp, "is_home": es_local})
    pd.DataFrame(filas).to_csv(fixt, index=False)

    ids = av._match_ids(stats, fixtures_csv=fixt)
    check("el universo incluye los fixtures por jugar (4 y 7)",
          ids == [1, 2, 3, 4, 7], ids)
    check("descarta fuera de horizonte (5) y otras competiciones (6)",
          5 not in ids and 6 not in ids, ids)
    check("la pasada semanal NO recorta el pasado: el partido de hace 400 dias "
          "sigue en el universo, para poder recuperar un hueco antiguo",
          1 in ids, ids)
    check("sin calendario -> solo jugados (con aviso)",
          av._match_ids(stats, fixtures_csv=pathlib.Path(td) / "no.csv") == [1, 2])

    past = av._past_ids(stats, fixt)
    check("sellable el jugado que solo esta en el calendario (3)", "3" in past)
    check("NO sellable el que aun no ha empezado (4) ni el inminente (7)",
          "4" not in past and "7" not in past, sorted(past))

# --------------------------------------------------------------------------- #
print("\n" + "=" * 40)
print(f"TOTAL: {len(PASS)} OK, {len(FAIL)} FAIL")
if FAIL:
    print("fallos:", FAIL)


def test_availability():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest; cortar el proceso queda reservado al uso como guion,
    porque un `sys.exit` durante la importacion abortaria la recoleccion.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
