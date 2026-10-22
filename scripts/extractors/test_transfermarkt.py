"""Validacion offline de transfermarkt.py (parseo + agregacion). Sin red."""
import sys, pathlib
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "transfermarkt.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import transfermarkt as tm

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))

# --------------------------------------------------------------------------- #
print("== _parse_value ==")
check("€80.00m -> 80.0", tm._parse_value("€80.00m") == 80.0)
check("€500k -> 0.5", tm._parse_value("€500k") == 0.5)
check("€1.20bn -> 1200", tm._parse_value("€1.20bn") == 1200.0)
check("€250Th. -> 0.25", tm._parse_value("€250Th.") == 0.25)
check("'-' -> None", tm._parse_value("-") is None)
check("'' -> None", tm._parse_value("") is None)
check("texto con valor -> 45.0", tm._parse_value("Market value: €45.00m") == 45.0)

# --------------------------------------------------------------------------- #
print("\n== parse_competition_clubs ==")
COMP_HTML = """
<html><body>
<table class="items"><tbody>
 <tr>
   <td class="zentriert no-border-rechts"><a href="/fc-barcelona/startseite/verein/131/saison_id/2024"><img src="x"/></a></td>
   <td class="hauptlink no-border-links"><a href="/fc-barcelona/startseite/verein/131/saison_id/2024" title="FC Barcelona">FC Barcelona</a></td>
 </tr>
 <tr>
   <td class="zentriert no-border-rechts"><a href="/real-madrid/startseite/verein/418/saison_id/2024"><img src="x"/></a></td>
   <td class="hauptlink no-border-links"><a href="/real-madrid/startseite/verein/418/saison_id/2024" title="Real Madrid">Real Madrid</a></td>
 </tr>
</tbody></table>
<a href="/laliga/startseite/wettbewerb/ES1">nav</a>
</body></html>
"""
clubs = tm.parse_competition_clubs(COMP_HTML, 2024)
check("2 clubes detectados", len(clubs) == 2, len(clubs))
byid = {c["club_id"]: c for c in clubs}
check("club 131 = FC Barcelona / fc-barcelona",
      byid.get("131", {}).get("club_name") == "FC Barcelona"
      and byid["131"]["club_slug"] == "fc-barcelona")
check("club 418 = Real Madrid", byid.get("418", {}).get("club_name") == "Real Madrid")

# --------------------------------------------------------------------------- #
print("\n== parse_squad ==")
SQUAD_HTML = """
<html><body>
<table class="items"><tbody>
 <tr class="odd">
   <td class="zentriert rueckennummer bg_Torwart"><div>1</div></td>
   <td class="posrela">
     <table class="inline-table"><tr>
       <td rowspan="2"><img src="x"/></td>
       <td class="hauptlink"><a href="/marc-andre-ter-stegen/profil/spieler/74857">Marc-André ter Stegen</a></td>
     </tr><tr><td>Goalkeeper</td></tr></table>
   </td>
   <td class="zentriert">Apr 30, 1992 (33)</td>
   <td class="zentriert"><img title="Germany" src="x"/></td>
   <td class="rechts hauptlink"><a href="#">€28.00m</a></td>
 </tr>
 <tr class="even">
   <td class="zentriert rueckennummer bg_Sturm"><div>9</div></td>
   <td class="posrela">
     <table class="inline-table"><tr>
       <td rowspan="2"><img src="x"/></td>
       <td class="hauptlink"><a href="/robert-lewandowski/profil/spieler/38253">Robert Lewandowski</a></td>
     </tr><tr><td>Centre-Forward</td></tr></table>
   </td>
   <td class="zentriert">Aug 21, 1988 (36)</td>
   <td class="zentriert"><img title="Poland" src="x"/></td>
   <td class="rechts hauptlink"><a href="#">€15.00m</a></td>
 </tr>
 <tr class="odd">
   <td class="zentriert rueckennummer bg_Abwehr"><div>23</div></td>
   <td class="posrela">
     <table class="inline-table"><tr>
       <td rowspan="2"><img src="x"/></td>
       <td class="hauptlink"><a href="/joven-cantera/profil/spieler/999999">Joven Cantera</a></td>
     </tr><tr><td>Centre-Back</td></tr></table>
   </td>
   <td class="zentriert">Jan 1, 2006 (19)</td>
   <td class="zentriert"><img title="Spain" src="x"/></td>
   <td class="rechts hauptlink">-</td>
 </tr>
</tbody></table>
</body></html>
"""
rows = tm.parse_squad(SQUAD_HTML, 2024, "131", "fc-barcelona")
check("3 jugadores", len(rows) == 3, len(rows))
r0 = rows[0]
check("nombre ter Stegen", r0["player"] == "Marc-André ter Stegen", r0["player"])
check("player_id 74857", r0["player_id"] == "74857", r0["player_id"])
check("player_slug", r0["player_slug"] == "marc-andre-ter-stegen", r0["player_slug"])
check("posicion Goalkeeper", r0["position"] == "Goalkeeper", r0["position"])
check("edad 33", r0["age"] == 33, r0["age"])
check("valor 28.0", r0["value_mio"] == 28.0, r0["value_mio"])
check("row_id year|club|player", r0["row_id"] == "2024|131|74857", r0["row_id"])
check("lewandowski 15.0", rows[1]["value_mio"] == 15.0, rows[1]["value_mio"])
check("cantera sin valor -> None", rows[2]["value_mio"] is None, rows[2]["value_mio"])
check("cantera edad 19", rows[2]["age"] == 19, rows[2]["age"])

# --------------------------------------------------------------------------- #
print("\n== aggregate_players ==")
df_players = pd.DataFrame(rows)
agg = tm.aggregate_players(df_players)
check("1 fila club-temporada", len(agg) == 1, len(agg))
a0 = agg.iloc[0]
check("squad_value = 43.0 (28+15, la cantera sin valor no suma)",
      a0["squad_value"] == 43.0, a0["squad_value"])
# squad_size cuenta jugadores CON valor conocido: 2 de los 3 (la cantera va sin
# valor), de modo que squad_value/squad_size sea coherente
check("squad_size = 2 (solo jugadores con valor)", a0["squad_size"] == 2,
      a0["squad_size"])
check("value_avg_mio = 21.5 (43/2)", a0["value_avg_mio"] == 21.5, a0["value_avg_mio"])

# un club cuya pagina devuelve TODOS los valores como '-' debe dar NaN, no 0.0:
# un 0.0 es plausible y ninguna comprobacion de nulos lo detectaria
sin_valor = df_players.copy()
sin_valor["value_mio"] = None
agg_nv = tm.aggregate_players(sin_valor)
check("todos los valores ausentes -> squad_value NaN (no 0.0)",
      pd.isna(agg_nv.iloc[0]["squad_value"]), agg_nv.iloc[0]["squad_value"])
check("squad_size = 0 cuando no hay ningun valor",
      agg_nv.iloc[0]["squad_size"] == 0, agg_nv.iloc[0]["squad_size"])

print("\n== assert_team_values (valor implausible) ==")
plaus = pd.DataFrame({"season": [2024], "team_key": ["barcelona"],
                      "squad_value": [1000.0], "squad_size": [25]})
try:
    tm.assert_team_values(plaus, strict=True)
    check("valor plausible no corta", True)
except Exception as e:  # noqa: BLE001
    check("valor plausible no corta", False, e)
for etiqueta, valor in (("valor 0.0", 0.0), ("valor NaN", float("nan"))):
    malo = plaus.assign(squad_value=[valor])
    try:
        tm.assert_team_values(malo, strict=True)
        check(f"{etiqueta} corta en strict", False, "no lanzo")
    except Exception:  # noqa: BLE001
        check(f"{etiqueta} corta en strict", True)
    try:  # sin strict solo avisa
        tm.assert_team_values(malo, strict=False)
        check(f"{etiqueta} solo avisa sin strict", True)
    except Exception as e:  # noqa: BLE001
        check(f"{etiqueta} solo avisa sin strict", False, e)

# --------------------------------------------------------------------------- #
print("\n== to_match_level (diffs) ==")
tmp = pathlib.Path("tm_tmp"); tmp.mkdir(exist_ok=True)
teamp = tmp / "team.parquet"
kb, kr = tm.tn.to_key("fc-barcelona"), tm.tn.to_key("real-madrid")
pd.DataFrame({
    "season": [2024, 2024], "team_key": [kb, kr],
    "squad_value": [1000.0, 1200.0],
}).to_parquet(teamp, index=False)
masterp = tmp / "master.csv"
pd.DataFrame({
    "match_id": [1], "home": ["FC Barcelona"], "away": ["Real Madrid"],
    "match_date": ["2024-10-26 21:00:00+02:00"], "season_name": ["24/25"],
}).to_csv(masterp, index=False)
ml = tm.to_match_level(team_parquet=teamp, master_csv=masterp)
check("una fila de partido", len(ml) == 1, len(ml))
row = ml.iloc[0]
check("home_squad_value=1000", row["home_squad_value"] == 1000.0, row.get("home_squad_value"))
check("away_squad_value=1200", row["away_squad_value"] == 1200.0, row.get("away_squad_value"))
check("squad_value_diff=-200", row["squad_value_diff"] == -200.0, row.get("squad_value_diff"))
check("match_key presente", isinstance(row["match_key"], str) and "|" in row["match_key"])

# --------------------------------------------------------------------------- #
print("\n== _season_start_year ==")
check("'24/25' -> 2024", tm._season_start_year("24/25") == 2024)
check("'2024/2025' -> 2024", tm._season_start_year("2024/2025") == 2024)
check("'2016' -> 2016", tm._season_start_year("2016") == 2016)

# --------------------------------------------------------------------------- #
print("== _run_refresh NO destructivo (C1) ==")
import tempfile

def _master_rows(club_id, n=20, value=10.0, year=2025):
    return [{"row_id": f"{year}|{club_id}|p{club_id}{i}", "season": year,
             "club_id": club_id, "club_slug": f"club-{club_id}",
             "club_name": f"Club {club_id}", "player_id": f"p{club_id}{i}",
             "player": f"jug{i}", "age": 25, "position": "DF", "value_mio": value}
            for i in range(n)]

with tempfile.TemporaryDirectory() as td:
    master = pathlib.Path(td) / "squads.csv"
    pd.DataFrame(_master_rows("131") + _master_rows("418")).to_csv(master, index=False)

    orig_discover, orig_squad = tm._discover_clubs, tm.scrape_club_squad
    try:
        tm._discover_clubs = lambda year: [
            {"club_id": "131", "club_slug": "club-131", "club_name": "Club 131"},
            {"club_id": "418", "club_slug": "club-418", "club_name": "Club 418"}]
        # el club 418 devuelve 0 filas (403 / cambio de DOM); el 131 refresca a 99.0
        tm.scrape_club_squad = lambda year, club: (
            [] if club["club_id"] == "418" else _master_rows("131", value=99.0))
        tm._run_refresh([2025], master, pause=0)
    finally:
        tm._discover_clubs, tm.scrape_club_squad = orig_discover, orig_squad

    final = pd.read_csv(master)
    n418 = (final["club_id"].astype(str) == "418").sum()
    v131 = final.loc[final["club_id"].astype(str) == "131", "value_mio"]
    check("club fallido CONSERVA sus filas", n418 == 20, f"filas 418 = {n418}")
    check("club correcto se REFRESCA", (v131 == 99.0).all(), f"valores = {sorted(set(v131))}")

with tempfile.TemporaryDirectory() as td:
    master = pathlib.Path(td) / "squads.csv"
    pd.DataFrame(_master_rows("131")).to_csv(master, index=False)
    orig_discover, orig_squad = tm._discover_clubs, tm.scrape_club_squad
    try:
        tm._discover_clubs = lambda year: [
            {"club_id": "131", "club_slug": "club-131", "club_name": "Club 131"}]
        tm.scrape_club_squad = lambda year, club: []   # TODO falla
        tm._run_refresh([2025], master, pause=0)
    finally:
        tm._discover_clubs, tm.scrape_club_squad = orig_discover, orig_squad
    check("si NADA se refresca, el master queda intacto",
          len(pd.read_csv(master)) == 20)

# --------------------------------------------------------------------------- #
print("\n" + "=" * 40)
print(f"TOTAL: {len(PASS)} OK, {len(FAIL)} FAIL")
if FAIL:
    print("fallos:", FAIL)


def test_transfermarkt():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest; cortar el proceso queda reservado al uso como guion,
    porque un `sys.exit` durante la importacion abortaria la recoleccion.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
