# scripts/verify_outputs.py
from pathlib import Path
import sys, glob, re

BASE = Path("outputs")
RADAR_DIR = BASE / "radar_prematch"

# ---------------------------
# Utilidades de logging
# ---------------------------
def fail(msg: str):
    print(f"❌ {msg}")
    sys.exit(1)

def warn(msg: str):
    print(f"⚠️  {msg}")

def ok(msg: str):
    print(f"OK {msg}")

# ---------------------------
# I/O helpers
# ---------------------------
def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        import csv as _csv
        return list(_csv.DictReader(f))

def _read_csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        import csv as _csv
        reader = _csv.DictReader(f)
        return reader.fieldnames or []

def _read_seasons_from_csv(path: Path, col: str = "Season") -> list[int]:
    rows = _read_csv_rows(path)
    vals = set()
    for r in rows:
        s = r.get(col)
        # tolerancia: season en minúscula
        if s is None:
            s = r.get(col.lower())
        if s is None or str(s).strip() == "":
            continue
        try:
            vals.add(int(float(s)))
        except Exception:
            pass
    return sorted(vals)

# ---------------------------
# Nuevos ficheros requeridos
# ---------------------------
REQUIRED_FILES = [
    # reportes agregados por temporada / modelo
    "classification_report_by_season.csv",
    "metrics_main_by_season.csv",
    "metrics_market_by_season.csv",
    "metrics_market_overall.json",
    "confusion_matrices_by_season.json",
    "roc_curves_by_season.json",
    # futuros
    "future_predictions_2025.csv",
    "future_predictions_2025.json",
]

SUMMARY_RE = re.compile(r"^future_predictions_summary_\d{8}-\d{6}\.json$")  # YYYYMMDD-XXXXXX

# ---------------------------
# Checks
# ---------------------------
def check_required_files():
    missing = [p for p in REQUIRED_FILES if not (BASE / p).exists()]
    if missing:
        for m in missing:
            print(f"- Falta outputs/{m}")
        fail("Faltan archivos obligatorios.")
    ok("ficheros obligatorios presentes")

def seasons_from_sources() -> list[int]:
    # Deriva temporadas esperadas de los CSV agregados principales
    s1 = _read_seasons_from_csv(BASE / "classification_report_by_season.csv", "Season")
    s2 = _read_seasons_from_csv(BASE / "metrics_main_by_season.csv", "Season")
    seasons = sorted(set(s1) | set(s2))
    if seasons:
        print(f"Temporadas detectadas (main): {seasons}")
    else:
        warn("No pude inferir temporadas desde los CSV principales; se omite check estricto de matchlogs 'main'.")
    return seasons

def seasons_from_market() -> list[int]:
    s = _read_seasons_from_csv(BASE / "metrics_market_by_season.csv", "Season")
    if s:
        print(f"Temporadas detectadas (market): {s}")
    else:
        warn("No pude inferir temporadas desde metrics_market_by_season.csv; se omite check estricto de matchlogs_market.")
    return s

def seasons_from_future_predictions() -> list[int]:
    # Une temporadas desde todos los future_predictions_*.csv presentes
    seasons = set()
    for p in glob.glob(str(BASE / "future_predictions_*.csv")):
        seasons |= set(_read_seasons_from_csv(Path(p), "Season"))
    seasons = sorted(seasons)
    if seasons:
        print(f"Temporadas detectadas (future predictions): {seasons}")
    else:
        warn("No pude inferir temporadas desde future_predictions_*.csv; el check de radar será laxo (existencia básica).")
    return seasons

def check_matchlogs_main(seasons: list[int]):
    if not seasons:
        return
    miss = []
    for y in seasons:
        p = BASE / f"matchlogs_{y}.csv"
        if not p.exists():
            miss.append(str(p.relative_to(BASE)))
    if miss:
        print("Faltan matchlogs por temporada (main):")
        for m in miss:
            print("-", m)
        fail("Matchlogs (main) incompletos.")
    ok("matchlogs (main) por temporada presentes")

def check_matchlogs_market(seasons: list[int]):
    if not seasons:
        return
    miss = []
    for y in seasons:
        p = BASE / f"matchlogs_market_{y}.csv"
        if not p.exists():
            miss.append(str(p.relative_to(BASE)))
    if miss:
        print("Faltan matchlogs por temporada (market):")
        for m in miss:
            print("-", m)
        fail("Matchlogs (market) incompletos.")
    ok("matchlogs (market) por temporada presentes")

def check_future_summaries():
    # Al menos un summary con patrón correcto
    files = [Path(p).name for p in glob.glob(str(BASE / "future_predictions_summary_*.json"))]
    if not files:
        fail("No se encontraron future_predictions_summary_YYYYMMDD-XXXXXX.json")
    bad = [f for f in files if not SUMMARY_RE.match(f)]
    if bad:
        warn("Se encontraron summary con nombre fuera de patrón (se ignoran): " + ", ".join(bad))
    good = [f for f in files if SUMMARY_RE.match(f)]
    if not good:
        fail("Existen summaries pero ninguno cumple patrón YYYYMMDD-XXXXXX.")
    ok(f"future_predictions_summary OK (encontrados: {len(good)})")

def check_confusions_and_roc_exist():
    need = [
        BASE / "confusion_matrices_by_season.json",
        BASE / "roc_curves_by_season.json",
    ]
    missing = [str(p.relative_to(BASE)) for p in need if not p.exists()]
    if missing:
        print("Faltan agregados de confusión/ROC:")
        for m in missing:
            print("-", m)
        fail("confusion/roc por temporada incompletos.")
    ok("confusion_matrices_by_season y roc_curves_by_season presentes")

# ---------------------------
# Nuevo: verificación radar_prematch
# ---------------------------
RADAR_MIN_COLS = [
    "Season","Date","Matchweek","HomeTeam_norm","AwayTeam_norm","match_id",
    "generated_at","norm_version"
]
RADAR_MIN_ANY = [
    "home_avg_xg_last7_norm", "away_avg_xg_last7_norm",
    "home_avg_shotsontarget_last7_norm", "away_avg_shotsontarget_last7_norm"
]

def check_radar_prematch(seasons_expected: list[int]):
    # Directorio y schema
    if not RADAR_DIR.exists():
        fail("No existe outputs/radar_prematch/. Ejecuta la generación de radar prematch.")
    schema = RADAR_DIR / "schemas.json"
    if not schema.exists():
        fail("Falta outputs/radar_prematch/schemas.json")

    # Si no hay temporadas esperadas desde future_predictions, valida existencia básica
    if not seasons_expected:
        files = sorted(glob.glob(str(RADAR_DIR / "radar_prematch_*.csv")))
        if not files:
            fail("No se encontraron CSV de radar prematch (radar_prematch_*.csv).")
        ok(f"radar_prematch: encontrados {len(files)} CSV (no se pudo inferir temporadas).")
        return

    miss = []
    for y in seasons_expected:
        p = RADAR_DIR / f"radar_prematch_{int(y)}.csv"
        if not p.exists():
            miss.append(str(p.relative_to(BASE)))
            continue

        header = _read_csv_header(p)
        rows = _read_csv_rows(p)
        if not rows:
            fail(f"{p} existe pero está vacío.")

        # Tolerancia: Season o season (case-insensitive)
        header_lower = {h.lower() for h in (header or [])}
        has_season = ("season" in header_lower)
        if not has_season:
            fail(f"{p.name} carece de columna Season (aceptamos Season/season).")

        # Otras mínimas (case-insensitive)
        must_have = ["date","matchweek","hometeam_norm","awayteam_norm","match_id","generated_at","norm_version"]
        missing = [c for c in must_have if c not in header_lower]
        if missing:
            fail(f"{p.name} carece de columnas mínimas: {', '.join(missing)}")

        # Al menos algún eje normalizado esperado
        expected_norms = {
            "home_avg_xg_last7_norm","away_avg_xg_last7_norm",
            "home_avg_shotsontarget_last7_norm","away_avg_shotsontarget_last7_norm"
        }
        if not (expected_norms & set(h.lower() for h in header)):
            fail(f"{p.name} no contiene columnas de ejes normalizados esperadas (xG7/OnTarget7 *_norm).")

    if miss:
        print("Faltan archivos radar_prematch por temporada:")
        for m in miss:
            print("-", m)
        fail("radar_prematch incompleto.")
    ok("radar_prematch por temporada presentes y válidos")

# ---------------------------
# Main
# ---------------------------
def main():
    if not BASE.exists():
        fail("No existe outputs/.")

    # 1) fijos obligatorios
    check_required_files()

    # 2) futuros (summary con patrón)
    check_future_summaries()

    # 3) matchlogs main y market en base a temporadas detectadas
    seasons_main = seasons_from_sources()
    check_matchlogs_main(seasons_main)

    seasons_mkt = seasons_from_market()
    check_matchlogs_market(seasons_mkt)

    # 4) confusión/roc agregados
    check_confusions_and_roc_exist()

    # 5) radar_prematch (temporadas tomadas de future_predictions_*.csv)
    seasons_future = seasons_from_future_predictions()
    check_radar_prematch(seasons_future)

    print("✔ outputs/ verificado correctamente.")

if __name__ == "__main__":
    main()
