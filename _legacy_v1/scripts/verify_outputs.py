# scripts/verify_outputs.py
from pathlib import Path
import sys, glob, re

BASE = Path("outputs")
RADAR_DIR = BASE / "radar_prematch"

# ---------------------------
# Logging helpers
# ---------------------------
def fail(msg: str):
    print(f"❌ {msg}")
    sys.exit(1)

def warn(msg: str):
    print(f"⚠️  {msg}")

def ok(msg: str):
    print(f"OK {msg}")

# ---------------------------
# CSV helpers (robustos)
# ---------------------------
_BOM = "\ufeff"

def _norm_header(x: str) -> str:
    if x is None:
        return ""
    return x.replace("\r", "").replace("\n", "").strip().lstrip(_BOM)

def _read_csv_header(path: Path):
    if not path.exists():
        return []
    import csv
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        return [_norm_header(h) for h in next(r, [])]

def _read_csv_rows(path: Path):
    if not path.exists():
        return []
    import csv
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = []
        for row in r:
            rows.append({ _norm_header(k): v for k, v in row.items() })
        return rows

def _read_seasons_from_csv(path: Path):
    rows = _read_csv_rows(path)
    seasons = set()
    for r in rows:
        s = r.get("Season") or r.get("season")
        if s:
            try:
                seasons.add(int(float(s)))
            except Exception:
                pass
    return sorted(seasons)

# ---------------------------
# Requisitos base
# ---------------------------
REQUIRED_FILES = [
    "classification_report_by_season.csv",
    "metrics_main_by_season.csv",
    "metrics_market_by_season.csv",
    "metrics_market_overall.json",
    "confusion_matrices_by_season.json",
    "roc_curves_by_season.json",
]

SUMMARY_RE = re.compile(r"^future_predictions_summary_\d{8}-\d{6}\.json$")

# ---------------------------
# Checks
# ---------------------------
def check_required_files():
    miss = [f for f in REQUIRED_FILES if not (BASE / f).exists()]
    if miss:
        for m in miss:
            print(f"- Falta outputs/{m}")
        fail("Faltan archivos obligatorios.")
    ok("ficheros obligatorios presentes")

def check_future_summaries():
    files = [Path(p).name for p in glob.glob(str(BASE / "future_predictions_summary_*.json"))]
    good = [f for f in files if SUMMARY_RE.match(f)]
    if not good:
        fail("No se encontró ningún future_predictions_summary válido.")
    ok(f"future_predictions_summary OK (encontrados: {len(good)})")

def check_matchlogs(prefix, seasons):
    if not seasons:
        warn(f"No se pudieron inferir temporadas ({prefix}); se omite check.")
        return
    miss = []
    for y in seasons:
        p = BASE / f"{prefix}_{y}.csv"
        if not p.exists():
            miss.append(p.name)
    if miss:
        for m in miss:
            print("-", m)
        fail(f"Matchlogs {prefix} incompletos.")
    ok(f"matchlogs {prefix} por temporada presentes")

# ---------------------------
# RADAR PREMATCH (CLAVE)
# ---------------------------
RADAR_MIN_COLS = {
    "season", "date", "matchweek",
    "hometeam_norm", "awayteam_norm",
    "match_id", "generated_at", "norm_version"
}

RADAR_NORM_HINTS = {
    "home_avg_xg", "away_avg_xg",
    "home_avg_shotsontarget", "away_avg_shotsontarget"
}

def check_radar_prematch(seasons_expected):
    if not RADAR_DIR.exists():
        fail("No existe outputs/radar_prematch/. No se han generado radares.")

    radar_files = sorted(glob.glob(str(RADAR_DIR / "radar_prematch_*.csv")))
    if not radar_files:
        fail("No se encontró ningún radar_prematch_*.csv")

    ok(f"radar_prematch: encontrados {len(radar_files)} CSV")

    # Si sabemos temporadas → exigir 1 por temporada
    if seasons_expected:
        expected = {f"radar_prematch_{y}.csv" for y in seasons_expected}
        found = {Path(p).name for p in radar_files}
        miss = expected - found
        if miss:
            for m in sorted(miss):
                print("-", m)
            fail("Faltan radares por temporada.")

    # Validación de estructura
    for p in radar_files:
        path = Path(p)
        header = {h.lower() for h in _read_csv_header(path)}
        rows = _read_csv_rows(path)

        if not rows:
            fail(f"{path.name} está vacío.")

        if not RADAR_MIN_COLS.issubset(header):
            missing = RADAR_MIN_COLS - header
            fail(f"{path.name} carece de columnas mínimas: {', '.join(missing)}")

        if not any(h.endswith("_norm") and any(k in h for k in RADAR_NORM_HINTS) for h in header):
            fail(f"{path.name} no contiene métricas *_norm válidas para radar.")

    ok("radar_prematch válidos y completos")

# ---------------------------
# MAIN
# ---------------------------
def main():
    if not BASE.exists():
        fail("No existe outputs/.")

    check_required_files()
    check_future_summaries()

    seasons_main = _read_seasons_from_csv(BASE / "classification_report_by_season.csv")
    print(f"Temporadas detectadas (main): {seasons_main}")
    check_matchlogs("matchlogs", seasons_main)

    seasons_mkt = _read_seasons_from_csv(BASE / "metrics_market_by_season.csv")
    print(f"Temporadas detectadas (market): {seasons_mkt}")
    check_matchlogs("matchlogs_market", seasons_mkt)

    seasons_future = []
    for p in glob.glob(str(BASE / "future_predictions_*.csv")):
        seasons_future += _read_seasons_from_csv(Path(p))
    seasons_future = sorted(set(seasons_future))
    print(f"Temporadas detectadas (future predictions): {seasons_future}")

    check_radar_prematch(seasons_future)

    print("✔ outputs/ verificado correctamente.")

if __name__ == "__main__":
    main()
