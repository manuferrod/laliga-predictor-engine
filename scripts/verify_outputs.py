# scripts/verify_outputs.py
from pathlib import Path
import sys, csv, glob

BASE = Path("outputs")

# ficheros "base" obligatorios (sin temporada)
REQUIRED_FILES = [
    # clasificación (modelo vs smote)
    "confusion_grid_base.json",
    "confusion_grid_smote.json",
    "classification_grid_base.json",
    "classification_grid_smote.json",
    "classification_by_season_base.csv",
    "classification_by_season_smote.csv",
    "roc_grid_base.json",
    "roc_grid_smote.json",
    "roc_by_season_base.csv",
    "roc_by_season_smote.csv",
    # roi por temporada (cabeceras y flat csv)
    "roi_by_season_base.json",
    "roi_by_season_base.csv",
    "roi_by_season_smote.json",
    "roi_by_season_smote.csv",
    # baseline bet365 (grid + métricas por temporada)
    "bet365_grid.json",
    "bet365_metrics_by_season.csv",
    # NOTA: ya NO exigimos cumprofit_index.csv/json
]

# carpetas que deben existir y tener al menos un fichero
# ⚠️ cumprofit_curves deja de ser obligatoria
REQUIRED_NONEMPTY_DIRS = [
    "matchlogs_base",
    "matchlogs_smote",
    "bet365_matchlogs",
]

def fail(msg: str):
    print(f"❌ {msg}")
    sys.exit(1)

def warn(msg: str):
    print(f"⚠️  {msg}")

def ok(msg: str):
    print(f"OK {msg}")

def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        import csv as _csv
        return list(_csv.DictReader(f))

def _read_seasons_from_csv(path: Path, col: str) -> list[int]:
    rows = _read_csv_rows(path)
    vals = set()
    for r in rows:
        s = r.get(col)
        if s is None or str(s).strip() == "":
            continue
        try:
            vals.add(int(float(s)))
        except Exception:
            pass
    return sorted(vals)

def seasons_from_classification() -> list[int]:
    # Unión de temporadas presentes en los CSV de clasificación
    s1 = _read_seasons_from_csv(BASE / "classification_by_season_base.csv", "Season")
    s2 = _read_seasons_from_csv(BASE / "classification_by_season_smote.csv", "Season")
    return sorted(set(s1) | set(s2))

def seasons_from_bet365_metrics() -> list[int]:
    # Temporadas en las que tenemos métricas del baseline Bet365
    rows = _read_csv_rows(BASE / "bet365_metrics_by_season.csv")
    out = []
    for r in rows:
        ts = r.get("test_season")
        ntest = r.get("n_test")
        try:
            ts_i = int(float(ts))
        except Exception:
            continue
        # si existe n_test y es numérico, exige n_test>0
        if ntest is not None:
            try:
                if int(float(ntest)) <= 0:
                    continue
            except Exception:
                pass
        out.append(ts_i)
    return sorted(set(out))

def check_required_files():
    missing = [p for p in REQUIRED_FILES if not (BASE / p).exists()]
    if missing:
        for m in missing:
            print(f"- Falta outputs/{m}")
        fail("Faltan archivos obligatorios.")

def check_required_dirs():
    for d in REQUIRED_NONEMPTY_DIRS:
        p = BASE / d
        if not p.exists():
            fail(f"Falta el directorio outputs/{d}")
        if not any(p.iterdir()):
            fail(f"Directorio vacío: outputs/{d}")
    ok("directorios base (modelo/baseline) presentes")

def check_matchlogs_per_season(seasons: list[int]):
    # Los matchlogs del modelo (base/smote) se exigen para las temporadas de clasificación
    miss = []
    for s in seasons:
        for tag, folder in [("base","matchlogs_base"), ("smote","matchlogs_smote")]:
            csvp  = BASE / folder / f"matchlog_{s}.csv"
            jsonp = BASE / folder / f"matchlog_{s}.json"
            if not csvp.exists():  miss.append(str(csvp.relative_to(BASE)))
            if not jsonp.exists(): miss.append(str(jsonp.relative_to(BASE)))
    if miss:
        print("Faltan matchlogs del modelo por temporada:")
        for m in miss: print("-", m)
        fail("Matchlogs (modelo) incompletos.")
    ok("matchlogs del modelo por temporada")

def check_bet365_matchlogs():
    # Exigimos matchlogs Bet365 SOLO para las temporadas que están en bet365_metrics_by_season.csv (y con n_test>0).
    seasons = seasons_from_bet365_metrics()
    if not seasons:
        warn("No se detectaron temporadas en bet365_metrics_by_season.csv con n_test>0; se omite check estricto de bet365_matchlogs.")
        return
    miss = []
    for s in seasons:
        csvp  = BASE / "bet365_matchlogs" / f"matchlog_{s}.csv"
        jsonp = BASE / "bet365_matchlogs" / f"matchlog_{s}.json"
        if not csvp.exists():  miss.append(str(csvp.relative_to(BASE)))
        if not jsonp.exists(): miss.append(str(jsonp.relative_to(BASE)))
    if miss:
        print(f"Temporadas baseline Bet365 detectadas (n_test>0): {seasons}")
        print("Faltan bet365_matchlogs por temporada:")
        for m in miss: print("-", m)
        fail("Bet365 matchlogs incompletos.")
    ok(f"bet365_matchlogs presentes para temporadas {seasons}")

def _seasons_from_curves_folder() -> list[int]:
    seasons = set()
    for p in glob.glob(str(BASE / "cumprofit_curves" / "cumprofit_*.csv")):
        try:
            s = int(Path(p).stem.split("_")[1])
            seasons.add(s)
        except Exception:
            pass
    for p in glob.glob(str(BASE / "cumprofit_curves" / "cumprofit_*.json")):
        try:
            s = int(Path(p).stem.split("_")[1])
            seasons.add(s)
        except Exception:
            pass
    return sorted(seasons)

def check_cumprofit_curves_optional():
    curves_dir = BASE / "cumprofit_curves"
    if not curves_dir.exists():
        warn("No existe outputs/cumprofit_curves; las curvas acumuladas son opcionales.")
        return
    any_csv  = glob.glob(str(curves_dir / "cumprofit_*.csv"))
    any_json = glob.glob(str(curves_dir / "cumprofit_*.json"))
    if not any_csv or not any_json:
        warn("outputs/cumprofit_curves existe pero no hay (cumprofit_<SEASON>.csv/.json). Validación opcional omitida.")
        return

    seasons = _seasons_from_curves_folder()
    if not seasons:
        warn("Se encontraron ficheros de curvas, pero no pude inferir temporadas; validación suave.")
        ok("curvas cumprofit presentes (validación suave)")
        return

    miss = []
    for s in seasons:
        cp_csv  = curves_dir / f"cumprofit_{s}.csv"
        cp_json = curves_dir / f"cumprofit_{s}.json"
        if not cp_csv.exists():  miss.append(str(cp_csv.relative_to(BASE)))
        if not cp_json.exists(): miss.append(str(cp_json.relative_to(BASE)))
    if miss:
        print(f"Temporadas detectadas por ficheros en cumprofit_curves: {seasons}")
        print("Faltan curvas por temporada (csv/json):")
        for m in miss: print("-", m)
        fail("Curvas cumprofit incompletas.")
    ok(f"curvas cumprofit presentes para temporadas {seasons}")

def check_comparisons():
    # Los comparativos "por temporada" del modelo base son obligatorios
    need = [
        BASE / "comparison_season_base_vs_bet365.csv",
        BASE / "comparison_season_base_vs_bet365.json",
    ]
    missing = [str(p.relative_to(BASE)) for p in need if not p.exists()]
    if missing:
        print("Faltan comparativos 'season' modelo vs Bet365:")
        for m in missing: print("-", m)
        fail("Comparativos por temporada incompletos.")
    ok("comparativos por temporada (modelo base vs Bet365)")

def main():
    if not BASE.exists():
        fail("No existe outputs/.")

    check_required_files()
    check_required_dirs()

    # 1) Matchlogs del modelo (base/smote) según temporadas de clasificación
    seasons_cls = seasons_from_classification()
    if seasons_cls:
        print(f"Temporadas detectadas (clasificación): {seasons_cls}")
        check_matchlogs_per_season(seasons_cls)
    else:
        warn("No pude inferir temporadas desde classification_by_season_*.csv; salto check estricto de matchlogs del modelo.")

    # 2) Curvas (opcionales)
    check_cumprofit_curves_optional()

    # 3) Bet365: exigir sólo para temporadas con métricas/n_test>0
    check_bet365_matchlogs()

    # 4) Comparativos requeridos
    check_comparisons()

    print("✔ outputs/ verificado correctamente.")

if __name__ == "__main__":
    main()
