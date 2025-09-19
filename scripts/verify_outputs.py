from pathlib import Path
import sys, csv, re, glob, json

BASE = Path("outputs")

# ficheros "base" obligatorios (sin temporada)
REQUIRED_FILES = [
    # clasificación
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
    # índice global de curvas
    "cumprofit_index.csv",
    "cumprofit_index.json",
]

# carpetas que deben existir y tener al menos un fichero
REQUIRED_NONEMPTY_DIRS = [
    "matchlogs_base",
    "matchlogs_smote",
    "bet365_matchlogs",
    "cumprofit_curves",
]

def fail(msg: str):
    print(f"❌ {msg}")
    sys.exit(1)

def warn(msg: str):
    print(f"⚠️  {msg}")

def read_seasons_from_csv(path: Path) -> list[int]:
    seasons = set()
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            s = row.get("Season")
            if s is None or str(s).strip() == "":
                continue
            try:
                seasons.add(int(float(s)))
            except Exception:
                pass
    return sorted(seasons)

def seasons_from_classification() -> list[int]:
    s1 = read_seasons_from_csv(BASE / "classification_by_season_base.csv")
    s2 = read_seasons_from_csv(BASE / "classification_by_season_smote.csv")
    # usa unión, por si alguna estrategia falla en una temporada concreta
    return sorted(set(s1) | set(s2))

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

def check_matchlogs_per_season(seasons: list[int]):
    miss = []
    for s in seasons:
        for tag, folder in [("base","matchlogs_base"), ("smote","matchlogs_smote")]:
            csvp  = BASE / folder / f"matchlog_{s}.csv"
            jsonp = BASE / folder / f"matchlog_{s}.json"
            if not csvp.exists():  miss.append(str(csvp.relative_to(BASE)))
            if not jsonp.exists(): miss.append(str(jsonp.relative_to(BASE)))
    if miss:
        print("Faltan matchlogs por temporada:")
        for m in miss: print("-", m)
        fail("Matchlogs incompletos.")

def check_bet365_matchlogs_per_season(seasons: list[int]):
    miss = []
    for s in seasons:
        csvp  = BASE / "bet365_matchlogs" / f"matchlog_{s}.csv"
        jsonp = BASE / "bet365_matchlogs" / f"matchlog_{s}.json"
        if not csvp.exists():  miss.append(str(csvp.relative_to(BASE)))
        if not jsonp.exists(): miss.append(str(jsonp.relative_to(BASE)))
    if miss:
        print("Faltan bet365_matchlogs por temporada:")
        for m in miss: print("-", m)
        fail("Bet365 matchlogs incompletos.")

def check_cumprofit_curves(seasons_hint: list[int]):
    curves_dir = BASE / "cumprofit_curves"
    # debe haber al menos un par CSV/JSON
    any_csv  = glob.glob(str(curves_dir / "cumprofit_*.csv"))
    any_json = glob.glob(str(curves_dir / "cumprofit_*.json"))
    if not any_csv or not any_json:
        fail("No se encontraron curvas en outputs/cumprofit_curves/ (cumprofit_<SEASON>.csv/.json)")

    # si tenemos temporadas, comprueba su presencia
    miss = []
    for s in seasons_hint:
        cp_csv  = curves_dir / f"cumprofit_{s}.csv"
        cp_json = curves_dir / f"cumprofit_{s}.json"
        if not cp_csv.exists():  miss.append(str(cp_csv.relative_to(BASE)))
        if not cp_json.exists(): miss.append(str(cp_json.relative_to(BASE)))
    if miss:
        print("Faltan curvas por temporada (según Seasons detectadas):")
        for m in miss: print("-", m)
        fail("Curvas cumprofit incompletas.")

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

    # Los comparativos "por partido" pueden ser opcionales (depende si llamaste a esa celda)
    opt = glob.glob(str(BASE / "comparison_matchlog_*_base_vs_bet365.csv"))
    if not opt:
        warn("No hay comparativos por partido (comparison_matchlog_*_base_vs_bet365.*). Es opcional si no ejecutaste esa celda.")

def main():
    if not BASE.exists():
        fail("No existe outputs/.")

    check_required_files()
    check_required_dirs()

    seasons = seasons_from_classification()
    if not seasons:
        warn("No pude inferir temporadas desde classification_by_season_*.csv (¿columna Season?). Se harán checks suaves.")
    else:
        print(f"Temporadas detectadas: {seasons}")

    # Por-temporada
    if seasons:
        check_matchlogs_per_season(seasons)
        check_bet365_matchlogs_per_season(seasons)
        check_cumprofit_curves(seasons)
    else:
        # Chequeos suaves si no tengo seasons
        if not glob.glob(str(BASE / "matchlogs_base" / "matchlog_*.csv")):
            warn("No se encontraron matchlogs_base/*.csv")
        if not glob.glob(str(BASE / "bet365_matchlogs" / "matchlog_*.csv")):
            warn("No se encontraron bet365_matchlogs/*.csv")
        if not glob.glob(str(BASE / "cumprofit_curves" / "cumprofit_*.json")):
            warn("No se encontraron cumprofit_curves/*.json")

    check_comparisons()

    print("✔ outputs/ verificado correctamente.")

if __name__ == "__main__":
    main()
