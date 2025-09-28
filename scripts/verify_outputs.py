# scripts/verify_outputs.py
from pathlib import Path
import sys, csv, re, glob, json

BASE = Path("outputs")

# === FICHEROS OBLIGATORIOS (ajustados a tu pipeline actual) ===
# Quitamos grids de clasificación/ROC antiguos y hacemos foco en lo que generas ahora.
REQUIRED_FILES = [
    # ROI por temporada (tu modelo)
    "roi_by_season_base.csv",
    "roi_by_season_base.json",
    "roi_by_season_smote.csv",
    "roi_by_season_smote.json",
    # baseline bet365 (grid + métricas por temporada)
    "bet365_grid.json",
    "bet365_metrics_by_season.csv",
    # comparativos temporada (modelo base vs Bet365)
    "comparison_season_base_vs_bet365.csv",
    "comparison_season_base_vs_bet365.json",
]

# === CARPETAS QUE DEBEN EXISTIR Y TENER CONTENIDO ===
# Mantengo matchlogs base+smote y bet365_matchlogs como obligatorias.
REQUIRED_NONEMPTY_DIRS = [
    "matchlogs_base",
    "matchlogs_smote",
    "bet365_matchlogs",
]

# Variantes aceptadas para las curvas e índice de curvas
CURVES_DIR_CANDIDATES = [
    "cumprofit_curves",        # genérico
    "cumprofit_curves_base",   # específico base
    "cumprofit_curves_smote",  # específico smote
]
CUMPROFIT_INDEX_VARIANTS = [
    ("cumprofit_index.csv",  "cumprofit_index.json"),
    ("cumprofit_index_base.csv",  "cumprofit_index_base.json"),
    ("cumprofit_index_smote.csv", "cumprofit_index_smote.json"),
]

def fail(msg: str):
    print(f"❌ {msg}")
    sys.exit(1)

def warn(msg: str):
    print(f"⚠️  {msg}")

# ---------------------------
# Utils para detectar seasons
# ---------------------------
def read_ints_from_csv_col(path: Path, candidates=("test_season","Season","season")) -> list[int]:
    if not path.exists():
        return []
    vals = set()
    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            for col in candidates:
                if col in rdr.fieldnames:
                    v = row.get(col)
                    if v is None or str(v).strip() == "":
                        continue
                    try:
                        vals.add(int(float(v)))
                    except Exception:
                        pass
                    break
    return sorted(vals)

def seasons_from_sources() -> list[int]:
    seasons = set()
    # 1) ROI por temporada (base + smote)
    seasons |= set(read_ints_from_csv_col(BASE / "roi_by_season_base.csv"))
    seasons |= set(read_ints_from_csv_col(BASE / "roi_by_season_smote.csv"))
    # 2) Métricas Bet365 por temporada
    seasons |= set(read_ints_from_csv_col(BASE / "bet365_metrics_by_season.csv"))
    # 3) Parseo de nombres en matchlogs (backup)
    for folder in ["matchlogs_base", "matchlogs_smote", "bet365_matchlogs"]:
        for p in (BASE / folder).glob("matchlog_*.csv"):
            m = re.search(r"matchlog_(\d+)\.csv$", p.name)
            if m:
                try:
                    seasons.add(int(m.group(1)))
                except Exception:
                    pass
    return sorted(seasons)

# --------------------------------
# Chequeos de presencia obligatoria
# --------------------------------
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

# ---------------------------------------------
# Curvas: aceptar variantes y verificar contenido
# ---------------------------------------------
def pick_curves_dir() -> Path | None:
    for d in CURVES_DIR_CANDIDATES:
        p = BASE / d
        if p.exists() and any(p.iterdir()):
            return p
    return None

def check_cumprofit_index_flexible():
    # Aceptamos cualquiera de las variantes: genérico, _base o _smote (CSV+JSON).
    for csv_name, json_name in CUMPROFIT_INDEX_VARIANTS:
        if (BASE / csv_name).exists() and (BASE / json_name).exists():
            print(f"OK índice de curvas: outputs/{csv_name} + outputs/{json_name}")
            return
    # Si no hay ninguno, fallo claro (antes fallaba por exigir sólo el genérico).
    # Si prefieres warning en lugar de fail, cambia a warn(...) y return.
    fail("No se encontró ningún índice de curvas: "
         "cumprofit_index.csv/json o cumprofit_index_base.csv/json o cumprofit_index_smote.csv/json")

def check_cumprofit_curves(seasons_hint: list[int]):
    curves_dir = pick_curves_dir()
    if not curves_dir:
        fail("No se encontraron curvas en ninguna variante: "
             "outputs/cumprofit_curves[_base|_smote]/")
    # Debe haber al menos un par CSV/JSON
    any_csv  = glob.glob(str(curves_dir / "cumprofit_*.csv"))
    any_json = glob.glob(str(curves_dir / "cumprofit_*.json"))
    if not any_csv or not any_json:
        fail(f"No se encontraron ficheros cumprofit en {curves_dir}/ "
             "(cumprofit_<SEASON>.csv/.json)")

    # Si tenemos temporadas, comprueba su presencia una a una
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
    else:
        print(f"Curvas OK en: {curves_dir}/")

# --------------------------------------------
# Matchlogs por temporada (base + smote + b365)
# --------------------------------------------
def check_matchlogs_per_season(seasons: list[int]):
    miss = []
    for s in seasons:
        for tag, folder in [("base","matchlogs_base"), ("smote","matchlogs_smote")]:
            csvp  = BASE / folder / f"matchlog_{s}.csv"
            jsonp = BASE / folder / f"matchlog_{s}.json"
            if not csvp.exists():  miss.append(str(csvp.relative_to(BASE)))
            if not jsonp.exists(): miss.append(str(jsonp.relative_to(BASE)))
    if miss:
        print("Faltan matchlogs (modelo) por temporada:")
        for m in miss: print("-", m)
        fail("Matchlogs de modelo incompletos.")

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

# ----------------------
# Comparativos obligados
# ----------------------
def check_comparisons():
    need = [
        BASE / "comparison_season_base_vs_bet365.csv",
        BASE / "comparison_season_base_vs_bet365.json",
    ]
    missing = [str(p.relative_to(BASE)) for p in need if not p.exists()]
    if missing:
        print("Faltan comparativos 'season' modelo vs Bet365:")
        for m in missing: print("-", m)
        fail("Comparativos por temporada incompletos.")

    # Comparativos por partido → opcional
    opt = glob.glob(str(BASE / "comparison_matchlog_*_base_vs_bet365.csv"))
    if not opt:
        warn("No hay comparativos por partido (comparison_matchlog_*_base_vs_bet365.*). Es opcional.")

# -----
# MAIN
# -----
def main():
    if not BASE.exists():
        fail("No existe outputs/.")

    check_required_files()
    check_required_dirs()
    check_cumprofit_index_flexible()

    seasons = seasons_from_sources()
    if seasons:
        print(f"Temporadas detectadas: {seasons}")
        check_matchlogs_per_season(seasons)
        check_bet365_matchlogs_per_season(seasons)
        check_cumprofit_curves(seasons)
    else:
        warn("No pude inferir temporadas (se harán checks suaves).")
        # Chequeos suaves si no tengo seasons
        if not glob.glob(str(BASE / "matchlogs_base" / "matchlog_*.csv")):
            warn("No se encontraron matchlogs_base/*.csv")
        if not glob.glob(str(BASE / "bet365_matchlogs" / "matchlog_*.csv")):
            warn("No se encontraron bet365_matchlogs/*.csv")
        curves_dir = pick_curves_dir()
        if curves_dir is None:
            warn("No se encontraron curvas (cumprofit_*.json) en ninguna variante.")

    check_comparisons()

    print("✔ outputs/ verificado correctamente.")

if __name__ == "__main__":
    main()
