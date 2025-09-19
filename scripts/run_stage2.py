# scripts/run_stage2.py
from pathlib import Path
import argparse, re, sys
import papermill as pm

NOTEBOOKS_DIR = Path("notebooks")
MANUAL_DIR = Path("manual")
OUT_DIR = Path("artifacts/executed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Ajusta si tu NB2 tiene otro nombre
DEFAULT_NB2 = "LIMPIEZA_Y_CREACION_DE_VARS.ipynb"

# Parquet que NB2 suele necesitar (producido por NB1)
REQ_PQ_FOR_NB2 = Path("data/02_processed/fd_xg_elo_transfermarkt_2005_2025.parquet")

# Parquet que MODELOS necesita (producido por PREPROCESADO/NB3)
REQ_PQ_FOR_MODELOS = Path("data/03_features/df_final.parquet")

def detect_run_date_from_filled() -> str | None:
    patt = re.compile(r"^b365_filled_(\d{4}-\d{2}-\d{2})\.csv$")
    if not MANUAL_DIR.exists():
        return None
    dates = []
    for p in MANUAL_DIR.glob("b365_filled_*.csv"):
        m = patt.match(p.name)
        if m:
            dates.append(m.group(1))
    return sorted(dates)[-1] if dates else None

def list_notebooks() -> list[str]:
    return sorted(p.name for p in NOTEBOOKS_DIR.glob("*.ipynb") if ".ipynb_checkpoints" not in str(p))

def find_nb_by_keywords(keywords: list[str]) -> str | None:
    """
    Devuelve el primer notebook cuyo nombre contenga cualquiera de los keywords (case-insensitive),
    priorizando el orden alfabético.
    """
    kws = [k.upper() for k in keywords]
    for nb in list_notebooks():
        u = nb.upper()
        if any(k in u for k in kws):
            return nb
    return None

def pick_nb1(nb2_name: str) -> str | None:
    """Notebook inmediatamente anterior (alfabético) a NB2. Útil como NB1."""
    nbs = list_notebooks()
    if nb2_name not in nbs:
        # intenta localizar NB2 por palabra clave "LIMPIEZA"
        guess = find_nb_by_keywords(["LIMPIEZA"])
        if guess:
            nb2_name = guess
    prev = [x for x in nbs if x < nb2_name]
    return prev[-1] if prev else None

def pick_rest_after(*already_run: str) -> list[str]:
    """Resto de notebooks (alfabéticos) excluyendo los ya ejecutados."""
    ran = set(already_run)
    return [nb for nb in list_notebooks() if nb not in ran]

def run_notebook(nb_name: str, params: dict | None = None):
    print(f"\n=== Ejecutando: {nb_name} ===")
    pm.execute_notebook(
        input_path=str(NOTEBOOKS_DIR / nb_name),
        output_path=str(OUT_DIR / nb_name),
        parameters=params or {},
        request_save_on_cell_execute=True,
        kernel_name="python3",
    )

def ensure_parquet_for_nb2(nb2_name: str, nb1_forced: str | None, force_run_nb1: bool):
    """
    Si falta el parquet que NB2 necesita, ejecuta NB1 (detectado o forzado).
    """
    if REQ_PQ_FOR_NB2.exists() and not force_run_nb1:
        print(f"[i] Encontrado {REQ_PQ_FOR_NB2}. No ejecuto NB1.")
        return
    nb1 = nb1_forced or pick_nb1(nb2_name)
    if not nb1:
        print(f"❌ Falta {REQ_PQ_FOR_NB2} y no pude detectar NB1 anterior a {nb2_name}.")
        print("   Opciones: usa --nb1 para forzar, o ajusta REQ_PQ_FOR_NB2 a tu ruta real.")
        sys.exit(1)
    print(f"[i] {REQ_PQ_FOR_NB2} no existe (o forzado). Ejecutando NB1: {nb1}")
    run_notebook(nb1)

def main():
    ap = argparse.ArgumentParser(description="Stage 2: NB2 (consume) -> NB3 (prepro) -> MODELOS -> resto")
    ap.add_argument("--nb2", default=DEFAULT_NB2, help="Nombre de NB2 (consume).")
    ap.add_argument("--nb1", default=None, help="Nombre de NB1 (opcional, si falta parquet de NB2).")
    ap.add_argument("--nb-preproc", default=None, help="Nombre del NB de PREPROCESADO (opcional).")
    ap.add_argument("--nb-modelos", default=None, help="Nombre del NB de MODELOS (opcional).")
    ap.add_argument("--rest", nargs="*", help="Resto de notebooks a ejecutar después (opcional).")
    ap.add_argument("--run-date", default=None, help="RUN_DATE (YYYY-MM-DD). Si no, se detecta del CSV filled.")
    ap.add_argument("--force-run-nb1", action="store_true", help="Ejecutar NB1 siempre antes de NB2.")
    args = ap.parse_args()

    # RUN_DATE
    rd = args.run_date or detect_run_date_from_filled()
    if not rd:
        print("❌ No se encontró manual/b365_filled_YYYY-MM-DD.csv ni se pasó --run-date.")
        sys.exit(1)

    # 0) Asegura parquet previo a NB2 si hace falta
    ensure_parquet_for_nb2(args.nb2, args.nb1, args.force_run_nb1)

    # 1) NB2 en modo consume
    run_notebook(args.nb2, {"MODE": "consume", "RUN_DATE": rd})

    # 2) PREPROCESADO (NB3) → genera df_final.parquet
    nb_preproc = args.nb_preproc or find_nb_by_keywords(["PREPRO", "PROCESADO", "FEATURE"])
    if nb_preproc:
        run_notebook(nb_preproc, {"RUN_DATE": rd})
    else:
        print("⚠️  No encontré NB de PREPROCESADO por nombre. Continuo, pero puede faltar df_final.parquet.")

    # 2b) guardrail: debe existir df_final.parquet tras el preprocesado
    if not REQ_PQ_FOR_MODELOS.exists():
        print(f"❌ MODELOS requiere {REQ_PQ_FOR_MODELOS}, pero no existe tras PREPROCESADO.")
        print("   Asegúrate de que tu NB de PREPROCESADO lo escribe en esa ruta.")
        sys.exit(1)

    # 3) MODELOS
    nb_modelos = args.nb_modelos or find_nb_by_keywords(["MODELO"])
    if nb_modelos:
        run_notebook(nb_modelos, {"RUN_DATE": rd})
    else:
        print("❌ No encontré el notebook de MODELOS por nombre. Pásalo con --nb-modelos.")
        sys.exit(1)

    # 4) Resto de notebooks
    rest = args.rest if args.rest else pick_rest_after(args.nb2, nb_preproc or "", nb_modelos)
    for nb in rest:
        run_notebook(nb, {"RUN_DATE": rd})

    print("\n✅ Stage 2 completado. (Si tienes verify_outputs.py, ejecútalo en el workflow).")

if __name__ == "__main__":
    main()
