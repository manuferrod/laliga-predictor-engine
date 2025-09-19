# scripts/run_stage1.py
from pathlib import Path
import argparse
import papermill as pm

NOTEBOOKS_DIR = Path("notebooks")
OUT_DIR = Path("artifacts/executed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_NB2 = "LIMPIEZA_Y_CREACION_DE_VARS.ipynb"

def pick_nb1(nb2_name: str) -> str | None:
    """Elige NB1 como el .ipynb que va antes alfabéticamente que NB2 (si existe)."""
    nbs = sorted(p.name for p in NOTEBOOKS_DIR.glob("*.ipynb") if ".ipynb_checkpoints" not in str(p))
    if nb2_name not in nbs:
        candidates = [x for x in nbs if "LIMPIEZA" in x.upper()]
        if candidates:
            nb2_name = sorted(candidates)[0]
    prev = [x for x in nbs if x < nb2_name]
    return prev[-1] if prev else None

def run_notebook(nb_name: str, params: dict | None = None):
    print(f"\n=== Ejecutando: {nb_name} ===")
    pm.execute_notebook(
        input_path=str(NOTEBOOKS_DIR / nb_name),
        output_path=str(OUT_DIR / nb_name),
        parameters=params or {},
        request_save_on_cell_execute=True,
        kernel_name="python3",
    )

def main():
    ap = argparse.ArgumentParser(description="Stage 1: NB1 -> NB2 (make_template)")
    ap.add_argument("--nb2", default=DEFAULT_NB2, help="Nombre del NB2 (por defecto: LIMPIEZA_Y_CREACION_DE_VARS.ipynb)")
    ap.add_argument("--nb1", default=None, help="Nombre del NB1 (opcional; si no se pasa, se detecta automáticamente)")
    ap.add_argument("--run-date", default=None, help="RUN_DATE (YYYY-MM-DD). Si no se pasa, lo decide el notebook.")
    args = ap.parse_args()

    nb2 = args.nb2
    nb1 = args.nb1 or pick_nb1(nb2)

    if nb1:
        run_notebook(nb1)  # NB1 sin parámetros

    params_nb2 = {"MODE": "make_template"}
    if args.run_date:
        params_nb2["RUN_DATE"] = args.run_date
    run_notebook(nb2, params_nb2)

    print("\n✅ Stage 1 completado. Se ha creado la plantilla b365_template_YYYY-MM-DD.csv en manual/.")
    print("   Rellénala con B365H/B365D/B365A y guarda como b365_filled_YYYY-MM-DD.csv.")
    print("   Luego ejecuta Stage 2: python scripts/run_stage2.py")

if __name__ == "__main__":
    main()
