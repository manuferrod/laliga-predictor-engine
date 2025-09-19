# scripts/run_stage2.py
from pathlib import Path
import argparse, re, sys
import papermill as pm

NOTEBOOKS_DIR = Path("notebooks")
MANUAL_DIR = Path("manual")
OUT_DIR = Path("artifacts/executed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_NB2 = "LIMPIEZA_Y_CREACION_DE_VARS.ipynb"

def detect_run_date_from_filled() -> str | None:
    patt = re.compile(r"^b365_filled_(\d{4}-\d{2}-\d{2})\.csv$")
    if not MANUAL_DIR.exists():
        return None
    candidates = []
    for p in MANUAL_DIR.glob("b365_filled_*.csv"):
        m = patt.match(p.name)
        if m:
            candidates.append(m.group(1))
    return sorted(candidates)[-1] if candidates else None

def pick_rest_after(nb2_name: str) -> list[str]:
    """Devuelve los notebooks que van DESPUÉS de NB2 por orden alfabético."""
    nbs = sorted(p.name for p in NOTEBOOKS_DIR.glob("*.ipynb") if ".ipynb_checkpoints" not in str(p))
    if nb2_name not in nbs:
        candidates = [x for x in nbs if "LIMPIEZA" in x.upper()]
        if candidates:
            nb2_name = sorted(candidates)[0]
    return [x for x in nbs if x > nb2_name]

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
    ap = argparse.ArgumentParser(description="Stage 2: NB2 (consume) -> NB3...")
    ap.add_argument("--nb2", default=DEFAULT_NB2, help="Nombre del NB2 (por defecto: LIMPIEZA_Y_CREACION_DE_VARS.ipynb)")
    ap.add_argument("--rest", nargs="*", help="Lista explícita de notebooks posteriores (opcional). Si no, se detectan.")
    ap.add_argument("--run-date", default=None, help="RUN_DATE (YYYY-MM-DD). Si no se pasa, se detecta del CSV filled.")
    args = ap.parse_args()

    rd = args.run_date or detect_run_date_from_filled()
    if not rd:
        print("❌ No se encontró manual/b365_filled_YYYY-MM-DD.csv ni se pasó --run-date.")
        print("   Asegúrate de haber rellenado y guardado el CSV y vuelve a ejecutar.")
        sys.exit(1)

    # 1) NB2 en modo "consume" (aplica cuotas y continúa)
    run_notebook(args.nb2, {"MODE": "consume", "RUN_DATE": rd})

    # 2) Resto de notebooks (NB3, NB4, ...)
    rest = args.rest if args.rest else pick_rest_after(args.nb2)
    if not rest:
        print("⚠️  No se encontraron notebooks posteriores a NB2. Nada más que ejecutar.")
        return

    for nb in rest:
        run_notebook(nb, {"RUN_DATE": rd})

    print("\n✅ Stage 2 completado. Outputs generados. (Puedes ejecutar scripts/verify_outputs.py si lo tienes)")

if __name__ == "__main__":
    main()
