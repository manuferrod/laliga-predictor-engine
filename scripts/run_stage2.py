# scripts/run_stage2.py
from pathlib import Path
import argparse, re, sys
import papermill as pm

NOTEBOOKS_DIR = Path("notebooks")
MANUAL_DIR = Path("manual")
OUT_DIR = Path("artifacts/executed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_NB2 = "LIMPIEZA_Y_CREACION_DE_VARS.ipynb"
# Parquet requerido por NB2 (ajusta si tu nombre cambia)
REQUIRED_PARQUET = Path("data/02_processed/fd_xg_elo_transfermarkt_2005_2025.parquet")

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

def pick_nb1(nb2_name: str) -> str | None:
    """Devuelve el notebook inmediatamente anterior (por orden alfabético)."""
    nbs = sorted(p.name for p in NOTEBOOKS_DIR.glob("*.ipynb") if ".ipynb_checkpoints" not in str(p))
    if nb2_name not in nbs:
        candidates = [x for x in nbs if "LIMPIEZA" in x.upper()]
        if candidates:
            nb2_name = sorted(candidates)[0]
    prev = [x for x in nbs if x < nb2_name]
    return prev[-1] if prev else None

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

def ensure_required_parquet(nb2_name: str, force_run_nb1: bool = False):
    """Si falta el parquet que NB2 necesita, ejecuta NB1 para generarlo."""
    if REQUIRED_PARQUET.exists() and not force_run_nb1:
        print(f"[i] Encontrado {REQUIRED_PARQUET}, no es necesario ejecutar NB1.")
        return
    nb1 = pick_nb1(nb2_name)
    if not nb1:
        print(f"❌ Falta {REQUIRED_PARQUET} y no se pudo detectar NB1 anterior a {nb2_name}.")
        print("   Opciones: (a) indica --nb1 explícito, (b) ajusta REQUIRED_PARQUET, (c) genera el parquet en NB2.")
        sys.exit(1)
    print(f"[i] {REQUIRED_PARQUET} no existe. Ejecutando NB1 primero: {nb1}")
    run_notebook(nb1)

def main():
    ap = argparse.ArgumentParser(description="Stage 2: NB2 (consume) -> NB3...")
    ap.add_argument("--nb2", default=DEFAULT_NB2, help="Nombre de NB2 (por defecto: LIMPIEZA_Y_CREACION_DE_VARS.ipynb)")
    ap.add_argument("--nb1", default=None, help="Forzar nombre de NB1 (opcional). Si no, se detecta automáticamente.")
    ap.add_argument("--rest", nargs="*", help="Lista explícita de notebooks posteriores (opcional). Si no, se detectan.")
    ap.add_argument("--run-date", default=None, help="RUN_DATE (YYYY-MM-DD). Si no, se detecta del CSV filled.")
    ap.add_argument("--force-run-nb1", action="store_true", help="Ejecutar NB1 siempre antes de NB2.")
    args = ap.parse_args()

    rd = args.run_date or detect_run_date_from_filled()
    if not rd:
        print("❌ No se encontró manual/b365_filled_YYYY-MM-DD.csv ni se pasó --run-date.")
        sys.exit(1)

    # Asegura el parquet que NB2 leerá
    if args.nb1:
        global pick_nb1
        def pick_nb1(_): return args.nb1
    ensure_required_parquet(args.nb2, force_run_nb1=args.force_run_nb1)

    # NB2: consumir CSV rellenado y continuar
    run_notebook(args.nb2, {"MODE": "consume", "RUN_DATE": rd})

    # Resto de notebooks
    rest = args.rest if args.rest else pick_rest_after(args.nb2)
    if not rest:
        print("⚠️  No se encontraron notebooks posteriores a NB2. Nada más que ejecutar.")
        return

    for nb in rest:
        run_notebook(nb, {"RUN_DATE": rd})

    print("\n✅ Stage 2 completado. Outputs generados. (Puedes ejecutar scripts/verify_outputs.py si lo tienes)")

if __name__ == "__main__":
    main()
