# scripts/normalize_curves_layout.py
from pathlib import Path
import shutil
import sys

BASE = Path("outputs")

def copy_tree(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    for p in src.glob("*"):
        if p.is_file():
            shutil.copy2(p, dst / p.name)

def main():
    if not BASE.exists():
        print("No existe outputs/. Nada que normalizar.")
        return

    # 1) Elegimos fuente (preferimos base; si no, smote)
    sources = [
        ("base", BASE / "cumprofit_curves_base", BASE / "cumprofit_index_base.csv", BASE / "cumprofit_index_base.json"),
        ("smote", BASE / "cumprofit_curves_smote", BASE / "cumprofit_index_smote.csv", BASE / "cumprofit_index_smote.json"),
    ]
    src_tag = None
    src_curves = src_idx_csv = src_idx_json = None
    for tag, curves_dir, idx_csv, idx_json in sources:
        if curves_dir.exists() and any(curves_dir.iterdir()):
            src_tag = tag
            src_curves, src_idx_csv, src_idx_json = curves_dir, idx_csv, idx_json
            break

    if src_curves is None:
        print("No encontré curvas en cumprofit_curves_base/ ni cumprofit_curves_smote/.")
        sys.exit(0)  # no falles la build aquí

    # 2) Destinos genéricos que exige verify_outputs.py
    dst_curves = BASE / "cumprofit_curves"
    dst_idx_csv = BASE / "cumprofit_index.csv"
    dst_idx_json = BASE / "cumprofit_index.json"

    # 3) Copiar índices
    if src_idx_csv.exists():
        shutil.copy2(src_idx_csv, dst_idx_csv)
        print(f"copiado: {src_idx_csv} -> {dst_idx_csv}")
    else:
        print(f"⚠️ No existe {src_idx_csv}")

    if src_idx_json.exists():
        shutil.copy2(src_idx_json, dst_idx_json)
        print(f"copiado: {src_idx_json} -> {dst_idx_json}")
    else:
        print(f"⚠️ No existe {src_idx_json}")

    # 4) Copiar curvas (borra destino para evitar residuos)
    if dst_curves.exists():
        shutil.rmtree(dst_curves)
    dst_curves.mkdir(parents=True, exist_ok=True)
    copy_tree(src_curves, dst_curves)
    print(f"copiado: {src_curves} -> {dst_curves}  (origen: {src_tag})")

    print("✅ Layout de curvas normalizado para verify_outputs.py")

if __name__ == "__main__":
    main()
