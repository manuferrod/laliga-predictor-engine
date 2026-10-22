"""Orquestador de PREPROCESADO (Fase D): features_matches -> features_prep_<régimen>.

Es el equivalente ejecutable del notebook 04: el notebook MUESTRA y AUDITA las
decisiones; este script las EJECUTA y persiste la salida que consume la fase 5
(modelado). Toda la lógica vive en `preprocess.py` (fuente única de la verdad);
aquí solo se orquesta: cargar -> fit(dev) -> invariantes -> apply(todo) -> guardar.

Disciplina anti-fuga
--------------------
`fit()` aprende EXCLUSIVAMENTE en dev (holdout 25/26 sellado). `apply()` reproduce
el estado congelado sobre dev, holdout y (en scoring) upcoming, sin volver a mirar
esas particiones.

Regímenes de información
------------------------
El ajuste se repite bajo los dos regímenes que declara `columns.REGIMES`, y cada
uno escribe su propio par de artefactos:

    features_prep_semanal.parquet        solo información conocible al predecir;
    preprocess_state_semanal.json        es el modelo que se publica y la única
                                         entrada admisible de la ruta de inferencia

    features_prep_retrospectivo.parquet  añade el precio de cierre; no se publica,
    preprocess_state_retrospectivo.json  y sirve para medir cuánta información
                                         añade el mercado entre apertura y cierre

Ajustar los dos con el mismo protocolo es lo que permite leer la diferencia entre
ambos como una magnitud del dominio —el valor del movimiento de precio— y no como
un artefacto de haber cambiado a la vez el conjunto de variables y el método.

Uso
---
    python scripts/features/build_preprocess.py         # construye y guarda
    python scripts/features/build_preprocess.py --dry   # construye, audita, NO escribe
    python scripts/features/build_preprocess.py --no-transform   # sin Yeo-Johnson (D.5)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import write_df_required  # noqa: E402
import columns as C  # noqa: E402
import preprocess as pp  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
FEATURES = PROC / "features_matches.parquet"


def prep_path(regime: str = pp.DEFAULT_REGIME) -> Path:
    """Ruta del artefacto preprocesado del régimen indicado."""
    return PROC / f"features_prep_{regime}.parquet"


def state_path(regime: str = pp.DEFAULT_REGIME) -> Path:
    """Ruta del estado congelado del régimen indicado."""
    return PROC / f"preprocess_state_{regime}.json"


def build_preprocess(feat: pd.DataFrame | None = None, *, transform: bool = True,
                     regime: str = pp.DEFAULT_REGIME):
    """fit en dev + apply a todo, bajo un régimen. Devuelve (Xall, state). No escribe."""
    if feat is None:
        feat = pd.read_parquet(FEATURES)
    if C.SPLIT not in feat.columns:
        raise ValueError(f"features_matches sin columna {C.SPLIT!r} (dev/holdout).")

    dev = feat[feat[C.SPLIT] == "dev"].copy()
    holdout = feat[feat[C.SPLIT] == "holdout"].copy()

    st = pp.fit(dev, transform=transform, regime=regime)   # aprende SOLO en dev
    Xdev = pp.apply(dev, st)
    Xhold = pp.apply(holdout, st)

    # --- invariantes de salida (E del notebook) ---
    fc = [c for c in st.feature_cols if c in Xdev.columns]
    assert Xdev[fc].isna().sum().sum() == 0,  "E.1 dev con NaN"
    assert Xhold[fc].isna().sum().sum() == 0, "E.1 holdout con NaN"
    assert list(Xdev.columns) == list(Xhold.columns), "E.2 esquema dev/holdout distinto"
    st2 = pp.fit(dev, transform=transform, regime=regime)  # refit determinista
    assert st2.drop_cols == st.drop_cols and st2.feature_cols == st.feature_cols, "E.3"
    assert len(Xdev) == len(dev) and len(Xhold) == len(holdout), "E.4 conteos de filas"

    Xall = pp.apply(feat, st)                   # dev + holdout, mismo estado

    # E.5 el baseline de mercado tiene que seguir siendo calculable DESDE el
    # artefacto: las probabilidades que ve el modelo salen escaladas, así que el
    # benchmark se lee de las copias crudas `*_raw`. Se exigen las tres lecturas del
    # precio —consenso, Pinnacle y Bet365— porque el número a batir es el mejor de las
    # tres y sin las tres la elección no sería verificable. Ninguna de ellas puede ser
    # feature: el consenso ya entra al modelo por su versión escalada, y las otras dos
    # son solo referencia de evaluación. La comprobación falla si esas columnas se
    # pierden o dejan de ser probabilidades.
    raw_cols = pp.PRICE_RAW_COLS
    missing_raw = [c for c in raw_cols if c not in Xall.columns]
    assert not missing_raw, f"E.5 sin baseline en el artefacto: {missing_raw}"
    assert not (set(raw_cols) & set(st.feature_cols)), \
        "E.5 las columnas crudas de precio no pueden ser features"
    assert not (set(pp.B365_PROB_COLS) & set(st.feature_cols)), \
        "E.5 Bet365 es referencia de evaluación, no variable del modelo"
    pp.market_baseline(Xall)     # valida rango y suma; lanza si no son probabilidades
    pp.sharp_baseline(Xall)
    pp.b365_baseline(Xall)

    # E.6 coherencia del régimen: bajo el régimen semanal, ninguna columna que solo
    # existe minutos antes del saque puede haber llegado ni a feature ni a flag. Es
    # la comprobación que convierte el régimen en una propiedad verificada del
    # artefacto y no en una intención del código que lo produce.
    if regime == "semanal":
        intrusas = C.weekly_unavailable(list(st.feature_cols) + list(st.flag_cols.values()))
        assert not intrusas, \
            f"E.6 el régimen semanal no admite estas columnas: {intrusas}"
    assert st.regime == regime, "E.6 el estado no declara el régimen con el que se ajustó"
    return Xall, st


def _resumen(Xall: pd.DataFrame, st: pp.PreprocessState) -> None:
    pp.report(st)
    n_dev = int((Xall[C.SPLIT] == "dev").sum()) if C.SPLIT in Xall else len(Xall)
    n_hold = int((Xall[C.SPLIT] == "holdout").sum()) if C.SPLIT in Xall else 0
    print(f"\nfeatures_prep [{st.regime}] = {Xall.shape}  (dev={n_dev}  holdout={n_hold})")
    print("E.1 sin NaN OK   E.2 esquema idéntico OK   E.3 refit determinista OK   "
          f"E.4 filas {n_dev}/{n_hold} OK   E.5 baseline de mercado OK   "
          "E.6 régimen coherente OK")

    # referencia a batir, calculada sobre las probabilidades crudas del artefacto.
    # Se informan las tres lecturas del precio y cuál queda como referencia: batir un
    # consenso peor que el mejor precio disponible no demostraría nada. Las métricas
    # sueltas van sobre la cobertura propia de cada precio; la comparación entre ellos
    # solo es legítima sobre la base común, y por eso se imprimen por separado.
    for label, sub in (("dev", Xall[Xall[C.SPLIT] == "dev"]),
                       ("holdout", Xall[Xall[C.SPLIT] == "holdout"])):
        if not len(sub):
            continue
        rep = pp.baselines_report(sub)
        for nombre, b in rep["precios"].items():
            if b is None:
                print(f"baseline {nombre:9s} [{label}]: no evaluable — {rep['note']}")
                continue
            print(f"baseline {nombre:9s} [{label}]: log-loss={b['log_loss']:.4f}  "
                  f"acierto={b['accuracy']:.1%}  (n={b['n']}, "
                  f"cobertura={b['coverage']:.1%})")
        if rep["paired"]:
            par = rep["paired"]
            detalle = "  ".join(
                f"{n}={v['log_loss']:.4f}" for n, v in par["por_precio"].items())
            print(f"  comparación pareada (n={par['n']}, "
                  f"cobertura={par['coverage']:.1%}): {detalle}")
            deltas = "  ".join(
                f"{n}={d:+.4f}" for n, d in par["delta_log_loss"].items()
                if n != "consenso")
            print(f"  delta log-loss frente al consenso: {deltas}  ->  "
                  f"referencia = {rep['reference']}")
        elif rep["note"]:
            print(f"  comparación pareada no disponible — {rep['note']}")


def main(save: bool = True, transform: bool = True) -> dict[str, pd.DataFrame]:
    feat = pd.read_parquet(FEATURES)
    print(f"features_matches: {feat.shape[0]} partidos x {feat.shape[1]} columnas")

    salidas: dict[str, pd.DataFrame] = {}
    estados: dict[str, pp.PreprocessState] = {}
    for regime in pp.REGIMES:
        print("\n" + "#" * 72)
        print(f"# RÉGIMEN: {regime}")
        print("#" * 72)
        Xall, st = build_preprocess(feat, transform=transform, regime=regime)
        _resumen(Xall, st)
        salidas[regime] = Xall
        estados[regime] = st

        if save:
            out_prep, out_state = prep_path(regime), state_path(regime)
            write_df_required(Xall, out_prep, fmt="parquet",
                              etiqueta=f"build_preprocess:{regime}")
            st.save(out_state)
            print(f"\n[OK] features -> {out_prep} "
                  f"({Xall.shape[0]} filas, {Xall.shape[1]} cols)")
            print(f"[OK] estado   -> {out_state} ({len(st.feature_cols)} features, "
                  f"{st.meta['n_scaled']} escaladas, {len(st.flag_cols)} flags)")

    # Lectura conjunta: la diferencia de anchura entre los dos regímenes es
    # exactamente el bloque de precio de cierre y lo que arrastra consigo.
    n_sem = len(estados["semanal"].feature_cols)
    n_ret = len(estados["retrospectivo"].feature_cols)
    print(f"\nfeatures: semanal={n_sem}  retrospectivo={n_ret}  (diferencia={n_ret - n_sem})")
    if not save:
        print("[DRY] no se ha escrito nada.")
    return salidas


if __name__ == "__main__":
    main(save="--dry" not in sys.argv,
         transform="--no-transform" not in sys.argv)
