"""Orquestador de FEATURES (Fase A): máster -> features_matches.parquet.

Ensambla, por match_key y garantizando 1 fila por partido:

    identidad del partido (KEYS)
      + bloque PRE-PARTIDO passthrough (precio del mercado, Elo, valor de
        plantilla, fatiga, bajas, rotación)
      + columnas de SERVICIO (cuotas crudas de cada casa: no son features, pero
        el backtest y el patrón de Bet365 las necesitan en la misma tabla)
      + bloque FORMA leak-safe (acumulados, ventanas móviles y decaimiento
        exponencial con shift, más la dureza del calendario reciente)
      + bloque CONTEXTO de calendario y geografía (franja de saque, fin de
        semana, desplazamiento del visitante y derbi)
      + bloque FUERZA LATENTE (ataque y defensa estimados con el modelo de goles
        y la terna 1X2 que se deriva de ellos)
      + objetivo y (1X2) derivado de ftr
      + split dev / holdout (holdout sellado, no se mira en selección)

QUÉ columnas produce cada bloque no se decide aquí ni en cada módulo por su
cuenta: lo declara `features/catalogo.py`, que es la única fuente de verdad del
conjunto de variables. Un atributo que no figure allí no llega a esta tabla
aunque el módulo que lo calcula siga calculándolo.

Del passthrough se ocupa además `_materializa_diferencias`: el máster almacena
cada magnitud de equipo por lado y el catálogo decide si lo que llega a la matriz
es el par o la resta, de modo que las restas que ningún reductor dejó hechas se
calculan al ensamblar. Es lo que permite que declarar DIFERENCIA en el catálogo
sea suficiente, sin tocar el módulo de la fuente.

Excluye por diseño TODA columna de fuga (goles, estadística de partido, xG/rating
crudos): esas solo se han usado como materia prima de la forma. qa_features.py lo
verifica.

Además del set de features, esta pasada publica los dos artefactos de
TRAZABILIDAD que documentan el conjunto (`features/lineage.py`):

    * `feature_lineage.parquet` : una fila por columna, con su bloque, el módulo
      que la emite, la magnitud que mide, sus fuentes y su régimen. Es el anexo
      de la memoria, generado y no transcrito.
    * `coverage_matrix.parquet` : una fila por partido, con qué fuentes llegaron
      y qué bloques analíticos son calculables. Permite tabular la cobertura en
      lugar de afirmarla y condicionar la métrica a la información disponible.

Que los escriba esta pasada y no la del máster responde a que la procedencia se
declara sobre columnas del SET DE FEATURES, que solo existen aquí, y a que la
cobertura necesita leer a la vez el máster —donde un hueco significa que la
fuente no llegó— y las features —donde puede significar arranque en frío—. Cada
artefacto sigue teniendo un único escritor, que es la regla que importa.

Uso:
    python scripts/features/build_features.py         # construye y guarda
    python scripts/features/build_features.py --dry   # construye y solo reporta
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import assert_unique, write_df_required  # noqa: E402

import columns as C  # noqa: E402
from form import build_form_features  # noqa: E402
from market_form import build_market_form_features  # noqa: E402
from h2h import build_h2h_features  # noqa: E402
from standings import build_standings_features  # noqa: E402
from prematch_derived import build_prematch_derived_features  # noqa: E402
from context import build_context_features, unknown_stadiums  # noqa: E402
from strength import build_strength_features  # noqa: E402
from lineage import (  # noqa: E402
    COVERAGE_OUT, LINEAGE_OUT, check_lineage, coverage_matrix, coverage_report,
    feature_lineage, lineage_report)

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
MASTER = PROC / "master_matches.parquet"
OUT = PROC / "features_matches.parquet"

_TARGET_MAP = {"H": 0, "D": 1, "A": 2}


def _load_master() -> pd.DataFrame:
    return pd.read_parquet(MASTER)


def _materializa_diferencias(m: pd.DataFrame) -> pd.DataFrame:
    """Calcula las diferencias que el catálogo declara y el máster no almacena.

    El máster guarda cada magnitud de equipo donde se mide, es decir, por lado:
    `home_fat_rest_days` y `away_fat_rest_days`. El catálogo decide después cuál
    de las dos lecturas llega a la matriz, y para casi todas elige la diferencia.
    Cuando el reductor de la fuente ya dejó la resta hecha —varios lo hacen— no
    hay nada que calcular; cuando no, la resta se hace aquí.

    Aquí y no en los nueve reductores por un motivo de responsabilidad: restar dos
    columnas de la misma fila es aritmética sin criterio, no reducción de una
    fuente, y repartirla entre los reductores significaría que cambiar una
    declaración del catálogo obliga a tocar el módulo de la fuente. Al centrarla
    en el ensamblaje de la matriz, el catálogo vuelve a ser suficiente: declarar
    DIFERENCIA basta para que la columna exista.

    Las lecturas booleanas se pasan a `float64` antes de restar, porque la
    diferencia de dos indicadores vive en {-1, 0, 1} y no en los booleanos.
    """
    out = m
    nuevas: dict[str, pd.Series] = {}
    for col in C.PREMATCH_PASSTHROUGH:
        if not col.endswith("_diff") or col in m.columns:
            continue
        base = col[: -len("_diff")]
        h, a = f"home_{base}", f"away_{base}"
        if h in m.columns and a in m.columns:
            nuevas[col] = (m[h].astype("float64") - m[a].astype("float64"))
    if nuevas:
        out = m.assign(**nuevas)
    return out


def build_features(master: pd.DataFrame | None = None) -> pd.DataFrame:
    """Construye el set de features en memoria (no escribe)."""
    m = master if master is not None else _load_master()
    m = _materializa_diferencias(m)

    keys = [c for c in C.KEYS if c in m.columns]
    passth = [c for c in C.PREMATCH_PASSTHROUGH if c in m.columns]
    # las cuotas crudas no son features, pero acompañan a la matriz porque el
    # backtest económico y el patrón de Bet365 las liquidan (ver `columns.SERVICIO`)
    servicio = [c for c in C.SERVICIO_MASTER if c in m.columns and c not in passth]

    base = m[keys + passth + servicio + [C.TARGET_SOURCE]].copy()
    form = build_form_features(m)
    market = build_market_form_features(m)
    h2h = build_h2h_features(m)
    standings = build_standings_features(m)
    derived = build_prematch_derived_features(m)
    context = build_context_features(m)
    strength = build_strength_features(m)

    feat = (base.merge(form, on="match_key", how="left")
                .merge(market, on="match_key", how="left")
                .merge(h2h, on="match_key", how="left")
                .merge(standings, on="match_key", how="left")
                .merge(derived, on="match_key", how="left")
                .merge(context, on="match_key", how="left")
                .merge(strength, on="match_key", how="left"))

    # objetivo 1X2
    feat[C.TARGET] = feat[C.TARGET_SOURCE].astype("string")
    feat[C.TARGET_NUM] = feat[C.TARGET].map(_TARGET_MAP).astype("Int64")
    feat = feat.drop(columns=[C.TARGET_SOURCE])

    # split dev / holdout sellado
    feat[C.SPLIT] = np.where(feat["season"].isin(C.HOLDOUT_SEASONS),
                             "holdout", "dev")

    assert_unique(feat, ["match_key"], "features")
    return feat.reset_index(drop=True)


def _report(feat: pd.DataFrame) -> None:
    print("=" * 72)
    print("FEATURES (Fase A) —", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 72)
    print(f"partidos={len(feat)}  columnas={feat.shape[1]}  "
          f"match_key único={feat['match_key'].is_unique}")
    seas = sorted(feat["season"].dropna().unique())
    print(f"temporadas: {seas[0]} .. {seas[-1]}  ({len(seas)})")
    n_hold = (feat[C.SPLIT] == "holdout").sum()
    print(f"split: dev={ (feat[C.SPLIT] == 'dev').sum() }  "
          f"holdout={n_hold} (sellado: {C.HOLDOUT_SEASONS})")

    # El desglose por bloques NO se calcula aquí a base de patrones sobre los
    # nombres. Lo emite `_report_trazabilidad` a partir de la tabla de
    # procedencia, que es la que declara a qué bloque pertenece cada columna.
    # Mantener aquí una segunda clasificación por patrones significaría tener
    # dos respuestas a la misma pregunta y garantizarse que un día discrepen.

    # La geografía es la única familia que puede quedarse muda sin que ninguna
    # fuente falle: basta con que ascienda un club que la tabla de estadios no
    # contempla. Se avisa aquí porque el síntoma —dos columnas en NaN para todos
    # los partidos de ese club— lo borra después la imputación.
    sin_sede = unknown_stadiums(feat)
    if sin_sede:
        print(f"\n[AVISO] clubes sin estadio en la tabla de `context.py`: {sin_sede}"
              "\n        `away_travel_km` e `is_derby` quedan NaN en sus partidos.")

    # objetivo
    dist = feat[C.TARGET].value_counts(dropna=False, normalize=True)
    print("\ndistribución objetivo 1X2 (H/D/A):")
    for k in ("H", "D", "A"):
        print(f"  {k}: {dist.get(k, 0):.1%}")


def _report_trazabilidad(feat: pd.DataFrame, lin: pd.DataFrame,
                         cov: pd.DataFrame) -> None:
    """Procedencia y cobertura del set recién construido.

    La comprobación de completitud se imprime aquí y se convierte en invariante
    dura en `qa_features`. La división de trabajo es deliberada: esta pasada
    construye y describe, y la de QA es la que decide si el resultado es
    admisible.
    """
    print()
    lineage_report(lin)

    problema = check_lineage(feat)
    if problema["sin_procedencia"]:
        print(f"\n[AVISO] {len(problema['sin_procedencia'])} columnas del set sin "
              f"procedencia declarada: {problema['sin_procedencia'][:8]}"
              "\n        declárelas en `features/lineage.py`; `qa_features` fallará.")
    if problema["declaradas_ausentes"]:
        print(f"\n[AVISO] {len(problema['declaradas_ausentes'])} columnas declaradas "
              f"que NO están en el set: {problema['declaradas_ausentes'][:8]}"
              "\n        no es un fallo de documentación sino de datos: revise si "
              "alguna fuente no llegó al máster.")
    if not any(problema.values()):
        print("\n[OK] toda columna del set tiene procedencia declarada, y toda "
              "procedencia declarada tiene columna.")

    coverage_report(cov)


def main(save: bool = True) -> pd.DataFrame:
    master = _load_master()
    feat = build_features(master)
    _report(feat)

    lin = feature_lineage()
    cov = coverage_matrix(master, feat)
    _report_trazabilidad(feat, lin, cov)

    if save:
        write_df_required(feat, OUT, fmt="parquet", etiqueta="build_features")
        print(f"\n[OK] features -> {OUT} ({len(feat)} filas, {feat.shape[1]} cols)")
        write_df_required(lin, LINEAGE_OUT, fmt="parquet", etiqueta="build_features")
        print(f"[OK] procedencia -> {LINEAGE_OUT} ({len(lin)} columnas descritas)")
        write_df_required(cov, COVERAGE_OUT, fmt="parquet", etiqueta="build_features")
        print(f"[OK] cobertura -> {COVERAGE_OUT} ({len(cov)} filas, "
              f"{cov.shape[1]} cols)")
    else:
        print("\n[DRY] no se ha escrito nada.")
    return feat


if __name__ == "__main__":
    main(save="--dry" not in sys.argv)
