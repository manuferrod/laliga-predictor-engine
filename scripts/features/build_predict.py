"""Punto de entrada UNICO de la prediccion semanal: fixtures -> matriz lista para el modelo.

Motivacion
----------
El entrenamiento consume el artefacto preprocesado que produce
`preprocess.fit/apply`. Si la prediccion se hiciera sobre
`features_upcoming.parquet` —la matriz CRUDA que produce `build_scoring`— el
modelo recibiria valores en una escala distinta a la que aprendio, sin NaN
imputados y sin las flags de faltante: las probabilidades saldrian sin ningun
sintoma visible de error. Este script cierra ese hueco encadenando las dos
mitades y verificando la paridad columna a columna antes de escribir nada.

Cadena
------
    build_scoring()             fixtures + master extendido -> features crudas
    PreprocessState.load()      estado congelado en dev, regimen semanal
    preprocess.apply(strict)    mismas imputaciones/lambdas/escalas que en train
    invariantes P.1 - P.5       esquema, NaN, mercado crudo y regimen
    features_predict.parquet    matriz que consume el modelo en inferencia

Regimen de informacion
----------------------
La prediccion semanal se emite con dias de antelacion, asi que solo puede apoyarse
en lo que ya existe en ese momento. Por eso esta ruta consume EXCLUSIVAMENTE el
estado del regimen `semanal`: el retrospectivo incluye el precio de cierre, que no
esta publicado cuando se puntua la jornada y llegaria imputado con la mediana de
dev, produciendo una prediccion peor que su propia metrica historica. La eleccion
no se deja a una convencion de nombres: P.5 la comprueba sobre el estado cargado.

Disciplina anti-fuga
--------------------
No se vuelve a ajustar nada: `apply()` reproduce el estado aprendido solo en dev.
El holdout 25/26 y la particion de desarrollo no se leen aqui; el unico uso del
artefacto de entrenamiento es comparar el ORDEN de columnas (P.2), nunca sus valores.

Uso
---
    python scripts/features/build_predict.py          # construye y guarda
    python scripts/features/build_predict.py --dry    # construye, audita, NO escribe
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
from build_preprocess import FEATURES, prep_path, state_path  # noqa: E402
from build_scoring import build_scoring  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"

# Regimen publicable: el unico que puede puntuar una jornada por jugar.
REGIME = "semanal"
STATE = state_path(REGIME)
PREP = prep_path(REGIME)
OUT_RAW = PROC / "features_upcoming.parquet"
OUT = PROC / "features_predict.parquet"


class StaleStateError(RuntimeError):
    """El estado congelado no existe o no es compatible con la matriz de scoring.

    Se lanza ANTES de escribir para que un cambio en el conjunto de features
    (nuevo extractor, columna renombrada, poda distinta) obligue a re-ejecutar
    `build_preprocess.py` en lugar de producir una prediccion incomparable con el
    modelo entrenado.
    """


def build_predict(*, today: pd.Timestamp | None = None,
                  state_path: Path = STATE) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devuelve (crudas, preprocesadas) para los partidos de la ventana. No escribe."""
    if not Path(state_path).exists():
        raise StaleStateError(
            f"no existe {state_path}. Ejecuta primero "
            "`python scripts/features/build_preprocess.py`, que congela un estado de "
            "preprocesado por regimen; la prediccion semanal usa el del regimen "
            f"'{REGIME}'.")

    up = build_scoring(today=today) if today is not None else build_scoring()
    st = pp.PreprocessState.load(state_path)

    # P.5 el estado tiene que ser el del regimen publicable. Se comprueban las dos
    # caras: que lo declare y que su matriz lo cumpla. La segunda es la que importa,
    # porque una columna de cierre entre las features llegaria SIEMPRE vacia en
    # inferencia —`load_upcoming_processed` solo publica apertura— y `apply()` la
    # rellenaria con la mediana de dev sin lanzar nada: el modelo puntuaria cada
    # jornada apoyandose en una constante y su metrica historica dejaria de
    # describir el sistema que se ejecuta.
    if st.regime != REGIME:
        raise StaleStateError(
            f"P.5 el estado congelado es del regimen '{st.regime}' y la prediccion "
            f"semanal exige '{REGIME}'. Ejecuta `build_preprocess.py`, que escribe "
            f"un artefacto por regimen, y apunta a {Path(state_path).name}.")
    intrusas = C.weekly_unavailable(list(st.feature_cols) + list(st.flag_cols.values()))
    if intrusas:
        raise StaleStateError(
            f"P.5 el estado incluye {len(intrusas)} columnas que no existen cuando se "
            f"emite la prediccion: {intrusas}. No son conocibles a dias vista, asi que "
            "el modelo las recibiria imputadas y su rendimiento real no seria el medido.")

    # P.1 paridad train<->inferencia: strict=True convierte una matriz de scoring
    # mas estrecha que la de entrenamiento en un error explicito (no en una
    # prediccion silenciosamente degradada).
    try:
        X = pp.apply(up, st, strict=True)
    except ValueError as exc:
        raise StaleStateError(
            f"{exc}\n-> la matriz de scoring no cuadra con el estado congelado. "
            "Regenera la cadena (build_master -> build_features -> build_preprocess) "
            "para que entrenamiento e inferencia compartan el mismo conjunto de "
            "features.") from exc

    feats = list(st.feature_cols)
    n_nan = int(X[feats].isna().sum().sum())
    if n_nan:
        peor = X[feats].isna().sum()
        peor = peor[peor > 0].sort_values(ascending=False).head(10)
        raise StaleStateError(
            f"P.3 quedan {n_nan} NaN en las features tras `apply()`. El estado solo "
            f"imputa las columnas vistas en dev; revisa: {list(peor.index)}")

    # P.2 el orden de columnas que ve el modelo tiene que ser el del artefacto de
    # entrenamiento del mismo regimen. Se compara si esta en disco.
    if PREP.exists():
        prep_cols = pd.read_parquet(PREP, columns=None).columns
        prep_feats = [c for c in prep_cols if c in set(feats)]
        if prep_feats != feats:
            raise StaleStateError(
                f"P.2 el orden/conjunto de features de {PREP.name} no coincide con "
                f"{Path(state_path).name}; el artefacto en disco es de otra "
                "ejecucion. Re-ejecuta build_preprocess.py.")

    # P.4 el baseline de mercado sigue siendo calculable desde el artefacto de
    # prediccion (mkt_p_*_raw), igual que en el de entrenamiento. Se exige el
    # CONSENSO y no Pinnacle: la cuota que acompana a la prediccion publicada tiene
    # que existir para todos los partidos, y el consenso admite el respaldo de la
    # plantilla manual mientras que el desglose por casa no.
    faltan_raw = [c for c in pp.MARKET_RAW_COLS if c not in X.columns]
    if faltan_raw:
        raise StaleStateError(f"P.4 sin baseline de mercado en la prediccion: {faltan_raw}")
    pp.market_baseline(X)

    _p6_sesgo_de_imputacion(up, st)
    return up, X


# Una feature esta DESPLOMADA si al emitir llega vacia en al menos esta fraccion
# de los partidos mientras que en desarrollo faltaba en menos de `_P6_DEV_MAX`.
# Los dos umbrales son laxos a proposito: lo que se persigue no es un desajuste de
# unos puntos —el calendario y los partes medicos producen variacion legitima
# semana a semana— sino el desplome, la columna que en el historico esta y al
# predecir no esta nunca. Entre el 50 % y el 90 % no hay regla porque no hay caso:
# una fuente publica o no publica, y una cobertura a medias sostenida es un
# problema de la fuente que denuncian los testigos de `qa_features`, no este gate.
_P6_SERVE_MIN = 0.90
_P6_DEV_MAX = 0.50


def desplome_de_cobertura(hueco_dev: dict[str, float],
                          hueco_jornada: dict[str, float]) -> dict[str, tuple]:
    """Features vacias casi siempre al emitir y casi nunca en desarrollo.

    Se separa de la lectura de disco para que la regla se pueda leer y probar
    sola: recibe las dos tasas de ausencia ya medidas y devuelve
    `{feature: (hueco_dev, hueco_jornada)}` con las desplomadas. Lo que la regla
    NO denuncia importa tanto como lo que denuncia: una columna que tambien
    faltaba en desarrollo no produce sesgo —el modelo la aprendio ausente— y una
    que falta en la mitad de la jornada es ruido de calendario.
    """
    return {f: (hueco_dev[f], s) for f, s in hueco_jornada.items()
            if s >= _P6_SERVE_MIN and f in hueco_dev
            and hueco_dev[f] < _P6_DEV_MAX}


def _p6_sesgo_de_imputacion(up: pd.DataFrame, st: pp.PreprocessState, *,
                            features: Path = FEATURES) -> None:
    """P.6 ninguna feature bien cubierta en desarrollo llega vacia al predecir.

    Es la red que P.3 no puede ser. P.3 exige que no queden NaN DESPUES de
    imputar, y por eso no ve esta averia: imputar es precisamente lo que la
    esconde. Una columna ausente en la ruta de inferencia no produce un hueco,
    produce la mediana de dev repetida en los diez partidos de la jornada, cada
    jornada. El modelo la aprendio informativa y la recibe constante, y su
    metrica historica deja de describir el sistema que se ejecuta sin que nada
    falle.

    Por eso se mide sobre las matrices CRUDAS y no sobre las preprocesadas: la
    ausencia solo es observable antes de imputar. A los dos lados se cuenta lo
    mismo —NaN en la columna de `features_matches.parquet`, filas `dev` contra las
    de la jornada por jugar—, de modo que las dos tasas son comparables sin
    convenios adicionales. Los flags de faltante no sirven para esto: hay seis,
    uno por BLOQUE, y el desplome de una columna suelta no enciende ninguno.

    El caso que motivo la comprobacion fue Pinnacle: `sharp_div_*` tiene 97.9 %
    de cobertura en dev y 0 % al emitir, porque el feed de la jornada por jugar
    no trae esa casa. Se cerro declarandola en `columns.SHARP_ODDS`, pero la
    clase de averia reaparece sola —una casa que deja de publicar, un endpoint
    que cambia de esquema— y entonces conviene que avise aqui y no en la memoria.
    """
    features = Path(features)
    if not features.exists() or up.empty:
        return
    comunes = [c for c in st.feature_cols if c in up.columns]
    hueco_jornada = {c: float(up[c].isna().mean()) for c in comunes}
    sospechosas = [c for c, r in hueco_jornada.items() if r >= _P6_SERVE_MIN]
    if not sospechosas:
        return

    tabla = pd.read_parquet(features, columns=sospechosas + [C.SPLIT])
    dev = tabla[tabla[C.SPLIT] == "dev"]
    if dev.empty:
        return
    malas = desplome_de_cobertura(
        {c: float(dev[c].isna().mean()) for c in sospechosas},
        {c: hueco_jornada[c] for c in sospechosas})
    if not malas:
        return
    detalle = "\n".join(
        f"    - {f}: vacia en el {d:.1%} de dev y en el {s:.1%} de la jornada"
        for f, (d, s) in sorted(malas.items(), key=lambda kv: -kv[1][1]))
    raise StaleStateError(
        f"P.6 {len(malas)} feature(s) del regimen '{st.regime}' estan bien "
        f"cubiertas en desarrollo y llegan vacias al emitir:\n{detalle}\n\n"
        "El modelo las aprendio informativas y las recibiria imputadas en TODOS "
        "los partidos de la jornada, asi que su metrica historica no describiria "
        "el sistema que se ejecuta. Si la fuente dejo de publicarlas, declaralas "
        "en `columns.py` como no disponibles al emitir y re-ejecuta "
        "build_preprocess.py; si es un fallo de extraccion, re-extrae antes de "
        "predecir.")


def _report(up: pd.DataFrame, X: pd.DataFrame, st: pp.PreprocessState) -> None:
    print("=" * 78)
    print("PREDICT · MATRIZ LISTA PARA EL MODELO")
    print("=" * 78)
    print(f"partidos              : {len(X)}")
    print(f"regimen               : {st.regime}")
    print(f"crudas  (upcoming)    : {up.shape[1]} columnas")
    print(f"preprocesadas         : {X.shape[1]} columnas "
          f"({len(st.feature_cols)} features + meta)")
    print(f"escaladas / flags     : {st.meta.get('n_scaled', '?')} / {len(st.flag_cols)}")
    print("P.1 paridad de features OK   P.2 orden identico al artefacto de train OK")
    print("P.3 sin NaN OK               P.4 baseline de mercado calculable OK")
    print("P.5 regimen semanal OK       P.6 sin desplome de cobertura al emitir OK")

    mb = pp.market_baseline(X)
    print("\nProbabilidad implicita del mercado (referencia a batir):")
    cols = ["date", "home_team", "away_team"]
    cols = [c for c in cols if c in X.columns]
    vista = X[cols].copy()
    for c in pp.MARKET_PROB_COLS:
        vista[c] = mb[c].round(3).values
    print(vista.to_string(index=False))


def main(save: bool = True) -> pd.DataFrame:
    up, X = build_predict()
    st = pp.PreprocessState.load(STATE)
    _report(up, X, st)

    if save:
        write_df_required(up, OUT_RAW, fmt="parquet", etiqueta="build_predict")
        write_df_required(X, OUT, fmt="parquet", etiqueta="build_predict")
        print(f"\n[OK] crudas       -> {OUT_RAW} ({len(up)} filas, {up.shape[1]} cols)")
        print(f"[OK] preprocesadas -> {OUT} ({len(X)} filas, {X.shape[1]} cols)")
    else:
        print("\n[DRY] no se ha escrito nada.")
    return X


if __name__ == "__main__":
    main(save="--dry" not in sys.argv)
