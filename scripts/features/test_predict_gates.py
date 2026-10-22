"""Validacion offline de la guarda P.6 de build_predict.py. Sin red y sin datos.

Las guardas P.1 a P.4 comprueban la matriz que el modelo recibe DESPUES de
preprocesar: que las features sean las mismas y en el mismo orden, que no queden
NaN, que el baseline de mercado siga siendo calculable. Ninguna de las cuatro puede
ver el sesgo entrenamiento-servicio, y la razon es que imputar es precisamente lo
que lo esconde: una columna que la ruta de inferencia no trae no llega como hueco,
llega como la mediana de desarrollo repetida en los diez partidos de la jornada. No
hay NaN que contar, el conjunto de features cuadra, el orden cuadra, y el modelo
puntua apoyandose en una constante. La metrica historica describe entonces un
sistema distinto del que se ejecuta cada semana, que es el unico error de este
pipeline capaz de invalidar la respuesta a la pregunta del TFM sin dar la cara.

P.6 cierra ese hueco comparando la tasa de AUSENCIA de cada feature en desarrollo
con la de la jornada por jugar. Se mide sobre las matrices crudas porque despues de
imputar ya no hay nada que medir, y con la misma definicion a los dos lados —NaN en
la columna— para que las tasas sean comparables sin convenios.

Lo que estas comprobaciones fijan es el perfil de la regla en sus dos direcciones:
que denuncia el desplome (la forma de Pinnacle, bien cubierta en el historico y
vacia al emitir) y que NO denuncia lo que es legitimo, porque un gate que avisa de
mas se desactiva y entonces no protege de nada. Una columna que ya faltaba en
desarrollo no produce sesgo, el modelo la aprendio ausente; y una cobertura a
medias es la variacion normal del calendario y los partes medicos.
"""
import pathlib
import sys
import tempfile

import numpy as np
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "features",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "features",
]
FEAT = next((p for p in _CANDS if (p / "build_predict.py").exists()), _CANDS[0])
sys.path.insert(0, str(FEAT))
sys.path.insert(0, str(FEAT.parent / "extractors"))
import build_predict as bp  # noqa: E402
import columns as C  # noqa: E402
import preprocess as pp  # noqa: E402

TMP = pathlib.Path(tempfile.mkdtemp())

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


# --------------------------------------------------------------------------- #
print("== la regla: que denuncia y que no ==")
# La forma de Pinnacle: la columna esta en casi todo el historico y no esta en
# ningun partido de la jornada.
PINNACLE = ({"sharp_div_home": 0.021}, {"sharp_div_home": 1.0})
d = bp.desplome_de_cobertura(*PINNACLE)
check("el desplome se denuncia", list(d) == ["sharp_div_home"], d)
check("y devuelve las dos tasas, que son el contenido del aviso",
      d["sharp_div_home"] == (0.021, 1.0), d)

check("una columna que tambien faltaba en dev NO es desplome",
      not bp.desplome_de_cobertura({"x": 0.98}, {"x": 1.0}),
      "el modelo la aprendio ausente: no hay sesgo que denunciar")
check("la variacion semanal legitima tampoco",
      not bp.desplome_de_cobertura({"x": 0.60}, {"x": 0.95}))
check("ni una cobertura a medias en la jornada",
      not bp.desplome_de_cobertura({"x": 0.00}, {"x": 0.50}))
check("una feature que dev no tiene no revienta la regla",
      bp.desplome_de_cobertura({}, {"nueva": 1.0}) == {},
      "sin referencia historica no hay comparacion posible")
check("se denuncian todas las desplomadas, no la primera",
      set(bp.desplome_de_cobertura(
          {"a": 0.0, "b": 0.02, "c": 0.7, "d": 0.0},
          {"a": 1.0, "b": 0.97, "c": 1.0, "d": 0.3})) == {"a", "b"})

# Los umbrales son del modulo: si alguien los afloja, estas dos cambian con el.
check("el umbral de la jornada es inclusivo",
      bool(bp.desplome_de_cobertura({"x": 0.0}, {"x": bp._P6_SERVE_MIN})))
check("y el de dev es estricto",
      not bp.desplome_de_cobertura({"x": bp._P6_DEV_MAX}, {"x": 1.0}))
check("los umbrales dejan una banda muerta declarada, no solapan",
      bp._P6_DEV_MAX < bp._P6_SERVE_MIN)

# --------------------------------------------------------------------------- #
print("\n== la guarda contra las matrices crudas ==")

FEATS = ["odds_avg_h", "sharp_div_home", "home_elo_z", "home_out_value_pct"]


def features_en_disco(nombre: str, huecos_dev: dict) -> pathlib.Path:
    """Escribe un `features_matches.parquet` de juguete con split dev/holdout."""
    n = 200
    df = pd.DataFrame({c: np.linspace(1.0, 2.0, n) for c in FEATS})
    df[C.SPLIT] = ["dev"] * 180 + ["holdout"] * 20
    dev = df[C.SPLIT] == "dev"
    for col, frac in huecos_dev.items():
        idx = df.index[dev][:int(round(frac * int(dev.sum())))]
        df.loc[idx, col] = np.nan
    p = TMP / nombre
    df.to_parquet(p, index=False)
    return p


def jornada(vacias=()) -> pd.DataFrame:
    """Matriz de scoring de una jornada: 10 partidos, las indicadas sin dato."""
    up = pd.DataFrame({c: np.linspace(1.0, 2.0, 10) for c in FEATS})
    for c in vacias:
        up[c] = np.nan
    return up


def estado(feature_cols=FEATS) -> pp.PreprocessState:
    return pp.PreprocessState(regime="semanal", feature_cols=list(feature_cols),
                              flag_cols={"flag_no_absences": "home_out_value_pct"})


# En dev: la casa afilada cubierta casi siempre (5 %) y las bajas con el hueco de
# epoca de 16/17, que en el maestro real es del 24 % y aqui se exagera a un tercio
# para probar la banda intermedia.
DISCO = features_en_disco("features_matches.parquet",
                          {"sharp_div_home": 0.05, "home_out_value_pct": 1 / 3})


def revienta(up, st=None, features=DISCO):
    try:
        bp._p6_sesgo_de_imputacion(up, st or estado(), features=features)
        return None
    except bp.StaleStateError as e:
        return str(e)


msg = revienta(jornada(vacias=["sharp_div_home"]))
check("la jornada sin Pinnacle no se publica", msg is not None)
check("el aviso nombra la guarda y la columna",
      bool(msg) and "P.6" in msg and "sharp_div_home" in msg, msg)
check("y las dos tasas, para que se vea que es desplome y no ruido",
      bool(msg) and "5.0% de dev" in msg and "100.0% de la jornada" in msg, msg)
check("nombra el regimen, que es donde hay que declarar la columna",
      bool(msg) and "semanal" in msg and "columns.py" in msg, msg)

check("una jornada completa pasa", revienta(jornada()) is None)
# Un hueco parcial en dev no absuelve: 33 % frente a 100 % sigue siendo el mismo
# sesgo, solo que con una mediana calculada sobre dos tercios del histórico. La
# absolucion exige que la columna faltase en la MAYORIA de dev, y ahi ya no hay
# nada que el modelo haya aprendido informativo. Sobre el maestro real ese caso no
# se da —la columna mas vacia de dev es `dynamic_pos_change_diff`, al 38 %—, de
# modo que hoy el segundo umbral no absuelve a ninguna columna existente; esta
# para que una fuente que entre a mitad de histórico no se lea como desplome.
check("un hueco parcial en dev no absuelve a la columna",
      revienta(jornada(vacias=["home_out_value_pct"])) is not None,
      "33 % en dev frente a 100 % al emitir es el mismo sesgo")

# El criterio es el sesgo del MODELO: una columna que no entra a la matriz puede
# llegar vacia sin consecuencia, porque nadie la aprendio.
check("una columna de servicio vacia no es asunto de P.6",
      revienta(jornada(vacias=["sharp_div_home"]),
               st=estado([c for c in FEATS if c != "sharp_div_home"])) is None)

# --------------------------------------------------------------------------- #
print("\n== la guarda no inventa un veredicto cuando no puede medir ==")
check("sin artefacto de desarrollo en disco se calla",
      revienta(jornada(vacias=["sharp_div_home"]),
               features=TMP / "no_existe.parquet") is None,
      "la tasa de dev es la mitad de la comparacion; sin ella no hay regla")
check("una jornada vacia no es un desplome",
      revienta(jornada().iloc[0:0]) is None,
      "sin partidos no hay nada que puntuar")
solo_holdout = pd.read_parquet(DISCO)
solo_holdout[C.SPLIT] = "holdout"
(TMP / "sin_dev.parquet").write_bytes(b"")
solo_holdout.to_parquet(TMP / "sin_dev.parquet", index=False)
check("un artefacto sin filas dev se calla",
      revienta(jornada(vacias=["sharp_div_home"]),
               features=TMP / "sin_dev.parquet") is None)

# Los flags de faltante NO sirven para esto, y conviene que quede fijado: hay seis,
# uno por BLOQUE, y el desplome de una columna suelta no enciende ninguno. P.6 mide
# por columna justamente por eso.
check("los flags no estan en la matriz cruda y no estorban",
      revienta(jornada(), st=estado(FEATS + ["flag_no_absences"])) is None)
check("medir por bloque no habria visto el caso Pinnacle",
      "sharp_div_home" not in set(pp.FLAG_BLOCKS.values()),
      "ninguna de las seis causas cubre la casa afilada")

# --------------------------------------------------------------------------- #
print("\n== P.6 y P.3 miran cosas distintas ==")
# Es la afirmacion que justifica que exista una sexta guarda: la averia que P.6
# denuncia pasa P.3 sin un solo NaN, porque la imputacion la convierte en una
# constante. Se comprueba con el mecanismo real de `preprocess`.
n = 300
dev = pd.DataFrame({
    "odds_avg_h": np.linspace(1.5, 4.0, n),
    "sharp_div_home": np.linspace(-0.1, 0.1, n),
    C.SPLIT: "dev",
    C.TARGET_NUM: ([0, 1, 2] * n)[:n],
})
st = pp.PreprocessState(regime="semanal", feature_cols=["sharp_div_home"],
                        impute_value={"sharp_div_home": float(
                            dev["sharp_div_home"].median())},
                        impute_strat={"sharp_div_home": "neutral"})
X = pp.apply(jornada(vacias=["sharp_div_home"])[["odds_avg_h", "sharp_div_home"]], st)
check("tras imputar no queda ningun NaN que P.3 pueda contar",
      int(X[["sharp_div_home"]].isna().sum().sum()) == 0)
check("lo que llega al modelo es una constante en los diez partidos",
      X["sharp_div_home"].nunique() == 1, X["sharp_div_home"].unique())
check("y P.6 si la ve, porque mira antes de imputar",
      revienta(jornada(vacias=["sharp_div_home"])) is not None)

# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_predict_gates():
    """Punto de entrada para `pytest` (ver test_scoring_gates.py)."""
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
