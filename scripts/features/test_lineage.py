"""Validacion offline de la PROCEDENCIA y la COBERTURA (lineage.py). Sin red.

La tabla de procedencia es el anexo de la memoria que responde, columna a
columna, de donde sale cada feature. Una tabla asi solo vale algo si se puede
demostrar que describe el set REAL y no una version anterior de el, y ese es
justamente el modo de fallo de toda documentacion escrita a mano: no produce
ningun error al desincronizarse, simplemente empieza a mentir en silencio.

De ahi el diseno que estas comprobaciones fijan. El contrato se GENERA a partir
de las mismas constantes que usan los modulos de features, de modo que un
atributo nuevo sin procedencia declarada revienta en la construccion; los
vocabularios (naturaleza, lado, regimen, fuentes) son cerrados, de modo que una
etiqueta inventada no llega al anexo; y `check_lineage` compara en los dos
sentidos, porque una columna sin declarar y una columna declarada que falta son
sintomas de dos problemas distintos y los dos importan.

La cobertura se comprueba aparte porque mide otra cosa: no de donde sale el dato
sino en que partidos llego. Se verifica sobre un master sintetico completo, con
un hueco introducido a proposito, para fijar que el indicador de un bloque de
fuente responde a la ausencia de SU columna y no a la de ninguna otra.
"""
import pathlib
import sys

import numpy as np
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "features",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "features",
]
FEAT = next((p for p in _CANDS if (p / "lineage.py").exists()), _CANDS[0])
sys.path.insert(0, str(FEAT))

import columns as C  # noqa: E402
import lineage as L  # noqa: E402

PASS, FAIL = [], []


def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


# --------------------------------------------------------------------------- #
# La construccion misma es la primera comprobacion: `feature_lineage` invoca
# `_verificar_contrato`, que exige igualdad de conjuntos entre los nombres
# generados aqui y los que declara cada modulo de features. Si un modulo emite
# una columna que nadie declara —o al reves—, esta linea ya no llega a ejecutarse.
LIN = L.feature_lineage()
REG = L.source_registry()

print("== el contrato se genera y cuadra con los modulos ==")
check("la tabla de procedencia se construye sin violar el contrato", len(LIN) > 0)
check("una fila por columna, sin duplicados",
      LIN["columna"].is_unique,
      LIN.loc[LIN["columna"].duplicated(), "columna"].tolist()[:8])
check("el registro de fuentes tiene una fila por bloque declarado",
      len(REG) == len(L.BLOQUES_FUENTE) and REG["fuente"].is_unique)
check("ninguna columna se queda sin descripcion",
      LIN["descripcion"].str.strip().str.len().gt(0).all(),
      LIN.loc[LIN["descripcion"].str.strip().eq(""), "columna"].tolist()[:8])

# --------------------------------------------------------------------------- #
print("\n== los vocabularios son cerrados ==")
# Un vocabulario abierto degrada la tabla a texto libre: bastaria un 'derivadas'
# en plural para partir en dos un recuento del anexo sin que nada fallara.
check("naturaleza en {observada, derivada, estimada}",
      set(LIN["naturaleza"]) <= {"observada", "derivada", "estimada"},
      sorted(set(LIN["naturaleza"])))
check("lado en {local, visitante, diferencia, partido}",
      set(LIN["lado"]) <= {"local", "visitante", "diferencia", "partido"},
      sorted(set(LIN["lado"])))
check("regimen en los dos declarados en columns.py",
      set(LIN["regimen"]) <= set(C.REGIMES), sorted(set(LIN["regimen"])))

ids = {b.id for b in L.BLOQUES_FUENTE}
fuentes_usadas = {f for fila in LIN["fuentes"] for f in fila.split(", ")}
check("toda fuente citada esta declarada como bloque de fuente",
      fuentes_usadas <= ids, sorted(fuentes_usadas - ids))
check("todo bloque de fuente declarado alimenta alguna columna",
      ids <= fuentes_usadas, sorted(ids - fuentes_usadas))
check("todo bloque de fuente apunta a un proveedor declarado",
      all(b.proveedor in L.PROVEEDORES for b in L.BLOQUES_FUENTE))
check("ninguna columna se queda sin fuente",
      LIN["fuentes"].str.strip().str.len().gt(0).all(),
      LIN.loc[LIN["fuentes"].str.strip().eq(""), "columna"].tolist()[:8])

# --------------------------------------------------------------------------- #
print("\n== el regimen de la tabla es el del convenio, no una copia a mano ==")
# La columna `regimen` no se escribe: se pregunta a `columns.weekly_unavailable`.
# Esta comprobacion fija que las dos respuestas no pueden divergir, que es la
# unica forma de que el anexo y el filtro que se aplica al entrenar coincidan.
retro = set(LIN.loc[LIN["regimen"] == "retrospectivo", "columna"])
check("retrospectivo == lo que el convenio marca como no conocible en semana",
      retro == set(C.weekly_unavailable(list(LIN["columna"]))), sorted(retro)[:8])
check("toda retrospectiva lo es por el convenio o por la casa afilada",
      all("close" in c or c in set(C.SHARP_ODDS) for c in retro),
      sorted(c for c in retro if "close" not in c and c not in set(C.SHARP_ODDS)))
# Los dos motivos tienen que estar REPRESENTADOS en el anexo, no solo caber en el
# predicado: si uno de los dos dejara de producir columnas, el anexo de la memoria
# describiria un regimen con una sola causa de exclusion y la tabla se leeria mal.
check("y el anexo refleja los dos motivos, no solo el del convenio",
      any("close" in c for c in retro)
      and bool(retro & set(C.SHARP_ODDS)), sorted(retro))
check("las cuotas de cierre alimentan alguna columna retrospectiva",
      any("cuotas_cierre" in f for f in
          LIN.loc[LIN["regimen"] == "retrospectivo", "fuentes"]))

# --------------------------------------------------------------------------- #
print("\n== check_lineage compara en los dos sentidos ==")
# Set sintetico con EXACTAMENTE las columnas declaradas, mas las que por
# definicion no son features (claves, objetivo y particion).
_NO_FEAT = ["match_key", "season", "date", "home_team", "away_team",
            C.TARGET, C.TARGET_NUM, "split"]
cols = list(LIN["columna"])
exacto = pd.DataFrame(np.zeros((3, len(cols))), columns=cols)
for c in _NO_FEAT:
    exacto[c] = "x"

r = L.check_lineage(exacto)
check("un set que coincide con la tabla no reporta nada",
      not r["sin_procedencia"] and not r["declaradas_ausentes"],
      {k: v[:5] for k, v in r.items()})

r = L.check_lineage(exacto.drop(columns=[cols[0], cols[-1]]))
check("una columna declarada que falta se reporta como ausente",
      set(r["declaradas_ausentes"]) == {cols[0], cols[-1]} and not r["sin_procedencia"],
      r["declaradas_ausentes"][:5])

intruso = exacto.copy()
intruso["home_invento_9"] = 0.0
r = L.check_lineage(intruso)
check("una columna sin declarar se reporta como sin procedencia",
      r["sin_procedencia"] == ["home_invento_9"] and not r["declaradas_ausentes"],
      r["sin_procedencia"][:5])

check("las claves y el objetivo no cuentan como features",
      not (set(_NO_FEAT) & set(L.feature_columns(exacto))))

# --------------------------------------------------------------------------- #
print("\n== la cobertura de fuente responde al hueco de SU bloque ==")
# Master sintetico: todas las columnas declaradas por los bloques de fuente, con
# dato en todas las filas. Los clubes se toman de la tabla de estadios para que
# el bloque de geografia —que no vive en el master sino en el modulo— tambien
# quede cubierto.
equipos = list(L.stadium_table()["team_key"])[:2]
n = 4
master = pd.DataFrame({
    "match_key": [f"k{i}" for i in range(n)],
    "season": ["2023/24"] * n,
    "date": pd.to_datetime(["2024-01-0%d" % (i + 1) for i in range(n)]),
    "home_team": [equipos[0]] * n,
    "away_team": [equipos[1]] * n,
    "home_key": [equipos[0]] * n,
    "away_key": [equipos[1]] * n,
})
for b in L.BLOQUES_FUENTE:
    for c in b.columnas:
        if c not in master.columns:
            master[c] = 1.0

cov = L.coverage_matrix(master)
ind = [f"fuente_{b.id}" for b in L.BLOQUES_FUENTE]
check("una fila por partido", len(cov) == n and cov["match_key"].is_unique)
check("un indicador por bloque de fuente", set(ind) <= set(cov.columns),
      sorted(set(ind) - set(cov.columns)))
check("con el master completo, todas las fuentes llegan",
      bool(cov[ind].all().all()),
      [c for c in ind if not cov[c].all()])
check("n_fuentes cuenta los bloques presentes",
      (cov["n_fuentes"] == len(L.BLOQUES_FUENTE)).all())

# Hueco dirigido: se vacia UNA columna de UN bloque en UNA fila. La columna se
# toma de la declaracion del bloque y no se escribe a mano: lo que se comprueba
# aqui es el MECANISMO —un hueco en una columna apaga el indicador de su fuente y
# solo el suyo—, y de los nombres concretos ya responde el contrato de arriba.
# Escrita a mano, un renombrado la convertiria en una asignacion a una columna
# nueva que nadie mira, y el fallo apareceria como «el hueco no apaga el
# indicador» en lugar de donde realmente esta.
_COL_ELO = next(b for b in L.BLOQUES_FUENTE if b.id == "elo").columnas[0]
hueco = master.copy()
hueco.loc[0, _COL_ELO] = np.nan
cov2 = L.coverage_matrix(hueco)
check("el hueco apaga el indicador de su bloque en esa fila",
      not bool(cov2.loc[0, "fuente_elo"]))
check("y no toca al resto de bloques ni al resto de filas",
      bool(cov2.loc[1:, ind].all().all())
      and bool(cov2.loc[0, [c for c in ind if c != "fuente_elo"]].all()))

# Geografia: un club que no esta en la tabla de estadios deja el bloque a cero
# sin que falte ninguna columna del master, que es el fallo silencioso que este
# indicador existe para hacer visible.
ascendido = master.copy()
ascendido.loc[0, "home_key"] = "__club_sin_estadio__"
cov3 = L.coverage_matrix(ascendido)
check("un club sin estadio apaga la geografia de sus partidos",
      not bool(cov3.loc[0, "fuente_geografia"]) and bool(cov3.loc[1:, "fuente_geografia"].all()))

# --------------------------------------------------------------------------- #
print("\n== el resumen por temporada es agregable ==")
res = L.coverage_by_season(cov2)
check("una fila por temporada, con el recuento de partidos",
      len(res) == 1 and int(res.loc[0, "partidos"]) == n)
check("la proporcion del bloque con hueco baja y las demas no",
      abs(float(res.loc[0, "fuente_elo"]) - (n - 1) / n) < 1e-9
      and all(float(res.loc[0, c]) == 1.0 for c in ind if c != "fuente_elo"))

# --------------------------------------------------------------------------- #
print("\n== un testigo que no existe no puede pasar por cobertura cero ==")
# El punto ciego de las comprobaciones de arriba: su master se FABRICA a partir
# de `b.columnas`, asi que cualquier nombre que se declare queda satisfecho por
# construccion y un testigo con una errata pasaria. Sobre el master real ese
# testigo no existe, `_cobertura_fuente` no encuentra ninguna columna presente y
# devuelve False en todos los partidos: el bloque declara un cero perpetuo que la
# rutina semanal imprime cada jornada sin que nada falle. La guardia vive en
# `qa_features` —ahi estan los invariantes duros— y se importa aqui dentro porque
# arrastra el resto del modulo de QA, que esta bateria no necesita.
from qa_features import _check_testigos  # noqa: E402

try:
    _check_testigos(master)
    check("un master con todos los testigos pasa", True)
except AssertionError as e:
    check("un master con todos los testigos pasa", False, str(e))

_renombrada = master.rename(columns={_COL_ELO: _COL_ELO + "_v2"})
try:
    _check_testigos(_renombrada)
    check("un testigo renombrado en el master se denuncia", False)
except AssertionError as e:
    check("un testigo renombrado en el master se denuncia",
          "no existen" in str(e) and "elo" in str(e), str(e))

# Y el caso que de verdad se dio: el testigo existe pero el liston es
# inalcanzable, de modo que el indicador nunca se enciende. En nombres no hay
# nada que reprochar y la tabla de cobertura imprime un cero en las diez
# temporadas.
_vacia = master.copy()
_vacia[_COL_ELO] = np.nan
try:
    _check_testigos(_vacia)
    check("un indicador que vale cero en todos los partidos se denuncia", False)
except AssertionError as e:
    check("un indicador que vale cero en todos los partidos se denuncia",
          "cero en los" in str(e) and "elo" in str(e), str(e))

# Lo que NO debe denunciar: un bloque al 100 % en todo el historico tiene un
# indicador constante, y ahi la constancia es la respuesta correcta. Si la
# guardia mirase la varianza en vez del cero, el master limpio de arriba --donde
# todas las fuentes llegan siempre-- fallaria entero.
check("un bloque siempre presente no es un testigo roto",
      bool(L.coverage_matrix(master)["fuente_elo"].all()))

# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_lineage():
    """Punto de entrada para `pytest` (ver test_regimen.py)."""
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
