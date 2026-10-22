"""Validacion offline de los gates de build_scoring.py. Sin red y sin datos.

La ruta de inferencia se detiene ante cualquier bloque pre-partido que falte, no
solo ante las cuotas. La razon es que un fallo de cobertura -- artefacto ausente o
danado, reductor que revienta, merge sin coincidencias -- no se manifiesta como una
prediccion que falle, sino como una peor: `preprocess.apply` imputa las columnas
vacias con la mediana de entrenamiento, de modo que el modelo recibe un valor
plausible donde no hay dato y las probabilidades salen con aspecto normal.

Se comprueban las guardas de lectura, frescura y cobertura, y la distincion que las
ordena: la ausencia de un dato que TODAVIA no existe es informacion legitima (NaN);
la ausencia de un dato que YA deberia estar es un fallo (excepcion).
"""
import os
import sys
import pathlib
import tempfile
import time

import numpy as np
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "features",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "features",
]
FEAT = next((p for p in _CANDS if (p / "build_scoring.py").exists()), _CANDS[0])
sys.path.insert(0, str(FEAT))
sys.path.insert(0, str(FEAT.parent / "extractors"))
import build_scoring as bs
import catalogo
import columns as C
import _common
from form import form_feature_names

TMP = pathlib.Path(tempfile.mkdtemp())
HOY = pd.Timestamp("2026-03-01")

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


def revienta(fn, *a, **kw):
    try:
        fn(*a, **kw)
        return None
    except bs.ScoringGateError as e:
        return str(e)


# --------------------------------------------------------------------------- #
print("== _leer_fuente: ausencia (dato que aun no hay) vs corrupcion (fallo) ==")
mot = {}
check("artefacto ausente y NO exigible -> None y motivo anotado",
      bs._leer_fuente(TMP / "no.parquet", "fatiga", exigible=False,
                      motivos=mot) is None and "fatiga" in mot)
msg = revienta(bs._leer_fuente, TMP / "no.parquet", "Elo", exigible=True)
check("artefacto ausente y exigible -> ScoringGateError", msg is not None)
check("el error nombra bloque y ruta",
      bool(msg) and "[Elo]" in msg and "no.parquet" in msg, msg)

roto = TMP / "roto.parquet"
roto.write_bytes(b"esto no es un parquet")
msg = revienta(bs._leer_fuente, roto, "fatiga", exigible=False)
check("artefacto ILEGIBLE aborta aunque el bloque no sea exigible", msg is not None)
check("y el mensaje lo distingue de una ausencia",
      bool(msg) and "no se puede leer" in msg, msg)

ok_csv = TMP / "ok.csv"
pd.DataFrame({"a": [1, 2]}).to_csv(ok_csv, index=False)
check("artefacto legible se devuelve intacto",
      len(bs._leer_fuente(ok_csv, "x", exigible=True, csv=True)) == 2)


# --------------------------------------------------------------------------- #
print("\n== frescura del master: partidos jugados sin consolidar ==")

def fx(filas):
    """filas: (dias_atras, status, esta_en_el_master)."""
    regs = []
    for i, (dias, status, _) in enumerate(filas):
        d = (HOY - pd.Timedelta(days=dias)).strftime("%Y-%m-%d")
        regs.append({"competition": "LaLiga", "is_home": "True", "date": d,
                     "status": status, "team": f"L{i}", "opponent": f"V{i}",
                     "team_key": f"l{i}", "opp_key": f"v{i}"})
    return pd.DataFrame(regs)


def master(filas):
    keys = [f"{(HOY - pd.Timedelta(days=d)).strftime('%Y-%m-%d')}|l{i}|v{i}"
            for i, (d, _, dentro) in enumerate(filas) if dentro]
    return pd.DataFrame({"match_key": keys})


AL_DIA = [(10, "finished", True), (5, "finished", True), (3, "finished", True)]
FALTA_UNO = [(10, "finished", True), (5, "finished", False), (3, "finished", True)]

check("master al dia -> sin pendientes",
      bs._pendientes_de_consolidar(master(AL_DIA), fx(AL_DIA), HOY).empty)
check("partido jugado y no consolidado -> pendiente",
      len(bs._pendientes_de_consolidar(master(FALTA_UNO), fx(FALTA_UNO), HOY)) == 1)

for etiqueta, filas in [
        ("el partido de anoche esta en periodo de gracia", [(1, "finished", False)]),
        ("un aplazado no consolidado no cuenta (nunca se jugo)",
         [(5, "postponed", False)]),
        ("un partido futuro no cuenta", [(-3, "notstarted", False)]),
        ("fuera de la ventana de lookback no se reclama",
         [(120, "finished", False)]),
        ("un paron de selecciones no es un master viejo",
         [(20, "finished", True)])]:
    check(etiqueta,
          bs._pendientes_de_consolidar(master(filas), fx(filas), HOY).empty)

msg = revienta(bs._freshness_gate, master(FALTA_UNO), fx(FALTA_UNO), HOY,
               verbose=False)
check("el gate corta con el master viejo", msg is not None)
check("y el error lista el partido concreto", bool(msg) and "L1 vs V1" in msg, msg)
check("no corta con el master al dia",
      revienta(bs._freshness_gate, master(AL_DIA), fx(AL_DIA), HOY,
               verbose=False) is None)
check("strict=False degrada a aviso",
      revienta(bs._freshness_gate, master(FALTA_UNO), fx(FALTA_UNO), HOY,
               strict=False, verbose=False) is None)
check("sin calendario de sofascore no se inventa un fallo",
      revienta(bs._freshness_gate, master(FALTA_UNO), None, HOY,
               verbose=False) is None)


# --------------------------------------------------------------------------- #
print("\n== cobertura por bloque y por partido ==")

def matriz(n=10, **vacios):
    """Matriz de scoring simulada; `vacios={'fatiga': 3}` deja 3 partidos sin ese bloque."""
    cols = {}
    for b in bs._politica_bloques():
        for p in b.patrones:
            cols[p.rstrip("*") + "5" if p.endswith("*") else p] = np.ones(n)
    out = pd.DataFrame(cols)
    out["date"] = [f"2026-03-{d:02d}" for d in range(1, n + 1)]
    out["home_team"] = [f"local_{i}" for i in range(n)]
    out["away_team"] = [f"visit_{i}" for i in range(n)]
    for nombre, k in vacios.items():
        b = next(x for x in bs._politica_bloques() if x.nombre == nombre)
        out.loc[out.index[:k], bs._resolver_cols(out, b.patrones)] = np.nan
    return out


check("matriz completa pasa el gate",
      revienta(bs._coverage_gate, matriz(), verbose=False) is None)
msg = revienta(bs._coverage_gate, matriz(10, **{"fatiga": 3}), verbose=False)
check("fatiga vacia en el 30% (umbral 25%) -> aborta", msg is not None)
check("el error nombra bloque, porcentaje y partidos",
      bool(msg) and "fatiga" in msg and "30%" in msg and "local_0" in msg, msg)
check("fatiga vacia en el 20% -> dentro del umbral",
      revienta(bs._coverage_gate, matriz(10, **{"fatiga": 2}), verbose=False) is None)
check("Elo vacio en 1 de 10 -> justo en el umbral, pasa",
      revienta(bs._coverage_gate, matriz(10, Elo=1), verbose=False) is None)
check("Elo vacio en 2 de 10 -> aborta",
      revienta(bs._coverage_gate, matriz(10, Elo=2), verbose=False) is not None)
check("el mercado no tolera ni un hueco",
      revienta(bs._coverage_gate, matriz(10, mercado=1), verbose=False) is not None)
check("bajas ausentes en TODOS los partidos -> hueco legitimo, no aborta",
      revienta(bs._coverage_gate,
               matriz(10, **{"bajas (recuento)": 10, "bajas (valor €)": 10}),
               verbose=False) is None)

sin_elo = matriz(10).drop(columns=C._ELO)
msg = revienta(bs._coverage_gate, sin_elo, verbose=False)
check("un bloque exigible SIN columnas es incumplimiento total", msg is not None)
check("y se reporta como '(todos)'", bool(msg) and "(todos)" in msg, msg)

msg = revienta(bs._coverage_gate, matriz(10, **{"fatiga": 5}),
               {"fatiga": "el puente event_id no casa con los fixtures"},
               verbose=False)
check("la causa registrada aguas arriba viaja hasta el mensaje del gate",
      bool(msg) and "causa registrada" in msg, msg)
msg = revienta(bs._coverage_gate, matriz(10, **{"fatiga": 5}),
               {"puente event_id": "sofascore no aporta fixtures de LaLiga"},
               verbose=False)
check("a falta de causa propia se cita la del puente",
      bool(msg) and "sofascore no aporta" in msg, msg)
check("strict=False no corta",
      revienta(bs._coverage_gate, matriz(10, **{"fatiga": 9}), strict=False,
               verbose=False) is None)
# La forma se reparte en TRES bloques que comparten el rotulo `form_` y difieren
# en la fuente: el historico propio sale del master, la calidad de ocasion del
# detalle de tiro y la dureza del calendario del Elo del rival. El gate declara
# vacio un partido cuando le faltan TODAS las columnas de un bloque, asi que el
# reparto tiene que cumplir dos cosas a la vez, y las dos se comprueban aqui.
#
# Que sean DISJUNTOS: si una columna cayera en dos bloques, la ausencia completa
# de su fuente quedaria enmascarada por el otro bloque, que si tiene dato.
#
# Y que sean EXHAUSTIVOS sobre su familia: lo que el catalogo emita con rotulo
# `form_` o `cum_` y no caiga en ninguno de los tres se queda sin vigilar, y una
# columna sin vigilar es exactamente el modo de fallo silencioso que el gate
# existe para impedir. Es la prueba que atrapa el caso real: mientras cada
# atributo salia como par, los prefijos `home_form_*` bastaban; desde que el
# catalogo emite la mayoria solo como diferencia, ningun prefijo de lado las
# captura y el bloque se quedaria medio vacio sin que nada protestara.
_BLOQUES_FORMA = ["forma (histórico propio)", "forma (calidad de ocasión)",
                  "forma vs rival (dureza del calendario)"]
_por_bloque = {n: set(next(b for b in bs._politica_bloques() if b.nombre == n)
                      .patrones) for n in _BLOQUES_FORMA}
_solapes = [(a, b, _por_bloque[a] & _por_bloque[b])
            for i, a in enumerate(_BLOQUES_FORMA) for b in _BLOQUES_FORMA[i + 1:]
            if _por_bloque[a] & _por_bloque[b]]
check("los tres bloques de forma son disjuntos", not _solapes, _solapes)

_nombres = form_feature_names()
_familia = {c for base in _nombres["numeric"] + _nombres["categorical"]
            if base.startswith(("form_", "cum_")) for c in catalogo.nombres(base)}
_vigiladas = set().union(*_por_bloque.values())
check("y cubren toda la familia que el catalogo emite (nada sin vigilar)",
      _familia == _vigiladas, sorted(_familia ^ _vigiladas)[:8])
check("ninguna columna vigilada es un patron huerfano con '*'",
      not any(c.endswith("*") for c in _vigiladas),
      [c for c in _vigiladas if c.endswith("*")])


# --------------------------------------------------------------------------- #
print("\n== enganche de un bloque que revienta ==")
# Se neutralizan los reductores vecinos para aislar el camino de error: lo que se
# valida es que una excepcion en `reduce_fatigue` NO se quede en un print.
_orig = (bs.add_clubelo, bs.reduce_squad_value, bs.reduce_fatigue, bs._merge_block)


def _prep_futuro():
    played = pd.DataFrame(columns=["match_key", "home_key", "away_key", "date",
                                   "season_year"] + C._ELO + C._FATIGUE)
    up = pd.DataFrame({"match_key": ["2026-03-01|a|b"], "home_key": ["a"],
                       "away_key": ["b"], "date": ["2026-03-01"],
                       "season_year": [2025]})
    fixtures = pd.DataFrame({"competition": ["LaLiga"], "is_home": ["True"],
                             "date": ["2026-03-01"], "event_id": [1],
                             "team_key": ["a"], "opp_key": ["b"],
                             "team": ["A"], "opponent": ["B"]})
    return up, played, fixtures


def _con_stubs(strict):
    up, played, fixtures = _prep_futuro()
    bs.add_clubelo = lambda df, elo, **kw: df.assign(
        **{c: np.nan for c in C._ELO})
    bs.reduce_squad_value = lambda meta, tv: meta[["match_key"]].copy()
    bs._merge_block = lambda fut, block: fut
    def _revienta(*_a, **_k):
        raise ValueError("merge sin claves comunes")
    bs.reduce_fatigue = _revienta
    try:
        return bs._future_master_rows(up, played, None, None,
                                      fat=pd.DataFrame({"x": [1]}), av=None,
                                      fixtures=fixtures, strict_blocks=strict)
    finally:
        (bs.add_clubelo, bs.reduce_squad_value, bs.reduce_fatigue,
         bs._merge_block) = _orig


msg = revienta(_con_stubs, True)
check("un reductor que revienta interrumpe la prediccion", msg is not None)
check("el error explica que no es un hueco de calendario",
      bool(msg) and "cadena de datos" in msg and "merge sin claves" in msg, msg)
check("strict_blocks=False lo degrada a aviso (exploracion manual)",
      revienta(_con_stubs, False) is None)


def _elo_revienta(strict):
    """Mismo criterio para Elo: es un bloque exigible, no puede quedar en aviso."""
    up, played, fixtures = _prep_futuro()
    def _boom(*_a, **_k):
        raise KeyError("home_key ausente en el historico de clubelo")
    bs.add_clubelo = _boom
    bs.reduce_squad_value = lambda meta, tv: meta[["match_key"]].copy()
    bs._merge_block = lambda fut, block: fut
    try:
        return bs._future_master_rows(up, played, pd.DataFrame({"x": [1]}), None,
                                      fat=None, av=None, fixtures=fixtures,
                                      strict_blocks=strict)
    finally:
        (bs.add_clubelo, bs.reduce_squad_value, bs.reduce_fatigue,
         bs._merge_block) = _orig


msg = revienta(_elo_revienta, True)
check("un fallo al enganchar Elo tambien interrumpe", msg is not None)
check("y el mensaje identifica el bloque",
      bool(msg) and "[Elo]" in msg and "clubelo" in msg, msg)


def _sin_elo():
    """Con `elo=None` el reductor ni se invoca: el hueco lo denuncia cobertura."""
    up, played, fixtures = _prep_futuro()
    def _boom(*_a, **_k):
        raise AssertionError("add_clubelo no deberia invocarse con elo=None")
    bs.add_clubelo = _boom
    try:
        return bs._future_master_rows(up, played, None, None, fat=None, av=None,
                                      fixtures=fixtures, strict_blocks=True)
    finally:
        (bs.add_clubelo, bs.reduce_squad_value, bs.reduce_fatigue,
         bs._merge_block) = _orig


check("una fuente ausente no se engancha a ciegas (queda NaN para cobertura)",
      revienta(_sin_elo) is None)


# --------------------------------------------------------------------------- #
print("\n== la base de tipificacion del Elo se impone, no se deduce ==")
# El Elo que recibe el modelo esta TIPIFICADO dentro de la jornada, y la base de
# esa tipificacion son los veinte clubes de Primera de la temporada. En la ruta de
# inferencia eso no se puede dejar deducir de las filas que se pasan: `fut` trae
# solo los partidos de la proxima jornada, de modo que la media saldria de ocho o
# diez equipos en vez de veinte. El modelo recibiria la columna en una escala
# distinta de la que aprendio, con valores plausibles, que es la forma mas dificil
# de ver del sesgo entrenamiento-servicio. Se comprueba que `_future_master_rows`
# pasa el censo COMPLETO —los equipos del historico ya jugado mas los de la
# ventana a puntuar— y no el de `fut` a solas.
_visto: dict = {}


def _captura_censo():
    up, _played, fixtures = _prep_futuro()
    # Historico de la temporada con dos clubes que NO juegan la proxima jornada.
    played = pd.DataFrame({
        "match_key": ["2026-02-01|c|d", "2026-02-08|a|c"],
        "home_key": ["c", "a"], "away_key": ["d", "c"],
        "date": ["2026-02-01", "2026-02-08"], "season_year": [2025, 2025]})
    for c in C._ELO + C._FATIGUE:
        played[c] = np.nan

    def _espia(df, elo, *, censo=None, **kw):
        _visto["censo"] = censo
        return df.assign(**{c: np.nan for c in C._ELO})

    bs.add_clubelo = _espia
    bs.reduce_squad_value = lambda meta, tv: meta[["match_key"]].copy()
    bs._merge_block = lambda fut, block: fut
    try:
        bs._future_master_rows(up, played, pd.DataFrame({"x": [1]}), None,
                               fat=None, av=None, fixtures=fixtures,
                               strict_blocks=True)
    finally:
        (bs.add_clubelo, bs.reduce_squad_value, bs.reduce_fatigue,
         bs._merge_block) = _orig


check("enganchar el Elo en inferencia no revienta", revienta(_captura_censo) is None)
check("el censo se pasa explicitamente, no se deja deducir de `fut`",
      isinstance(_visto.get("censo"), dict) and 2025 in _visto["censo"],
      _visto.get("censo"))
check("y contiene los clubes del historico ademas de los de la jornada",
      set(_visto.get("censo", {}).get(2025, ())) == {"a", "b", "c", "d"},
      _visto.get("censo", {}).get(2025))


# --------------------------------------------------------------------------- #
print("\n== la clave del calendario se RE-DERIVA, no se lee del crudo ==")
# El crudo de sofascore guarda `team_key`/`opp_key` junto al nombre, y esa clave
# es una copia en cache de algo que depende de la tabla de alias. La cache se
# queda vieja justo cuando aparece un club nuevo: un ascendido se extrae el
# primer fin de semana de agosto con el alias todavia sin declarar, el CSV
# congela el slug de reserva, y como es acumulativo y salta por `row_id` lo ya
# guardado, ninguna re-extraccion lo corrige. Aqui se fabrica esa situacion.
_fx_stale = pd.DataFrame({
    "event_id": [1, 1, 2, 2],
    "team": ["Real Racing Club", "Villarreal", "Barcelona", "Getafe"],
    "opponent": ["Villarreal", "Real Racing Club", "Getafe", "Barcelona"],
    # la clave almacenada esta OBSOLETA para el ascendido y bien para el resto
    "team_key": ["real_racing", "villarreal", "barcelona", "getafe"],
    "opp_key": ["villarreal", "real_racing", "getafe", "barcelona"],
    "is_home": [True, False, True, False],
    "date": ["2026-08-16"] * 2 + ["2026-08-16"] * 2,
    "start_ts": [1_755_000_000] * 2 + [1_755_010_000] * 2,
    "competition": ["LaLiga"] * 4,
    "status": ["finished"] * 4,
})
_locales = bs._fixtures_local_rows(_fx_stale)
check("el ascendido recibe la clave canonica y no el slug congelado",
      "2026-08-16|racing_santander|villarreal" in set(_locales["match_key"]),
      sorted(_locales["match_key"]))
check("y la clave obsoleta no sobrevive en ningun match_key",
      not any("real_racing|" in k for k in _locales["match_key"]),
      sorted(_locales["match_key"]))
check("los clubes que ya estaban no cambian",
      "2026-08-16|barcelona|getafe" in set(_locales["match_key"]),
      sorted(_locales["match_key"]))
check("el puente de event_id hereda la clave re-derivada",
      bs._fixtures_event2key(_fx_stale).get(1)
      == "2026-08-16|racing_santander|villarreal")
check("y el sello de saque se publica bajo esa misma clave",
      "2026-08-16|racing_santander|villarreal"
      in set(bs._fixtures_kickoff_ts(_fx_stale).index))
# El cross-check de frescura comparte el convenio: si no lo compartiera, daria
# por no consolidados partidos que el master SI tiene, y abortaria la jornada.
_played = pd.DataFrame({"match_key": ["2026-08-16|racing_santander|villarreal",
                                      "2026-08-16|barcelona|getafe"]})
_pend = bs._pendientes_de_consolidar(_played, _fx_stale,
                                     pd.Timestamp("2026-08-18"))
check("el gate de frescura no denuncia un partido que el master si tiene",
      _pend.empty, list(_pend.get("match_key", [])))
# Y si falta el nombre no hay nada mejor que la clave almacenada: se usa.
_fx_sin_nombre = _fx_stale.copy()
_fx_sin_nombre.loc[0, "team"] = None
check("sin nombre se recurre a la clave almacenada como ultimo recurso",
      "2026-08-16|real_racing|villarreal"
      in set(bs._fixtures_local_rows(_fx_sin_nombre)["match_key"]),
      sorted(bs._fixtures_local_rows(_fx_sin_nombre)["match_key"]))


# --------------------------------------------------------------------------- #
print("\n== caducidad del checkpoint de descarga ==")
mcsv = TMP / "master.csv"
ck = _common.ckpt_path(mcsv)
pd.DataFrame({"row_id": ["a", "b"]}).to_csv(ck, index=False)
df, keys = _common.load_checkpoint(mcsv, key_col="row_id")
check("un checkpoint reciente se reanuda", len(df) == 2 and keys == {"a", "b"})

viejo = time.time() - 10 * 86400
os.utime(ck, (viejo, viejo))
df, keys = _common.load_checkpoint(mcsv, key_col="row_id")
check("un checkpoint de 10 dias se descarta", len(df) == 0 and keys == set())
check("y se borra para que no reaparezca en la siguiente pasada", not ck.exists())

pd.DataFrame({"row_id": ["a"]}).to_csv(ck, index=False)
os.utime(ck, (viejo, viejo))
check("max_age_days=None desactiva la caducidad",
      len(_common.load_checkpoint(mcsv, key_col="row_id", max_age_days=None)[0]) == 1)
pd.DataFrame({"row_id": ["a"]}).to_csv(ck, index=False)
os.utime(ck, (viejo, viejo))
check("el umbral es configurable",
      len(_common.load_checkpoint(mcsv, key_col="row_id", max_age_days=30)[0]) == 1)
ck.write_text("a,b\n1,2,3,4\n\"sin cerrar")
check("un checkpoint ilegible se sigue descartando con aviso",
      len(_common.load_checkpoint(mcsv, key_col="row_id")[0]) == 0)
check("sin checkpoint no pasa nada",
      _common.load_checkpoint(TMP / "otro.csv")[0].empty)


# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_scoring_gates():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest; cortar el proceso queda reservado al uso como guion,
    porque un `sys.exit` durante la importacion abortaria la recoleccion.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
