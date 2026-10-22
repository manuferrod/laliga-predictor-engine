"""Validacion offline de sofascore.py (capa de normalizacion). Sin red.

Cubre el renombrado de columnas de estadistica, la normalizacion de equipos y,
sobre todo, la asercion de cobertura del bloque `sofa_*`: un master con las ~200
columnas de estadistica vacias superaba TODAS las validaciones de esquema
anteriores (claves y nombres eran correctos) y se publicaba como parquet valido.
"""
import sys, pathlib
import pandas as pd

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "sofascore.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import sofascore as sofa
import _common

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


# --------------------------------------------------------------------------- #
print("== renombrado de columnas de estadistica ==")
check("prefijo ALL__ se descarta y se anade sofa_",
      sofa._rename_stat_col("ALL__Shots__Total shots__home")
      == "sofa_shots_total_shots_home",
      sofa._rename_stat_col("ALL__Shots__Total shots__home"))
check("acentos y signos -> snake_case",
      sofa._rename_stat_col("ALL__Posesión__% Balón__away").startswith("sofa_"),
      sofa._rename_stat_col("ALL__Posesión__% Balón__away"))


# --------------------------------------------------------------------------- #
print("\n== stats_missing_mask: la marca se lee en cualquier grafia ==")
def master(n=10, n_cols=120, *, flagged=(), huecos=(), vacias=0, pobre=None,
           flag_val=True):
    """Master sintetico: n partidos con n_cols columnas `sofa_*` pobladas.

    `flagged` = filas con stats_missing (ausencia REAL en Sofascore);
    `huecos`  = filas sin estadisticas y SIN marca (fallo de descarga);
    `vacias`  = n columnas completamente vacias; `pobre` = (columna, cobertura).
    """
    d = {"match_id": list(range(n))}
    for j in range(n_cols):
        d[f"sofa_c{j:03d}"] = [1.0] * n
    df = pd.DataFrame(d)
    df["stats_missing"] = [flag_val if i in set(flagged) else None for i in range(n)]
    cols = [c for c in df.columns if c.startswith("sofa_")]
    for i in list(flagged) + list(huecos):
        df.loc[i, cols] = None
    for j in range(vacias):
        df[f"sofa_c{j:03d}"] = None
    if pobre is not None:
        col, frac = pobre
        df.loc[df.index[:int(round(n * (1 - frac)))], col] = None
    return df

for val, esperado in [(True, 1), ("True", 1), ("true", 1), ("1", 1), (1.0, 1),
                      ("false", 0), ("", 0)]:
    got = int(sofa.stats_missing_mask(master(3, 120, flagged=[0], flag_val=val)).sum())
    check(f"stats_missing={val!r} -> {esperado} marcada(s)", got == esperado, got)
check("columna stats_missing ausente -> ninguna marcada",
      int(sofa.stats_missing_mask(
          master(3, 120).drop(columns=["stats_missing"])).sum()) == 0)


# --------------------------------------------------------------------------- #
print("\n== assert_stats_coverage ==")
r = sofa.assert_stats_coverage(master(10, 120))
check("master sano -> sin incidencias",
      r == {"n_stat_cols": 120, "n_sin_stats": 0, "n_huecos": 0,
            "cols_vacias": [], "cols_pobres": []}, r)

r = sofa.assert_stats_coverage(master(10, 120, flagged=[3, 7]))
check("filas stats_missing: ausencia real, no son hueco",
      r["n_sin_stats"] == 2 and r["n_huecos"] == 0, r)

def falla(df, **kw):
    try:
        sofa.assert_stats_coverage(df, **kw)
        return False
    except _common.SchemaError:
        return True

check("hueco (sin stats y sin marca) -> error", falla(master(10, 120, huecos=[5])))
check("hueco con strict=False -> solo aviso",
      not falla(master(10, 120, huecos=[5]), strict=False))
check("bloque de columnas insuficiente -> error", falla(master(10, 40)))
check("master con TODAS las estadisticas vacias -> error",
      falla(master(10, 120, vacias=120)))
check("una sola columna vacia (deriva de nombre) -> error",
      falla(master(10, 120, vacias=1)))

r = sofa.assert_stats_coverage(master(10, 120, pobre=("sofa_c005", 0.3)))
check("columna al 30% de cobertura -> aviso, no error",
      r["cols_pobres"] == ["sofa_c005"] and r["cols_vacias"] == [], r)
r = sofa.assert_stats_coverage(master(10, 120, pobre=("sofa_c005", 0.6)))
check("columna justo en el umbral -> sin aviso", r["cols_pobres"] == [], r)
r = sofa.assert_stats_coverage(master(10, 120, flagged=[0, 1, 2, 3, 4]))
check("las filas marcadas no entran en el denominador de cobertura",
      r["cols_pobres"] == [] and r["cols_vacias"] == [], r)


# --------------------------------------------------------------------------- #
print("\n== _fetch_season_events: fin de paginacion vs error ==")

class FakeSession:
    """Session de mentira: devuelve la respuesta programada para cada pagina."""
    def __init__(self, paginas):
        self.paginas = paginas
        self.pedidas = []

    def fetch_json(self, url, **_):
        self.pedidas.append(url)
        page = int(url.rsplit("/", 1)[-1])
        if page < len(self.paginas):
            return self.paginas[page]
        return {"ok": False, "err": "404", "data": None}


def ok(n=10, has_next=True):
    return {"ok": True, "data": {"events": [{"id": i} for i in range(n)],
                                 "hasNextPage": has_next}}

VACIA = {"ok": True, "data": {"events": [], "hasNextPage": True}}
SOBRE_ERROR = {"ok": True, "data": {"error": {"code": 404}}}
def caida(err="timeout"):
    return {"ok": False, "err": err, "data": None}


def eventos(paginas, **kw):
    return sofa._fetch_season_events(FakeSession(paginas), 61643, **kw)

def revienta(paginas, **kw):
    try:
        eventos(paginas, **kw)
        return None
    except _common.HarvestError as e:
        return str(e)

check("pagina vacia = fin limpio", len(eventos([ok(), ok(), VACIA])) == 20)
check("hasNextPage=False = fin limpio", len(eventos([ok(has_next=False), ok()])) == 10)
check("sobre de error = fin limpio", len(eventos([ok(), SOBRE_ERROR])) == 10)
check("404 = fin limpio (pagina inexistente)", len(eventos([ok(), caida("404")])) == 10)

msg = revienta([ok(), caida("timeout")])
check("fallo de transporte -> FetchError, no fin de paginacion", msg is not None)
check("el error identifica pagina y eventos acumulados",
      bool(msg) and "pagina 1" in msg and "10 evento" in msg, msg)
check("caida en la primera pagina tampoco pasa por 'sin datos'",
      revienta([caida("403 Forbidden")]) is not None)
check("strict=False degrada el corte a aviso",
      len(eventos([ok(), caida("timeout")], strict=False)) == 10)
check("temporada completa sin senal de fin: devuelve todo (con aviso)",
      len(eventos([ok(), ok(), ok()], max_pages=3)) == 30)


# --------------------------------------------------------------------------- #
print("\n== fusion maestro/descarga: la fila incompleta no se congela ==")

_maestro = pd.DataFrame([
    {"match_id": 1, "xg": 1.4, "shots": 12},          # capturado ya finalizado
    {"match_id": 2, "xg": None, "shots": None},       # capturado con el partido en juego
])
_nuevo = pd.DataFrame([
    {"match_id": 1, "xg": 9.9, "shots": 99},          # redundante: no debe ganar
    {"match_id": 2, "xg": 0.8, "shots": 7},           # completa el hueco
    {"match_id": 3, "xg": 2.1, "shots": 15},          # partido nuevo
])
r = _common.dedupe_preferring_complete(
    pd.concat([_maestro, _nuevo], ignore_index=True), "match_id")

check("no se pierden ni se duplican partidos",
      len(r) == 3 and r["match_id"].is_unique, r["match_id"].tolist())
check("una fila ya completa NO se sobrescribe (el maestro manda)",
      float(r.loc[r.match_id == 1, "xg"].iloc[0]) == 1.4,
      r.loc[r.match_id == 1, "xg"].tolist())
check("una fila incompleta SI se completa en la siguiente pasada",
      float(r.loc[r.match_id == 2, "xg"].iloc[0]) == 0.8,
      r.loc[r.match_id == 2, "xg"].tolist())
check("se conserva el orden del maestro", r["match_id"].tolist() == [1, 2, 3],
      r["match_id"].tolist())

_emp = pd.concat([pd.DataFrame([{"match_id": 5, "xg": 1.0}]),
                  pd.DataFrame([{"match_id": 5, "xg": 2.0}])], ignore_index=True)
r2 = _common.dedupe_preferring_complete(_emp, "match_id")
check("con igual completitud gana el maestro (fusion estable)",
      len(r2) == 1 and float(r2["xg"].iloc[0]) == 1.0, r2.to_dict("records"))


# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_sofascore():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest; cortar el proceso queda reservado al uso como guion,
    porque un `sys.exit` durante la importacion abortaria la recoleccion.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
