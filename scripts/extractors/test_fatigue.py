"""Validacion offline de fatigue.py (cosecha de calendario). Sin red.

Se centra en un guardarrail concreto: la paginacion debe distinguir el FIN de datos
del FALLO de transporte. En este extractor la confusion no deja hueco visible: un
tramo de calendario perdido por un 403 se traduce en `rest_days` artificialmente
largos, un valor plausible que el modelo aprende como si fuera cierto.
"""
import sys, pathlib

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "fatigue.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import fatigue as fat
import _common

fat.time.sleep = lambda *_a, **_k: None      # el crawl no espera en los tests

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


# --------------------------------------------------------------------------- #
#  Dobles de prueba
# --------------------------------------------------------------------------- #
_TS = 1_700_000_000          # nov-2023, dentro de la ventana por defecto

class FakeSession:
    """Devuelve la respuesta programada para cada pagina de last/ y next/."""
    def __init__(self, last=(), siguiente=()):
        self.last, self.siguiente = list(last), list(siguiente)
        self.pedidas = []

    def fetch_json(self, url, **_):
        self.pedidas.append(url)
        seq = self.last if "/last/" in url else self.siguiente
        page = int(url.rsplit("/", 1)[-1])
        if page < len(seq):
            return seq[page]
        return {"ok": False, "err": "404", "data": None}


def ok(ids, *, ts=_TS, has_next=True):
    """Pagina valida con un evento por id (el equipo 1 juega en casa)."""
    return {"ok": True, "data": {
        "events": [{"id": i, "startTimestamp": ts,
                    "homeTeam": {"id": 1, "name": "Real Betis"},
                    "awayTeam": {"id": 2, "name": "Sevilla FC"},
                    "tournament": {"name": "LaLiga"},
                    "status": {"type": "finished"}} for i in ids],
        "hasNextPage": has_next}}

def caida(err="timeout"):
    return {"ok": False, "err": err, "data": None}

VACIA = {"ok": True, "data": {"events": [], "hasNextPage": True}}


def fixtures(last=(), siguiente=(), **kw):
    return fat._fetch_team_fixtures(FakeSession(last, siguiente), 1,
                                    since_ts=0, **kw)

def revienta(fn, *a, **kw):
    try:
        fn(*a, **kw)
        return None
    except _common.HarvestError as e:
        return str(e)


# --------------------------------------------------------------------------- #
print("== _fetch_team_fixtures: fin de calendario vs error ==")
check("paginas validas hasta la vacia",
      len(fixtures(last=[ok([1, 2]), ok([3]), VACIA])) == 3)
check("hasNextPage=False corta limpio",
      len(fixtures(last=[ok([1, 2], has_next=False), ok([3])])) == 2)
check("404 en last = fin limpio", len(fixtures(last=[ok([1]), caida("404")])) == 1)
check("404 en next = equipo sin proximos, no es error",
      len(fixtures(last=[ok([1]), VACIA], siguiente=[caida("404")])) == 1)
check("los proximos se acumulan con los pasados",
      len(fixtures(last=[ok([1]), VACIA], siguiente=[ok([9]), VACIA])) == 2)

msg = revienta(fixtures, last=[ok([1]), caida("403 Forbidden")])
check("fallo de transporte en last -> FetchError", msg is not None)
check("el error identifica tramo, pagina y acumulado",
      bool(msg) and "last/1" in msg and "1 partido" in msg, msg)
check("fallo de transporte en next tampoco se toma por 'sin proximos'",
      revienta(fixtures, last=[ok([1]), VACIA],
               siguiente=[caida("timeout")]) is not None)
check("caida en la primera pagina no se lee como equipo sin partidos",
      revienta(fixtures, last=[caida("timeout")]) is not None)

# el corte por antiguedad sigue funcionando (no se confunde con un fallo)
ses = FakeSession(last=[ok([1], ts=_TS), ok([2], ts=_TS - 10 ** 7), ok([3])])
n = len(fat._fetch_team_fixtures(ses, 1, since_ts=_TS - 10 ** 6, next_pages=0))
check("se para al rebasar since_ts, sin pedir mas paginas", n == 2 and
      sum("/last/" in u for u in ses.pedidas) == 2, (n, ses.pedidas))


# --------------------------------------------------------------------------- #
print("\n== _crawl: contabilidad de equipos fallidos ==")

def crawl(n_equipos, fallan, **kw):
    """Cosecha simulada de `n_equipos`, de los que fallan los `fallan` primeros."""
    equipos = {i: f"equipo_{i}" for i in range(n_equipos)}
    caidos = set(range(fallan))

    def _fake(session, tid, **_):
        if tid in caidos:
            raise _common.HarvestError(f"[fatigue] equipo {tid}: fallo de transporte")
        return [{"row_id": f"{tid}|x"}]

    original = fat._fetch_team_fixtures
    fat._fetch_team_fixtures = _fake
    try:
        return fat._crawl(None, equipos, set(), **kw)
    finally:
        fat._fetch_team_fixtures = original

check("cosecha sana -> una fila por equipo", len(crawl(20, 0)) == 20)
r = crawl(20, 1)
check("1 fallo de 20 (5%) -> aviso y se continua", len(r) == 19, len(r))
check("5 fallos de 20 (25%) -> sigue dentro del umbral", len(crawl(20, 5)) == 15)

msg = revienta(crawl, 20, 6)
check("6 fallos de 20 (30%) -> aborta", msg is not None)
check("el error resume cuantos y cuales", bool(msg) and "6/20" in msg
      and "equipo_0" in msg, msg)
check("todos los equipos caidos -> aborta", revienta(crawl, 20, 20) is not None)
check("umbral configurable", revienta(crawl, 20, 1, max_failed_frac=0.0) is not None)

# lo cosechado antes del corte no se pierde: se vuelca por el callback
volcado = []
msg = revienta(crawl, 20, 6, flush=lambda rows: volcado.extend(rows), flush_every=5)
check("al abortar se vuelca lo pendiente (nada en el aire)",
      msg is not None and len(volcado) == 14, (msg is not None, len(volcado)))


# --------------------------------------------------------------------------- #
print("\n== compute_features: huso horario de referencia ==")
import pandas as pd


def _epoch(iso_utc: str) -> int:
    return int(pd.Timestamp(iso_utc, tz="UTC").timestamp())


# lunes 23:30 UTC = martes 00:30 en Madrid: el dia de la semana (y por tanto
# `is_midweek`) depende del huso con el que se derive
_LUNES_NOCHE = _epoch("2024-03-11 23:30:00")

check("la fecha se deriva en hora local, no en UTC",
      fat._fecha_local(_LUNES_NOCHE) == "2024-03-12",
      fat._fecha_local(_LUNES_NOCHE))
check("una marca ausente no rompe la derivacion de fecha",
      fat._fecha_local(None) is None)


def _fila(team_id, ts, *, comp="LaLiga", casa=True, rival="Sevilla FC"):
    return {"team_id": team_id, "team": f"equipo_{team_id}", "opponent": rival,
            "start_ts": ts, "competition": comp, "is_home": casa,
            "status": "finished"}


f = fat.compute_features(pd.DataFrame([_fila(1, _LUNES_NOCHE)]))
check("is_midweek usa el dia local (martes), no el de UTC (lunes)",
      bool(f["is_midweek"].iloc[0]), f["is_midweek"].iloc[0])


# --------------------------------------------------------------------------- #
print("\n== compute_features: is_post_intl_break se corrobora con la liga ==")

D = 86400
_INICIO = _epoch("2024-02-25 20:00:00")

# Escenario 1: PARON REAL. Los cuatro equipos juegan, la competicion se detiene
# 14 dias y todos vuelven a la vez.
paron = []
for tid in (1, 2, 3, 4):
    paron += [_fila(tid, _INICIO), _fila(tid, _INICIO + 7 * D),
              _fila(tid, _INICIO + 21 * D)]      # hueco de 14 dias para todos
r1 = fat.compute_features(pd.DataFrame(paron))
tras_paron = r1[r1["start_ts"] == _INICIO + 21 * D]
check("tras un paron de toda la competicion se marca is_post_intl_break",
      bool(tras_paron["is_post_intl_break"].all()),
      tras_paron["is_post_intl_break"].tolist())

# Escenario 2: INCIDENCIA DE UN SOLO EQUIPO. El equipo 1 tiene el mismo hueco de
# 14 dias (aplazamiento), pero el resto de la liga sigue jugando cada semana.
inc = [_fila(1, _INICIO), _fila(1, _INICIO + 7 * D), _fila(1, _INICIO + 21 * D)]
for tid in (2, 3, 4):
    inc += [_fila(tid, _INICIO + k * D) for k in (0, 7, 14, 21)]
r2 = fat.compute_features(pd.DataFrame(inc))
solo1 = r2[(r2["team_id"] == 1) & (r2["start_ts"] == _INICIO + 21 * D)]
check("el hueco de un unico equipo NO se confunde con un paron",
      not bool(solo1["is_post_intl_break"].any()),
      solo1["is_post_intl_break"].tolist())
check("y ese hueco si se refleja en rest_days (la carga es real)",
      float(solo1["rest_days"].iloc[0]) == 14.0, solo1["rest_days"].tolist())

# Escenario 3: el mes sigue filtrando. Un paron de toda la liga en enero (mes sin
# ventana FIFA) no es un paron de selecciones.
_ENERO = _epoch("2024-01-07 20:00:00")
ene = []
for tid in (1, 2, 3, 4):
    ene += [_fila(tid, _ENERO), _fila(tid, _ENERO + 21 * D)]
r3 = fat.compute_features(pd.DataFrame(ene))
check("un paron fuera de mes de ventana FIFA no cuenta",
      not bool(r3["is_post_intl_break"].any()),
      r3["is_post_intl_break"].tolist())


# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_fatigue():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest; cortar el proceso queda reservado al uso como guion,
    porque un `sys.exit` durante la importacion abortaria la recoleccion.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
