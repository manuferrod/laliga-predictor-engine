"""Validacion offline del transporte compartido de `_common.py`. Sin red.

Lo que se valida es la POLITICA de reintento, que es la que decide si un fallo
de una semana tumba la jornada o se absorbe solo:

  * lo transitorio -errores de red, 429 y 5xx- se reintenta con espera
    creciente, porque una fuente caida suele volver en segundos;
  * los demas 4xx vuelven en el PRIMER intento y sin espera: un 404 es "no
    existe" y reintentarlo solo alarga el recorrido del historico, y un 403 es
    la senal con la que `understat.py` escala del endpoint JSON al HTML y
    `sofascore.py` salta al espejo, asi que llegar tarde la inutiliza;
  * el ultimo status se DEVUELVE, nunca se lanza, para que el llamante pueda
    escalar a su via alternativa. Solo se relanza si NINGUN intento obtuvo
    respuesta HTTP.

La red se sustituye por un `_raw_get_bytes` con respuestas guionizadas y
`time.sleep` por un contador, de modo que la bateria mide la politica sin
esperar ni salir a internet.
"""
import sys, pathlib

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "_common.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import _common

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


def pedir(guion, *, retries=3, wait=2.0, texto=False):
    """Ejecuta `http_get_bytes` contra un `guion` de respuestas.

    Cada elemento del guion es un status (se devuelve con cuerpo b"x") o una
    excepcion (se lanza, simulando un error de red). Devuelve
    (resultado, n_peticiones, esperas) donde `resultado` es la tupla devuelta o
    la excepcion relanzada.
    """
    pedidas, esperas = [], []

    def falso_raw(url, *, headers, timeout):
        i = len(pedidas)
        pedidas.append(url)
        item = guion[i] if i < len(guion) else guion[-1]
        if isinstance(item, Exception):
            raise item
        return int(item), b"x"

    raw0, sleep0 = _common._raw_get_bytes, _common.time.sleep
    _common._raw_get_bytes = falso_raw
    _common.time.sleep = esperas.append
    try:
        fn = _common.http_get if texto else _common.http_get_bytes
        res = fn("http://fuente/x", headers={}, retries=retries, wait=wait)
    except Exception as e:  # noqa: BLE001  el relanzado tambien es un resultado
        res = e
    finally:
        _common._raw_get_bytes, _common.time.sleep = raw0, sleep0
    return res, len(pedidas), esperas


RED = ConnectionError("conexion rechazada")


# --------------------------------------------------------------------------- #
print("== camino feliz y codigos que NO se reintentan ==")
res, n, esperas = pedir([200])
check("200 -> devuelve (200, cuerpo) en un intento", res == (200, b"x") and n == 1,
      f"{res} en {n} intentos")
check("200 no espera", esperas == [], esperas)

for status in (400, 403, 404, 410, 451):
    res, n, esperas = pedir([status])
    check(f"{status} vuelve en el primer intento y sin espera",
          res == (status, b"x") and n == 1 and esperas == [],
          f"{res} en {n} intentos, esperas={esperas}")

res, n, _ = pedir([403, 200])
check("403 NO se reintenta aunque el siguiente intento fuera bueno",
      res == (403, b"x") and n == 1, f"{res} en {n} intentos")


# --------------------------------------------------------------------------- #
print("\n== 5xx: se reintenta con espera creciente ==")
for status in (500, 502, 503, 504):
    res, n, _ = pedir([status, 200])
    check(f"{status} transitorio -> reintenta y entrega el 200",
          res == (200, b"x") and n == 2, f"{res} en {n} intentos")

res, n, esperas = pedir([502], retries=3)
check("502 persistente -> DEVUELVE 502 (no lanza) tras agotar los intentos",
      res == (502, b"x"), res)
check("502 persistente -> un intento por cada 'retries'", n == 3, n)
check("502 persistente -> espera entre intentos, no despues del ultimo",
      len(esperas) == 2, esperas)
check("las esperas de 5xx son crecientes",
      len(esperas) == 2 and esperas[1] > esperas[0], esperas)
check("la espera de 5xx escala sobre 'wait' (2.0 -> 4s, 8s)",
      esperas == [4.0, 8.0], esperas)

res, n, esperas = pedir([500], retries=1)
check("retries=1 -> un solo intento y ninguna espera",
      res == (500, b"x") and n == 1 and esperas == [], f"{res}/{n}/{esperas}")

res, n, _ = pedir([502, 503, 200], retries=5)
check("varios 5xx seguidos se encadenan hasta la respuesta buena",
      res == (200, b"x") and n == 3, f"{res} en {n} intentos")


# --------------------------------------------------------------------------- #
print("\n== 429 y errores de red ==")
res, n, esperas = pedir([429, 200])
check("429 espera y reintenta", res == (200, b"x") and n == 2, f"{res} en {n}")
check("la espera de 429 es la larga del rate-limit (>= 15s)",
      esperas and esperas[0] >= 15, esperas)

res, n, _ = pedir([429], retries=3)
check("429 persistente -> devuelve 429", res == (429, b"x") and n == 3, f"{res}/{n}")

res, n, esperas = pedir([RED, 200])
check("error de red -> reintenta y entrega el 200",
      res == (200, b"x") and n == 2, f"{res} en {n}")
check("el error de red espera antes de reintentar", esperas == [2.0], esperas)

res, n, _ = pedir([RED], retries=3)
check("red caida en todos los intentos -> RuntimeError",
      isinstance(res, RuntimeError), type(res).__name__)
check("el RuntimeError de red menciona la url",
      isinstance(res, RuntimeError) and "http://fuente/x" in str(res), str(res))
check("red caida -> se agotan los intentos antes de rendirse", n == 3, n)

res, n, _ = pedir([RED, 502, 200], retries=5)
check("red y 5xx se mezclan en la misma cascada de reintentos",
      res == (200, b"x") and n == 3, f"{res} en {n} intentos")


# --------------------------------------------------------------------------- #
print("\n== http_get: la variante de texto hereda la politica ==")
res, n, _ = pedir([502, 200], texto=True)
check("http_get decodifica a texto y reintenta igual",
      res == (200, "x") and n == 2, f"{res} en {n} intentos")
res, n, _ = pedir([404], texto=True)
check("http_get tampoco reintenta el 404", res == (404, "x") and n == 1, f"{res}/{n}")


# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test__common():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
