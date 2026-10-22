"""Validacion offline de teamnames.py (alias + claves canonicas). Sin red.

teamnames es la UNICA fuente de verdad de las claves de equipo: si una fuente
emite un nombre que no esta mapeado, `to_key` degrada a un slug de reserva y ese
equipo deja de cruzar con las demas fuentes EN SILENCIO. Este test fija los
nombres largos/oficiales que aparecen en Sofascore, Transfermarkt y FBref.
"""
import sys, pathlib

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "teamnames.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
import teamnames as tn

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))

# --------------------------------------------------------------------------- #
print("== nombres oficiales largos (los que degradaban en silencio) ==")
LARGOS = {
    "Real Club Deportivo de La Coruña": "deportivo_la_coruna",
    "Real Sporting de Gijón": "sporting_gijon",
    "Real Racing Club de Santander": "racing_santander",
    "Club Deportivo Leganés": "leganes",
    "Athletic Club de Bilbao": "athletic_bilbao",
    "Unión Deportiva Las Palmas": "las_palmas",
    "Reial Club Deportiu Espanyol de Barcelona": "espanyol",
}
for nombre, clave in LARGOS.items():
    got = tn.to_key(nombre)
    check(f"{nombre[:34]:34s} -> {clave}", got == clave, got)

print("\n== regresion: las formas cortas de siempre siguen igual ==")
CORTOS = {
    "Real Madrid": "real_madrid",
    "Barcelona": "barcelona",
    "Atlético Madrid": "atletico_madrid",
    "Ath Bilbao": "athletic_bilbao",
    "Betis": "real_betis",
    "deportivo-la-coruna": "deportivo_la_coruna",
    "Sevilla FC": "sevilla",
}
for nombre, clave in CORTOS.items():
    got = tn.to_key(nombre)
    check(f"{nombre:22s} -> {clave}", got == clave, got)

# --------------------------------------------------------------------------- #
print("\n== consistencia interna del mapa ==")
huerfanos = sorted(set(tn.ALIASES.values()) - set(tn.CANONICAL))
check("todo alias apunta a una clave de CANONICAL", not huerfanos, huerfanos)

# un alias cuya forma limpia no es el mismo texto es INALCANZABLE (clean() se
# aplica antes de buscar). Se tolera solo si su forma limpia resuelve a la misma
# clave (es decir, no cambia el comportamiento).
rotos = []
for k, v in tn.ALIASES.items():
    c = tn.clean(k)
    if c != k and tn.ALIASES.get(c) != v:
        rotos.append((k, c, v, tn.ALIASES.get(c)))
check("sin alias inalcanzables que cambien la clave", not rotos, rotos)

# --------------------------------------------------------------------------- #
print("\n== assert_known (corta el pipeline en vez de publicar huecos) ==")
try:
    tn.assert_known(["Real Madrid", "Sevilla"])
    check("no lanza con equipos conocidos", True)
except ValueError as e:
    check("no lanza con equipos conocidos", False, e)

try:
    tn.assert_known(["Real Madrid", "Equipo Inventado CF"], source="prueba")
    check("lanza ValueError con un equipo desconocido", False, "no lanzo")
except ValueError as e:
    msg = str(e)
    check("lanza ValueError con un equipo desconocido", True)
    check("el mensaje nombra el equipo culpable", "Equipo Inventado CF" in msg, msg[:80])
    check("el mensaje dice donde arreglarlo", "ALIASES" in msg, msg[:80])

# --------------------------------------------------------------------------- #
print("\n== match_key ==")
mk = tn.match_key(["2025-08-17"], ["Real Madrid"], ["Sevilla FC"])[0]
check("match_key = fecha|local|visitante", mk == "2025-08-17|real_madrid|sevilla", mk)
mk_bad = tn.match_key(["fecha-invalida"], ["Real Madrid"], ["Sevilla FC"])[0]
check("fecha invalida -> match_key nulo (lo detecta assert_non_null)",
      mk_bad != mk_bad or mk_bad is None, mk_bad)  # NaN != NaN

print("\n" + "=" * 40)
print(f"TOTAL: {len(PASS)} OK, {len(FAIL)} FAIL")
if FAIL:
    print("fallos:", FAIL)


def test_teamnames():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest; cortar el proceso queda reservado al uso como guion,
    porque un `sys.exit` durante la importacion abortaria la recoleccion.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
