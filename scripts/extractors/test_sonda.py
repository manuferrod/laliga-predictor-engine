"""Validacion offline de sonda.py. Sin red.

Las sondas en si no se pueden validar aqui: su trabajo es salir a internet. Lo
que si se puede validar -y es lo que se rompe en silencio- es todo lo que las
rodea:

  1. CONTRATO  cada puerta declara que pasos de la rutina semanal alimenta, y
               esas etiquetas tienen que existir de verdad en `PASOS`. Es la
               comprobacion que importa: si alguien renombra un paso de la
               rutina, el parte de la sonda seguiria imprimiendose tan ufano
               mientras senala a un paso que ya no existe, y el remedio que
               ofrece dejaria de poder seguirse.
  2. ESTADOS   quien bloquea la jornada y quien no. Una sonda que aborta cuando
               no debe convierte una semana degradada en una semana perdida; una
               que no aborta cuando debe deja correr la cadena entera para que
               muera en el paso 2.
  3. CODIGOS   la politica de codigos de salida, con las sondas sustituidas por
               respuestas fijas, incluidos --exigente y --permisivo.
  4. PARTE     el informe que queda en disco es JSON valido y contiene lo que
               hace falta para diagnosticar una semana despues.
"""
import json
import pathlib
import sys
import tempfile

_CANDS = [
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "extractors",
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts" / "extractors",
]
EXTR = next((p for p in _CANDS if (p / "sonda.py").exists()), _CANDS[0])
sys.path.insert(0, str(EXTR))
sys.path.insert(0, str(EXTR.parent))          # para importar rutina_semanal
import sonda  # noqa: E402
import rutina_semanal as rt  # noqa: E402

PASS, FAIL = [], []
def check(n, c, extra=""):
    (PASS if c else FAIL).append(n)
    print(("  OK  " if c else " FAIL ") + n + (f"  -> {extra}" if extra and not c else ""))


D = sonda.Diagnostico


def corre(respuestas, argv=()):
    """Ejecuta `sonda.main` con las sondas sustituidas por respuestas fijas.

    `respuestas` es {nombre: (estado, detalle)}. Las puertas que no aparezcan se
    quedan fuera, de modo que cada caso sondea solo lo que le interesa. El parte
    se desvia a un directorio temporal: la bateria no escribe en `outputs/`.
    """
    puertas = tuple(
        sonda.Puerta(q.nombre, (lambda est, nom=q.nombre: (lambda: D(nom, est)))(
            respuestas[q.nombre]), q.pasos, q.critica)
        for q in sonda.PUERTAS if q.nombre in respuestas)
    original_p, original_o = sonda.PUERTAS, (sonda._OUT, sonda._INFORME,
                                             sonda._HISTORICO)
    with tempfile.TemporaryDirectory() as td:
        raiz = pathlib.Path(td)
        sonda.PUERTAS = puertas
        sonda._OUT, sonda._INFORME = raiz, raiz / "fuentes.json"
        sonda._HISTORICO = raiz / "historico.jsonl"
        try:
            codigo = sonda.main(list(argv))
            informe = json.loads(sonda._INFORME.read_text(encoding="utf-8"))
            lineas = sonda._HISTORICO.read_text(encoding="utf-8").splitlines()
        finally:
            sonda.PUERTAS = original_p
            sonda._OUT, sonda._INFORME, sonda._HISTORICO = original_o
    return codigo, informe, lineas


# --------------------------------------------------------------------------- #
print("== 1) contrato con la rutina semanal ==")
ETIQUETAS = {p.etiqueta for p in rt.PASOS}
declaradas = {e for q in sonda.PUERTAS for e in q.pasos}

check("cada paso declarado por una puerta existe en PASOS",
      declaradas <= ETIQUETAS, sorted(declaradas - ETIQUETAS))

extraccion = {p.etiqueta for p in rt.PASOS
              if p.fase == "extracción" and p.etiqueta != "sonda"}
check("y toda la extraccion esta cubierta por alguna puerta",
      extraccion <= declaradas, sorted(extraccion - declaradas))
check("la sonda no se sondea a si misma", "sonda" not in declaradas)

check("ninguna puerta se llama igual que otra",
      len({q.nombre for q in sonda.PUERTAS}) == len(sonda.PUERTAS))
check("ningun paso lo reclaman dos puertas",
      sum(len(q.pasos) for q in sonda.PUERTAS) == len(declaradas))
check("todas las puertas tienen sonda invocable",
      all(callable(q.sonda) for q in sonda.PUERTAS))
check("la sonda es el primer paso de la rutina", rt.PASOS[0].etiqueta == "sonda")
check("y no admite --dry: --dry no sale a la red",
      not next(p for p in rt.PASOS if p.etiqueta == "sonda").admite_dry)


# --------------------------------------------------------------------------- #
print("\n== 2) quien bloquea la jornada ==")
check("critica + CAIDA bloquea",
      D("x", sonda.CAIDA, critica=True).bloquea)
check("critica + DEGRADADA no bloquea: degradar con aviso es aceptable",
      not D("x", sonda.DEGRADADA, critica=True).bloquea)
check("critica + OK no bloquea", not D("x", sonda.OK, critica=True).bloquea)
check("no critica + CAIDA no bloquea: solo degrada columnas",
      not D("x", sonda.CAIDA, critica=False).bloquea)

criticas = {q.nombre for q in sonda.PUERTAS if q.critica}
check("son criticas el calendario, el resultado y las cuotas",
      criticas == {"sofascore", "sofascore_lineups", "footballdata",
                   "footballdata_fixtures"}, sorted(criticas))
check("ClubElo y Transfermarkt NO son criticas: tienen sustituto o retardo",
      not {"clubelo", "transfermarkt"} & criticas)
check("los dos pasos opcionales de la rutina cuelgan de puertas no criticas",
      not {q.nombre for q in sonda.PUERTAS
           if q.critica and {"alineaciones", "remates"} & set(q.pasos)}
      - {"sofascore_lineups"})


# --------------------------------------------------------------------------- #
print("\n== 3) codigos de salida ==")
codigo, _, _ = corre({"footballdata": sonda.OK, "clubelo": sonda.OK})
check("todo OK -> 0", codigo == 0, codigo)

codigo, inf, _ = corre({"footballdata": sonda.OK, "clubelo": sonda.CAIDA})
check("una fuente NO critica caida -> 0 (la jornada sigue)", codigo == 0, codigo)
check("y el parte no la marca como bloqueante", inf["bloquea"] == [], inf["bloquea"])

codigo, inf, _ = corre({"footballdata": sonda.CAIDA, "clubelo": sonda.OK})
check("una fuente CRITICA caida -> 1", codigo == 1, codigo)
check("y el parte dice cual bloquea", inf["bloquea"] == ["footballdata"],
      inf["bloquea"])

codigo, _, _ = corre({"footballdata": sonda.DEGRADADA})
check("critica DEGRADADA -> 0: hay dato, aunque sea por el respaldo", codigo == 0,
      codigo)
codigo, _, _ = corre({"footballdata": sonda.DEGRADADA}, argv=("--exigente",))
check("la misma con --exigente -> 1", codigo == 1, codigo)
codigo, _, _ = corre({"footballdata": sonda.CAIDA}, argv=("--permisivo",))
check("critica CAIDA con --permisivo -> 0: diagnostico sin frenar nada",
      codigo == 0, codigo)
codigo, _, _ = corre({"footballdata": sonda.OK}, argv=("--exigente",))
check("--exigente con todo OK no inventa un fallo", codigo == 0, codigo)


# --------------------------------------------------------------------------- #
print("\n== 4) el parte que queda en disco ==")
codigo, inf, lineas = corre({"footballdata": sonda.OK, "clubelo": sonda.CAIDA,
                             "transfermarkt": sonda.DEGRADADA})
check("el resumen cuenta los tres estados",
      inf["resumen"] == {"OK": 1, "DEGRADADA": 1, "CAIDA": 1}, inf["resumen"])
check("hay una entrada por fuente sondeada", len(inf["fuentes"]) == 3)
check("cada entrada lleva fuente, estado y pasos",
      all({"fuente", "estado", "pasos", "critica"} <= set(f)
          for f in inf["fuentes"]))
check("el momento es fechable", inf["momento"][:4].isdigit(), inf["momento"])
check("el historico gana una linea por ejecucion", len(lineas) == 1, len(lineas))
check("y esa linea resume el estado de cada fuente",
      json.loads(lineas[0])["estados"]["clubelo"] == sonda.CAIDA)

try:
    sonda.main(["--solo", "no_existe"])
    check("una fuente inventada en --solo se rechaza", False, "no lanzo")
except SystemExit as e:
    check("una fuente inventada en --solo se rechaza", "no conozco" in str(e), str(e))


# --------------------------------------------------------------------------- #
print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
if FAIL:
    print("FALLOS:", FAIL)
else:
    print("Todo correcto.")


def test_sonda():
    """Punto de entrada para `pytest`.

    Las comprobaciones se ejecutan en el cuerpo del modulo, que asi se lee de
    corrido como un guion. Esta funcion se limita a traducir su resultado al
    protocolo de pytest.
    """
    assert not FAIL, FAIL


if __name__ == "__main__":
    sys.exit(1 if FAIL else 0)
