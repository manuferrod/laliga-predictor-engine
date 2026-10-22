"""Validacion de la RUTINA SEMANAL (rutina_semanal.py). Sin red y sin ejecutar nada.

Lo que este banco vigila no es que la cadena funcione —cada eslabon tiene su propio
banco— sino que el ORDEN escrito en `PASOS` siga siendo el correcto, porque es lo
unico que este modulo aporta y lo unico que puede romperse aqui.

Las tres averias posibles son silenciosas:

  (a) que un paso desaparezca de la tupla, o que su script se renombre. La cadena
      terminaria sin error y publicaria la jornada anterior, porque el paso que
      faltaba es justamente el que traia lo nuevo.
  (b) que el orden se altere. Fatiga, bajas y alineaciones leen el calendario que
      publica Sofascore: pedirlas antes que el no falla, devuelve la semana pasada.
  (c) que `--dry` llegue a un script que no lo entiende. Un extractor tratando
      `--dry` como orden, o un control que lo recibe como argumento suelto, o bien
      revienta con un mensaje confuso o —peor— escribe cuando se le habia pedido
      que no escribiera.

Se comprueba, en este orden:

  1. CADENA    los pasos son los esperados, sin repetidos, cada uno con su script
               en disco, y la fase termina midiendo, auditando y publicando.
  2. FILTROS   --desde, --hasta, --sin-extraccion y --sin-opcionales recortan lo
               que dicen recortar, y un paso inventado o un rango del reves son
               errores explicitos en vez de una cadena vacia que no hace nada.
  3. ORDENES   la linea de ordenes que se construye para cada paso es exacta, y
               --dry solo viaja a los scripts que lo admiten.
  4. SONDEO    el conteo estatico del conjunto acierta en los cuatro casos, y
               cuando no puede afirmar nada lo dice en lugar de inventar un cero
               que detendria la rutina por las buenas.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import rutina_semanal as rt  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")


def _todos() -> list[rt.Paso]:
    return rt.selecciona(desde=None, hasta=None, sin_extraccion=False,
                         sin_opcionales=False)


# ---------------------------------------------------------------------------
def caso_cadena() -> None:
    print("\n[1] la cadena")
    pasos = _todos()
    etiquetas = [p.etiqueta for p in pasos]

    check(len(etiquetas) == len(set(etiquetas)),
          "ninguna etiqueta se repite: `--desde` senala un paso y solo uno")
    check(etiquetas[0] == "sonda",
          "empieza sondeando las fuentes: el diagnostico va antes de pedir nada")
    check(etiquetas[1] == "sofascore",
          "y sigue por el calendario, del que cuelgan fatiga, bajas y alineaciones")
    check(etiquetas.index("fatiga") > etiquetas.index("sofascore")
          and etiquetas.index("bajas") > etiquetas.index("sofascore")
          and etiquetas.index("alineaciones") > etiquetas.index("sofascore"),
          "y las tres derivadas del calendario van despues de el")
    check(etiquetas[-4:] == ["modelos", "qa_modelos", "web", "app"],
          "y termina midiendo, auditando, publicando y trasladando, en ese orden")
    check(etiquetas.index("qa_master") == etiquetas.index("master") + 1
          and etiquetas.index("qa_features") == etiquetas.index("features") + 1,
          "cada control va inmediatamente detras de lo que audita")
    check(etiquetas.index("predict") < etiquetas.index("modelos"),
          "la matriz de la jornada se construye antes de puntuarla")
    check(etiquetas.index("cuotas") < etiquetas.index("master"),
          "el precio de la jornada por jugar entra antes de consolidar")

    faltan = [p.script for p in pasos if not p.ruta.exists()]
    check(not faltan, f"todos los scripts de la cadena existen en disco: {faltan}")

    fases = [p.fase for p in pasos]
    check(fases == sorted(fases, key=["extracción", "reconstrucción",
                                      "publicación"].index),
          "las fases no se entrelazan: extraer, reconstruir, publicar")
    check(all(p.fase == "extracción" for p in pasos if p.opcional),
          "los pasos opcionales son de extraccion: lo caro es pedir, no calcular")
    check("seleccion.py" not in {p.script.split("/")[-1] for p in pasos},
          "la busqueda de variables NO esta en la rutina semanal")


def caso_filtros() -> None:
    print("\n[2] los filtros")
    total = len(rt.PASOS)

    desde = rt.selecciona(desde="master", hasta=None, sin_extraccion=False,
                          sin_opcionales=False)
    check(desde[0].etiqueta == "master" and desde[-1].etiqueta == "app",
          "--desde retoma en el paso pedido y llega hasta el final")

    hasta = rt.selecciona(desde=None, hasta="predict", sin_extraccion=False,
                          sin_opcionales=False)
    check(hasta[-1].etiqueta == "predict" and hasta[0].etiqueta == "sonda",
          "--hasta incluye el paso pedido y se detiene ahi")

    ambos = rt.selecciona(desde="features", hasta="preprocess",
                          sin_extraccion=False, sin_opcionales=False)
    check([p.etiqueta for p in ambos] == ["features", "qa_features", "preprocess"],
          "los dos juntos recortan un tramo cerrado por los dos lados")

    sin_red = rt.selecciona(desde=None, hasta=None, sin_extraccion=True,
                            sin_opcionales=False)
    check(all("extractors/" not in p.script for p in sin_red),
          "--sin-extraccion no deja ningun paso que pida nada a la red")

    sin_op = rt.selecciona(desde=None, hasta=None, sin_extraccion=False,
                           sin_opcionales=True)
    quitados = {p.etiqueta for p in rt.PASOS} - {p.etiqueta for p in sin_op}
    check(quitados == {"alineaciones", "remates"},
          f"--sin-opcionales quita los dos bloques caros y solo esos: {quitados}")
    check(len(sin_op) == total - 2, "y no se lleva por delante ningun otro")

    for kw, msg in (
            (dict(desde="noexiste", hasta=None), "un paso inventado"),
            (dict(desde=None, hasta="tampoco"), "un destino inventado"),
            (dict(desde="web", hasta="master"), "un rango del reves")):
        try:
            rt.selecciona(sin_extraccion=False, sin_opcionales=False, **kw)
            check(False, f"{msg} se rechaza en vez de dejar la cadena vacia")
        except SystemExit:
            check(True, f"{msg} se rechaza en vez de dejar la cadena vacia")


def caso_ordenes() -> None:
    print("\n[3] las ordenes")
    por_etiqueta = {p.etiqueta: p for p in rt.PASOS}

    web = por_etiqueta["web"]
    orden = web.orden(dry=False)
    check(orden[0] == sys.executable,
          "cada paso se lanza con el mismo interprete que ejecuta la rutina")
    check(orden[1].endswith("publish_web.py") and len(orden) == 2,
          "sin argumentos de mas")
    check(web.orden(dry=True)[-1] == "--dry", "y con --dry cuando se pide")

    check("--dry" not in por_etiqueta["qa_modelos"].orden(dry=True),
          "a los controles, que no escriben ni admiten --dry, no se les pasa")
    check(all("--dry" not in p.orden(dry=True) for p in rt.PASOS
              if "extractors/" in p.script),
          "y a los extractores tampoco: no lo entienden y lo tomarian por orden")

    check(por_etiqueta["cuotas"].orden(dry=False)[-1] == "upcoming",
          "el precio de la jornada por jugar se pide con `upcoming`, no con `weekly`")
    check(all(p.argumentos == ("weekly",) for p in rt.PASOS
              if "extractors/" in p.script
              and p.etiqueta not in ("cuotas", "sonda")),
          "el resto de la extraccion va en modo semanal")
    check(rt.PASOS[0].orden(dry=False)[1:] == [str(rt.SCRIPTS / "extractors"
                                                   / "sonda.py")],
          "la sonda se lanza sin argumentos: diagnostica las ocho puertas")


def caso_sondeo() -> None:
    print("\n[4] el sondeo del conjunto")
    scripts, seleccion = rt.SCRIPTS, rt.SELECCION
    try:
        with tempfile.TemporaryDirectory() as tmp:
            raiz = Path(tmp)
            (raiz / "models").mkdir()
            fuente = raiz / "models" / "build_models.py"
            rt.SCRIPTS = raiz

            art = raiz / "conjunto.json"
            art.write_text(json.dumps(
                {"conjunto": ["a", "b", "c"], "familia": "logistica",
                 "corte_seleccion": "2022/23"}), encoding="utf-8")
            rt.SELECCION = art

            fuente.write_text("CONJUNTO: tuple[str, ...] = ()\n", encoding="utf-8")
            n, de_donde = rt.conjunto_de_partida()
            check(n == 3 and "logistica" in de_donde and "2022/23" in de_donde,
                  f"con CONJUNTO vacio se cuenta el de la busqueda: {n}, {de_donde!r}")

            fuente.write_text('CONJUNTO: tuple = ("x", "y")\n', encoding="utf-8")
            n, de_donde = rt.conjunto_de_partida()
            check(n == 2 and "build_models" in de_donde,
                  f"y lo escrito a mano manda sobre el artefacto: {n}, {de_donde!r}")

            fuente.write_text("CONJUNTO = ()\n", encoding="utf-8")
            rt.SELECCION = raiz / "no_existe.json"
            n, de_donde = rt.conjunto_de_partida()
            check(n == 0, "sin ninguna de las dos cosas, cero, que es lo que avisa")

            fuente.write_text("CONJUNTO = (\n", encoding="utf-8")
            n, _ = rt.conjunto_de_partida()
            check(n == -1, "un fuente que no compila no se traduce en un cero falso")

            fuente.write_text("CONJUNTO = tuple(DE_OTRO_SITIO)\n", encoding="utf-8")
            n, _ = rt.conjunto_de_partida()
            check(n == -1, "ni un CONJUNTO que no es un literal: aqui no se importa")
    finally:
        rt.SCRIPTS, rt.SELECCION = scripts, seleccion


# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 74)
    print("BANCO DE PRUEBAS — rutina_semanal.py")
    print("=" * 74)
    caso_cadena()
    caso_filtros()
    caso_ordenes()
    caso_sondeo()
    print(f"\n===== {len(PASS)} OK / {len(FAIL)} FAIL =====")
    if FAIL:
        print("FALLOS:")
        for f in FAIL:
            print("  -", f)
    else:
        print("Todo correcto.")
    return 1 if FAIL else 0


def test_rutina_semanal() -> None:
    """Punto de entrada para `pytest`."""
    assert main() == 0, FAIL


if __name__ == "__main__":
    raise SystemExit(main())
