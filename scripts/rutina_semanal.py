"""La actualización de la jornada, en una sola orden.

Motivación
----------
La rutina semanal son veintiuna órdenes —dos de ellas opcionales— cuyo orden no es
negociable. Fatiga, disponibilidad y alineaciones se apoyan en el calendario que
publica Sofascore, de modo que salen mal si se piden antes que él; el maestro no
debe regenerarse con fuentes a medio actualizar; la proyección a la web no puede
ejecutarse sobre una evidencia que todavía no ha pasado la auditoría, porque no
vuelve a decidir nada y publicaría lo que le den; y el traslado al repositorio de
la aplicación va detrás de la proyección porque copia lo que esta declara.

La forma natural de equivocarse un domingo con prisa no es escribir mal una orden
—eso falla y se ve—, sino saltarse una y no enterarse. Cada paso lee lo que dejó el
anterior, y un artefacto de la semana pasada se deja leer exactamente igual de bien
que uno de hoy: la cadena termina, la web se publica y las probabilidades son las
de una jornada que ya se jugó. Este módulo existe para que ese error no dependa de
la memoria de quien ejecuta.

Qué hace y qué no
-----------------
No calcula nada ni decide nada. Ejecuta los mismos scripts, en el mismo orden, con
el mismo intérprete que lo invoca, y se detiene en el primero que falle. Toda la
lógica sigue viviendo donde vivía; aquí solo está escrito el orden.

Tampoco busca variables: `seleccion.py` no forma parte de la cadena. La búsqueda se
ejecuta cuando se elige el conjunto —una vez por trabajo, sobre el bloque de
selección— y lo que llega hasta aquí es su resultado, recogido de disco por
`build_models.py`. Encadenarla aquí convertiría en semanal una decisión que debe
tomarse una sola vez, y cada ejecución elegiría un conjunto ligeramente distinto
sobre los mismos datos, que es la forma más limpia de que la cifra publicada deje
de significar nada.

Tampoco abre el holdout: `build_models.py --holdout` es una pasada única y aparte.

Uso
---
    python scripts/rutina_semanal.py                  # la cadena completa
    python scripts/rutina_semanal.py --listar         # enseña el plan, no ejecuta
    python scripts/rutina_semanal.py --desde master   # retoma tras un fallo
    python scripts/rutina_semanal.py --hasta predict  # se detiene antes de modelar
    python scripts/rutina_semanal.py --sin-extraccion # recalcula sobre lo que hay
    python scripts/rutina_semanal.py --sin-opcionales # sin alineaciones ni remates
    python scripts/rutina_semanal.py --dry            # audita sin escribir nada
"""
from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
RAIZ = SCRIPTS.parent
SELECCION = RAIZ / "outputs" / "models" / "seleccion" / "conjunto.json"


@dataclass(frozen=True)
class Paso:
    """Un eslabón de la cadena: qué se ejecuta, en qué fase y por qué ahí."""

    etiqueta: str
    script: str
    argumentos: tuple[str, ...] = ()
    fase: str = ""
    admite_dry: bool = False
    opcional: bool = False
    porque: str = ""

    @property
    def ruta(self) -> Path:
        return SCRIPTS / self.script

    def orden(self, dry: bool) -> list[str]:
        extra = ["--dry"] if (dry and self.admite_dry) else []
        return [sys.executable, str(self.ruta), *self.argumentos, *extra]


# El orden de esta tupla ES la rutina. Las fases están separadas porque los fallos
# de cada una se arreglan de manera distinta: la extracción falla por la red o por
# un cambio en una web ajena, y la reconstrucción falla por el código de este
# repositorio.
#
# El sondeo va delante de todo y no pide permiso a nadie: es la única pieza que
# mira las fuentes en conjunto. Solo detiene la jornada si cae una de las que no
# tienen sustituto; que ClubElo o Transfermarkt fallen degrada columnas, se avisa
# y se sigue.
PASOS: tuple[Paso, ...] = (
    # --- 0. sondeo: el estado de las nueve fuentes, antes de pedirles nada ------
    Paso("sonda", "extractors/sonda.py", (), "extracción",
         porque="informa del estado de las OCHO puertas de red de una vez; la "
                "cadena se detiene en el primer fallo y sin esto no se sabría "
                "si lo demás también está caído"),

    # --- 1. extracción: primero el calendario, del que dependen los demás -------
    Paso("sofascore", "extractors/sofascore.py", ("weekly",), "extracción",
         porque="publica el calendario del que cuelgan fatiga, bajas y alineaciones"),
    Paso("footballdata", "extractors/footballdata.py", ("weekly",), "extracción",
         porque="el resultado de lo ya jugado y las cuotas de las casas"),
    Paso("understat", "extractors/understat.py", ("weekly",), "extracción"),
    Paso("clubelo", "extractors/clubelo.py", ("weekly",), "extracción"),
    Paso("transfermarkt", "extractors/transfermarkt.py", ("weekly",), "extracción"),
    Paso("fatiga", "extractors/fatigue.py", ("weekly",), "extracción",
         porque="descanso y congestión, que se leen del calendario ya actualizado"),
    Paso("bajas", "extractors/availability.py", ("weekly",), "extracción"),
    Paso("alineaciones", "extractors/lineups.py", ("weekly",), "extracción",
         opcional=True,
         porque="pide la alineación de cada partido: es el bloque caro de la cadena"),
    Paso("remates", "extractors/understat_shots.py", ("weekly",), "extracción",
         opcional=True, porque="remate a remate; opcional por el mismo motivo"),
    Paso("cuotas", "extractors/footballdata.py", ("upcoming",), "extracción",
         porque="el precio de la jornada POR JUGAR, que `weekly` no trae"),

    # --- 2. reconstrucción: cada paso audita lo que el anterior escribió --------
    Paso("master", "consolidate/build_master.py", (), "reconstrucción",
         admite_dry=True),
    Paso("qa_master", "consolidate/qa_master.py", (), "reconstrucción"),
    Paso("features", "features/build_features.py", (), "reconstrucción",
         admite_dry=True),
    Paso("qa_features", "features/qa_features.py", (), "reconstrucción"),
    Paso("preprocess", "features/build_preprocess.py", (), "reconstrucción",
         admite_dry=True,
         porque="reajusta el estado congelado que la inferencia carga sin tocar"),
    Paso("predict", "features/build_predict.py", (), "reconstrucción",
         admite_dry=True,
         porque="la matriz de la jornada por jugar, ya preprocesada. Es el paso que "
                "puede pedir teclado: si football-data no ha publicado el precio de "
                "la jornada, para con MissingOddsError y deja la plantilla en "
                "data/01_raw/footballdata/footballdata_upcoming_manual_odds.csv. Se "
                "rellena odds_h/odds_d/odds_a y se reanuda con --desde predict; lo "
                "tecleado se guarda con su sello y prevalece en los reprocesos"),

    # --- 3. modelado y publicación: medir, auditar, y solo entonces emitir ------
    Paso("modelos", "models/build_models.py", (), "publicación", admite_dry=True,
         porque="reevalúa el conjunto ya cerrado con la jornada nueva"),
    Paso("qa_modelos", "models/qa_models.py", (), "publicación",
         porque="la puerta entre medir y publicar: si no pasa, no se emite"),
    Paso("web", "models/publish_web.py", (), "publicación", admite_dry=True),
    Paso("app", "models/sincroniza_app.py", (), "publicación", admite_dry=True,
         porque="el motor y la web son dos repositorios y no comparten disco en "
                "producción: sin este traslado la aplicación sigue sirviendo la "
                "publicación de la semana pasada, que se lee igual de bien que "
                "la de hoy y no avisa de nada"),
)

_ETIQUETAS = tuple(p.etiqueta for p in PASOS)


# ===========================================================================
# Antes de empezar
# ===========================================================================
def conjunto_de_partida() -> tuple[int, str]:
    """Cuántas variables van a llegar al modelo, y de dónde salen.

    Se mira ANTES de la cadena y sin importar nada: `build_models.py` se lee como
    texto y se resuelve su literal. El motivo es de coste. Si no hay conjunto, el
    único paso que lo nota es el último, y enterarse de que no hay modelo que
    emitir después de haber recorrido nueve extractores es una hora perdida por
    algo que se sabía al principio.

    Es un análisis estático y por tanto conservador: devuelve `(-1, ...)` cuando no
    puede afirmar nada, y en ese caso la cadena sigue. Un aviso que se equivoca y
    detiene la rutina hace más daño que el fallo que previene.
    """
    fuente = SCRIPTS / "models" / "build_models.py"
    try:
        arbol = ast.parse(fuente.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return -1, "no he podido leer build_models.py"

    for nodo in arbol.body:
        objetivo = (nodo.target if isinstance(nodo, ast.AnnAssign)
                    else nodo.targets[0] if isinstance(nodo, ast.Assign) else None)
        if not (isinstance(objetivo, ast.Name) and objetivo.id == "CONJUNTO"):
            continue
        try:
            valor = ast.literal_eval(nodo.value)
        except (ValueError, TypeError):
            return -1, "CONJUNTO no es un literal: lo dirá build_models.py"
        if valor:
            return len(valor), "escrito en build_models.py"
        break

    # Vacío: entonces el conjunto sale de la búsqueda, y build_models comprueba al
    # recogerlo que el corte y la familia son los que rigen. Aquí solo se cuenta.
    try:
        guardado = json.loads(SELECCION.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0, "no hay conjunto: ni escrito ni guardado por la búsqueda"
    cols = guardado.get("conjunto") or ()
    return len(cols), (f"de la búsqueda ({SELECCION.name}, familia "
                       f"{guardado.get('familia', '?')}, corte "
                       f"{guardado.get('corte_seleccion', '?')})")


# ===========================================================================
# La cadena
# ===========================================================================
def selecciona(*, desde: str | None, hasta: str | None,
               sin_extraccion: bool, sin_opcionales: bool) -> list[Paso]:
    """Los pasos que se van a ejecutar, en orden, según los filtros dados."""
    for etiqueta in (desde, hasta):
        if etiqueta is not None and etiqueta not in _ETIQUETAS:
            raise SystemExit(f"«{etiqueta}» no es un paso. Los pasos son: "
                             + ", ".join(_ETIQUETAS))
    i = _ETIQUETAS.index(desde) if desde else 0
    j = _ETIQUETAS.index(hasta) + 1 if hasta else len(PASOS)
    if i >= j:
        raise SystemExit(f"«{desde}» va después de «{hasta}»: no hay nada que hacer")

    elegidos = list(PASOS[i:j])
    if sin_extraccion:
        elegidos = [p for p in elegidos if p.fase != "extracción"]
    if sin_opcionales:
        elegidos = [p for p in elegidos if not p.opcional]
    if not elegidos:
        raise SystemExit("los filtros no dejan ningún paso que ejecutar")
    return elegidos


def enseña(pasos: list[Paso], *, dry: bool) -> None:
    """El plan, tal cual se va a ejecutar."""
    fase = ""
    for p in pasos:
        if p.fase != fase:
            fase = p.fase
            print(f"\n--- {fase} " + "-" * (66 - len(fase)))
        marca = "  (opcional)" if p.opcional else ""
        orden = " ".join((p.script, *p.argumentos,
                          *(["--dry"] if dry and p.admite_dry else [])))
        print(f"  {p.etiqueta:<14} {orden}{marca}")
        if p.porque:
            print(f"  {'':<14} · {p.porque}")


def ejecuta(paso: Paso, *, dry: bool) -> tuple[int, float]:
    """Lanza un paso y devuelve su código de salida y lo que ha tardado.

    La salida del hijo no se captura: va directa a la consola. Un resumen propio
    sería más bonito y escondería justo lo que hay que leer cuando algo va mal.
    """
    orden = paso.orden(dry)
    print("\n" + "=" * 74)
    print(f"[{paso.etiqueta}] {' '.join(orden[1:])}")
    print("=" * 74, flush=True)
    t0 = time.perf_counter()
    codigo = subprocess.call(orden, cwd=str(RAIZ))
    return codigo, time.perf_counter() - t0


def corre(pasos: list[Paso], *, dry: bool) -> int:
    """Recorre la cadena y se detiene en el primer fallo.

    Detenerse no es una comodidad: seguir tras un paso fallido dejaría en disco una
    mezcla de artefactos de dos semanas distintas, y ninguno de los siguientes
    tiene forma de notarlo, porque un parquet viejo se lee igual de bien que uno
    nuevo.
    """
    tiempos: list[tuple[str, float, bool]] = []
    for i, paso in enumerate(pasos, 1):
        print(f"\n>>> paso {i} de {len(pasos)}", flush=True)
        codigo, segundos = ejecuta(paso, dry=dry)
        tiempos.append((paso.etiqueta, segundos, codigo == 0))
        if codigo != 0:
            resumen(tiempos, dry=dry)
            print(f"\nFALLO en «{paso.etiqueta}» (código {codigo}). La cadena se "
                  f"detiene aquí: los pasos siguientes leerían artefactos a medias.")
            print(f"Cuando esté arreglado, se retoma sin repetir lo anterior:\n"
                  f"    python scripts/rutina_semanal.py --desde {paso.etiqueta}")
            if paso.opcional:
                print(f"Y si el problema no tiene arreglo hoy, «{paso.etiqueta}» es "
                      f"opcional: la cadena se completa sin él con --sin-opcionales, "
                      f"y sus columnas llegan a nulo y se podan.")
            return codigo
    resumen(tiempos, dry=dry)
    print("\nCadena completa." + (" Nada se ha escrito: era --dry." if dry else
                                  " La jornada está publicada bajo outputs/web/."))
    return 0


def resumen(tiempos: list[tuple[str, float, bool]], *, dry: bool) -> None:
    total = sum(s for _, s, _ in tiempos)
    print("\n" + "-" * 74)
    print(f"{'paso':<16}{'estado':<10}{'tiempo':>10}")
    for etiqueta, segundos, bien in tiempos:
        print(f"{etiqueta:<16}{'OK' if bien else 'FALLO':<10}"
              f"{segundos / 60:>9.1f}m")
    print(f"{'TOTAL':<16}{'':<10}{total / 60:>9.1f}m"
          + ("   (--dry: sin escribir)" if dry else ""))
    print("-" * 74)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="La rutina semanal completa, en el orden en que debe ir.")
    p.add_argument("--desde", metavar="PASO", help="retoma la cadena desde ese paso")
    p.add_argument("--hasta", metavar="PASO", help="se detiene después de ese paso")
    p.add_argument("--sin-extraccion", action="store_true",
                   help="no pide nada a la red: recalcula sobre lo que hay en disco")
    p.add_argument("--sin-opcionales", action="store_true",
                   help="omite alineaciones y remates, los dos bloques caros")
    p.add_argument("--listar", action="store_true",
                   help="enseña el plan y termina, sin ejecutar nada")
    p.add_argument("--dry", action="store_true",
                   help="ejecuta y audita sin escribir; implica --sin-extraccion")
    args = p.parse_args(argv)

    # `--dry` sin esto sería una promesa falsa: los extractores no tienen esa
    # opción y escribirían en el almacén crudo mientras el resto de la cadena
    # simula no escribir.
    sin_extraccion = args.sin_extraccion or args.dry

    pasos = selecciona(desde=args.desde, hasta=args.hasta,
                       sin_extraccion=sin_extraccion,
                       sin_opcionales=args.sin_opcionales)

    n, fuente = conjunto_de_partida()
    print("=" * 74)
    print("RUTINA SEMANAL" + ("  ·  --dry: no se escribirá nada" if args.dry else ""))
    print("=" * 74)
    if n > 0:
        print(f"conjunto de partida : {n} variables, {fuente}")
    elif n == 0:
        print("AVISO · no hay conjunto de variables con el que emitir "
              f"({fuente}).\n        La cadena llegará hasta «web» y fallará ahí. "
              "Cierra antes la\n        búsqueda: python scripts/models/seleccion.py")
    else:
        print(f"conjunto de partida : sin determinar ({fuente})")
    if sin_extraccion:
        print("extracción          : omitida; se recalcula sobre lo que hay en disco")
    print(f"pasos               : {len(pasos)} de {len(PASOS)}")

    if args.listar:
        enseña(pasos, dry=args.dry)
        print("\n(--listar: no se ha ejecutado nada)")
        return 0
    return corre(pasos, dry=args.dry)


if __name__ == "__main__":
    raise SystemExit(main())
