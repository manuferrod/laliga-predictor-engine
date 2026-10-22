"""Traslado de lo publicado por el motor al repositorio de la aplicación.

El motor y la web son dos repositorios y **no comparten disco en producción**.
`publish_web.py` deja catorce artefactos en `outputs/web/`, que no se versiona
(`.gitignore`), y la aplicación se despliega en Streamlit Cloud, que solo ve lo
que está **commiteado** en `laliga-predictor-portfolio`. Entre las dos cosas no
había nada: el traslado se venía haciendo a mano, y el modo de fallo de un
traslado manual no es que falte un fichero —eso se ve en cuanto la página abre—
sino que **sobre un artefacto viejo**, que se lee igual de bien que uno nuevo y
no avisa de nada. Es el mismo error que la regla 10 describe para la rutina
semanal, y la respuesta es la misma: un paso declarado, en orden, que falla
pronto.

Qué se copia, y por qué no un comodín
-------------------------------------
Se copia **exactamente lo que declara `manifiesto_web.json`**, no lo que haya en
la carpeta. La diferencia no es de estilo. `outputs/web/` es un directorio de
salida que nadie vacía entre ejecuciones, así que un artefacto que el motor dejó
de emitir —porque se retiró del contrato, o porque la publicación de hoy no tuvo
jornada pendiente y la de la semana pasada sí— **sigue en el disco**. Un
`copy outputs/web/*` lo trasladaría, y la web serviría indefinidamente la última
copia de algo que el modelo actual ya no respalda. Leyendo el manifiesto, lo que
no está declarado no viaja, y lo que el destino tenga de más **se retira**: la
carpeta de destino es de este paso y de nadie más, de modo que ahí sobrar es
exactamente lo mismo que estar obsoleto.

Esa es también la razón de copiar el manifiesto al final y no al principio. Es
el índice de lo demás; si el traslado se interrumpe a la mitad, el destino se
queda con el manifiesto ANTERIOR, que describe un conjunto de ficheros que sí
está completo, en vez de con el nuevo prometiendo artefactos que no llegaron.

Dónde se deja
-------------
En `<app>/outputs/web/`, en una subcarpeta propia y no suelto en `outputs/`.
Ahí abajo siguen viviendo los CSV del pipeline v1 —`matchlogs_2010.csv` y
compañía, unos 6.7 MB commiteados— que el contrato nuevo no usa. **Borrarlos no
es decisión de este script**: son material de la versión anterior y su destino lo
decide Manuel, igual que el de `notebooks/referencia_v1/`. Mezclar las dos
poblaciones en el mismo directorio obligaría a este paso a distinguir entre «lo
que sobra porque lo retiré del contrato» y «lo que sobra porque nunca fue mío», y
esa distinción no se puede hacer con fiabilidad desde aquí. Con una subcarpeta
propia, la regla de limpieza es trivial y no puede equivocarse.

Uso
---
    python scripts/models/sincroniza_app.py          # traslada
    python scripts/models/sincroniza_app.py --dry    # dice qué haría, no escribe
    python scripts/models/sincroniza_app.py --destino <ruta>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
for _p in (_HERE, _EXTRACTORS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from _common import clear_readonly  # noqa: E402

_ROOT = _HERE.parent.parent
WEB = _ROOT / "outputs" / "web"
MANIFIESTO = "manifiesto_web.json"

# El repositorio de la aplicación, junto a este. Es un valor por defecto y no una
# constante del sistema: `--destino` lo sustituye, y por eso el script comprueba
# que la ruta exista antes de escribir en ella en vez de crearla. Crear
# `laliga-predictor-portfolio/outputs/web/` donde no hay repositorio de la app
# convertiría una ruta mal escrita en un traslado silencioso a ninguna parte.
APP = _ROOT.parent / "laliga-predictor-portfolio"
DESTINO = APP / "outputs" / "web"

# El recibo que queda en el destino. No duplica el manifiesto: el manifiesto dice
# qué publicó el motor, y esto dice qué llegó, cuándo y con qué huella. Las dos
# cosas se separan porque la pregunta que contestan es distinta —«¿es este el
# modelo que se midió?» frente a «¿está la web leyendo la última publicación?»— y
# porque es lo único que permite a la aplicación FECHAR lo que muestra sin
# depender de la fecha de modificación de un fichero, que `git clone` no conserva.
RECIBO = "sincronizacion.json"


def _huella(ruta: Path) -> str:
    """SHA-256 abreviado de un fichero, leído por tramos.

    Abreviado a dieciséis caracteres porque su uso es comparar dos copias del
    mismo artefacto, no resistir un adversario: con 64 bits de huella, dos
    parquets distintos coincidiendo por azar es un suceso que no ocurre en la
    vida de este proyecto, y el recibo se lee a ojo.
    """
    h = hashlib.sha256()
    with ruta.open("rb") as f:
        for tramo in iter(lambda: f.read(1 << 20), b""):
            h.update(tramo)
    return h.hexdigest()[:16]


def lee_manifiesto(origen: Path = WEB) -> dict:
    """El manifiesto de la última publicación, o un fallo que dice qué ejecutar.

    Un manifiesto ausente no es un error de este script y el mensaje lo dice: lo
    que falta es la publicación, no el traslado.
    """
    ruta = origen / MANIFIESTO
    if not ruta.exists():
        raise FileNotFoundError(
            f"no hay {MANIFIESTO} en {origen}. Ejecuta primero "
            "`python scripts/models/publish_web.py`.")
    return json.loads(ruta.read_text(encoding="utf-8"))


def declarados(manifiesto: dict) -> list[str]:
    """Los artefactos que el manifiesto declara, sin contarse a sí mismo.

    El manifiesto se lista dentro de su propia lista de artefactos —así se
    publicó, y es coherente: es uno de los ficheros que hay que servir—, pero el
    traslado lo trata aparte porque va en último lugar, de modo que aquí se
    retira para que no viaje dos veces ni se cuente dos veces.
    """
    arte = list(manifiesto.get("artefactos") or [])
    if not arte:
        raise ValueError(
            f"{MANIFIESTO} no declara ningún artefacto; el traslado no tiene "
            "nada que copiar y eso es un fallo de la publicación, no de aquí.")
    return [a for a in arte if a != MANIFIESTO]


def revisa_origen(nombres: list[str], origen: Path = WEB) -> list[Path]:
    """Que esté en el disco todo lo que el manifiesto promete.

    Se comprueba el conjunto entero antes de copiar un solo fichero, y no fichero
    a fichero sobre la marcha. Un traslado que copia nueve de catorce y aborta
    deja el destino en un estado que ningún manifiesto describe —ni el viejo, ni
    el nuevo—, y es el único estado del que no se sale releyendo nada.
    """
    rutas, faltan = [], []
    for n in nombres:
        r = origen / n
        (rutas if r.exists() else faltan).append(r if r.exists() else n)
    if faltan:
        raise FileNotFoundError(
            f"{MANIFIESTO} declara {len(faltan)} artefacto(s) que no están en "
            f"{origen}: {', '.join(sorted(faltan))}. La publicación quedó a "
            "medias; vuelve a ejecutar `publish_web.py`.")
    return rutas


# Margen de tolerancia del orden temporal. `publish_web` escribe los artefactos y
# luego el manifiesto dentro del mismo proceso, así que la diferencia legítima es
# de milisegundos; dos segundos cubren la resolución de la marca de tiempo del
# sistema de ficheros sin tapar un fichero reescrito después, que es lo que se
# busca detectar.
_MARGEN_S = 2.0


def desfase(nombres: list[str], origen: Path = WEB) -> list[str]:
    """Artefactos más nuevos que el manifiesto que los declara.

    Es la firma de una publicación interrumpida o de un artefacto reescrito a
    mano después: el manifiesto describiría un estado que ya no es el del disco.
    Se devuelve en vez de abortar porque el margen de un segundo entre la
    escritura del último parquet y la del manifiesto es normal, y quien decide
    qué hacer con ello es `main`, con un margen declarado.
    """
    corte = (origen / MANIFIESTO).stat().st_mtime
    return sorted(n for n in nombres
                  if (origen / n).stat().st_mtime > corte + _MARGEN_S)


def sobrantes(nombres: list[str], destino: Path) -> list[Path]:
    """Lo que hay en el destino y el contrato ya no declara.

    La carpeta de destino es exclusiva de este paso, de modo que no hay que
    distinguir entre lo obsoleto y lo ajeno: cuanto no esté declarado, sobra. El
    recibo se excluye porque lo escribe este script y no viene del manifiesto.
    """
    if not destino.exists():
        return []
    vivos = set(nombres) | {MANIFIESTO, RECIBO}
    return sorted(p for p in destino.iterdir()
                  if p.is_file() and p.name not in vivos)


def _copia(origen: Path, destino: Path) -> dict:
    """Copia un fichero y verifica la copia por huella, no por tamaño.

    El tamaño coincide en una copia truncada por un disco lleno con más
    frecuencia de la que parece —el parquet acaba con su pie, y un fichero
    cortado pesa distinto, pero una escritura parcial sobre un fichero anterior
    del mismo tamaño no—. La huella cierra el caso por un coste despreciable
    frente a leer y escribir el fichero, que ya se está haciendo.
    """
    clear_readonly(destino)
    shutil.copy2(origen, destino)
    h_o, h_d = _huella(origen), _huella(destino)
    if h_o != h_d:
        raise OSError(f"la copia de {origen.name} no coincide con el original "
                      f"({h_o} != {h_d}); el destino no es de fiar.")
    return {"artefacto": origen.name, "bytes": origen.stat().st_size,
            "huella": h_o}


def sincroniza(origen: Path = WEB, destino: Path = DESTINO, *,
               dry: bool = False) -> dict:
    """Traslada la última publicación al repositorio de la aplicación.

    Devuelve el recibo, lo haya escrito o no, para que `--dry` pueda informar de
    exactamente lo que habría quedado.
    """
    manifiesto = lee_manifiesto(origen)
    nombres = declarados(manifiesto)
    rutas = revisa_origen(nombres, origen)
    tarde = desfase(nombres, origen)
    de_mas = sobrantes(nombres, destino)

    if not dry:
        destino.mkdir(parents=True, exist_ok=True)

    copiados = []
    for r in rutas:
        copiados.append(_copia(r, destino / r.name) if not dry
                        else {"artefacto": r.name, "bytes": r.stat().st_size,
                              "huella": _huella(r)})
    for p in de_mas:
        if not dry:
            clear_readonly(p)
            os.remove(p)

    recibo = {
        "sincronizado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        # La identidad de la publicación trasladada, copiada del manifiesto y no
        # recalculada: es la que permite a la web decir de qué ejecución del motor
        # procede lo que muestra, y tiene que ser la misma cadena que auditó
        # `qa_models`.
        "run_code": manifiesto.get("run_code"),
        "generado": manifiesto.get("generado"),
        "regimen": manifiesto.get("regimen"),
        "modelo": manifiesto.get("modelo"),
        # El repositorio y el subdirectorio de procedencia, no la ruta absoluta:
        # el recibo viaja versionado en un repositorio público y la ruta de la
        # máquina no añade nada que la identidad de la publicación no diga ya.
        "origen": "/".join(origen.parts[-3:]),
        "n_artefactos": len(copiados),
        "artefactos": copiados,
        "retirados": [p.name for p in de_mas],
        "posteriores_al_manifiesto": tarde,
    }
    if not dry:
        # El manifiesto, el último de todos, y el recibo detrás: hasta que el
        # manifiesto nuevo esté en su sitio, el destino describe con el viejo un
        # conjunto completo de ficheros.
        _copia(origen / MANIFIESTO, destino / MANIFIESTO)
        d = destino / RECIBO
        clear_readonly(d)
        d.write_text(json.dumps(recibo, indent=2, ensure_ascii=False),
                     encoding="utf-8")
    return recibo


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry", action="store_true",
                    help="dice qué trasladaría y qué retiraría, sin escribir")
    ap.add_argument("--destino", type=Path, default=DESTINO,
                    help=f"carpeta de la app donde dejarlo (por omisión {DESTINO})")
    args = ap.parse_args(argv)

    destino = args.destino
    # Se exige que el repositorio de la app exista, y no se crea. Una ruta mal
    # escrita crearía el árbol entero y el traslado «funcionaría» dejando los
    # catorce artefactos donde nadie los lee.
    if not destino.parent.parent.exists():
        print(f"[ERROR] no existe el repositorio de la app en "
              f"{destino.parent.parent}. Usa --destino si está en otro sitio.")
        return 1

    try:
        recibo = sincroniza(WEB, destino, dry=args.dry)
    except (FileNotFoundError, ValueError, OSError) as e:
        print(f"[ERROR] {e}")
        return 1

    marca = "[DRY] " if args.dry else ""
    print(f"{marca}publicación {recibo['run_code']} · modelo "
          f"{recibo['modelo']} · régimen {recibo['regimen']}")
    print(f"{marca}{recibo['n_artefactos']} artefactos "
          f"-> {destino}")
    total = sum(a["bytes"] for a in recibo["artefactos"])
    print(f"{marca}{total / 1e6:.2f} MB")
    if recibo["retirados"]:
        print(f"{marca}retirados por no estar declarados: "
              f"{', '.join(recibo['retirados'])}")
    if recibo["posteriores_al_manifiesto"]:
        # Aviso y no abortar: el traslado es consistente consigo mismo —copia lo
        # declarado— y lo que está en duda es la publicación de origen.
        print(f"[AVISO] {len(recibo['posteriores_al_manifiesto'])} artefacto(s) "
              f"son posteriores al manifiesto que los declara: "
              f"{', '.join(recibo['posteriores_al_manifiesto'])}. "
              "Vuelve a ejecutar `publish_web.py` para que el manifiesto "
              "describa lo que hay en el disco.")
    if not args.dry:
        print("Falta el `git push` del repositorio de la app: Streamlit Cloud "
              "solo sirve lo commiteado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
