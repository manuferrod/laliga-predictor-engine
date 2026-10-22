"""Banco de `sincroniza_app.py`.

Lo que hay que comprobar aquí no es que `shutil.copy2` copie —eso no es de este
repositorio— sino las **cuatro reglas** que hacen que el traslado no pueda servir
algo viejo sin decirlo, que son justamente las que un `copy outputs/web/*` no
tiene:

  1. viaja lo que el manifiesto DECLARA, no lo que haya en la carpeta
  2. lo que el destino tenga de más se retira
  3. si falta un artefacto declarado, no se copia NADA
  4. el manifiesto llega el último, para que una interrupción deje el destino
     describiéndose con el manifiesto anterior y no con uno que miente

Todo se ejecuta sobre un origen y un destino sintéticos en un directorio
temporal: el banco no mira `outputs/web/` ni el repositorio de la aplicación, de
modo que pasa en una máquina que nunca haya publicado nada.

    python scripts/models/test_sincroniza_app.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import sincroniza_app as sa  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(ok: bool, msg: str) -> None:
    (PASS if ok else FAIL).append(msg)
    print(f"  [{'OK ' if ok else 'FALLO'}] {msg}")


# ===========================================================================
# Un origen de juguete con la forma del real
# ===========================================================================
_ARTE = ("metricas_temporada.parquet", "matchlogs.parquet", "ficha_modelo.json")


def _origen(base: Path, arte: tuple[str, ...] = _ARTE, *,
            falta: str | None = None) -> Path:
    """Una carpeta con el manifiesto y sus artefactos, como la deja `publish_web`.

    `falta` escribe el manifiesto declarando un artefacto que no se crea, que es
    el estado de una publicación interrumpida.
    """
    o = base / "web"
    o.mkdir(parents=True, exist_ok=True)
    for n in arte:
        if n != falta:
            (o / n).write_bytes(f"contenido de {n}".encode("utf-8"))
    (o / sa.MANIFIESTO).write_text(json.dumps({
        "generado": "2026-10-19T18:53:30+00:00",
        "run_code": "7f228a4bbd64",
        "regimen": "semanal",
        "modelo": "mezcla",
        # Así se publica de verdad: el manifiesto se incluye en su propia lista.
        "artefactos": sorted([*arte, sa.MANIFIESTO]),
    }, indent=2), encoding="utf-8")
    return o


# ===========================================================================
def test_1_declarados() -> None:
    print("\n[1] la lista de lo que viaja sale del manifiesto")
    with tempfile.TemporaryDirectory() as td:
        o = _origen(Path(td))
        man = sa.lee_manifiesto(o)
        decl = sa.declarados(man)
        check(set(decl) == set(_ARTE),
              "los artefactos declarados son los del manifiesto")
        check(sa.MANIFIESTO not in decl,
              "y el manifiesto no se cuenta entre ellos: va aparte y el ultimo")

        # Un artefacto huerfano en la carpeta: lo que dejo una publicacion
        # anterior y el contrato de hoy ya no respalda.
        (o / "proxima_jornada.csv").write_text("viejo", encoding="utf-8")
        check(set(sa.declarados(sa.lee_manifiesto(o))) == set(_ARTE),
              "un artefacto en la carpeta que el manifiesto no declara NO viaja")

        try:
            sa.lee_manifiesto(Path(td) / "noexiste")
            check(False, "sin manifiesto se aborta diciendo que falta publicar")
        except FileNotFoundError as e:
            check("publish_web" in str(e),
                  "sin manifiesto se aborta diciendo que falta publicar")

        (o / sa.MANIFIESTO).write_text(json.dumps({"artefactos": []}),
                                       encoding="utf-8")
        try:
            sa.declarados(sa.lee_manifiesto(o))
            check(False, "un manifiesto sin artefactos es un fallo, no un cero")
        except ValueError:
            check(True, "un manifiesto sin artefactos es un fallo, no un cero")


def test_2_traslado() -> None:
    print("\n[2] el traslado y su recibo")
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        o = _origen(base)
        d = base / "app" / "outputs" / "web"
        recibo = sa.sincroniza(o, d)

        check(sorted(p.name for p in d.iterdir())
              == sorted([*_ARTE, sa.MANIFIESTO, sa.RECIBO]),
              "en el destino quedan los artefactos, el manifiesto y el recibo")
        check(recibo["n_artefactos"] == len(_ARTE),
              f"el recibo cuenta los artefactos sin el manifiesto "
              f"({recibo['n_artefactos']})")
        check(recibo["run_code"] == "7f228a4bbd64"
              and recibo["modelo"] == "mezcla",
              "y copia del manifiesto la identidad de la publicacion trasladada")
        check(all((o / n).read_bytes() == (d / n).read_bytes() for n in _ARTE),
              "el contenido llega intacto")
        check(all(a["huella"] == sa._huella(d / a["artefacto"])
                  for a in recibo["artefactos"]),
              "y la huella del recibo es la del fichero que hay en el destino")
        check(json.loads((d / sa.RECIBO).read_text(encoding="utf-8"))["run_code"]
              == "7f228a4bbd64",
              "el recibo se escribe en el destino, que es quien tiene que fecharlo")
        check(recibo["posteriores_al_manifiesto"] == [],
              "una publicacion coherente no deja artefactos mas nuevos que su indice")


def test_3_retira_lo_sobrante() -> None:
    print("\n[3] lo que el contrato ya no declara se retira")
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        o = _origen(base)
        d = base / "app" / "outputs" / "web"
        sa.sincroniza(o, d)

        # La semana pasada hubo jornada pendiente y esta no: el destino arrastra
        # cinco artefactos de emision que el manifiesto de hoy no declara.
        viejos = ("proxima_jornada.csv", "narrativa_jornada.parquet")
        for n in viejos:
            (d / n).write_text("de la semana pasada", encoding="utf-8")
        check(sorted(p.name for p in sa.sobrantes(list(_ARTE), d))
              == sorted(viejos),
              "se detecta lo que sobra en el destino")

        recibo = sa.sincroniza(o, d)
        check(not any((d / n).exists() for n in viejos),
              "y se retira: un artefacto que el modelo ya no respalda no se sirve")
        check(sorted(recibo["retirados"]) == sorted(viejos),
              "el recibo dice que lo retiro, para que no sea un borrado silencioso")
        check((d / sa.RECIBO).exists() and (d / sa.MANIFIESTO).exists(),
              "la limpieza no se lleva por delante el manifiesto ni el recibo")


def test_4_publicacion_a_medias() -> None:
    print("\n[4] una publicacion incompleta no se traslada a medias")
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        o = _origen(base, falta="matchlogs.parquet")
        d = base / "app" / "outputs" / "web"
        try:
            sa.sincroniza(o, d)
            check(False, "si falta un artefacto declarado, se aborta")
        except FileNotFoundError as e:
            check("matchlogs.parquet" in str(e),
                  "si falta un artefacto declarado, se aborta nombrandolo")
        check(not d.exists() or not list(d.iterdir()),
              "y no se copia NINGUNO: el destino no queda en un estado que no "
              "describa ningun manifiesto")


def test_5_dry() -> None:
    print("\n[5] --dry no escribe y dice lo mismo")
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        o = _origen(base)
        d = base / "app" / "outputs" / "web"
        (d).mkdir(parents=True)
        (d / "sobrante.csv").write_text("x", encoding="utf-8")

        recibo = sa.sincroniza(o, d, dry=True)
        check(sorted(p.name for p in d.iterdir()) == ["sobrante.csv"],
              "no se copia nada ni se retira nada")
        check(recibo["n_artefactos"] == len(_ARTE)
              and recibo["retirados"] == ["sobrante.csv"],
              "pero el recibo informa de lo que habria hecho")
        check(not (d / sa.RECIBO).exists(),
              "y el recibo no se escribe: en seco no hay nada que fechar")


def test_6_desfase() -> None:
    print("\n[6] un artefacto reescrito despues del manifiesto se avisa")
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        o = _origen(base)
        # Mas nuevo que el manifiesto por bastante mas que el margen: es la firma
        # de haber tocado un parquet a mano despues de publicar.
        corte = (o / sa.MANIFIESTO).stat().st_mtime
        import os
        os.utime(o / "matchlogs.parquet", (corte + 60, corte + 60))

        check(sa.desfase(list(_ARTE), o) == ["matchlogs.parquet"],
              "se detecta el artefacto posterior al indice que lo declara")
        d = base / "app" / "outputs" / "web"
        recibo = sa.sincroniza(o, d)
        check(recibo["posteriores_al_manifiesto"] == ["matchlogs.parquet"],
              "y viaja al recibo")
        check((d / "matchlogs.parquet").exists(),
              "pero no aborta: el traslado copia lo declarado y lo que esta en "
              "duda es la publicacion de origen, no el traslado")


def test_7_destino_inventado() -> None:
    print("\n[7] una ruta de destino mal escrita no se crea")
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        _origen(base)
        inventado = base / "no-es-un-repo" / "outputs" / "web"
        code = sa.main(["--destino", str(inventado), "--dry"])
        check(code == 1,
              "se niega si no existe el repositorio de la app bajo el destino")
        check(not inventado.exists(),
              "y no crea el arbol: un traslado a ninguna parte parece funcionar")


def test_8_paso_de_la_rutina() -> None:
    print("\n[8] el paso declarado en la rutina semanal")
    sys.path.insert(0, str(_HERE.parent))
    import rutina_semanal as rt

    por_etiqueta = {p.etiqueta: p for p in rt.PASOS}
    check("app" in por_etiqueta,
          "la rutina declara el traslado: es el error de la regla 10, saltarse "
          "un paso, y por eso no puede quedar fuera de PASOS")
    paso = por_etiqueta["app"]
    check([p.etiqueta for p in rt.PASOS][-1] == "app",
          "va el ultimo: copia lo que publish_web declara")
    check([p.etiqueta for p in rt.PASOS].index("app")
          > [p.etiqueta for p in rt.PASOS].index("web"),
          "y por tanto detras de la proyeccion, nunca delante")
    check(paso.admite_dry and paso.orden(dry=True)[-1] == "--dry",
          "admite --dry, para que la rutina en seco no escriba en el otro repo")
    check(paso.ruta.exists() and not paso.opcional,
          "el script existe y no es opcional")


# ===========================================================================
def _run() -> None:
    """Las pruebas en orden. Toda prueba nueva se añade aquí (ver el aviso de
    `test_publish_web._run`: pytest recoge por su cuenta y tapa el olvido)."""
    print("=" * 72)
    print("BANCO DE PRUEBAS · sincroniza_app.py")
    print("=" * 72)
    test_1_declarados()
    test_2_traslado()
    test_3_retira_lo_sobrante()
    test_4_publicacion_a_medias()
    test_5_dry()
    test_6_desfase()
    test_7_destino_inventado()
    test_8_paso_de_la_rutina()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for m in FAIL:
        print(f"  - {m}")
    print("=" * 72)


def test_sincroniza_app() -> None:
    """Entrada para pytest."""
    _run()
    assert not FAIL, FAIL


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
