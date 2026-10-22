"""Comprobacion de higiene: todo `modulo.nombre` que el proyecto escribe, existe.

El fallo que esta bateria previene es de los que no se ven leyendo el codigo. Un
modulo importa otro con un alias corto —`import columns as C`— y lo consulta en unos
pocos sitios; si el nombre consultado nunca se llego a definir, o si se movio a otro
modulo al reorganizar, el error no aparece al compilar ni al importar, sino en el
instante en que se ejecuta la linea concreta que lo usa. En una cadena donde la
mitad de los caminos solo se recorren con artefactos en disco, esa linea puede
tardar semanas en ejecutarse por primera vez.

La comprobacion recorre el arbol de `scripts/`, resuelve para cada fichero los
alias de sus importaciones internas y verifica que cada atributo consultado a traves
de uno de esos alias figura entre los nombres publicos del modulo apuntado. Es un
analisis estatico y por tanto conservador: solo mira asignaciones, definiciones e
importaciones de nivel superior, que es donde vive el vocabulario compartido entre
modulos. Lo que se crea dinamicamente queda fuera y se declara en `_TOLERADOS`.

Los cuadernos se revisan igual, y por un motivo mas fuerte todavia. Un notebook
no lo importa nadie, de modo que ninguna otra bateria lo toca: si una celda
llama a una funcion que se renombro, el fallo espera a que alguien ejecute esa
celda concreta, que en un documento de setenta y tres celdas puede ser la ultima
del dia. Aqui se comprueban ademas los `from modulo import nombre`, que en los
cuadernos son la forma habitual de traer una funcion y fallan igual de tarde.

Que el nombre exista no basta: tambien se comprueba que la LLAMADA encaje con la
firma declarada. Un nombre puede seguir estando y haber cambiado de forma —una
funcion que gana un parametro obligatorio, otra que pierde uno— y entonces el
`AttributeError` no llega nunca; llega un `TypeError` mucho mas adentro, cuando
alguien recorre esa rama. Se revisan tres cosas: que no se pasen mas posicionales
de los que la firma admite, que ningun nombre de argumento sea desconocido para
ella, y que no falte ninguno de los obligatorios. Es deliberadamente conservador:
en cuanto la llamada desempaqueta con `*` o `**` no se puede saber que llega, y esa
llamada se deja pasar.

Se vigila ademas la FORMA de importar. Las carpetas de `scripts/` se registran en
`sys.path` por separado y sus modulos se traen por su nombre a secas; escribir
`import models.columns` parece incluso mas ordenado, pero no resuelve, y el error
no lo ve ninguna de las comprobaciones anteriores: si el alias nunca llega a
ligarse, tampoco deja referencias que revisar. Es el hueco por el que se colo la
celda de preparacion del cuaderno 05, y por eso se comprueba aparte.

No sustituye a las baterias de cada modulo. Vigila una propiedad distinta: que el
vocabulario con el que los modulos y los cuadernos se hablan entre si sigue
siendo el mismo.
"""
import ast
import json
import pathlib
import sys

_CANDS = [
    pathlib.Path(__file__).resolve().parent,
    pathlib.Path.home() / "Desktop" / "TFM 2026" / "TFM" / "repos" /
        "laliga-predictor-engine" / "scripts",
]
SCRIPTS = next((p for p in _CANDS if (p / "features" / "columns.py").exists()),
               _CANDS[0])
NOTEBOOKS = SCRIPTS.parent / "notebooks"

# Atributos que un analisis estatico no puede ver porque el modulo los ata en
# tiempo de ejecucion. Cada entrada necesita una razon, para que la lista no crezca
# como cajon de sastre de referencias rotas de verdad.
_TOLERADOS = {
    # `availability` y `transfermarkt` importan su dependencia dentro de un bloque
    # try/except para funcionar igual como paquete y como script suelto; el nombre
    # queda ligado al modulo, pero no en el cuerpo de nivel superior.
    ("availability", "sofa"),
    ("transfermarkt", "tn"),
}

PASS, FAIL = [], []


def check(nombre: str, cond: bool, extra: str = "") -> None:
    (PASS if cond else FAIL).append(nombre)
    print(("  OK  " if cond else " FAIL ") + nombre +
          (f"  -> {extra}" if extra and not cond else ""))


def _publicos(arbol: ast.Module) -> set[str]:
    """Nombres que el modulo deja disponibles para quien lo importe.

    Se incluyen las importaciones porque un modulo reexporta de hecho lo que
    importa: `preprocess.C` es una referencia legitima aunque `C` no se defina alli.
    """
    ns: set[str] = set()
    pendientes = list(arbol.body)
    while pendientes:
        n = pendientes.pop()
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            ns.add(n.name)
        elif isinstance(n, ast.Assign):
            # Se recorre el destino en vez de leer solo los `Name` de primer
            # nivel, porque una asignacion multiple --`OK, DEGRADADA, CAIDA =
            # ...`, como declara `sonda.py` sus tres estados-- tiene por destino
            # un `Tuple`, y leyendo solo el primer nivel los tres nombres
            # quedarian invisibles y toda referencia a ellos se daria por rota.
            for t in n.targets:
                ns |= {x.id for x in ast.walk(t) if isinstance(x, ast.Name)}
        elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name):
            ns.add(n.target.id)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            ns |= {(a.asname or a.name.split(".")[0]) for a in n.names}
        elif isinstance(n, (ast.Try, ast.If)):
            # Las importaciones condicionales -el patron paquete/script suelto- son
            # de nivel superior a todos los efectos.
            pendientes += n.body + n.orelse + getattr(n, "finalbody", [])
            for h in getattr(n, "handlers", []):
                pendientes += h.body
    return ns


_CACHE: dict[str, tuple[pathlib.Path, set[str]]] = {}


def _modulos() -> dict[str, tuple[pathlib.Path, set[str]]]:
    # Las dos pruebas leen el mismo arbol; recorrerlo una vez evita ademas contar
    # dos veces un modulo que no compile.
    if _CACHE:
        return _CACHE
    fuera = {"_legacy_v1", "__pycache__", ".venv"}
    mods: dict[str, tuple[pathlib.Path, set[str]]] = {}
    for p in sorted(SCRIPTS.rglob("*.py")):
        if fuera & set(p.parts):
            continue
        try:
            arbol = ast.parse(p.read_text(encoding="utf-8"))
        except SyntaxError as e:                       # se reporta abajo
            mods[p.stem] = (p, set())
            print(f" FAIL  {p.name} no compila (linea {e.lineno})")
            FAIL.append(f"{p.name} no compila")
            continue
        mods[p.stem] = (p, _publicos(arbol))
        _FIRMAS[p.stem] = _firmas(arbol)
    _CACHE.update(mods)
    return mods


def _alias(arbol: ast.Module, mods: dict) -> dict[str, str]:
    """Alias -> modulo del proyecto, incluidas las importaciones condicionales."""
    alias: dict[str, str] = {}
    for n in ast.walk(arbol):
        if isinstance(n, ast.Import):
            for a in n.names:
                m = a.name.split(".")[0]
                if m in mods:
                    alias[a.asname or m] = m
        elif isinstance(n, ast.ImportFrom) and n.level:
            for a in n.names:              # from . import sofascore as sofa
                if a.name in mods:
                    alias[a.asname or a.name] = a.name
    return alias


def _rotas_atributo(arbol: ast.Module, mods: dict, alias: dict[str, str],
                    etiqueta: str) -> list[str]:
    rotas: list[str] = []
    for n in ast.walk(arbol):
        if not (isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)):
            continue
        if n.value.id not in alias or n.attr.startswith("__"):
            continue
        destino = alias[n.value.id]
        if n.attr in mods[destino][1] or (destino, n.value.id) in _TOLERADOS:
            continue
        rotas.append(f"{etiqueta}:{n.lineno}  {n.value.id}.{n.attr} "
                     f"(no existe en {destino}.py)")
    return rotas


_FIRMAS: dict[str, dict[str, ast.arguments]] = {}


def _firmas(arbol: ast.Module) -> dict[str, ast.arguments]:
    """Firmas de las funciones de nivel superior del modulo.

    Solo las de nivel superior, y solo funciones. Un metodo se llama sobre una
    instancia y el analisis estatico no sabe de que clase es; una clase se
    instancia con un `__init__` que puede venir de un decorador. Comprobar lo que
    no se puede resolver produciria avisos falsos, y una bateria que avisa en
    falso se acaba desactivando, que es peor que no tenerla.
    """
    return {n.name: n.args for n in arbol.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _encaja(firma: ast.arguments, llamada: ast.Call) -> str:
    """Motivo por el que la llamada no encaja con la firma, o cadena vacia.

    Devuelve el motivo en lugar de un booleano porque el valor de esta bateria
    esta en el mensaje: saber que algo no encaja no ahorra trabajo, saber que
    sobra un posicional o que falta `regime` lo ahorra entero.
    """
    posicionales = [a.arg for a in firma.posonlyargs + firma.args]
    solo_nombre = [a.arg for a in firma.kwonlyargs]
    n_defecto = len(firma.defaults)
    obligatorios = posicionales[:len(posicionales) - n_defecto] if n_defecto \
        else list(posicionales)
    obligatorios += [a.arg for a, d in zip(firma.kwonlyargs, firma.kw_defaults)
                     if d is None]

    estrellado = any(isinstance(a, ast.Starred) for a in llamada.args)
    doble = any(k.arg is None for k in llamada.keywords)
    dados_pos = sum(1 for a in llamada.args if not isinstance(a, ast.Starred))
    dados_nom = {k.arg for k in llamada.keywords if k.arg is not None}

    if not estrellado and firma.vararg is None and dados_pos > len(posicionales):
        return (f"{dados_pos} argumentos posicionales y la firma declara "
                f"{len(posicionales)}")
    if firma.kwarg is None:
        desconocidos = sorted(dados_nom - set(posicionales) - set(solo_nombre))
        if desconocidos:
            return f"argumento(s) que la firma no declara: {', '.join(desconocidos)}"
    if not estrellado and not doble:
        cubiertos = set(posicionales[:dados_pos]) | dados_nom
        faltan = [p for p in obligatorios if p not in cubiertos]
        if faltan:
            return f"falta(n) el argumento obligatorio {', '.join(faltan)}"
    return ""


def _rotas_aridad(arbol: ast.Module, alias: dict[str, str],
                  etiqueta: str) -> list[str]:
    rotas: list[str] = []
    for n in ast.walk(arbol):
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        if not (isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name)):
            continue
        if f.value.id not in alias:
            continue
        firma = _FIRMAS.get(alias[f.value.id], {}).get(f.attr)
        if firma is None:
            continue
        motivo = _encaja(firma, n)
        if motivo:
            rotas.append(f"{etiqueta}:{n.lineno}  {f.value.id}.{f.attr}(...) "
                         f"recibe {motivo}")
    return rotas


def _paquetes() -> set[str]:
    """Las carpetas de `scripts/`, que NO son paquetes importables."""
    return {p.name for p in SCRIPTS.iterdir()
            if p.is_dir() and not p.name.startswith((".", "_"))}


def _rotas_paquete(arbol: ast.Module, paquetes: set[str],
                   etiqueta: str) -> list[str]:
    """`import models.columns`: el proyecto no se importa como paquete.

    Cada carpeta de `scripts/` se registra en `sys.path` por separado y sus
    modulos se traen por su nombre a secas —`import columns as C`—. Escribirlo
    con punto se lee como correcto y ademas parece mas ordenado, pero no
    resuelve: la carpeta no esta en el camino de busqueda como paquete. El
    fallo llega al ejecutar la primera celda, y hasta entonces ninguna otra
    comprobacion lo ve, porque un alias que nunca se llego a ligar tampoco deja
    referencias que revisar.
    """
    rotas: list[str] = []
    for n in ast.walk(arbol):
        if isinstance(n, ast.Import):
            nombres = [a.name for a in n.names]
        elif isinstance(n, ast.ImportFrom) and not n.level and n.module:
            nombres = [n.module]
        else:
            continue
        for nombre in nombres:
            raiz, _, resto = nombre.partition(".")
            if resto and raiz in paquetes:
                rotas.append(
                    f"{etiqueta}:{n.lineno}  import {nombre} "
                    f"(«{raiz}» no es un paquete: importa "
                    f"«{nombre.split('.')[-1]}» a secas)")
    return rotas


def _rotas_importacion(arbol: ast.Module, mods: dict, etiqueta: str) -> list[str]:
    """`from modulo import nombre` donde el nombre ya no esta en el modulo."""
    rotas: list[str] = []
    for n in ast.walk(arbol):
        if not (isinstance(n, ast.ImportFrom) and not n.level):
            continue
        if n.module not in mods:
            continue
        for a in n.names:
            if a.name != "*" and a.name not in mods[n.module][1]:
                rotas.append(f"{etiqueta}:{n.lineno}  from {n.module} import "
                             f"{a.name} (no existe)")
    return rotas


def test_referencias_cruzadas() -> None:
    mods = _modulos()
    paquetes = _paquetes()
    rotas: list[str] = []
    dotadas: list[str] = []
    torcidas: list[str] = []

    for stem, (ruta, _) in sorted(mods.items()):
        try:
            arbol = ast.parse(ruta.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        alias = _alias(arbol, mods)
        rotas += _rotas_atributo(arbol, mods, alias, ruta.name)
        dotadas += _rotas_paquete(arbol, paquetes, ruta.name)
        torcidas += _rotas_aridad(arbol, alias, ruta.name)

    check(f"referencias entre modulos resueltas ({len(mods)} modulos)",
          not rotas, "; ".join(sorted(set(rotas))[:8]))
    check("los modulos se importan por su nombre, no como paquete",
          not dotadas, "; ".join(sorted(set(dotadas))[:8]))
    check("las llamadas entre modulos encajan con la firma declarada",
          not torcidas, "; ".join(sorted(set(torcidas))[:8]))
    assert not rotas, "referencias rotas:\n  " + "\n  ".join(sorted(set(rotas)))
    assert not dotadas, ("importaciones con punto:\n  "
                         + "\n  ".join(sorted(set(dotadas))))
    assert not torcidas, ("llamadas que no encajan:\n  "
                          + "\n  ".join(sorted(set(torcidas))))


def _celdas_de_codigo(ruta: pathlib.Path) -> list[tuple[int, str]]:
    nb = json.loads(ruta.read_text(encoding="utf-8"))
    return [(i, "".join(c["source"])) for i, c in enumerate(nb["cells"])
            if c["cell_type"] == "code"]


def test_referencias_en_notebooks() -> None:
    """El mismo control, sobre los cuadernos que son entregable.

    Los alias se resuelven sobre el cuaderno entero -la celda de preparacion
    importa y el resto consulta- y las referencias se localizan celda a celda,
    que es la unidad en la que uno abre el documento y corrige.
    """
    mods = _modulos()
    paquetes = _paquetes()
    fuera = {"referencia_v1", ".ipynb_checkpoints", "_legacy_v1"}
    cuadernos = [p for p in sorted(NOTEBOOKS.rglob("*.ipynb"))
                 if not fuera & set(p.parts)]
    check("los cuadernos entregables estan donde se espera", bool(cuadernos),
          f"no hay .ipynb bajo {NOTEBOOKS}")

    rotas: list[str] = []
    dotadas: list[str] = []
    torcidas: list[str] = []
    for p in cuadernos:
        celdas = _celdas_de_codigo(p)
        try:
            entero = ast.parse("\n".join(src for _, src in celdas))
        except SyntaxError as e:
            rotas.append(f"{p.name} no compila (linea {e.lineno}: {e.msg})")
            continue
        alias = _alias(entero, mods)
        for i, src in celdas:
            try:
                arbol = ast.parse(src)
            except SyntaxError as e:
                rotas.append(f"{p.name} celda {i} no compila ({e.msg})")
                continue
            etiqueta = f"{p.name} celda {i}"
            rotas += _rotas_atributo(arbol, mods, alias, etiqueta)
            rotas += _rotas_importacion(arbol, mods, etiqueta)
            dotadas += _rotas_paquete(arbol, paquetes, etiqueta)
            torcidas += _rotas_aridad(arbol, alias, etiqueta)

    check(f"referencias de los cuadernos resueltas ({len(cuadernos)} cuadernos)",
          not rotas, "; ".join(sorted(set(rotas))[:8]))
    check("los cuadernos importan por nombre, no como paquete",
          not dotadas, "; ".join(sorted(set(dotadas))[:8]))
    check("las llamadas de los cuadernos encajan con la firma declarada",
          not torcidas, "; ".join(sorted(set(torcidas))[:8]))
    assert not rotas, "referencias rotas:\n  " + "\n  ".join(sorted(set(rotas)))
    assert not dotadas, ("importaciones con punto:\n  "
                         + "\n  ".join(sorted(set(dotadas))))
    assert not torcidas, ("llamadas que no encajan:\n  "
                          + "\n  ".join(sorted(set(torcidas))))


def test_split_una_sola_definicion() -> None:
    """El nombre de la columna de sellado se declara una vez y se reutiliza.

    `columns.py` es el registro de nombres de columna del proyecto y `preprocess.py`
    lo consulta. Que ambos escribiesen el literal por su cuenta no romperia nada hoy,
    pero convertiria un cambio de nombre en un fallo silencioso: la mitad de la
    cadena leeria una columna y la otra mitad otra que ya no existe.
    """
    sys.path.insert(0, str(SCRIPTS / "features"))
    import columns as C          # noqa: E402
    import preprocess as pp      # noqa: E402

    check("columns declara SPLIT", getattr(C, "SPLIT", None) == "split",
          repr(getattr(C, "SPLIT", None)))
    check("preprocess toma SPLIT de columns", pp.SPLIT is C.SPLIT)
    assert C.SPLIT == "split" and pp.SPLIT is C.SPLIT


if __name__ == "__main__":
    test_referencias_cruzadas()
    test_referencias_en_notebooks()
    test_split_una_sola_definicion()
    print(f"\n{len(PASS)} OK / {len(FAIL)} FAIL")
    sys.exit(1 if FAIL else 0)
