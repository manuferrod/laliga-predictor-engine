"""Parche puntual del cuaderno 05: el banco mide sobre el bloque de selección.

Parte C exploraba con `evaluar` por omisión, que es el bloque de validación ENTERO
—2019/20-2024/25—, e incluye por tanto 2022/23-2024/25, que es el bloque de medición
que C.10 publica como la única cifra citable. C.8 y E.5 afirman lo contrario, así que
el cuaderno se contradecía consigo mismo.
"""
from __future__ import annotations

import json
from pathlib import Path

RUTA = Path(__file__).resolve().parent / "notebooks" / "05_MODELADO_Y_EVALUACION.ipynb"

# (índice, texto que tiene que estar, texto nuevo)
PARCHES: list[tuple[int, str, str]] = [
    # --- la constante compartida, en la celda de entorno ---------------------
    (3,
     'PALETA = {"modelo": "#1f4e79", "mercado": "#c0504d", "apoyo": "#7f7f7f"}\n',
     'PALETA = {"modelo": "#1f4e79", "mercado": "#c0504d", "apoyo": "#7f7f7f"}\n'
     '\n'
     '# Todo ensayo exploratorio de este cuaderno se mide sobre el bloque de\n'
     '# SELECCIÓN, y no sobre el de validación entero. El banco de `laboratorio`\n'
     '# acepta cualquier bloque menos el holdout y la temporada en curso, de modo que\n'
     '# el límite no lo pone el código: lo pone el método. Comparar conjuntos a mano\n'
     '# sobre 2022/23-2024/25 gastaría el bloque que C.10 publica como la única cifra\n'
     '# citable del trabajo, y lo gastaría sin dar ningún error y devolviendo una\n'
     '# tabla de aspecto perfectamente normal.\n'
     'BLOQUE_BANCO = sl.BLOQUE_SELECCION      # "seleccion" = 2019/20-2021/22\n'),

    # --- B.3, calibrado de coste ---------------------------------------------
    (23,
     '              desde="2023/24", registrar=False, verbose=False)\n'
     'print(f"ensayo de prueba (logística, 8 variables, desde 2023/24): "\n',
     '              evaluar=BLOQUE_BANCO, desde="2021/22",\n'
     '              registrar=False, verbose=False)\n'
     'print(f"ensayo de prueba (logística, 8 variables, desde 2021/22): "\n'),

    # --- C.2, reconocimiento por dominio ------------------------------------
    (27,
     'DESDE_RECONOCIMIENTO   = "2021/22"      # None = histórico completo\n',
     'DESDE_RECONOCIMIENTO   = "2021/22"      # None = el bloque de selección entero\n'),
    (27,
     'mapa = lb.por_dominio(FAMILIA_RECONOCIMIENTO,\n'
     '                      dominios_pedidos=DOMINIOS_A_MIRAR,\n'
     '                      desde=DESDE_RECONOCIMIENTO,\n',
     'mapa = lb.por_dominio(FAMILIA_RECONOCIMIENTO,\n'
     '                      dominios_pedidos=DOMINIOS_A_MIRAR,\n'
     '                      evaluar=BLOQUE_BANCO, desde=DESDE_RECONOCIMIENTO,\n'),

    # --- C.3, un conjunto a mano --------------------------------------------
    (30,
     'DESDE    = None                          # None = histórico completo\n',
     'DESDE    = None                          # None = el bloque de selección entero\n'),
    (30,
     'ensayo = lb.evalua(CONJUNTO, FAMILIA, nombre=NOMBRE, desde=DESDE, feat=feat)\n',
     'ensayo = lb.evalua(CONJUNTO, FAMILIA, nombre=NOMBRE,\n'
     '                   evaluar=BLOQUE_BANCO, desde=DESDE, feat=feat)\n'),

    # --- C.4, ampliar de una en una -----------------------------------------
    (34,
     'r1 = lb.ronda(BASE, CANDIDATAS, FAMILIA,\n'
     '              etiqueta=ETIQUETA, desde=DESDE, feat=feat)\n',
     'r1 = lb.ronda(BASE, CANDIDATAS, FAMILIA, etiqueta=ETIQUETA,\n'
     '              evaluar=BLOQUE_BANCO, desde=DESDE, feat=feat)\n'),

    # --- C.4 bis, incorporar la elegida -------------------------------------
    (36,
     '    ensayo = lb.evalua(CONJUNTO, FAMILIA, nombre=NOMBRE_NUEVO, desde=DESDE, feat=feat)\n',
     '    ensayo = lb.evalua(CONJUNTO, FAMILIA, nombre=NOMBRE_NUEVO,\n'
     '                       evaluar=BLOQUE_BANCO, desde=DESDE, feat=feat)\n'),

    # --- C.6, las cuatro familias -------------------------------------------
    (40,
     'fam = lb.por_familia(CONJUNTO, desde=DESDE, feat=feat)\n',
     'fam = lb.por_familia(CONJUNTO, evaluar=BLOQUE_BANCO, desde=DESDE, feat=feat)\n'),

    # --- C.1, la convención que lo decía al revés ---------------------------
    (25,
     '- **Tantea con `desde=`, decide con el histórico completo.**\n',
     '- **Tantea con `desde=`, decide con el bloque de selección entero.** Lo que no se\n'
     '  hace es tantear sobre el histórico completo: `BLOQUE_BANCO` fija todos los\n'
     '  ensayos de esta parte en 2019/20-2021/22, porque 2022/23-2024/25 es el bloque\n'
     '  que C.10 puntúa una sola vez y mirarlo aquí lo gastaría (C.8).\n'),
]


def main() -> int:
    nb = json.loads(RUTA.read_text(encoding="utf-8"))
    for idx, viejo, nuevo in PARCHES:
        src = "".join(nb["cells"][idx]["source"])
        if nuevo in src:
            print(f"celda {idx}: ya parcheada, se salta")
            continue
        if src.count(viejo) != 1:
            raise SystemExit(
                f"celda {idx}: el ancla aparece {src.count(viejo)} veces, no 1. "
                f"El cuaderno ha cambiado; no se toca nada.\n--- ancla ---\n{viejo}")
        nb["cells"][idx]["source"] = (src.replace(viejo, nuevo)).splitlines(keepends=True)
        print(f"celda {idx}: parcheada")
    # Sin salto final: el fichero original no lo lleva, y añadirlo convertiría un
    # parche de ocho celdas en un diff de todo el cuaderno.
    RUTA.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nescrito {RUTA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
