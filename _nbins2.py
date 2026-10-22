"""Inserta C.9.2 —el modelo sin precio— detrás de C.9.1 en el cuaderno 05."""
from __future__ import annotations

import json
from pathlib import Path

RUTA = Path(__file__).resolve().parent / "notebooks" / "05_MODELADO_Y_EVALUACION.ipynb"

MD = r"""### C.9.2 · ¿Y miden algo, las otras? El modelo sin precio

Todo lo de C.9 y C.9.1 mide las variables **como añadido** al precio: la búsqueda parte
de la semilla de mercado y pregunta qué suma cada candidata. El resultado fue nulo, y un
resultado nulo así admite dos lecturas que no se parecen en nada:

1. las variables de juego de este banco no miden casi nada; o
2. miden mucho, y el precio ya lo ha incorporado antes de que el partido empiece.

La diferencia importa, y no es retórica: la primera lectura dice que el trabajo de
extracción y de variables fue en balde, y la segunda dice que midió bien algo que el
mercado también mide, lo cual es un hallazgo sobre el mercado y no un fracaso del banco.
Las dos lecturas son indistinguibles mientras el precio esté dentro del modelo, porque
condicionar por el precio es exactamente lo que las confunde.

Se separan quitándoselo. Se ajustan las **59 variables de juego sin ninguna columna de
precio**, sobre el bloque de selección, con dos referencias que acotan el problema por
arriba y por abajo: la **previa de frecuencias** —la distribución de clases de la propia
ventana, que no mira ni quién juega— como suelo, y la **semilla de mercado** como techo.
La columna `camino` sitúa cada conjunto en ese recorrido: 0 es la previa, 1 es la
semilla.

| conjunto | k | RPS | acierto | ROI | `camino` |
|---|---|---|---|---|---|
| semilla de mercado | 3 | **0.193174** | 52.46 % | −0.50 % | 1.000 |
| dominio de mercado entero | 6 | 0.193623 | 52.28 % | −3.15 % | 0.985 |
| **solo `fuerza`** | 9 | **0.198001** | 51.75 % | −3.80 % | **0.841** |
| las 65 juntas | 65 | 0.200669 | 50.18 % | −2.34 % | 0.753 |
| **las 59 de juego, sin precio** | 59 | **0.202853** | 49.65 % | −4.16 % | **0.681** |
| solo `forma` | 26 | 0.202919 | 49.39 % | −12.38 % | 0.679 |
| solo `clasificacion` | 7 | 0.207586 | 48.16 % | −9.34 % | 0.525 |
| solo `historial` | 3 | 0.213901 | 46.49 % | −13.80 % | 0.317 |
| solo `fatiga` | 6 | 0.219168 | 44.74 % | −10.40 % | 0.143 |
| solo `bajas` | 4 | 0.223182 | 43.51 % | −10.10 % | 0.011 |
| previa de frecuencias | — | 0.223518 | 43.60 % | — | 0.000 |
| solo `contexto` | 4 | 0.224266 | 43.60 % | −7.18 % | −0.025 |

**La segunda lectura es la correcta.** Las 59 variables de juego, sin ver un solo precio,
recorren el **68.1 %** del camino que separa la previa de frecuencias del mercado
(0.020665 de los 0.030344 de hueco). Y el acierto, que es una métrica independiente y
mucho más basta, dice **68.3 %** —6.05 de los 8.86 puntos porcentuales—. Que dos
métricas que no comparten construcción coincidan en la primera décima es, en este
cuaderno, la coincidencia más tranquilizadora que se ha encontrado: el banco de variables
contiene de verdad dos tercios de la información con la que el mercado fija su precio, y
la reconstruye desde cero, sin verlo.

Lo que no contiene es el tercio que falta, y ahí la respuesta honesta es que el proyecto
no puede tenerlo: son alineaciones confirmadas, noticias de vestuario, y el dinero de los
propios apostantes, que es el mecanismo por el que el precio agrega información privada
que ninguna fuente pública publica. El techo de las variables públicas está, en este
banco, dos tercios del camino.

**Y hay un segundo hallazgo, que cambia la interpretación de C.9.1.** El dominio de
`fuerza` solo —nueve columnas, Elo y valor de plantilla— llega al **0.841 del camino con
RPS 0.198001, y bate a las 59 juntas** (0.202853). Añadir las otras cincuenta no mejora:
empeora. Ese 68.1 % no es, por tanto, el techo del banco de juego, sino el techo de un
**ajuste de 59 variables** sobre 1140 partidos: lo que sobra es varianza. La lectura
mecánica de por qué la voraz no encontró nada queda entonces completa, y no es que no
hubiera señal —la hay, y mucha— sino que **cada variable que entra cuesta más varianza de
la que trae de señal nueva**, porque la señal que trae ya estaba en el precio y la
varianza es toda suya.

La fila de `todo` lo dice del modo más crudo: las 65 juntas dan **0.200669**, que es
**0.0075 peor que las tres del precio solas**. Cincuenta y nueve columnas de juego,
añadidas a un precio que ya acierta, no solo no aportan: ahogan lo que el precio sabía.
Con eso, la escalera de C.9 y la parada de la búsqueda dejan de ser un resultado
decepcionante y pasan a ser el resultado correcto.

La escalera por dominios, por último, se ordena como ordenaría cualquiera que entienda de
fútbol, y eso es una comprobación de cordura del pipeline entero: `fuerza` (0.841) por
delante de `forma` (0.679), después la `clasificacion` (0.525), el `historial` directo
(0.317), la `fatiga` (0.143), y al final `bajas` (0.011) y `contexto` (−0.025), que por sí
solos **no se distinguen de no saber nada**. Que `bajas` esté pegado al suelo encaja con
lo que la cuarta puerta ya había rechazado: `out_minshare_max_diff`, una de las dos
variables que la voraz adoptó y la confirmación devolvió, es justamente de ese dominio.

El dinero no rescata a ninguno: todos los modelos sin precio pierden, de −4.16 % a
−13.80 %, contra el −2.18 % del listón de Bet365 en este bloque. Solo los dos que llevan
precio dentro se acercan a no perder. Es la regla 4 otra vez —manda el RPS— pero esta vez
las dos métricas apuntan al mismo sitio.

> Ninguna cifra de esta sección es citable (regla 5): sale del bloque de selección, que
> es donde se decide. Lo que esta sección aporta a la memoria no es una cifra sino la
> **interpretación** del resultado negativo de C.9, y esa interpretación ya no es una
> conjetura razonable sino una medida. La produce `_sinprecio.py` y la deja en
> `seleccion.SINPRECIO`.
"""

CODE = r'''# Regla 8: la tabla se lee de disco, no se recalcula. La produjo `_sinprecio.py` con
# doce protocolos completos sobre el bloque de selección, y vive bajo `outputs/`, que no
# se versiona: si no está, la celda lo dice y el texto de arriba conserva las cifras.
if not sl.SINPRECIO.exists():
    print(f"sin descomposicion en {sl.SINPRECIO}")
else:
    sp = pd.read_csv(sl.SINPRECIO).sort_values("camino", ascending=False)

    suelo = sp.loc[sp["conjunto"] == "previa_frecuencias"].iloc[0]
    techo = sp.loc[sp["conjunto"] == "semilla_mercado"].iloc[0]
    juego = sp.loc[sp["conjunto"] == "juego_sin_precio"].iloc[0]
    hueco = float(suelo["rps"]) - float(techo["rps"])
    d_acc = float(techo["accuracy"]) - float(suelo["accuracy"])

    print(f"hueco previa -> semilla      : {hueco:.6f} de RPS "
          f"({100 * d_acc:.2f} pp de acierto)")
    print(f"lo recorren las 59 de juego  : {100 * float(juego['camino']):.1f} % en RPS, "
          f"{100 * (float(juego['accuracy']) - float(suelo['accuracy'])) / d_acc:.1f} %"
          f" en acierto")
    mejor = sp.loc[sp["conjunto"].str.startswith("solo_"), :].iloc[0]
    print(f"mejor dominio por si solo    : {mejor['conjunto'][5:]} "
          f"(k={int(mejor['k'])}, RPS {mejor['rps']:.6f}, "
          f"camino {100 * float(mejor['camino']):.1f} %)"
          f"{'  <- bate a las 59 juntas' if mejor['rps'] < juego['rps'] else ''}")
    todo = sp.loc[sp["conjunto"] == "todo"].iloc[0]
    print(f"las 65 juntas vs la semilla  : {float(todo['rps']) - float(techo['rps']):+.6f}"
          f" de RPS (el precio se ahoga)")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))

    vis = sp[sp["conjunto"] != "previa_frecuencias"]
    col = [PALETA["mercado"] if c in ("semilla_mercado", "mercado_entero")
           else PALETA["modelo"] if c in ("juego_sin_precio", "todo")
           else PALETA["apoyo"] for c in vis["conjunto"]]
    ax1.barh(vis["conjunto"], vis["camino"], color=col)
    ax1.axvline(0, color="black", lw=1)
    ax1.axvline(1, color=PALETA["mercado"], lw=1, ls="--")
    ax1.set_xlabel("camino recorrido entre la previa (0) y la semilla de mercado (1)")
    ax1.set_title("Cuanto del precio reconstruye cada bloque, sin verlo")
    ax1.invert_yaxis()

    # La misma lectura con el acierto, que no comparte construccion con el RPS. Si las
    # dos nubes caen sobre la diagonal, el orden no es un artefacto de la metrica.
    acc_cam = (vis["accuracy"] - float(suelo["accuracy"])) / d_acc
    ax2.scatter(vis["camino"], acc_cam, color=PALETA["modelo"], s=55, zorder=3)
    lim = [min(-0.1, float(vis["camino"].min())), 1.08]
    ax2.plot(lim, lim, color=PALETA["apoyo"], lw=1, ls="--", zorder=1)
    for _, r in vis.iterrows():
        ax2.annotate(str(r["conjunto"]).replace("solo_", ""),
                     (r["camino"], (r["accuracy"] - float(suelo["accuracy"])) / d_acc),
                     fontsize=7.5, xytext=(4, -3), textcoords="offset points")
    ax2.set_xlabel("camino segun RPS")
    ax2.set_ylabel("camino segun acierto")
    ax2.set_title("Dos metricas independientes, el mismo orden")
    plt.tight_layout(); plt.show()
'''


def main() -> int:
    nb = json.loads(RUTA.read_text(encoding="utf-8"))
    cells = nb["cells"]

    # Ancla: C.9.1 es markdown en 51 y su celda de codigo en 52; C.10 abre en 53.
    assert "C.9.1" in "".join(cells[51]["source"]), "51 no es C.9.1"
    assert "CAMINO_65" in "".join(cells[52]["source"]), "52 no es el codigo de C.9.1"
    assert "C.10" in "".join(cells[53]["source"]), "53 no es C.10"
    if any("C.9.2" in "".join(c["source"]) for c in cells):
        print("C.9.2 ya existe, no se toca nada")
        return 0

    nuevas = [
        {"cell_type": "markdown", "metadata": {}, "source": MD.splitlines(keepends=True)},
        {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
         "source": CODE.splitlines(keepends=True)},
    ]
    cells[53:53] = nuevas
    RUTA.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"insertadas 2 celdas en 53-54; el cuaderno tiene ahora {len(cells)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
