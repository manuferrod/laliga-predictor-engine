"""Inserta C.9.5, C.9.6 y C.9.7 en el cuaderno 05, entre C.9.4 y C.10.

C.9 a C.9.4 documentan nueve rutas de búsqueda, y el README y `seleccion.py` documentan
once más la pasada que puntúa el acervo de conjuntos; las tres secciones que cierran esa
cuenta en el cuaderno —regla 9, los `.py`, los notebooks y el README dicen lo mismo— son
la décima ruta (`_censo.py`), la undécima (`_rodante.py`) y el máximo sobre el censo de
nueve (`_nueve.py`). Van entre C.9.4 y C.10 porque C.10 es ya la cifra citable.

El código de las tres celdas se importa de `_nbceldas4.py`, que es donde se ejecuta
contra los artefactos reales antes de insertarlo; así lo que entra al cuaderno es
literalmente lo que se probó, y no una copia editada a mano. Se ejecuta una sola vez con
`python _nbins4.py` y es idempotente: si las secciones ya están, no hace nada.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_RAIZ = Path(__file__).resolve().parent
if str(_RAIZ) not in sys.path:
    sys.path.insert(0, str(_RAIZ))

from _nbceldas4 import CELDA_95, CELDA_96, CELDA_97     # noqa: E402

NB = _RAIZ / "notebooks" / "05_MODELADO_Y_EVALUACION.ipynb"

MD_95 = """### C.9.5 · La décima ruta: partir el censo, y el mayor salto del proyecto

Las nueve rutas anteriores preguntan por el **RPS**, que es la métrica con la que se
construyó el banco. El 20-oct-2026 Manuel cambió el criterio —«el modelo que voy a
utilizar va a ser el que mejore el accuracy y el ROI al máximo… olvídate del RPS para
encontrar el mejor modelo»— y esta es la primera ruta que lo obedece.

En su forma literal la pregunta es «maximiza el acierto y el ROI sobre todas las
temporadas disponibles», y contestada así se contesta mal: la tabla que sale enumera en
su campo `temporadas` las seis mismas sobre las que reporta, de modo que **elige y mide
en los mismos 2280 partidos**. Sus dos ganadores —55.00 % de acierto con nueve variables
y +4.56 % de ROI con diez— son incitables por la regla 5, y el optimismo crece con el
número de configuraciones probadas, que allí fueron miles.

La única forma de cerrar ese hueco es **partir el censo**: elegir en `seleccion` (1140
partidos) y medir en `medicion` (1140) y en `confirmacion` (760, con el arranque abierto
a 2016/17 sola, como hace `seleccion.confirma()`). Ninguno de los 1900 de fuera
participó en elegir nada y los 760 son anteriores a todo, así que la regla 6 queda en
pie. Lo que esto permite y la pasada literal no: si un conjunto bate a la semilla en
acierto y en ROI **en las dos ventanas de fuera**, su cifra es citable y es el modelo del
TFM.

**Dentro del bloque que eligió, el salto es el mayor medido en todo el proyecto.** El
ganador por acierto —siete variables, `C=0.3, L2`— sube de 52.46 % a **54.30 %** y de
+2.20 % a **+9.50 %** de ROI a la mejor cuota. Visto solo, parece el hallazgo del
trabajo.

**Fuera se parte en dos**, que es la firma de haber escogido el máximo de una nube:

| ventana | n | acierto contra la semilla | ROI máx contra la semilla |
|---|---|---|---|
| `medicion` (no eligió) | 1140 | +0.09 pp [−1.23, +1.41] | +1.55 pp [−2.32, +5.42] |
| `confirmacion` (no eligió) | 760 | **−1.05 pp** [−2.64, +0.54] | **−1.82 pp** [−6.19, +2.54] |
| las dos juntas | 1900 | **−0.37 pp** [−1.38, +0.65] | +0.20 pp [−2.71, +3.11] |

Gana en una ventana y pierde en la otra, y sobre los 1900 juntos el acierto queda por
debajo de la semilla. **El signo no repite: se descarta, y es la décima ruta nula.**

Y hay un daño que sí es significativo, el único de esta sección: **el RPS empeora en las
tres ventanas con los tres intervalos excluyendo el cero** (+0.001500 en `medicion`,
+0.002828 en `confirmacion`, +0.002031 en las dos juntas). Comparado con la mezcla de
C.9.4 —que pierde 0.000527 de RPS sin significación y cuyo ROI sí repite signo fuera—,
este conjunto de siete es **estrictamente peor en las dos cosas a la vez**.

La pista estaba en su propia estructura, y sirve de detector para la próxima vez: de las
tres columnas de precio retiene **solo `mkt_p_away`**. Tirar dos tercios de lo único que
el proyecto ha medido como señal fiable, para ganar acierto justo en el bloque donde se
elige, es lo que hace un conjunto ajustado al bloque.

> Las cifras de dentro de `seleccion` no son citables (regla 5); las de `medicion` y
> `confirmacion` sí, y son las que descartan la ruta. Lo producen `_maximo.py` y
> `_censo.py`, y lo dejan en `maximo*.csv` y `censo*.csv` bajo `outputs/`, que no se
> versiona. Las rutas las declara `seleccion.py`, para que este cuaderno no las lleve
> escritas a mano."""

MD_96 = """### C.9.6 · La undécima ruta: elegir un procedimiento y no un conjunto

Las diez rutas anteriores comparten una limitación de cobertura: todas eligen con
2019/20–2021/22 y puntúan fuera, de modo que **dejan tres temporadas sin cifra**. Manuel
pidió «el mejor modelo que mejore las métricas de ROI y accuracy en todo el histórico de
partidos desde 17/18 hasta hoy sin excepción», y las dos mitades de esa petición se
estorban: ampliar el bloque de elección arreglaría la cobertura y rompería la regla 5.

Solo son compatibles **eligiendo un procedimiento y no un conjunto**. Para cada temporada
S se repite la búsqueda entera **con solo partidos anteriores a S**, y con lo que salga se
predice S. El conjunto cambia con el origen —eso es el punto, no un defecto—; lo que se
fija y se juzga es el procedimiento. Son nueve orígenes por dos criterios de búsqueda
—`accuracy` y `ventaja`, los dos que la regla 4 ampliada deja arbitrar—, 4.2 h de
cómputo.

Un detalle del arranque que conviene saber antes de leer la tabla: los orígenes de
2017/18 y 2018/19 tienen 0 y 380 partidos de pasado, por debajo del suelo declarado de
760, así que **esas dos temporadas se predicen con la semilla** en las dos pistas. No es
una concesión: es lo que significa elegir con el pasado cuando todavía no hay pasado.

Cuatro pistas sobre el mismo censo, con el listón al lado. Las cifras son las del **censo
limpio** —las seis temporadas que no eligieron el peso de la mezcla publicada, 1978
partidos, el único censo donde las cuatro columnas se comparan—:

| pista | acierto | RPS | ROI b365 | ventaja sobre el listón |
|---|---|---|---|---|
| listón (favorito de b365) | 53.54 % | — | −2.78 % | — |
| semilla (3 de precio) | 53.44 % | 0.193962 | −2.97 % | −0.19 pp [−1.53, +1.20] |
| publicado (mezcla, 62) | 53.84 % | 0.194437 | −1.60 % | **+1.18 pp** [−0.84, +3.33] |
| rodante por acierto | 53.49 % | 0.194118 | −2.64 % | +0.14 pp [−1.64, +1.93] |
| rodante por ventaja | 53.59 % | 0.194335 | −1.96 % | +0.82 pp [−1.35, +3.01] |

**Ni un intervalo excluye el cero, ninguno de los cuatro**, y sobre los 3118 enteros
tampoco: el mayor es el de la mezcla publicada, +1.52 pp [−0.19, +3.31]. De las doce
comparaciones emparejadas entre pistas —seis pares en los dos censos— **solo una excluye
el cero**, `publicado` contra `semilla` sobre los 3118, y en el censo limpio ya no.

Esa es la respuesta honesta a lo que se pidió: **cubiertas las nueve temporadas sin
excepción, ninguna ruta del proyecto produce una ventaja distinguible de cero contra el
favorito de Bet365, y la mejor de las cuatro sigue siendo la mezcla que ya se publica.**
Lo que esta ruta aporta no es un modelo mejor —pierde, y además no hay nada que ganar—:
es la primera cifra del trabajo que **cubre las nueve temporadas sin que ninguna haya
participado en elegir lo que la predice**, y por tanto la primera citable entera.

Tres cosas del cuadro que no hay que volver a medir, y la primera es una predicción
fallada:

- **Esperaba que la pista de ventaja quedara por debajo de la de acierto, y queda por
  encima.** El argumento era que su tolerancia es dos o tres veces más ancha —0.0068
  contra 0.0025 en el origen de 2020/21— y compra ruido. Adopta más profundo, sí (hasta
  nueve variables en 2022/23, frente a las cuatro de la otra pista), pero da +0.82 pp
  contra +0.14 y más acierto. **Lo que cuesta está en el RPS**: 0.194335 contra 0.194118,
  0.000217 peor, que es exactamente lo que la regla 4 obliga a seguir reportando aunque
  ya no arbitre.
- **Las dos pistas rodantes difieren en 91 partidos de 3118**, el 2.9 %; emparejadas, la
  diferencia es +0.36 pp [−1.09, +1.79]. Las cuatro coinciden en el signo en más del
  95 % de los partidos, así que las cifras marginales de la tabla **exageran las
  distancias**, y la comparación que vale es la emparejada.
- **La celda más bonita es la única incitable**: la pista de acierto en 2026/27 da
  +6.18 % de ROI y +8.21 pp de ventaja, y son 78 partidos con un error típico de
  10.93 pp. Está a **0.75 errores típicos de cero**.

Y un hallazgo lateral que va contra la directriz que lo produjo: **el orden de las
candidatas por RPS es más estable entre orígenes que el orden por acierto**, y la brecha
**se abre con la distancia**, que es lo que lo hace un hallazgo y no una curiosidad. Rho
de Spearman 0.78 contra 0.69 promediando los 21 pares del criterio de acierto, y **0.588
contra 0.371** en el par más lejano, 2019/20 contra 2026/27. No cambia quién elige, que
lo fijó Manuel, pero es el mecanismo por el que una búsqueda guiada por acierto compra
daño en RPS sin anunciarlo.

> Lo producen `_rodante.py` y `_estabilidad.py`, y lo dejan en `rodante_*.csv/json` y
> `estabilidad*.csv` bajo `outputs/`, que no se versiona. Las rutas las declara
> `seleccion.py`."""

MD_97 = """### C.9.7 · El máximo por ROI y acierto sobre el censo de nueve

Con el censo ya fijado —2017/18–2024/25 y 2026/27, **3118 partidos en 345 pliegues**— se
puede hacer la pregunta en su forma literal: «el mejor modelo en términos de roi y
accuracy… que maximice esas métricas tanto en cómputo global como a nivel de temporada».
Esta pasada **no es una ruta más: no busca nada, puntúa.** Mide los **50 conjuntos
distintos** que han producido las once rutas anteriores sobre los **mismos** 3118
partidos, temporada a temporada.

El resultado es que **no hay conjunto que adoptar**, y las tres lecturas lo dicen con
tres fuerzas distintas.

**Lectura 1, el global.** El máximo lo firma `maximo_roi` (k=10): acierto 54.11 %, ROI
+1.35 %, ventaja **+4.22 pp [+1.72, +6.77]** sobre el listón de los 3118 (−2.87 % y
53.11 %). Ese intervalo excluye el cero, y **es exactamente por eso por lo que no es
citable**: ese conjunto se buscó sobre partidos que están dentro de estos 3118, así que
la cifra arrastra el defecto que C.9.5 diagnosticó en vez de repararlo. El publicado de
62 queda en 53.50 % y +1.52 pp [−0.19, +3.31].

Del mismo cuadro sale un aviso que hay que leer con cuidado, porque **meter el banco
entero cuesta acierto y RPS y no cuesta ventaja**: `todas65` cae a 50.58 % de acierto y
0.202736 de RPS, y `juego59` a 50.03 % y 0.204590, pero sus ventajas sobre el listón
siguen saliendo positivas (+1.66 y +0.25 pp) y la peor de las 50 es `semilla+fuerza` con
−0.93 pp. El ROI plano sobre 3118 partidos tiene un error típico de 1.79 pp y **no
distingue un modelo que acierta el 50 % de uno que acierta el 53.5 %**. El daño existe y
está medido; quien lo mide es el acierto, no el dinero — y de ahí que la directriz pida
dos métricas y haya que seguir reportando las tres.

**Lectura 2, por temporada: no hay respuesta que dar, y eso es un hallazgo y no una
evasiva.** Las nueve temporadas las firman **nueve conjuntos distintos, en los dos
criterios, y ninguno repite**. El puesto medio del ganador en las otras ocho va de 12.2 a
40.6 de 50 por acierto: `escalera+clasificacion` gana 2023/24 con .5605 y promedia .5110
fuera. Nueve máximos sobre 380 partidos con un error típico de 5.0 pp son nueve sorteos.
No existe «el conjunto que maximiza cada temporada», existe el que ganó ahí.

**Lectura 3, dejar una fuera: la única citable, y no excluye el cero.** Se elige con ocho
temporadas y se puntúa en la novena, que no participó en elegir. Guiando por acierto
salen 4 conjuntos distintos y **53.30 % contra 53.11 %** del listón, con ventaja
**+1.46 pp [−0.42, +3.34]**; guiando por ventaja, 3 conjuntos y 53.11 % contra 53.11 %
—el mismo acierto al decimal— con **+1.45 pp [−0.84, +3.69]**. **Los dos quedan por
debajo del +1.52 pp del modelo publicado**, y los vaivenes por vuelta dicen de dónde sale
la media: guiando por ventaja, +8.41 pp en 2019/20 contra −4.55 pp en 2026/27.

Un aviso sobre el censo llamado **limpio**, que son las seis temporadas que no eligieron
el peso de la mezcla: **es censo limpio para el publicado y para la semilla, y no lo es
para ningún conjunto de búsqueda.** `maximo_roi` se buscó sobre 2019/20–2024/25 y pisa
tres de las seis; `rod_ve_202425`, que ahí firma el máximo con +3.61 pp [+1.35, +5.97],
se eligió con el pasado de 2024/25, que contiene cuatro de las seis. Ese +3.61 **no es
una cifra de fuera de muestra**.

Y la voraz con los 3118 **dentro** del bloque de elección —el máximo de información que
el proyecto puede darle— **se agota en dos variables sobre la semilla**: para a k=5 por
los dos criterios, y en la tolerancia y no en el techo. Por acierto, la mejor candidata
del paso 3 (`form_ga_ewm_diff`) mejoraba +0.000000 contra una tolerancia de 0.00153093.

> La lectura citable es la 3; las lecturas 1 y 2 no lo son (regla 5) y la celda lo
> imprime en su propia salida, no solo aquí arriba. Lo produce `_nueve.py` y lo deja en
> `nueve_*.csv/json` bajo `outputs/`, que no se versiona."""


def _md(texto: str) -> dict:
    return {"cell_type": "markdown", "metadata": {},
            "source": _lineas(texto)}


def _code(texto: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "execution_count": None,
            "outputs": [], "source": _lineas(texto.strip("\n"))}


def _lineas(texto: str) -> list[str]:
    """Una lista de líneas con `\\n` final salvo en la última, como las del cuaderno."""
    part = texto.split("\n")
    return [ln + "\n" for ln in part[:-1]] + [part[-1]]


def main() -> int:
    nb = json.loads(NB.read_text(encoding="utf-8"))
    hay = [i for i, c in enumerate(nb["cells"])
           if c["cell_type"] == "markdown" and "C.9.5" in "".join(c["source"])]
    if hay:
        print(f"C.9.5 ya está en la celda {hay[0]}: no hago nada")
        return 0

    # Detrás de la celda de código de C.9.4 y delante de la de C.10, que es donde
    # cae por orden de relato.
    anclas = [i for i, c in enumerate(nb["cells"])
              if c["cell_type"] == "markdown" and "## C.10 " in "".join(c["source"])]
    if len(anclas) != 1:
        print(f"esperaba una sola cabecera de C.10 y hay {len(anclas)}")
        return 1
    corte = anclas[0]

    nuevas = [_md(MD_95), _code(CELDA_95), _md(MD_96), _code(CELDA_96),
              _md(MD_97), _code(CELDA_97)]
    nb["cells"] = nb["cells"][:corte] + nuevas + nb["cells"][corte:]
    NB.write_text(json.dumps(nb, ensure_ascii=False, indent=1) + "\n",
                  encoding="utf-8")
    print(f"insertadas {len(nuevas)} celdas en {corte}; el cuaderno pasa a "
          f"{len(nb['cells'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
