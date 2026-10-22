"""Origen rodante: elige con el pasado de cada temporada y puntúa esa temporada.

LA PREGUNTA QUE CONTESTA, Y POR QUÉ NINGUNA DE LAS DIEZ RUTAS ANTERIORES LA CONTESTA
====================================================================================
El encargo del 20-oct-2026 es literal: «el mejor modelo que mejore las métricas de
accuracy y ROI en todo el histórico de partidos desde 17/18 hasta hoy sin excepción»,
y «que siga prediciendo partidos jornada a jornada». Son dos exigencias a la vez y las
diez rutas anteriores no pueden cumplir las dos:

  · `_censo.py`, `_maximo.py`, `_bloques.py`, `_mejores.py` y la voraz de
    `seleccion.adelante` eligen UN conjunto con 2019/20–2021/22 y lo puntúan donde no
    eligieron. Eso es honesto, pero deja 2017/18, 2018/19 y 2026/27 fuera de la cifra
    y no responde a «todo el histórico sin excepción».
  · Ampliar el bloque de elección a las ocho temporadas arreglaría la cobertura y
    rompería lo otro: elegir y medir sobre los mismos partidos es exactamente lo que
    prohíbe la regla 5 del proyecto, y lo que convirtió a `_maximo.py` en incitable.

Las dos exigencias sí son compatibles, pero no eligiendo un conjunto: eligiendo un
**procedimiento**. El origen rodante es el estándar de la predicción de series
temporales y aquí se lee solo: para cada temporada S, se vuelve a correr la búsqueda
**usando únicamente partidos anteriores a S**, y con lo que salga se predice S. La
temporada que se puntúa no participó jamás en elegir lo que la predice, de modo que
las nueve lecturas son todas de fuera de muestra y se pueden sumar. Y el objeto medido
es el mismo que luego se ejecuta cada jornada —«vuelve a elegir con lo que sabías»—, no
un conjunto congelado que ya no se podría defender la temporada que viene.

El precio de la ruta, dicho antes de verla: lo que se mide no es un conjunto sino una
regla, así que si gana, lo que se publica es la regla. Y el conjunto puede cambiar de
una temporada a otra; eso no es un defecto del método sino la mitad de su contenido,
porque si cambia mucho la propia inestabilidad es el resultado.

ESA MITAD, YA MEDIDA, Y NO DICE LO QUE YO ESPERABA
==================================================
Cambia mucho: **once variables distintas en catorce adopciones** sobre los siete
orígenes que eligen, y la **intersección de los siete está vacía** —nada más allá de la
semilla de precio declarada sobrevive a que se le pregunte siete veces—. Solo se repiten
dos, `home_matches_left` (3 de 7) y `home_post_big_win` (2 de 7).

Leí eso como la firma del ruido y **me equivoqué**; lo contesta `_estabilidad.py`, que
compara el ORDEN de las 60 candidatas del paso 1 entre orígenes en vez de solo la
ganadora. El orden se repite: **rho de Spearman +0.677 de mediana en acierto** sobre los
21 pares, los 21 positivos, mínimo +0.342, y la ganadora de cada origen suele caer entre
las diez primeras de los demás (`home_matches_left` es 1ª a 4ª en cuatro de seis).
Debajo hay una ordenación estable, no una lotería. Lo que vacía la intersección no es
que cada origen encuentre una cosa al azar, es que **arriba hay un grupo de variables
casi empatadas y la voraz corta justo donde empatan**: cada una captura el mismo resto,
así que entrada una, las demás ya no aportan —exactamente lo que midieron `_bloques.py`
y `_mejores.py` por otra vía—.

Con un control que impide leer esa rho como reproducibilidad, y que es el que casi me
hace firmar la conclusión contraria: **los orígenes no son muestras independientes.** La
ventana es expansiva, así que la de 2019/20 está contenida en la de 2024/25, y la rho
**sigue al solape** y no a la distancia en sí: +0.872 entre vecinos (82 % de partidos
compartidos) cayendo monótonamente a **+0.371 en el par más lejano** (2019/20 contra
2026/27, 25 % compartido). La lectura limpia es esa última: positiva y modesta.

Y un hallazgo lateral que hay que decir en voz alta porque va contra la directriz que
estoy ejecutando: **el orden por RPS es más estable que el orden por acierto en los seis
gaps, y la brecha se abre con la distancia** (+0.028 entre vecinos, +0.216 en el par más
lejano: +0.588 contra +0.371). El acierto ordena las candidatas peor que la métrica a la
que el 20-oct se le quitó el papel de elegir. **No cambia quién elige** —el criterio lo
fijó Manuel y es suyo— pero es el mecanismo concreto por el que una búsqueda guiada por
acierto puede comprar daño en RPS sin anunciarlo, que es justo por lo que la regla 4
obliga a seguir reportándolo en cada tabla.

Tercer hallazgo, del mismo guion: **acierto y ventaja eligen casi lo mismo** —idéntico
conjunto en 2 de los 3 orígenes donde ya hay las dos pasadas—. Era previsible y conviene
declararlo antes de presentar las dos pistas: con `politica="favorito"` se apuesta una
unidad plana al argmax del modelo, así que la ventaja es el acierto reponderado por la
cuota. **No son dos confirmaciones independientes: son dos lecturas de lo mismo.**

EL REPARTO, TEMPORADA A TEMPORADA
=================================
`protocol` ya tiene todo lo que hace falta y no se toca ni una línea. El bloque de
elección es `dev & season < corte_seleccion`, así que mover `corte_seleccion` a S es
literalmente «elige con el pasado de S». Con el arranque bajado a 2016/17 sola
—la única temporada que de verdad lo necesita: cobertura media 88.00 %, contra 96.42 %
y 96.69 % de 2017/18 y 2018/19— el universo evaluable queda así, medido:

    temporada   pliegues  partidos   pasado evaluable   ¿elige?
     2017/18       40        380            0            no, semilla declarada
     2018/19       40        380          380            no, semilla declarada
     2019/20       40        380          760            sí
     2020/21       47        380         1140            sí
     2021/22       43        380         1520            sí
     2022/23       39        380         1900            sí
     2023/24       42        380         2280            sí
     2024/25       44        380         2660            sí
     2025/26        —          —            —            HOLDOUT, no se toca
     2026/27       10         78         3040            sí
    total                   3118

Las dos primeras no eligen nada, y el motivo no es la pereza sino que no hay con qué:
con 0 y 380 partidos de pasado, el error típico de la diferencia emparejada en acierto
pasa de 2.5e-3 a 4.3e-3 (escalado por la raíz desde los 1140 en los que se midió), es
decir que la tolerancia se le come cualquier mejora real. Se predicen con la **semilla
de mercado declarada** (`seleccion.SEMILLA_MERCADO`), que no es una elección hecha con
sus partidos sino una decisión escrita de antemano, y por tanto no mira hacia adelante.
Lo que NO se hace es predecirlas con el conjunto publicado hoy: su peso se eligió con
2019/20–2021/22, que son posteriores, y usarlo aquí sería mirar hacia adelante.

2026/27 se puntúa con la lista de pliegues de `produccion` y no con la de desarrollo.
`lb.evalua` se niega a mirar ese bloque, y hace bien —ahí no se decide nada—, así que la
puntuación va por `protocol.run_protocol` directamente, que es la función de lectura.

EL CRITERIO, Y LO QUE CUESTA CADA EVALUACIÓN
============================================
Manuel levantó la regla 4 el 19-oct-2026 y el 20-oct extendió el alcance a la búsqueda:
«olvídate del RPS para encontrar el mejor modelo». Se recorren por tanto los dos
criterios que pidió, cada uno con su propia pasada completa y su propia tolerancia
medida (`seleccion.TOLERANCIAS`, escalada a los partidos del bloque de cada origen):

    rodante_ac   adopta por ACIERTO    tolerancia 2.5e-3 sobre 1140 partidos
    rodante_roi  adopta por VENTAJA    tolerancia 6.8e-3 sobre 1140 partidos

La ventaja es el ROI de Manuel menos el del listón: una unidad plana al signo que el
modelo señala, en todos los partidos con precio, sin umbral
(`seleccion.POLITICA_SELECCION = "favorito"`), contra el apostante que sigue al
favorito de Bet365. Con política de valor el denominador se movería con cada columna
que entra y los conjuntos dejarían de compararse sobre los mismos partidos.

El coste está medido antes de diseñar, no estimado, y la primera cuenta que hice era
mala por un factor de tres: la escribí suponiendo que el reparto entre procesos escala
con los núcleos y no escala. Lo que mide `_paralelo.py` sobre doce candidatas del
bloque de 2019/20 —ochenta pliegues— es que una candidata cuesta **4.0 s en el padre
caliente** y **1.42 s repartida entre seis procesos**, un reparto de **2.8x**; con diez
procesos sale igual (1.43 s) y con cuatro peor (1.60 s), porque cada hijo sostiene su
propia matriz y su propia caché de estados y la memoria libre no da para doce copias.
El arranque de los hijos se paga una vez y no por paso —el mismo lote cuesta 29.8 s la
primera vez y 17.1 s la segunda, porque `loky` los conserva—, y envolver la búsqueda en
un `parallel_backend` persistente no recupera nada (16.8 s contra 17.1 s), así que aquí
no hay contexto ninguno.

Con eso el presupuesto sale así: 0.0178 s por pliegue y candidata repartido, los siete
bloques de elección suman **1432 pliegues**, y un paso son 59 candidatas, de modo que un
paso sobre los siete orígenes cuesta **~25 min** y el tope de doce pasos pone el peor
caso en **5 h por criterio, 10 h las dos**. Es el peor caso y no lo esperado: la voraz
se detiene cuando ninguna candidata pasa las puertas, y con el RPS como criterio se
detuvo en cinco variables. Esas horas son lo que la directiva del 20-oct autoriza
gastar, y el punto de control por origen es lo que hace que una interrupción cueste un
origen y no la búsqueda.

LA TABLA QUE SE CITA
====================
Al final se puntúan cinco pistas sobre **exactamente los mismos 3118 partidos**, con
los mismos pliegues y el mismo estado de preprocesado:

    liston      el favorito de Bet365, unidad plana. El listón de la regla 3.
    semilla     las tres columnas de precio, fijas en las nueve temporadas.
    publicado   la mezcla logit w=0.85 sobre las 62 de `build_models.CONJUNTO`.
                **Su cifra de 2019/20–2021/22 es de dentro de la elección del peso** y
                la tabla lo marca; no es comparable con las otras dos columnas ahí.
    rodante_ac  el origen rodante adoptando por acierto.
    rodante_roi el origen rodante adoptando por ventaja.

La pista `publicado` sirve además de dos cosas que no estaban previstas y que conviene
no volver a medir desde cero:

**Es la comprobación de que esta ruta está cableada al modelo que se publica.** Sobre el
bloque de validación entero da acierto **0.534211 y RPS 0.194369 en 3040 partidos**,
contra el **0.533750 y 0.194494 sobre 3037** que publican `evaluate` y `qa_models`. La
diferencia son tres partidos y tiene causa conocida y escrita: `evaluate.compare` mide
sobre la intersección de los partidos que cubren todas las referencias, y **Pinnacle
cubre 3037 de los 3040**. Si esa diferencia dejara de ser de tres partidos, lo que ha
cambiado no es el censo. Restringida a las seis temporadas que tienen cuota de cierre
—2019/20–2024/25, 2280 partidos— la misma pista da 0.538158 y 0.192254, y ese par es el
que explica el apartado siguiente: no es otra medición, es otra población.

**Y es lo que cuesta el censo de nueve, medido y no estimado.** `cfg_de` baja el arranque
a 2016/17 sola, así que esta pasada puntúa el modelo publicado también en 2017/18 y
2018/19: **acierto 52.24 % y RPS 0.200715 en esos 760 partidos**. Esas dos temporadas son
las que separan el agregado de las seis con cuota de cierre —0.538158 / 0.192254 sobre
2280— del agregado de las ocho, **0.534211 / 0.194369 sobre 3040**: 0.39 pp menos de
acierto y 0.0021 peor de RPS **sin que el modelo cambie en nada**. Las cifras por
temporada no se mueven una centésima, porque la ventana de entrenamiento de un pliegue no
depende de la máscara de evaluable. Lo que baja el agregado es que entran dos temporadas
más difíciles, y difíciles **para el mercado y no para el modelo**: en 2018/19 el favorito
de Bet365 acertó el **48.16 %** y perdió el **14.12 %**, su peor temporada de las nueve, y
ahí el modelo acierta 48.95 %. Dicho de otro modo, el modelo **bate al favorito del
mercado en acierto en 7 de las 9 temporadas**, incluidas las dos que el censo abre; pierde
en 2020/21 (53.16 contra 53.42) y en 2022/23 (53.68 contra 54.74). **El agregado más bajo
hay que presentarlo siempre con esto al lado**, o se lee como una regresión del modelo y
no lo es.

Y el 0.534211 sobre 3040 **sale idéntico** en el acierto de la economía a consenso de la
ficha, que también liquida los 3040, lo que cierra el cableado por el otro extremo: la
ruta, el evaluador y la economía publicada miden el mismo modelo sobre las mismas tres
poblaciones y no se contradicen en ningún decimal.

El ROI se liquida a tres precios en la misma pasada y gratis —Bet365, consenso y mejor
cuota entre casas— porque la palanca del precio vale más que cualquier conjunto medido
en el proyecto. **El eje tiene que ser b365 y no la cuota de cierre promediada**, y eso
no es una preferencia: `odds_close_avg_*` no existe antes de 2019/20, cobertura 0 de
380 en 2016/17, 2017/18 y 2018/19, mientras que b365, consenso y máxima están a 380 de
380 en las nueve. Con el eje de cierre, «todo el histórico sin excepción» perdería tres
temporadas por falta de precio.

EL VEREDICTO, YA MEDIDO (20-oct-2026)
=====================================
Las cuatro pistas sobre el **censo limpio** —las seis temporadas que no eligieron el peso
de `publicado`, 1978 partidos, el único donde las cuatro columnas se comparan entre sí—:

    pista         acierto       RPS   ROI b365   ventaja sobre el listón
    liston         53.54 %        —     -2.78 %
    semilla        53.44 %   0.193962   -2.97 %   -0.19 pp [-1.53, +1.20]
    publicado      53.84 %   0.194437   -1.60 %   +1.18 pp [-0.84, +3.33]
    rodante_ac     53.49 %   0.194118   -2.64 %   +0.14 pp [-1.64, +1.93]
    rodante_roi    53.59 %   0.194335   -1.96 %   +0.82 pp [-1.35, +3.01]

**Ni un intervalo excluye el cero. Ninguno de los cuatro.** Y sobre los 3118 enteros
tampoco: el mayor es el de `publicado`, +1.52 pp [-0.19, +3.31], que se queda a dos
décimas. Esa es la respuesta literal al encargo —«el mejor modelo que mejore accuracy y
ROI en todo el histórico sin excepción»—: cubiertas las nueve temporadas, **ninguna de
las once rutas del proyecto produce una ventaja distinguible de cero contra el favorito
de Bet365**. Lo que esta ruta aporta no es un modelo mejor; es la primera cifra del
trabajo que cubre las nueve temporadas **sin que ninguna haya participado en elegir lo
que la predice**, y por tanto la primera que se puede citar entera.

Cuatro lecturas más del mismo cuadro, y la primera es una corrección mía.

**Predije que `rodante_roi` quedaría por debajo de `rodante_ac`, y queda por encima.** El
razonamiento era que adoptar por ventaja compra ruido, porque su tolerancia es 6.8e-3
contra 2.5e-3 del acierto —casi tres veces más ancha—, así que la puerta deja pasar
candidatas que el acierto habría parado. Deja pasar, sí: adopta más profundo. Pero no
cuesta lo que yo dije. En el censo limpio da **+0.82 pp de ventaja contra +0.14**, y
también más acierto (53.59 % contra 53.49 %); sobre los 3118, +0.82 contra +0.46. Lo que
sí cuesta está donde la regla 4 obliga a seguir mirando aunque ya no arbitre: **el RPS,
0.194335 contra 0.194118, 0.000217 peor**. El criterio hace exactamente lo que se le
pidió —compra ROI y lo paga en RPS—; la mitad de dirección de mi predicción era buena y
la de magnitud estaba al revés.

**Las dos pistas rodantes se diferencian en 91 partidos de 3118**, el 2.9 %, y eso pone
en escala todo lo anterior. Emparejadas sobre los mismos partidos, la diferencia entre
ellas es +0.36 pp [-1.09, +1.79] en ROI y +0.03 pp [-0.48, +0.55] en acierto. De las
**doce comparaciones emparejadas** (seis pares en los dos censos) **solo una excluye el
cero**: `publicado` contra `semilla` sobre los 3118, +1.53 pp [+0.06, +3.03] —y en el
censo limpio ya no, +1.37 pp [-0.30, +3.09]—. Las once restantes son indistinguibles. Las
cifras marginales de la tabla exageran las distancias porque las cuatro pistas coinciden
en el signo en más del 95 % de los partidos; lo que de verdad separa a dos pistas son los
cien partidos en que difieren, y cien partidos no bastan para separar nada.

**El precio sigue valiendo más que el conjunto, y ahora medido en las nueve
temporadas.** La misma pista liquidada a la mejor cuota entre casas en vez de a Bet365
pasa de -2.05 % a **+2.42 %** (`rodante_roi`), y el propio listón pasa de -2.87 % a
+1.93 %: **4.5 puntos de ROI por elegir dónde se apuesta**, contra 1.5 del mejor conjunto
de once rutas de búsqueda. Arriba esa frase estaba escrita como conjetura antes de medir;
medida, se quedaba corta. Lo que la cifra **no** hace es prometer nada: la mejor cuota
entre casas es un precio que hay que poder tomar, y tomarlo en 3118 partidos de nueve
temporadas es un supuesto sobre el que este trabajo no tiene dato ninguno.

**Y la celda más bonita del cuadro es la única que no se puede citar.** `rodante_ac` en
2026/27 da +6.18 % de ROI y +8.21 pp de ventaja, la mejor casilla de la tabla entera. Son
**78 partidos**, y el error típico del ROI sobre 78 partidos es 10.93 pp medido en
`matchlogs.parquet`: la cifra está a **0.75 errores típicos de cero**. No distingue un
modelo bueno de uno malo, y es la primera que cualquiera querrá leer.

LA CUARTA PUERTA NO SE APLICA AQUÍ, Y NO ES UN OLVIDO
=====================================================
`seleccion.confirma()` existe porque la voraz de un solo corte adoptaba juzgándose en el
mismo bloque que la había elegido, y hacía falta una lectura de fuera: por eso puntúa el
conjunto sobre 2017/18–2018/19, anteriores a la selección. **Esta ruta no la necesita,
porque la construcción ya es la puerta.** Cada temporada se predice con el conjunto que
eligió su propio pasado, de modo que los 3118 partidos de la tabla son todos de fuera de
la elección; no hay un bloque contaminado del que haya que salir. Aplicar `confirma()`
sobre las dos temporadas que no eligen nada sería además puntuar la semilla declarada
contra sí misma, que no mide nada.

Lo que sustituye a la puerta es el segundo censo de la tabla: las **seis temporadas que
no eligieron nada**, que es donde `publicado` deja de tener ventaja de construcción y
las cuatro columnas se pueden comparar entre sí.

2025/26 no aparece. Es el holdout y se abre una vez, al final, desde `build_models.py
--holdout`; es decisión de Manuel y no se toca desde aquí.

Uso:
    python _rodante.py --elige             las dos búsquedas, con punto de guardado
    python _rodante.py --elige --criterio accuracy    solo una
    python _rodante.py --puntua            las cinco pistas sobre los 3118
    python _rodante.py                     las dos cosas, en orden
"""
from __future__ import annotations

import os

# Antes de numpy: los hijos ajustan matrices de dos mil filas y el BLAS multihilo se
# estorba a sí mismo. Es el mismo motivo medido que en `_censo.py` y `_maximo.py`.
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

import argparse                                              # noqa: E402
import json                                                  # noqa: E402
import sys                                                    # noqa: E402
import time                                                   # noqa: E402
from pathlib import Path                                      # noqa: E402

import numpy as np                                            # noqa: E402
import pandas as pd                                           # noqa: E402

_RAIZ = Path(__file__).resolve().parent
for _sub in (".", "scripts/models", "scripts/features"):
    if str(_RAIZ / _sub) not in sys.path:
        sys.path.insert(0, str(_RAIZ / _sub))

# Los módulos de modelado se importan PLANOS, igual que se importan entre ellos
# (`seleccion.py` l. 237 hace `import laboratorio as lb`), y no por el paquete. No es
# cosmética: `scripts.models.laboratorio` y `laboratorio` son dos objetos de módulo
# distintos, con dos `_feat_cache` distintos, y `seleccion._lote` decide si puede
# repartir entre procesos preguntando `feat is lb._feat_cache` —su manera de no
# mandarle a un hijo una matriz que el hijo no leería—. Importando por el paquete la
# matriz que se le pasa no es nunca la de SU caché, la guarda cae a la serie y la
# búsqueda tarda tres veces más sin que nada lo avise. Medido: un paso de doce
# candidatas cuesta 42.2 s con la matriz pasada por la ruta del paquete y 29.8 s por la
# plana, en la misma máquina y al mismo tiempo. La guarda hace lo que debe; lo que
# estaba mal era la puerta por la que yo entraba.
import build_models as bm                                     # noqa: E402
import evaluate as ev                                         # noqa: E402
import laboratorio as lb                                      # noqa: E402
import protocol as pt                                         # noqa: E402
import seleccion as sl                                        # noqa: E402

# ===========================================================================
# 1. Las decisiones, declaradas antes de buscar
# ===========================================================================
# Las nueve temporadas alcanzables sin tocar el sello. 2025/26 no está y no es un
# olvido (regla 7); 2016/17 tampoco, porque es el arranque y no tiene pasado del que
# sacar forma, racha ni enfrentamiento directo (cobertura media 88.00 %).
ORIGENES: tuple[str, ...] = ("2017/18", "2018/19", "2019/20", "2020/21",
                             "2021/22", "2022/23", "2023/24", "2024/25",
                             "2026/27")

# La temporada en curso, que se puntúa con la lista de pliegues de `produccion`.
# Se declara y no se deduce para que, cuando pase a desarrollo, falle a la vista.
EN_CURSO = "2026/27"

# Las tres temporadas que eligieron el peso de la mezcla publicada. La pista
# `publicado` no es comparable con las demás AHÍ, y avisarlo sin dar la cifra limpia
# deja al lector con la advertencia y sin el número: la tabla reporta por tanto los dos
# censos, el de nueve temporadas y el de las seis que no eligieron nada. No afecta a
# `semilla` ni a las rodantes —la semilla está declarada de antemano y cada origen
# rodante se eligió con su propio pasado—, así que su columna solo cambia por el cambio
# de censo.
SUCIAS_PESO: tuple[str, ...] = ("2019/20", "2020/21", "2021/22")

# Pasado mínimo para que un origen elija por su cuenta, en partidos.
#
# El número es el primer múltiplo de una temporada que deja la tolerancia por debajo
# de lo que la búsqueda puede distinguir. Las tolerancias se midieron sobre 1140
# partidos (`seleccion.N_TOLERANCIA`) y escalan con la raíz, de modo que el error
# típico de la diferencia emparejada en acierto vale 2.5e-3 con 1140, 3.1e-3 con 760 y
# 4.3e-3 con 380: con una temporada de pasado, la tolerancia pide 0.43 puntos
# porcentuales de mejora, que son casi el doble de lo mejor que ha medido el proyecto
# en cualquier bloque. Con 760 pide 0.31 y ya es un listón que algo puede pasar.
#
# Por debajo de este suelo el origen no elige: predice con `sl.SEMILLA_MERCADO`, que
# es una decisión declarada de antemano y no una elección hecha con sus partidos.
MIN_ELECCION = 760

# Los dos criterios que pidió Manuel. El RPS no está: se reporta en la tabla final
# —porque el daño en RPS es lo que delató a las rutas anteriores— pero no adopta.
CRITERIOS: tuple[str, ...] = ("accuracy", "ventaja")

# Tope de variables por origen. No es una creencia sobre el tamaño bueno sino el
# presupuesto hecho explícito, y está donde está por una medida: en `camino65.csv`,
# con la adopción forzada y 58 pasos, el RPS de fuera de la elección empeora desde
# k=6 sin una sola excepción, y el mejor de fuera está en k=5. Doce deja holgura de
# siete variables sobre el mejor conocido y acota el reloj: cada paso de los siete
# orígenes cuesta ~25 min repartido entre seis procesos (59 candidatas × 1432 pliegues
# × 0.0178 s), de modo que el peor caso son 5 h por criterio.
MAX_K = 12

FAMILIA = "logistica"
HIPER: dict = {"C": 0.3, "l1_ratio": 0.0}

# El eje económico. b365 es el listón declarado del proyecto (`backtest.REFERENCIA`) y
# las otras dos son lecturas gratuitas de la misma pasada. La cuota de cierre
# promediada NO puede ser el eje: cobertura 0 de 380 en 2016/17, 2017/18 y 2018/19.
CASA = "b365"
PROB = ["p_home", "p_draw", "p_away"]
PRECIOS: dict[str, list[str]] = {
    "b365": ["odds_b365_h", "odds_b365_d", "odds_b365_a"],
    "consenso": ["odds_avg_h", "odds_avg_d", "odds_avg_a"],
    "maxima": ["odds_max_h", "odds_max_d", "odds_max_a"],
}

# La ventana del protocolo, igual para los nueve orígenes: lo ÚNICO que cambia entre
# ellos es `corte_seleccion`. Eso es lo que permite compartir una sola caché de
# estados de preprocesado entre todos, porque la ventana de entrenamiento de un
# pliegue no depende de la máscara evaluable (`protocol.make_folds`, l. 643-658).
VENTANA = {"window": "expansiva", "sliding_seasons": pt.SLIDING_SEASONS,
           "halflife_days": None}

N_BOOT = 2000
SEMILLA_BOOT = 7

# Las rutas las declara `seleccion.py`, junto a las de las nueve rutas anteriores, y
# aquí solo se les pone nombre corto. El mismo literal escrito en dos ficheros es
# exactamente como se descuelga el cuaderno de lo que el guion dejó en disco.
OUT = sl.RODANTE
ELECCION = sl.RODANTE_ELECCION
PRED = sl.RODANTE_PRED
TEMPORADA = sl.RODANTE_TEMPORADA
RESUMEN = sl.RODANTE_RESUMEN

_ODDS: pd.DataFrame | None = None


# ===========================================================================
# 2. El protocolo de cada origen
# ===========================================================================
def cfg_de(corte: str) -> pt.ProtocolConfig:
    """El protocolo que elige con el pasado de `corte` y nada más.

    El arranque baja a 2016/17 sola, que es la que de verdad lo necesita, y con eso
    2017/18 y 2018/19 pasan de arranque a evaluables. `corte_seleccion` es el único
    campo que distingue a un origen de otro.
    """
    return pt.ProtocolConfig(regime="semanal", **VENTANA,
                             warmup_seasons=list(sl.WARMUP_CONFIRMACION),
                             corte_seleccion=str(corte))


def _slug(s: str) -> str:
    return str(s).replace("/", "")


def _odds(feat: pd.DataFrame) -> pd.DataFrame:
    """Las nueve columnas de cuota, una fila por partido. Se lee una vez."""
    global _ODDS
    if _ODDS is None:
        cols = [c for v in PRECIOS.values() for c in v]
        _ODDS = feat[["match_key"] + cols].drop_duplicates("match_key").copy()
    return _ODDS


# ===========================================================================
# 3. La liquidación: acierto, RPS y ROI plano a tres precios
# ===========================================================================
def _plano(ok: np.ndarray, q: np.ndarray) -> np.ndarray:
    """Unidad plana: el acierto paga `cuota - 1`, el fallo pierde 1."""
    return np.where(ok, q - 1.0, -1.0)


def liquida(pred: pd.DataFrame, feat: pd.DataFrame) -> pd.DataFrame:
    """Añade a las predicciones el acierto, el RPS y el beneficio partido a partido.

    El listón viaja en las mismas filas y no en una tabla aparte: la ventaja es una
    diferencia emparejada y calcularla sobre dos censos distintos es el error que la
    invariante 6 de `qa_models` existe para impedir.
    """
    p = pred.merge(_odds(feat), on="match_key", how="left", validate="one_to_one")
    y = p["y_num"].to_numpy(dtype=int)
    P = p[PROB].to_numpy(dtype=float)
    fila = np.arange(len(p))
    sel = P.argmax(axis=1)

    out = p[["season", "jornada", "date", "match_key", "y_num"]].copy()
    out["signo"] = sel
    out["ok"] = (sel == y).astype(float)
    out["rps"] = ev.rps_vector(y, P)
    for nom, cols in PRECIOS.items():
        Q = p[cols].to_numpy(dtype=float)
        fav = np.where(np.isfinite(Q).all(axis=1), Q.argmin(axis=1), -1)
        out[f"ben_{nom}"] = _plano(out["ok"].to_numpy() > 0, Q[fila, sel])
        out[f"ok_ref_{nom}"] = (fav == y).astype(float)
        out[f"ben_ref_{nom}"] = _plano(fav == y, Q[fila, np.where(fav < 0, 0, fav)])
        out.loc[fav < 0, [f"ben_{nom}", f"ok_ref_{nom}", f"ben_ref_{nom}"]] = np.nan
    return out


def _boot(d: np.ndarray, bloque: np.ndarray) -> tuple[float, float]:
    """Intervalo al 95 % de la media de `d`, remuestreando jornadas completas.

    El remuestreo va por bloques y no por partidos porque los diez partidos de una
    jornada comparten el mismo estado del modelo: tratarlos como independientes
    estrecha el intervalo por una correlación que no se ha medido, sino supuesta.
    """
    ok = np.isfinite(d)
    d, bloque = d[ok], bloque[ok]
    if d.size == 0:
        return float("nan"), float("nan")
    claves, inv = np.unique(bloque, return_inverse=True)
    grupos = [d[inv == i] for i in range(len(claves))]
    rng = np.random.default_rng(SEMILLA_BOOT)
    muestras = np.empty(N_BOOT)
    for b in range(N_BOOT):
        idx = rng.integers(0, len(grupos), len(grupos))
        muestras[b] = np.concatenate([grupos[i] for i in idx]).mean()
    return float(np.quantile(muestras, 0.025)), float(np.quantile(muestras, 0.975))


def resume(liq: pd.DataFrame, *, con_intervalo: bool = True) -> dict:
    """Las cifras de una pista sobre un censo: acierto, RPS, ROI y ventaja."""
    d = {"n": int(len(liq)), "acierto": float(liq["ok"].mean()),
         "rps": float(liq["rps"].mean())}
    bloque = (liq["season"].astype(str) + "|" + liq["jornada"].astype(str)).to_numpy()
    for nom in PRECIOS:
        ben, ref = liq[f"ben_{nom}"], liq[f"ben_ref_{nom}"]
        d[f"roi_{nom}"] = float(ben.mean())
        d[f"roi_ref_{nom}"] = float(ref.mean())
        d[f"vent_{nom}"] = float((ben - ref).mean())
        d[f"n_{nom}"] = int(ben.notna().sum())
        if con_intervalo:
            lo, hi = _boot((ben - ref).to_numpy(dtype=float), bloque)
            d[f"vent_{nom}_inf"], d[f"vent_{nom}_sup"] = lo, hi
    d["acierto_ref"] = float(liq[f"ok_ref_{CASA}"].mean())
    return d


# ===========================================================================
# 4. Fase de elección: una voraz por origen y por criterio
# ===========================================================================
def _ruta_eleccion(criterio: str, origen: str) -> Path:
    return OUT / f"eleccion_{criterio}_{_slug(origen)}.json"


def elige_origen(origen: str, criterio: str, *, feat: pd.DataFrame,
                 pool: list[str], n_jobs: int = 0,
                 verbose: bool = True) -> dict:
    """La voraz sobre el pasado de `origen`. Devuelve el conjunto y con qué se obtuvo.

    El punto de guardado es por origen y por criterio, no global: la pasada completa
    son varias horas y una interrupción no puede costar lo ya medido.

    `guardar=False` no es descuido. `seleccion.guarda` escribe el `conjunto.json` y la
    `traza.parquet` canónicas del proyecto, que son las del conjunto publicado; una
    búsqueda exploratoria de nueve orígenes las sobreescribiría nueve veces y dejaría
    en disco la del último. La traza de cada origen se guarda aquí, aparte.
    """
    ruta = _ruta_eleccion(criterio, origen)
    if ruta.exists():
        d = json.loads(ruta.read_text(encoding="utf-8"))
        if verbose:
            print(f"  {origen} [{criterio}]: ya medido, "
                  f"{len(d['conjunto'])} variables", flush=True)
        return d

    cfg = cfg_de(origen)
    n_bloque = int(pt._bloque_evaluable(feat, cfg, sl.BLOQUE_SELECCION).sum())
    base = {"origen": origen, "criterio": criterio, "n_bloque": n_bloque,
            "protocolo": cfg.describe(), "familia": FAMILIA, "hiper": HIPER}

    if n_bloque < MIN_ELECCION:
        # No hay con qué elegir. Se declara el motivo en el propio artefacto, para
        # que la tabla final no tenga que adivinar por qué este origen no eligió.
        d = {**base, "conjunto": list(sl.SEMILLA_MERCADO), "n_pasos": 0,
             "tolerancia": None, "segundos": 0.0,
             "motivo": f"pasado de {n_bloque} partidos, por debajo del suelo "
                       f"declarado de {MIN_ELECCION}: se predice con la semilla"}
        ruta.write_text(json.dumps(d, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        if verbose:
            print(f"  {origen} [{criterio}]: {d['motivo']}", flush=True)
        return d

    tol = sl.tolerancia_de(criterio, n_bloque)
    if verbose:
        print(f"\n--- origen {origen} · criterio {criterio} · "
              f"{n_bloque} partidos de pasado · tolerancia {tol:.6f} ---",
              flush=True)
    t0 = time.time()
    sel = sl.adelante(pool, FAMILIA, semilla=sl.SEMILLA_MERCADO,
                      criterio=criterio, tolerancia=tol, anchura=0,
                      max_vars=MAX_K, feat=feat, cfg=cfg, regime="semanal",
                      guardar=False, verbose=verbose, n_jobs=n_jobs,
                      hiper=HIPER, cuotas=CASA, n_boot=0)
    seg = time.time() - t0

    d = {**base, "conjunto": list(sel.conjunto), "n_pasos": len(sel.pasos),
         "tolerancia": float(tol), "segundos": round(seg, 1),
         "motivo": sel.motivo_parada}
    ruta.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    if not sel.traza.empty:
        sel.traza.to_csv(OUT / f"traza_{criterio}_{_slug(origen)}.csv",
                         index=False, encoding="utf-8")
    if verbose:
        print(f"  -> {origen} [{criterio}]: {len(sel.conjunto)} variables en "
              f"{seg / 60:.1f} min · {sel.motivo_parada}", flush=True)
    return d


def elige(criterios=CRITERIOS, *, n_jobs: int = 0, verbose: bool = True) -> dict:
    """Las nueve elecciones de cada criterio, en orden de origen."""
    OUT.mkdir(parents=True, exist_ok=True)
    feat = lb.datos()
    pool = sl.candidatas(feat, regime="semanal")
    if verbose:
        print(f"=== elección rodante: {len(ORIGENES)} orígenes × "
              f"{len(criterios)} criterios · {len(pool)} candidatas ===",
              flush=True)
    todo: dict[str, dict] = {}
    for criterio in criterios:
        todo[criterio] = {s: elige_origen(s, criterio, feat=feat, pool=pool,
                                          n_jobs=n_jobs, verbose=verbose)
                          for s in ORIGENES}
    filas = [{"criterio": c, "origen": s, "n_variables": len(d["conjunto"]),
              "n_bloque": d["n_bloque"], "n_pasos": d["n_pasos"],
              "minutos": round(d["segundos"] / 60, 2),
              "conjunto": " ".join(d["conjunto"]), "motivo": d["motivo"]}
             for c, por in todo.items() for s, d in por.items()]
    pd.DataFrame(filas).to_csv(ELECCION, index=False, encoding="utf-8")
    if verbose:
        print(f"\nelección escrita en {ELECCION.name}", flush=True)
    return todo


# ===========================================================================
# 5. Fase de puntuación: las cinco pistas sobre los mismos partidos
# ===========================================================================
def fabrica_rodante(eleccion: dict[str, list[str]], familia: str = FAMILIA,
                    hiper: dict | None = None):
    """La fábrica que da a cada pliegue el conjunto elegido con el pasado de SU
    temporada.

    Es el punto de enganche que `protocol.fit_fold` declara y que hasta ahora no tenía
    usuario: la fábrica recibe el pliegue como RÓTULO —de él solo lee la temporada—,
    nunca como acceso a sus partidos, y devuelve el estimador cuya configuración se
    decidió con partidos anteriores a él. La ventana de entrenamiento y la de
    evaluación las construye `fit_fold` y esta función no las ve.
    """
    base = {str(s): lb.fabrica(familia, cols, hiper)
            for s, cols in eleccion.items()}

    def _f(*, fold):
        s = str(fold.season)
        if s not in base:
            raise KeyError(f"la temporada {s} no tiene conjunto elegido; "
                           f"declarados {sorted(base)}")
        return base[s]()

    _f.ACEPTA = ("fold",)
    _f.familia = familia
    return _f


def _pistas(eleccion: dict) -> dict:
    """Las cinco pistas de la tabla final, cada una con su fábrica.

    `publicado` lleva la mezcla con su peso tal como se publica. Su cifra de
    2019/20–2021/22 es de DENTRO del bloque que eligió el peso y la tabla la marca:
    no es comparable con las otras columnas en esas tres temporadas.
    """
    pistas = {
        "semilla": lb.fabrica(FAMILIA, list(sl.SEMILLA_MERCADO), HIPER),
        "publicado": lb.fabrica(bm.FAMILIA, list(bm.CONJUNTO), bm.HIPER or None),
    }
    for criterio, nombre in (("accuracy", "rodante_ac"),
                             ("ventaja", "rodante_roi")):
        por = eleccion.get(criterio)
        if not por:
            continue
        pistas[nombre] = fabrica_rodante(
            {s: d["conjunto"] for s, d in por.items()}, FAMILIA, HIPER)
    return pistas


def puntua(eleccion: dict | None = None, *, verbose: bool = True) -> dict:
    """Puntúa las cinco pistas sobre los 3118 partidos y escribe los tres artefactos."""
    OUT.mkdir(parents=True, exist_ok=True)
    feat = lb.datos()
    if eleccion is None:
        eleccion = {}
        for criterio in CRITERIOS:
            por = {}
            for s in ORIGENES:
                r = _ruta_eleccion(criterio, s)
                if r.exists():
                    por[s] = json.loads(r.read_text(encoding="utf-8"))
            if len(por) == len(ORIGENES):
                eleccion[criterio] = por
            elif por and verbose:
                print(f"aviso: {criterio} tiene {len(por)} de "
                      f"{len(ORIGENES)} orígenes; se omite la pista", flush=True)

    # Una sola `cfg` y una sola caché para las cinco pistas. El corte no interviene
    # en los pliegues de desarrollo ni en los de producción —solo parte el bloque
    # evaluable, y aquí se evalúa TODO— así que cualquiera sirve y se declara el
    # último, que es el del origen en curso.
    cfg = cfg_de(EN_CURSO)
    dev = [f for f in pt.make_folds(feat, cfg, evaluar="validacion")
           if str(f.season) in set(ORIGENES)]
    prod = pt.make_folds(feat, cfg, evaluar="produccion")
    folds = dev + prod
    cache = pt._StateCache(feat, cfg)
    if verbose:
        print(f"=== puntuación: {len(folds)} pliegues "
              f"({len(dev)} de desarrollo + {len(prod)} de producción) ===",
              flush=True)

    pistas = _pistas(eleccion)
    liqs: dict[str, pd.DataFrame] = {}
    for nombre, fab in pistas.items():
        t0 = time.time()
        pred = pt.run_protocol(feat, fab, cfg, folds=folds, cache=cache)
        # `attrs` lleva el `ProtocolConfig` y los estados ajustados, que pandas
        # intenta volcar a los metadatos del parquet como JSON y no sabe serializar.
        # Es el fallo que abortó la primera ejecución real de `build_models`.
        pred.attrs.clear()
        liqs[nombre] = liquida(pred, feat)
        if verbose:
            print(f"  {nombre:<12} {len(pred):>5} partidos en "
                  f"{time.time() - t0:>5.1f} s", flush=True)

    # El listón sale de cualquiera de las pistas: sus columnas `*_ref_*` son del
    # precio y no del modelo, así que son idénticas en las cinco. Se toma de la
    # semilla y se comprueba que lo son, porque si no lo fuesen la ventaja estaría
    # comparando dos censos distintos.
    ref = liqs["semilla"]
    for nombre, liq in liqs.items():
        if not np.allclose(liq[f"ben_ref_{CASA}"].to_numpy(dtype=float),
                           ref[f"ben_ref_{CASA}"].to_numpy(dtype=float),
                           equal_nan=True):
            raise AssertionError(
                f"el listón de {nombre} no coincide con el de la semilla: las "
                f"cinco pistas tienen que liquidarse sobre los mismos partidos")

    largo = pd.concat([liq.assign(pista=n) for n, liq in liqs.items()],
                      ignore_index=True)
    largo.to_parquet(PRED, index=False)

    # Temporada a temporada, que es la lectura que pidió Manuel para la app: todas
    # las temporadas sin excepción, cada una con sus cifras y su listón.
    filas = []
    for nombre, liq in liqs.items():
        d = resume(liq, con_intervalo=False)
        filas.append({"pista": nombre, "temporada": "TODAS", **d})
        for s, g in liq.groupby(liq["season"].astype(str)):
            filas.append({"pista": nombre, "temporada": s,
                          **resume(g, con_intervalo=False)})
    tabla = pd.DataFrame(filas).sort_values(["temporada", "pista"])
    tabla.to_csv(TEMPORADA, index=False, encoding="utf-8")

    agregado = {n: resume(liq) for n, liq in liqs.items()}
    limpio = {n: resume(liq[~liq["season"].astype(str).isin(SUCIAS_PESO)])
              for n, liq in liqs.items()}
    res = {"origenes": list(ORIGENES), "pliegues": len(folds),
           "n_partidos": int(len(ref)),
           "eleccion": {c: {s: d["conjunto"] for s, d in por.items()}
                        for c, por in eleccion.items()},
           "agregado": agregado, "limpio": limpio,
           "sucias_peso": list(SUCIAS_PESO)}
    RESUMEN.write_text(json.dumps(res, ensure_ascii=False, indent=2),
                       encoding="utf-8")
    if verbose:
        informa(agregado, tabla, limpio)
    return res


def _bloque(titulo: str, agregado: dict) -> None:
    """Un censo de la tabla: el listón y una fila por pista."""
    print("\n" + "=" * 86)
    print(titulo)
    print("=" * 86)
    print(f"{'pista':<12} {'acierto':>8} {'RPS':>9} {'ROI b365':>9} "
          f"{'ROI cons':>9} {'ROI max':>9} {'ventaja b365':>22}")
    lis = agregado["semilla"]
    print(f"{'liston':<12} {lis['acierto_ref']:>7.2%} {'—':>9} "
          f"{lis[f'roi_ref_{CASA}']:>8.2%} {lis['roi_ref_consenso']:>8.2%} "
          f"{lis['roi_ref_maxima']:>8.2%}")
    for n, d in agregado.items():
        print(f"{n:<12} {d['acierto']:>7.2%} {d['rps']:>9.6f} "
              f"{d[f'roi_{CASA}']:>8.2%} {d['roi_consenso']:>8.2%} "
              f"{d['roi_maxima']:>8.2%}  "
              f"{d[f'vent_{CASA}']:>+7.2%} "
              f"[{d[f'vent_{CASA}_inf']:>+.2%}, {d[f'vent_{CASA}_sup']:>+.2%}]")


def informa(agregado: dict, tabla: pd.DataFrame,
            limpio: dict | None = None) -> None:
    """La tabla que se cita, por pantalla, en sus dos censos."""
    _bloque(f"LAS PISTAS SOBRE LOS MISMOS {agregado['semilla']['n']} PARTIDOS "
            f"(17/18-26/27)", agregado)
    if limpio:
        _bloque(f"SOLO LAS 6 TEMPORADAS QUE NO ELIGIERON NADA: "
                f"{limpio['semilla']['n']} PARTIDOS", limpio)
        print(f"  (fuera {', '.join(SUCIAS_PESO)}, que eligieron el peso de "
              f"`publicado`. Es el único\n   censo en el que las cuatro columnas "
              f"se pueden comparar entre sí.)")
    print("\nTemporada a temporada (acierto · ROI b365), todas sin excepción:")
    piv = tabla[tabla.temporada != "TODAS"].pivot(
        index="temporada", columns="pista", values=["acierto", f"roi_{CASA}"])
    print(piv.round(4).to_string())
    print("\nAVISO: la fila `publicado` de 2019/20-2021/22 es de DENTRO del bloque "
          "que\neligió su peso. No es comparable con las otras pistas en esas tres.")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--elige", action="store_true",
                    help="ejecuta las búsquedas por origen")
    ap.add_argument("--puntua", action="store_true",
                    help="puntúa las cinco pistas sobre los 3118 partidos")
    ap.add_argument("--criterio", choices=CRITERIOS, default=None,
                    help="limita la elección a un criterio")
    ap.add_argument("--n-jobs", type=int, default=0,
                    help="procesos; 0 = núcleos menos dos, 1 = serie")
    a = ap.parse_args(argv)
    hacer_todo = not (a.elige or a.puntua)

    eleccion = None
    if a.elige or hacer_todo:
        crit = (a.criterio,) if a.criterio else CRITERIOS
        eleccion = elige(crit, n_jobs=a.n_jobs)
    if a.puntua or hacer_todo:
        puntua(eleccion if (eleccion and len(eleccion) == len(CRITERIOS))
               else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
