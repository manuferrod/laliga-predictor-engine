"""Selección de variables hacia delante, con el bloque de elección separado.

QUÉ HACE Y POR QUÉ SE PUEDE DEFENDER
====================================
`laboratorio.py` es un banco: se le da un conjunto y devuelve lo que rinde, y no
elige nada. Este módulo sí elige. Recorre las variables una a una, se queda en cada
paso con la que más mejora el criterio, y se detiene cuando ninguna lo mejora lo
bastante. Es la selección hacia delante de manual, con un solo añadido: la mejora de
una candidata tiene que sobrevivir a quitarle su mejor temporada, porque una media
calculada sobre tres temporadas la puede comprar una sola.

Lo que la hace publicable no es el algoritmo —que es el de siempre— sino **dónde
mira**. Una voraz que elige sobre los mismos partidos con los que después mide no
publica una estimación del rendimiento futuro sino el máximo de un ruido con cien
oportunidades de parecer señal: con cien candidatas y una moneda por variable, la
mejor de las cien parecerá informativa aunque ninguna lo sea. El problema no se
arregla midiendo mejor; se arregla midiendo en otro sitio.

Por eso el bloque de validación se parte (`protocol.CORTE_SELECCION`):

    2017/18 – 2018/19   CONFIRMACIÓN. Anteriores a la selección, y por tanto fuera
                        de la elección sin mirar hacia adelante. El conjunto ya
                        cerrado tiene que seguir ganando aquí; si no, se revierte a
                        la semilla. Lo abre `confirma()`, bajando el arranque a
                        2016/17 sola.
    2019/20 – 2021/22   SELECCIÓN. La voraz recorre candidatas aquí, y las cifras
                        que produce son cifras de trabajo: sirven para ordenar
                        variables y no se citan en ninguna parte de la memoria.
    2022/23 – 2024/25   MEDICIÓN. El conjunto llega cerrado —ni una variable más,
                        ni una menos— y se puntúa una sola vez. Es la cifra que la
                        memoria publica.
    2025/26             HOLDOUT. Ni se toca desde aquí. Se abre al final, desde
                        `build_models.py --holdout`, con la familia y los
                        hiperparámetros también cerrados.

Los cuatro bloques responden a cuatro preguntas distintas, y confundirlos es el error
que este módulo existe para no cometer. El de selección responde «¿qué variables
parecen aportar?»; el de confirmación, «¿lo siguen pareciendo donde no se eligieron?»;
el de medición, «¿cuánto rinde ese conjunto sobre temporadas que no participaron en
elegirlo?»; el holdout, «¿cuánto rinde el sistema entero, con todo decidido?». Cada
uno se gasta una vez y no se vuelve.

El de confirmación es el más joven de los cuatro y se añadió por una razón concreta:
sin él, la búsqueda cerró un conjunto de cinco variables que las ocho temporadas de
desarrollo desmienten. Esa historia está escrita más abajo, en el apartado del
veredicto, y el conjunto que hoy queda en disco son las tres del precio.

QUÉ CRITERIO GUÍA LA BÚSQUEDA, Y POR QUÉ NO ES EL ROI
=====================================================
La voraz avanza minimizando el **RPS**, y el acierto y el rendimiento económico se
reportan en cada paso pero no deciden. La razón es que los tres números no son
igual de estables.

El RPS es una regla de puntuación propia y lee las tres probabilidades de los
novecientos y pico partidos del bloque: cada partido aporta información y una
predicción mejor calibrada baja el RPS aunque no cambie de favorito. El acierto solo
mira cuál de las tres es la mayor, de modo que mover una probabilidad de 0.34 a 0.44
no cambia nada mientras no cruce a la primera posición; sobre novecientos partidos
su ruido es de casi un punto porcentual, que es más que casi cualquier diferencia
real entre dos conjuntos. El rendimiento económico es todavía más ruidoso, y por un
motivo estructural: solo se calcula sobre los partidos donde la política encuentra
valor —a menudo un tercio del bloque—, se paga a cuota, y una cuota de 6.00 que entra
mueve el rendimiento agregado más que veinte apuestas normales. Guiar una voraz con
él sería seleccionar las variables que acertaron los cuatro o cinco partidos raros
que dominan la suma.

De modo que se elige por RPS y se **publica** por acierto y por rendimiento frente a
Bet365, temporada a temporada. Es la separación habitual entre el criterio interno de
un procedimiento y la cifra que interesa al lector, y conviene que quede escrita para
que no parezca que se ha ido a buscar el número que mejor quedaba.

EL COSTE, Y LA CRIBA QUE LO HACE VIABLE
=======================================
Cada candidata evaluada es un protocolo walk-forward completo: unos cien ajustes.
Una voraz de veinte pasos sobre ciento doce candidatas son dos mil doscientos
protocolos, que no caben en el tiempo que queda.

La rebaja es una **criba** previa: un solo ensayo por variable, cada una sola contra
el resultado, sobre el bloque de selección. Es barato —una regresión de una variable
ajusta en milisegundos— y ordena las candidatas por lo que valen por sí mismas. La
voraz recorre después solo las `anchura` primeras.

Esto restringe el espacio de búsqueda y hay que decir en qué: una variable que no
aporta sola pero sí en compañía de otra puede quedar fuera de la criba y no llegar a
probarse nunca. Es una limitación real y no se disimula. A cambio, el espacio
explorado es pequeño y declarado, que es justamente la propiedad que hace creíble la
cifra final: cuantas menos combinaciones se miran, menos oportunidades tiene el ruido
de parecer señal. La criba no es solo un ahorro de tiempo, es también parte de la
honestidad del procedimiento.

LO QUE DIJO LA MEDICIÓN, Y EL LÍMITE DE POTENCIA DEL DISEÑO
===========================================================
El bloque de medición se leyó sobre el conjunto cerrado de cinco variables, y el
resultado hay que decirlo sin adornos: el conjunto **no bate al precio**. Sobre
2022/23–2024/25, mil ciento cuarenta partidos:

    conjunto de cinco     RPS 0.19154   acierto 54.82 %   rendimiento −11.20 %
    precio crudo de B365  RPS 0.19154   acierto 55.00 %   rendimiento  +2.09 %
    semilla de mercado    RPS 0.19093   acierto 54.39 %

El empate en RPS es literal: la diferencia son −0.000001, con intervalo por jornadas
[−0.00105, +0.00094].

Y hay algo peor que un empate. Las dos variables que la voraz adoptó **empeoran** el
modelo en los partidos que no las eligieron, y eso se mide dos veces de forma
independiente: en el bloque de medición, que es posterior, y en 2017/18-2018/19, que es
anterior al bloque de selección y que un arranque de una sola temporada abre sin tocar
el holdout. Las ocho temporadas de desarrollo, con el signo de una pérdida de RPS:

    temporada   ¿eligió?   k5 − k3
     2017/18       no     +0.000424
     2018/19       no     +0.000507
     2019/20       sí     −0.000799
     2020/21       sí     −0.001000
     2021/22       sí     −0.000109
     2022/23       no     +0.000988
     2023/24       no     +0.001160
     2024/25       no     −0.000326

El conjunto gana en las tres temporadas que lo eligieron y pierde en cuatro de las
cinco que no. Agrupadas las mil novecientas de fuera de la elección, las dos variables
cuestan **+0.000551 de RPS, con error típico 0.000235 e intervalo [+0.000104,
+0.001005]**, que excluye el cero. No es que no aporten: restan. Esa es la firma del
sobreajuste de la propia búsqueda, y es la cifra que hay que citar.

No es un fallo del procedimiento: es el tamaño del bloque. Tres medidas lo fijan, las
tres sobre el bloque de selección, que ya está gastado y mirarlo otra vez no cuesta
nada:

  · El ruido de una comparación emparejada entre dos conjuntos, remuestreando
    jornadas, es 0.00026 de error típico. Las dos mejoras adoptadas fueron +0.000351
    y +0.000285, es decir 1.35 y 1.1 errores típicos.
  · Las sesenta y dos mejoras del primer paso forman una nube centrada en −0.000175
    con desviación típica 0.000184, y solo cinco son positivas. Las dos adoptadas
    quedan a 2.86 y 2.38 desviaciones sobre la media, donde el máximo esperado de
    sesenta y dos sorteos de ruido está en 2.19. Asoman por el borde del ruido.
  · Quitarle la elección a la voraz no arregla nada. Las sesenta y cinco variables a
    la vez con logística penalizada —ocho rejillas, de C=0.01 a C=0.3, ridge y
    lasso— son todas peores que la semilla; la mejor pierde 0.001383 con intervalo
    [−0.00234, −0.00033]. La penalización no rescata el banco.

El bloque de selección sí se puede agrandar —aquí estuvo escrito lo contrario y era
falso—, pero agrandarlo no arregla nada, y también está medido. De las tres temporadas
de arranque solo la primera lo necesita: 2016/17 no tiene pasado del que sacar forma,
racha ni enfrentamiento directo, y su cobertura media de variables es del 88.00 %;
2017/18 y 2018/19 están al 96.42 % y al 96.69 %, indistinguibles del 96.70 % de
2019/20. Con un arranque de una sola temporada el bloque de selección pasa de 130
pliegues y 1140 partidos a 210 y 1900, y el de medición queda idéntico:

    arranque          n    error típico   el k5 gana al k3      t
    3 temporadas   1140      0.000381          +0.000636     1.67
    2 temporadas   1520      0.000311          +0.000350     1.13
    1 temporada    1900      0.000266          +0.000195     0.73

El ruido baja un 30 %, cerca de la raíz de la proporción que predice la teoría. Lo que
no mejora es la discriminación: el efecto que habría que detectar baja MÁS deprisa que
el ruido —precisamente porque las temporadas que se añaden son las que desmienten al
conjunto— y con 1900 partidos se distingue peor que con 1140.

De modo que el techo no era el tamaño del bloque. Con cincuenta y nueve candidatas por
paso, la voraz adopta el máximo de cincuenta y nueve sorteos de una nube de ruido cuya
desviación típica es del tamaño de las mejoras reales, y eso no se arregla con un 67 %
más de partidos: haría falta un orden de magnitud. La conclusión honesta del ejercicio
es que, con este banco y este calendario, **las variables de juego no añaden nada
medible sobre el precio**.

«No añaden nada sobre el precio» no es «no miden nada», y la diferencia está medida
—la separa quitarle el precio al modelo, porque mientras esté dentro las dos lecturas
son indistinguibles—. Las 59 variables de juego solas, sin una columna de precio, dan
RPS 0.202853 sobre el bloque de selección, entre la previa de frecuencias (0.223518) y
la semilla (0.193174): recorren el 68.1 % del hueco, y el acierto —métrica
independiente— dice 68.3 %. El banco reconstruye desde cero dos tercios de lo que el
mercado sabe, sin verlo; el tercio que falta es el que no se puede tener, porque es la
alineación confirmada y el dinero de los apostantes.

Y el techo del banco no es ese 68 %, sino más: el dominio de `fuerza` solo —nueve
columnas— llega a 0.198001 y BATE a las 59 juntas, el 84.1 % del camino. Lo que sobra
en el ajuste de 59 es varianza, no falta de señal, y de ahí la lectura mecánica de por
qué la voraz se queda quieta: cada variable que entra cuesta más varianza de la que
trae de señal *nueva*, porque su señal ya estaba en el precio. Las 65 juntas dan
0.200669, 0.0075 PEOR que las tres del precio solas: añadidas a un precio que ya
acierta, ahogan lo que el precio sabía. La tabla por dominios está en `SINPRECIO`.

Queda entonces la objeción directa: ¿y un conjunto con el mercado MÁS lo mejor del
resto? La voraz no podía encontrarlo aunque existiera, porque comparaba k=3 contra k=12
con la penalización clavada de `lb.HIPER` —calibrada para k=3— y porque, siendo miope,
nunca probó un dominio entero. Corregidas las dos cosas (`BLOQUES`, `MEJORES`), la
distancia se estrecha de verdad: `semilla+fuerza` pasa de perder 0.001081 a perder
0.000780, luego la comparación original era injusta. Pero ninguno de los cuatro caminos
—voraz de una en una, L1 sobre las 65, bloques enteros afinados, y las N mejores
ordenadas sin el precio delante— produce un conjunto que gane. Los dos que lo parecían
en el bloque de selección se cayeron por la cuarta puerta, y el signo se les da la
vuelta: `semilla+bajas` gana 0.000211 donde se eligió y pierde 0.000312 donde no;
`semilla+historial` empata en RPS y mejora el acierto en 0.61 pp —que son 0.41 errores
típicos de los 1.48 pp que mide el acierto sobre 1140 partidos, es decir nada— y fuera
pierde 0.000586 con intervalo [+0.000075, +0.001115], que ya excluye el cero.

Lo único que lo bate de forma consistente es recalibrar el precio: la semilla de
mercado ajustada gana al precio crudo en los dos bloques y en la misma dirección
(+0.00031 en selección, +0.00061 en medición), aunque en ninguno con significación
—unos 1.2 errores típicos—. La semilla no es una elección: estaba fijada de antemano,
así que esa lectura no gasta presupuesto de selección.

LO QUE QUEDA ESCRITO
====================
Cada paso de la voraz se anota en `outputs/models/seleccion/traza.parquet`: qué
variables se probaron, cuál ganó, cuánto mejoró y qué cifras tenía el conjunto en ese
momento. La traza es el documento que permite a un tribunal reconstruir la selección
sin repetirla, y también lo que permite retomar una búsqueda interrumpida sin volver
a empezar. El conjunto elegido se guarda aparte, en `conjunto.json`, junto al corte y
al protocolo con el que se obtuvo, porque un conjunto sin esas dos cosas no es
reproducible.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import catalogo as cat  # noqa: E402
import columns as C  # noqa: E402
import evaluate as ev  # noqa: E402
import laboratorio as lb  # noqa: E402
import protocol as pt  # noqa: E402

_ROOT = _HERE.parent.parent
OUT = _ROOT / "outputs" / "models" / "seleccion"
TRAZA = OUT / "traza.parquet"
CRIBA = OUT / "criba.parquet"
CONJUNTO = OUT / "conjunto.json"

# Las dos lecturas de FUERA de la elección, que no son parte de la búsqueda sino la
# evidencia de que su parada está en el sitio. Se declaran aquí para que el cuaderno
# 05 no lleve rutas escritas a mano y para que se sepa quién las produce:
#   CAMINO_FUERA   el recorrido con `corte_seleccion` en 2021/22 —eligiendo solo con
#                  19/20 y 20/21— puntuado prefijo a prefijo sobre 2021/22, que no
#                  participó en elegirlo. Es lo que sostiene el tamaño del conjunto.
#   CAMINO_65      el mismo recorrido llevado hasta agotar el banco entero de
#                  candidatas, con la cifra de dentro y la de fuera en cada paso. Es
#                  la respuesta a «¿y si el conjunto bueno fuese más grande?».
# Viven bajo `outputs/`, que no se versiona: el cuaderno los lee si están y, si no,
# se apoya en las cifras escritas en su propio texto.
CAMINO_FUERA = OUT / "camino_2021.json"
CAMINO_65 = OUT / "camino65.csv"

# La descomposición que contesta a la otra mitad de la pregunta. Todo lo anterior mide
# las variables COMO AÑADIDO al precio, de modo que un resultado nulo admite dos
# lecturas incompatibles: que las variables no midan nada, o que midan mucho y el
# precio ya lo haya incorporado. Se distinguen quitando el precio y ajustando las 59
# de juego solas, con la previa de frecuencias como suelo y la semilla como techo.
SINPRECIO = OUT / "sinprecio.csv"

# Las dos tablas que cierran la objeción «¿y un modelo con el mercado MÁS lo mejor del
# resto?». La voraz de `adelante` no podía encontrarlo aunque existiese, por dos
# limitaciones del procedimiento y no de los datos:
#
#   · la penalización está clavada en `lb.HIPER` para toda la búsqueda, así que k=3 y
#     k=12 se comparan con un castigo de varianza calibrado para k=3. Cuesta caro:
#     `ridge.csv` mide 0.0061 de RPS entre la mejor y la peor penalización sobre las
#     MISMAS 65 columnas. `afina()` compensaba eso, pero solo sobre el conjunto que ya
#     había ganado, de modo que la compensación nunca llegaba a quien la necesitaba;
#   · la voraz es miope: añade de una en una y se queda con la que más mejora sola, así
#     que un bloque que ayuda sin que ninguno de sus miembros ayude por separado le es
#     invisible. Y eso aquí no es hipotético: en `SINPRECIO`, `fuerza` entero bate a las
#     59 de juego juntas.
#
#   BLOQUES  la semilla más cada dominio ENTERO y la escalera acumulativa, cada conjunto
#            con SU mejor penalización de una rejilla de cinco, incluida la semilla,
#            para que la comparación sea justa en los dos ejes a la vez.
#   MEJORES  la semilla más las N mejores de juego, con las mejores ordenadas por mérito
#            PROPIO (k=1, sin el precio delante). La voraz las ordena por lo que añaden
#            DADO el precio, y ahí una variable que mide bien lo mismo que el mercado
#            puntúa cero y queda al fondo de la cola por redundante, no por mala.
#
# Las dos dicen lo mismo y ninguna cifra mejora a la semilla. Afinar sí estrecha la
# distancia —`semilla+fuerza` pasa de perder 0.001081 a perder 0.000780—, luego la
# comparación original era injusta y ahora no lo es; el veredicto no se mueve. La
# escalera acumulativa empeora de forma monótona, de 0.193949 con `fuerza` a 0.194399
# con las 62, y desde `forma` los intervalos ya excluyen el cero: no es ruido, es daño.
BLOQUES = OUT / "bloques.csv"
MEJORES = OUT / "mejores.csv"
MEJORES_ORDEN = OUT / "mejores_orden.csv"

# La novena ruta, y la única que sobrevive fuera del bloque que la eligió: en vez de
# preguntar qué columnas meter en un ajuste —que es lo que preguntan las ocho
# anteriores—, ajustar DOS modelos separados, el precio y el juego, y combinar sus
# probabilidades como `p ∝ p_mercado^w · p_juego^(1-w)`, renormalizando. El motivo de
# que esto sí repita es de tamaño: se elige UN parámetro, frente a los 2^65 conjuntos
# entre los que elegía la voraz, así que apenas hay nada que sobreajustar.
#
#   CURVA / `mezcla.csv`          el barrido de `w` por bloque, con RPS, acierto y ROI.
#   PAR   / `mezcla_par.csv`      la diferencia EMPAREJADA contra la semilla, con su
#                                 intervalo, que es lo que decide: un nivel no dice si
#                                 la diferencia excluye el cero.
#   RESUMEN / `mezcla_resumen.json`  los tres óptimos de `w` y si el signo repite fuera.
#
# A w=0.85 el acierto y el ROI repiten signo en los dos bloques que no eligieron
# (+0.35 pp y +1.12 pp en `medicion`, +0.39 pp y +1.51 pp en `confirmacion`), y **el RPS
# empeora en los tres**. Por eso adoptarla fue una decisión y no una lectura: levanta la
# regla 4 del proyecto, y la levantó Manuel el 19-oct-2026. Ninguna diferencia de fuera
# excluye el cero: el signo no se desmorona, que no es lo mismo que estar demostrado.
#
# El peso se elige por ACIERTO y no por ROI, y eso está medido aquí. Fuera del bloque
# que eligió —`medicion` más `confirmacion`, promediando por partidos— el 0.85 bate al
# 0.35 en las dos convenciones de liquidación, y hay que nombrar cuál se cita porque los
# niveles no son intercambiables: a la mejor cuota entre casas (columna `roi_max`)
# **+2.51 % contra +1.38 %**, y de ventaja sobre el listón a cierre promediado (columna
# `vent`, que es la emparejada) **+1.08 % contra −0.05 %**. El ROI sobre 1140 tiene un
# error típico de ~3 pp, de modo que elegir con él es elegir ruido. Se selecciona por
# acierto y se reporta ROI.
MEZCLA = OUT / "mezcla.csv"
MEZCLA_PAR = OUT / "mezcla_par.csv"
MEZCLA_RESUMEN = OUT / "mezcla_resumen.json"

# La DÉCIMA ruta, en dos pasadas, y la primera que pregunta por el acierto y el dinero en
# vez de por el RPS: es la que trae la directriz del 20-oct-2026. `_maximo.py` contesta la
# pregunta en su forma literal —maximizar acierto y ROI sobre todas las temporadas
# disponibles— y su propia tabla lo delata: el campo `temporadas` de `maximo.csv` enumera
# las seis mismas sobre las que reporta, así que elige y mide en los mismos 2280 partidos.
# Sus dos ganadores, 55.00 % de acierto con nueve variables y +4.56 % de ROI con diez, son
# por tanto **incitables** (regla 5), y el optimismo crece con el número de
# configuraciones probadas, que allí fueron miles.
#
#   MAXIMO_CRIBA / MAXIMO_CURVA / MAXIMO_DETALLE / MAXIMO_GANADORES
#   MAXIMO_FUERA   el mismo haz liquidado fuera, que es lo que permite leerlo.
#
# `_censo.py` cierra ese hueco de la única forma que hay: **partiendo el censo**. Elige en
# `seleccion` (1140) y mide en `medicion` (1140) y en `confirmacion` (760, con el arranque
# abierto a 2016/17 sola como hace `confirma()`), de modo que ninguno de los 1900 de fuera
# participó en elegir nada y los 760 son anteriores a todo: la regla 6 queda en pie.
#
#   CENSO_CURVA / CENSO_DETALLE / CENSO_GANADORES / CENSO_FUERA
#
# **RECHAZA, y la décima se suma a las nulas** (2314 evaluaciones, 107 min). Dentro de
# `seleccion` el ganador por acierto —siete variables, `C=0.3, L2`— sube de 52.46 % a
# 54.30 % y de +2.20 % a +9.50 % de ROI a la mejor cuota, el mayor salto medido en el
# proyecto. Fuera **se parte en dos**: gana en `medicion` (+1.55 pp de ROI máx, +0.09 pp
# de acierto) y pierde en `confirmacion` (−1.82 pp, −1.05 pp), y sobre los 1900 juntos
# queda en +0.20 pp de ROI [−2.71, +3.11] y **−0.37 pp de acierto**. El signo no repite:
# es el máximo de una nube. Y hay un daño que sí es significativo —el RPS empeora en las
# tres ventanas con los tres intervalos excluyendo el cero—, de modo que frente a la
# mezcla el conjunto de siete es estrictamente peor en las dos cosas a la vez.
MAXIMO_CRIBA = OUT / "maximo_criba.csv"
MAXIMO_CURVA = OUT / "maximo.csv"
MAXIMO_DETALLE = OUT / "maximo_detalle.csv"
MAXIMO_GANADORES = OUT / "maximo_ganadores.json"
MAXIMO_FUERA = OUT / "maximo_fuera.csv"
CENSO_CURVA = OUT / "censo.csv"
CENSO_DETALLE = OUT / "censo_detalle.csv"
CENSO_GANADORES = OUT / "censo_ganadores.json"
CENSO_FUERA = OUT / "censo_fuera.csv"

# La UNDÉCIMA ruta y la pasada que puntúa el acervo entero, que son las dos cosas que
# cubren el universo completo —2017/18 a 2026/27 sin excepción— y por tanto las únicas
# que contestan la directriz tal como está escrita. Las diez rutas anteriores eligen con
# 2019/20–2021/22 y puntúan fuera, de modo que dejan tres temporadas sin cifra; ampliar
# el bloque de elección arreglaría la cobertura y rompería la regla 5. Las dos exigencias
# solo son compatibles eligiendo un PROCEDIMIENTO y no un conjunto.
#
# ORIGEN RODANTE (`_rodante.py`, nueve orígenes × dos criterios, 4.2 h). Para cada
# temporada S se repite la búsqueda con solo partidos ANTERIORES a S, y con lo que salga
# se predice S. Lo que se fija es el procedimiento; el conjunto cambia con el origen.
#
#   RODANTE_ELECCION   qué adoptó cada origen, cuántos pasos dio y por qué paró.
#   RODANTE_TEMPORADA  las cuatro pistas —`semilla`, `publicado`, `rodante_ac`,
#                      `rodante_roi`— temporada a temporada y en el agregado.
#   RODANTE_RESUMEN    el agregado, el censo limpio y qué temporadas ensucian el peso.
#   RODANTE_PRED       la terna por partido de cada pista, para emparejar diferencias.
#
# El veredicto, sobre el censo limpio: **ni un intervalo excluye el cero, ninguno de los
# cuatro**, y de las doce comparaciones emparejadas entre pistas solo una lo excluye
# —`publicado` contra `semilla` sobre los 3118, y en el censo limpio ya no—. La mejor de
# las cuatro sigue siendo la mezcla que ya se publica, así que **lo que esta ruta aporta
# no es un modelo mejor**: es la primera cifra del trabajo que cubre las nueve
# temporadas sin que ninguna haya participado en elegir lo que la predice.
#
#   ESTABILIDAD / ESTABILIDAD_ORDEN  el hallazgo lateral, que va contra la directriz que
#       lo produjo: el orden de las candidatas por RPS es más estable entre orígenes que
#       el orden por acierto —rho de Spearman **0.791 contra 0.693** promediando los 27
#       pares de la tabla, **0.782 contra 0.688** sobre los 21 del criterio de acierto, y
#       **0.588 contra 0.371** en el par más lejano, 2019/20 contra 2026/27—. Di sobre
#       qué pares promedias al citarlo: la tabla tiene dos criterios con 21 y 6 pares, y
#       el promedio global no es el de ninguno de los dos. La brecha se abre con la
#       distancia, que es lo que hace el hallazgo. No cambia quién elige, que lo fijó
#       Manuel, pero es el mecanismo por el que una búsqueda guiada por acierto compra
#       daño en RPS sin anunciarlo.
RODANTE = OUT / "rodante"
RODANTE_ELECCION = RODANTE / "rodante_eleccion.csv"
RODANTE_TEMPORADA = RODANTE / "rodante_temporada.csv"
RODANTE_RESUMEN = RODANTE / "rodante_resumen.json"
RODANTE_PRED = RODANTE / "rodante_pred.parquet"
ESTABILIDAD = RODANTE / "estabilidad.csv"
ESTABILIDAD_ORDEN = RODANTE / "estabilidad_orden.csv"

# EL MÁXIMO SOBRE EL CENSO DE NUEVE (`_nueve.py`, los 345 pliegues, 24.3 min). No es una
# ruta más: no busca nada, puntúa. Con el censo ya fijado se puede hacer la pregunta en su
# forma literal —qué conjunto maximiza el ROI y el acierto, en el agregado y en cada
# temporada— midiendo los **50 conjuntos distintos** que han producido las once rutas
# sobre los MISMOS 3118 partidos.
#
#   NUEVE_ACERVO     los 50 conjuntos × las 9 temporadas más las filas `TODAS` y
#                    `LIMPIO`, con los tres precios y el listón al lado.
#   NUEVE_TEMPORADA  solo el desglose por temporada, que es lo que alimenta la lectura 2.
#   NUEVE_LECTURA    las tres lecturas: global, por temporada y dejar-una-fuera.
#   NUEVE_DENTRO     la voraz con los 3118 DENTRO del bloque de elección, que es el
#                    máximo de información que el proyecto puede darle. Se agota en dos
#                    variables sobre la semilla: para a k=5 por los dos criterios, y en
#                    la tolerancia y no en el techo.
#   NUEVE_PRED       la terna por partido de cada conjunto.
#
# Tres lecturas y **la tercera es la única citable**. El máximo global —`maximo_roi`,
# +4.22 pp [+1.72, +6.77]— excluye el cero y es por eso mismo por lo que no vale: ese
# conjunto se buscó sobre partidos que están dentro de estos 3118. Por temporada no hay
# respuesta que dar, y eso es un hallazgo: las nueve las firman nueve conjuntos
# distintos, en los dos criterios, y ninguno repite. Dejar una fuera sí es citable, y
# **no excluye el cero** (+1.46 pp [−0.42, +3.34] por acierto, +1.45 pp [−0.84, +3.69]
# por ventaja), y los dos quedan por debajo del +1.52 pp del modelo publicado.
#
# El censo llamado LIMPIO son las seis temporadas que no eligieron el peso de la mezcla:
# **es censo limpio para `publicado` y para `semilla`, y no lo es para ningún conjunto de
# búsqueda.** No leas la columna como si lo fuera.
NUEVE = OUT / "nueve"
NUEVE_ACERVO = NUEVE / "nueve_acervo.csv"
NUEVE_TEMPORADA = NUEVE / "nueve_temporada.csv"
NUEVE_LECTURA = NUEVE / "nueve_lectura.json"
NUEVE_DENTRO = NUEVE / "nueve_dentro.json"
NUEVE_PRED = NUEVE / "nueve_pred.parquet"


# ===========================================================================
# 1. Las decisiones de la búsqueda, declaradas antes de buscar
# ===========================================================================
# Los tres bloques y su papel. Escribirlos aquí —y no dejarlos en el argumento de
# cada llamada— es lo que permite que las guardas de más abajo comprueben que nadie
# elige mirando donde se mide.
BLOQUE_SELECCION = "seleccion"
BLOQUE_MEDICION = "medicion"

# El cuarto bloque: CONFIRMACIÓN, y por qué hubo que inventarlo.
#
# La búsqueda tenía tres puertas y las tres se pasaron: la candidata mejoraba por
# encima de `TOLERANCIA`, mejoraba en las tres temporadas y no solo de media, y su
# mejora quedaba 1.39 veces por encima del techo del máximo de cincuenta y nueve
# sorteos de la nube de ruido (+0.000351 observado contra +0.000253 de techo, 2.86
# desviaciones típicas sobre la media de la nube). Con esos tres sellos se adoptaron
# dos variables.
#
# Las dos empeoran el modelo en los partidos que no las eligieron: **+0.000551 de RPS
# sobre mil novecientos partidos, error típico 0.000235, intervalo [+0.000104,
# +0.001005]**, que excluye el cero. De modo que las tres puertas, juntas, no separan
# el mérito del azar. Esto no es una sospecha metodológica: es el resultado.
#
# Lo que NO se hace con ese resultado es subir `TOLERANCIA` hasta que esas dos caigan.
# Cualquier número elegido así está ajustado a lo que se quería que pasara, y el propio
# comentario de `TOLERANCIA` presumía de no hacerlo. Lo que se hace es añadir una
# cuarta puerta que no tiene número que elegir: **lo que se adoptó mirando unos
# partidos tiene que seguir siendo ganancia en partidos que no lo eligieron.**
#
# El bloque de confirmación son 2017/18 y 2018/19, que es el único sitio donde eso se
# puede preguntar sin romper nada. Son ANTERIORES al bloque de selección —así que
# usarlas no mira hacia adelante y la regla 6 queda en pie—, están fuera de la elección
# por construcción, y no son el bloque de medición ni el holdout. Hoy viven como
# arranque; `confirma()` las abre bajando el arranque a 2016/17 sola, que es la única
# temporada que lo necesita de verdad (cobertura media 88.00 %, contra 96.42 % y
# 96.69 % de estas dos).
#
# El precio de usarlas, dicho en voz alta: dejan de ser una lectura limpia citable y
# pasan a ser parte del aparato de selección. Se paga a gusto. La cifra citable sigue
# siendo la del bloque de medición, y lo que se compra es que la búsqueda no vuelva a
# cerrar un conjunto que el propio histórico desmiente.
BLOQUE_CONFIRMACION = "confirmacion"
WARMUP_CONFIRMACION = ["2016/17"]

# El criterio que guía. `rps` se minimiza; los otros dos se maximizan.
#
# La elección NO es inocua, y conviene decirlo con las cifras delante. Repitiendo la
# búsqueda con cada criterio, forzando la adopción en cada paso hasta nueve variables,
# los tres caminos **no comparten ni una sola variable**. Puntuados los tres conjuntos
# de cinco con las tres métricas sobre el bloque de selección, partiendo de la semilla
# de mercado (RPS 0.19317, acierto 52.46 %, ventaja +1.67 %):
#
#   guiado por rps       RPS 0.19254   acierto 52.81 %   ventaja +12.31 %
#   guiado por accuracy  RPS 0.19337   acierto 53.51 %   ventaja  +0.51 %
#   guiado por ventaja   RPS 0.19321   acierto 52.46 %   ventaja  +6.88 %
#
# Solo el camino del RPS es bueno en las tres. El del acierto compra 0.70 puntos
# —ocho partidos de mil ciento cuarenta— a cambio de un RPS PEOR que el de la semilla
# desnuda y de hundir la ventaja; el de la ventaja consigue menos ventaja que el del
# RPS, y también empeora la semilla en RPS.
#
# Perseguir el ROI no da ROI porque el ROI se liquida sobre el subconjunto en el que
# la política de valor apuesta, y ese subconjunto se mueve con cada columna que entra:
# su primer paso forzado ya lo lanza de +1.67 % a +6.97 % con una sola variable. Y el
# acierto no tiene resolución para guiar nada: sobre mil ciento cuarenta partidos solo
# se mueve a saltos de 0.088 puntos, y tres de los seis pasos de su camino «mejoran»
# en exactamente +0.000000, es decir que adoptan por desempate ciego.
#
# Esto es la regla 4 medida: cuando el ROI y el RPS discrepan, manda el RPS.
#
# **La regla 4 la levantó Manuel el 19-oct-2026** —«quiero que el ROI mande, no el
# RPS»— y el 20-oct-2026 extendió el alcance a la búsqueda de variables: «olvídate del
# RPS para encontrar el mejor modelo». Las cifras de arriba siguen siendo verdad y por
# eso no se borran, pero **ya no son el argumento que decide**: el criterio es suyo. Lo
# que sí cambia es que una de las dos objeciones de arriba estaba mal planteada y se
# corrige abajo, en `POLITICA_SELECCION`.
CRITERIOS: dict[str, int] = {"rps": -1, "accuracy": +1, "ventaja": +1}

# Con qué política se liquida el dinero MIENTRAS SE BUSCA.
#
# La búsqueda no lo fijaba, así que heredaba el defecto del banco (`lb.POLITICA =
# "valor"`, con `umbral = 0.02`): apuesta solo donde el modelo le ve ventaja al precio.
# Y eso invalida el criterio `ventaja` de dos maneras a la vez. La primera es que **no
# es la definición de ROI de Manuel**, que es una unidad plana en cada partido. La
# segunda es la que explica el párrafo de arriba: con política de valor, el subconjunto
# sobre el que se liquida **se mueve con cada columna que entra**, de modo que dos
# conjuntos no se comparan sobre los mismos partidos y la cifra puede mejorar por
# apostar menos y mejor elegido, no por predecir mejor. De ahí salía el salto de
# +1.67 % a +6.97 % con una sola variable que el comentario de `CRITERIOS` leía como
# «perseguir el ROI no da ROI»: era en buena parte un artefacto de la política, no una
# propiedad del criterio.
#
# Con `favorito` —una unidad al signo del modelo, en todos los partidos con precio, sin
# umbral— el denominador es el mismo para todos los conjuntos y la comparación vuelve a
# ser pareada, que es lo que `TOLERANCIAS` supone al medirse así. Es además la política
# que publica la cadena (`build_models.POLITICA_ECONOMIA = "favorito"`), de modo que la
# búsqueda y la publicación dejan de responder preguntas distintas.
#
# Quien llame puede pasar otra `politica` y gana sobre esta: el `**kw` de `criba`,
# `adelante` y `flotante` llega hasta `lb.evalua`.
POLITICA_SELECCION = "favorito"

# Mejora mínima para adoptar una variable, en unidades del criterio.
#
# El listón no es el error típico de una cifra aislada, que es el primero que viene a
# la cabeza y aquí no sirve: medido sobre el bloque de selección, el RPS tiene una
# desviación típica de 0.1317 partido a partido, y por tanto un error típico de 0.0039
# sobre los mil ciento cuarenta. Pero la tolerancia no compara dos mediciones
# independientes, sino dos conjuntos sobre LOS MISMOS partidos, que comparten la
# semilla de mercado y difieren en una columna: la diferencia por partido tiene una
# desviación típica de 0.0086 —quince veces menor— y un error típico de 0.00026,
# remuestreando jornadas completas.
#
# Dos diezmilésimas son, entonces, cuatro quintos de ese error típico. El umbral no
# pretende separar señal de ruido en un paso —con una sola comparación eso no se puede
# hacer, y de hecho la quinta variable mejora 0.000285, que son 1.1 errores típicos,
# con intervalo de bloque [-0.00023, +0.00078]— sino impedir que la búsqueda siga
# añadiendo variables cuando ya solo raspa: es una condición de PARADA, y su efecto es
# acortar el conjunto final, que es la dirección en la que conviene equivocarse. Un
# conjunto de cinco variables que rinde como uno de treinta es mejor trabajo, no peor.
#
# Lo que de verdad separa a una candidata del ruido no es este umbral, sino la
# exigencia de abajo: que mejore en las tres temporadas y no solo de media. Y subir la
# tolerancia al error típico exacto (0.00026) no cambiaría el conjunto: las dos que
# entran aportan 0.000351 y 0.000285, las dos por encima, y la primera que se rechaza
# aporta -0.000021. Se queda en 2e-4 por ser el número redondo del mismo orden.
TOLERANCIA = 2e-4

# Una tolerancia POR CRITERIO, porque `TOLERANCIA` está en unidades de RPS y usarla
# con los otros dos criterios no es un umbral estricto: es no tener umbral.
#
# El RPS vive en [0, 1] y una mejora buena son diezmilésimas. El acierto se mide en
# proporción —2e-4 son 0.02 puntos porcentuales, menos de un cuarto de partido sobre
# mil ciento cuarenta— y la ventaja sobre el listón, en ROI plano, también. Pedirle a
# cualquiera de los dos que mejore 2e-4 es dejar la condición de parada desactivada,
# y entonces `adelante` corre hasta `MAX_VARS` adoptando ruido. Es un defecto, no una
# decisión, y estaba aquí desde que `CRITERIOS` admitió tres criterios.
#
# Medido con `_tolerancia.py`, que repite el experimento del comentario de arriba para
# los tres criterios: seis candidatas, cada una contra la semilla de mercado sobre LOS
# MISMOS partidos, liquidando la unidad plana de Manuel al listón del proyecto
# (`backtest.REFERENCIA = "b365"`, cobertura 380/380 en las ocho temporadas, mientras
# que la cuota de cierre promediada no existe antes de 2019/20). Mediana del error
# típico de la diferencia emparejada, remuestreando jornadas completas:
#
#                        error típico de bloque        tolerancia a 0.77x
#   criterio          1140 part.     3040 part.     1140 part.   3040 part.
#   rps                0.000159       0.000133       0.000122     0.000102
#   accuracy           0.003254       0.001940       0.002503     0.001492
#   ventaja            0.008873       0.005317       0.006825     0.004090
#
# La razón 0.77 no es nueva: es la que el proyecto ya eligió para el RPS, donde 2e-4
# son cuatro quintos del 0.00026 medido arriba. Se aplica igual a los tres para que la
# exigencia sea la misma en los tres y no se elija un número por criterio.
#
# Dos lecturas que salen del mismo cuadro y conviene no perder. Primera: el error
# típico del RPS medido ahora (0.000159) es MENOR que el 0.00026 del comentario de
# arriba, porque aquel promediaba las candidatas que la búsqueda miró y este es la
# mediana de seis fijadas de antemano; `relative_perf_diff` sola da 0.000265 y
# reproduce la cifra documentada, así que el método es el mismo y no hay contradicción.
# Segunda: la razón `ee_bloque / ee_iid` se queda entre 0.90 y 1.11 en los dieciocho
# casos, es decir que en diferencias EMPAREJADAS no queda correlación intra-jornada que
# recuperar — el remuestreo por bloques y el error típico iid coinciden.
TOLERANCIAS: dict[str, float] = {"rps": TOLERANCIA,
                                 "accuracy": 2.5e-3,
                                 "ventaja": 6.8e-3}

# Los partidos sobre los que se midieron las de arriba. El error típico baja con la
# raíz del tamaño, así que al ensanchar el bloque de elección la constante caduca: por
# eso se escala en lugar de reescribirse. El escalado es conservador —medido, el error
# típico del acierto cae 1.68x al pasar de 1140 a 3040 partidos y la raíz predice
# 1.63x, porque la desviación por partido también baja cuando el ajuste se estabiliza
# sobre una ventana más larga—, de modo que la tolerancia escalada queda un pelo por
# encima de la medida y la parada peca de pronta, que es la dirección buena.
N_TOLERANCIA = 1140


def tolerancia_de(criterio: str = "rps", n: int | None = None) -> float:
    """La mejora mínima para adoptar, en unidades del criterio y escalada al tamaño.

    `n` son los partidos del bloque sobre el que se busca. Si se omite, devuelve la
    tolerancia tal como se midió, sobre los 1140 del bloque de selección.
    """
    if criterio not in TOLERANCIAS:
        raise KeyError(f"criterio desconocido: {criterio!r}; "
                       f"los medidos son {sorted(TOLERANCIAS)}")
    base = TOLERANCIAS[criterio]
    if n is None or n <= 0:
        return base
    return base * float(np.sqrt(N_TOLERANCIA / n))


# La mejora no puede ser de una sola temporada.
#
# `TOLERANCIA` compara medias, y una media se compra con una temporada. Medido sobre
# el bloque de selección, en ganancia de RPS por temporada —positiva cuando el RPS
# baja, que es el signo con el que la traza reporta la mejora—:
#
#   4ª variable  out_minshare_max_diff  +0.000138  +0.000817  +0.000100
#   5ª variable  relative_perf_diff     +0.000661  +0.000183  +0.000010
#   6ª variable  n_missing_diff         -0.000317  -0.000174  +0.001276
#
# Las tres medias son +0.000351, +0.000285 y +0.000261, y las tres pasan la
# tolerancia. Pero la tercera EMPEORA dos de las tres temporadas y le debe el signo
# a 2021/22 sola, mientras que las dos primeras mejoran en las tres. Adoptar la
# sexta por su media es adoptar una temporada afortunada, y el trabajo pide
# maximizar también temporada a temporada.
#
# La condición que separa los dos casos es un jackknife de temporada: se quita la
# que más favorece a la candidata y se exige que la mejora siga siendo mejora. Quitar
# la mejor deja +0.000119, +0.000096 y -0.000246: la sexta cambia de signo y las dos
# primeras no. No es un umbral ajustado al resultado —no hay número que elegir— sino
# la pregunta de a quién le debe la media su signo.
#
# Es criterio de ELEGIBILIDAD, no de parada: una candidata con suerte no es motivo
# para dejar de buscar, así que el paso sigue con la siguiente. Y no se aplica en la
# pasada hacia atrás, donde no haría falta: lo que la poda quita es precisamente lo
# que no paga su sitio en el agregado, y una variable que solo rinde en una temporada
# es lo primero que tiene que poder salir.
CONSISTENCIA_MIN_TEMPORADAS = 2

# Cuántas de las cribadas mira la voraz en cada paso, y hasta dónde puede crecer el
# conjunto. Los dos números son el presupuesto de cómputo hecho explícito: el trabajo
# cuesta del orden de `ANCHURA × MAX_VARS` protocolos.
#
# `ANCHURA = 0` significa «todas», y es el valor por defecto porque el banco cabe
# entero: el régimen semanal son 65 candidatas, de las que la criba mide 62 —`ah_line`
# y las dos de fuerza no son evaluables solas, porque el preprocesado las retira del
# pliegue por colinealidad antes de que el modelo pida columna alguna— y la voraz
# prueba 59 en cada paso, descontada la semilla. Recortar por mérito individual tendría
# sentido si eso no cupiese en una tarde, y cabe. La criba sigue ejecutándose y sigue
# siendo informativa —ordena las candidatas y es la tabla que dice qué vale cada
# variable por sí misma—, pero su orden es LECTURA y no recorte, de modo que el
# conjunto elegido no depende de dónde se habría puesto el corte y no se corre el
# riesgo propio del recorte: que una variable que no aporta sola pero sí acompañada
# no llegue a probarse nunca.
ANCHURA = 0

# `MAX_VARS` es un tope de presupuesto, no una creencia sobre el tamaño bueno. Lo que
# dice el tamaño bueno es la curva entera, y la curva está medida: se forzó la adopción
# en cada paso —tolerancia negativa— hasta agotar el banco, 1770 walk-forwards, y cada
# prefijo se puntuó DOS veces, dentro de las temporadas que eligieron el orden y en la
# que no lo eligió (corte de selección adelantado a 2021/22, de modo que 2021/22 juzga
# sin haber participado). RPS, que es pérdida:
#
#     k   entra                      dentro (760)   fuera (380)
#     3   (la semilla del precio)      0.193768       0.191987
#     4   out_minshare_max_diff        0.193290       0.191888
#     5   relative_perf_diff           0.192868       0.191878   <- mejor fuera
#     6   points_above_drop_diff       0.192247       0.193000
#     9   elo_mom_5_z_diff             0.191703       0.194967   <- mejor dentro
#    15   form_opp_elo_z_ewm_diff      0.191958       0.195947
#    25   h2h_draw_rate_ewm            0.192666       0.198062
#    40   form_sot_ewm_diff            0.195241       0.198998
#    60   matches_prev_diff            0.200916       0.200464
#
# Tres cosas, por orden de importancia.
#
# La primera es que la curva de dentro **señala el conjunto equivocado**. Su mínimo
# está en nueve variables, y ese mismo conjunto de nueve es, fuera, el séptimo peor de
# los quince primeros: 0.194967 contra 0.191987 de la semilla desnuda. La regla 5 ya
# decía que las cifras del bloque de selección no son citables por optimistas; lo que
# esto añade es que tampoco sirven para ORDENAR, que es un defecto peor y no estaba
# escrito en ninguna parte.
#
# La segunda es que la distancia entre las dos curvas mide el sobreajuste de la propia
# búsqueda: en k=9 valen 0.191703 y 0.194967, **0.003264** de diferencia, cinco veces
# el efecto que la voraz creía estar comprando. Esa distancia crece hasta k=23 y luego
# se cierra, porque con cuarenta variables el modelo ya es igual de malo en los dos
# sitios.
#
# La tercera es que fuera de la elección k=3, k=4 y k=5 son indistinguibles —0.191987,
# 0.191888, 0.191878 sobre trescientos ochenta partidos, con error típico del orden de
# 0.0007— y que desde k=6 todas empeoran, sin una sola excepción en cincuenta y cinco
# pasos. El modelo de sesenta variables es el peor de los sesenta y dos en las dos
# lecturas y también en acierto (50.00 % dentro, 49.47 % fuera, contra 52.63 % y
# 52.11 % de la semilla).
#
# De modo que el tope de veinticinco nunca llegó a ser el límite operativo: la búsqueda
# se detiene en cinco variables y la confirmación la devuelve a tres. Se deja donde
# está porque un tope más bajo sería ajustar el presupuesto al resultado.
MAX_VARS = 25

# Procesos que evalúan candidatas a la vez. Las candidatas de un paso son protocolos
# independientes —ninguna mira el resultado de otra— así que reparten sin cambiar una
# sola cifra, y eso está comprobado por igualdad exacta en la batería (sección 8).
#
# Lo que el reparto ahorra está medido en esta máquina (doce núcleos, 15.8 GB con 5.1
# libres), sobre doce candidatas del bloque de elección de 2019/20 —ochenta pliegues—:
# en el padre caliente una candidata cuesta 4.0 s, y repartida entre seis procesos
# 1.42 s, de modo que el reparto vale **2.8x**. No diez. La razón de que no escale con
# los núcleos es que un hijo es más lento que el padre aunque esté caliente —cada uno
# sostiene su propia matriz y su propia caché de estados de preprocesado, y la memoria
# libre no da para doce copias—, así que seis y diez procesos empatan (1.42 y 1.43
# s/candidata) y cuatro va por detrás (1.60). Con diez el arranque es además más caro
# (3.04 s/candidata en la primera llamada contra 2.48 con seis).
#
# Esa primera llamada se paga una vez y no por paso: el mismo lote cuesta 29.8 s la
# primera vez y 17.1 s la segunda, porque `loky` conserva los procesos. Envolver la
# búsqueda entera en un `parallel_backend` persistente se midió por si recuperaba algo
# más y no lo recupera: 16.8 s contra 17.1 s, un 1.8 % que cabe en el ruido entre
# pasadas. De ahí que aquí no haya contexto ninguno.
#
# `0` significa «los núcleos de la máquina menos dos», para dejar sitio al sistema.
# `1` fuerza la serie, y es lo que usan las pruebas y lo que conviene cuando algo
# falla: un error dentro de un proceso hijo llega envuelto y cuesta más de leer.
N_JOBS = 0

FAMILIA = "logistica"

# El conjunto de partida por defecto: las tres probabilidades implícitas en la cuota
# media de apertura. No es un adorno metodológico sino la forma de plantear bien la
# pregunta. Si la búsqueda arrancase del vacío, sus primeros pasos se irían en
# redescubrir el precio del mercado —que es, con diferencia, el mejor predictor
# individual del resultado— y la tabla de la memoria diría lo que ya se sabía. Con el
# precio dentro desde el primer paso, cada variable que entra después responde a la
# única pregunta interesante: qué añade a lo que el mercado ya sabe.
#
# Se toma del catálogo y no se escribe a mano para que una columna que deje de
# construirse deje también de sembrar la búsqueda, en lugar de detenerla con un error
# el día que alguien la ejecute.
#
# Las tres suman uno exactamente —desviación 7.5e-17, el épsilon de la máquina— así
# que la semilla es una matriz singular por construcción y una de las tres columnas es
# redundante en el sentido algebraico. La poda por colinealidad no lo ve porque mira
# pares y esta dependencia es de tres, de modo que la pregunta de si conviene quitar
# una a mano se midió: sobre el bloque de selección, las tres juntas dan RPS 0.19317 y
# las tres parejas posibles 0.19315, 0.19318 y 0.19319. El abanico completo son cuatro
# cienmilésimas, la cincuentava parte del error típico del RPS con novecientos
# partidos, porque la penalización L2 reparte el coeficiente indeterminado en lugar de
# atascarse. La singularidad es inocua medida, y se dejan las tres: la lectura de los
# coeficientes de un simplejo completo es más directa que la de dos coordenadas contra
# una categoría de referencia implícita.
SEMILLA_MERCADO: tuple[str, ...] = tuple(cat.MERCADO_CONSENSO)


# ===========================================================================
# 2. Guardas: que no se elija mirando donde se mide
# ===========================================================================
def _guarda_bloque(evaluar: str, esperado: str, quien: str) -> None:
    """Impide que una función de este módulo mire un bloque que no le toca.

    La separación entre elegir y medir no se sostiene sola: se sostiene si el código
    la impone. Un argumento `evaluar` libre habría bastado para que una búsqueda
    lanzada con prisa recorriese candidatas sobre el bloque de medición y arruinase
    la única cifra publicable del trabajo, sin dar ningún error y devolviendo una
    tabla de aspecto normal. De ahí que el bloque no sea un parámetro de estas
    funciones sino una propiedad suya.
    """
    if evaluar != esperado:
        raise PermissionError(
            f"{quien} solo opera sobre el bloque {esperado!r} y se ha pedido "
            f"{evaluar!r}. Elegir y medir son dos pasos con dos bloques distintos: "
            f"si la búsqueda mirase el bloque de medición, la cifra que la memoria "
            f"publica dejaría de ser una estimación del rendimiento futuro")


def candidatas(feat: pd.DataFrame | None = None, *,
               regime: str = "semanal", excluye=()) -> list[str]:
    """Las variables entre las que se elige, ya recortadas al régimen.

    El recorte por régimen va aquí y no después por una razón de método: el sistema
    que se publica emite su predicción con días de antelación, de modo que las
    columnas derivadas del precio de cierre no existen en el momento de predecir.
    Seleccionar sobre ellas y descartarlas al final produciría un conjunto elegido
    para un problema que no es el que se resuelve. Quien quiera el estudio
    retrospectivo —con el cierre dentro— lo pide por `regime`, a la vista.
    """
    feat = lb.datos() if feat is None else feat
    cols = lb.columnas_disponibles(feat)
    cols = C.regime_columns(cols, regime)
    fuera = set(map(str, excluye))
    return [c for c in cols if c not in fuera]


# ===========================================================================
# 3. Medir muchos conjuntos a la vez
# ===========================================================================
@dataclass(frozen=True)
class Cifras:
    """Lo que la búsqueda necesita saber de un conjunto medido.

    No es un `Ensayo` recortado por capricho. Un `Ensayo` lleva dentro las
    predicciones de los mil ciento cuarenta partidos del bloque y la tabla por
    temporada, y devolver eso de un proceso a otro sesenta y cinco veces por paso
    costaría más que la medición. Aquí viajan los cinco números con los que la voraz
    decide y los tres que luego se reportan, y nada más.

    Los nombres son los del `Ensayo` a propósito: `_lee` resuelve el criterio con
    `getattr`, de modo que una medición hecha en otro proceso y una hecha aquí se leen
    con el mismo código y no hay dos caminos que mantener sincronizados.
    """
    rps: float
    accuracy: float
    logloss: float
    brier: float
    roi: float
    roi_referencia: float
    ventaja: float
    n: int
    n_apuestas: int
    ausentes: tuple[str, ...] = ()
    por_temporada: tuple = ()

    @classmethod
    def de(cls, e) -> "Cifras":
        m, ap = e.metricas, e.economia["modelo"]
        eco = e.economia.get("por_temporada")
        temp: tuple = ()
        if eco is not None and not eco.empty:
            # El RPS por temporada no está en la tabla económica —viene de
            # `por_temporada`, que es de clasificación— así que se juntan aquí: la
            # lectura temporada a temporada que pide el trabajo necesita las dos.
            rps = e.por_temporada["rps"].to_dict()
            acc = e.por_temporada["accuracy"].to_dict()
            temp = tuple(
                (str(r.season), int(r.n_modelo), float(acc.get(r.season, float("nan"))),
                 float(rps.get(r.season, float("nan"))),
                 float(r.rendimiento_modelo), float(r.rendimiento_ref))
                for r in eco.itertuples())
        return cls(
            rps=e.rps, accuracy=e.accuracy, logloss=float(m["logloss"]),
            brier=float(m["brier"]), roi=e.roi, roi_referencia=e.roi_referencia,
            ventaja=e.ventaja, n=int(m["n"]), n_apuestas=int(ap.get("n_apuestas", 0)),
            ausentes=tuple(e.ausentes), por_temporada=temp)


def _mide_uno(cols, familia: str, cfg: pt.ProtocolConfig, kw: dict,
              feat: pd.DataFrame | None = None) -> Cifras | str:
    """Un conjunto, un protocolo completo, sobre el bloque de selección.

    Vive en el nivel del módulo —y no dentro de `adelante`, donde se usa— porque es
    lo que se envía a los procesos hijos y una función anidada no se puede enviar.
    Con `feat=None` lee la matriz con `lb.datos()` en lugar de recibirla: el hijo la
    tiene en su propia caché desde la primera tarea, y mandarle tres mil ochocientas
    filas por cada candidata costaría más que medirla.

    Devuelve el motivo como texto si el conjunto no se puede ajustar, en lugar de
    propagar. Una candidata inviable es un resultado del paso —se anota y el
    recorrido sigue—, no un motivo para tirar una búsqueda entera; y el texto cruza
    la frontera entre procesos sin depender de que la excepción sea serializable.
    """
    try:
        # La política va por delante de `kw` y no detrás: fija el valor por omisión de
        # la búsqueda —la unidad plana de `POLITICA_SELECCION`, que es la definición de
        # ROI del trabajo— sin quitarle a quien llama la posibilidad de pasar otra.
        return Cifras.de(lb.evalua(
            list(cols), familia, evaluar=BLOQUE_SELECCION,
            feat=lb.datos() if feat is None else feat, cfg=cfg,
            registrar=False, verbose=False,
            **{"politica": POLITICA_SELECCION, **kw}))
    except Exception as exc:                            # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"


def _nucleos(n_jobs: int) -> int:
    """Procesos efectivos. `0` deja dos núcleos al sistema; `1` fuerza la serie."""
    if int(n_jobs) != 0:
        return max(1, int(n_jobs))
    return max(1, (os.cpu_count() or 2) - 2)


def _lote(conjuntos, familia: str, cfg: pt.ProtocolConfig, *, kw: dict,
          n_jobs: int = N_JOBS, feat: pd.DataFrame | None = None,
          informa=None) -> list:
    """Mide varios conjuntos. Devuelve una `Cifras` —o el motivo— por conjunto.

    Reparte entre procesos cuando se le pide y la matriz es la canónica. Si quien
    llama trajo su propia matriz —las pruebas lo hacen, con marcos sintéticos— el
    reparto no es posible, porque el hijo leería del disco otra matriz distinta y
    devolvería cifras de otro experimento sin dar ningún error. En ese caso cae a la
    serie, que es la respuesta correcta: perder velocidad, nunca cambiar lo medido.
    """
    conjuntos = [tuple(c) for c in conjuntos]
    # Las tres que `_mide_uno` fija por su cuenta. Si llegasen en `kw` —y llegan con
    # facilidad, porque `criba`, `adelante` y `flotante` reenvían a ciegas lo que no
    # entienden— la llamada fallaría con un `TypeError` por argumento duplicado, uno
    # por conjunto, y la búsqueda lo contaría como «ninguna candidata resultó
    # evaluable»: un error de tecleo disfrazado de resultado. Se corta aquí, una vez.
    choque = sorted({"evaluar", "registrar", "verbose", "feat"} & set(kw))
    if choque:
        raise TypeError(
            f"{', '.join(choque)}: la medición de un lote los fija ella misma y no "
            f"se pueden reenviar; para callar la traza use el `verbose` de la "
            f"función que llama, no el del banco")
    # Se consulta la caché del banco en lugar de llamar a `lb.datos()` porque la
    # pregunta es «¿es esta matriz la que leería un hijo?» y llamar la respondería
    # leyendo el parquet, que es justamente el efecto que no se quiere: una batería
    # con marcos sintéticos tocaría el disco solo para decidir cómo repartir.
    propia = None if (feat is None or feat is lb._feat_cache) else feat
    n = 1 if propia is not None else _nucleos(n_jobs)

    if n == 1 or len(conjuntos) == 1:
        bruto = [_mide_uno(c, familia, cfg, kw, propia) for c in conjuntos] \
            if informa is None else None
        if bruto is None:
            bruto = []
            for i, cols in enumerate(conjuntos, 1):
                r = _mide_uno(cols, familia, cfg, kw, propia)
                informa(i, cols, r)
                bruto.append(r)
        return bruto

    from joblib import Parallel, delayed
    # `loky` mantiene vivos los procesos entre llamadas, así que la caché de estados
    # de preprocesado de cada hijo se calienta una vez en el primer paso y sirve para
    # los veinticinco. Sin reutilización el reparto no compensaría: ajustar los tres
    # estados cuesta más que medir una candidata.
    bruto = list(Parallel(n_jobs=n, backend="loky", batch_size=1)(
        delayed(_mide_uno)(cols, familia, cfg, kw) for cols in conjuntos))
    if informa is not None:
        for i, (cols, r) in enumerate(zip(conjuntos, bruto), 1):
            informa(i, cols, r)
    return bruto


# ===========================================================================
# 4. La criba: cada variable sola contra el resultado
# ===========================================================================
def _ordena(tabla: pd.DataFrame, criterio: str = "rps") -> list[str]:
    """Las candidatas de una criba, de la que más promete a la que menos.

    Ordenar SIEMPRE por RPS —que es lo que hacían los tres sitios que ahora llaman
    aquí— es un defecto cuando el criterio no es el RPS, y no uno cosmético: con
    `anchura > 0` este orden decide qué candidatas se llegan a mirar en cada paso, de
    modo que una búsqueda guiada por acierto o por ventaja miraba primero lo que
    prometía en una métrica que había dejado de guiar. Con `ANCHURA = 0` no cambia
    nada —se recorre el banco entero— pero el orden del informe tampoco era el del
    criterio, y eso ya se lee mal.
    """
    if criterio not in CRITERIOS:
        raise KeyError(f"criterio desconocido: {criterio!r}; "
                       f"los admitidos son {sorted(CRITERIOS)}")
    orden = tabla.sort_values(criterio, ascending=CRITERIOS[criterio] < 0)
    return [str(c) for c in orden["candidata"]]


def criba(cols=None, familia: str = FAMILIA, *, feat: pd.DataFrame | None = None,
          cfg: pt.ProtocolConfig | None = None, regime: str = "semanal",
          criterio: str = "rps", guardar: bool = True, verbose: bool = True,
          n_jobs: int = N_JOBS, **kw) -> pd.DataFrame:
    """Un ensayo por variable, cada una sola, sobre el bloque de selección.

    Es un paso de selección con el conjunto base vacío, medido con el mismo
    protocolo que todo lo demás: su orden no sería comparable con lo que venga
    después si se midiese de otra manera.

    Devuelve la tabla ordenada por `criterio`, de mejor a peor. La cabeza son las
    variables que más dicen por sí solas —el precio del mercado y el Elo, casi con
    seguridad— y la
    cola, las que no dicen nada sin compañía. Que una variable esté en la cola no
    demuestra que sea inútil: demuestra que no es útil sola, que es una afirmación
    más modesta y la única que este experimento sostiene. Con `ANCHURA = 0` la tabla
    ya no recorta el espacio de búsqueda y se lee solo como eso, un retrato de
    variable a variable.
    """
    _guarda_bloque(kw.pop("evaluar", BLOQUE_SELECCION), BLOQUE_SELECCION,
                   "la criba")
    if criterio not in CRITERIOS:
        raise KeyError(f"criterio desconocido: {criterio!r}; "
                       f"los admitidos son {sorted(CRITERIOS)}")
    feat = lb.datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    cols = [str(c) for c in cols] if cols is not None else \
        candidatas(feat, regime=regime)
    if not cols:
        raise ValueError("no hay ninguna candidata que cribar")

    if verbose:
        print(f"Criba: {len(cols)} variables, una a una, sobre el bloque "
              f"{BLOQUE_SELECCION!r} ({familia}), {_nucleos(n_jobs)} proceso(s).",
              flush=True)

    def _informa(i, c, r):
        if not verbose:
            return
        if isinstance(r, str):
            print(f"  [{i:>3}/{len(cols)}] {c[0]:<40} no evaluable — {r}",
                  flush=True)
        else:
            print(f"  [{i:>3}/{len(cols)}] {c[0]:<40} RPS {r.rps:.5f}  "
                  f"acc {r.accuracy:6.2%}  ventaja {r.ventaja:+.2%}", flush=True)

    res = _lote([[c] for c in cols], familia, cfg, kw=kw, n_jobs=n_jobs,
                feat=feat, informa=_informa)
    filas = [{"candidata": c, "accuracy": r.accuracy, "rps": r.rps,
              "logloss": r.logloss, "brier": r.brier, "roi": r.roi,
              "roi_referencia": r.roi_referencia, "ventaja": r.ventaja,
              "n": r.n, "n_apuestas": r.n_apuestas}
             for c, r in zip(cols, res) if not isinstance(r, str)]
    fallos = [{"candidata": c, "motivo": r}
              for c, r in zip(cols, res) if isinstance(r, str)]
    if not filas:
        raise ValueError("ninguna candidata de la criba resultó evaluable")

    tabla = (pd.DataFrame(filas)
             .sort_values(criterio, ascending=CRITERIOS[criterio] < 0)
             .reset_index(drop=True))
    tabla.insert(0, "orden", np.arange(1, len(tabla) + 1))
    if fallos:
        tabla.attrs["no_evaluables"] = fallos
    if guardar:
        OUT.mkdir(parents=True, exist_ok=True)
        tabla.to_parquet(CRIBA, index=False)
    return tabla


# ===========================================================================
# 4. La voraz
# ===========================================================================
@dataclass
class Paso:
    """Lo que ocurrió en un paso de la búsqueda. Una fila de la traza."""
    paso: int
    # `adelante` o `atras`. Sin este campo la traza de una búsqueda flotante no se
    # puede leer: una fila con `elegida` y `mejora` no dice por sí sola si la
    # variable entró o salió, y son las dos lecturas opuestas.
    fase: str
    elegida: str
    n_probadas: int
    criterio: str
    valor: float
    mejora: float
    accuracy: float
    rps: float
    roi: float
    roi_referencia: float
    ventaja: float
    n_variables: int
    conjunto: tuple[str, ...]
    adoptada: bool = True
    # Mejora que le queda a la elegida tras descontarle su mejor temporada, y cuántas
    # candidatas del paso quedaron fuera por no sobrevivir a ese descuento. Sin las dos
    # la traza no explica por qué un paso adoptó la segunda mejor del paso y no la
    # mejor. `nan` y 0 cuando la exigencia no se pudo aplicar.
    jackknife: float = float("nan")
    n_inconsistentes: int = 0

    def fila(self) -> dict:
        d = self.__dict__.copy()
        d["conjunto"] = " + ".join(self.conjunto)
        return d


@dataclass
class Seleccion:
    """El resultado de una búsqueda: el conjunto, la traza y con qué se obtuvo."""
    conjunto: tuple[str, ...]
    familia: str
    criterio: str
    pasos: list[Paso] = field(default_factory=list)
    semilla: tuple[str, ...] = ()
    corte: str = pt.CORTE_SELECCION
    protocolo: str = ""
    regime: str = "semanal"
    motivo_parada: str = ""
    n_podadas: int = 0
    vueltas: int = 0
    # Lo que dijo la cuarta puerta. `confirmado` es None mientras no se haya
    # preguntado, y `propuesto` guarda lo que la voraz había cerrado, porque un
    # conjunto rechazado por la confirmación es un resultado y no una nota al pie.
    confirmado: bool | None = None
    confirmacion: dict = field(default_factory=dict)
    propuesto: tuple[str, ...] = ()
    # Respaldo del recuento de pasos para un objeto reconstruido desde disco, donde
    # `pasos` viene vacío porque el camino vive en `traza.parquet` y no se reescribe.
    # Sin esto, volver a cerrar el conjunto borraría del JSON cuántos pasos se dieron.
    n_pasos_previos: int = 0

    @property
    def traza(self) -> pd.DataFrame:
        """La búsqueda entera, paso a paso, tal como se anota en disco."""
        return pd.DataFrame([p.fila() for p in self.pasos])

    def resumen(self) -> str:
        # El último paso adoptado, no el último anotado. El paso que detiene la
        # búsqueda se anota igualmente —es la evidencia de que se paró por algo— pero
        # sus cifras son las del conjunto que se RECHAZÓ, y leerlas aquí haría que el
        # resumen atribuyese al conjunto elegido las cifras de uno que no es.
        ult = next((p for p in reversed(self.pasos) if p.adoptada), None)
        cab = (f"Conjunto de {len(self.conjunto)} variables "
               f"({self.familia}, criterio {self.criterio}, corte {self.corte})"
               f"\n  parada: {self.motivo_parada}")
        if self.confirmado is False:
            # Con el conjunto revertido, las cifras del último paso adoptado son las
            # del conjunto que se rechazó. Decirlo y no imprimirlas.
            return (cab + f"\n  propuesto y rechazado: "
                          f"{len(self.propuesto)} variables. Las cifras del bloque "
                          f"de selección eran suyas y no del conjunto que queda.")
        if ult is None:
            return cab
        return (cab + f"\n  en el bloque de SELECCIÓN (no citable): "
                      f"acierto {ult.accuracy:6.2%}  RPS {ult.rps:.5f}  "
                      f"ventaja {ult.ventaja:+.2%}")

    def __repr__(self) -> str:              # pragma: no cover - cosmético
        return (f"<Seleccion {len(self.conjunto)}v {self.familia} "
                f"{self.criterio} corte={self.corte}>")


def _sello(familia: str, criterio: str, semilla, pool, cfg, regime: str) -> str:
    """Huella de una búsqueda, para reconocer un punto de guardado que sirve.

    Reanudar una búsqueda con otro pool o con otro protocolo produciría una traza
    que mezcla dos experimentos y no es la de ninguno. La huella cubre todo lo que,
    de cambiar, obliga a empezar de cero.
    """
    crudo = json.dumps({
        "familia": familia, "criterio": criterio,
        "semilla": sorted(map(str, semilla)), "pool": sorted(map(str, pool)),
        "cfg": cfg.describe(), "regime": regime,
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(crudo.encode("utf-8")).hexdigest()[:12]


def adelante(pool=None, familia: str = FAMILIA, *, semilla=(),
             criterio: str = "rps", tolerancia: float | None = None,
             anchura: int = ANCHURA, max_vars: int = MAX_VARS,
             feat: pd.DataFrame | None = None,
             cfg: pt.ProtocolConfig | None = None, regime: str = "semanal",
             guardar: bool = True, verbose: bool = True,
             n_jobs: int = N_JOBS, sel: "Seleccion | None" = None,
             **kw) -> Seleccion:
    """Selección hacia delante sobre el bloque de selección. La función central.

    En cada paso prueba `conjunto + una candidata` para cada candidata del pool,
    adopta la que más mejora el criterio y repite. Se detiene cuando la mejor mejora
    no llega a `tolerancia`, cuando el conjunto alcanza `max_vars` o cuando se agota
    el pool.

    «La que más mejora» se entiende entre las ELEGIBLES: una candidata cuya ganancia
    no sobrevive a quitarle su mejor temporada le debe la media a esa temporada y no
    compite, por lo que un paso puede adoptar la segunda mejor del paso. El por qué
    está medido arriba, junto a `CONSISTENCIA_MIN_TEMPORADAS`.

    `tolerancia` por omisión es la de `criterio`, que es la medida para él y no la del
    RPS: pasar la del RPS a una búsqueda guiada por acierto o por ventaja equivale a
    no poner umbral, porque 2e-4 en esas dos escalas son 0.02 puntos porcentuales. El
    detalle está arriba, en `TOLERANCIAS`.

    `pool` es la lista de candidatas y se espera que venga ya ordenada por `criba`:
    de ella se toman las `anchura` primeras —todas, si es 0— y esas son las que se
    recorren en TODOS los pasos. Fijar el pool una vez, en lugar de recribar tras
    cada adopción, es lo que mantiene el coste en `anchura × max_vars` protocolos.

    `semilla` permite arrancar de un conjunto que ya se da por bueno, y lo previsto es
    pasarle `SEMILLA_MERCADO`: la pregunta que la memoria responde no es «¿qué predice
    el resultado?» —a lo que la respuesta aburrida es el precio— sino «¿qué añade algo
    al precio?». El valor por defecto es el conjunto vacío para que arrancar de la
    nada siga siendo posible y sea siempre una decisión escrita en la llamada.

    `sel` permite continuar una búsqueda en curso en lugar de abrir una: es lo que
    usa `flotante`, que alterna avance y poda y necesita que los pasos de las dos
    fases se anoten en una sola traza.

    No mira el bloque de medición ni una vez. Esa es toda la garantía que ofrece y es
    la que importa.
    """
    if criterio not in CRITERIOS:
        raise ValueError(f"criterio desconocido: {criterio!r}; "
                         f"declarados {sorted(CRITERIOS)}")
    _guarda_bloque(kw.pop("evaluar", BLOQUE_SELECCION), BLOQUE_SELECCION,
                   "la búsqueda hacia delante")

    feat = lb.datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    signo = CRITERIOS[criterio]
    if tolerancia is None:
        # El tamaño del bloque sale del propio protocolo y no de una constante: con
        # el arranque bajado y el corte movido, el bloque de elección pasa de 1140
        # partidos a 3040, y la tolerancia tiene que acompañarlo. Si el marco no trae
        # el calendario —las pruebas pasan marcos sintéticos— no hay tamaño que medir
        # y se usa la tolerancia tal como se midió, sin escalar.
        n_bloque = (int(pt._bloque_evaluable(feat, cfg, BLOQUE_SELECCION).sum())
                    if {C.SPLIT, "season"} <= set(feat.columns) else 0)
        tolerancia = tolerancia_de(criterio, n_bloque)
        if verbose:
            print(f"Tolerancia de {criterio!r}: {tolerancia:.6f} "
                  f"({n_bloque or N_TOLERANCIA} partidos en el bloque de "
                  f"elección).", flush=True)

    if pool is None:
        pool = _ordena(criba(familia=familia, feat=feat, cfg=cfg, regime=regime,
                             criterio=criterio, guardar=guardar, verbose=verbose,
                             n_jobs=n_jobs, **kw), criterio)
    elif isinstance(pool, pd.DataFrame):
        pool = _ordena(pool, criterio)
    pool = [str(c) for c in pool if str(c) != "(base)"]

    arranca = sel is None
    actual = list(sel.conjunto) if sel is not None else \
        list(dict.fromkeys(str(c) for c in semilla))
    disponibles = [c for c in pool if c not in set(actual)]
    if int(anchura) > 0:
        disponibles = disponibles[:int(anchura)]
    if not disponibles:
        raise ValueError("el pool no deja ninguna candidata fuera de la semilla")

    if sel is None:
        sel = Seleccion(conjunto=tuple(actual), familia=familia,
                        criterio=criterio, semilla=tuple(actual),
                        corte=str(cfg.corte_seleccion), protocolo=cfg.describe(),
                        regime=regime)
    sello = _sello(familia, criterio, sel.semilla, pool, cfg, regime)
    paso0 = max((p.paso for p in sel.pasos), default=0)

    def _informa(paso, total):
        def f(i, cols, r):
            if not verbose:
                return
            c = cols[-1]
            if isinstance(r, str):
                print(f"  [{paso}·{i:>3}/{total}] {c:<38} no evaluable — {r}",
                      flush=True)
            elif c in r.ausentes:
                print(f"  [{paso}·{i:>3}/{total}] {c:<38} muda — la poda por "
                      f"colinealidad la descarta en todos los pliegues",
                      flush=True)
            else:
                print(f"  [{paso}·{i:>3}/{total}] {c:<38} RPS {r.rps:.5f}  "
                      f"acc {r.accuracy:6.2%}  ventaja {r.ventaja:+.2%}",
                      flush=True)
        return f

    # Punto de partida. Si no hay semilla no hay conjunto que medir todavía, y el
    # primer paso se juzga contra el infinito: cualquier variable mejora la nada.
    if actual:
        c0 = _lote([actual], familia, cfg, kw=kw, n_jobs=1, feat=feat)[0]
        if isinstance(c0, str):
            raise RuntimeError(f"la semilla no es evaluable — {c0}")
        mejor = signo * _lee(c0, criterio)
        base_temp = _por_temporada(c0, criterio)
        if verbose and arranca:
            print(f"Semilla: {len(actual)} vars · acc {c0.accuracy:6.2%}  "
                  f"RPS {c0.rps:.5f}  ventaja {c0.ventaja:+.2%}", flush=True)
    else:
        mejor = -np.inf
        base_temp = {}
        if verbose:
            print("Sin semilla: el primer paso arranca del conjunto vacío.",
                  flush=True)

    if verbose:
        print(f"Voraz: hasta {max_vars} variables, {len(disponibles)} candidatas "
              f"por paso, criterio {criterio} (tolerancia {tolerancia:g}, "
              f"{_nucleos(n_jobs)} proceso(s)).", flush=True)

    motivo = f"conjunto lleno: {max_vars} variables"
    for paso in range(paso0 + 1, paso0 + int(max_vars) - len(actual) + 1):
        if not disponibles:
            motivo = "pool agotado"
            break

        res = _lote([actual + [c] for c in disponibles], familia, cfg, kw=kw,
                    n_jobs=n_jobs, feat=feat,
                    informa=_informa(paso, len(disponibles)))

        # Una candidata que la poda por colinealidad descarta en todos los pliegues no
        # llega a entrar en ningún ajuste: lo que se ha medido es el conjunto actual
        # otra vez, y su «mejora» es cero por construcción. Dejarla competir sería
        # dejar que un empate degenerado gane el paso y detenga la búsqueda con un
        # motivo falso —así se detuvo la primera prueba de esta función, con `ah_line`,
        # que es una transformación monótona del precio del mercado y por tanto
        # redundante con la semilla—. Se saca del pool, no solo del paso: la poda la
        # descarta por ser casi combinación lineal de lo que ya hay, y añadir más
        # columnas solo puede aumentar la colinealidad del conjunto, nunca deshacerla.
        mudas = [c for c, r in zip(disponibles, res)
                 if not isinstance(r, str) and c in r.ausentes]
        puntos = [(signo * _lee(r, criterio), c, r)
                  for c, r in zip(disponibles, res)
                  if not isinstance(r, str) and c not in r.ausentes]
        if mudas:
            disponibles = [c for c in disponibles if c not in set(mudas)]
            if verbose:
                print(f"  (fuera del pool, mudas: {', '.join(mudas)})", flush=True)

        if not puntos:
            motivo = "ninguna candidata del paso resultó evaluable"
            break

        # Elegibilidad por consistencia. Una candidata cuya ganancia no sobrevive a
        # quitarle su mejor temporada le debe la media a esa temporada, y no compite:
        # el paso se decide entre las que siguen mejorando con una temporada menos.
        #
        # La condición se aplica solo a quien MEJORA la media. A quien la empeora no
        # hay nada que reprocharle de reparto —ya va a perder el paso por su cifra— y
        # etiquetarla de inconsistente dejaría la cuenta que se anota sin significado:
        # en un paso tardío la mayoría de las candidatas empeoran. Así el umbral no
        # interviene, y un camino forzado (`tolerancia` negativa) se lee igual que uno
        # normal: de las que mejoran, la mejor que no dependa de una temporada.
        jacks = {c: _jackknife(r, base_temp, criterio, signo)
                 for _, c, r in puntos}
        fuera = [c for v, c, _ in puntos if jacks[c] <= 0.0 and v > mejor]
        elegibles = [t for t in puntos if t[1] not in set(fuera)]
        if fuera and verbose:
            print(f"  (sin consistencia, fuera del paso: "
                  f"{', '.join(f'{c} [{jacks[c]:+.6f}]' for c in fuera)})",
                  flush=True)
        if not elegibles:
            motivo = (f"las {len(puntos)} candidatas del paso {paso} mejoraban la "
                      f"media y todas le debían la mejora a una sola temporada")
            if verbose:
                print(f"  -> parada: {motivo}", flush=True)
            break

        valor, elegida, cif = max(elegibles, key=lambda t: t[0])
        mejora = valor - mejor if np.isfinite(mejor) else float("nan")
        adopta = (not np.isfinite(mejor)) or (mejora >= tolerancia)

        sel.pasos.append(Paso(
            paso=paso, fase="adelante", elegida=elegida, n_probadas=len(puntos),
            criterio=criterio, valor=float(signo * valor), mejora=float(mejora),
            accuracy=cif.accuracy, rps=cif.rps, roi=cif.roi,
            roi_referencia=cif.roi_referencia, ventaja=cif.ventaja,
            n_variables=len(actual) + 1,
            conjunto=tuple(actual + [elegida]), adoptada=bool(adopta),
            jackknife=float(jacks[elegida]), n_inconsistentes=len(fuera)))

        if not adopta:
            # La mejor del paso no llega al umbral. Se anota igualmente —el paso que
            # detiene la búsqueda es parte de la evidencia de que se detuvo por algo
            # y no por cansancio— pero no se adopta.
            motivo = (f"la mejor candidata del paso {paso} ({elegida}) mejoraba "
                      f"{mejora:+.6f}, por debajo de la tolerancia {tolerancia:g}")
            if verbose:
                print(f"  -> parada: {motivo}", flush=True)
            break

        actual.append(elegida)
        disponibles = [c for c in disponibles if c != elegida]
        mejor = valor
        base_temp = _por_temporada(cif, criterio)
        sel.conjunto = tuple(actual)
        if verbose:
            print(f"  -> paso {paso}: entra {elegida}  ({criterio} "
                  f"{signo * valor:.5f}, mejora {mejora:+.6f})  "
                  f"conjunto de {len(actual)}", flush=True)
        if guardar:
            guarda(sel, sello=sello)

    sel.conjunto = tuple(actual)
    sel.motivo_parada = motivo
    if guardar:
        guarda(sel, sello=sello)
    if verbose:
        print("\n" + sel.resumen(), flush=True)
    return sel


def _lee(ensayo, criterio: str) -> float:
    """El valor del criterio en un ensayo, en su escala natural."""
    return float(getattr(ensayo, criterio))


# Dónde está cada criterio dentro de las filas de `Cifras.por_temporada`, que son
# (temporada, n_apuestas, acierto, rps, rendimiento, rendimiento_referencia). La
# ventaja no viaja como columna porque es una resta, y aquí se hace.
_COLUMNA_TEMPORADA = {"accuracy": 2, "rps": 3}


def _por_temporada(cif, criterio: str) -> dict[str, float]:
    """El criterio temporada a temporada, en la misma escala que el agregado."""
    i = _COLUMNA_TEMPORADA.get(criterio)
    if i is None:
        return {str(t[0]): float(t[4]) - float(t[5]) for t in cif.por_temporada}
    return {str(t[0]): float(t[i]) for t in cif.por_temporada}


def _jackknife(cif, base: dict[str, float], criterio: str,
               signo: int) -> float:
    """Ganancia media del criterio quitando la temporada que más la favorece.

    Positiva significa que la candidata sigue mejorando cuando se le descuenta su
    mejor temporada. `nan` significa que no hay reparto que mirar —no llegó tabla por
    temporada, o llega una sola— y entonces la exigencia no se puede aplicar y no se
    aplica: es la situación de un bloque de una temporada, no un caso a tapar.
    """
    ahora = _por_temporada(cif, criterio)
    g = [signo * (ahora[s] - base[s]) for s in ahora if s in base]
    if len(g) < CONSISTENCIA_MIN_TEMPORADAS:
        return float("nan")
    return (sum(g) - max(g)) / (len(g) - 1)


def atras(conjunto, familia: str = FAMILIA, *, criterio: str = "rps",
          proteger=(), tolerancia: float = 0.0, min_vars: int = 1,
          feat: pd.DataFrame | None = None,
          cfg: pt.ProtocolConfig | None = None, regime: str = "semanal",
          guardar: bool = True, verbose: bool = True, n_jobs: int = N_JOBS,
          sel: "Seleccion | None" = None, **kw) -> Seleccion:
    """Quita variables mientras quitarlas no empeore. La pasada hacia atrás.

    POR QUÉ HACE FALTA
    ------------------
    El avance puro no puede deshacer. Una variable que entró en el paso dos porque
    era lo mejor que había entonces puede quedar redundante en el paso nueve, cuando
    ya han entrado dos columnas que dicen lo mismo mejor; el avance no la revisa
    nunca y el conjunto final la arrastra. La redundancia no es inocua en una
    logística: dos columnas casi colineales reparten el coeficiente entre las dos,
    cada una con el doble de error típico, y el modelo queda peor estimado y menos
    interpretable sin que el criterio agregado lo denuncie con fuerza.

    Esta función recorre el conjunto quitando una variable cada vez y se queda con
    la que menos daño hace, mientras el daño no pase de `tolerancia`. Con
    `tolerancia = 0` quita solo lo que mejora o deja igual, que es el criterio
    estricto: nada sale a cambio de nada.

    `proteger` son las variables que no se ofrecen a la poda. Lo previsto es pasarle
    la semilla de mercado: el precio es la pregunta del trabajo —cuánto se le añade—
    y un conjunto sin él respondería a otra. Que salga por decisión escrita y no
    porque un paso lo encontró prescindible.
    """
    if criterio not in CRITERIOS:
        raise ValueError(f"criterio desconocido: {criterio!r}; "
                         f"declarados {sorted(CRITERIOS)}")
    _guarda_bloque(kw.pop("evaluar", BLOQUE_SELECCION), BLOQUE_SELECCION,
                   "la poda hacia atrás")

    feat = lb.datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    signo = CRITERIOS[criterio]
    actual = list(dict.fromkeys(str(c) for c in conjunto))
    fijas = set(map(str, proteger))

    if sel is None:
        sel = Seleccion(conjunto=tuple(actual), familia=familia,
                        criterio=criterio, semilla=tuple(sorted(fijas)),
                        corte=str(cfg.corte_seleccion), protocolo=cfg.describe(),
                        regime=regime)
    sello = _sello(familia, criterio, sel.semilla, actual, cfg, regime)

    c0 = _lote([actual], familia, cfg, kw=kw, n_jobs=1, feat=feat)[0]
    if isinstance(c0, str):
        raise RuntimeError(f"el conjunto de partida no es evaluable — {c0}")
    mejor = signo * _lee(c0, criterio)
    paso = max((p.paso for p in sel.pasos), default=0)
    quitadas = 0

    if verbose:
        print(f"Poda: {len(actual)} variables, {len(fijas & set(actual))} "
              f"protegidas, criterio {criterio} (tolerancia {tolerancia:g}).",
              flush=True)

    while True:
        ofrecidas = [c for c in actual if c not in fijas]
        if len(actual) - 1 < int(min_vars) or not ofrecidas:
            motivo = "no queda nada que poder quitar"
            break

        paso += 1
        total = len(ofrecidas)

        def _informa(i, cols, r, _t=total, _p=paso, _of=ofrecidas):
            if not verbose:
                return
            c = _of[i - 1]
            if isinstance(r, str):
                print(f"  [{_p}·{i:>3}/{_t}] sin {c:<34} no evaluable — {r}",
                      flush=True)
            else:
                print(f"  [{_p}·{i:>3}/{_t}] sin {c:<34} RPS {r.rps:.5f}  "
                      f"acc {r.accuracy:6.2%}  ventaja {r.ventaja:+.2%}",
                      flush=True)

        res = _lote([[x for x in actual if x != c] for c in ofrecidas],
                    familia, cfg, kw=kw, n_jobs=n_jobs, feat=feat,
                    informa=_informa)
        puntos = [(signo * _lee(r, criterio), c, r)
                  for c, r in zip(ofrecidas, res) if not isinstance(r, str)]
        if not puntos:
            motivo = "ninguna retirada del paso resultó evaluable"
            break

        valor, fuera, cif = max(puntos, key=lambda t: t[0])
        # Signo al revés que en el avance: aquí `mejora` positiva significa que el
        # conjunto es mejor SIN la variable. `-tolerancia` admite el empate y un
        # deterioro hasta ese tamaño, porque una variable que no paga su sitio es
        # una variable que sobra aunque su retirada cueste una diezmilésima.
        mejora = valor - mejor
        adopta = mejora >= -abs(tolerancia)

        sel.pasos.append(Paso(
            paso=paso, fase="atras", elegida=fuera, n_probadas=len(puntos),
            criterio=criterio, valor=float(signo * valor), mejora=float(mejora),
            accuracy=cif.accuracy, rps=cif.rps, roi=cif.roi,
            roi_referencia=cif.roi_referencia, ventaja=cif.ventaja,
            n_variables=len(actual) - 1,
            conjunto=tuple(x for x in actual if x != fuera),
            adoptada=bool(adopta)))

        if not adopta:
            motivo = (f"quitar la más prescindible del paso {paso} ({fuera}) "
                      f"costaba {mejora:+.6f}: todas pagan su sitio")
            if verbose:
                print(f"  -> parada: {motivo}", flush=True)
            break

        actual = [x for x in actual if x != fuera]
        mejor = valor
        quitadas += 1
        sel.conjunto = tuple(actual)
        if verbose:
            print(f"  -> paso {paso}: sale {fuera}  ({criterio} "
                  f"{signo * valor:.5f}, {mejora:+.6f})  conjunto de "
                  f"{len(actual)}", flush=True)
        if guardar:
            guarda(sel, sello=sello)

    sel.conjunto = tuple(actual)
    sel.motivo_parada = motivo
    sel.n_podadas = quitadas
    if guardar:
        guarda(sel, sello=sello)
    if verbose:
        print(f"Poda terminada: {quitadas} fuera, {len(actual)} dentro. {motivo}",
              flush=True)
    return sel


def flotante(pool=None, familia: str = FAMILIA, *, semilla=SEMILLA_MERCADO,
             criterio: str = "rps", tolerancia: float | None = None,
             anchura: int = ANCHURA, max_vars: int = MAX_VARS,
             max_vueltas: int = 6, feat: pd.DataFrame | None = None,
             cfg: pt.ProtocolConfig | None = None, regime: str = "semanal",
             guardar: bool = True, verbose: bool = True,
             n_jobs: int = N_JOBS, **kw) -> Seleccion:
    """Avance y poda alternados hasta que ninguna de las dos cosas cambia nada.

    Es la selección secuencial flotante, y la diferencia con el avance puro cabe en
    una frase: el avance explora un solo camino de longitud creciente y no vuelve,
    mientras que alternar con la poda permite abandonar una variable que dejó de
    hacer falta y poner otra en su lugar. No explora todas las combinaciones —eso
    son dos elevado a sesenta y cinco y no cabe en ninguna máquina— pero sí sale de
    los óptimos locales que el avance no puede abandonar por construcción.

    Termina cuando una vuelta completa no añade ni quita nada, que es el punto fijo
    del procedimiento, o al agotar `max_vueltas`. El tope existe porque la
    alternancia podría en principio oscilar —meter lo que la vuelta anterior quitó—
    y una búsqueda que no termina no es un procedimiento.

    La semilla de mercado se protege de la poda, por lo dicho en `atras`: la
    pregunta del trabajo es qué se le añade al precio, no si el precio sobra.
    """
    feat = lb.datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    if pool is None:
        pool = criba(familia=familia, feat=feat, cfg=cfg, regime=regime,
                     criterio=criterio, guardar=guardar, verbose=verbose,
                     n_jobs=n_jobs, **kw)
    if isinstance(pool, pd.DataFrame):
        pool = _ordena(pool, criterio)
    pool = [str(c) for c in pool if str(c) != "(base)"]

    sel = Seleccion(conjunto=tuple(dict.fromkeys(map(str, semilla))),
                    familia=familia, criterio=criterio,
                    semilla=tuple(dict.fromkeys(map(str, semilla))),
                    corte=str(cfg.corte_seleccion), protocolo=cfg.describe(),
                    regime=regime)
    comun = dict(familia=familia, criterio=criterio, feat=feat, cfg=cfg,
                 regime=regime, guardar=guardar, verbose=verbose,
                 n_jobs=n_jobs, **kw)

    for vuelta in range(1, int(max_vueltas) + 1):
        antes = tuple(sel.conjunto)
        if verbose:
            print(f"\n=== vuelta {vuelta}: avance desde {len(antes)} variables ===",
                  flush=True)
        sel = adelante(pool, semilla=sel.conjunto, tolerancia=tolerancia,
                       anchura=anchura, max_vars=max_vars, sel=sel, **comun)
        tras_avance = tuple(sel.conjunto)

        if verbose:
            print(f"\n=== vuelta {vuelta}: poda de {len(tras_avance)} variables "
                  f"===", flush=True)
        sel = atras(sel.conjunto, proteger=sel.semilla, tolerancia=0.0,
                    sel=sel, **comun)

        sel.vueltas = vuelta
        if tuple(sel.conjunto) == antes:
            sel.motivo_parada = (
                f"punto fijo en la vuelta {vuelta}: el avance no añadió nada que "
                f"la poda no quitase, y el conjunto volvió a ser el de entrada")
            break
        if tras_avance == antes and tuple(sel.conjunto) == antes:
            sel.motivo_parada = f"punto fijo en la vuelta {vuelta}"
            break
    else:
        sel.motivo_parada = (f"agotadas las {max_vueltas} vueltas sin punto fijo; "
                             f"el conjunto es el de la última poda")

    # La cuarta puerta, una sola vez y con el conjunto ya cerrado. Va aquí y no
    # dentro de `adelante` a propósito: preguntarlo en cada paso convertiría el bloque
    # de confirmación en un segundo bloque de selección —la voraz elegiría también
    # mirándolo— y dejaría de confirmar nada. Se pregunta al final, del conjunto
    # entero, y la respuesta solo puede quitar variables, nunca sugerir cuáles.
    sel.propuesto = tuple(sel.conjunto)
    try:
        res = confirma(sel.conjunto, semilla=sel.semilla, familia=familia,
                       feat=feat, cfg=cfg, verbose=verbose)
    except ValueError as exc:
        sel.confirmacion = {"motivo": f"no se pudo confirmar: {exc}"}
        sel.confirmado = None
    else:
        sel.confirmacion = res
        sel.confirmado = bool(res["confirmado"])
        if not sel.confirmado:
            sel.conjunto = tuple(sel.semilla)
            sel.motivo_parada = (
                f"{sel.motivo_parada}; RECHAZADO por la confirmación y revertido a "
                f"la semilla de {len(sel.semilla)} variables: {res['motivo']}")

    if guardar:
        guarda(sel, sello=_sello(familia, criterio, sel.semilla, pool, cfg,
                                 regime))
    if verbose:
        print("\n" + sel.resumen(), flush=True)
    return sel


def confirma(conjunto, semilla=SEMILLA_MERCADO, familia: str = FAMILIA, *,
             feat: pd.DataFrame | None = None,
             cfg: pt.ProtocolConfig | None = None,
             hiper: dict | None = None,
             n_boot: int = 4000, verbose: bool = True) -> dict:
    """La cuarta puerta: ¿gana el conjunto donde no se eligió a sí mismo?

    Puntúa el conjunto y su semilla sobre 2017/18-2018/19 —anteriores al bloque de
    selección, fuera de la elección por construcción— y devuelve la diferencia
    emparejada partido a partido con su intervalo por jornadas enteras.

    El criterio no tiene número que elegir, y eso es su virtud: se exige que lo que se
    adoptó mirando unos partidos siga siendo ganancia en partidos que no lo eligieron.
    No se pide que el intervalo excluya el cero —con setecientos sesenta partidos y un
    error típico de 0.00035 eso exigiría un efecto del doble del que la búsqueda cree
    estar comprando, y rechazaría cualquier cosa, siempre—, se pide sencillamente que
    el signo aguante. Una variable que mejora en el bloque que la eligió y empeora en
    el que no, no es una variable que aporte: es la huella de la propia búsqueda.

    Las dos variables que la voraz adoptó NO pasan esta puerta: cuestan +0.000424 y
    +0.000507 de RPS en las dos temporadas (positivo es pérdida), y agrupadas con el
    bloque de medición suman +0.000551 sobre mil novecientos partidos, con intervalo
    [+0.000104, +0.001005] que excluye el cero.

    La comparación se hace sobre la INTERSECCIÓN de los partidos que los dos conjuntos
    puntúan, y no sobre las dos medias por separado: los pliegues que se saltan por
    falta de entrenamiento o por una columna constante pueden no ser los mismos en los
    dos, y promediar sobre censos distintos mezcla la diferencia entre conjuntos con la
    diferencia entre partidos.

    `hiper` se acepta porque un conjunto puede haberse elegido con una penalización que
    no es la declarada en `laboratorio.HIPER`, y entonces la puerta tiene que juzgarlo
    con la misma con la que se eligió: un conjunto de doce columnas adoptado bajo
    `C=0.1, L1` y confirmado bajo `C=0.3, L2` se estaría puntuando con un castigo de
    varianza que su elección no tuvo, y el rechazo no distinguiría la falta de señal
    del cambio de configuración. La semilla y el conjunto se puntúan siempre con la
    MISMA, que es lo que mantiene la diferencia emparejada interpretable.
    """
    feat = lb.datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    conjunto = [str(c) for c in conjunto]
    semilla = [str(c) for c in semilla]
    # Las temporadas de confirmación son las de arranque que no lo necesitan: todas
    # menos la primera del histórico, que no tiene pasado del que sacar forma ni racha.
    temporadas = [s for s in cfg.warmup_seasons if s not in WARMUP_CONFIRMACION]
    if not temporadas:
        raise ValueError(
            "no hay bloque de confirmación: el arranque ya está en su mínimo, así "
            "que no quedan temporadas anteriores a la selección que abrir")
    abierto = pt.ProtocolConfig(**{**cfg.__dict__,
                                   "warmup_seasons": list(WARMUP_CONFIRMACION)})
    if set(conjunto) == set(semilla):
        return {"confirmado": True, "n": 0, "temporadas": temporadas,
                "motivo": "el conjunto es la semilla; no hay nada que confirmar"}

    clave = ["season", "jornada", "match_key"]

    def puntua(cols):
        e = lb.evalua(cols, familia, hiper=hiper, cfg=abierto, feat=feat,
                      evaluar="validacion", n_boot=0, registrar=False,
                      verbose=False)
        p = e.pred.loc[ev.evaluable(e.pred)].copy()
        p["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
        p = p.loc[p["season"].astype(str).isin(temporadas)]
        return p.set_index(clave)

    if verbose:
        print(f"\n=== confirmación sobre {', '.join(temporadas)} "
              f"(fuera de la elección) ===", flush=True)
    a, b = puntua(semilla), puntua(conjunto)
    com = a.index.intersection(b.index)
    if len(com) == 0:
        raise ValueError("la confirmación no comparte ningún partido entre los dos "
                         "conjuntos; revise el bloque abierto")
    dif = pd.DataFrame({"dif": b.loc[com, "rps"] - a.loc[com, "rps"]}).reset_index()
    iv = ev.block_bootstrap(dif, lambda d: float(d["dif"].mean()),
                            n_boot=n_boot, seed=7)
    # `cuesta` es RPS del conjunto menos RPS de la semilla, y el RPS es una pérdida:
    # POSITIVO significa que lo adoptado empeora donde no se eligió.
    cuesta = float(dif["dif"].mean())
    res = {"confirmado": cuesta < 0, "cuesta": cuesta, "ee": iv["ee"],
           "inf": iv["inf"], "sup": iv["sup"], "n": int(len(com)),
           "temporadas": temporadas,
           "rps_semilla": float(a.loc[com, "rps"].mean()),
           "rps_conjunto": float(b.loc[com, "rps"].mean()),
           "motivo": ""}
    res["motivo"] = (
        f"las {len(conjunto) - len(semilla)} variables adoptadas "
        f"{'ganan' if res['confirmado'] else 'PIERDEN'} "
        f"{abs(cuesta):.6f} de RPS sobre los {res['n']} partidos de "
        f"{', '.join(temporadas)}, que no participaron en elegirlas "
        f"[{iv['inf']:+.6f}, {iv['sup']:+.6f}]")
    if verbose:
        print(f"  semilla  RPS {res['rps_semilla']:.6f}", flush=True)
        print(f"  conjunto RPS {res['rps_conjunto']:.6f}", flush=True)
        print(f"  {'CONFIRMADO' if res['confirmado'] else 'RECHAZADO'}: "
              f"{res['motivo']}", flush=True)
    return res


# ===========================================================================
# 5. La medición: una sola pasada, con el conjunto ya cerrado
# ===========================================================================
def mide(conjunto, familia: str = FAMILIA, *, nombre: str | None = None,
         feat: pd.DataFrame | None = None, cfg: pt.ProtocolConfig | None = None,
         hiper: dict | None = None, verbose: bool = True, **kw):
    """Puntúa un conjunto cerrado sobre el bloque de medición. Se llama una vez.

    Devuelve el `Ensayo` del banco, con lo que eso trae: métricas agregadas, tabla
    por temporada y comparación económica con el favorito de Bet365 sobre exactamente
    los mismos partidos.

    Que se llame una vez no es una restricción del código sino del método, y conviene
    ser claro sobre la diferencia. Nada impide ejecutar esta función veinte veces con
    veinte conjuntos; lo que ocurre si se hace es que el bloque de medición se
    convierte en un segundo bloque de selección, la cifra que publica deja de
    significar lo que dice, y no queda ningún bloque limpio salvo el holdout, que se
    gasta también. La disciplina la pone quien ejecuta, y la traza deja constancia de
    cuántas veces se ejecutó.
    """
    _guarda_bloque(kw.pop("evaluar", BLOQUE_MEDICION), BLOQUE_MEDICION,
                   "la medición del conjunto cerrado")
    cols = list(dict.fromkeys(str(c) for c in conjunto))
    if not cols:
        raise ValueError("no hay conjunto que medir")
    feat = lb.datos() if feat is None else feat
    cfg = cfg or pt.ProtocolConfig()
    nombre = nombre or f"medicion·{familia}·{len(cols)}v"

    ensayo = lb.evalua(cols, familia, nombre=nombre, hiper=hiper,
                       evaluar=BLOQUE_MEDICION, feat=feat, cfg=cfg,
                       verbose=False, **kw)
    if verbose:
        print(f"Bloque de MEDICIÓN (temporadas >= {cfg.corte_seleccion}), "
              f"conjunto cerrado de {len(cols)} variables.\n")
        print(ensayo.resumen(), flush=True)
        print("\n" + informe(ensayo).to_string(index=False), flush=True)
    return ensayo


def informe(ensayo) -> pd.DataFrame:
    """La tabla citable: temporada a temporada, el modelo frente a Bet365.

    Es la lectura que decide si un conjunto ha mejorado al mercado o ha tenido suerte
    una temporada. Se reduce a lo que la memoria reporta —acierto y rendimiento de
    cada uno, y la diferencia— porque una tabla con veinte columnas no se lee y esta
    tiene que leerse.

    Los dos aciertos que aparecen no son el mismo número medido dos veces y conviene
    no confundirlos. `acierto` es el del modelo sobre TODOS los partidos de la
    temporada: es la métrica de clasificación. `acierto_apostados` y `acierto_b365`
    son los de cada apostante sobre los partidos en los que apostó, que son los que
    explican el rendimiento y no tienen por qué ser los mismos partidos ni el mismo
    número de ellos.
    """
    t = ensayo.tabla()
    # El renombrado NO toca `accuracy` y `acierto_modelo` a la vez a un mismo nombre:
    # son subconjuntos distintos —toda la temporada frente a los partidos apostados—
    # y colisionarlos dejaría la tabla con dos columnas homónimas que dicen cosas
    # diferentes.
    t = t.rename(columns={
        "season": "temporada", "n": "partidos", "accuracy": "acierto",
        "acierto_modelo": "acierto_apostados", "acierto_referencia": "acierto_b365",
        "rendimiento_modelo": "roi_modelo",
        "rendimiento_referencia": "roi_b365", "diferencia": "ventaja"})
    cols = [c for c in ("temporada", "partidos", "acierto", "rps",
                        "acierto_apostados", "acierto_b365",
                        "roi_modelo", "roi_b365", "ventaja")
            if c in t.columns]
    return t[cols]


# ===========================================================================
# 6. Persistencia
# ===========================================================================
def guarda(sel: Seleccion, *, sello: str = "", ruta: Path | None = None) -> Path:
    """Escribe la traza y el conjunto. Se llama tras cada paso, no al final.

    Guardar por pasos convierte una búsqueda de horas en algo que sobrevive a un
    cierre del cuaderno o a un portátil que se suspende. El coste es dos ficheros
    pequeños por paso; la alternativa es perder la tarde entera por el último paso.
    """
    ruta = ruta or TRAZA
    ruta.parent.mkdir(parents=True, exist_ok=True)
    traza = sel.traza
    if not traza.empty:
        traza.to_parquet(ruta, index=False)
    CONJUNTO.write_text(json.dumps({
        "conjunto": list(sel.conjunto),
        "semilla": list(sel.semilla),
        "familia": sel.familia,
        "criterio": sel.criterio,
        "corte_seleccion": sel.corte,
        "regime": sel.regime,
        "protocolo": sel.protocolo,
        "motivo_parada": sel.motivo_parada,
        "n_pasos": len(sel.pasos) or sel.n_pasos_previos,
        "n_podadas": sel.n_podadas,
        "vueltas": sel.vueltas,
        # Lo que la voraz había cerrado y lo que dijo la confirmación. Van al JSON
        # porque un conjunto revertido a la semilla y una búsqueda que no encontró
        # nada se leen igual desde fuera, y no son lo mismo.
        "propuesto": list(sel.propuesto or sel.conjunto),
        "confirmado": sel.confirmado,
        "confirmacion": sel.confirmacion,
        "sello": sello,
        "momento": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return ruta


def carga(ruta: Path | None = None) -> dict:
    """Lee el conjunto elegido y con qué se eligió.

    Es la puerta por la que `build_models.py` recoge el resultado de la selección sin
    tenerlo copiado a mano en ningún sitio. Devuelve el diccionario entero y no solo
    la lista de columnas a propósito: un conjunto sin su corte y sin su protocolo no
    es reproducible, y quien lo consuma tiene que poder comprobar que coinciden con
    los suyos antes de usarlo.
    """
    ruta = ruta or CONJUNTO
    if not ruta.exists():
        raise FileNotFoundError(
            f"no hay ninguna selección guardada en {ruta}; ejecute "
            f"`python seleccion.py` o `adelante()` desde el cuaderno")
    return json.loads(ruta.read_text(encoding="utf-8"))


def traza(ruta: Path | None = None) -> pd.DataFrame:
    """La búsqueda paso a paso, leída de disco."""
    ruta = ruta or TRAZA
    return pd.read_parquet(ruta) if ruta.exists() else pd.DataFrame()


# ===========================================================================
# 7. Línea de órdenes
# ===========================================================================
def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Selección de variables con el bloque de elección separado.")
    p.add_argument("--familia", default=FAMILIA, choices=list(lb.FAMILIAS))
    p.add_argument("--criterio", default="rps", choices=sorted(CRITERIOS))
    p.add_argument("--regime", default="semanal", choices=list(C.REGIMES))
    p.add_argument("--anchura", type=int, default=ANCHURA,
                   help="candidatas que la voraz mira en cada paso")
    p.add_argument("--max-vars", type=int, default=MAX_VARS)
    # Sin valor por omisión a propósito: la toma `adelante` de `TOLERANCIAS`, que es
    # la del criterio pedido y escalada al tamaño del bloque. Clavar aquí la del RPS
    # era el defecto; pasar `--tolerancia` sigue siendo posible y manda sobre ella.
    p.add_argument("--tolerancia", type=float, default=None)
    p.add_argument("--semilla", nargs="*", default=list(SEMILLA_MERCADO),
                   help="conjunto de partida; por defecto, el precio del mercado")
    p.add_argument("--busqueda", default="flotante",
                   choices=("adelante", "flotante"),
                   help="solo avance, o avance y poda alternados hasta el punto fijo")
    p.add_argument("--max-vueltas", type=int, default=6,
                   help="tope de vueltas de avance y poda; solo con --busqueda "
                        "flotante")
    p.add_argument("--n-jobs", type=int, default=N_JOBS,
                   help="procesos que miden candidatas a la vez; 0 = núcleos - 2")
    # El remuestreo no interviene en ninguna comparación de la búsqueda —se decide con
    # la cifra puntual— y cuesta `n_boot` reordenaciones por protocolo. Con mil y pico
    # protocolos en una búsqueda flotante eso es la diferencia entre media hora y una
    # tarde, así que por defecto la búsqueda no remuestrea y el intervalo se calcula
    # una sola vez, al medir el conjunto ganador.
    p.add_argument("--n-boot", type=int, default=0,
                   help="remuestreos durante la búsqueda; 0 los desactiva")
    p.add_argument("--criba-solo", action="store_true",
                   help="ejecuta solo la criba y sale")
    p.add_argument("--medir", action="store_true",
                   help="mide sobre el bloque de medición el conjunto guardado")
    p.add_argument("--confirmar", action="store_true",
                   help="pasa el conjunto guardado por la cuarta puerta y lo "
                        "vuelve a cerrar; revierte a la semilla si no la pasa")
    a = p.parse_args(argv)

    cfg = pt.ProtocolConfig(regime=a.regime)
    feat = lb.datos()

    if a.medir:
        guardado = carga()
        if guardado["corte_seleccion"] != str(cfg.corte_seleccion):
            raise SystemExit(
                f"el conjunto se eligió con corte {guardado['corte_seleccion']!r} y "
                f"el protocolo actual usa {cfg.corte_seleccion!r}: medir con un corte "
                f"distinto del que se usó para elegir mezcla los dos bloques")
        mide(guardado["conjunto"], guardado["familia"], feat=feat, cfg=cfg)
        return 0

    if a.confirmar:
        # Reaplica la cuarta puerta a lo que haya cerrado en disco, sin repetir la
        # búsqueda. Existe porque la puerta se añadió DESPUÉS de que la búsqueda
        # cerrase su conjunto, y repetir catorce horas de voraz para obtener el mismo
        # camino no demuestra nada: el camino está en `traza.parquet` y no cambia.
        # La traza no se toca —`guarda` solo la escribe si el objeto trae pasos— y el
        # sello se conserva, porque la búsqueda que produjo el conjunto es la misma.
        g = carga()
        sel = Seleccion(
            conjunto=tuple(g["conjunto"]), familia=g["familia"],
            criterio=g["criterio"], semilla=tuple(g["semilla"]),
            corte=g["corte_seleccion"], protocolo=g.get("protocolo", ""),
            regime=g["regime"], motivo_parada=g["motivo_parada"],
            n_podadas=g.get("n_podadas", 0), vueltas=g.get("vueltas", 0),
            n_pasos_previos=int(g.get("n_pasos", 0)))
        sel.propuesto = tuple(g.get("propuesto") or g["conjunto"])
        res = confirma(sel.propuesto, semilla=sel.semilla, familia=sel.familia,
                       feat=feat, cfg=cfg)
        sel.confirmacion = res
        sel.confirmado = bool(res["confirmado"])
        sel.conjunto = tuple(sel.propuesto if sel.confirmado else sel.semilla)
        if not sel.confirmado and "RECHAZADO por la confirmación" not in \
                sel.motivo_parada:
            sel.motivo_parada = (
                f"{sel.motivo_parada}; RECHAZADO por la confirmación y revertido a "
                f"la semilla de {len(sel.semilla)} variables: {res['motivo']}")
        guarda(sel, sello=g.get("sello", ""))
        print(f"\n{sel.resumen()}")
        print(f"\nCerrado en {CONJUNTO}: {len(sel.conjunto)} variables.")
        return 0

    comun = dict(feat=feat, cfg=cfg, regime=a.regime, n_jobs=a.n_jobs,
                 n_boot=a.n_boot)
    tabla = criba(familia=a.familia, criterio=a.criterio, **comun)
    print(tabla.head(20).to_string(index=False))
    if a.criba_solo:
        return 0

    busca = flotante if a.busqueda == "flotante" else adelante
    extra = {"max_vueltas": a.max_vueltas} if a.busqueda == "flotante" else {}
    sel = busca(tabla, a.familia, semilla=a.semilla, criterio=a.criterio,
                tolerancia=a.tolerancia, anchura=a.anchura,
                max_vars=a.max_vars, **extra, **comun)
    print(f"\nGuardado en {CONJUNTO}. La medición se lanza aparte, con "
          f"`--medir`, y una sola vez.")
    return 0 if sel.conjunto else 1


if __name__ == "__main__":
    raise SystemExit(main())
