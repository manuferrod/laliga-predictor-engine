# Mejoras propuestas sobre la cadena de datos

Este documento recoge el análisis crítico de las cuatro capas ya construidas
—extracción, consolidación, ingeniería de variables y preprocesado— una vez
cerradas las subsanaciones de la revisión técnica (`REGISTRO_SUBSANACIONES.md`).
Se distingue deliberadamente de aquel registro: allí se corrigieron **defectos**,
es decir, comportamientos que no hacían lo que decían hacer; aquí se proponen
**mejoras**, es decir, decisiones de diseño que son correctas pero podrían ser
mejores. Ninguna de las propuestas que siguen invalida un resultado ya obtenido.

El criterio de priorización es doble. Primero pesa la **corrección
metodológica**: una mejora que evita que una cifra publicada sea optimista va
antes que una que añade poder predictivo, porque la primera afecta a la validez
de la memoria y la segunda solo a su resultado. Después pesa el **coste de
obtención**: una variable que se calcula con aritmética sobre columnas que ya
están en el maestro no cuesta nada, mientras que una que exige una campaña nueva
de extracción compromete horas de red y un riesgo de bloqueo por parte de la
fuente.

Cada propuesta se etiqueta además según **dónde puede ejecutarse**. Las marcadas
como *en frío* se implementan y validan sin red y sin artefactos nuevos, sobre
datos sintéticos, y solo requieren una regeneración de la cadena para surtir
efecto. Las marcadas como *con extracción* exigen una pasada de los extractores
en la máquina de desarrollo antes de que exista el dato sobre el que operan.

---

## 0. Qué no debe tocarse

Conviene fijar primero lo que el análisis confirma como acertado, porque un
documento de mejoras que no dice qué está bien invita a rehacer lo que funciona.

La **disciplina de artefactos** —un único escritor por artefacto, publicación
siempre a través de `_common.publish_df` con guarda de no regresión de filas, y
la separación entre notebooks que muestran y scripts que persisten— es la razón
por la que la cadena es auditable y debe seguir aplicándose a todo lo que se
añada. La **distinción entre ausencia de dato y valor cero** está correctamente
implementada y es el fundamento de la política de imputación por causa; ninguna
de las variables nuevas que se proponen abajo puede rellenarse con ceros por
comodidad. La **corrección punto-en-el-tiempo** está mejor resuelta de lo
habitual en trabajos de este tipo: las bajas se consultan por partido y no por
plantilla actual, y el valor de plantilla de Transfermarkt entra con retardo
t−1, de modo que una revalorización posterior no puede filtrarse hacia atrás. El
**holdout sellado** y la verificación explícita de su sellado en `qa_features.py`
son la garantía de que la cifra final es honesta. Y la **importación perezosa**
de las bibliotecas de scraping, que permite validar toda la lógica pura en un
entorno mínimo, es lo que hace posible la integración continua.

---

## 1. Preprocesado

### M1. El ajuste único sobre todo el bloque de desarrollo no sobrevive a la validación temporal

**Prioridad: crítica. Coste: bajo. Ejecución: en frío.**

`preprocess.fit` recibe el bloque `dev` completo —de la temporada 2016/17 a la
2024/25— y de él aprende tres cosas: las medianas y cuantiles de imputación, los
parámetros λ de la simetrización de Yeo-Johnson y, sobre todo, la selección de
variables del podado colineal, que es *supervisado* porque desempata cada pareja
correlacionada por información mutua con el objetivo. Ese diseño es exactamente
el correcto para el esquema con el que se concibió: una única partición
desarrollo/holdout, donde todo lo que se aprende se aprende en desarrollo y el
holdout no se observa.

El problema aparece en cuanto la fase de modelado adopte —como debe— un
protocolo **walk-forward**. Un pliegue que predice la jornada 12 de la temporada
2019/20 recibiría una matriz cuyas medianas de imputación, cuyos λ y cuya lista
de 202 variables supervivientes se estimaron *también* con las temporadas 2020/21
a 2024/25. No es fuga del objetivo del partido concreto que se predice, pero sí
es **fuga de la distribución futura**: el pliegue conoce qué variables acabarían
siendo informativas a lo largo de toda la muestra. El efecto sobre la métrica es
pequeño en las medianas, moderado en los λ y potencialmente notable en la
selección, que es donde más grados de libertad hay. Y es exactamente el tipo de
optimismo que un tribunal está entrenado para buscar.

La corrección no exige rehacer nada, porque la separación `fit`/`apply` ya está
construida y `PreprocessState` ya es serializable: basta con que el protocolo de
la fase 5 llame a `fit` sobre la ventana de entrenamiento de cada pliegue y a
`apply` sobre su ventana de evaluación, en lugar de leer un
`features_prep.parquet` ajustado una sola vez. El coste es de cómputo, no de
código. El artefacto `features_prep.parquet` conserva su función —es la matriz
de referencia, la que documenta el notebook 04 y la que se usa para el ajuste
final sobre todo el desarrollo antes de puntuar el holdout— pero deja de ser la
entrada de la validación.

Merece la pena, además, **medir la diferencia**: ajustar un pliegue de las dos
maneras y publicar el delta es un resultado metodológico presentable por sí
mismo, y convierte una objeción potencial del tribunal en una sección de la
memoria.

### M2. Sesgo entrenamiento-servicio en las cuotas de cierre

**Prioridad: crítica. Coste: bajo. Ejecución: en frío.**

Las cuotas de cierre están en el maestro y en `PREMATCH_PASSTHROUGH`: el bloque
`odds_close_avg_*`, `odds_close_max_*`, `odds_b365_close_*`, `odds_close_over25`,
`odds_close_under25`, `ah_close_line` y `odds_ah_close_*`. Formalmente no son
fuga: se conocen antes del pitido inicial. Materialmente son la variable más
informativa que existe en este dominio, porque el precio de cierre agrega toda
la información que el mercado ha incorporado hasta el mismo instante del saque,
incluidas las alineaciones confirmadas.

El punto es que **en la ruta semanal no existen y no pueden existir**.
`footballdata.load_upcoming_processed` lo declara sin ambigüedad en su propio
contrato: publica cuotas de apertura y solo de apertura, porque el cierre «no
existe aún». `_OPENING_ODDS_MAP` se construye filtrando del mapa general todas
las claves que contienen `close`. Aguas abajo, `_future_master_rows` levanta el
armazón de filas futuras con todas las columnas del maestro en NaN y copia
encima únicamente las que trae el artefacto de próximos partidos, de modo que
todo el bloque de cierre llega a `preprocess.apply` vacío. Y la política de
bloques de `build_scoring` (`_politica_bloques`) exige cobertura de
`mkt_p_home/draw/away`, de Elo, de valor de plantilla, de forma y de fatiga,
pero **no del bloque de cierre**, de manera que ninguna guarda se dispara.

Lo que ocurre entonces es que `apply` levanta `flag_no_closing_odds` e imputa el
bloque con la mediana de desarrollo. El flag es una honestidad que juega a
nuestro favor —el modelo *sabe* que no hay cierre— pero su soporte en
entrenamiento son exclusivamente las temporadas 2016/17 a 2018/19, el hueco de
época en que la fuente no recogía el cierre. Es decir: **cada predicción semanal
se emite en un régimen que el modelo solo ha visto en las tres temporadas más
antiguas de la muestra**, con el fútbol, los equipos y los niveles de precio de
hace ocho años. A la vez, la métrica histórica que se publicaría en la web se
habría calculado sobre partidos donde el cierre sí estaba, y sería por tanto
sistemáticamente mejor que lo que el usuario va a experimentar el domingo.

La solución que propongo es **explicitar los dos regímenes y modelar los dos**,
en lugar de mezclarlos:

Declarar en `columns.py` un conjunto `WEEKLY_UNAVAILABLE` con las columnas que,
por construcción, no pueden conocerse en el momento de predecir, y derivar de él
dos conjuntos de entrada: el *semanal*, que las excluye, y el *retrospectivo*,
que las incluye. Entrenar y evaluar los dos modelos con el mismo protocolo. El
semanal es el que alimenta la web y el que da la cifra que se defiende como
capacidad predictiva real. El retrospectivo no se descarta: la diferencia entre
ambos **mide cuánta información aporta el movimiento del mercado entre la
apertura y el cierre**, que es un resultado interesante y publicable, y sitúa el
techo alcanzable. Añadir por último una aserción en `build_predict` que falle si
la matriz de inferencia contiene alguna columna de `WEEKLY_UNAVAILABLE` con
valor imputado y el modelo cargado la declara entre sus variables de entrada:
así el desajuste deja de ser silencioso para siempre.

Conviene registrar que las cuotas de apertura sí están disponibles en ambos
regímenes —incluidas `odds_max_*`, que abren la puerta a la propuesta M5— y que
el baseline de mercado (`preprocess.market_baseline`) se construye sobre
`mkt_p_*`, derivadas de la apertura, por lo que **el número a batir no se ve
afectado por esta cuestión**.

### M3. Robustez de la imputación neutral frente a la deriva de nivel

**Prioridad: media. Coste: bajo. Ejecución: en frío.**

La estrategia `neutral` rellena con la mediana global de desarrollo. Para
variables cuyo nivel deriva a lo largo de las nueve temporadas —el valor de
plantilla en euros es el caso evidente, pero también los volúmenes de tiro y el
propio nivel de precios— la mediana global no es el valor neutro de la
temporada, sino un punto intermedio entre 2016 y 2024. Un hueco en 2024 se
rellena con un valor que corresponde aproximadamente a 2020. La alternativa es
una mediana condicionada a la temporada, o la mediana de la ventana de
entrenamiento en el protocolo walk-forward, que resuelve el problema como efecto
secundario de M1. Merece una comprobación empírica antes de cambiarla: si el
número de huecos afectados es pequeño, la mejora es cosmética y no justifica
complicar el estado serializado.

---

## 2. Variables

Este es el nivel con mayor rendimiento por unidad de esfuerzo, porque casi todo
lo que sigue es aritmética sobre columnas que **ya están en el maestro**.

### M4. Movimiento de línea y estructura del precio

**Prioridad: alta. Coste: nulo. Ejecución: en frío.**

Dos familias derivadas de las cuotas, con destinos distintos según M2.

La **dispersión entre casas** en el momento de la apertura —`odds_max_h −
odds_avg_h` y sus análogas, más el *overround* del trío, que es la suma de las
inversas antes de normalizar— está disponible en las dos rutas, porque
`_OPENING_ODDS_MAP` conserva las columnas `max`. Mide el desacuerdo del mercado
y la confianza con que se ofrece el precio: un partido con mucha dispersión es un
partido sobre el que el propio mercado no tiene una opinión firme, y es
razonable esperar que ahí sea donde un modelo pueda aportar. Es también la
variable natural para filtrar apuestas de valor, porque el margen de la casa
entra directamente en el retorno.

En la misma familia y con el mismo coste está la **divergencia entre la casa
afilada y el consenso**: `_ODDS_MAP` normaliza las cuotas de Pinnacle
(`odds_ps_*`) junto a la media de mercado, y ambas están disponibles en la ruta
semanal. Pinnacle es la referencia habitual de precio informado en la
literatura, de modo que la diferencia entre su probabilidad implícita y la del
consenso mide dónde el dinero profesional se separa del minorista. Merece
además una comprobación aparte: si el baseline construido sobre Pinnacle resulta
mejor que el actual, construido sobre la media, el número a batir debería ser
aquel, porque batir un consenso peor no demuestra nada.

El **movimiento apertura-cierre** —`odds_close_avg_h − odds_avg_h` y la
diferencia entre las probabilidades implícitas correspondientes— pertenece al
régimen retrospectivo de M2. Es donde se concentra la información de última hora
(alineaciones, lesiones de calentamiento, dinero profesional), y por eso mismo
es el mejor candidato para cuantificar el techo del que habla M2.

### M5. Ponderación exponencial de la forma reciente

**Prioridad: alta. Coste: bajo. Ejecución: en frío.**

La forma reciente se calcula hoy con ventanas móviles fijas de tres y cinco
partidos (`form.WINDOWS`), más un acumulado de temporada, más un híbrido de seis
partidos que da peso 0,7 a los de la temporada anterior para no dejar la jornada
1 en blanco. Son ventanas rectangulares: dentro de la ventana todos los partidos
pesan igual y fuera de ella no pesan nada, de modo que el quinto partido pesa
tanto como el primero y el sexto desaparece de golpe.

Lo interesante es que **el propio repositorio ya usa la alternativa** en otras
dos familias: el historial directo (`h2h.py`) y la forma de mercado
(`market_form.py`) se calculan con media exponencialmente ponderada de semivida
declarada, sobre la serie desplazada. La propuesta es, por tanto, de coherencia
más que de novedad: extender al bloque de forma el mecanismo que ya está
implementado, probado y sujeto a la misma garantía antifuga —el `shift` que
verifica la prueba correspondiente de `qa_features.py`— en lugar de mantener dos
parametrizaciones distintas por razones históricas.

No se trata de sustituir las ventanas, sino de **añadir el mismo contenido en la
otra parametrización** y dejar que la selección decida: si la semivida gana, gana
con evidencia; si no, se poda por colinealidad y no cuesta nada.

### M6. Forma ajustada por la calidad del rival

**Prioridad: alta. Coste: bajo. Ejecución: en frío.**

Doce puntos en cinco jornadas no significan lo mismo contra los cinco primeros
que contra los cinco últimos, y las variables de forma actuales no distinguen el
caso. La corrección es directa y leak-safe: ponderar cada resultado de la
ventana por la fuerza del rival **en el momento en que se jugó**, usando el Elo
o el valor de plantilla que ya están en el maestro con su fecha correcta.
Equivale a construir una forma «esperada» y quedarse con el residuo, que es el
sobrerrendimiento real del equipo. Es una de las variables que más
sistemáticamente aparece en la literatura y el maestro ya tiene todos los
ingredientes.

### M7. Fuerzas latentes de ataque y defensa

**Prioridad: media-alta. Coste: medio. Ejecución: en frío.**

El Elo resume la fuerza de un equipo en un único escalar y no separa si un
equipo es bueno porque marca mucho o porque encaja poco. Un modelo de fuerzas al
estilo Dixon-Coles —un parámetro de ataque y otro de defensa por equipo, más una
ventaja de campo— estimado sobre una ventana expansiva o deslizante que solo mire
al pasado proporciona dos variables interpretables por equipo que el Elo no da, y
además abre la puerta a que ese mismo modelo entre en la fase 5 como pronóstico
autónomo con el que comparar los ensayos (véase `DISENO_MODELADO.md`). La
disciplina antifuga es
la de siempre: los parámetros de la jornada *j* se estiman con partidos
anteriores a *j*, lo que en la práctica significa un reajuste por jornada. El
coste es de cómputo y de una prueba cuidadosa; el beneficio es doble, porque la
misma pieza sirve para variable y para modelo.

### M8. Derivada del Elo y contexto de calendario

**Prioridad: media. Coste: nulo. Ejecución: en frío.**

El nivel del Elo está; su **tendencia** no. La variación del rating en las
últimas cinco jornadas separa a un equipo de 1.700 puntos que viene subiendo de
otro de 1.700 que viene cayendo, y son situaciones distintas. En la misma línea,
el maestro contiene fecha y hora de cada partido: día de la semana, franja
horaria y la marca de jornada posterior a parón internacional —esta última ya
existe— son variables de contexto que no cuestan nada.

### M9. Árbitro, derbi y desplazamiento

**Prioridad: media. Coste: bajo. Ejecución: en frío.**

Football-data publica el árbitro de cada partido y el dato viaja hasta el crudo.
Codificado con cuidado —agrupando por tramos de historial y no como categórica
de alta cardinalidad, que sobreajustaría— aporta el sesgo arbitral en tarjetas y
en tendencia local, que es un efecto documentado. El carácter de derbi y la
distancia de desplazamiento del visitante requieren una tabla de coordenadas de
estadios de unas treinta filas, escrita a mano una sola vez, sin scraping: la
fatiga de viaje del visitante es una de las pocas asimetrías físicas medibles y
la cadena ya modela el resto de la fatiga.

### M10. Contexto competitivo de final de temporada

**Prioridad: baja. Coste: bajo. Ejecución: en frío.**

Un partido de la jornada 37 entre dos equipos matemáticamente salvados y sin
opción europea se juega de otra manera, y el mercado lo sabe mejor que cualquier
variable de forma. Con la clasificación acumulada que ya calcula la capa de
variables se puede derivar, sin mirar al futuro, la distancia a cada objetivo y
una marca de «partido sin trascendencia». Es un efecto pequeño pero concentrado
en un número reducido de partidos, donde los errores del modelo suelen ser
grandes.

---

## 3. Consolidación

### M11. Persistir la matriz de procedencia y cobertura

**Prioridad: alta. Coste: bajo. Ejecución: en frío.**

Hoy la cobertura por bloque se calcula y se **imprime**: `qa_master.py` la
reporta sobre el histórico y `_readiness_report` de `build_scoring` la reporta
sobre la jornada por jugar. Al no persistirse, se pierde en cuanto termina la
ejecución, y con ella la posibilidad de responder a una pregunta que la web va a
plantear de forma natural: *¿con qué información se emitió esta predicción?*

Propongo un artefacto `coverage_matrix.parquet` con una fila por partido y una
columna booleana por bloque de fuente (mercado, Elo, valor de plantilla, forma,
fatiga, bajas por recuento, bajas por valor), escrito por la misma pasada que
construye el maestro y extendido por la ruta de inferencia. El beneficio es
triple. En la memoria, permite tabular la cobertura real por temporada en lugar
de afirmarla. En la evaluación, permite condicionar las métricas a la
disponibilidad —saber si el modelo es peor cuando falta el parte médico es un
resultado, no una anécdota—. Y en la web, permite que una tarjeta de partido
muestre con qué se predijo, que es precisamente el tipo de transparencia que
distingue un trabajo académico de un pronóstico.

### M12. Ponderar las bajas por minutos jugados, no solo por valor de mercado

**Prioridad: media-alta. Coste: medio. Ejecución: con extracción (depende de M13).**

La ponderación por valor de Transfermarkt es un buen *prior* de importancia, pero
no distingue al titular indiscutible del fichaje caro que no juega desde octubre.
Un jugador de cuarenta millones ausente puede valer cero en pérdida real. La
métrica correcta combina el valor con la **cuota de minutos disputados por el
jugador en la temporada hasta la fecha**, que es estrictamente pasada y por tanto
leak-safe. Requiere el dato de minutos por jugador y partido, que es lo que
propone M13.

---

## 4. Extracción

### M13. Alineaciones y minutos de Sofascore

**Prioridad: alta. Coste: alto. Ejecución: con extracción.**

Es la ampliación con mayor retorno de las tres que quedan, porque desbloquea
tres cosas a la vez. Habilita M12, la ponderación de bajas por minutos.
Proporciona la **continuidad del once**, es decir, cuántos titulares repiten
respecto al partido anterior, que es una medida de rotación y por tanto de
prioridad competitiva mucho más directa que la fatiga por calendario. Y da la
carga de trabajo real por jugador —minutos acumulados en ventana móvil— que es
el denominador que hoy le falta al bloque de fatiga, construido sobre partidos y
no sobre minutos.

El extractor seguiría el contrato ya establecido (`backfill`, `update`,
`process`, `weekly`) y su reductor viviría junto a los demás. El coste está en la
campaña de backfill sobre nueve temporadas.

### M14. Datos a nivel de tiro de Understat

**Prioridad: media. Coste: alto. Ejecución: con extracción.**

Understat publica cada tiro con su xG, su localización, la parte del cuerpo y la
situación de juego. El agregado por partido que hoy se consume descarta toda esa
estructura. Con el detalle se obtienen dos cosas distintas. La primera es
descriptiva y barata: xG por tiro, proporción de tiros dentro del área, reparto
entre jugada y balón parado, que son variables de **calidad de ocasión** y
separan al equipo que genera poco y bueno del que dispara mucho desde fuera. La
segunda es un **modelo xG propio**, que es un componente académicamente vistoso
—una regresión sobre coordenadas y contexto de tiro— pero cuyo valor incremental
sobre el xG que Understat ya publica es discutible. Recomiendo la primera y
tratar la segunda como material opcional de la memoria si sobra tiempo, no como
línea crítica.

### M15. Sustituir la comprobación cruzada perdida de FBref

**Prioridad: baja. Coste: medio. Ejecución: con extracción.**

Tras la retirada del dato avanzado a nivel equipo-partido, `fbref.py` ya no
aporta más que un marcador redundante con Sofascore. La cadena se queda, por
tanto, **sin una segunda fuente independiente para las estadísticas de partido**,
que es lo que permitía detectar un error silencioso de la fuente principal. No es
urgente porque el maestro no depende de FBref, pero conviene dejar constancia de
que la redundancia se ha perdido y de que cualquier fuente alternativa que la
restituya —aunque cubra solo un subconjunto de métricas— tiene valor de control
de calidad más que de variable.

---

## 5. Orden recomendado

El orden que sigue está pensado para que cada paso deje la cadena en un estado
consistente y para que lo que condiciona la validez de las cifras vaya primero.

Antes de entrenar nada conviene resolver **M1** y **M2**, porque los dos
determinan qué significan las cifras que se van a producir: el primero, si la
validación es honesta; el segundo, si la cifra publicada corresponde a lo que el
sistema hará realmente el domingo. Ambos son de coste bajo y se implementan en
frío, y ambos encajan de forma natural en el diseño de la fase 5, donde se
detallan (`DISENO_MODELADO.md`).

Inmediatamente después, y todavía antes del primer entrenamiento serio, el
bloque de variables baratas: **M4**, **M5**, **M6**, **M8** y **M9**, que son
aritmética sobre el maestro actual y solo exigen una regeneración de
`build_features`. Con ellas la matriz de partida es sensiblemente más rica sin
haber tocado la red. **M11** puede ir en la misma pasada porque comparte
recorrido.

**M7** entra a continuación, ya con criterio: si el modelo lineal y el de refuerzo
de gradiente dejan claro dónde está el error, las fuerzas de ataque y defensa
tienen un sitio evidente; si no, es una pieza que se justifica igualmente como
pronóstico autónomo con el que comparar los ensayos.

Las ampliaciones con extracción —**M13** primero, **M14** después— se abordan
solo cuando la fase 5 esté funcionando de extremo a extremo con lo que ya hay.
El motivo es de gestión de riesgo: una campaña de backfill puede fallar, tardar
días o encontrarse con un bloqueo de la fuente, y ninguna de las dos es
condición necesaria para que exista un modelo defendible. **M3**, **M10** y
**M15** quedan como mejoras de cierre, valiosas pero no estructurales.
