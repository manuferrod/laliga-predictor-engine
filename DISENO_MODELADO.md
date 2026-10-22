# Diseño de la fase de modelado

Documento de diseño de la quinta y última fase de la cadena: la que convierte
`features_prep.parquet` en probabilidades 1X2, en una evaluación defendible de
esas probabilidades y en el conjunto de artefactos que consume la aplicación web
(`laliga-predictor-portfolio`). Se escribe antes de implementar para que las
decisiones metodológicas queden fijadas por escrito y no emerjan como resultado
de lo que fue saliendo.

La fase tiene tres obligaciones, y conviene no confundirlas porque tiran en
direcciones distintas. La **obligación académica** es producir una estimación
honesta de la capacidad predictiva, con su incertidumbre y comparada contra una
referencia exigente. La **obligación de producto** es emitir cada semana una
predicción para la jornada siguiente, con la misma transformación con que se
entrenó y sin depender de información que ese día no existe. Y la **obligación
expositiva** es que todo lo anterior se pueda mirar: gráficos históricos,
métricas por temporada, predicciones futuras y una ficha por partido. El diseño
que sigue intenta satisfacer las tres sin sacrificar la primera, que es la que
se defiende ante tribunal.

---

## 1. Qué se hereda y qué se cambia respecto a la referencia

El notebook `notebooks/referencia_v1/MODELOS.ipynb` fija el nivel de salidas que
se espera y sirve de inventario de partida. De él se conserva la **estructura de
artefactos** —métricas por temporada, matrices de confusión, informes de
clasificación, curvas ROC, registros de partido con retorno acumulado, métricas
del mercado como referencia y el bloque de radar pre-partido—, se conserva la
idea de un **código de ejecución determinista** derivado del contenido, que hace
reproducible y citable cada publicación, y se conserva el **retorno a apuesta
plana contra una casa concreta** como métrica de utilidad económica.

Se cambian cuatro cosas.

La primera es el **protocolo**: la referencia entrena día a día con una ventana
deslizante de cuatro temporadas y un peso de recencia fijo, pero preprocesa una
sola vez; aquí el preprocesado se reajusta dentro de cada pliegue (mejora M1 de
`MEJORAS_PROPUESTAS.md`).

La segunda es la **separación de regímenes**: la referencia no distinguía entre
las variables disponibles el jueves y las disponibles el domingo a la hora del
partido, y esa distinción es ahora explícita (mejora M2).

La tercera es la **batería de métricas**: la referencia mide *accuracy*, log-loss
y Brier; se añade el **Ranked Probability Score**, que es el estándar en
predicción de resultados 1X2 porque respeta el orden natural
victoria-empate-derrota, y se añaden intervalos de confianza en lugar de cifras
puntuales.

La cuarta es el **alcance del estudio de modelos**: la referencia se detiene en
una regresión logística multinomial; aquí esa regresión se conserva —por
continuidad y porque es interpretable y competitiva— pero comparte cartel con
otras tres familias, y la comparación entre ellas se hace ensayo a ensayo, no
recorriendo un catálogo cerrado. La elección de **variables**, en cambio, no se hace
a mano: la recorre `seleccion.py` con una búsqueda flotante y cuatro puertas, y lo
que decide es el RPS (§3).

---

## 2. Protocolo de validación

### 2.1 Cinco bloques, no dos

El calendario de la muestra se parte en cinco, y la disciplina de cada bloque es
distinta.

Las temporadas **2016/17 a 2018/19** funcionan como bloque de arranque: son las
que carecen de cuotas de cierre y de datos de bajas, y su papel principal es
alimentar las ventanas de forma, de Elo y de historial de los pliegues
posteriores. Entran en el entrenamiento pero no se evalúan, porque una métrica
calculada sobre ellas mezcla el rendimiento del modelo con la escasez de la
fuente. Con una excepción declarada: **2017/18 y 2018/19** son además el bloque de
**confirmación**, el único sitio donde se puede preguntar si lo que la búsqueda de
variables adoptó sigue siendo ganancia en partidos que no lo eligieron sin mirar
hacia adelante, porque son anteriores a todo lo que la búsqueda vio. El precio de
usarlas así, dicho en voz alta, es que dejan de ser una lectura limpia citable.

Las temporadas **2019/20 a 2024/25** son el bloque de **desarrollo y validación
temporal**. Sobre ellas se ejecuta el walk-forward, se comparan modelos, se
eligen hiperparámetros y se toman todas las decisiones de diseño. Toda cifra que
sirva para decidir algo sale de aquí.

Y se parte en dos mitades cronológicas por `protocol.CORTE_SELECCION`, porque una
búsqueda de variables que informa sobre los mismos partidos en los que eligió no
publica una estimación de lo que rendirá el sistema, sino el máximo de un ruido al
que se le han dado doscientas oportunidades de parecer señal. **Selección**
(2019/20–2021/22) es donde la búsqueda recorre variables, y ninguna de sus cifras
es citable. **Medición** (2022/23–2024/25) se puntúa una sola vez, con el conjunto
ya cerrado, y es de donde sale la cifra que va a la memoria. El bloque no es un
parámetro de las funciones de `seleccion.py`: `criba` y `adelante` solo saben medir
sobre selección, `mide` solo sobre medición, y pedirles otra cosa levanta
`PermissionError` —un argumento libre convertiría la única cifra citable del
trabajo en el descuido de una tarde, porque una ejecución que seleccionase sobre
medición no daría ningún error y devolvería una tabla de aspecto normal—.

La temporada **2025/26** es el **holdout sellado**, declarado en
`columns.py:HOLDOUT_SEASONS` y verificado por `qa_features.py`. Se puntúa **una
sola vez**, al final, con el modelo ya cerrado, y su resultado se reporta sin
reajustar nada después. Si la cifra decepciona, se reporta la cifra que salga:
volver atrás a cambiar el modelo convierte el holdout en un conjunto de
validación más y anula su valor probatorio. Esta regla debe quedar escrita en la
memoria antes de conocer el número.

La temporada **en curso** —2026/27 mientras se escribe esto— es el bloque de
**producción**: la que el sistema predice cada semana. Se puntúa jornada a jornada
con el modelo ya cerrado y da la tercera cifra que el trabajo necesita, la que
responde a «cómo va el modelo en la temporada que se está jugando» y la única que
un lector de la aplicación puede contrastar él mismo el lunes siguiente. No
decide nada, y eso no es una cautela de estilo sino la condición que la hace
publicable: cualquier jornada posterior a la sellada se entrena **con** la
sellada dentro de la ventana —necesariamente, y eso está bien, porque el modelo
que de verdad se publica en octubre de 2026 ha visto 2025/26—, de modo que si
estas predicciones entrasen en la elección de familia, de hiperparámetros o de
variables, el holdout habría participado en la decisión y su cifra dejaría de ser
independiente. Por eso el bloque vive fuera de validación, confirmación, selección
y medición;
`laboratorio.py` se niega a tocarlo, y el bloque **no** lleva sello de pasada
única, porque se reejecuta cada semana por definición: la jornada recién
disputada es una fila nueva de la misma tabla.

Conviene separar dos actos que la palabra «abrir» confunde: **usar** 2025/26 como
pasado de entrenamiento de 2026/27 es producción corriente, y **puntuarla** para
leer su métrica es el acto único que `build_models.py --holdout` sella. El primero
no consume el segundo.

El bloque se deduce del dato —toda temporada cuyo rótulo va después del de
`HOLDOUT_SEASONS`— y no de una lista escrita a mano, que es lo que evita el fallo
silencioso del primer fin de semana de agosto siguiente: una lista literal dejaría
entrar la temporada nueva en el bloque de desarrollo sin que nada avisara.

### 2.2 Walk-forward por jornada

La unidad de avance es la **jornada**, no el partido. La razón es práctica y
también conceptual: la predicción real se emite por jornada, y avanzar partido a
partido daría al modelo información de los partidos del sábado para predecir los
del domingo, que es una ventaja que en producción no se tiene salvo que se
re-ejecute la cadena a mitad de fin de semana.

Para cada jornada *j* del bloque de validación, el procedimiento es: tomar todos
los partidos anteriores a *j* como ventana de entrenamiento; **ajustar el
preprocesado sobre esa ventana** con `preprocess.fit`; entrenar el estimador;
**aplicar** el estado congelado a la jornada *j* con `preprocess.apply`; predecir
y guardar. El resultado es un conjunto de predicciones fuera de muestra que
cubre el bloque de validación completo y sobre el que se calculan todas las
métricas.

La ventana de entrenamiento admite dos variantes que conviene comparar
explícitamente porque la respuesta no es obvia: **expansiva**, que usa todo el
pasado disponible, y **deslizante** de cuatro temporadas, que es la que usaba la
referencia y que apuesta por que el fútbol reciente es más informativo que el
antiguo. La comparación entre ambas es un resultado de la memoria por sí misma.
Sobre la variante que se elija se superpone un **peso de recencia** que decae
exponencialmente con la antigüedad del partido, gobernado por una **semivida**:
los días que tarda el peso en caer a la mitad. Esa semivida no se fija a ojo: los
valores admisibles se declaran en `protocol.SEMIVIDAS` —de media temporada a
cinco años, con «sin ponderar» entre ellos— y cuál conviene se decide midiendo.
En el banco de ensayos se fija al abrir cada ensayo, de modo que dos ensayos que
solo difieren en la semivida son dos cifras enfrentadas sobre los mismos
pliegues; lo que se paga por recordar menos pasado es tamaño de muestra
efectivo, y ese compromiso se lee en la comparación en lugar de suponerse.

### 2.3 Los dos regímenes de información

Todo lo anterior se ejecuta **dos veces**, sobre dos conjuntos de variables de
entrada derivados de un único conjunto declarado en `columns.py`:

El **régimen semanal** excluye las columnas que no existen en el momento de
predecir —el bloque completo de cuotas de cierre y sus derivadas— y es el que
gobierna el producto. Su cifra es la que se publica como capacidad predictiva
real del sistema y la que va a la web.

El **régimen retrospectivo** las incluye. No alimenta el producto, pero su
comparación con el semanal cuantifica cuánta información añade el movimiento del
mercado entre apertura y cierre, y establece un techo razonable de lo alcanzable
con información pre-partido completa.

La separación se hace en un solo sitio, mediante un conjunto
`WEEKLY_UNAVAILABLE` en `columns.py`, y `build_predict` verifica que la matriz de
inferencia no contiene ninguna de esas columnas entre las variables que el modelo
cargado declara usar. Un desajuste debe ser un error, no una imputación
silenciosa.

---

## 3. Referencias, familias y banco de ensayos

El estudio de modelado tiene dos piezas que conviene no confundir. Están las
**referencias**, que no se ajustan a nada y existen para dar suelo y techo a
cualquier cifra; y están las **cuatro familias de algoritmos**, que sí se
ajustan y que compiten entre sí sobre conjuntos de variables elegidos a mano.
Ninguna familia entra en la memoria por ser más moderna: entra con su cifra al
lado de la de las referencias, y una familia que no mejora a la logística sobre
el mismo conjunto es un resultado negativo documentado, que también es un
resultado.

**Referencia de mercado.** Las probabilidades implícitas des-margen que ya
calcula `preprocess.market_baseline`, con las métricas de
`market_baseline_metrics`. No es un modelo: es el número a batir. En este dominio
el mercado es una referencia dura y no batirlo en log-loss es el desenlace
mayoritario en la literatura; lo que sí es alcanzable —y es el objetivo honesto
del trabajo— es acercarse mucho y encontrar subconjuntos de partidos donde el
modelo aporte. Conviene evaluar de entrada **dos versiones de la referencia**, la
del consenso de mercado que se usa hoy y una construida sobre las cuotas de
Pinnacle, que también viajan en el maestro: si la segunda es mejor, es la que
debe figurar como número a batir, porque superar un consenso peor no demuestra
capacidad predictiva.

**Referencia trivial.** La distribución a priori de clases y la regla «gana el
local». Sirven para dimensionar todo lo demás y para que las métricas tengan un
suelo, no solo un techo.

Sobre esas dos referencias se miden cuatro familias, declaradas en
`estimators.py` con una firma común para que el protocolo no sepa qué está
entrenando. Cada una responde a una hipótesis distinta sobre dónde está la señal.

**Regresión logística multinomial regularizada** (`logistica`). Continuidad con
la referencia v1 y la familia interpretable del estudio. La penalización se
gobierna con dos mandos, la fuerza `C` y el reparto `l1_ratio` entre la que
encoge y la que anula; la configuración fija del estudio usa cresta pura
—`l1_ratio = 0`— porque anular coeficientes es una forma de selección de
variables y aquí la selección la hace quien lee, no el penalizador. Los
coeficientes estandarizados se leen como qué pesa. Es también la familia que
mejor tolera el número de variables frente al de observaciones que tenemos
—unos 2.300 partidos de validación frente a 202 variables disponibles— y es
razonable esperar que sea competitiva.

**Bosque aleatorio** (`bosque`). Muchos árboles profundos promediados, con la
varianza controlada por el promedio y no por la poda. Captura interacciones sin
que haya que declararlas y sirve de contraste directo con la logística: si la
diferencia entre ambas es pequeña, la estructura del problema es sustancialmente
lineal en las variables que se le están dando, y eso es información.

**Refuerzo de gradiente sobre árboles** (`refuerzo_xgb`). La familia que
previsiblemente extrae mejor las interacciones no lineales entre forma, fatiga,
bajas y mercado. Con control estricto del sobreajuste: profundidad baja,
submuestreo, hoja mínima alta y, cuando se usa detención temprana, **la última
porción temporal de la ventana de entrenamiento** y nunca el pliegue de
evaluación. Este último punto es una fuente clásica de fuga silenciosa y tiene
prueba explícita en el banco de `scripts/models/`.

**Red neuronal densa** (`red`). Un perceptrón multicapa de una o dos capas
ocultas. No se espera de él que gane —con este tamaño de muestra rara vez lo
hace—, sino que responda a la pregunta de si queda estructura que las otras tres
no estén viendo. Un resultado plano es tan publicable como uno bueno.

Fuera de las cuatro quedan dos ideas que la exploración previa manejó y que este
diseño no adopta como familias propias: el apilamiento de salidas y el modelo
sobre el residuo del mercado. Ambas son composiciones —un modelo que se ajusta
sobre las predicciones de otros— y su coste metodológico es alto: multiplican
los grados de libertad justo donde el protocolo es más caro de auditar, y el
segundo se obtiene, en la práctica, dando al modelo las probabilidades de
mercado como variables de entrada y leyendo qué añade el resto. Esa lectura
—«qué información no está incorporada en el precio»— se conserva, pero se
responde comparando ensayos con y sin el bloque de mercado, que es más simple de
defender y no exige un estimador nuevo.

**Cómo se recorre todo esto.** No hay catálogo que ejecutar de principio a fin.
El banco de ensayos `laboratorio.py` mide, sobre el protocolo de la sección 2, un
conjunto de variables propuesto a mano con una familia y una configuración fijas,
y devuelve sus cifras junto a las del apostante de referencia. Quien lee compone
el siguiente conjunto a la vista de las anteriores. Los hiperparámetros
permanecen fijos mientras se buscan variables —`laboratorio.HIPER`— para que la
comparación entre dos conjuntos no esté contaminada por un ajuste distinto, y
solo al final, con el conjunto ya cerrado, se recorre una rejilla corta
(`laboratorio.ESPACIOS_FINAL`). Cada ensayo se anota con un sello derivado de sus
ingredientes, de modo que el estudio es reconstruible ensayo a ensayo.

**Y cuándo deja de componerse a mano.** El banco responde «¿es mejor este conjunto
que aquel?»; no responde «¿cuál es el mejor de los 2⁶⁵ subconjuntos posibles?».
Para eso está
`seleccion.py`, que recorre el banco de candidatas con una búsqueda **flotante**
—alterna avance y poda hasta punto fijo, porque una variable que entró en el paso
dos puede quedar redundante en el nueve y en una logística dos columnas casi
colineales se reparten el coeficiente sin que las cifras lo denuncien— partiendo
de una semilla de mercado protegida de la poda, porque la pregunta del trabajo es
qué se le añade al precio, no si el precio sobra. Cuatro puertas deciden si una
candidata entra: que mejore por encima de la tolerancia **del criterio que guía**
—`seleccion.TOLERANCIAS`, una por criterio y escalada al tamaño del bloque, porque
las tres métricas viven en escalas distintas y el umbral del RPS aplicado al acierto
o a la ventaja equivale a no poner umbral—; que mejore **en todas las
temporadas** y no solo de media, lo que se comprueba descontándole su mejor
temporada y exigiendo que siga siendo mejora; que su mejora quede por encima del
techo del máximo de una nube de sorteos de ruido del mismo tamaño que el número de
candidatas probadas, para que no se adopte el mejor de cincuenta y nueve azares; y
—la única que se pregunta fuera del bloque de selección, con el conjunto ya
cerrado— que lo adoptado **siga siendo ganancia donde no se eligió**
(`seleccion.confirma()`, sobre el bloque de confirmación). La cuarta no tiene
ningún número que elegir, y se añadió porque las tres primeras, todas internas al
bloque de selección, dejaron pasar dos variables que empeoraban el modelo fuera de
él. Ejecutada, rechaza, y el conjunto queda cerrado sobre la semilla de precio.
Qué significa eso y qué no, con el recorrido del banco entero que lo sostiene, está
en el README y en C.9.1 del cuaderno 05.

---

## 4. Batería de métricas

### 4.1 Calidad probabilística

El **Ranked Probability Score** es la métrica principal. A diferencia del Brier
multiclase, penaliza más un error de local a visitante que uno de local a
empate, que es la ordenación correcta del dominio. Se reporta agregado, por
temporada y por tramo de confianza.

El **log-loss** acompaña como métrica estrictamente propia y por comparabilidad
con la referencia v1 y con la literatura. El **Brier multiclase** se reporta
descompuesto en fiabilidad, resolución e incertidumbre: la descomposición separa
«el modelo está mal calibrado» de «el modelo no discrimina», que son problemas
distintos con soluciones distintas, y es una figura que se defiende bien. Se
publica además un cuarto término, el residuo, porque la identidad de Murphy solo
tiene tres piezas cuando el pronosticador emite un número finito de valores
distintos; un modelo continuo mete en cada tramo probabilidades que solo se
parecen, y el resto de esa aproximación se declara en vez de repartirse a ciegas
entre los otros tres. Con él dentro la igualdad es exacta, que es lo que permite
usarla como comprobación y no como aproximación de cortesía.

La **calibración** se evalúa con curvas de fiabilidad por clase, en deciles de
probabilidad predicha, y con el error de calibración esperado como resumen
numérico. Es la métrica que un usuario de la web percibe aunque no la conozca:
cuando el sistema dice sesenta por ciento, debe acertar seis de cada diez veces.

### 4.2 Clasificación

*Accuracy*, matriz de confusión, precisión-exhaustividad-F1 por clase con
promedios macro y ponderado, y ROC-AUC uno-contra-resto por clase con
promedios macro y micro. Se mantienen porque son las que espera un lector no
especialista y porque la referencia v1 ya las produce, pero se subordinan a las
probabilísticas. La clase **empate** merece lectura separada en todos los casos:
es la minoritaria, la que ningún modelo predice bien y la que explica la mayor
parte de la diferencia entre modelos.

### 4.3 Habilidad frente al mercado

Toda métrica anterior se reporta además en forma de **habilidad**: la mejora
relativa respecto a la referencia de mercado sobre el mismo conjunto de
partidos. Es la única comparación que sitúa el resultado, porque un log-loss de
0,94 no significa nada en abstracto y sí lo significa junto al del mercado en
esos mismos partidos.

Se añade el análisis por **desacuerdo**: agrupar los partidos según la distancia
entre la probabilidad del modelo y la del mercado y medir quién acierta más en
cada grupo. Si el modelo solo gana donde coincide con el mercado, no aporta; si
gana donde discrepa, aporta de verdad. Este corte es probablemente el gráfico más
informativo de todo el trabajo.

### 4.4 Utilidad económica

Se conserva el marco de la referencia v1 —apuesta plana a la clase más probable
contra las cuotas de una casa concreta, con retorno acumulado y registro por
partido— y se le añade lo que le falta para ser una evaluación seria: **filtrado
por valor esperado**, apostando solo cuando la ventaja estimada supera un umbral;
**criterio de Kelly fraccionado** como alternativa al importe plano; **máxima
caída acumulada** junto al retorno, porque un retorno positivo con una caída del
cuarenta por ciento no es una estrategia; y una advertencia explícita en la
memoria de que el retorno es la métrica **más ruidosa** de todas las que se
reportan, porque depende de un puñado de aciertos a cuota alta.

Y se le añade sobre todo un **listón**, porque una cifra de retorno sin nada al
lado no responde a la pregunta que motiva el trabajo. El listón es un apostante
de referencia que sigue a Bet365 y respalda siempre su cuota más baja —su
favorito— en todos los partidos con precio y sin umbral que los filtre. Ve el
mismo calendario y los mismos precios que el modelo, de modo que la diferencia
entre ambos retornos no mide suerte de calendario sino calidad de pronóstico
frente a quien pone el precio. Esa comparación se publica **temporada a
temporada** y **jornada a jornada**, con el intervalo de la diferencia obtenido
por remuestreo emparejado por bloques —cada réplica reordena bloques de jornada
y vuelve a simular las dos estrategias, para que el choque común no infle el
intervalo—, y la evaluación automática semanal la recalcula para las dos partes
sin excepción.

Una nota de honestidad que conviene dejar escrita: el retorno se calcula contra
cuotas de **cierre** en las temporadas históricas, mientras que una apuesta real
emitida el jueves iría contra las de apertura. Es el mismo desajuste que describe
la mejora M2 y hay que reportarlo, no ocultarlo.

### 4.5 Incertidumbre

Ninguna métrica se publica como cifra puntual. Los intervalos se obtienen por
**remuestreo por bloques** —bloques de jornada, no de partido, para respetar la
dependencia temporal— y la comparación entre dos modelos se acompaña de un
**contraste de permutación** sobre la diferencia de sus métricas. Con unos 2.300
partidos evaluados, la diferencia entre dos modelos razonables cabe
perfectamente dentro del ruido, y decirlo es más defendible que presentar una
ordenación falsamente nítida.

---

## 5. Interpretabilidad

La interpretabilidad no es aquí un extra sino un requisito, porque la web tiene
que explicar cada predicción y porque una memoria que solo presenta un número no
demuestra comprensión del problema.

A nivel **global**: coeficientes estandarizados del modelo lineal, importancia
por permutación calculada sobre el bloque de validación —no sobre el de
entrenamiento— y valores de Shapley agregados para el modelo de árboles. Las tres
lecturas deben contarse juntas, señalando dónde coinciden y dónde no, porque la
discrepancia entre ellas suele ser informativa.

A nivel **local**: la contribución de cada variable a la predicción de un partido
concreto, agrupada en los bloques que el usuario entiende —mercado, fuerza
histórica, forma reciente, fatiga, bajas— y reducida a las tres o cuatro
contribuciones dominantes. Esto es lo que alimenta la ficha de partido de la web y
lo que convierte una probabilidad en una explicación.

---

## 6. Contrato de salidas para la web

Los artefactos que siguen son el punto de encuentro entre este repositorio y
`laliga-predictor-portfolio`, cuyas páginas actuales son *Home*, *Temporada
Actual*, *Histórico*, *Métricas* y *Matchlogs*, más una zona privada con las
predicciones futuras. Todos se escriben bajo `outputs/`, todos llevan el código
de ejecución determinista, y ninguno tiene más de un escritor.

**Predicciones de la próxima jornada.** Una fila por partido con la fecha, la
temporada, la jornada, los dos equipos en clave canónica, las tres
probabilidades, la clase predicha, la confianza como probabilidad máxima, la
entropía y el margen entre las dos clases más probables; las cuotas disponibles
en el momento de emitir; el valor esperado y la fracción de Kelly de la selección;
y la cobertura por bloque procedente de la matriz de procedencia (mejora M11),
para poder señalar en la interfaz que una predicción se emitió sin parte médico.
Se publica en CSV para el repositorio y en JSON para la aplicación.

**Métricas históricas por temporada.** Una fila por temporada y modelo con la
batería completa de la sección 4 y sus intervalos, más las mismas cifras del
mercado en las mismas filas para que la comparación no dependa de cruzar dos
ficheros. Es la fuente de la página *Métricas*.

**Registros de partido por temporada.** Una fila por partido evaluado con la
verdad, la predicción, las tres probabilidades, la cuota de la selección, el
valor esperado, si se apostó bajo la política de filtrado, el beneficio y el
acumulado. Es la fuente de la página *Matchlogs* y del gráfico de retorno
acumulado, que es el más vistoso del conjunto.

**Diagnósticos de clasificación.** Matrices de confusión por temporada, informe
de clasificación por clase, curvas ROC uno-contra-resto y —como novedad respecto
a la referencia v1— **curvas de fiabilidad por clase y temporada**, que son la
evidencia visual de la calibración.

**Explicaciones por partido.** Las contribuciones locales de la sección 5 para
los partidos de la próxima jornada, en el formato agrupado por bloques que
consume la ficha de partido.

**Radar y barras pre-partido.** Se hereda el bloque de la referencia v1, con sus
ejes normalizados, sus rangos declarados, sus ejes invertidos y su versión de
normalización explícita, y se extiende con los ejes que habiliten las variables
nuevas. La normalización debe seguir siendo **declarativa y versionada**: un
cambio de rango que no cambie la versión rompe la comparabilidad de los gráficos
históricos sin dejar rastro.

**Perfiles de equipo y línea temporal de temporada.** Series por equipo y jornada
con Elo, fuerzas latentes de ataque y defensa (`strength.py`), forma ajustada y
posición, que alimentan la página *Histórico* con gráficos de evolución —que es
exactamente el tipo de salida que da valor expositivo a la web y que hoy no
existe.

**Ficha del modelo y manifiesto.** Un documento con la especificación del modelo
publicado, el protocolo con que se validó, las variables de entrada, las métricas
de referencia y las limitaciones conocidas —incluida la de la sección 4.4— y un
manifiesto con la fecha de generación de cada artefacto y el código de ejecución,
que es lo que la aplicación necesita para mostrar cuándo se actualizó por última
vez.

---

## 7. Arquitectura

La fase respeta la disciplina establecida: los scripts ejecutan y persisten, el
notebook muestra y audita, y ningún artefacto tiene dos escritores. Bajo
`scripts/models/`:

| Módulo | Responsabilidad |
|---|---|
| `protocol.py` | generación de pliegues walk-forward, ventanas expansiva y deslizante, pesos de recencia y el ciclo `fit`/`apply` del preprocesado por pliegue |
| `estimators.py` | un constructor por familia, con firma común y con los valores admisibles declarados, de modo que el protocolo no sepa qué modelo está entrenando |
| `laboratorio.py` | el banco de ensayos manual: compone conjuntos de variables, los mide bajo el protocolo, los compara con la referencia y los registra |
| `seleccion.py` | la búsqueda flotante con sus cuatro puertas, el reparto selección / medición / confirmación impuesto por módulo, y el conjunto cerrado que `build_models.py` recoge con `carga()` |
| `evaluate.py` | la batería de métricas de la sección 4, incluidos remuestreo por bloques y contrastes de permutación |
| `backtest.py` | política de apuesta, valor esperado, Kelly, retorno acumulado y caída máxima |
| `explain.py` | interpretabilidad global y local |
| `build_models.py` | orquestador: ejecuta el protocolo, entrena el modelo final y publica los artefactos de evaluación |
| `publish_web.py` | proyecta los artefactos al contrato de la sección 6 |
| `qa_models.py` | comprobaciones de calidad sobre lo publicado |
| `test_*.py` | bancos offline con datos sintéticos, en el modo dual que ya usan los demás |

Las comprobaciones de `qa_models.py` deben incluir, como mínimo, que las
probabilidades publicadas suman uno y caen en el intervalo unidad; que ninguna
predicción de la próxima jornada corresponde a un partido ya disputado; que el
orden de columnas de la matriz de inferencia coincide bit a bit con el de
entrenamiento; que ninguna columna de `WEEKLY_UNAVAILABLE` alimenta el modelo
semanal; que el holdout se puntúa exactamente una vez; que el conjunto de
variables que se publica es el que quedó declarado y no otro; que toda cifra
económica viaja emparejada con la del apostante de referencia; y que las métricas
publicadas para el mercado y para el modelo se calculan sobre el mismo conjunto
de partidos, porque compararlas sobre coberturas distintas es la forma más fácil
de producir una mejora ficticia.

El **notebook 05** documenta el protocolo y es donde se ejecuta el banco de
ensayos: se componen conjuntos de variables a mano, se miden con las cuatro
familias y se comparan con el apostante de referencia. Presenta además las
figuras de calibración, desacuerdo con el mercado e interpretabilidad, y cierra
con la puntuación única del holdout. La búsqueda vive ahí y no en el orquestador
por una razón de método: es una decisión de analista, con su rastro visible, y no
un procedimiento automático cuyo resultado nadie vuelve a mirar. Lo que el
cuaderno decide se cierra en `build_models.py`, que ya no busca nada.

---

## 8. Orden de trabajo

Primero el **esqueleto sin modelo**: `protocol.py` con su banco de pruebas y
`evaluate.py` con la referencia de mercado y las referencias triviales. Cuando el
protocolo esté probado sobre un estimador trivial y las métricas reproduzcan las
del mercado, la infraestructura es de fiar y todo lo demás son piezas
intercambiables.

Después el **banco de ensayos** de la sección 3, incorporando la separación de
regímenes desde el primer ensayo. Se empieza por conjuntos pequeños y legibles
—mercado solo, mercado más fuerza— y se crece variable a variable, midiendo cada
paso contra el anterior y contra el apostante de referencia. Cada conjunto que se
retiene se cierra con su cifra y su intervalo antes de proponer el siguiente.

Cuando el banco haya agotado lo que se puede proponer a mano, la **búsqueda** de
`seleccion.py`: la criba de una variable sola para retratar el banco, la flotante con
sus cuatro puertas sobre el bloque de selección, y una única llamada a `mide` sobre
el de medición con el conjunto ya cerrado. El orden importa porque la última llamada
no se puede repetir sin convertir medición en un segundo bloque de selección.

A continuación las **mejoras de variables baratas** de `MEJORAS_PROPUESTAS.md`,
que a estas alturas ya pueden evaluarse con el protocolo funcionando, de modo que
cada bloque de variables nuevas se justifica con una diferencia medida en lugar
de con un argumento.

Después **interpretabilidad y publicación**, que es cuando la web empieza a tener
contenido real.

Y al final, una sola vez y sin vuelta atrás, el **holdout**.
