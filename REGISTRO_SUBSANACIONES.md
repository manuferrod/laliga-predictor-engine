# Registro de subsanaciones de la revisión técnica

**Alcance.** Este documento acompaña al informe de revisión del 27 de julio de 2026
(`INFORME_REVISION_2026-07-27.md`, en la raíz del proyecto) y deja constancia de qué
se ha hecho con cada hallazgo, con qué criterio y con qué evidencia. Su propósito es
doble: que cualquier lectura posterior del código —propia o ajena— encuentre la
justificación de las decisiones sin tener que reconstruirla, y que una futura
validación parta de este estado y no repita el diagnóstico ya cerrado.

Se mantiene la nomenclatura del informe: **C** para los hallazgos críticos e **I**
para los importantes. El estado *cerrado* significa que el código está modificado y
validado sin red; *pendiente de ejecución local* significa que el código está listo
pero requiere una pasada de scraping o de regeneración en la máquina de desarrollo,
que es donde vive el dato.

---

## 1. Criterios transversales adoptados

Las correcciones no son parches independientes: comparten cuatro criterios que
conviene enunciar una sola vez, porque se aplican a todo el código de extracción y
consolidación y deben seguir aplicándose a lo que se añada.

**Un artefacto nuevo nunca empeora al vigente.** Las aserciones de esquema
(`assert_columns`, `assert_non_null`, `assert_unique`) son vacuamente ciertas sobre
un marco vacío, de modo que un maestro ausente o truncado podía publicar un parquet
de cero filas superando todas las comprobaciones. La publicación pasa ahora por
`_common.publish_df`, que compara el número de filas contra el artefacto en disco
(`assert_no_regression`, umbral del 90 %) y escribe con copia de seguridad. La única
excepción documentada es el parquet de la jornada próxima, volátil por naturaleza.

**Ausencia de dato y valor cero son estados distintos.** Un equipo sin bajas es un
cero real; un equipo no consultado es un hueco. Colapsarlos hace que el preprocesado
impute la mediana de la liga a una plantilla completa. La distinción se materializa
en columnas explícitas (`queried`, `flag_no_absences`) y en agregaciones con
`min_count`, no en convenios implícitos.

**Fin de datos y fallo de transporte son estados distintos.** Un bucle de paginación
que rompe ante cualquier respuesta no satisfactoria convierte un 403 en "no hay más
partidos". La distinción se materializa en `_common.HarvestError`, que se propaga en
lugar de devolver una cosecha truncada con la misma apariencia que una completa.

**Lo que degrada en silencio acaba pareciendo un acierto.** Cada relajación de una
comprobación lleva asociada una decisión explícita sobre si el bloque es exigible; si
lo es, el aviso se eleva a error. Este criterio es el que gobierna el hallazgo I6.

---

## 2. Estado por hallazgo

| Ítem | Estado | Dónde |
|---|---|---|
| C1 — Cadena de artefactos anterior a la decisión D.0 | Pendiente de ejecución local | `build_master` → `build_features` → `qa_features` → `build_preprocess` |
| C2 — Transfermarkt no cubre 2015 | Cerrado (falta el scraping de 2015) | `transfermarkt.py`, `squadvalue.py` |
| C3 — Probabilidades de mercado estandarizadas | Cerrado | `preprocess.py`, `build_preprocess.py` |
| C4 — Sin paridad entrenamiento↔inferencia | Cerrado | `build_predict.py`, `preprocess.apply` |
| I1 — Bajas ponderadas no enganchadas en inferencia | Cerrado | `build_scoring.py` |
| I2 — Bajas inexistentes antes del partido | Cerrado | `availability.py` |
| I3 — "Cero bajas" indistinguible de "no consultado" | Cerrado | `availability.py` |
| I4 — Aplazados contados como carga de fatiga | Cerrado | `fatigue.py` |
| I5 — Fixtures sin ventana temporal | Cerrado | `build_scoring._future_window` |
| I6 — Solo las cuotas cortan la ejecución | Cerrado | `build_scoring.py`, `_common.load_checkpoint` |
| I7 — Bajas ponderadas sin lag t−1 | Cerrado | `absences.py` |
| I8 — Publicación de parquets vacíos o parciales | Cerrado | los ocho extractores, vía `_common.publish_df` |
| I9 — Paginación silenciosa y guardarraíl inalcanzable | Cerrado | `sofascore.py`, `fatigue.py`, `_common.HarvestError` |
| I10 — `squad_value` igual a 0,0 en lugar de NaN | Cerrado | `transfermarkt.py` |
| I11 — Ascendidos sin Elo al inicio de temporada | Cerrado | `clubelo.py` |
| I12 — Cobertura no validada de las estadísticas | Cerrado | `sofascore.py` |
| I13 — Notebooks 03 y 04 sin salidas ejecutadas | Pendiente de ejecución local | `notebooks/` |
| I14 — Contradicciones notebook↔código | Cerrado | `notebooks/02`, `notebooks/03`, `notebooks/04` |
| I15 — Doble escritor de `features_prep.parquet` | Cerrado | `notebooks/04`, E.5 |
| I16 — Sin README ni integración continua | Cerrado | `README.md`, `pytest.ini`, `.github/workflows/validacion.yml` |
| I17 — Falta la guarda de cuota en `_implied_probs` | Cerrado | `prematch_derived.py` |
| Menores — registro académico y residuos del repositorio | Cerrado | `notebooks/01`, `notebooks/03`, `scripts/extractors/`, `scripts/features/`, `.gitignore` |

---

## 3. Hallazgos críticos

**C1.** El máster en disco es anterior a la decisión D.0 (valor de plantilla con
retardo de una temporada), y todo el apartado D del notebook 04 está calculado sobre
él. No es un problema de código sino de artefacto: la corrección consiste en
regenerar la cadena completa en local, en el orden indicado en la tabla, después de
haber ejecutado el scraping de C2. Hasta entonces, ninguna cifra de D.3–D.6 debe
citarse en la memoria.

**C2.** `transfermarkt.py` arrancaba en 2016 en tres puntos distintos (constante,
rango del backfill y filtro de `process`), de modo que el retardo t−1 dejaba la
temporada 2016/17 entera sin valor de plantilla. Se introduce
`_VALUE_FIRST_SEASON = FIRST_SEASON - 1` y se usa en los tres. Queda por ejecutar
`backfill(seasons=[2015])`: una sola temporada de scraping que elimina de raíz el
10 % de histórico imputado.

**C3.** Las tres probabilidades implícitas del mercado entraban en el escalado y
salían del preprocesado con media cero y valores negativos, lo que inutilizaba
`market_baseline()` —la comparación central del trabajo— desde
`features_prep.parquet`. Se persisten las versiones crudas (`MARKET_RAW_COLS`) junto
a las escaladas, se comprueba en `build_preprocess` que esas columnas crudas **no**
forman parte de `feature_cols` (no pueden filtrarse al modelo) y `market_baseline()`
falla ruidosamente si lo que recibe no son probabilidades: valores fuera de [0, 1] o
sumas alejadas de la unidad.

**C4.** La ruta de inferencia construía la matriz cruda y la servía sin pasar por el
preprocesador. Se añade `build_predict.py` como punto de entrada único de la
predicción semanal, que encadena `build_scoring` → `PreprocessState.load` →
`preprocess.apply` y verifica cuatro invariantes antes de escribir: paridad de
esquema y de orden de columnas contra el artefacto de entrenamiento, ausencia de NaN,
y presencia del mercado en escala cruda. Complementariamente, `apply()` deja de
tolerar en silencio una matriz de inferencia más estrecha que la de entrenamiento.

---

## 4. Hallazgos importantes cerrados

**Inferencia (I1, I5, I7, I17).** `build_scoring` engancha ahora el bloque de bajas
ponderadas por valor llamando a `build_absences` con el mismo retardo t−1 que usa la
consolidación, de modo que el bloque de mayor información mutua deja de llegar en NaN
—y por tanto de marcarse como "sin bajas"— en todo partido futuro. Cuando el crudo no
está disponible, el bloque se devuelve en NaN de forma explícita, que es un hueco
honesto, no un cero. La selección de partidos a puntuar pasa por `_future_window`, que
recorta a `[hoy − stale_days, hoy + horizon_days]` y evita que un fixture rancio se
prediga indefinidamente. Y `prematch_derived._implied_probs` incorpora la guarda de
cuota `> 1.0` que ya tenían las otras dos implementaciones.

**Fatiga y disponibilidad (I2, I3, I4).** El universo de partidos a consultar deja de
salir exclusivamente del maestro de estadísticas —que solo contiene finalizados— y se
alimenta también del calendario, de modo que la rama de partido futuro deja de ser
código muerto. La agregación se reindexa contra el producto de partidos consultados
por lado y registra si hubo consulta, con lo que "cero bajas" y "sin dato" dejan de
confundirse. En fatiga, los encuentros no disputados salen de la ventana de
look-back: un aplazado ya no infla la congestión ni acorta el descanso de los
partidos vecinos con una carga que nunca existió, mientras que los aún no jugados sí
cuentan, por paridad con el entrenamiento.

**Publicación de artefactos (I8).** Todos los `process`/`load_processed` de los
extractores abortan con `RegressionError` si el maestro no existe y publican con
`_common.publish_df`. En `footballdata`, el mensaje advierte de que un parquet vacío
se llevaría por delante el target del proyecto. La excepción razonada es
`load_upcoming_processed`, cuyo tamaño es el de la próxima jornada y puede
legítimamente quedarse a cero entre jornadas.

**Valores de plantilla (I10).** La agregación usa `min_count=1`, el tamaño de
plantilla cuenta solo jugadores con valor y una aserción exige un mínimo por
club-temporada, de modo que una página que devuelva todos los valores como guion
produce NaN y no un cero plausible.

**Elo de los ascendidos (I11).** El sondeo de descubrimiento usa la fecha de hoy para
la temporada vigente en lugar del 1 de octubre, con lo que los recién ascendidos se
descubren desde la primera jornada. Se añade una aserción de cobertura de la
temporada en curso —al menos dieciocho clubes de Primera con Elo vigente— acotada a
la ventana competitiva (del 1 de agosto al 1 de junio) y en modo aviso hasta el 1 de
septiembre, para no dar falsas alarmas durante el tránsito de ascensos y descensos.

**Cobertura de las estadísticas (I12).** Se comprueba que el bloque `sofa_*` está
realmente poblado: tamaño mínimo del bloque de columnas, ausencia de filas sin
estadísticas y sin marca —que son fallos de descarga, no ausencias reales— y ausencia
de columnas completamente vacías, con un nivel de aviso para las métricas que
legítimamente no cubren todo el histórico. Las filas marcadas como sin estadísticas
en origen **se conservan**: son el único puente `match_id → match_key` que usan los
reductores de fatiga y disponibilidad, así que se excluyen del denominador de
cobertura en lugar de eliminarse.

**Gates de la ruta de inferencia (I6).** Antes de esta corrección, la única
comprobación capaz de interrumpir una predicción era la de cuotas: las lecturas de
fuente devolvían `None` ante cualquier excepción y los enganches de fatiga y bajas
quedaban dentro de un `try/except` que solo imprimía un aviso. La consecuencia no era
un fallo visible sino lo contrario: como `preprocess.apply` imputa las columnas
ausentes con la mediana de entrenamiento, un bloque desaparecido producía
probabilidades de aspecto normal, sin ningún síntoma. El criterio adoptado es el que
ordena todo el arreglo: *la ausencia de un dato que todavía no existe es información
legítima y se propaga como NaN; la ausencia de un dato que ya debería estar es un
fallo y se propaga como excepción.*

Se materializa en una política declarativa (`_politica_bloques`) que fija, para cada
bloque pre-partido, su fuente, su tolerancia de vacío y si es exigible, y en cinco
guardas encadenadas dentro de `build_scoring`. La de **lectura** (`_leer_fuente`)
separa el artefacto ausente del artefacto dañado: la corrupción aborta siempre —un
fichero roto no es un hueco de dato—, mientras que la ausencia solo aborta si el
bloque es exigible. La de **frescura** (`_freshness_gate`) cruza el calendario de
Sofascore contra el maestro y aborta si hay partidos ya jugados sin consolidar; se
mide por partidos pendientes y no por días transcurridos, porque un parón de
selecciones o el final de temporada harían saltar cualquier cuenta de días, y porque
un maestro rancio no genera NaN alguno: genera la forma y la clasificación de la
semana anterior, que es precisamente lo que ninguna comprobación posterior vería. La
de **cuotas** se mantiene como estaba. La de **enganche** eleva a `ScoringGateError`
los fallos de los reductores, que antes eran avisos. Y la de **cobertura**
(`_coverage_gate`) mide, bloque a bloque y partido a partido, qué fracción de la
jornada queda entera en NaN, la contrasta con la tolerancia declarada y, al abortar,
enumera los partidos afectados y cita la causa registrada aguas arriba. La medición
es por partido y no por celda porque promediar celdas diluye justamente el caso que
importa: un puñado de encuentros sin ningún dato del bloque.

Las tolerancias se fijan deliberadamente holgadas —Elo y valor de plantilla admiten
un 10 % por los recién ascendidos, fatiga un 25 %, las bajas nunca abortan y la edad
de los ficheros es solo un indicio informativo que jamás corta por sí mismo—, y cada
guarda dispone de un interruptor (`strict_blocks`, `strict_freshness`,
`strict_coverage`) que la degrada a aviso para exploración manual. Como corolario del
mismo criterio, `_common.load_checkpoint` descarta y elimina los puntos de control con
más de siete días: pasado ese plazo dejan de ser una reanudación y pasan a ser la
mezcla de dos cosechas separadas en el tiempo, cuyas claves suprimen la re-descarga
de partidos que entretanto han cambiado de estado.

**Coherencia entre notebooks y código (I14).** Los notebooks son la parte del
trabajo que un tribunal lee de principio a fin, de modo que una discrepancia con el
código no es un detalle cosmético: es la clase de error que hace dudar de todo lo
demás. Se corrigen cuatro. El cierre del notebook 03 anunciaba un preprocesado "aún
por implementar", cuando existe un módulo completo y un notebook entero dedicado a
él; ahora remite explícitamente a `preprocess.py`, a su orquestador y al notebook 04,
y explica que la garantía antifuga demostrada a nivel de *features* se prolonga hasta
la matriz de entrada al modelo. El encabezado de la Parte D del 04 citaba una ruta
inexistente (`scripts/preprocess/preprocess.py`) y describía la interfaz como
`fit(dev) → params` / `transform(split, params)`, cuando el módulo expone
`fit(dev) → PreprocessState` y `apply(df, state)`; se corrigen ambas, y también la
misma descripción errónea en el contrato antifuga de la portada. En el notebook 02,
la cobertura de las fuentes reducidas frente al *spine* se imprimía en la dirección
inútil —qué fracción de las filas de la fuente aparece en el *spine*, que es 1,0 por
construcción—, con lo que la disponibilidad declaraba un 100 % que el propio pie de
celda desmentía; se invierte el cálculo, de modo que ahora mide qué fracción del
*spine* cubre la fuente (3.353 de 3.800 partidos, 88,2 %, coherente con que la tabla
arranque en la 17/18). La cuarta discrepancia señalada —un máster de 203 columnas—
se ha verificado y **no existe en el material actual**: la única cifra presente es la
que imprime el propio ensamblado, 3.800 × 210, coincidente con `build_master()`.

**Escritor único del artefacto de preprocesado (I15).** `build_preprocess.py`
enuncia la disciplina —el notebook muestra y audita, el script ejecuta y persiste—
pero el apartado E.5 del notebook 04 escribía `features_prep.parquet` por su cuenta.
Dos rutas de escritura del mismo artefacto no pueden garantizar que coincidan, y la
que se audita en pantalla no tiene por qué ser la que acabe en disco. E.5 importa
ahora el orquestador, ejecuta `build_preprocess()` —que reproduce `fit(dev)` y
`apply` y verifica dentro las invariantes E.1 a E.5, incluida la de que el baseline
de mercado siga siendo calculable en escala cruda desde el artefacto— y comprueba que
el estado y el esquema resultantes coinciden con los auditados en las celdas
anteriores. La persistencia se documenta como una orden de línea de comandos, con su
variante `--dry` para auditar sin escribir. Como las celdas afectadas del 02 y del 04
cambian de código, sus salidas guardadas se han vaciado: son parte de la
re-ejecución pendiente de I13.

**Documentación de la cadena e integración continua (I16).** El repositorio no
declaraba en ninguna parte qué artefacto produce cada script ni en qué orden deben
ejecutarse, de modo que la cadena solo era reconstruible leyendo los orquestadores uno
a uno; y el directorio de flujos de trabajo estaba vacío, con lo que ninguna
comprobación se ejecutaba por sí sola. Se añade un `README.md` que fija el mapa de
artefactos —de cada fuente a su parquet publicado, y de los ocho parquets al maestro,
a las variables y a la matriz preprocesada—, la rama de inferencia semanal, las dos
secuencias de ejecución (reconstrucción completa y actualización de jornada, cuyo
orden no es indiferente porque fatiga y disponibilidad se apoyan en el calendario de
Sofascore), y las tres reglas de disciplina de artefactos enunciadas en el apartado 1.

El flujo `validacion.yml` se organiza en dos trabajos por una razón de fondo: `data/`
no está bajo control de versiones —los parquets se regeneran de forma reproducible
desde las fuentes, y versionarlos crearía una segunda verdad que envejece en
silencio—, así que un ejecutor limpio no puede correr las comprobaciones de calidad
sobre artefactos que no existen. El primer trabajo es hermético y siempre se ejecuta:
compila todos los módulos, lanza las baterías y verifica que no se haya versionado
ningún parquet ni ninguna copia de seguridad. El segundo declara las comprobaciones de
artefacto (`qa_master`, `qa_features` y `build_preprocess --dry`) condicionadas a la
presencia del maestro, de manera que el mismo flujo se activa sin cambios en un
ejecutor que sí disponga de los datos, en lugar de quedar sin escribir hasta entonces.
El entorno de validación se instala deliberadamente mínimo —`pandas`, `numpy`,
`scipy`, `pyarrow` y `scikit-learn`—: como los módulos importan las bibliotecas de
scraping de forma perezosa, dentro de las funciones que las usan, toda la lógica pura
se valida sin Chrome, sin `soccerdata` y sin `selenium`. Un `pytest.ini` acota la
recolección a `scripts/`, lo que además evita que entre en el entorno virtual y en el
material archivado, que traen sus propios ficheros `test_*.py` ajenos al proyecto.

**Paginación (I9).** Descrito en el criterio transversal correspondiente. Se
distinguen los finales legítimos —404, sobre de error de la API, página vacía y
`hasNextPage=False`— del fallo de transporte, que se propaga con el detalle de página
y volumen acumulado. En el calendario por equipo la confusión era más grave, porque
un tramo perdido no deja hueco visible sino un descanso artificialmente largo, es
decir, un valor plausible. La cosecha de fatiga contabiliza equipos fallidos, avisa
por debajo del 25 % y aborta por encima, volcando antes lo pendiente.

---

## 4 bis. Hallazgos menores

El informe agrupaba bajo el epígrafe *Menor* un conjunto heterogéneo de defectos que,
sin comprometer ningún resultado, sí comprometen la lectura: un tribunal que tropieza
con una cifra que no cuadra o con una frase de tono interno pierde confianza en el
resto. Se cierran todos.

**Registro académico.** La documentación arrastraba tres rasgos impropios de una
memoria: el lenguaje de versionado y registro de cambios («v1», «v2», «lo nuevo
respecto a…»), que describe la historia del desarrollo en lugar del método; las
referencias en primera persona y a la persona autora dentro del propio código y de
los notebooks; y la cita de un memorando interno de revisión como si fuera fuente
bibliográfica. La redacción pasa a ser impersonal y descriptiva del estado actual del
sistema, que es lo único que se defiende: el histórico del desarrollo vive en el
control de versiones y en este mismo registro, no en la memoria.

**Cifras y enumeraciones de los notebooks.** El notebook 01 declaraba «~110» columnas
para la tabla procesada de Sofascore en tres lugares distintos, cuando la salida
ejecutada de la propia celda de ensamblado imprime **124**; se corrigen los tres, más
un cuarto en el apartado descriptivo de la fuente. El mismo notebook enumeraba los
extractores omitiendo `fbref.py`, cuya situación —comprobación cruzada, no fuente del
maestro, tras la retirada del dato avanzado en enero de 2026— queda ahora explícita en
el apartado introductorio en lugar de deducirse por ausencia. En el notebook 03 se
ajusta la enumeración de atributos del bloque descriptivo para que coincida con la
que produce el código, y se elimina el parche temporal `_demo_add_clubelo`, un
monkey-patch de demostración que sustituía la lectura real del bloque de Elo: un
notebook que enseña un camino distinto del que ejecuta el orquestador es exactamente
el tipo de discrepancia que se corrigió en I14, en pequeño.

**Fecha ISO con marca de tiempo (`teamnames._iso_date`).** El reconocimiento de
formatos usaba patrones de prefijo, de modo que una fecha ISO con hora
(`2024-05-01 20:00:00`) entraba en la rama de fecha pura y fallaba después al
parsearse con `format="%Y-%m-%d"`. El resultado no era un error visible sino un `NaT`
que se propagaba hasta `match_key` y desconectaba ese partido de todas las demás
fuentes. Los patrones se anclan ahora por completo (`fullmatch`) y la variante con
hora tiene su propia rama, que trunca a los diez primeros caracteres antes de parsear.

**Corroboración del parón de selecciones (`fatigue.is_post_intl_break`).** La marca se
deducía únicamente del hueco de calendario del propio equipo, con lo que una lesión
larga, una eliminación europea o un aplazamiento producían la misma señal que una
ventana FIFA. Se corrobora contra el calendario de la competición: solo se marca
cuando el parón afecta a la liga entera, no a un equipo aislado. El banco de pruebas
de fatiga fija las tres situaciones —parón general, hueco de un solo equipo y
descanso ordinario— para que la distinción no se pierda en una modificación futura.

**Deduplicación por completitud (`_common.dedupe_preferring_complete`).** Las cosechas
sucesivas de Sofascore y Understat pueden devolver dos veces el mismo partido, una
con la ficha completa y otra con la ficha a medio publicar; quedarse con la última
observación, que es el comportamiento por defecto, equivale a preferir sistemáticamente
la peor. La función selecciona por número de campos informados, y su uso queda
centralizado en `_common` en lugar de repetirse en cada extractor.

**Bancos de prueba recogibles por `pytest`.** Los siete bancos offline se escribieron
como guiones con `sys.exit` en el cuerpo del módulo, lo que impedía que `pytest` los
recogiera: la salida se producía durante la importación y abortaba la sesión completa.
Cada uno expone ahora una función `test_*` que traduce el resultado al protocolo de
`pytest`, mientras que la interrupción del proceso queda confinada al bloque
`if __name__ == "__main__":`. Las comprobaciones siguen ejecutándose en el cuerpo del
módulo, que es lo que permite leerlos de corrido como un guion, y el código de salida
en línea de comandos se conserva. Es el paso previo a la integración continua de I16.

**Residuos del repositorio.** Se completa el `.gitignore` con los directorios
temporales que escriben las autocomprobaciones de los extractores (`tm_tmp/`), las
copias de seguridad de `_common.write_df_safe` (`*.bak`), las cachés de Python y los
notebooks ejecutados de descarte. Los residuos ya presentes en el árbol de trabajo se
eliminan en local, en el paso indicado en el apartado siguiente.

---

## 5. Validación

La validación de todo lo anterior es **offline y sin red**, conforme a la disciplina
del proyecto: el código se escribe y se prueba aquí, y el scraping se ejecuta en
local. Los bancos de prueba viven junto a los módulos (`scripts/extractors/test_*.py` y,
desde I6, `scripts/features/test_scoring_gates.py`) y cubren invariantes reales, no
humo: normalización de nombres, cobertura de Elo por ventana temporal, cobertura de
estadísticas, distinción entre cero y ausencia, distinción entre fin de paginación y
error, y —en el banco de la ruta de inferencia— artefacto ausente frente a artefacto
corrupto, partidos jugados sin consolidar frente a parón de selecciones, cada umbral
de cobertura por encima y por debajo de su tolerancia, y caducidad del punto de
control. Admiten dos modos de ejecución equivalentes: como guion, con el intérprete,
que imprime el detalle de cada comprobación y devuelve código de salida; y como
batería recogible por `pytest`, que es la forma en que los invoca el flujo de
integración continua (I16); `pytest.ini` acota la recolección a `scripts/`, de modo
que basta con `pytest` desde la raíz.

**Ejecución pendiente en local**, en este orden: scraping de Transfermarkt 2015 (C2);
regeneración de `build_master` → `build_features` → `qa_features` →
`build_preprocess` (C1); ejecución de los bancos de prueba, con `pytest` desde la
raíz o guion a guion; re-ejecución de los notebooks 02, 03 y 04 de
arriba abajo con las salidas guardadas (I13, y el 02 además porque las celdas de
cobertura corregidas en I14 han quedado sin salida), que es la evidencia que un
tribunal valora; y borrado de los residuos ya ignorados por git que siguen en el
árbol de trabajo: `tm_tmp/` —en la raíz y en `scripts/extractors/`—, los
`__pycache__/` y las copias `.bak` de `data/01_raw/`.

---

## 6. Trabajo restante

Todos los hallazgos del informe quedan cerrados en código. Lo único que resta es la
ejecución local enumerada en el apartado anterior, que es la que convierte las
correcciones de C1 y C2 en artefactos regenerados y las de I13 e I14 en notebooks con
salidas guardadas. El modelado (notebook 05), la automatización desatendida y la capa
web permanecen como trabajo futuro previsto, no como defectos.

Ese trabajo futuro está analizado en dos documentos que continúan a este y que conviene
leer antes de abrir la fase de modelado. `MEJORAS_PROPUESTAS.md` recoge el análisis
crítico de las cuatro capas ya construidas: no son defectos —de ahí que no figuren
aquí— sino decisiones correctas que pueden mejorarse, priorizadas por corrección
metodológica y por coste de obtención. Dos de ellas condicionan la validez de las
cifras que producirá la fase 5 y deben resolverse antes de entrenar: el ajuste único
del preprocesado sobre todo el bloque de desarrollo, que no sobrevive a una validación
temporal por pliegues, y el desajuste entre las cuotas de cierre disponibles en
entrenamiento e inexistentes en la ruta semanal. `DISENO_MODELADO.md` fija el protocolo
de validación, las cuatro familias de algoritmos, la batería de métricas, la arquitectura de
`scripts/models/` y el contrato de artefactos que consume la aplicación web.
