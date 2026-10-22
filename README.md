# laliga-predictor-engine

Motor de predicción del resultado 1X2 (victoria local, empate, victoria visitante)
de LaLiga. El repositorio contiene la cadena completa —extracción de las fuentes,
consolidación en un maestro de partido, ingeniería de variables estrictamente
pre-partido, preprocesado, protocolo de validación temporal, estimadores, evaluación e
interpretabilidad— junto a los notebooks
que la documentan y auditan. El alcance temporal es de la temporada 2016/17 en
adelante: 3.800 partidos, de los cuales la temporada más reciente (2025/26) queda
**sellada como holdout** y no se observa en ninguna decisión de diseño.

La disciplina de trabajo separa dos planos. Los notebooks **muestran y auditan**: se
leen de principio a fin y su valor probatorio está en las salidas ejecutadas. Los
scripts de `scripts/` **ejecutan y persisten**: son el único camino por el que un
artefacto llega a disco. Ningún artefacto tiene dos escritores.

---

## Mapa de artefactos

Cada fuente tiene un extractor propio en `scripts/extractors/`, todos con el mismo
contrato: `backfill` reconstruye el histórico, `update` refresca lo reciente,
`process` normaliza el crudo a la tabla publicable y `weekly` encadena los dos
últimos. El crudo se acumula en `data/01_raw/` y lo publicable en `data/02_processed/`.

Una regla gobierna ese reparto y conviene declararla aquí porque se incumple sin dar
error: **el crudo guarda nombres, las claves canónicas se derivan al leer**. La clave
de un club no es un dato, es una función de la tabla de alias de `teamnames.py`, y esa
tabla cambia justo cuando aparece un club nuevo. Un recién ascendido se extrae el
primer fin de semana de agosto con su alias todavía sin declarar, el CSV acumulativo
congela el slug de reserva, y a partir de ahí **ninguna re-extracción lo corrige**: el
crudo salta por `row_id` lo que ya tiene guardado. Leer la clave almacenada en lugar de
recalcularla deja al club ascendido sin enganche —claves que no coinciden con las del
maestro, variables de calendario nulas para él y solo para él— sin que nada falle.
Derivarla al leer hace que cualquier mejora de la tabla de alias se aplique al
reprocesar, sin volver a la red. `fatigue.py`, `lineups.py` y `availability.py` lo
declaran en su propio código y `build_scoring.py` lo cumple en un único sitio.

```
FUENTE            EXTRACTOR            ARTEFACTO PUBLICADO
─────────────────────────────────────────────────────────────────────────────
football-data     footballdata.py  →   footballdata_match.parquet      (resultado 1X2 + cuotas)
                                       footballdata_upcoming.parquet   (cuotas de la próxima jornada)
   ″  (teclado)   build_scoring.py →   footballdata_odds_manual.csv    (cuotas tecleadas, prevalecen)
Understat         understat.py     →   understat_match.parquet         (xG, npxG, PPDA, deep, xPts)
   ″              understat_shots.py → understat_shots.parquet         (un disparo por fila)
                                       team_shot_quality.parquet       (forma de la distribución de remates)
Sofascore         sofascore.py     →   sofascore_team_match.parquet    (124 métricas equipo-partido)
   ″              fatigue.py       →   team_fatigue.parquet            (descanso y congestión multicompetición)
   ″              availability.py  →   team_availability.parquet       (bajas, sanciones y dudas)
   ″              lineups.py       →   team_lineups.parquet            (rotación del once y carga de minutos)
                                       player_minutes.parquet          (minutos por jugador y partido)
ClubElo           clubelo.py       →   clubelo_elo.parquet             (rating Elo por club y fecha)
Transfermarkt     transfermarkt.py →   team_squad_value.parquet        (valor de plantilla, retardo t−1)
                                       transfermarkt_player_values.parquet
FBref             fbref.py         →   (comprobación cruzada; no alimenta el maestro)
```

`fbref.py` se conserva como comprobación cruzada y no forma parte del maestro: tras
la retirada del dato avanzado a nivel equipo-partido en enero de 2026, lo que sigue
publicando es un marcador básico redundante con Sofascore.

De ahí se sigue una limitación que conviene declarar antes de que la lea nadie en las
cifras: **la cadena no dispone de una segunda fuente independiente para la estadística
de partido**. Sofascore es la única que publica el detalle de volumen, duelos y pases,
de modo que un error silencioso suyo —una métrica mal mapeada, un partido con el marcador
invertido— no tiene contra qué contrastarse columna a columna. Lo que sí queda es el
contraste entre lo que dos proveedores distintos miden a la vez: el detalle de tiro de
Understat produce, por equipo y partido, un recuento de remates que debe parecerse al de
Sofascore, y el notebook 01 lo comprueba. Es una verificación parcial —cubre el volumen
ofensivo, no las ciento y pico métricas restantes— y por eso se declara como tal. Si en
algún momento se incorporase una fuente que restituyera la redundancia perdida, su papel
sería el mismo: **control de calidad, no variable**, porque duplicar en el conjunto de
entrada una magnitud que ya viaja en él añade colinealidad y no información.

`clubelo.py` es el único extractor que **empalma dos series de la misma fuente**, y
conviene declararlo porque la decisión condiciona la forma en que el Elo llega al
modelo. ClubElo publica hoy el rating en las fichas por país de su web
(`clubelo.com/ESP`, una por club) y ya no en la ruta de descarga que servía el
archivo histórico, de modo que el crudo son dos CSV —`clubelo_laliga_elo.csv`, el
archivo de la ruta antigua desde 16/17 hasta el 18-10-2022, y
`clubelo_laliga_elo_web.csv`, la serie que la web sirve desde entonces— que `process`
une en `clubelo_elo.parquet` dejando constancia del origen de cada tramo en la
columna `fuente`. El Elo no se recalcula en ningún momento: es un dato de la fuente,
y mantenerlo coherente manda sobre cualquier comodidad de implementación.

Lo que separa las dos series es que **ClubElo rebasó su escala** en ese corte: la
serie nueva está desplazada unos 85 puntos y su dispersión comprimida alrededor de un
11 %, así que los niveles crudos de los dos tramos no son comparables entre sí. El
empalme no los parchea con un desplazamiento estimado —eso sería inventar dato— sino
que el modelo consume el Elo **tipificado dentro de la jornada**: `home_elo_z` y
`away_elo_z`, cada rating en desviaciones respecto a los de los veinte equipos de
Primera de esa fecha. La tipificación dentro de la fecha es el invariante de una
transformación afín, de modo que un rebase de escala la deja exactamente igual, y la
base de normalización es el censo de Primera que el propio proyecto mantiene por
temporada, nunca el campo `level` de ClubElo. El nivel crudo (`home_elo`, `away_elo`,
`elo_diff`) se conserva en el maestro como columna descriptiva y auditable, pero no
entra en la matriz de variables; del bloque llegan al modelo cinco columnas, el par
tipificado y las tres derivadas que se calculan ya sobre la `z`
(`elo_mom_5_z_diff`, `form_opp_elo_z_3_diff`, `form_opp_elo_z_ewm_diff`). La séptima
invariante de `qa_master` mide el precio del empalme: el salto de la `z` en el corte
(n=20) tiene mediana 0.073 y máximo 0.194, por debajo del máximo de una semana
corriente de competición (n=7380, mediana 0.048, p99 0.180, máximo 0.343).

`lineups.py` es el único extractor que publica dos artefactos, y la razón es que
sirve a dos preguntas con granularidades distintas. `team_lineups.parquet` describe
al equipo —cuántos titulares repite respecto al partido anterior, cuántos minutos
llevan encima esos once, en cuántos futbolistas se concentran los minutos de la
temporada— y `player_minutes.parquet` describe al futbolista, con una fila por
jugador y partido. El segundo no alimenta ninguna variable por sí mismo: es la
materia prima que permite preguntar, para un ausente concreto y en un instante
concreto, qué cuota de los minutos de su equipo venía disputando. La alineación es
un hecho **del** partido y por tanto una fuga si se usa cruda; lo que llega al
maestro son siempre agregados calculados mirando exclusivamente hacia atrás.

La cadena de consolidación y preparación encadena cuatro artefactos, cada uno con su
comprobación de calidad inmediatamente después. Tres de esas fuentes —las
alineaciones, los minutos por jugador y la forma de la distribución de remates— se
leen como **opcionales**: nacen de volcados iniciales largos y de una sola vez, de
modo que el maestro se construye igual sin ellas, con el bloque correspondiente en
hueco declarado, en lugar de exigir haber ejecutado esos volcados para poder recorrer
la cadena entera:

```
  los parquets de fuente
          │   scripts/consolidate/build_master.py
          ▼
  master_matches.parquet          3.800 partidos  ← qa_master.py
          │   scripts/features/build_features.py
          ▼
  features_matches.parquet        3.800 filas     ← qa_features.py  (invariantes antifuga)
  feature_lineage.parquet         procedencia por columna
  coverage_matrix.parquet         cobertura por partido
          │   scripts/features/build_preprocess.py
          ▼
  features_prep_<régimen>.parquet + preprocess_state_<régimen>.json
```

El preprocesado se ajusta además una segunda vez, dentro de cada pliegue de la
validación temporal, y ese ajuste no pasa por el artefacto anterior sino que parte
del conjunto de variables:

```
  features_matches.parquet
          │   scripts/models/protocol.py
          │   (pliegues por jornada; preprocess.fit dentro de cada ventana)
          ▼
  predicciones fuera de muestra, una fila por partido evaluado
```

El estado congelado que publica `build_preprocess.py` y el estado que reajusta el
protocolo cumplen funciones distintas y por eso conviven. El primero es el que
consume la ruta de inferencia: para puntuar la jornada por jugar hace falta un
estado fijo, aprendido sobre todo el desarrollo, porque la alternativa —reajustar
en producción— haría que dos predicciones emitidas en semanas distintas no fueran
comparables. El segundo existe porque medir es otro problema que predecir: dentro
de la validación, un estado único aprendido sobre todo el desarrollo conocería la
distribución futura del pliegue que evalúa.

Junto al conjunto de variables se publican dos artefactos de **trazabilidad**, que
`scripts/features/lineage.py` genera a partir de las mismas constantes que usan los
módulos de variables. `feature_lineage.parquet` da una fila por columna con su
bloque analítico, el atributo que mide, sus fuentes y su régimen; es el anexo de la
memoria. `coverage_matrix.parquet` da una fila por partido declarando qué fuentes
llegaron —medido sobre el maestro, donde un hueco solo puede significar que el dato
no existe— y qué bloques son calculables —medido sobre las variables, donde un hueco
puede además ser el arranque en frío de una ventana móvil—. Leídas juntas, las dos
lecturas distinguen el fallo de una fuente del efecto esperado del diseño antifuga.

La procedencia no se transcribe: `qa_features.py` exige igualdad de conjuntos entre
las columnas del conjunto y las declaradas, de modo que una variable nueva sin
origen documentado detiene la cadena en lugar de llegar al modelo sin justificar.
La ruta de inferencia publica su propia `coverage_upcoming.parquet` con la misma
definición, que es lo que permite auditar, semanas después, con qué información se
emitió una predicción concreta.

El preprocesado se ajusta bajo dos **regímenes de información**, que se distinguen
por el momento en que cada dato llega a estar disponible. Ser pre-partido significa
existir antes del pitido inicial; no significa existir cuando se emite la
predicción, que es lo único que el modelo publicado puede usar. El precio de cierre
es el caso que obliga a separarlos: solo aparece minutos antes del saque, cuando la
jornada ya está puntuada.

```
  semanal          solo lo conocible al predecir. Es el modelo que se publica y
                   cuya métrica se defiende como capacidad predictiva del sistema.
  retrospectivo    añade el precio de cierre y la casa afilada. No se publica; su
                   comparación con el semanal mide cuánta información aporta el
                   mercado entre la apertura y el cierre, es decir, el techo con
                   información pre-partido completa.
```

El régimen se declara en `scripts/features/columns.py`, gobierna el ajuste completo
—poda, imputación, transformación, escalado y flags de faltante— y queda impreso en
el estado congelado. Ambos regímenes se ajustan con el mismo protocolo, que es la
condición para que la diferencia entre ellos se lea como una magnitud del dominio y
no como el efecto de haber cambiado a la vez las variables y el método.

El cierre queda fuera del régimen semanal por **convenio de nombres** —toda columna
derivada de él lleva el token `close`, de modo que una variable nueva que respete el
convenio queda cubierta sin editar ninguna lista—, respaldado por una lista explícita.
Pero hay un **segundo motivo** de indisponibilidad que no se resuelve por convenio
porque no tiene token utilizable, y que es el mismo error de categoría recorrido al
revés: una cuota de **apertura** es pre-partido y es legítima, y eso no la hace
servible si la fuente no la publica para la jornada por jugar. El feed semanal de
football-data trae Bet365, el máximo y la media, y **no trae Pinnacle**, de modo que
`odds_ps_*` y todo lo derivado de ella —su margen y su divergencia con el consenso—
llegan vacías cada semana aunque en el desarrollo estén en el 97.9 % de los partidos.
Se declaran una sola vez en `columns.SHARP_ODDS`, enumeradas en lugar de resueltas por
subcadena porque «ps» casaría con media docena de columnas inocentes. Dos motivos
distintos, una sola consecuencia; y el peor de los dos es el segundo, porque no deja
rastro: nada avisa de que una cuota de apertura no esté disponible al emitir.

La ruta de inferencia semanal parte de la jornada por jugar y termina en una matriz
con el mismo esquema y el mismo orden de columnas que la de entrenamiento, que es la
condición para que el modelo pueda puntuarla. Consume el estado del régimen
`semanal` y lo verifica antes de escribir nada:

```
  footballdata_upcoming.parquet + calendario de Sofascore
          │   scripts/features/build_scoring.py
          │   (guardas de lectura, frescura, cuotas, enganche y cobertura)
          ▼
  features_upcoming.parquet + coverage_upcoming.parquet
          │   scripts/features/build_predict.py
          │   = build_scoring → PreprocessState.load → preprocess.apply
          ▼
  features_predict.parquet
```

### El precio de la jornada por jugar, cuando la fuente no lo publica

Esa ruta tiene un punto único de fallo que no es un defecto del código sino del
calendario de publicación de la fuente: **football-data sirve el fixture y el precio
juntos o no sirve ninguno de los dos**. Su `fixtures.csv` es una ventana de pocos días
que aparece cuando a la fuente le parece, de modo que a mitad de semana la jornada que
empieza el viernes no existe para la cadena: ni fila que puntuar ni cuota con la que
hacerlo. Y las tres variables de precio son `sl.SEMILLA_MERCADO`, el núcleo del modelo
publicado, así que sin ellas no hay emisión posible.

El circuito que lo resuelve tiene tres piezas, y la segunda es la única que requiere
teclado:

```
1. SIEMBRA     build_scoring._seed_from_sofascore
               el calendario de Sofascore conoce la jornada con semanas de
               antelación; aporta el fixture SIN cuota, recortado a la primera
               jornada que viene
2. PLANTILLA   build_scoring._write_odds_template → MissingOddsError
               data/01_raw/footballdata/footballdata_upcoming_manual_odds.csv
               una fila por partido sin precio; se rellena `odds_h/d/a` y se
               reejecuta
3. PREVALENCIA build_scoring.promote_manual_odds → footballdata_odds_manual.csv
               footballdata.apply_manual_odds, en `load_processed`
               lo tecleado se guarda y se vuelve a aplicar en cada reproceso del
               histórico
```

**La siembra corta por el hueco de calendario y no por el número de jornada**
(`_GAP_JORNADA = 3`, el mismo criterio y el mismo valor que `protocol._GAP_DIAS`). El
recorte no es cosmético: `fixtures.csv` se autolimita a ~7 días y por eso
`_HORIZON_DAYS = 10` nunca llegó a abarcar dos jornadas, pero Sofascore conoce la
temporada entera y con el mismo horizonte siembra la jornada siguiente **y la de
después** —diecinueve partidos en lugar de diez—. Los nueve de más son partidos para
los que ninguna casa ha abierto mercado todavía: teclear su cuota sería inventarla. Y
el corte va por hueco porque la `round` nominal es una etiqueta que un aplazamiento
desordena, igual que en el protocolo. Se siembra solo `status == notstarted`, solo lo
que no está ya en el máster ni en el fixture de football-data —que **manda cuando lo
tiene**, porque trae el precio real de las casas— y la procedencia queda en
`fixture_src`, para que el informe de completitud pueda distinguir un partido con cuota
automática de uno que espera teclado.

**El alcance de la prevalencia es una decisión con criterio, y conviene leerla antes de
ampliarla.** El override toca exclusivamente `odds_avg_h/d/a`, que es el canal de
entrada del modelo: de ahí salen `mkt_p_home/draw/away`. **No toca `odds_b365_*`,
`odds_ps_*`, `odds_max_*` ni ninguna columna de cierre**, y el motivo es la regla 3 —las
columnas de Bet365 son el listón contra el que se publica toda cifra económica
(`backtest.REFERENCIA = "b365"`)—: si una cuota escrita a mano pudiera entrar ahí, la
pregunta que motiva el trabajo se contestaría contra un precio que no publicó ninguna
casa y que nadie puede auditar. Hay además un argumento más simple: cuando football-data
publica el partido ya jugado trae la cuota de **cierre**, que es mejor dato que la de
apertura que se vio al teclear, de modo que pisarla degradaría el histórico en lugar de
corregirlo. Lo que prevalece es **con qué precio predijo** el modelo; lo que se conserva
es **a qué precio se liquida**. Son dos preguntas distintas y el override solo contesta
la primera.

**Y el límite que lo hace defendible es la regla 6 escrita en código**: la cuota se
aplica solo si su sello `entered_ts` es anterior al día del partido. Sin esa
comprobación nada impediría «corregir» el precio de un partido de 2019 y mejorar el
backtest de un bloque ya cerrado. Con ella el override queda confinado por construcción
a partidos que estaban por jugarse cuando se tecleó —en la práctica, la temporada en
curso— y los bloques de validación son intocables. Por el mismo motivo la promoción al
registro conserva la entrada **más antigua** de cada partido y no la última, que es lo
contrario de lo que haría un upsert corriente: sobreescribir el sello con el de una
reejecución posterior al pitido final haría que el propio límite rechazara la fila y el
override desaparecería en silencio justo cuando ya era historia.

Las dos vías son indistinguibles aguas abajo —la cuota teclada y la publicada pasan por
el mismo `_implied_probs`, con el mismo rechazo de márgenes imposibles entre 0.95 y
1.60—, y eso es exactamente lo que obliga a declarar la procedencia: `odds_src` vale
`"footballdata"` o `"manual"` en el histórico y `"auto"` o `"manual"` en el informe de
emisión. Sin esa columna la desviación entre el precio que se vio y el que la fuente
publicó después —que es medible y vale como resultado— quedaría invisible.

`build_predict.py` cierra la ruta con seis guardas numeradas, y la razón de que sean
seis y no cinco es que la sexta vigila lo que las otras no pueden ver. P.1 exige
paridad de features entre entrenamiento e inferencia; P.2, que el **orden** de las
columnas sea el del artefacto de entrenamiento; P.3, que no queden nulos tras
`apply()`; P.4, que el baseline de mercado siga siendo calculable; P.5, que el estado
congelado sea el del régimen publicable y que su matriz lo cumpla. Las cinco miran la
matriz **después** de preprocesar, y por eso ninguna puede ver el sesgo
entrenamiento-servicio: imputar es precisamente lo que lo esconde. Una columna que la
ruta semanal no trae no llega como hueco, llega como la mediana de desarrollo
repetida en los diez partidos de la jornada, cada jornada. El conjunto de features
cuadra, el orden cuadra, no hay un solo nulo que contar, y el modelo puntúa apoyándose
en una constante: la métrica histórica describe entonces un sistema distinto del que
se ejecuta cada semana.

**P.6** cierra ese hueco comparando, para cada variable del modelo, la tasa de
ausencia en desarrollo con la de la jornada por jugar. Se mide sobre las matrices
**crudas**, porque después de imputar ya no hay nada que medir, y con la misma
definición a los dos lados —nulos en la columna— para que las dos tasas sean
comparables sin convenios. Denuncia el **desplome**: la variable que está en el
histórico y no está nunca al emitir. No denuncia la variación legítima del calendario
y los partes médicos, porque un gate que avisa de más se desactiva y entonces no
protege de nada. Los flags de faltante no servían para esto: hay seis, uno por
**bloque**, y el desplome de una columna suelta no enciende ninguno. El caso que la
motivó fue Pinnacle, ya cerrado por declaración; la guarda se queda porque la clase de
avería reaparece sola —una casa que deja de publicar, un endpoint que cambia de
esquema— y conviene que avise aquí y no en la memoria.

Los dos últimos tramos de la cadena ya no producen datos sino **evidencia** y
**contrato**, y por eso viven en `outputs/` y no en `data/`:

```
  features_matches.parquet (+ features_predict.parquet para la emisión)
          │   scripts/models/laboratorio.py + notebooks/05  ← el banco manual
          │   (ensayos a mano: un conjunto de variables, una familia, y sus cifras
          │    frente al apostante de referencia; el banco mide y quien lee decide)
          ▼
  outputs/models/laboratorio/registro.parquet   la memoria del estudio: un ensayo
                              por fila, con su sello, sus variables y sus cifras
          │   scripts/models/seleccion.py       ← la búsqueda del conjunto
          │   (elige variables en 2019/20–2021/22 y solo ahí; confirma en
          │    2017/18–2018/19, anteriores a la elección, que lo adoptado sigue
          │    ganando donde no se eligió; mide en 2022/23–2024/25 una vez, con el
          │    conjunto ya cerrado; 2025/26 sigue sellada)
          ▼
  outputs/models/seleccion/   traza de la búsqueda (un paso por fila) y conjunto
                              elegido en json, con su sello, su corte y su parada
          │   scripts/models/build_models.py
          │   (el conjunto ya elegido, medido en los dos regímenes; aquí no se busca)
          ▼
  outputs/models/<régimen>/   métricas del conjunto cerrado, predicciones, economía
                              del modelo y del apostante de referencia por temporada
                              y por jornada, importancias, manifiesto.json y
                              holdout_sello.json
          │   scripts/models/qa_models.py      ← nueve invariantes de publicación
          │   scripts/models/publish_web.py
          ▼
  outputs/web/                próxima jornada, explicaciones, radar, perfiles,
                              diagnósticos, ficha del modelo y manifiesto_web.json
```

Ninguno de los dos directorios se versiona: se reconstruyen de forma reproducible
desde los artefactos de datos y el código de ejecución que los estampa permite
comprobar, semanas después, que las tablas que se están leyendo juntas proceden de la
misma pasada.

---

## Validación temporal

`scripts/models/protocol.py` define **cómo se mide** el sistema. Su única salida es
un conjunto de predicciones fuera de muestra: no conoce ningún estimador concreto
—los recibe como una fábrica— ni calcula ninguna métrica. La separación es
deliberada, porque el protocolo debe poder auditarse sin saber qué modelo se
entrenó.

La muestra no se parte en dos sino en cuatro, porque las temporadas antiguas, las
recientes y la que se está jugando no admiten la misma disciplina:

```
  2016/17 – 2018/19   arranque. Carecen de cuota de cierre y de parte médico.
                      Entran en el entrenamiento —alimentan las ventanas de forma,
                      de Elo y de historial— pero no se evalúan: una métrica
                      calculada sobre ellas mezclaría el rendimiento del modelo
                      con la escasez de la fuente.
  2019/20 – 2024/25   validación. Aquí se ejecuta el walk-forward, se comparan
                      modelos y se eligen hiperparámetros. Toda cifra que sirva
                      para decidir algo sale de este bloque.
  2025/26             holdout sellado. Se puntúa una sola vez, con el modelo ya
                      cerrado, y su resultado se reporta sin reajustar nada.
  2026/27             producción: la temporada EN CURSO, la que el sistema predice
                      cada semana. Se puntúa jornada a jornada con el modelo ya
                      cerrado y da la tercera cifra que el trabajo necesita —cómo
                      va el modelo en la temporada que se está jugando—, pero no
                      decide nada.
```

El cuarto bloque no es un adorno: es el que impide una fuga concreta. La temporada en
curso es posterior al holdout, así que la ventana de entrenamiento de cualquiera de
sus jornadas contiene las 380 filas de la temporada sellada —necesariamente, y eso
está bien: el modelo que de verdad se publica en octubre de 2026 ha visto 2025/26, y
negarse a usarla empobrecería la predicción sin ganar nada de método—. Lo que no puede
pasar es que esas predicciones entren en una decisión: si el banco que compara familias
las evaluase, la familia se elegiría en parte mirando predicciones de un modelo que vio
el holdout, y el holdout dejaría de ser una prueba independiente sin que nada diera
error. De ahí que `produccion` sea un bloque aparte, que la temporada en curso quede
fuera de `validacion`, `seleccion` y `medicion`, y que `laboratorio.py` se niegue por
`PermissionError` tanto a ajustar como a evaluar sobre ella. Sus predicciones viajan en
`predicciones_produccion.parquet` —en su propio fichero, porque un artefacto llamado
«validacion» no puede contener la temporada en curso sin desmentirse— y rotuladas con
la columna `bloque` en las tablas descriptivas que lee la aplicación.

Ese rótulo tiene una regla, y la regla tiene dos mitades. **Lo que se desglosa** por
temporada lleva las dos poblaciones, cada fila con su bloque: la temporada en curso es
una de las tres cifras que el trabajo necesita —el agregado, el desglose por temporada
y cómo va el modelo en la que se está jugando— y tiene que publicarse. **Lo que produce
una sola cifra** lleva solo validación: la fila `total` de `fiabilidad` y `confusion`,
la tabla de desacuerdo, y el bloque `economia` del manifiesto con su intervalo. La
temporada en curso tiene su propia economía, con su propio listón de Bet365, en
`en_curso.economia` del manifiesto y en las filas `bloque = produccion` de
`economia_por_temporada` y `economia_por_jornada`.

El reparto no es presentación. Un agregado que incluya la temporada en curso ya no
responde a la pregunta del trabajo, y la avería no se ve: no rompe ninguna columna ni
cambia el número de filas, solo desplaza el número unas décimas en la dirección
favorable. En la economía, además, sumar no sería «una temporada más»: la curva
acumulada, la caída máxima y el intervalo por remuestreo son funciones de toda la
serie, de modo que medirlas juntas daría una cifra que no es ninguna de las dos. El
rótulo es también lo que permite que `qa_models.check_economia` cuadre el cierre de la
serie contra el manifiesto; sin él cuadraría contra la última jornada jugada y fallaría
cada semana por el motivo equivocado, que es la forma más rápida de que una
comprobación deje de leerse.

Conviene no confundir dos actos que la palabra «abrir» mezcla: **usar** 2025/26 como
pasado de entrenamiento de 2026/27 es producción corriente, y **puntuarla** para leer
su métrica es el acto único que `build_models.py --holdout` sella. El primero no
consume el segundo.

Qué temporadas son «en curso» se deduce del propio dato —las posteriores a
`columns.HOLDOUT_SEASONS`, vía `protocol.temporadas_en_curso()`— y no se escribe a mano
en ninguna lista. Una lista literal envejece en silencio: el primer fin de semana de
agosto de 2027 la temporada nueva se colaría en el bloque de desarrollo y ninguna
comprobación lo diría.

El protocolo avanza **por jornada y no por partido**: avanzar partido a partido
daría al modelo el resultado de los encuentros del sábado para predecir los del
domingo, una ventaja que en producción no existe. Ahora bien, la jornada nominal
que publica Sofascore no sirve como unidad, porque un aplazamiento la desordena —un
partido de la jornada 5 disputado en febrero seguiría rotulado como jornada 5 y
arrastraría hacia atrás todo el pliegue—. `effective_matchday` reconstruye por
tanto el conjunto de partidos que se puntuarían juntos: bloques contiguos del
calendario, separados cuando media un hueco de varios días o cuando un equipo
aparecería dos veces en el mismo bloque. Esa segunda condición garantiza por
construcción la propiedad que importa, que dentro de un pliegue ningún equipo juegue
dos veces, y `matchday_report` deja por escrito en qué medida la reconstrucción
coincide con el rótulo administrativo.

Sobre esa unidad se ofrecen las dos variantes de ventana que la memoria compara,
porque cuál es mejor no es obvio a priori y la respuesta es un resultado en sí
misma: la **expansiva** usa todo el pasado disponible y maximiza el tamaño de
muestra; la **deslizante** usa las últimas temporadas y apuesta por que el fútbol
reciente es más informativo que el antiguo. Sobre cualquiera de las dos puede
superponerse un peso de recencia con decaimiento exponencial en días, gobernado por
una **semivida**: un partido de hace tantos días pesa la mitad que uno de hoy. La
semivida es un eje más del ensayo, con sus valores de referencia declarados en
`protocol.SEMIVIDAS` —de media temporada a cinco años, más el valor `None`, que
significa «todo el pasado pesa igual» y se prueba como un candidato y no como el
supuesto de partida—. Se fija al abrir el ensayo, como la familia o el conjunto de
variables, y se compara midiendo: dos ensayos que solo difieren en la semivida tienen
sellos distintos y se leen uno al lado del otro. Ponderar tiene un precio medible, la
caída del tamaño muestral efectivo, y que compense o no es un resultado del trabajo.

**Y es un resultado negativo, medido.** Se recorrieron las treinta combinaciones de
ventana (expansiva y deslizante de 2 a 6 temporadas) por semivida (sin peso, 180, 365,
730 y 1460 días) sobre el bloque de selección —1140 partidos, regla 6— y con dos
conjuntos, para que la respuesta no dependiera de uno. **La expansiva sin peso es la
mejor de las treinta, con los dos conjuntos, y no hay una sola configuración que la
bata.** Acortar la ventana sin ponderar es indiferente —deslizante-5 pierde 0.000028 de
RPS con la semilla y 0.000015 con el conjunto de cinco, y deslizante-6 da exactamente la
cifra de la expansiva, porque con el arranque dentro no queda más pasado que recortar—,
así que acortar no hace daño: no sirve de nada. Ponderar **sí** hace daño, de forma
monótona y tanto más cuanto más corta la semivida: la de 180 días es la peor de las cinco
en las dos lecturas (−0.000732 con la semilla, −0.001179 con el conjunto de cinco, este
con intervalo [−0.00242, −0.00001], que excluye el cero).

La hipótesis que se pone a prueba ahí es razonable —que el fútbol de hace nueve
temporadas no informe sobre la jornada que viene— y **no sobrevive a la medida**. El
motivo mecánico es que este es un modelo dominado por el precio, y el precio ya es
reciente por construcción: cada cuota es la lectura que el mercado hizo esa semana. Lo
que el pasado antiguo aporta no es información sobre la forma de los equipos, sino
partidos con los que estimar la relación entre cuota y resultado; para eso, cuantos más,
mejor. El acierto se mueve a veces en sentido contrario —deslizante-2 sin peso llega al
53.77 %— y por eso no decide: manda el RPS. Las cifras están escritas junto a los
valores por omisión, en la sección 2 de `protocol.py`.

Qué columnas pueden ser variables lo decide una sola función, `preprocess.feature_candidates`,
y es pública precisamente para que no haya dos respuestas: el preprocesado la usa para
ajustar, el banco manual para ofrecer variables y los cuadernos para contarlas. Quedan
fuera las claves y los metadatos del partido, y quedan fuera las probabilidades
implícitas de Bet365, que en este proyecto son la referencia contra la que se mide y no
pueden ser a la vez una de las cosas medidas. El filtro de régimen se aplica encima, de
modo que ninguna capa posterior llega a ver una columna que el régimen no admite.

El punto delicado es el preprocesado. `preprocess.fit` aprende medianas de
imputación, parámetros λ de simetrización y —sobre todo— la selección de variables
de la poda colineal, que es supervisada. Ajustarlo una sola vez sobre todo el
desarrollo no filtra el resultado del partido que se predice, pero sí filtra la
**distribución futura**: el pliegue de 2019 sabría qué variables acabarían siendo
informativas en 2024. El protocolo lo resuelve reajustando el estado dentro del
pliegue, con una cadencia declarada:

```
  temporada   (por defecto) el estado se reajusta al empezar cada temporada, con
              datos estrictamente anteriores a ella. Es un resumen distribucional
              que se mueve despacio, de modo que reajustarlo cada jornada
              multiplicaría el coste para mover unas medianas que apenas cambian.
  jornada     el estado se reajusta en cada pliegue. Es la lectura más estricta y
              la que mide cuánto importaba la cadencia; su coste es de cómputo.
  unico       un solo ajuste sobre todo el desarrollo. No es un protocolo válido:
              se conserva como término de comparación, para publicar la diferencia
              entre hacerlo bien y hacerlo mal en lugar de afirmar que importa.
```

El estado se aprende siempre sobre las filas de desarrollo de la ventana. En los
pliegues de validación eso no cambia nada, porque la ventana es desarrollo entera;
en la pasada del holdout es lo que mantiene el sello, porque el estado que
transforma el holdout se ha aprendido sin mirarlo, aunque la ventana de
entrenamiento del estimador sí incorpore las jornadas del holdout ya disputadas,
que en el momento de predecir son pasado legítimo.

Como efecto secundario, el reajuste corrige la deriva de nivel de la imputación
neutral: la mediana deja de ser la de nueve temporadas y pasa a ser la de la ventana
que precede al pliegue, de modo que un hueco de 2024 se rellena con un valor de 2024
y no con uno intermedio entre 2016 y 2024. `imputation_drift` cuantifica esa deriva
sobre los estados realmente ajustados, que es la comprobación empírica que la
decisión pedía.

La cadencia por temporada tiene además una consecuencia de cómputo que merece la pena
explotar. Si las treinta y ocho jornadas de una temporada comparten un estado y la
ventana es creciente, aplicar ese estado pliegue a pliegue vuelve a transformar las
mismas filas que el pliegue anterior y una más: un trabajo cuadrático en una tarea que
no lo es. Como `preprocess.apply` trabaja **fila a fila** con todos sus parámetros
congelados en el estado —los indicadores de faltante salen de `isna()`, y los mapas de
categorías, los valores de imputación, las λ y las medias y desviaciones de escalado
vienen del ajuste—, transformar una ventana y transformar el marco entero para quedarse
luego con esa ventana dan exactamente lo mismo. El protocolo transforma por tanto una
vez por estado y cada pliegue toma sus dos trozos. **Eso no mira hacia adelante**:
transformar de más no es entrenar de más, el estado se sigue ajustando solo con las
filas anteriores al pliegue y qué partidos ve el modelo lo siguen decidiendo las dos
ventanas del pliegue. La batería lo fija por las dos vías —valor a valor en cuatro
pliegues y predicción a predicción en la ejecución completa— porque una equivalencia
afirmada y no comprobada es exactamente el tipo de atajo que invalidaría la medición.
Con cadencia `jornada` el reaprovechamiento no existe, así que ahí no se intenta.

Antes de entrenar nada, cada pliegue pasa tres guardas: que ninguna fila de la
ventana de entrenamiento pertenezca al bloque evaluado, que bajo el régimen semanal
no llegue a la matriz ninguna columna derivada del precio de cierre, y que no
sobreviva ningún valor no finito al preprocesado. Las tres detienen la ejecución
nombrando la columna culpable, porque un protocolo mal especificado no produce
métricas malas —que se detectarían solas— sino métricas buenas.

El orden de esas operaciones es lo que hace válida la cifra, y por eso vive en una
sola función: `fit_fold` comprueba la ventana, congela el preprocesado con datos
anteriores al pliegue, valida la matriz resultante y solo entonces entrena. Devuelve
un `PliegueAjustado` con el modelo y todo el estado con el que se obtuvo —las dos
matrices, el objetivo, el estado congelado y los dos contextos—, de modo que el
recorrido completo y cualquier análisis posterior sobre un pliegue concreto trabajan
sobre el mismo objeto en vez de sobre dos ajustes que se parecen.

La matriz que el pliegue entrega al estimador es deliberadamente estrecha: variables
estandarizadas, sin fecha y sin precio en escala original. Dos familias necesitan algo
más: las que deciden cuántas épocas entrenar reservando el **final temporal** de su
ventana, y para localizarlo necesitan las fechas —confiar en que el marco venga
ordenado sería una suposición silenciosa que un reordenamiento inocente rompería sin
dejar rastro—. El protocolo publica las fechas y la terna de precios por un canal
aparte, `Contexto`, al que un estimador se suscribe declarándolo en su firma: la
suscripción se detecta por inspección, igual que ya se hacía con el peso de recencia,
de modo que el protocolo sigue sin saber qué modelo entrena y los estimadores que no
lo necesitan no se enteran de que existe.

---

## Modelos y evaluación

`scripts/models/estimators.py` declara **cuatro familias**, y que sean cuatro no es
una cuestión de presupuesto sino de diseño experimental: cada una encarna una
hipótesis distinta sobre la forma de la relación entre las variables y el resultado, y
una quinta que compartiese hipótesis con alguna de estas alargaría la tabla sin añadir
una respuesta.

```
  logistica       lineal en el logit. Multinomial con penalización elástica. Es la
                  familia interpretable —sus coeficientes se leen como aportación de
                  cada variable— y la que mejor tolera doscientas variables frente a
                  poco más de dos mil partidos de ventana.
  bosque          promedio de árboles independientes. Lo que reduce el error es
                  promediar modelos que se equivocan en sitios distintos, y con
                  variables correlacionadas, señal débil y ruido alto esa estrategia
                  compite de verdad.
  refuerzo_xgb    suma secuencial de árboles que se corrigen: la hipótesis contraria
                  a la del bosque. Es quien mejor extrae interacciones no lineales
                  entre forma, fatiga, bajas y precio. XGBoost es una dependencia
                  OPCIONAL: si falta, la familia consta como no aplicable en lugar
                  de desaparecer de la tabla.
  red             composición libre de interacciones. La única que compone
                  interacciones de orden alto sin que nadie las declare, y por eso la
                  prueba de si queda estructura que los árboles no capturan. Con la
                  muestra disponible se espera que sobreajuste, y comprobarlo es
                  parte del resultado.
```

Las cuatro comparten la misma firma —una fábrica sin argumentos que devuelve un
estimador nuevo— y se construyen por la misma puerta, `construye(familia, **params)`,
que rechaza todo parámetro no declarado en `ESPACIOS`. `ESPACIOS` es una rejilla por
familia escrita en el módulo y **antes** de medir: un espacio que se retoca después de
ver resultados deja de ser un espacio y pasa a ser un ajuste sobre el conjunto
evaluado, y tenerlo en el control de versiones hace visible cualquier retoque.

### El banco manual

La búsqueda de variables no la conduce un algoritmo, la conduce quien firma el
trabajo. `scripts/models/laboratorio.py` es el banco donde eso ocurre, y su regla se
enuncia en una línea: **el banco mide y quien lee decide**. Un ensayo es un conjunto
de columnas, una familia y una configuración fija; el banco lo recorre con el
protocolo completo y devuelve sus cifras junto a las del apostante de referencia, sin
proponer un ganador ni adoptar nada por su cuenta.

```python
import laboratorio as lb

lb.catalogo()                                    # qué hay disponible, por dominio
base = lb.variables("mercado", "fuerza")         # un conjunto compuesto a mano
e = lb.evalua(base, "logistica", nombre="mercado+fuerza")
lb.ronda(base, ["form_points_ewm_diff", "home_fat_rest_days"], "logistica")
f = lb.evalua(base + ["form_points_ewm_diff"], "logistica", nombre="+forma")
lb.compara(e, f, ordenar="ventaja")              # ordenar no es decidir
lb.historial()                                   # todos los ensayos registrados
```

`ronda` es la pieza que más se usa y la que mejor expresa el criterio. Mide el
conjunto base y luego el base **más cada candidata por separado**, nunca acumulando, y
devuelve una tabla cuya primera fila es el propio base con sus diferencias a cero. No
adopta la mejor ni encadena una segunda vuelta: una `ronda` que iterase adoptando el
máximo sería una selección automática disfrazada, y la cifra publicada quedaría
contaminada por las decenas de comparaciones que la produjeron sin que nadie las
contase. Que la iteración la haga una persona no es una limitación del banco: es lo
que mantiene el número de comparaciones a la vista y anotado.

Lo caro se calcula una vez. Los pliegues y los estados de preprocesado no dependen del
estimador ni de las columnas elegidas, así que se guardan en caché por protocolo y se
reutilizan entre ensayos; `lb.olvida()` la vacía cuando cambia el parquet. Cada ensayo
lleva un **sello** que resume familia, columnas, configuración, protocolo y recorte: dos
ensayos con el mismo sello son el mismo ensayo aunque se les haya puesto otro rótulo, y
por eso registrar el mismo sello dos veces sustituye la fila en lugar de duplicarla.

El registro (`outputs/models/laboratorio/registro.parquet`) es la memoria del estudio
entre sesiones, y es lo que permite responder a la pregunta que gradúa la confianza en
el resultado: cuántos conjuntos se miraron antes de quedarse con el que se cuenta.
Guarda todos los ensayos, no solo los buenos. La configuración está **fija** durante
toda la búsqueda —la declara `laboratorio.HIPER`— y solo se afina al final, sobre el
conjunto ya cerrado y con las rejillas cortas de `laboratorio.ESPACIOS_FINAL`: afinar
mientras se buscan variables mezcla dos decisiones y hace imposible atribuir la mejora
a ninguna de las dos.

La temporada de reserva no se abre desde el banco. `lb.evalua` y `lb.ajusta` levantan
`PermissionError` si se les pide `evaluar="holdout"`, y el mensaje remite a
`build_models.py --holdout`, que es la única puerta y deja sello en disco.

### La búsqueda del conjunto

El banco manual compara conjuntos de uno en uno; con un centenar de candidatas eso deja
de ser practicable, y recorrerlas a mano tampoco es más honesto: son igual de muchas las
comparaciones, solo que sin contarlas. `scripts/models/seleccion.py` automatiza el
recorrido —**búsqueda flotante**: se parte de la semilla, se prueba cada candidata por
separado, se adopta la que más aporta, y una pasada de poda alterna con la de avance
hasta que ninguna de las dos cambia nada— y a cambio impone la condición que lo hace
publicable, que es **elegir y medir en temporadas distintas**.

El avance puro no podría deshacer una adopción temprana, y una variable que entró en el
paso dos puede quedar redundante en el paso nueve, cuando ya han entrado otras que dicen
lo mismo mejor; en una logística eso reparte el coeficiente entre dos columnas casi
colineales, dobla su error típico y ensucia la lectura sin que las cifras lo denuncien.
La semilla de mercado está protegida de la poda, porque la pregunta del trabajo es qué
se le añade al precio, no si el precio sobra.

**La tolerancia es la del criterio que guía, y hubo que medirla.** `TOLERANCIA = 2e-4`
está en unidades de RPS —cuatro quintos del error típico de la diferencia emparejada,
0.00026— y usarla con los otros dos criterios no es un umbral estricto, es no tener
umbral: en la escala del acierto y en la de la ventaja, 2e-4 son **0.02 puntos
porcentuales**, menos de un cuarto de partido sobre mil ciento cuarenta, de modo que la
condición de parada no se dispararía nunca y la voraz correría hasta `MAX_VARS` adoptando
ruido. `_tolerancia.py` repite el experimento para los tres criterios —seis candidatas,
cada una contra la semilla de mercado sobre los mismos partidos, liquidando la unidad
plana al listón de Bet365, remuestreando jornadas completas— y de ahí sale
`seleccion.TOLERANCIAS`, una por criterio:

| criterio | ee de bloque (1140) | ee de bloque (3040) | tolerancia a 0.77× | en lenguaje llano |
|---|---|---|---|---|
| rps | 0.000159 | 0.000133 | 0.000122 | 0.0001 de RPS |
| accuracy | 0.003254 | 0.001940 | 0.002503 | +0.25 pp de acierto |
| ventaja | 0.008873 | 0.005317 | 0.006825 | +0.68 pp de ROI plano |

La razón 0.77 no es nueva: es la que el proyecto ya había elegido para el RPS. Y la
tolerancia **se escala con la raíz del tamaño del bloque** (`seleccion.tolerancia_de`),
para que la constante no caduque cuando la elección se ensancha de 1140 partidos a 3040.
Dos lecturas del mismo cuadro: `relative_perf_diff` sola da 0.000265 de error típico en
RPS y **reproduce** el 0.00026 documentado, así que el método es el mismo; y la razón
`ee_bloque / ee_iid` se queda entre 0.90 y 1.11 en los dieciocho casos, es decir que en
diferencias **emparejadas** no queda correlación intra-jornada que recuperar.

Y la búsqueda **liquida con la unidad plana** (`seleccion.POLITICA_SELECCION =
"favorito"`), que es la definición de ROI del trabajo y la que publica la cadena, y no
con la política de valor del banco. La diferencia no es de gusto: con política de valor
el subconjunto apostado **se mueve con cada columna que entra**, así que dos conjuntos no
se comparan sobre los mismos partidos y la cifra puede mejorar por apostar menos y mejor
elegido, no por predecir mejor, lo que invalida el criterio `ventaja` por una razón de
forma. Con la unidad plana el denominador es el mismo para todos los conjuntos y la
comparación vuelve a ser pareada, que es como `TOLERANCIAS` se midió. El orden del pool
sigue al criterio que guía y no al RPS; con `anchura > 0` ese orden decide qué candidatas
se llegan a mirar.

Y la adopción exige algo más que superar la tolerancia: **la mejora no puede ser de una
sola temporada**. La tolerancia compara medias, y una media sobre tres temporadas la
puede comprar una. A cada candidata que mejora la media se le descuenta su mejor
temporada y se le exige que la mejora siga siendo mejora; la que no sobrevive al
descuento no compite en ese paso. Es exigencia de *elegibilidad* y no de parada —una
candidata con suerte no es motivo para dejar de buscar—, así que el paso sigue con la
siguiente, y la traza anota cuántas quedaron fuera y qué le quedaba a la elegida. El por
qué está medido junto a `CONSISTENCIA_MIN_TEMPORADAS` y en C.9 del cuaderno: la sexta
variable que la voraz quería adoptar pasaba la tolerancia de media y empeoraba dos
temporadas de tres.

`protocol.CORTE_SELECCION` parte el bloque de desarrollo en dos mitades cronológicas. La
voraz recorre variables en la primera (2019/20–2021/22) y solo en ella; la cifra que se
cita sale de la segunda (2022/23–2024/25), medida una sola vez con el conjunto ya
cerrado; y 2025/26 sigue sellada para el final. La razón es que una búsqueda que elige
sobre los mismos partidos sobre los que después informa no publica una estimación de lo
que rendirá el sistema, sino el máximo de un ruido al que se le han dado doscientas
oportunidades de parecer señal. Eso no se arregla midiendo mejor: se arregla midiendo en
otro sitio.

Hay un **cuarto bloque**, y hubo que inventarlo porque las tres puertas de la búsqueda no
bastaron. La voraz adoptó dos variables que pasaron las tres: mejoraban por encima de
la tolerancia, mejoraban en las tres temporadas y no solo de media, y su mejora quedaba
1.39 veces por encima del techo del máximo de cincuenta y nueve sorteos de la nube de
ruido (+0.000351 observado contra +0.000253 de techo, 2.86 desviaciones típicas sobre la
media de la nube). Y aun así **empeoran el modelo en los partidos que no las eligieron**:
+0.000551 de RPS sobre mil novecientos partidos, intervalo [+0.000104, +0.001005], que
excluye el cero. Lo que no se hace con ese resultado es subir la tolerancia hasta que esas
dos caigan —eso sería ajustar el umbral al resultado ya visto—. Lo que se hace es añadir
una puerta que no tiene número que elegir: **lo que se adoptó mirando unos partidos tiene
que seguir siendo ganancia en partidos que no lo eligieron.** La abre
`seleccion.confirma()` sobre **2017/18–2018/19**, que son anteriores al bloque de
selección, de modo que usarlas no mira hacia adelante y la regla 6 queda en pie; el precio,
dicho en voz alta, es que esas dos temporadas dejan de ser una lectura limpia citable.
Ejecutada, la puerta **rechaza**: el conjunto de cinco cuesta +0.000466 de RPS sobre los
760 partidos de confirmación, y `conjunto.json` queda cerrado sobre las **tres variables
de precio** de la semilla, con `propuesto`, `confirmado` y `confirmacion` guardados en el
fichero para que un conjunto revertido y una búsqueda que no encontró nada no se lean
igual desde fuera.

Queda una objeción razonable que la puerta no contesta: que la búsqueda parase pronto por
timidez del umbral y que el conjunto bueno fuese mucho más grande. Se contesta recorriendo
el **banco entero** con la adopción forzada —`tolerancia = -1`, sin la puerta de
consistencia y sin techo de tamaño—, cincuenta y ocho pasos de las tres de la semilla a
las sesenta evaluables, eligiendo solo con 2019/20 y 2020/21 y puntuando cada prefijo
sobre 2021/22, que no participó en elegirlo. **El RPS de fuera es peor que el de la
semilla desnuda en 55 de los 57 pasos**, y los dos únicos que lo mejoran son los de las
dos variables que la voraz había adoptado por su cuenta (0.191888 y 0.191878 contra
0.191987). Dentro, la curva se porta como una búsqueda sana y toca mínimo en nueve
variables (0.191703); fuera, las seis que entran entre la sexta y la novena empeoran
todas. La economía dice lo mismo: el ROI de fuera va de +18.53 % con la semilla a
−10.83 % con veinticinco variables, y la ventaja sobre el listón de Bet365 cruza el cero
en la octava sin recuperarse. No hay ningún tamaño grande que la lectura de fuera
sostenga. El recorrido completo está en **C.9.1** del cuaderno 05, que lo lee de
`outputs/models/seleccion/camino65.csv` y `camino_2021.json`.

Queda entonces la pregunta de qué significa ese resultado nulo, porque admite dos
lecturas que no se parecen en nada —que las variables de juego no midan nada, o que midan
mucho y el precio ya lo haya incorporado— y mientras el precio esté dentro del modelo son
indistinguibles. Se separan quitándoselo, y **está medido**: las **59 variables de juego
sin una sola columna de precio** dan RPS **0.202853** sobre el bloque de selección, entre
la previa de frecuencias (0.223518) y la semilla de mercado (0.193174). Recorren el
**68.1 %** del hueco que las separa —0.020664 de 0.030343— y el acierto, que es una
métrica independiente y mucho más basta, dice **68.3 %**: 6.05 de los 8.86 puntos. **El
banco reconstruye desde cero dos tercios de la información con la que el mercado fija su
precio, sin verlo.** El tercio que falta es el que el proyecto no puede tener —alineación
confirmada, vestuario, y el dinero de los propios apostantes, que es el mecanismo por el
que el precio agrega información privada—, así que ése es el techo de las variables
públicas en este banco.

La misma tabla trae un segundo hallazgo que cambia la lectura de C.9.1: el dominio de
**`fuerza` solo —nueve columnas, Elo y valor de plantilla— llega a 0.198001 y bate a las
59 juntas** (0.202853), el 84.1 % del camino. Ese 68.1 %, por tanto, no es el techo del
banco de juego sino el de un ajuste de 59 variables sobre 1140 partidos: lo que sobra es
varianza. Y las 65 juntas dan **0.200669, es decir 0.0075 peor que las tres del precio
solas**: añadidas a un precio que ya acierta, las columnas de juego no solo no aportan,
ahogan lo que el precio sabía. Por eso la voraz no encontró nada, y no por falta de
señal: cada variable que entra cuesta más varianza de la que trae de señal **nueva**. La
escalera por dominios se ordena además como la ordenaría cualquiera que entienda de
fútbol —`fuerza` 0.841, `forma` 0.679, `clasificacion` 0.525, `historial` 0.317, `fatiga`
0.143, `bajas` 0.011 y `contexto` −0.025, los dos últimos indistinguibles de no saber
nada—, lo que sirve de comprobación de cordura de toda la capa de variables. La tabla
está en **C.9.2** del cuaderno 05 y la produce `_sinprecio.py` en
`outputs/models/seleccion/sinprecio.csv` (`seleccion.SINPRECIO`).

Queda la objeción directa, que es la que cualquiera haría en la defensa: **¿y un conjunto
con el mercado más lo mejor del resto?** Ese conjunto la voraz no podía encontrarlo aunque
existiera, y por dos limitaciones que son del procedimiento y no de los datos. **Una: la
penalización está clavada** en `laboratorio.HIPER` para toda la búsqueda (`C=0.3, L2`), de
modo que k=3 y k=12 se comparan con un castigo de varianza calibrado para k=3; el módulo
declaraba el precio de esa decisión y lo remitía a `afina()`, pero `afina()` solo afina el
conjunto que ya ganó, así que la compensación nunca llegaba a los conjuntos que la
necesitaban. Y la necesitaban: `_ridge.py` mide **0.0061 de RPS entre la mejor y la peor
penalización sobre las mismas 65 columnas** (0.194557 con `C=0.03, L1` contra 0.200669 con
`C=0.3, L2`), recuperados sin tocar una sola variable. **Dos: la voraz es miope** —añade de
una en una—, así que un dominio que ayude entero mientras ninguno de sus miembros ayuda
solo le es invisible, y eso no es hipotético aquí: `fuerza` como bloque bate a las 59 de
juego juntas.

Que esa primera limitación favorecía al conjunto pequeño no es una deducción, está medido
en el propio cuadro: dejar que cada conjunto elija su penalización le recupera **0.005663
de RPS al más grande y solo 0.000006 a la semilla**. La rejilla ayuda a los conjuntos
grandes y a la semilla no le hace nada, que es exactamente la asimetría que inflaba la
ventaja del conjunto pequeño.

Corregidas las dos cosas, la distancia **se estrecha de verdad**: `semilla+fuerza` pasa de
perder 0.001081 a perder 0.000780, luego la comparación original era injusta y ahora no lo
es. Pero el veredicto no cambia, y ahora lo sostienen **cuatro rutas independientes**: la
voraz de una en una (peor que la semilla en 55 de 57 pasos), L1 sobre las 65 dejando que
la penalización seleccione sin sesgo de máximo (0.194557, pierde 0.001383, intervalo que
excluye el cero), los **siete dominios enteros cada uno con su mejor penalización** de
cinco (`BLOQUES`) y la **semilla más las N mejores ordenadas por mérito propio sin el
precio delante** (`MEJORES`, con N = 3, 5, 8, 12, 20 y 30: pierden de 0.000252 a
0.000928). La escalera acumulativa —la pregunta en su forma más literal, el mercado más
los dominios enteros de mejor a peor— es **monótonamente peor**: 0.193949 con `fuerza`,
0.194226 con `forma`, y así hasta **0.194399** con las 62, con intervalos que excluyen el
cero desde `forma` en adelante. No es ruido: es daño.

Dos conjuntos sí batieron a la semilla en el bloque de selección, y **la cuarta puerta
rechaza los dos con el signo invertido**, que es la firma de haber escogido el máximo de
treinta y cinco configuraciones. `semilla+bajas` gana 0.000211 donde se eligió y pierde
0.000312 donde no —y es, no por casualidad, el mismo dominio de `out_minshare_max_diff`,
una de las dos variables que la voraz adoptó y esta misma puerta devolvió—.
`semilla+historial` empata en RPS (0.000057, intervalo que cubre el cero de sobra) y gana
**+0.61 pp de acierto**, que parecen algo hasta que se miden: el error típico del acierto
sobre 1140 partidos es **1.48 pp**, así que esa ganancia son **0.41 errores típicos**, es
decir nada; fuera pierde 0.000586 con intervalo [+0.000075, +0.001115], que ya excluye el
cero. Para que esa puerta pudiera juzgarlos, `confirma()` acepta ahora `hiper`: un conjunto
adoptado bajo `C=0.1, L1` y confirmado bajo `C=0.3, L2` se estaría puntuando con un castigo
de varianza que su elección no tuvo, y el rechazo no distinguiría la falta de señal del
cambio de configuración. La semilla y el conjunto se puntúan siempre con la misma. El
cuadro está en **C.9.3** del cuaderno 05; lo producen `_bloques.py` y `_mejores.py` en
`seleccion.BLOQUES`, `MEJORES` y `MEJORES_ORDEN`.

**Y queda la novena ruta, que es la que sobrevive y la que se publicó.** Las ocho
anteriores preguntan todas lo mismo de la misma manera —qué columnas meter en un
ajuste— y las ocho dan el mismo resultado nulo con la misma firma: el signo se invierte
fuera del bloque que eligió. Si el problema es la varianza del ajuste conjunto, la
salida no es elegir mejor las columnas sino **no ajustarlas juntas**: se ajustan dos
modelos separados —el precio y las 59 de juego— y se combinan sus probabilidades como
una **mezcla logit**, `p ∝ p_mercado^w · p_juego^(1−w)`, renormalizando. Lo que cambia
es el tamaño de lo que se elige: **un solo parámetro** frente a los 2^65 conjuntos entre
los que elegía la voraz, así que apenas hay nada que sobreajustar.

Y es la **única de las nueve cuyo signo no se invierte**. La diferencia emparejada
contra la semilla a `w=0.85`, en los dos bloques que no eligieron el peso: **+0.35 pp de
acierto y +1.12 pp de ROI en `medicion`** (1140 partidos) y **+0.39 pp y +1.51 pp en
`confirmacion`** (760). Dos avisos que van siempre pegados a esas cifras: **ninguna
diferencia de fuera excluye el cero** —que el signo repita no es que la ventaja esté
demostrada, es que no se desmorona, que es lo que hacían las otras ocho—, y **el RPS
empeora en los tres bloques**, con todos los pesos. Eso último es lo que obliga a que
adoptarla sea una decisión y no una lectura, y es la decisión que Manuel tomó al
levantar la regla 4.

El peso se eligió **por acierto**, y ese es el hallazgo de método de toda la sección. La
curva da tres óptimos distintos sobre el bloque de selección: `w_rps = 1.00` —no
mezclar—, `w_acierto = 0.85` y `w_roi = 0.35`. Y los dos pesos se separan **fuera**: la
columna `roi_max` de `mezcla.csv` —liquidación a la mejor cuota entre casas—, promediada
por número de partidos sobre `medicion` más `confirmacion`, da **+2.51 % con w=0.85
contra +1.38 % con w=0.35**; es la misma cifra que validó `_mezcla_control.py`, y por
bloque son +5.31 % y −1.69 %. En la convención de cierre promediado la ordenación es la
misma y los niveles no: la ventaja sobre el listón (`vent`, que es la columna emparejada)
es **+1.08 % con w=0.85 contra −0.05 % con w=0.35**. Las dos lecturas dicen lo mismo, y
hay que citar cuál se está usando porque los niveles no son intercambiables. El ROI
sobre 1140
partidos tiene un error típico de unos **3 pp**, de modo que la diferencia entre dos
pesos es ruido frente a su propia incertidumbre: **seleccionar por ROI es seleccionar
ruido**, aunque el ROI sea la cifra que se quiere maximizar. La regla que queda escrita
es **se selecciona por acierto y se reporta ROI**. La misma mezcla con `fuerza` en lugar
de las 59 —el mejor dominio del banco según C.9.2— **no mejora**: su `w_acierto` es 0.65
y el signo del acierto no repite fuera. El cuadro está en **C.9.4** del cuaderno 05; lo
produce `_mezcla.py` en `seleccion.MEZCLA`, `MEZCLA_PAR` y `MEZCLA_RESUMEN`.

Un aviso para no cruzar dos cifras que no son la misma: el ROI de esta sección es el del
banco —política `valor` con umbral, diferencia **contra la semilla**—, y el que publica
`build_models.py` es política `favorito` y la diferencia va **contra el listón de
Bet365**. No se citan intercambiados.

### La décima ruta: preguntar por el dinero, y partir el censo para poder leerlo

Las nueve rutas anteriores buscan guiadas por el RPS. La **décima** es la primera que
pregunta por el acierto y el dinero, que es lo que pide la directriz del 20-oct-2026, y
va en dos pasadas porque la primera no se puede leer. `_maximo.py` contesta la pregunta
en su forma literal —maximizar acierto y ROI sobre todas las temporadas disponibles— y su
propia tabla lo delata: el campo `temporadas` de `maximo.csv` enumera las seis mismas
sobre las que reporta, así que **elige y mide en los mismos 2280 partidos**. Sus dos
ganadores, 55.00 % de acierto con nueve variables y +4.56 % de ROI con diez, son por eso
**incitables** (regla 5), y el optimismo crece con el número de configuraciones probadas,
que allí fueron miles. Vive en `seleccion.MAXIMO_CRIBA`, `MAXIMO_CURVA`, `MAXIMO_DETALLE`,
`MAXIMO_GANADORES` y `MAXIMO_FUERA`.

`_censo.py` cierra ese hueco de la única forma que hay: **partiendo el censo**. Elige en
`seleccion` (1140 partidos) y mide en `medicion` (1140) y en `confirmacion` (760, con el
arranque abierto a 2016/17 sola, como hace `confirma()`), de modo que ninguno de los 1900
de fuera participó en elegir nada y los 760 son anteriores a todo: la regla 6 queda en
pie. Son 2314 evaluaciones y 107 min, en `seleccion.CENSO_CURVA`, `CENSO_DETALLE`,
`CENSO_GANADORES` y `CENSO_FUERA`.

**Y rechaza, con lo que la décima se suma a las nulas.** Dentro del bloque de selección el
ganador por acierto —siete variables, `C=0.3, L2`— sube de 52.46 % a **54.30 %** y de
+2.20 % a **+9.50 %** de ROI a la mejor cuota: es el **mayor salto medido en el
proyecto**, y es exactamente la clase de cifra que la regla 5 prohíbe citar. Fuera **se
parte en dos**, y las diferencias son las emparejadas **contra la semilla** —columnas
`droimax_semilla` y `dac_semilla` de `censo_fuera.csv`, no contra el listón—: gana en
`medicion` (+1.55 pp de ROI máx, +0.09 pp de acierto) y pierde en `confirmacion`
(−1.82 pp, −1.05 pp), y sobre los 1900 juntos queda en **+0.20 pp de ROI
[−2.71, +3.11]** y **−0.37 pp de acierto**. El signo no repite, que es la firma del máximo
de una nube. Lo que sí es significativo es el daño: **el RPS empeora en las tres ventanas,
con los tres intervalos excluyendo el cero**, de modo que frente a la mezcla publicada el
conjunto de siete es estrictamente peor en las dos cosas a la vez. El cuadro está en
**C.9.5** del cuaderno 05.

### El origen rodante: elegir un procedimiento y no un conjunto

Las diez rutas anteriores eligen con 2019/20–2021/22 y puntúan fuera, de modo que
**dejan tres temporadas sin cifra**. Ampliar el bloque de elección arreglaría la
cobertura y rompería la regla 5, así que cubrir el universo entero —2017/18 a 2026/27 sin
excepción— y no elegir donde se mide solo son compatibles de una forma: eligiendo un
**procedimiento** en vez de un conjunto. Para cada temporada S se repite la búsqueda
**con solo partidos anteriores a S**, y con lo que salga se predice S. El conjunto cambia
con el origen; lo que se fija y se juzga es el procedimiento. Es la **undécima ruta**, y
la produce `_rodante.py` —nueve orígenes, los dos criterios de `CRITERIOS`, 4.2 h— en
`seleccion.RODANTE_ELECCION`, `RODANTE_TEMPORADA`, `RODANTE_RESUMEN` y `RODANTE_PRED`.

Cuatro pistas sobre el mismo censo, y la del listón al lado. Las cifras son las del
**censo limpio** —las seis temporadas que no eligieron el peso de la mezcla publicada,
1978 partidos, que es el único censo donde las cuatro columnas se comparan—:

| pista | acierto | RPS | ROI b365 | ventaja sobre el listón |
|---|---|---|---|---|
| listón (favorito de b365) | 53.54 % | — | −2.78 % | — |
| `semilla` (3 de precio) | 53.44 % | **0.193962** | −2.97 % | −0.19 pp [−1.53, +1.20] |
| **`publicado`** (mezcla, 62) | **53.84 %** | 0.194437 | **−1.60 %** | **+1.18 pp [−0.84, +3.33]** |
| `rodante_ac` (por acierto) | 53.49 % | 0.194118 | −2.64 % | +0.14 pp [−1.64, +1.93] |
| `rodante_roi` (por ventaja) | 53.59 % | 0.194335 | −1.96 % | +0.82 pp [−1.35, +3.01] |

**Ni un intervalo excluye el cero, ninguno de los cuatro**, y sobre los 3118 enteros
tampoco: el mayor es el de la mezcla publicada, +1.52 pp [−0.19, +3.31]. De las **doce
comparaciones emparejadas** entre pistas —seis pares en los dos censos— **solo una
excluye el cero**, `publicado` contra `semilla` sobre los 3118: +1.53 pp [+0.06, +3.03],
y en el censo limpio ya no. Cubiertas las nueve temporadas sin excepción, **ninguna ruta
del proyecto produce una ventaja distinguible de cero contra el favorito de Bet365, y la
mejor de las cuatro sigue siendo la mezcla que ya se publica**. Lo que esta ruta aporta
no es un modelo mejor: es la primera cifra del trabajo que cubre las nueve temporadas sin
que ninguna haya participado en elegir lo que la predice, y por tanto la primera citable
entera.

Tres avisos que van pegados a ese cuadro. La pista de **ventaja** queda por encima de la
de acierto aunque su tolerancia sea casi tres veces más ancha (6.8e-3 contra 2.5e-3), y
lo que cuesta está en el RPS —0.194335 contra 0.194118—, que es lo que la regla 4 obliga
a seguir reportando aunque ya no arbitre. Las **dos pistas rodantes difieren en 91
partidos de 3118**, el 2.9 %, y emparejadas la diferencia es +0.36 pp [−1.09, +1.79]: las
cuatro coinciden en el signo en más del 95 % de los partidos, así que **las cifras
marginales de la tabla exageran las distancias** y la comparación que vale es la
emparejada. Y la celda más bonita es la única incitable: `rodante_ac` en 2026/27 da
+6.18 % de ROI y +8.21 pp de ventaja sobre 78 partidos con un error típico de 10.93 pp,
es decir **0.75 errores típicos de cero**.

Queda un hallazgo lateral, que va contra la directriz que lo produjo y que mide
`_estabilidad.py` en `seleccion.ESTABILIDAD` y `ESTABILIDAD_ORDEN`: **el orden de las
candidatas por RPS es más estable entre orígenes que el orden por acierto** —rho de
Spearman 0.791 de media contra 0.693, y +0.588 contra +0.371 en el par de orígenes más
lejano—. No cambia quién elige, que lo fijó Manuel, pero es el mecanismo por el que una
búsqueda guiada por acierto compra daño en RPS sin anunciarlo. Di sobre qué pares
promedias al citar ese rho: la tabla tiene dos criterios con 21 y 6 pares, el promedio
global de los 27 es 0.791 contra 0.693 y el de los 21 del criterio de acierto 0.782
contra 0.688, y el global no es el de ninguno de los dos.

El cuadro de esta sección está en **C.9.6** del cuaderno 05.

### El máximo por ROI y acierto sobre las nueve temporadas

Las once rutas eligen cada una su conjunto; esta pasada no busca nada, **puntúa**. Con el
censo de nueve ya fijado —2017/18–2024/25 y 2026/27, **3118
partidos en 345 pliegues**— se puede hacer la pregunta en la forma en que está escrita:
qué conjunto maximiza el ROI y el acierto, en el agregado y en cada temporada. Lo hace
`_nueve.py`, que puntúa los **50 conjuntos distintos** que han producido las once rutas
sobre los mismos 3118 partidos, temporada a temporada, con los tres precios y el listón al
lado. Tres lecturas, y la tercera es la única citable.

**El agregado.** El máximo lo firma `maximo_roi` (k=10) con acierto **54.11 %**, ROI
**+1.35 %** y ventaja **+4.22 pp [+1.72, +6.77]** sobre un listón que en los 3118 va a
−2.87 % y 53.11 %. El intervalo excluye el cero, y **es por eso mismo por lo que la cifra
no es citable**: ese conjunto se buscó sobre partidos que están dentro de estos 3118. El
mejor acierto global es de `rod_ve_202425` (k=7) con 54.14 %; el publicado de 62 queda en
53.50 % y **+1.52 pp [−0.19, +3.31]**, y la semilla de precio en 53.08 % y −0.01 pp.

| conjunto | k | acierto | RPS | ROI b365 | ventaja sobre el listón |
|---|---|---|---|---|---|
| `maximo_roi` | 10 | 54.11 % | 0.195439 | +1.35 % | +4.22 pp [+1.72, +6.77] |
| `maximo_acierto` | 9 | 54.07 % | 0.195364 | +1.14 % | +4.01 pp [+1.70, +6.30] |
| `rod_ve_202425` | 7 | **54.14 %** | 0.194208 | +0.86 % | +3.73 pp [+1.92, +5.57] |
| **publicado** | 62 | 53.50 % | 0.194041 | −1.35 % | +1.52 pp [−0.19, +3.31] |
| `semilla` | 3 | 53.08 % | **0.193674** | −2.88 % | −0.01 pp [−1.14, +1.14] |
| `todas65` | 65 | 50.58 % | 0.202736 | −1.21 % | +1.66 pp [−2.40, +5.53] |
| `juego59` | 59 | 50.03 % | 0.204590 | −2.62 % | +0.25 pp [−3.68, +4.11] |

Las dos últimas filas dicen una cosa que conviene no leer mal: **meter el banco entero
cuesta acierto y RPS, y no cuesta ventaja**. `juego59` pierde tres puntos y medio de
acierto contra el publicado y su ventaja sigue saliendo positiva; la peor de las 50 es
`semilla+fuerza`, con solo −0.93 pp. El ROI plano sobre 3118 partidos tiene un error
típico de 1.79 pp y **no distingue un modelo que acierta el 50 % de uno que acierta el
53.5 %**. El daño está medido, pero quien lo mide es el acierto.

**Por temporada no hay respuesta que dar, y eso es el hallazgo.** Las nueve temporadas las
firman **nueve conjuntos distintos, en los dos criterios, y ninguno repite**. Cada ganador
se desmorona fuera de su temporada: `escalera+contexto` da **+16.34 pp** de ventaja en
2019/20 y **+0.21 pp** en las otras ocho; `escalera+clasificacion` gana 2023/24 con 56.05 %
de acierto y promedia 51.10 % fuera, con un puesto medio de **40.6 de 50**. Los puestos
medios de los nueve ganadores van de 12.2 a 40.6. Nueve máximos sobre 380 partidos con un
error típico de 5.0 pp son nueve sorteos: no existe «el conjunto que maximiza cada
temporada», existe el que ganó ahí.

**Dejar una temporada fuera es la versión honesta, y no excluye el cero.** Para cada
temporada S se elige el conjunto que maximiza las otras ocho, sin ver S, y se puntúa en S.
Por acierto salen 4 conjuntos distintos y el agregado da **53.30 % contra 53.11 %** del
listón, ventaja **+1.46 pp [−0.42, +3.34]**; por ventaja salen 3 y el agregado da **53.11 %
contra 53.11 %** —el mismo acierto al decimal— con **+1.45 pp [−0.84, +3.69]**. **Los dos
quedan por debajo del +1.52 pp del modelo publicado**, y los vaivenes por vuelta dicen de
dónde sale la media: guiando por ventaja, **+8.41 pp en 2019/20 contra −4.55 pp en
2026/27**. Las tres lecturas están en **C.9.7** del cuaderno 05.

Un aviso sobre el censo llamado LIMPIO, que son las seis temporadas que no eligieron el
peso de la mezcla: **es censo limpio para el publicado y para la semilla, y no lo es para
ningún conjunto de búsqueda.** `maximo_roi` se buscó sobre 2019/20–2024/25 y pisa tres de
las seis; `rod_ve_202425`, que ahí firma el máximo con +3.61 pp [+1.35, +5.97], se eligió
con el pasado de 2024/25, que contiene cuatro. Ese intervalo excluye el cero y **no es una
cifra de fuera de muestra**.

Y la voraz con los 3118 dentro del bloque de elección —la petición en su forma más literal,
y no citable por la misma razón— **se agota en dos variables sobre la semilla**. Para en la
tolerancia y no en el techo, a **k=5 por los dos criterios**: guiada por acierto, la mejor
candidata del paso 3 (`form_ga_ewm_diff`) mejoraba **+0.000000** contra una tolerancia de
0.00153093; guiada por ventaja, +0.003211 contra 0.00416413. Con el máximo de información
que el proyecto puede darle, la búsqueda no encuentra una tercera variable que pase.

El bloque **no es un parámetro** de estas funciones. `adelante` y `criba` solo saben
medir sobre selección, `mide` solo sobre medición, y pedirles otra cosa levanta
`PermissionError`. Un argumento libre convertiría la única cifra citable del trabajo en
un descuido de una tarde: una ejecución apresurada que seleccionase sobre el bloque de
medición no daría ningún error y devolvería una tabla de aspecto perfectamente normal.

El banco admite **los tres criterios** —`seleccion.CRITERIOS` son `rps`, `accuracy` y
`ventaja`, el primero minimizándose y los otros dos maximizándose— y cuál guía es
decisión de Manuel, no del código: la regla 4 del proyecto puso el RPS a decidir, y al
levantarla el 20-oct-2026 los criterios de búsqueda pasaron a ser el acierto y la
ventaja, que son los dos que recorre `_rodante.CRITERIOS`. Lo que el RPS sigue rigiendo
es todo lo que no es elegir: el piso frente a la previa de frecuencias, la ordenación de
la tabla comparativa y la métrica de permutación con la que se explica.

El precio de esa elección está medido, y **las cifras siguen siendo verdad aunque ya no
sean el argumento que decide**. Repitiendo la búsqueda con cada criterio, forzando la
adopción hasta nueve variables, los tres caminos **no comparten ni una sola variable**, y
puntuados los tres conjuntos de cinco sobre el bloque de selección —la semilla de mercado
está en RPS 0.19317, acierto 52.46 %, ventaja +1.67 %—: guiado por `rps`, 0.19254 /
52.81 % / +12.31 %; guiado por `accuracy`, 0.19337 / **53.51 %** / +0.51 %; guiado por
`ventaja`, 0.19321 / 52.46 % / +6.88 %. Solo el camino del RPS es bueno en las tres. El
del acierto compra 0.70 puntos —ocho partidos de mil ciento cuarenta— a cambio de un RPS
peor que el de la semilla desnuda. Y los dos criterios que no son el RPS tienen mucha
menos resolución para guiar —el cuadro de tolerancias de arriba les mide 0.003254 y
0.008873 de error típico de bloque contra 0.000159 del RPS—: sobre 1140 partidos el
acierto se mueve a saltos de 0.088 puntos y tres de los seis pasos de su camino «mejoran»
en exactamente +0.000000, es decir que adoptan por desempate ciego. Es la razón por la
que `TOLERANCIAS` les exige mucho más que al RPS, y por la que la cuarta puerta sigue
existiendo.

Antes de la voraz hay una **criba**: un ensayo de una sola variable por candidata, que
ordena las candidatas por lo que dicen solas. Con `ANCHURA = 0` —el valor por defecto— la
voraz mira **todas** en cada paso y la criba no recorta nada: es lectura, no recorte.
Recortar a las mejores `anchura` abarataría el recorrido (`anchura × max_vars` protocolos
en vez de `n_candidatas × max_vars`), pero tendría la limitación que conviene decir en voz
alta —una variable inútil por su cuenta pero útil acompañada se quedaría fuera— y el banco
del régimen semanal son 65 candidatas, de las que la voraz prueba 59 por paso descontada
la semilla. Cabe entero, así que no se recorta y el conjunto elegido no depende de dónde
se habría puesto el corte.

```python
import seleccion as sl

pool = sl.candidatas(feat)                       # el banco del régimen
sl.criba(pool, "logistica", feat=feat)           # una pasada de una variable cada vez
sel = sl.flotante(pool, "logistica", feat=feat)  # avance y poda, sobre el bloque de selección
sel.resumen()                                    # qué entró, en qué orden y cuánto aportó
sl.mide(sel.conjunto, sel.familia)               # la cifra citable, una sola vez
```

Cada paso adoptado deja el conjunto en disco (`outputs/models/seleccion/`: la traza en
parquet y el conjunto en json, con su sello, su corte y su motivo de parada), de modo que
un recorrido de horas sobrevive a un reinicio del núcleo y la búsqueda se puede
reconstruir después sin repetirla. La línea de órdenes hace lo mismo:
`python scripts/models/seleccion.py --familia logistica`, y `--medir` refuerza la
separación negándose a publicar si el corte guardado no es el corte vigente.

En el cuaderno 05 la búsqueda ocupa las secciones **C.8** (la criba, con la partición en
bloques explicada y las candidatas ordenadas por lo que dicen solas), **C.9** (la voraz,
con la traza paso a paso y la curva de RPS frente al tamaño del conjunto) y **C.10** (la
medición única sobre el bloque de medición, que es la tabla que va a la memoria). Lo que
sale de ahí no se copia a mano a ningún sitio: si `build_models.CONJUNTO` está vacío, el
orquestador lo recoge de `seleccion.carga()`, comprobando antes que el corte y la familia
guardados son los que rigen en él —y desentendiéndose, con aviso, si no lo son—. Una
lista copiada sería una segunda fuente de verdad, y las dos se separan el día en que se
repite la búsqueda y nadie actualiza la copia.

`scripts/models/evaluate.py` mide. El RPS es la métrica principal porque respeta el
orden natural del dominio; el log-loss y el Brier descompuesto en fiabilidad y
resolución separan dos diagnósticos que suelen confundirse —estar mal calibrado y no
discriminar—; esa descomposición lleva un cuarto término, el residuo, porque la
identidad de Murphy solo tiene tres piezas cuando el pronosticador emite un número
finito de valores distintos, y un modelo continuo mete en cada tramo probabilidades
que solo se parecen: declarar ese resto en vez de repartirlo a ciegas entre los otros
tres es lo que permite comprobar la identidad como igualdad exacta y no como
aproximación de cortesía; y toda cifra se publica además como **habilidad** frente a
cuatro
referencias medidas sobre los mismos partidos: consenso de casas, Pinnacle, previa y
local. El análisis de **desacuerdo** cierra el bloque agrupando los partidos por la
distancia entre la terna del modelo y la del mercado: si el modelo solo gana donde
coincide con el precio, no aporta nada.

`scripts/models/backtest.py` traduce las probabilidades a dinero. Ofrece dos políticas
—`valor`, que apuesta solo donde la probabilidad del modelo supera a la implícita en la
cuota por un margen declarado, y `favorito`, una unidad plana al signo que el modelo
señala en todos los partidos con precio—, dimensiona con importe plano o con Kelly
fraccionado y topado, calcula el
tamaño una vez por jornada porque las apuestas de un fin de semana son simultáneas,
y publica la **caída máxima** junto al retorno, porque un retorno positivo con una
caída del cuarenta por ciento no es una estrategia. El margen del conjunto de cuotas
se reporta siempre, porque es el listón exacto que hay que superar. Dos advertencias
acompañan a estas cifras en la memoria: el retorno es la métrica más ruidosa de todo
el trabajo, y el rendimiento histórico se calcula contra cuotas de **cierre** mientras
que una apuesta emitida antes de la jornada iría contra las de apertura, por lo que se
reportan ambas lecturas.

**La que se publica es `favorito`**, declarada en `build_models.POLITICA_ECONOMIA`, y la
elección está medida. Enfrentadas las dos sobre el modelo publicado, a cuota de cierre
promediada, cada cifra con su listón de Bet365 al lado (regla 3): en el bloque que eligió
el peso, `favorito` rinde −0.35 % contra −2.18 % del listón —ventaja **+1.83 %**— y
`valor` rinde −7.93 % contra el mismo listón —**−5.75 %**—; fuera de ese bloque,
`favorito` da +1.93 % contra +2.09 % —**−0.16 %**— y `valor` +0.25 % —**−1.84 %**—.
`favorito` gana en las dos ventanas, y con umbral 0.00 y 0.06 el orden no cambia, así que
la lectura no depende de haber acertado con el margen. Lo que sostiene la elección es esa
estabilidad del orden al cambiar de ventana, **no la significación**: la diferencia entre
las dos políticas no se ha acotado. Pesa además que `favorito` no tiene ningún parámetro
que elegir, y que es el ROI con el que se midió y se adoptó la mezcla.

Esto **corrige una creencia heredada del propio proyecto**, que daba por supuesto que
apostar al favorito compra el margen de la casa una y otra vez y que por eso había que
seleccionar por valor. Lo primero es cierto y lo segundo no se sigue. El umbral de valor
retiene las apuestas donde el modelo más se aparta del precio, y cuando la ventaja del
modelo sobre el precio es del tamaño que este trabajo mide —compatible con cero—, esas
son también las apuestas donde el modelo tiene más probabilidad de estar equivocado: el
filtro concentra el importe en el desacuerdo, que aquí no es señal. Cuesta además la
mitad de las apuestas —576 de 1140 fuera del bloque—, y eso se ve en la anchura del
intervalo: [−15.67, +11.55] frente a [−2.78, +2.28].

Ninguna de esas cifras se publica sola. El módulo simula además un **apostante de
referencia** que sigue a Bet365 y respalda siempre su cuota más baja —el favorito de
la casa— sobre los mismos partidos, con el mismo importe y a los mismos precios, de
modo que la distancia entre los dos solo puede venir del pronóstico y no del acceso a
un precio mejor. La comparación se corta por temporada y por jornada, y la diferencia
de rendimiento viaja con un intervalo por remuestreo **pareado** de jornadas
completas: cada réplica vuelve a simular las dos estrategias sobre las mismas
jornadas, porque remuestrear cada una por su lado sumaría dos varianzas que en
realidad están correlacionadas —un fin de semana de resultados sorprendentes castiga a
los dos contendientes a la vez— y produciría un intervalo demasiado ancho. Esta
comparación es la que responde a la pregunta que motiva el trabajo, que no es si el
modelo gana dinero sino si pronostica mejor que quien pone el precio, y por eso se
recalcula también en cada evaluación automática semanal.

---

## Interpretabilidad

`scripts/models/explain.py` responde a dos preguntas que no son la misma: qué mira el
modelo en general y por qué dice lo que dice en un partido concreto. La unidad de
análisis no es la columna sino el **bloque**, porque con doscientas variables
fuertemente correlacionadas —tres formas del mismo Elo, cuatro lecturas del mismo
precio— cualquier ranking columna a columna reparte una misma señal entre variables
que se sustituyen unas a otras y ninguna sale importante. Los diecisiete bloques
analíticos que declara `lineage.py` se agrupan en ocho bloques explicativos —mercado,
fuerza, forma, historial, clasificación, fatiga, bajas y contexto—, que son las
categorías con las que se habla de un partido de fútbol. Dos de esas agrupaciones
juntan deliberadamente lo que el pipeline separa: el desgaste del calendario absorbe
también la rotación del once, porque la carga y el reparto de la carga son la misma
idea vista desde dos sitios, y las bajas absorben las dos formas de medir la
importancia del ausente, su precio y sus minutos. La correspondencia se define
una sola vez y es exhaustiva: un bloque analítico sin destino detiene la ejecución, de
modo que una familia de variables nueva no puede quedarse fuera del relato en
silencio. Los indicadores de faltante heredan el bloque de la variable cuya ausencia
declaran, para que «no hay parte médico» y «el parte médico dice esto» se lean en la
misma barra.

La lectura global se cuenta por **tres vías**, deliberadamente distintas:

```
  coeficientes  el peso lineal de cada variable en el modelo logístico, agregado por
                bloque. Es exacto y es lo único que da signo, pero solo describe al
                modelo que es lineal.
  permutación   cuánto empeora el RPS al sustituir un bloque entero por el de otro
                partido. Mide de lo que el modelo no puede prescindir, y se calcula
                sobre el bloque de validación, nunca sobre la ventana de ajuste.
  Shapley       el reparto exacto de la diferencia entre la probabilidad emitida y
                la de un partido de referencia. Es el único que vale a la vez para
                el conjunto y para un partido suelto.
```

Que las tres coincidan permite afirmar la conclusión sin matizar; que discrepen no es
un fallo del método sino información sobre el modelo, y por eso `agreement` publica la
correlación de rangos entre ellas en lugar de elegir una. El caso instructivo es el
mercado: encabeza coeficientes y Shapley y cae en permutación cuando el Elo y el valor
de plantilla pueden reconstruir buena parte de lo que el precio dice, porque quitarle
al modelo algo que sabe por otra vía no le duele.

El reparto de Shapley se calcula por **enumeración exacta de coaliciones** sobre los
ocho bloques —doscientas cincuenta y seis subconjuntos, evaluados en bloque sobre toda
la matriz—, no por muestreo: con esta cardinalidad el cálculo exacto es más barato que
la aproximación y evita tener que discutir el error de estimación en la memoria. La
función de valor se define sobre la escala de **probabilidad** y no sobre el logit, de
manera que la eficiencia se cumple en puntos porcentuales y la suma de las
aportaciones de los ocho bloques reconstruye exactamente la diferencia con la
referencia; `verifica_eficiencia` lo comprueba antes de publicar nada. La referencia es
la mediana columna a columna de la ventana de entrenamiento del pliegue, es decir «el
partido corriente», y la lectura correcta de cada cifra es siempre relativa a ella: un
bloque con aportación nula no es un bloque que no importe, es un bloque que en ese
partido no dice nada distinto de lo habitual.

La permutación sustituye el bloque completo por el de una fila tomada al azar de la
**ventana de entrenamiento de ese mismo pliegue**, no baraja dentro del conjunto
evaluado. Son dos decisiones con motivo: el conjunto evaluado de un pliegue es una
jornada —una decena de partidos—, donde barajar desplaza la asociación en vez de
romperla; y la ventana de entrenamiento comparte el estado de preprocesado del
pliegue, de modo que el valor sustituido procede exactamente de la misma distribución
transformada. Sustituir el bloque entero de una sola fila, y no columna a columna,
preserva además la coherencia interna del bloque: una fila con el Elo de un equipo y la
forma de otro es un partido que no existe y el modelo respondería a un imposible.

Lo que se interpreta es, por construcción, lo mismo que se evaluó. `protocol.fit_fold`
aísla el ajuste de un pliegue —comprobar la ventana, congelar el preprocesado, validar
la matriz, pesar y entrenar— y devuelve un `PliegueAjustado` con el modelo y todo el
estado con el que se obtuvo. `run_protocol` lo usa para producir sus predicciones y el
módulo de interpretabilidad recibe esos mismos objetos, de manera que ninguna cifra
explicada procede de un reajuste parecido al que dio la métrica.

Para un partido concreto, `explain_match` publica la tabla de aportaciones por bloque
en puntos de probabilidad y `narrate` la convierte en una frase con los bloques
dominantes, que es la forma en que el resultado llega a la aplicación. Un bloque cuya
aportación no llega al umbral no se menciona: una explicación que enumera ocho causas
de las que seis son ruido no explica nada.

---

## Ejecución, decisión y publicación

Los módulos anteriores son bibliotecas. Quien los encadena es
`scripts/models/build_models.py`, y lo que produce no es un modelo sino una
**decisión documentada**: qué modelo se publica, con qué cifra, medido contra qué
referencias y con qué límites declarados. La división del trabajo con el banco es
estricta: **el cuaderno busca y el script cierra**. Aquí no se prueba nada. El script
recibe el conjunto ya elegido —variables, familia e hiperparámetros—, comprueba que
sus columnas existen en la matriz, lo mide con el protocolo completo en los dos
regímenes, lo explica, simula la economía y escribe la evidencia bajo
`outputs/models/<régimen>/` junto a un manifiesto que declara qué se cerró.

**Lo que recibe, dicho con nombre y apellidos.** El modelo publicado es la **mezcla
logit** de `estimators.mezcla`: dos ajustes separados —uno sobre las tres columnas de
precio, otro sobre las 59 de juego— combinados como `p ∝ p_mercado^0.85 · p_juego^0.15`
y renormalizados. Son las **62 columnas** que declara `build_models.CONJUNTO`, con su
procedencia estampada en el manifiesto (`C.9.4 · mezcla logit w=0.85 sobre las 62`), y
la política económica publicada es `POLITICA_ECONOMIA = "favorito"`.

**Son 62 pedidas y el modelo ajusta 61**, y por eso dos artefactos publicados dan
números distintos sin que ninguno esté mal: `conjunto_declarado` tiene 62 filas mientras
`importancia_bloques` y `registro_bloques` suman 61. Las tres probabilidades del modelo
de goles suman uno, de modo que la poda por colinealidad del preprocesado retira una de
las tres **en cada pliegue**; medido sobre los 255 pliegues de los dos regímenes, cae
`strength_p_home` en 208 y `strength_p_away` en los 47 restantes. **Las tres se declaran
a propósito**: quitar del conjunto la que cae más veces dejaría 60 columnas en los 47
pliegues donde la poda elige otra, porque la elección es del pliegue y no del conjunto.
Y por eso la columna `presente` se mide contra la **matriz de features** y no contra la
matriz ya preprocesada: dice si la variable existe para que el pliegue pueda elegir, no
si sobrevivió a la poda de un pliegue concreto. Que faltase de la matriz de features sí
sería una avería, y es lo que esa columna vigila.

Esa elección **levanta la regla 4 del proyecto**, que decía que cuando el ROI y el RPS
discrepan manda el RPS. La levantó **Manuel el 19-oct-2026** —«quiero que el ROI mande,
no el RPS»—, y el alcance es **la elección del modelo publicado y nada más**: el RPS
sigue rigiendo el piso frente a la previa de frecuencias, la ordenación de la tabla
comparativa y la métrica de la importancia por permutación, que es lo que hace
`METRICA_ELECCION = "rps"`. No es una decisión tomada en el código, y el motivo de que
haga falta levantarla es que **la mezcla cuesta RPS**: 0.000527 peor que el modelo de
solo precio al que sustituye, a cambio de +0.39 pp de acierto y de un ROI plano que
cambia de signo. Ese precio va declarado en `LIMITACIONES` y por tanto en la ficha
publicada, no omitido.

Por qué la mezcla y no otra cosa: de **once rutas medidas** buscando batir al precio,
**diez son nulas**. Ocho lo son con el mismo patrón —el signo se invierte fuera del
bloque que las eligió—; la décima repite ese patrón partido en dos, porque gana en
`medicion` y pierde en `confirmacion` y sobre los 1900 juntos pierde acierto; y la
undécima, que es la que cubre las nueve temporadas, falla de otra forma: **ni uno de sus
intervalos excluye el cero**, y sus dos pistas quedan por debajo de la mezcla. La mezcla
es la única cuyo signo no se invierte, y el motivo es estructural: tiene
**un solo parámetro** frente a los 2^65 conjuntos entre los que elegía la voraz, de modo
que apenas hay nada que sobreajustar. El peso se eligió **por acierto y no por ROI**, y
eso también está medido: elegirlo por ROI sobre el bloque de selección da w=0.35 y por
acierto da w=0.85, y fuera gana el segundo —**+2.51 % contra +1.38 %** a la mejor cuota
entre casas, **+1.08 % contra −0.05 %** de ventaja sobre el listón a cierre promediado—.
El ROI sobre 1140 partidos tiene un error típico de unos 3 pp, así que **seleccionar con
él es seleccionar ruido**: se selecciona por acierto y se reporta ROI. Las dos
convenciones de liquidación están nombradas a propósito; el detalle está en la sección
de la novena ruta.

Que la búsqueda no viva aquí es una decisión de método. Un orquestador que eligiera
variables mientras publica cifras haría imposible saber cuántas combinaciones se
miraron antes de quedarse con la que se cuenta, y ese número es justamente lo que
gradúa la confianza que merece el resultado. El registro del banco guarda todos los
ensayos; el manifiesto de aquí solo declara cuál de ellos se cerró.

No hay, por tanto, elección entre candidatos que arbitrar. Lo que sí se comprueba, y
antes de mirar ninguna cifra, es que el modelo aporte algo: su RPS tiene que quedar
por debajo del de una previa que reparte siempre la frecuencia observada de las tres
clases **en los propios partidos evaluados**. Es la previa más favorable posible
—conoce el reparto que un pronosticador real tendría que estimar— y por eso batirla es
una condición mínima y no un adorno: un modelo que no la bate no ha aprendido nada del
partido, solo del campeonato.

Cerrada la decisión, y solo después, el script puntúa la **temporada en curso** jornada
a jornada con el modelo ya cerrado: es la tercera cifra que el trabajo necesita, junto
a la agregada y la de temporada a temporada. Su resultado no vuelve sobre la
comparación de familias ni sobre el piso frente al mercado —todo lo que elige algo se
ha medido ya, y solo sobre validación—, sino que alimenta la parte **descriptiva**, que
es la que lee la aplicación: economía por temporada y por jornada, fiabilidad, confusión
y registro de apuesta a apuesta, con la columna `bloque` distinguiendo las dos
poblaciones según la regla de las dos mitades que se explica en el apartado del
protocolo. Las predicciones se escriben aparte, en `predicciones_produccion.parquet`, y
el manifiesto declara en `en_curso` qué temporadas, cuántas jornadas, cuántos partidos
se puntuaron y qué rendimiento llevan contra el listón de Bet365. A diferencia del
holdout no lleva sello de pasada única: se reejecuta cada semana por definición.

El holdout 2025/26 se puntúa **exactamente una vez**, y esa propiedad no se confía a
la disciplina de quien ejecuta: al puntuarlo se escribe `holdout_sello.json` con el
recuento de partidos, las temporadas, las métricas y el código de ejecución, y una
segunda puntuación que no case con el sello se detecta al auditar. El código de
ejecución —una huella de doce dígitos derivada de la configuración del protocolo—
viaja estampado en cada artefacto, de modo que una web que mezclase tablas de dos
pasadas distintas fallaría en voz alta en lugar de mostrar una comparación imposible.

`scripts/models/qa_models.py` es la puerta entre medir y publicar. Impone **nueve
invariantes duros** sobre lo escrito —el quinto lleva dos mitades, el sello y la
separación del bloque en curso, porque la segunda es consecuencia de la primera: todo
lo que viene después de la temporada sellada se entrena con ella dentro y por eso no
puede decidir nada—: que toda terna publicada sume uno y viva en `[0,1]`;
que la jornada emitida sea futura y no contenga ningún partido ya disputado; que el
orden de columnas de la matriz de inferencia sea **idéntico**, y no solo equivalente
como conjunto, al de la matriz de entrenamiento; que bajo el régimen semanal no se
haya colado ninguna columna posterior al saque; que el sello del holdout concuerde con
la tabla que dice describir; que la temporada en curso se publique en su propio
artefacto, sea posterior al holdout y no comparta ninguna temporada con validación ni
con el holdout, porque una cifra que no ha podido entrar en ninguna decisión no puede
aparecer donde se decide; que todos los contendientes de una comparación se hayan
medido sobre el mismo número de partidos; que el conjunto que el manifiesto declara
—familia, valores y variables— sea el que produjo las cifras y esté marcado como
cerrado, porque quien emite la próxima jornada ajusta leyendo de ahí y no repitiendo
el recorrido; que ningún rendimiento económico se publique sin el del apostante de
referencia al lado —ni el del conjunto ni el de la temporada en curso, que es el que
más tienta a publicarse solo—, con su casa declarada, con la diferencia siendo
exactamente la resta de los dos y con la serie de validación por jornada terminando
donde el manifiesto dice; y que
todos los artefactos lleven el mismo código de ejecución. El tercero
es el que justifica el módulo entero: un reordenamiento
de columnas no produce ningún error —el estimador aplica cada coeficiente a la columna
equivocada y devuelve probabilidades perfectamente plausibles—, de modo que sin esta
comprobación el fallo solo sería visible en la degradación silenciosa de una métrica
que nadie vuelve a mirar.

`scripts/models/publish_web.py` proyecta la evidencia al contrato de salidas de la
aplicación bajo `outputs/web/`. La separación con el módulo anterior responde a una
regla: **quien mide no publica y quien publica no mide**. La capa de proyección no
puede reelegir modelo ni recalcular una métrica; solo lee lo decidido, deriva
magnitudes que son función determinista de una terna —clase, confianza, entropía
normalizada por `log 3`, margen entre las dos clases más probables— y **emite** la
jornada por jugar, que es lo único que no existía antes.

La emisión ajusta el modelo publicado sobre todos los partidos ya disputados, que en
ese momento son pasado legítimo, pero con el estado de preprocesado **congelado** que
escribió `build_preprocess.py` para el régimen semanal: el mismo con el que
`build_predict.py` construyó la matriz de inferencia. Ajustar con otro estado, aunque
fuese mejor, produciría una predicción cuyo rendimiento histórico no es el medido. La
paridad de esquema se comprueba **antes** de puntuar y no después, porque un error
detectado después ya se habría publicado.

Tres decisiones de la capa de publicación merecen constar. La tabla de la próxima
jornada lleva dos columnas que suelen confundirse y aquí se publican por separado: la
**clase predicha**, que responde a qué va a pasar, y la **selección por valor**, que
responde a qué merecería la pena apostar y es el máximo de `p·c − 1`, no el máximo de
`p`. Un partido sin ningún precio publicado no hereda selección: se deja vacía, porque
el argumento máximo de una fila de ausentes apuntaría siempre a la victoria local y se
leería como una recomendación. Y cada predicción viaja con su **cobertura por bloque**,
de modo que la ficha de un partido puede decir que se emitió sin parte médico en lugar
de presentar una probabilidad indistinguible de las demás: un modelo que imputa un
bloque ausente sigue devolviendo un número, y sin ese indicador ese número parecería
tan informado como cualquier otro.

Los ejes del radar se normalizan con rangos **declarados en el código y versionados**,
no contra el mínimo y el máximo observados. Escalar contra lo observado es lo cómodo y
tiene una consecuencia que ningún aviso corrige: el mismo equipo con el mismo
rendimiento dibujaría una forma distinta según la temporada que se estuviese mostrando,
y dos gráficos históricos dejarían de ser comparables sin que nada lo advirtiese.
Cambiar un rango obliga a subir `VERSION_NORMALIZACION`, y la versión viaja dentro de
cada fila del artefacto. Los ejes en los que menos es mejor —posición en la tabla, valor
de la plantilla ausente— se marcan como invertidos para que el radar se lea siempre
igual: más área, mejor situación.

La ficha del modelo publica las limitaciones en el mismo documento y con la misma
jerarquía que las métricas. Es una decisión expositiva con un motivo concreto: una web
que muestra un retorno del ocho por ciento sin decir que su intervalo cruza el cero no
está informando.

### Lo que mide el modelo publicado, y lo que no

Cifras del régimen **semanal** —el que publica—, bloque de validación
**2017/18–2024/25**, ocho temporadas: **3037 partidos** puntuados de 3040 —la
intersección de los que cubren a la vez las tres casas de referencia—. RPS **0.194494**
con intervalo [0.18977, 0.19923] y error típico 0.002523, log-loss **0.977547**, acierto
**53.38 %**. Frente a la previa de frecuencias (0.225345 y 44.78 %) la distancia es
amplia, y ese es el piso que el script comprueba antes de mirar nada más.

**Frente a los precios la comparación se reparte, y no es un ranking.** En RPS el modelo
**queda por detrás de los tres**: Pinnacle 0.193958 le gana por 0.000536, el consenso
0.194116 por 0.000378 y Bet365 0.194237 por 0.000257. En log-loss, lo mismo: 0.977547
contra 0.975511, 0.976058 y 0.976742. Lo que el modelo gana es el **acierto**, y a los
tres: **53.38 %** contra 53.11 %, 53.08 % y 53.01 %. La tabla publicada coloca al modelo
en la primera fila **porque ordena los candidatos antes que las referencias**, no porque
gane. Ese reparto es el precio declarado del criterio de adopción —la familia se eligió
por acierto y por dinero, no por RPS— y aquí se ve entero.

**El daño en RPS está localizado.** Contra el consenso, el modelo pierde **0.002142 en
2017/18** y **0.002322 en 2018/19**, y empata o gana en las seis restantes. Son
exactamente las dos temporadas sin cuota de cierre, las de precio más pobre de la serie;
**y en las dos el modelo gana en acierto** (55.53 % contra 54.74 % y 48.95 % contra
48.68 %). Lo que ocurre ahí no es que falle más: reparte peor la probabilidad entre los
tres signos.

En dinero, con la política publicada, **son dos niveles con signos opuestos y no el mismo
con otro precio**:

| eje del precio | n | cobertura | modelo | listón | diferencia | caída máx (listón) |
|---|---|---|---|---|---|---|
| cuota de cierre media | 2280 | 0.75 | **+0.79 %** | −0.04 % | +0.84 pp [−1.17, +3.08] | 37.60 % (41.84 %) |
| consenso del mercado | 3040 | 1.00 | **−0.35 %** | −1.74 % | +1.39 pp [−0.43, +3.33] | **72.61 % (98.51 %)** |

**Ninguno de los dos intervalos excluye el cero.** Las dos filas no son la misma
medición: la de consenso incluye 2017/18 y 2018/19 y la de cierre no puede incluirlas,
porque football-data no publica cuota de cierre antes de 2019/20. **Al entrar esas dos
temporadas el nivel cruza a negativo y la ventaja crece**, lo que suena contradictorio y
no lo es: 2018/19 es un año en el que apostar al favorito perdió **13.03 %** y el modelo
perdió **10.53 %**. Ganarle dos puntos y medio a quien pierde trece sigue siendo perder
diez. Y la caída máxima a consenso —72.61 % el modelo, **98.51 % el listón**— es la
columna que convierte un −0.35 % en una serie inasumible.

**Y aquí va la lectura que no puede faltar nunca al lado de la cifra agregada.** La
ventaja sale de las tres temporadas que eligieron el peso: a cuota de cierre, la
diferencia frente al listón es **+1.83 pp en 2019/20–2021/22 —las que lo eligieron— y
−0.16 pp en 2022/23–2024/25, las que no**. Fuera del bloque que eligió el peso **el
modelo empata con el listón, no lo bate**, y las dos temporadas de `confirmacion` que el
censo de nueve añade no lo cambian: agregando por beneficio sobre importe, fuera del
bloque son **−0.16 pp sobre 1140 partidos** a cuota de cierre (0.05 errores típicos de
cero) y **+1.00 pp sobre 1900** a consenso (0.44 errores típicos). Citar el +0.84 % sin
esta frase al lado es contar media cosa.

**Los dos regímenes dan la misma cifra hasta el cuarto decimal, y eso es correcto por
construcción.** El conjunto se declaró desde el banco **semanal**, así que ninguna de
las cinco columnas que solo existen en `retrospectivo` —`close_move_fav`,
`flag_no_closing_odds`, `sharp_div_*`— entra en él; y de las 61 columnas declaradas
presentes en las dos matrices de preprocesado, las 61 son **idénticas bit a bit**. Si
alguna vez entra al conjunto una columna de movimiento de línea, el techo retrospectivo
deja de colapsar sobre el semanal: entonces hay que **volver a medirlo, no volver a
afirmarlo**.

---

## Orden de ejecución

**Reconstrucción completa.** Solo es necesaria si se parte de cero o si cambia la
definición de alguna variable. Los extractores hacen peticiones en vivo y se ejecutan
siempre en la máquina de desarrollo; el resto es cálculo puro.

```bash
python scripts/extractors/footballdata.py   backfill && python scripts/extractors/footballdata.py   process
python scripts/extractors/understat.py      backfill && python scripts/extractors/understat.py      process
python scripts/extractors/sofascore.py      backfill && python scripts/extractors/sofascore.py      process
python scripts/extractors/clubelo.py        backfill && python scripts/extractors/clubelo.py        process
python scripts/extractors/transfermarkt.py  backfill && python scripts/extractors/transfermarkt.py  process
python scripts/extractors/fatigue.py        backfill && python scripts/extractors/fatigue.py        process
python scripts/extractors/availability.py   backfill && python scripts/extractors/availability.py   process
python scripts/extractors/lineups.py        backfill && python scripts/extractors/lineups.py        process
python scripts/extractors/understat_shots.py backfill && python scripts/extractors/understat_shots.py process

python scripts/consolidate/build_master.py
python scripts/consolidate/qa_master.py
python scripts/features/build_features.py
python scripts/features/qa_features.py
python scripts/features/build_preprocess.py

python scripts/models/seleccion.py                     # la búsqueda, una vez por trabajo
python scripts/models/build_models.py                  # evidencia y decisión
python scripts/models/qa_models.py                     # puerta entre medir y publicar
python scripts/models/publish_web.py                   # contrato de la aplicación
```

`seleccion.py` está en la lista pero no en la rutina: se ejecuta cuando se elige el
conjunto de variables, no cada vez que se actualiza la jornada. Su resultado —el
conjunto y su sello, bajo `outputs/models/seleccion/`— es lo que `build_models.py`
consume después, de modo que la búsqueda ocurre una vez y la publicación semanal se
limita a reevaluar lo ya elegido.

Los cinco módulos de modelado —`protocol.py`, `estimators.py`, `evaluate.py`,
`backtest.py` y `explain.py`— son bibliotecas sin interfaz de línea de órdenes: se
invocan desde `build_models.py` y devuelven en memoria las predicciones fuera de
muestra, sus métricas, la simulación económica y las explicaciones. `laboratorio.py`
es de la misma clase, pero su destinatario es el cuaderno y no el orquestador: es el
banco donde se prueban conjuntos a mano, y su salida —el registro de ensayos— no es
evidencia publicable sino la memoria del estudio. El notebook de modelado es, por
tanto, de dos naturalezas: **busca** con el banco y **exhibe** el mecanismo del resto
—reconstruir los pliegues, verificar que las aportaciones suman lo que dicen
explicar—, y lee de disco todo lo que sea una cifra publicada, porque una cifra
recalculada en el cuaderno podría no ser la misma que auditó el control. Los tres
últimos
comandos son los que sí escriben: `build_models.py` produce la evidencia y la decisión
bajo `outputs/models/`, `qa_models.py` la audita y `publish_web.py` la proyecta al
contrato de la aplicación bajo `outputs/web/`. El orden no es negociable, porque la
proyección no vuelve a decidir nada y por tanto no puede ejecutarse sobre una evidencia
que no ha pasado la auditoría.

De las nueve reconstrucciones, dos son de otro orden de coste: `lineups.py`, que pide
la alineación de cada partido de cada equipo en todas las competiciones que disputa, y
`understat_shots.py`, que pide la lista de remates partido a partido. Las dos se
ejecutan una sola vez; a partir de ahí la actualización semanal solo pide lo que falta.
Son también los dos únicos bloques **opcionales** de la cadena: mientras no se
hayan volcado, `build_master.py` construye el maestro igual, sus columnas llegan
enteras a nulo y la poda por constancia del preprocesado las retira antes de imputar
nada, de modo que el modelo se ajusta sin ellas en lugar de detenerse.

**Actualización semanal.** Lo que cada extractor descarga lo decide **el atraso**, no
una ventana de días: compara lo que ya hay en disco —maestro y sentinela de
consultados— con el universo de partidos jugados y pide la diferencia. En una semana
corriente esa diferencia son los diez partidos de la jornada; tras un parón del
proyecto, los que falten, y la cadena se recupera sola sin pasada especial. La
alternativa, recortar la petición a los últimos *N* días, cubre la semana solo si la
pasada anterior se ejecutó: si no, deja un hueco permanente **y no da error**, porque
el extractor termina anunciando que todo fue bien. Las bajas son el caso aparte dentro
de esa regla: un partido **por jugar** nunca se considera hecho, porque su parte médica
cambia en los dos sentidos hasta el saque —aparecen lesionados y se recuperan— y es
justo la que alimenta la predicción; se vuelve a pedir en cada pasada y su foto nueva
**pisa** la anterior en lugar de sumarse a ella, que es lo que dejaría contando como
baja al que ya está disponible.

El orden importa: fatiga, disponibilidad y alineaciones se
apoyan en el calendario que publica Sofascore, y el maestro no debe regenerarse con
fuentes a medio actualizar. Ese orden está escrito en `scripts/rutina_semanal.py`, que
es la forma normal de ejecutarla:

```bash
python scripts/rutina_semanal.py              # la cadena entera, en orden
python scripts/rutina_semanal.py --listar     # enseña el plan sin ejecutar nada
python scripts/rutina_semanal.py --desde master   # retoma tras arreglar un fallo
```

No calcula nada: lanza los mismos scripts, en el mismo orden, y se detiene en el
primero que falle, porque seguir dejaría en disco una mezcla de artefactos de dos
semanas y ninguno de los pasos siguientes tiene forma de notarlo —un parquet viejo se
lee igual de bien que uno nuevo—. Antes de empezar cuenta las variables con las que se
va a emitir, para que la falta de conjunto se sepa al principio y no después de nueve
extractores. Admite además `--sin-extraccion` (recalcular sobre lo que ya hay),
`--sin-opcionales` (sin alineaciones ni remates, los dos bloques caros) y `--dry`, que
implica lo primero: los extractores no admiten esa opción, y sin la implicación `--dry`
sería una promesa falsa.

**El sondeo va primero.** El paso 0 de la rutina es `extractors/sonda.py`, que es la
única pieza que mira las ocho puertas de red *en conjunto* antes de pedirles nada. La
cadena se detiene en el primer fallo, así que sin el sondeo una caída de Sofascore deja
sin saber si las otras siete también están caídas, y el diagnóstico se hace a mano,
fuente por fuente. La sonda clasifica cada una en `OK`, `DEGRADADA` o `CAIDA` por la
**forma** de la respuesta y no solo por el código HTTP —el fallo peligroso es un 200 con
las claves renombradas—, dice por qué vía llegó el dato, deja el parte en
`outputs/sonda/fuentes.json` y una línea por ejecución en `outputs/sonda/historico.jsonl`,
que es lo que responde a si esto se rompió hoy o lleva un mes roto. Solo detiene la
jornada si cae una fuente **sin sustituto** (calendario, resultado, cuotas): que fallen
ClubElo o Transfermarkt degrada columnas, se avisa y se sigue. Con `--exigente` aborta
también con fuentes degradadas y con `--permisivo` nunca aborta.

```bash
python scripts/extractors/sonda.py                     # las ocho puertas, ~35 s
python scripts/extractors/sonda.py --solo clubelo      # una sola
```

La cadena que ejecuta, orden por orden, es esta:

```bash
python scripts/extractors/sonda.py                     # estado de las ocho fuentes

for m in footballdata understat sofascore clubelo transfermarkt fatigue availability lineups understat_shots; do
    python scripts/extractors/$m.py weekly
done
python scripts/extractors/footballdata.py upcoming     # cuotas de la jornada por jugar

python scripts/consolidate/build_master.py && python scripts/consolidate/qa_master.py
python scripts/features/build_features.py  && python scripts/features/qa_features.py
python scripts/features/build_preprocess.py
python scripts/features/build_predict.py               # matriz de la próxima jornada

python scripts/models/build_models.py                  # reevalúa con la jornada nueva
python scripts/models/qa_models.py
python scripts/models/publish_web.py                   # emite la jornada por jugar
```

Los orquestadores admiten `--dry`, que ejecuta y reporta sin escribir. Es la forma
de auditar un cambio antes de dejar que toque los artefactos.

**Si football-data no ha publicado todavía el precio de la jornada**, el paso de
`build_predict.py` se detiene con `MissingOddsError` y deja escrita la plantilla. La
rutina no se reinicia desde el principio: se rellena y se reanuda por donde paró.

```bash
python scripts/rutina_semanal.py                       # para en `predict`
                                                       # ↓ rellenar odds_h/odds_d/odds_a
# data/01_raw/footballdata/footballdata_upcoming_manual_odds.csv
python scripts/rutina_semanal.py --desde predict       # reanuda y emite
```

Teclear una vez basta: al escribir la emisión, `promote_manual_odds` guarda lo tecleado
con su sello en `footballdata_odds_manual.csv` y `footballdata.apply_manual_odds` lo
vuelve a aplicar en cada reconstrucción posterior del histórico, de modo que la
predicción emitida y el registro histórico no se contradicen. El alcance exacto de esa
prevalencia —y por qué no alcanza a Bet365 ni al cierre— está en **El precio de la
jornada por jugar** de la ruta de inferencia.

`build_models.py` admite además `--regimen <nombre>`, que restringe la pasada a un
régimen de información:

```bash
python scripts/models/build_models.py --regimen semanal
```

La búsqueda de variables sí tiene orden de línea —`seleccion.py`—, pero no figura en
ninguna de las dos rutinas anteriores: se ejecuta cuando se elige el conjunto, no cuando
se actualiza la jornada. Lo que llega hasta aquí es su resultado, y llega por disco.

**La pasada única del holdout.** No forma parte de ninguna de las dos rutinas
anteriores y por eso figura aparte:

```bash
python scripts/models/build_models.py --holdout    # una sola vez, al cerrar el trabajo
```

Se ejecuta cuando el diseño está cerrado y no queda ninguna decisión por tomar,
porque tomar una después de ver esta cifra convertiría la temporada sellada en un
segundo conjunto de validación. La orden escribe `holdout_sello.json` y
`predicciones_holdout.parquet`; una segunda llamada encuentra el sello y se niega a
ejecutarse, de modo que la propiedad no depende de que nadie recuerde respetarla.

---

## Disciplina de artefactos

Tres reglas gobiernan todo el código y se aplican a cualquier módulo que se añada.

Un artefacto nuevo nunca empeora al vigente: la publicación pasa por
`_common.publish_df`, que compara el número de filas contra lo que hay en disco y
aborta si la cosecha se ha quedado corta, de modo que un fallo de descarga no puede
sustituir un maestro completo por uno truncado que superaría todas las aserciones de
esquema. Ausencia de dato y valor cero son estados distintos: un equipo sin bajas es
un cero real y un equipo no consultado es un hueco, y confundirlos hace que el
preprocesado impute la mediana de la liga a una plantilla completa. Fin de datos y
fallo de transporte son estados distintos: un bucle de paginación que se detiene ante
cualquier respuesta no satisfactoria convierte un error de red en «no hay más
partidos», que es un dato plausible y por tanto invisible.

El holdout se declara en `scripts/features/columns.py` (`HOLDOUT_SEASONS`) y
`qa_features.py` verifica que está sellado: todas las filas de esas temporadas en el
split `holdout` y ninguna otra.

---

## Comprobaciones

Las baterías de prueba viven junto a los módulos que validan y son **offline**: no
tocan la red ni requieren los artefactos, porque prueban la lógica de parseo,
agregación y guardas con datos sintéticos. Admiten dos modos equivalentes.

```bash
pytest                                              # todas las baterías
python scripts/extractors/test_teamnames.py         # una sola, con el detalle en pantalla
```

Sobre los artefactos ya generados, `qa_master.py` comprueba la unicidad de
`match_key`, la cobertura por fuente y la coherencia del calendario, más dos
invariantes de **arrastre de dato viejo**, que son las que persiguen el fallo
semanal que no da la cara: una fuente que deja de publicar y una jornada que se
publica con el dato de la anterior. El Elo entra por `merge_asof` backward, que no
tiene techo, así que si ClubElo cae su último rating se pega a todos los partidos
nuevos y la columna sigue llena de números plausibles. El listón se mide por eso en
**partidos del propio club** y no en días: el desfase en días tiene máximos
legítimos de tres meses —el parón de verano, la parada de la pandemia— y cualquier
tolerancia que los admita se traga también un mes de fuente caída en plena
temporada, mientras que en partidos el parón de verano vale cero, igual que una
semana corriente. Que al rating le falten **dos** resultados del club aborta sin
techo que lo tolere: son al menos dos jornadas sin publicar. Que le falte **uno** es
el retraso de refresco que la fuente sí tiene —16 de 7600 filas equipo-partido, un
0.21 %— y se tolera por debajo de un techo declarado, cinco veces esa tasa; se
cuenta y se informa, no se corrige inventando un valor que ClubElo no dio.
El resto de bloques entra por `match_key`, de modo que un
extractor que no corrió deja vacías solo las filas nuevas: a un bloque al que le
falten las dos últimas jornadas de las treinta y ocho le sale un 95 % y pasa todos
los suelos por temporada, así que la cobertura se mide además sobre la **cola** del
maestro, donde esa jornada pesa un tercio.

`qa_features.py` impone doce invariantes duros sobre el conjunto de variables:

```
 1  antifuga        ninguna columna del propio partido (goles, métricas crudas)
 2  unicidad        una fila por match_key
 3  holdout         las temporadas selladas, todas y solo ellas, en split holdout
 4  shift           en la 1ª jornada de cada equipo toda variable de forma es nula
 5  rangos          probabilidades y tasas en [0,1], margen de mercado, residuo Elo
                    acotado, zonas de tabla, franja horaria e indicadores binarios
 6  head-to-head    las tres tasas del historial suman 1, el historial de un par es
                    el mismo visto desde los dos lados y el bloque está computado
 7  contexto        todo club tiene estadio y el sello de saque no es una variable
 8  fuerza latente  la terna 1X2 del modelo de goles suma 1, los goles esperados
                    son de orden futbolístico y el bloque no queda a medias
 9  procedencia     igualdad de conjuntos entre columnas del set y declaradas
10  mercado         el rendimiento relativo es un cociente definido y no negativo,
                    y los arrastres psicológicos son indicadores binarios
11  precios         las ternas de consenso, Pinnacle y Bet365 suman 1 y son tres
                    lecturas distintas, no la misma repetida bajo tres nombres
12  testigos        las columnas testigo de cada bloque de fuente existen en el
                    máster y su indicador de cobertura se enciende en algún partido
```

El duodécimo guarda la matriz de cobertura, que es la tabla por la que se mira cada
jornada si una fuente contestó. Su listón son las columnas **testigo** que declara
cada bloque —no todas las que trae el proveedor: exigir las ciento seis del panel de
Sofascore a la vez, cuya composición varía de un partido a otro, convertiría el
indicador en un cero constante—. Esa lista está escrita a mano, es el único
conocimiento que no se deduce de ningún sitio, y por eso es el punto por el que la
cobertura se degrada en silencio: un testigo renombrado no produce ningún error,
produce un indicador que vale cero en los 3800 partidos y que el ojo aprende a
saltarse. Lo que se denuncia es el cero **perpetuo**, no la constancia: un bloque al
100 % en las diez temporadas también tiene un indicador constante, y ahí la
constancia es la respuesta correcta.

El séptimo es el **único punto de mantenimiento anual** de toda la cadena, y está
puesto donde detiene el paso en lugar de donde lo disimula. La tabla de estadios de
`features/context.py` se escribe a mano —treinta y dos filas que no cambian: montar
un scraping para eso añadiría una dependencia de red a cambio de nada—, así que el
club **recién ascendido** no está en ella el primer agosto que juega en Primera, y
sin su estadio `away_travel_km` e `is_derby` nacen nulos. El invariante lo nombra y
aborta: «clubes sin estadio en la tabla de `context.py`: […]». Se cierra añadiendo
una línea con el emplazamiento y la comunidad autónoma —la comunidad es el criterio
de derbi— y reconstruyendo las variables. Fallar aquí es lo correcto: la alternativa,
publicar el bloque de geografía con nulos, se vería en la matriz de cobertura solo si
alguien se para a leerla.

El sexto merece otra nota, porque es el que convierte en error un fallo que de otro
modo produciría números plausibles. `h2h.py` agrupa por par dirigido, de modo que las
dos filas de un partido recorren la misma secuencia de enfrentamientos desde lados
opuestos: la tasa de victoria del local contra ese rival **es** la de derrota del
visitante, y las diferencias de goles son opuestas. Si esa identidad se rompe, el
agrupamiento está mezclando pares, que es la firma de una clave de club sin
canonizar.

El protocolo tiene su propia batería, `scripts/models/test_protocol.py`, construida
sobre una liga sintética generada por el método del círculo: un doble round-robin en
el que ninguna jornada repite equipo **por construcción**, de modo que la
reconstrucción del calendario puede contrastarse contra una verdad conocida en lugar
de contra una suposición. Sus comprobaciones más útiles son las que fabrican el fallo
a propósito —una ventana de entrenamiento contaminada, una columna de cierre llegando
a la matriz bajo el régimen semanal, un valor no finito sobreviviendo al
preprocesado, una terna de clases permutada o incompleta— y verifican que la
ejecución se detiene. Una de ellas mide, además, lo que en otro sitio sería una
afirmación: interceptando las llamadas a `preprocess.fit`, comprueba que con cadencia
`unico` el preprocesado ve todo el bloque de desarrollo —incluido el futuro del
pliegue— mientras que con cadencia `temporada` ve estrictamente menos y sobre una
ventana que crece. Por el mismo camino se fija la equivalencia del reaprovechamiento
del preprocesado: que la matriz entregada sea idéntica valor a valor a la de
transformar solo la ventana, que la ejecución completa prediga lo mismo —medido con un
estimador que sí lee las features, porque el de frecuencias daría la misma salida
incluso con una matriz mal transformada— y que el número de transformaciones sea una
por temporada y no dos por pliegue.

Una sección propia fija el cuarto bloque, y lo hace añadiendo a la liga sintética una
temporada posterior al holdout, porque la fuga que previene solo existe cuando la hay:
que `temporadas_en_curso` la detecte y no invente ninguna cuando no la hay, que no
aparezca en los pliegues de `validacion`, `seleccion` ni `medicion`, que los de
`produccion` sean exactamente ella y solo con partidos ya jugados —la jornada por jugar
no es un pliegue, se emite por la ruta de inferencia—, y que sus ventanas de
entrenamiento contengan filas con `split == "holdout"`, que es la afirmación que
justifica todo lo anterior.

Cada módulo de modelado tiene batería propia y con la misma disciplina. La de
`estimators.py` no comprueba que los modelos funcionen sino que hacen lo que dicen:
que las cuatro familias se construyan por la misma puerta y devuelvan un estimador
nuevo en cada llamada —una fábrica que devolviese siempre el mismo objeto haría que
todos los pliegues compartiesen ajuste sin que ninguna métrica lo delatara—; que el
peso de recencia llegue a quien lo declara y solo a quien lo declara; que el tramo que
la parada temprana reserva esté enteramente **después** del tramo de ajuste y no parta
ninguna fecha por la mitad; que la logística plante una variable informativa en cabeza
de sus coeficientes; y que `construye` rechace todo parámetro fuera de `ESPACIOS`, que
es lo que impide que un valor se cuele por la firma de un constructor sin figurar en el
espacio declarado. Una sección aparte comprueba que las familias retiradas ya no
existen bajo ningún nombre. La de `evaluate.py` construye una liga en la que los aciertos se
agrupan por jornada para medir que el remuestreo por bloques da un intervalo más
ancho —y por tanto más honesto— que el remuestreo por partidos, y ejerce la identidad
de Murphy sobre los dos casos que la distinguen: un pronóstico constante, donde el
residuo es exactamente cero, y uno continuo, donde no lo es y la igualdad solo cierra
si se le cuenta. La de `backtest.py`
calcula libros de apuestas a mano y verifica la propiedad que sirve de calibración a
todo el bloque: apostar siguiendo el propio precio del mercado pierde exactamente el
margen, así que la política de valor se planta y no apuesta a nada. Sobre esa misma
liga de juguete comprueba la comparación con el apostante de referencia: que el
listón apuesta a la cuota más baja de su casa y a ninguna otra, que ambos ven los
mismos partidos, que la diferencia publicada es exactamente la resta de los dos
rendimientos, y que el informe declara contra quién se ha medido en lugar de dejar la
cifra suelta. Comprueba además lo que ocurre con un partido que tiene precio pero no
acta: no se liquida, no entra en el libro y no rebaja el beneficio, y la cobertura lo
refleja bajando —el fallo que previene es el silencioso, tratar un resultado
desconocido como una apuesta perdida y publicar un retorno más pesimista de lo que los
datos permiten afirmar—; con el bloque entero sin actas el libro sale vacío y el
resumen cuenta cero apuestas, en lugar de detenerse. Y comprueba que la unidad de
liquidación es la **jornada** incluso cuando el calendario la parte: un partido
aplazado que se juega semanas después sigue dimensionándose con el banco del inicio de
su jornada y no con lo que se haya ganado entre medias. Es una distinción que solo
afecta a Kelly, y la comprobación gemela dice por qué la cifra que se publica es la
del importe plano: con importe plano el orden de los partidos no altera el beneficio,
así que un aplazamiento no puede mover la cifra.

La de `laboratorio.py` vigila lo que hace defendible un banco manual, que es menos el
resultado que la promesa de no decidir por su cuenta. Sobre una liga sintética de seis
temporadas comprueba primero el envoltorio que recorta columnas, porque ahí la matriz
que ve el estimador deja de ser la del preprocesado: que el estimador recibe las
nombradas **en el orden de la matriz** y no en el de la lista —dos conjuntos con las
mismas columnas escritas al revés tienen que ajustar el mismo modelo, o sus
coeficientes no serían comparables—, que una columna repetida entra una sola vez, y
que los pesos de recencia y el contexto se reenvían solo a los estimadores que los
declaran. El caso delicado es la columna que la poda por colinealidad retira dentro de
un pliegue: detener el ensayo por eso haría depender la medición de un detalle del
preprocesado, así que se anota como ausencia y el ajuste sigue; si no sobrevive
ninguna, en cambio, se protesta, porque la matriz quedaría vacía.

Del catálogo se afirma no solo qué ofrece sino de dónde lo saca. Que ni el objetivo ni
la marca de reserva ni los identificadores ni el precio en escala cruda son candidatos
ya estaba comprobado; se le añade que tampoco lo es la probabilidad implícita de
Bet365, que sí viaja con cada partido porque la evaluación económica la necesita —una
referencia que fuese además una de las variables medidas convertiría la comparación en
una tautología—, y que una categórica ordenada sí lo es, porque no ser numérica no
excluye a una columna: la codifica el preprocesado, y filtrarla aquí por su tipo dejaría
fuera un bloque entero sin que ninguna cifra lo delatase. Las tres afirmaciones se
cierran con una cuarta que las hace duraderas: la lista que el banco ofrece es
**idénticamente** la de `preprocess.feature_candidates`, no una copia que empiece igual
y se separe en cuanto una de las dos definiciones cambie.

Del ensayo se afirma que su sello depende de lo que lo define y solo de eso: el mismo
conjunto con otro rótulo tiene el mismo sello y las mismas cifras, el orden en que se
escriben las columnas no lo cambia, y añadir una variable o mover un hiperparámetro sí
lo cambian. La procedencia que publica la fila se lee sobre las columnas **efectivas**
y no sobre las pedidas, que es lo que impide que una errata en el nombre de una columna
ausente detenga el ensayo justo al resumirlo. De la caché, que pedir dos veces los
mismos pliegues no los recalcula, que dos recortes distintos son dos claves distintas y
que `olvida` vacía las dos. Y del recorte por temporada, que sus tres formas de salir
mal se distinguen: un rótulo inventado, un rótulo que existe pero deja el bloque
evaluado vacío, y el que de verdad importa, un rótulo bien escrito con el separador
cambiado. Ese último es el peligroso porque no falla solo: el guion va antes que la
barra en la tabla de caracteres, de modo que `2019-20` recorta exactamente por donde
recortaría `2019/20` y nada en la salida delata la errata; el día que caiga en otro
sitio, el recorte se moverá y la cifra cambiará sin motivo aparente. La batería exige
que se pare antes, y que el aviso enseñe el formato que sí existe. Del holdout, que ni
`evalua` ni `ajusta` lo abren y que una evaluación corriente no toca ni una fila de la
temporada sellada.

La sección que expresa el criterio es la de `ronda`: que su primera fila es el conjunto
base con las diferencias a cero, que cada candidata se mide sobre el base más ella y no
sobre las anteriores, que ninguna se queda fuera, y que lo que devuelve es una tabla y
no un ganador. Una candidata que no puede ajustarse —porque su columna no existe— se
mide como ausente y no corta el recorrido, igual que una familia que no puede
instanciarse por faltar su dependencia opcional queda anotada con su motivo: descartada
por mala y descartada por rota no son lo mismo, y quien lee tiene que distinguirlas. Del
recorrido por dominios se comprueba además que reparte el conjunto **ya recortado por el
régimen** que se le pasa y no la matriz entera: el recorte lo hace el cuaderno, a la
vista, y si no llegase hasta aquí el reconocimiento inicial se haría sobre columnas que
el modelo publicable no puede usar y ordenaría en falso todo el trabajo posterior. La
batería cierra con el registro: que volver a registrar el mismo sello sustituye la fila
en lugar de duplicarla, que el historial guarda el recorte y el protocolo con que se
midió y no solo las cifras, y que un registro que aún no existe es un historial vacío y
no un error.

La de `seleccion.py` comprueba lo único que puede invalidar la cifra publicada, que no
es el resultado de la búsqueda sino dónde se buscó. Empieza por la partición: que
selección y medición no comparten ni un partido, que juntas cubren la validación entera
sin dejar huecos, que el arranque, la temporada sellada y la que se está jugando quedan
fuera de las dos, y que
los pliegues de la mitad que publica siguen entrenando solo con el pasado. Sigue por las
guardas, que son la parte que de verdad hace falta probar: que la voraz, la criba y la
medición rechazan cada una el bloque que no les toca, que el holdout es inalcanzable
desde el módulo, y que un criterio no declarado se detiene en vez de improvisar un
sentido de mejora. El aislamiento se afirma contando: interceptando cada medición, la
batería comprueba que **todas** las llamadas de un recorrido completo pidieron el bloque
de selección y ninguna rozó los otros dos.

De la búsqueda en sí se afirma el comportamiento sobre una escala fabricada con la
respuesta conocida: cuatro variables que aportan cantidades decrecientes y ocho inertes,
con la tercera por encima de la tolerancia y la cuarta por debajo. La voraz tiene que
adoptar exactamente las tres primeras y en ese orden, no repetir ninguna, no inventarse
ninguna fuera del conjunto de candidatas, y probar una menos en cada paso. La parada
recibe sección propia porque dejar de añadir es una decisión y no un agotamiento: el
paso que detiene la búsqueda se anota con su mejora aunque no se adopte, y el motivo
queda escrito con la cifra que lo provocó. Los tres finales posibles —tolerancia, tope de
variables y candidatas agotadas— se provocan a propósito y se distinguen por su motivo.

La exigencia de consistencia tiene sección propia, y el caso está fabricado al revés de
lo que conviene a la regla: la candidata inconsistente es la que **mejor media** tiene,
de modo que una voraz que decidiese por la media la adoptaría. La batería comprueba
primero que el caso está bien construido —que esa candidata gana por media—, después que
el descuento de la mejor temporada separa a las dos, y por último que no entra nunca,
que la consistente entra primero, que la traza anota cuántas quedaron fuera, y que con
una tabla por temporada vacía no se descarta a nadie: sin reparto que mirar la exigencia
no se puede aplicar, y entonces no se aplica.

La última sección exige que la traza permita reconstruir el recorrido sin repetirlo: una
fila por paso, el conjunto entero en cada una, y una ida y vuelta a disco que devuelve el
conjunto con el corte y el protocolo con que se obtuvo.

La de `explain.py` es la que más se apoya en aritmética conocida de antemano, porque
un reparto de importancia es justamente el tipo de resultado que parece razonable
mientras está mal. Sobre un modelo aditivo cuyas dos contribuciones se escriben a mano
comprueba que el reparto de Shapley devuelve **exactamente** esos dos números; sobre un
modelo simétrico en dos bloques, que cada uno recibe exactamente la mitad; sobre un
bloque que el modelo no mira, que su aportación y su importancia por permutación son
cero exacto, no un residuo pequeño. Un modelo espía verifica que la permutación
sustituye el bloque fila a fila y no columna a columna, que es la diferencia entre
evaluar un partido distinto y evaluar un partido imposible. La batería instala una
tabla de procedencia sintética en la caché del módulo, de modo que no arrastra la
cadena de variables y sigue siendo offline.

Las dos baterías de la capa de publicación parten de una premisa que conviene decir en
voz alta: **una comprobación de calidad que nunca se ha visto fallar no es una
comprobación, es una línea de código**. Por eso cada prueba de `test_qa_models.py`
construye a mano el defecto exacto que la función debe cazar y verifica las dos caras
—que el caso sano pasa limpio y que el averiado produce un fallo—, porque una función
que siempre devolviera lista vacía superaría la mitad de las pruebas que solo miran el
caso sano. El caso central es el reordenamiento de columnas: la prueba afirma primero
que los dos conjuntos son idénticos y después que la comprobación falla igualmente,
que es justamente la distinción que un contraste de conjuntos no puede hacer. El
conjunto cerrado se audita con el mismo criterio: un manifiesto que no declara con qué
variables se midió falla —el silencio no es una declaración—, un conjunto sin marcar
como cerrado falla aunque sus cifras sean correctas, y un manifiesto que publica una
familia y declara otra falla nombrando las dos.

La de `publish_web.py` ataca el modo de fallo propio de una capa de presentación, que
no es la excepción sino el número plausible. Comprueba que la entropía de una terna
uniforme vale exactamente uno y que la certeza absoluta da cero y no `NaN` pese al
logaritmo de cero; que el mismo valor crudo se normaliza igual en dos muestras
distintas, que es la propiedad que se pierde al escalar contra lo observado; que un
único valor ausente rebaja la cobertura del partido afectado y **solo** la de ese; que
la selección por valor difiere de la clase predicha en el caso construido para que
difiera; y que un partido sin precio no hereda selección. La emisión se prueba con un
estimador de juguete cuya probabilidad es una función cerrada de dos columnas, de modo
que la aditividad del reparto y el orden de las comprobaciones se contrastan contra
valores conocidos y no contra la salida de un modelo real, que solo permitiría
verificar que no ha cambiado. Dos de sus pruebas fabrican el fallo a propósito —un
esquema permutado y otro más estrecho— y verifican que la emisión se aborta antes de
puntuar, no después.

La de `rutina_semanal.py` no ejecuta ni un paso de la cadena —cada eslabón tiene su
propia batería— sino que vigila lo único que ese módulo aporta, que es el **orden**.
Comprueba que ningún paso ha desaparecido de la tupla ni ha cambiado de nombre de
fichero, que los tres que leen el calendario van después de quien lo publica, que cada
control va inmediatamente detrás de lo que audita, que la publicación es lo último y
que la búsqueda de variables no está. Las averías que previene no dan error: una
cadena a la que le falta un paso termina limpiamente y publica la jornada anterior,
porque el paso que faltaba era el que traía lo nuevo. Comprueba también que `--dry`
solo viaja a los scripts que lo entienden —un extractor lo tomaría por una orden— y
que el sondeo estático del conjunto, el que avisa al principio de que no hay variables
con las que emitir, dice «no lo sé» en lugar de «cero» cuando no puede afirmarlo: un
aviso equivocado que detuviese la rutina haría más daño que el fallo que previene.

Aparte de las baterías de cada módulo, `scripts/test_referencias.py` vigila una
propiedad del conjunto: que el vocabulario con el que los módulos se hablan entre sí
sigue existiendo. Recorre el árbol, resuelve los alias de importación de cada fichero
—`import columns as C` y equivalentes— y comprueba que todo atributo consultado a
través de uno de ellos figura entre los nombres del módulo apuntado. El fallo que
previene no se ve leyendo el código ni compilándolo: una constante que se movió de
módulo al reorganizar sigue compilando y solo revienta cuando se ejecuta la línea
concreta que la usa, que en una cadena con caminos que exigen artefactos en disco
puede tardar semanas en ejecutarse por primera vez. La comprobación es estática y por
tanto conservadora —solo mira el nivel superior de cada módulo, que es donde vive el
vocabulario compartido—, y las escasas ataduras en tiempo de ejecución se declaran
una a una con su motivo.

Que el nombre exista no basta, así que también se comprueba que la llamada encaje con
la firma: que no sobren posicionales, que ningún nombre de argumento sea desconocido y
que no falte ninguno de los obligatorios. Es la variante silenciosa del mismo fallo —la
función sigue estando y ha cambiado de forma—, y entonces no hay `AttributeError` que
avise: hay un `TypeError` mucho más adentro, cuando alguien recorre esa rama. Aquí
también se prefiere callar a avisar en falso: en cuanto la llamada desempaqueta con
`*` o `**` no se puede saber qué llega, y esa llamada se deja pasar.

Los cinco cuadernos pasan por el mismo control, y ahí importa más todavía. Un
cuaderno no lo importa nadie, de modo que ninguna otra batería lo toca: si una celda
llama a algo que se renombró, el fallo espera a que alguien ejecute esa celda
concreta, y en un documento de setenta y tres celdas puede ser la última del día. Los
alias se resuelven sobre el cuaderno entero —la celda de preparación importa y el
resto consulta— y las referencias se localizan celda a celda, que es la unidad en la
que uno abre el documento y corrige. Se añaden aquí los `from módulo import nombre`,
que en los cuadernos son la forma habitual de traer una función y fallan igual de
tarde.

Se vigila además la *forma* de importar. Las carpetas de `scripts/` se registran en
`sys.path` por separado y sus módulos se traen por su nombre a secas: escribir
`import models.columns` se lee como correcto y hasta parece más ordenado, pero no
resuelve, porque la carpeta no está en el camino de búsqueda como paquete. Este
error concreto se cuela por debajo de las dos comprobaciones anteriores —si el
alias nunca llega a ligarse, tampoco deja referencias que revisar— y por eso se
comprueba aparte.

La batería del preprocesado, `scripts/features/test_imputacion.py`, además de fijar el
recuento de columnas que cada estrategia toca, comprueba una propiedad que solo se
manifiesta al reutilizar un artefacto: aplicar dos veces el mismo estado a un marco ya
preprocesado devuelve exactamente las mismas columnas que aplicarlo una vez. Es la
comprobación que caza el defecto más difícil de ver de toda la fase, porque no lanza
ninguna excepción: si el módulo volviera a adjuntar los indicadores de faltante o la
copia cruda del precio sin retirar los que ya venían, el marco quedaría con la columna
duplicada y el fallo aparecería mucho después, cuando alguien pidiera `mkt_p_home_raw`
y recibiera una matriz de dos columnas donde esperaba una serie.

La ruta de inferencia tiene dos baterías, una por cada cosa que puede fallar en ella.
`test_scoring_gates.py` fabrica a propósito los fallos de cobertura y de enganche
—artefacto ausente o dañado, reductor que revienta, censo de tipificación deducido en
vez de impuesto, y la clave canónica congelada de un recién ascendido— y verifica que
la ejecución se detiene. `test_predict_gates.py` fija el perfil de **P.6** en sus dos
direcciones: que denuncia el desplome con la forma de Pinnacle y que no denuncia la
variación legítima, porque un gate que avisa de más se desactiva. Su comprobación más
útil no prueba P.6 sino que lo justifica: con el mecanismo real de `preprocess`,
construye la avería y enseña que tras imputar no queda un solo nulo que P.3 pueda
contar y que lo que llega al modelo es una constante en los diez partidos.

Por último, `build_preprocess.py --dry` reproduce el ajuste de los dos regímenes y
verifica que el *baseline* de mercado sigue siendo calculable en escala cruda desde
el artefacto y que el artefacto semanal no contiene ninguna columna que no exista en
el momento de predecir.

---

## Integración continua

El flujo de `.github/workflows/validacion.yml` se ejecuta en cada envío y en cada
propuesta de cambio. Comprueba que todos los módulos compilan, ejecuta las baterías
offline y verifica que no se ha versionado ningún artefacto de datos.

No ejecuta las comprobaciones de calidad sobre los artefactos porque `data/` no está
bajo control de versiones: los parquets pesan y se regeneran de forma reproducible
desde las fuentes, de modo que versionarlos duplicaría el problema que resuelven. El
flujo incluye ese paso condicionado a la presencia de los artefactos, de manera que
se activa por sí solo en un ejecutor que sí disponga de ellos.

---

## Entorno

Python 3.12 y las dependencias de `requirements.txt`. Las bibliotecas de scraping
solo hacen falta en la máquina donde se ejecutan los extractores: los módulos las
importan de forma perezosa, dentro de las funciones que las usan, para que la lógica
pura pueda validarse en un entorno mínimo con `pandas`, `numpy` y `pyarrow`.

```bash
python -m venv .venv && .venv/Scripts/activate      # en Windows
pip install -r requirements.txt
```

La comprobación cruzada con FBref tiene su propio fichero, `requirements-fbref.txt`,
y no está en el principal. La razón es de dependencias: `soccerdata` arrastra
`seleniumbase`, que fija su propia versión de `requests` y de `beautifulsoup4`, que
son las dos bibliotecas con las que se extraen Understat, Transfermarkt y
football-data. Un módulo que por diseño no alimenta el maestro no puede gobernar la
pila HTTP de las fuentes que sí lo alimentan, así que se instala aparte —y
preferiblemente en un entorno propio, porque la instalación moverá esas dos
versiones—. Requiere además Google Chrome instalado en el sistema, que es lo que
SeleniumBase usa para atravesar Cloudflare.

```bash
pip install -r requirements-fbref.txt               # solo para el contraste de FBref
```

---

## Estructura

| Ruta | Contenido |
|---|---|
| `scripts/test_referencias.py` | higiene del conjunto: todo `módulo.nombre` que escriben los módulos y los cuadernos existe, y cada llamada encaja con la firma declarada |
| `scripts/extractors/` | un módulo por fuente, más `teamnames.py` (claves canónicas) y `_common.py` (transporte, publicación y puntos de control) |
| `scripts/consolidate/` | reducción de cada fuente a partido y ensamblado del maestro |
| `scripts/features/` | variables pre-partido, preprocesado, ruta de inferencia y sus comprobaciones |
| `scripts/models/` | protocolo de validación temporal, las cuatro familias y sus espacios declarados, banco manual de ensayos, búsqueda flotante con su bloque de selección separado, evaluación, simulación económica, interpretabilidad, orquestador de la decisión, QA de publicación, proyección a la aplicación y sus comprobaciones |
| `notebooks/` | 01 extracción, 02 consolidación, 03 variables e inferencia, 04 análisis y preprocesado, 05 modelado y evaluación |
| `data/` | `01_raw/` crudos por fuente y `02_processed/` artefactos publicables (no versionado) |
| `outputs/` | `models/` evidencia y decisión por régimen, `web/` contrato de la aplicación (no versionado) |
