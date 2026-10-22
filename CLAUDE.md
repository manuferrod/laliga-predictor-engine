# CLAUDE.md — laliga-predictor-engine

Contexto operativo para cualquier sesión de Claude que abra este repo. Lo escribió
Claude el 2026-10-08 a partir del histórico del proyecto, al migrar el trabajo de
Cowork a Claude Code. **Mantenlo actualizado**: si una decisión cambia, se corrige
aquí, no se añade un apartado nuevo.

## Qué es esto

Motor predictivo **1X2 de LaLiga** (local / empate / visitante) que sostiene el TFM
de Manuel Fernández en el Máster en Big Data Analytics Aplicado al Fútbol. Tres
piezas: el motor (este repo), la aplicación pública Streamlit
(`../laliga-predictor-portfolio`) y la memoria escrita.

La pregunta que motiva el trabajo es una sola: **¿bate el modelo al mercado de
apuestas?** De ahí se derivan casi todas las reglas de abajo.

Repos públicos (owner `manuferrod`):
`laliga-predictor-engine` · `laliga-predictor-portfolio`.

Fechas: entrega prevista 2026-08-25. El histórico documentado llega al 2026-08-14;
**lo que haya pasado desde entonces hay que preguntárselo a Manuel**, no deducirlo
de este fichero.

## Lo primero que hay que leer

El README de este repo son ~1200 líneas y es la especificación real del sistema, no
un resumen de cortesía. Este CLAUDE.md no lo sustituye: lo indexa.

| Documento | Para qué |
|---|---|
| `README.md` | mapa de artefactos, validación temporal, orden de ejecución, comprobaciones, estructura |
| `DISENO_MODELADO.md` | protocolo walk-forward, escalera de modelos, métricas, contrato de salida a la app |
| `MEJORAS_PROPUESTAS.md` | las quince mejoras M1–M15 y su justificación |
| `REGISTRO_SUBSANACIONES.md` | los 21 hallazgos de la revisión técnica y cómo se cerraron |
| `../../CATALOGO_PROPUESTO.md` | propuesta del catálogo curado de variables |
| `../../FASE_5_PASO_A_PASO.md` | guion de la fase de modelado |

Secciones del README a las que conviene ir directo: **Orden de ejecución**,
**Comprobaciones**, **Entorno** y **Estructura**, las cuatro con encabezado `##`. Van por
título y no por número de línea: el README crece con cada ruta medida y un número de línea
escrito aquí caduca a la primera inserción. `grep -n "^## " README.md` las localiza.

## Arquitectura, en una pasada

```
scripts/extractors/   9 fuentes + sonda.py (salud de las 8 puertas de red) +
                      teamnames.py (claves canónicas) + _common.py (transporte)
scripts/consolidate/  reduce cada fuente a partido y ensambla data/02_processed/master_matches.parquet
scripts/features/     variables pre-partido, preprocesado, ruta de inferencia
scripts/models/       protocolo temporal, familias, selección, evaluación, economía, publicación
scripts/rutina_semanal.py   los 21 pasos de la jornada, en orden, parando en el primer fallo
notebooks/            01 extracción · 02 consolidación · 03 variables · 04 preprocesado · 05 modelado
data/, outputs/       no versionados (ver .gitignore)
```

Fuentes: Sofascore (estadística de partido y ratings), Understat (xG/npxG/PPDA/deep/xPTS
desde 16/17), ClubElo (rating de club), football-data (resultado = objetivo + cuotas de
casas), Transfermarkt (valor de plantilla, prior de fuerza), más tres derivadas propias:
`fatigue` (descanso multicompetición y congestión), `availability` (bajas, ponderadas en
euros) y `lineups` (rotación y minutos). `understat_shots` da calidad de ocasión.
`fbref.py` está **aparcado** como contraste: el dato avanzado a nivel equipo-partido
desapareció en enero de 2026 y lo que queda es box-score redundante. Vive con su propio
`requirements-fbref.txt` porque `soccerdata` arrastra `seleniumbase` y fijaría versiones
de `requests`/`beautifulsoup4` que usan las fuentes que sí alimentan el maestro.

Histórico desde la temporada 16/17. La temporada 25/26 está **sellada como holdout**.

## Reglas de trabajo que no se negocian

Son decisiones de Manuel, ya discutidas. Respétalas antes de proponer alternativas.

1. **Todo extractor tiene dos modos**: `backfill` (histórico completo, idempotente,
   salta lo ya almacenado) y actualización semanal adaptativa que solo añade lo nuevo.
   Invocación única y canónica: `python scripts/extractors/X.py <orden>`. Nunca
   `python -m scripts.extractors.X`.
2. **El catálogo manda sobre las features.** `scripts/features/catalogo.py` es la única
   fuente declarativa: `LADOS` dice si un atributo sale como par, como diferencia, como
   magnitud de partido o si no sale, y `catalogo.nombres()` genera los nombres finales.
   Ningún módulo decide por su cuenta lo que emite. Para añadir o quitar una variable
   se toca primero el catálogo y luego el módulo lo lee. Cada batería tiene un test
   CONTRATO que compara por igualdad de conjuntos en las dos direcciones, de modo que
   una columna calculada y no declarada y una declarada y no producida fallan ambas.
3. **Toda cifra económica se publica emparejada con el listón de Bet365**
   (`backtest.REFERENCIA = "b365"`, su cuota más baja, todos los partidos con precio,
   sin umbral), por temporada y por jornada. `qa_models.check_economia` aborta la
   publicación si una cifra de ROI sale sola. Sin listón, la cifra no responde a la
   pregunta del TFM.
4. **Cuando ROI y RPS discrepan para elegir el modelo publicado, manda el ROI.** Lo
   decidió Manuel el 19-oct-2026 —«quiero que el ROI mande, no el RPS»—, y es suya la
   decisión, no del código: el proyecto se construyó con el criterio contrario (el RPS
   como métrica principal y el ROI como traducción al lenguaje de la pregunta, criterio
   C.1 del cuaderno 05), así que el cambio va escrito aquí y no aplicado en silencio.
   El **20-oct-2026 Manuel amplió el alcance** —«el modelo que voy a utilizar va a ser
   el que mejore el accuracy y el ROI al máximo… olvídate del RPS para encontrar el mejor
   modelo»—, de modo que el RPS deja de arbitrar también en la **selección de variables**:
   los criterios de búsqueda son `accuracy` y `ventaja` (el ROI de la política plana menos
   el del listón), que son los dos que recorre `_rodante.CRITERIOS`. Lo que el RPS sigue
   rigiendo es todo lo que no es elegir: el piso frente a la previa de frecuencias, la
   ordenación de la tabla comparativa y la métrica de permutación con la que se explica,
   que es lo que hace `build_models.METRICA_ELECCION = "rps"` y no arbitra entre las dos.
   **Y se sigue reportando en cada tabla aunque no adopte**, porque el daño en RPS es lo
   que delató a las rutas anteriores y una búsqueda guiada por acierto puede comprarlo sin
   decirlo. El precio de la decisión está medido y declarado: el modelo publicado tiene un
   RPS **0.000527 peor** que el de solo precio al que sustituye.

   Con la ampliación vienen dos directivas más del mismo día, de alcance y no de criterio.
   **El universo evaluable es 17/18–26/27 sin excepción** —«todos esos partidos son
   necesarios de evaluar»—, lo que obliga a bajar el arranque a 2016/17 sola
   (`seleccion.WARMUP_CONFIRMACION`) y a elegir con **origen rodante**: la configuración de
   cada temporada se busca con su propio pasado, que es la única forma de evaluar 17/18 y
   18/19 sin romper la regla 6. Y **la app tiene que dejar repasar temporada por temporada,
   todas sin excepción**, con resultados, predicciones y métricas agregadas.
5. **Ninguna cifra del bloque de selección es citable.** La curva de RPS frente al
   tamaño del conjunto que dibuja **C.9** elige y mide sobre los mismos partidos, así que
   es optimista por construcción: el codo es una lectura, no una decisión. Lo que sostiene
   el tamaño elegido es la lectura de FUERA de la elección —repetir la búsqueda con
   `corte_seleccion` en `2021/22`, forzando la adopción, y puntuar cada prefijo sobre la
   temporada que no participó en elegirlo—, y la cifra que se cita es la del bloque de
   medición (**C.10**).
6. **La selección decide en el pasado.** La configuración de cada temporada se elige con
   información anterior a lo que luego se puntúa; eso es lo que hace defendible el modelo
   ante el tribunal, y `scripts/models/seleccion.py` + su banco son la pieza que lo
   sostiene. No introduzcas nada que mire hacia adelante.
7. **El holdout se abre una vez.** `build_models.py --holdout` escribe
   `holdout_sello.json` y una segunda llamada se niega a ejecutarse. No lo ejecutes tú
   ni propongas borrar el sello.
8. **Los notebooks leen de disco, no recalculan.** Una cifra recalculada en el cuaderno
   podría no ser la que auditó `qa_models`. Si tocas `build_models.py`, `evaluate.py` o
   `publish_web.py`, revisa las celdas C.0–C.7 y E.1–E.4 del notebook 05, que dependen
   de los nombres de columna de los parquets publicados.
9. **La entrega es versión única y final.** Nada de lenguaje de changelog: ni "antes
   se…", ni "en la versión anterior", ni códigos de informe de revisión, ni
   desincronización entre `.py`, notebooks y README. Si cambias código, sincroniza los
   tres.
10. **Al tocar un eslabón de la rutina semanal**, actualiza también la tupla `PASOS` de
    `scripts/rutina_semanal.py` y su batería. El error semanal probable no es escribir
    mal una orden —eso falla y se ve— sino saltarse una: cada paso lee lo que dejó el
    anterior y un parquet de la semana pasada se lee igual de bien que uno nuevo.
11. **Los notebooks de `notebooks/referencia_v1/` y la carpeta `_legacy_v1/` no se
    borran por iniciativa propia.** Son referencia de razonamiento del pipeline v1.
    Manuel tiene abierta la decisión de si acompañan a la entrega; es suya.
12. **Clasifica una columna por lo que mide, no por el módulo que la calcula.** Los
    bloques de `build_scoring.py` van por FUENTE.
13. **Claude no hace `git push`.** Prepara los cambios; Manuel publica.

## Estado conocido (a 2026-08-14)

Lo que estaba cerrado por escrito:

- Las nueve fuentes extraídas y validadas; consolidación, features, preprocesado,
  modelado y publicación implementados.
- Capa de features reducida vía catálogo desde ~488 columnas. **Recuento del
  14-oct-2026, hecho con `catalogo.nombres()` y con los esquemas de los parquets**, que
  es el que hay que citar: el catálogo declara **47 atributos → 65 nombres de columna**,
  y esos 65 son exactamente el banco de candidatas que recorre `seleccion.py`. La matriz
  de features son **131 columnas** (los 65 más las de SERVICIO, que no entran al modelo
  pero viajan porque `backtest` y `evaluate` liquidan con ellas). Ocho de los 65 salen
  del catálogo sin prefijo y en la matriz lo llevan (`fat_` para descanso y viajes,
  `h2h_` para las tres de historial), que es lo que cubre el argumento `prefijo` de
  `nombres()`. **Vuelve a contar antes de citar**: si el catálogo cambia, estas cifras
  caducan.
- Las quince mejoras M1–M15 implementadas y sincronizadas en `.py` + notebooks + README.
  M1 era fuga de distribución futura (`preprocess.fit` una sola vez sobre todo `dev`);
  M2, el sesgo entrenamiento-servicio de las cuotas de cierre, resuelto con dos
  regímenes: `semanal` (lo que la web puede saber) y `retrospectivo` (el techo medible).
- Notebooks 01–05 escritos. El 05 con estructura A–E: protocolo, escalera, evaluación,
  interpretabilidad, decisión y sello.
- Baterías en verde offline y `test_referencias.py` limpio (74 módulos + 5 notebooks sin
  referencias rotas).
- `scripts/rutina_semanal.py` orquesta los **21 pasos**, dos de ellos opcionales. El
  último es `app`, el traslado a `../laliga-predictor-portfolio` que añadió
  `sincroniza_app.py`; va detrás de `web` porque copia lo que `publish_web` declara.

Añadido el 2026-10-08, al retomar, con la fiabilidad de la extracción como prioridad:

- `_common.http_get_bytes` **reintenta con espera creciente** en lo transitorio —red,
  429 y 5xx— y devuelve los 4xx en el primer intento, porque un 403 es la señal de
  escalada de `understat.py` y `sofascore.py` y tiene que llegar pronto.
- `extractors/sonda.py`, paso 0 de la rutina: estado de las ocho puertas de red en una
  orden, con vía, motivo y remedio, parte en `outputs/sonda/` e histórico en JSONL.
  Solo aborta si cae una fuente sin sustituto.
- `qa_master.py` gana la invariante 6, **arrastre de dato viejo**, y se mide en
  **partidos del propio club** y no en días: cuántos resultados le faltan al rating que
  `merge_asof` pegó. Faltar dos aborta sin techo; faltar uno es el retraso de refresco
  real de ClubElo —**16 filas, 0.21 %**— y se tolera bajo un techo declarado del 1 %. En
  días no hay listón posible: el parón de verano tiene máximos legítimos de tres meses y
  en partidos vale cero. La cobertura se mide además sobre la cola del maestro, no solo
  en promedio por temporada.

  **Esa cifra es un indicador de frescura, y leerla como deriva es un error que ya
  cometí.** El 19-oct-2026, antes de refrescar las fuentes, medí 21 de 7738 filas
  (0.27 %) y escribí aquí que la diferencia frente a las 16 documentadas era deriva de la
  cola del maestro. No lo era: tras ejecutar la rutina completa, con ClubElo al día, la
  invariante vuelve a medir **16 de 7756 filas (0.21 %)** aunque el maestro ha crecido. El
  numerador no sigue al tamaño del maestro, sigue al retraso del extracto de ClubElo, así
  que **si esta cifra sube, la pregunta es cuándo se extrajo el Elo, no si el maestro ha
  derivado.**
- **ClubElo se extrae de la web** (`clubelo.com/ESP`, una ficha por club), porque la
  ruta de descarga que servía el archivo histórico dejó de responder. El crudo son dos
  CSV que `process` empalma en `ELO_CORTE = 2022-10-18`, y como la fuente **rebasó su
  escala** en ese corte —+85 puntos, 11 % menos de dispersión— el modelo consume el Elo
  **tipificado dentro de la jornada** (`home_elo_z`, `away_elo_z` y las tres derivadas
  `elo_mom_5_z_diff`, `form_opp_elo_z_3_diff`, `form_opp_elo_z_ewm_diff`). La
  tipificación dentro de la fecha es el invariante de la transformación afín; la base es
  el censo de Primera del propio proyecto, nunca el `level` de ClubElo. El nivel crudo
  se queda en el maestro como columna descriptiva y **no entra en la matriz**. La
  invariante 7 de `qa_master` mide el precio del empalme: el salto de la `z` en el corte
  (mediana 0.073, máx 0.194) cabe dentro de una semana corriente de competición
  (mediana 0.048, p99 0.180, máx 0.343).
- `qa_features.py` gana la invariante 12, **testigos de cobertura**: las columnas que
  cada bloque de fuente declara como testigo existen en el maestro y su indicador se
  enciende en algún partido. Lo que denuncia es el cero **perpetuo**, no la constancia
  —un bloque al 100 % en diez temporadas también es constante, y ahí está bien—.

En `data/02_processed/` hay maestro (**3878 × 249**), features (**3878 × 131**) y los dos
regímenes de preprocesado: `_semanal` son **90 columnas** en el parquet, de las que el
modelo ve **68**, y **5 de esas 68 son indicadores de faltante** (`flag_no_absences`,
`flag_coldstart`, `flag_no_h2h`, `flag_no_squadvalue`, `flag_no_strength`);
`_retrospectivo`, **95 columnas** y **73**, de las que **6** son indicadores —el que añade
es `flag_no_closing_odds`—. **El «de esas» no es retórico: los indicadores van dentro del
recuento, no sumados a él**, y `n_missing_diff` no se cuenta entre ellos porque es un
conteo y no una bandera. El resto de columnas del parquet es SERVICIO. Las cifras de
columnas salen de los esquemas y de `preprocess_state_*.json`. **No las confundas con un
recuento de columnas de parquet**: 110 y 112 son los anchos de los ficheros en un diseño
de un solo régimen, no variables de entrada del modelo.
`qa_master`, `qa_features` y `pytest -q` completo (**184 pruebas**, salida 0; el recuento
fiable sale de `--collect-only`, porque `pytest -q` escribe su resumen al final y un
`| tail` lo pierde por bufferado), en verde. Quedan sin leer
por nadie `features_prep.parquet` (215 columnas) y `preprocess_state.json` (202
`feature_cols`), del 27-jul y del diseño de un solo régimen: **son de Manuel la decisión
de borrarlos o renombrarlos `.bak`**.

## Lo que quedaba pendiente — verificar antes de hacer nada

Esto es lo primero que hay que confirmar con Manuel o mirando el disco, porque es
trabajo de **ejecución local** que Claude no podía hacer desde el sandbox anterior:

1. ~~La búsqueda de variables no se había ejecutado nunca.~~ **Cerrada el 13-oct-2026.**
   La voraz adoptó dos variables que pasaron las tres puertas —tolerancia, consistencia
   por temporada y techo de la nube de ruido (1.39× el máximo esperado de 59 sorteos,
   z = 2.86)— y aun así **empeoraban el modelo donde no se eligieron**: +0.000551 de RPS
   sobre 1900 partidos, intervalo [+0.000104, +0.001005], que excluye el cero. De ahí
   salió una cuarta puerta sin parámetro que elegir, `seleccion.confirma()`, sobre el
   bloque de **confirmación** (2017/18–2018/19, anteriores a la selección, así que la
   regla 6 queda en pie). Ejecutada, **rechaza** (+0.000466 sobre 760 partidos), y
   `conjunto.json` queda cerrado sobre las **tres variables de precio**
   (`mkt_p_home/draw/away`), con `propuesto`, `confirmado` y `confirmacion` guardados
   para que un conjunto revertido no se lea igual que una búsqueda que no encontró nada.
   Copia del estado anterior en `conjunto_k5.json.bak`.

   Y la objeción de fondo —«¿no paró la búsqueda demasiado pronto?»— está contestada
   midiendo, en **C.9.1** del cuaderno 05 y en el README: recorrido del **banco entero**
   con la adopción forzada, 58 pasos de k=3 a k=60, eligiendo solo con 19/20 y 20/21 y
   puntuando cada prefijo sobre 2021/22. **El RPS de fuera es peor que el de la semilla
   desnuda en 55 de los 57 pasos**, y los dos que lo mejoran son los de las dos variables
   que la cuarta puerta luego rechazó. Dentro hay un mínimo creíble en k=9 (0.191703) que
   fuera no existe (0.194967). El dinero dice lo mismo: ROI de fuera de +18.53 % con la
   semilla a −10.83 % con 25 variables, y la ventaja sobre el listón cruza el cero en la
   octava. Lo lee de `outputs/models/seleccion/camino65.csv` y `camino_2021.json`
   (`seleccion.CAMINO_65` y `CAMINO_FUERA`), que viven en `outputs/` y **no se versionan**:
   por eso las cifras están escritas también en prosa en los tres sitios.

   **Y la otra mitad de la pregunta también está medida (14-oct-2026).** Todo lo
   anterior evalúa las variables **como añadido** al precio, y por eso su resultado nulo
   admitía dos lecturas incompatibles: que las variables de juego no midan casi nada, o
   que midan mucho y el precio ya lo haya incorporado. Mientras el precio esté dentro del
   modelo las dos son indistinguibles, porque condicionar por él es exactamente lo que
   las confunde. Se separan quitándoselo: las **59 variables de juego, sin una sola
   columna de precio**, dan **RPS 0.202853** sobre el bloque de selección, frente a
   0.223518 de la previa de frecuencias y 0.193174 de la semilla. Recorren el **68.1 %**
   del hueco, y el acierto —métrica independiente, que no comparte construcción con el
   RPS— dice **68.3 %**. **El banco reconstruye desde cero dos tercios de la información
   con la que el mercado fija su precio, sin verlo**; el tercio que falta es el que el
   proyecto no puede tener (alineaciones confirmadas, vestuario, y el dinero de los
   propios apostantes). Dos hallazgos más, del mismo cuadro: el dominio de **`fuerza`
   solo** —nueve columnas, Elo y valor de plantilla— llega a **0.198001 y bate a las 59
   juntas**, así que ese 68 % es el techo de un *ajuste* de 59 variables sobre 1140
   partidos y no el techo del banco; y **las 65 juntas dan 0.200669, 0.0075 peor que las
   tres del precio solas**, es decir que ahogan lo que el precio sabía. Con eso queda
   contestada la objeción de Manuel —«no puede ser que un modelo de una sola variable
   mejore a todo el resto posible»—: el modelo de precio no gana por estrecho, gana
   porque **cada variable que entra cuesta más varianza de la que trae de señal nueva**,
   ya que su señal estaba en el precio y la varianza es toda suya. La escalera por
   dominios (`fuerza` 0.841 → `forma` 0.679 → `clasificacion` 0.525 → `historial` 0.317 →
   `fatiga` 0.143 → `bajas` 0.011 → `contexto` −0.025) se ordena como la ordenaría
   cualquiera que entienda de fútbol, lo que es además una comprobación de cordura de la
   capa de variables entera. Está en **C.9.2** del cuaderno 05 y en el README; la produce
   `_sinprecio.py` y la deja en `seleccion.SINPRECIO`
   (`outputs/models/seleccion/sinprecio.csv`, tampoco versionado).

   **Y la objeción directa —«¿y el mercado MÁS lo mejor del resto?»— también está medida
   (14-oct-2026), y al medirla aparecieron dos defectos del PROCEDIMIENTO de la búsqueda
   que conviene no volver a introducir.** Manuel tenía razón en que la comparación no era
   limpia. **Uno: la penalización estaba clavada** en `laboratorio.HIPER` (`C=0.3, L2`)
   para toda la búsqueda, de modo que k=3 y k=12 se comparaban con un castigo de varianza
   calibrado para k=3; el módulo declaraba el precio de esa decisión y lo remitía a
   `afina()`, pero `afina()` solo afina el conjunto que **ya ganó**, así que la
   compensación nunca llegaba a los conjuntos que la necesitaban. Y cuesta mucho:
   `_ridge.py` mide **0.0061 de RPS entre la mejor y la peor penalización sobre las mismas
   65 columnas** (0.194557 con `C=0.03, L1` contra 0.200669 con `C=0.3, L2`), recuperados
   sin tocar una variable. En el propio cuadro de `_bloques.py` se ve de dónde sale: dejar
   que cada conjunto elija su penalización le recupera **0.005663** al más grande y solo
   **0.000006** a la semilla, es decir que la rejilla ayuda a los conjuntos grandes y no
   al pequeño — que es exactamente el sesgo que inflaba la ventaja del conjunto pequeño.
   **Dos: la voraz es miope**, añade de una en una, así que un dominio que ayude entero
   mientras ninguno de sus miembros ayuda solo le es invisible, y eso no es hipotético
   aquí porque `fuerza` como bloque bate a las 59 de juego juntas.

   Corregidas las dos, **la distancia se estrecha de verdad**: `semilla+fuerza` pasa de
   perder 0.001081 a perder **0.000780**. Pero el veredicto no cambia, y ahora lo
   sostienen **cuatro rutas independientes**: la voraz de una en una (peor que la semilla
   en 55 de 57 pasos), L1 sobre las 65 dejando que la penalización seleccione (0.194557,
   pierde 0.001383, intervalo que excluye el cero), los **siete dominios enteros cada uno
   con su mejor penalización de cinco** (`seleccion.BLOQUES`) y la **semilla más las N
   mejores ordenadas por mérito propio sin el precio delante** (`MEJORES`, N = 3, 5, 8,
   12, 20, 30: pierden de 0.000252 a 0.000928). La escalera acumulativa —la pregunta en su
   forma literal— es **monótonamente peor**: 0.193949 con `fuerza`, 0.194226 con `forma`,
   hasta **0.194399** con las 62, con intervalos que excluyen el cero desde `forma` en
   adelante. No es ruido: es daño. Dos conjuntos sí batieron a la semilla en el bloque de
   selección y **la cuarta puerta rechaza los dos con el signo invertido**, que es la firma
   de haber escogido el máximo de 35 configuraciones: `semilla+bajas` gana 0.000211 donde
   se eligió y pierde 0.000312 donde no —mismo dominio que `out_minshare_max_diff`, una de
   las dos que la voraz adoptó y esta puerta ya había devuelto—; y `semilla+historial`
   empata en RPS y gana +0.61 pp de acierto, que son **0.41 errores típicos** de los
   1.48 pp que mide el acierto sobre 1140 partidos, es decir nada, y fuera pierde 0.000586
   con intervalo [+0.000075, +0.001115], que ya excluye el cero. Para que la puerta pudiera
   juzgarlos, **`confirma()` acepta ahora `hiper`**: un conjunto adoptado bajo `C=0.1, L1`
   y confirmado bajo `C=0.3, L2` se puntuaría con un castigo que su elección no tuvo, y el
   rechazo no distinguiría la falta de señal del cambio de configuración. Está en
   **C.9.3** del cuaderno 05 y en el README; lo producen `_bloques.py` y `_mejores.py` en
   `seleccion.BLOQUES`, `MEJORES` y `MEJORES_ORDEN` (tampoco versionados).
2. ~~**`pytest -q` completo en local.**~~ **Cerrada el 21-oct-2026**: las **184 pruebas**
   de los **35 ficheros** de batería en verde, salida 0, incluidas `test_strength.py` y
   las de `scripts/models/` que ajustan modelos de verdad. Dos avisos de recuento, porque
   los dos me han dado una cifra equivocada: `pytest -q` escribe su línea de resumen al
   final y un `| tail` la pierde por bufferado; y `--collect-only -q` **no imprime el
   total**, solo una línea `fichero.py: n` por batería, así que el recuento fiable sale de
   sumarlas.
3. **Reejecutar la cadena** `build_master → qa_master → build_features → qa_features →
   build_preprocess`, luego `build_models.py`, `qa_models.py`, `publish_web.py`, y
   reejecutar los notebooks 01–05. **La cadena entera está reejecutada** —los tres de
   modelado se estrenaron el 14-oct-2026 y la cola de publicación se volvió a correr el
   19-oct sobre el maestro ya crecido, 8.9 min, las nueve invariantes duras en verde y los
   catorce artefactos de `outputs/web/` reescritos—; **lo que queda sin hacer son los
   notebooks**. Del 05 en particular: lleva **95 celdas**, y las tres secciones nuevas
   —C.9.5, C.9.6 y C.9.7, la décima ruta, la undécima y el máximo sobre el censo de
   nueve— **entraron con el código probado y sin salida guardada**. Se probó fuera del
   `.ipynb` con `_nbceldas4.py`, contra los artefactos reales, porque una celda que falla
   dentro deja el cuaderno con una traza guardada y el cuaderno es entregable; quien las
   insertó es `_nbins4.py`, que es idempotente. Reejecutar el 05 es lo que lo cierra.
   Lo que `publish_web.py` sigue sin poder escribir es la emisión, y el motivo
   no es un fallo suyo: la puerta de frescura de `build_scoring` aborta `build_predict`
   mientras falte por consolidar la jornada en juego —hace lo que debe: la forma y la
   clasificación se calcularían con valores de semanas atrás sin que saliera ningún NaN—.
   Y aunque el maestro estuviera al día, a mitad de semana no habría nada que emitir:
   `footballdata_fixtures` sirve el fichero vacío, que es lo normal. **Primero la rutina
   semanal, después la emisión.**

   **El recuento de la puerta es exacto, y conviene saber por qué lo parece y no lo es.**
   Al auditar esto el 19-oct-2026 llegué a escribir aquí que los «ocho partidos» eran un
   suelo y que el hueco real eran tres jornadas, contando los estados `notstarted` del
   calendario. **Era falso, y queda como aviso porque el error es fácil de repetir**:
   `sofascore_team_fixtures.csv` es **multicompetición** por diseño —trae LaLiga 2,
   Primera Federación, Champions, Europa League y amistosos, porque `fatigue` necesita el
   descanso real de cada club—, así que contar `status` sin filtrar por competición
   inventa un problema que no existe. Las filas `notstarted` de los viernes y domingos
   son de LaLiga 2; y una fecha con unas `finished` y otras `notstarted` no es un estado
   congelado, son dos categorías el mismo día. `_sofa_future_window` filtra
   `competition == "LaLiga"` (l. 1160) antes de comparar, de modo que la puerta nombra
   exactamente los partidos de Primera que faltan: **ni uno más ni uno menos.**

   **La rutina completa, ejecutada el 19-oct-2026**: 17 pasos en 8.9 min, los once de
   extracción en verde con sofascore `DEGRADADA` (403 del host canónico, cascada escalada
   al espejo; la sonda lo autoriza). El maestro pasa de **3869 a 3878 filas** y 2026/27 de
   69 a **78 partidos**, con la jornada 8 dentro —9 de 10: el Rayo–Athletic del 10-oct
   consta `canceled` en el calendario—. La puerta baja de 8 partidos a **5**, y los 5 son
   de la jornada 9 (16 y 17-oct). **El bloqueo que queda no es del proyecto**: el CSV de
   football-data.co.uk termina el 12-oct y todavía no ha publicado la jornada 9, y el
   resultado es el objetivo, así que sin él no hay fila que consolidar. Se resuelve solo
   cuando la fuente publique; no hay nada que arreglar. Los partidos del 18 y 19-oct no
   los nombra porque `grace_days = 2` existe justamente para no señalar el de anoche.

   **La cadena entera reverificada el 22-oct-2026, y lo único que la detiene son dos
   esperas de fuente.** Rutina completa con extracción en vivo: los once pasos en verde en
   7.0 min, otra vez con sofascore `DEGRADADA` por el espejo. El maestro pasa de **3878 a
   3889 filas** y 2026/27 de 78 a **89 partidos**: football-data publicó la jornada 9 —el
   bloqueo del 19-oct se resolvió solo, como estaba previsto— y con ella entró el
   **Levante–Athletic de la jornada 6**, aplazado y jugado el 21-oct. `features`,
   `qa_features` y `preprocess` en verde sobre el maestro nuevo (**3889 × 90**, 68 al
   modelo, y **3889 × 95**, 73), y `build_models` mide **12 pliegues y 89 partidos** en
   2026/27 contra los 10 y 78 de la semana anterior. **El invariante se cumple**: la
   validación sale idéntica al sexto decimal —3037 partidos, RPS 0.194494, acierto 53.38 %,
   `skill_rps` −0.001947, y las dos filas de economía con sus +0.79 % / −0.04 % y
   −0.35 % / −1.74 %—, de modo que la jornada nueva solo movió `produccion`. `qa_modelos`,
   `web` y `app` también en verde.

   Las dos esperas, y **ninguna de las dos es un fallo del proyecto**:

   - **`qa_master` aborta por la invariante 6b**: el bloque `xg` cubre 29 de los últimos 30
     partidos (97 %, suelo 98 %), porque Understat no ha publicado el aplazado del 21-oct
     —su API declara 88 finalizados en 2026/27 y el maestro tiene 89—. Es el caso simétrico
     del de la semana pasada con football-data, y se cura cuando la fuente publique. **El
     patrón que conviene reconocer para no volver a investigarlo**: cuando un aplazado se
     consolida el día de después, football-data suele llegar antes que Understat, y un solo
     partido de treinta ya baja del suelo.
   - **No hay nada que emitir**: `footballdata_upcoming` trae 21 fixtures y los 21 están
     jugados —`fixtures.csv` sirvió **1** próximo, el del 21-oct—, así que football-data no
     ha publicado todavía las cuotas de la jornada 10, que empieza el 23-oct. La puerta de
     frescura de `build_scoring`, en cambio, **ya pasa**: todos los bloques a 0 días desde
     su última escritura. **Esto ya no es un bloqueo**: desde el 22-oct el precio se puede
     teclear, y la plantilla de la jornada 10 está escrita y esperando (ver el apartado
     siguiente).

   Dos consecuencias de estado que hay que tener presentes. `outputs/web/` sigue siendo la
   publicación del 19-oct (`run_code 7f228a4bbd64`, 78 partidos en 2026/27) mientras
   `data/02_processed/` ya va por 3889: **la app sirve las cifras de la semana pasada hasta
   que la cadena se complete**, que es justo lo que `sincroniza_app` existe para que no pase
   en silencio. Y los recuentos que crecerán al publicar son los cuatro de siempre:
   `matchlogs` 5476 → **5498** (once partidos por dos ejes), `economia_jornada` 610 →
   **614**, `predicciones` 3118 → **3129** y `perfiles_equipo`; `metricas_temporada` se
   queda en **53**, como debe.

   **Un detalle cosmético medido el mismo día**, por si reaparece: `build_scoring` descarta
   «por ANTIGUOS» el `2026-10-10 Rayo–Athletic` y aconseja reejecutar la consolidación del
   maestro. El consejo no aplica a ese partido —consta `canceled`, así que no se consolidará
   nunca— y el aviso se repetirá cada semana sin significar nada.
4. **`build_models.py --holdout`**, al final del todo y una sola vez.
5. **Idea de app todavía sin definir.** Manuel quería añadir una pieza nueva al
   Streamlit; la favorita del cribado era un **módulo de scouting y similitud de
   jugadores**. Nunca se concretó.

## El precio de la jornada por jugar se puede teclear (22-oct-2026)

Lo pidió Manuel: una opción para **meter a mano las cuotas de la jornada siguiente** al
ejecutar el pipeline, cuando football-data no las publica, y que esas cuotas **prevalezcan
después en el histórico** para que la predicción emitida y el registro no se contradigan.

**La mitad de atrás ya existía y era inútil, y el motivo es el que hay que recordar.**
`_apply_manual_odds` y `_write_odds_template` estaban escritos desde el principio, pero el
docstring de `build_scoring` declaraba que **«la ÚNICA fuente de qué partidos vienen es
football-data (`fixtures.csv`)»** y el aborto por `up.empty` está **antes** de la puerta de
cuotas, que es quien escribe la plantilla. Como football-data sirve fixture y precio juntos
o ninguno, el caso que motiva el mecanismo —no hay precio— era exactamente el caso en el que
el mecanismo no llegaba a ejecutarse. Comprobado: nunca se había escrito una plantilla.

El circuito son tres piezas y solo la segunda pide teclado:

1. **Siembra** — `_seed_from_sofascore`: el calendario de Sofascore conoce la jornada con
   semanas de antelación, ya es dependencia de la cadena y ya se leía aquí para la puerta de
   frescura. Aporta el fixture **sin cuota**, con `fixture_src` declarado. Recorta a la
   **primera** jornada que viene con `_solo_proxima_jornada` / `_GAP_JORNADA = 3`, el mismo
   criterio y valor que `protocol._GAP_DIAS`. **Ese recorte no es cosmético**: `fixtures.csv`
   se autolimita a ~7 días, así que `_HORIZON_DAYS = 10` nunca abarcó dos jornadas, pero
   Sofascore conoce la temporada entera y sin recorte sembró **19 partidos en vez de 10** —y
   los nueve de más no tienen mercado abierto en ninguna casa: teclear su cuota sería
   inventarla, y Manuel pidió «la jornada futura siguiente»—.
2. **Plantilla** — la puerta de cuotas escribe
   `data/01_raw/footballdata/footballdata_upcoming_manual_odds.csv` y lanza
   `MissingOddsError`. Se rellena `odds_h/odds_d/odds_a` y se reanuda con
   `rutina_semanal.py --desde predict`. Re-ejecutar **no borra lo tecleado**.
3. **Prevalencia** — `promote_manual_odds` guarda lo tecleado con su sello en
   `footballdata_odds_manual.csv`, y `footballdata.apply_manual_odds`, enganchado en
   `load_processed`, lo vuelve a aplicar en cada reproceso del histórico. **Artefacto propio
   y no edición del CSV maestro a propósito**: `_merge_upsert` funde con `keep="last"`, de
   modo que la siguiente descarga de football-data pisaría cualquier valor escrito a mano.

**El alcance es una decisión con criterio, no una omisión, y no la amplíes sin releer esto.**
El override toca **exclusivamente `odds_avg_h/d/a`**, el canal de entrada del modelo, de
donde salen `mkt_p_home/draw/away`. **No toca `odds_b365_*`, `odds_ps_*`, `odds_max_*` ni
ninguna columna de cierre**, por la regla 3: las de Bet365 son el listón de toda cifra
económica, y una cuota escrita a mano ahí haría que la pregunta del TFM se contestara contra
un precio que no publicó ninguna casa. Y hay un argumento más simple: cuando football-data
publica el partido ya jugado trae la cuota de **cierre**, mejor dato que la apertura que se
vio al teclear, así que pisarla degradaría el histórico. **Lo que prevalece es con qué
precio PREDIJO el modelo; lo que se conserva es a qué precio se LIQUIDA.** Dos preguntas
distintas, y el override solo contesta la primera.

**El límite que lo hace defendible es la regla 6 escrita en código**: se aplica solo si
`entered_ts` es anterior al día del partido. Sin eso nada impediría «corregir» el precio de
un partido de 2019 y mejorar el backtest de un bloque cerrado. Con eso el override queda
confinado por construcción a la temporada en curso. Por lo mismo, la promoción conserva la
entrada **más antigua** (`keep="first"`), al contrario de un upsert corriente: pisar el sello
con el de una reejecución posterior al pitido final haría que el propio límite rechazara la
fila y el override desaparecería en silencio justo cuando ya era historia.

Las dos vías son **indistinguibles aguas abajo** —misma `_implied_probs`, mismo rechazo de
márgenes imposibles— y por eso la procedencia se declara: `odds_src` vale
`"footballdata"`/`"manual"` en el histórico y `"auto"`/`"manual"` en el informe de emisión.

**Y de paso salió un fallo terminal que la pasada en seco no podía ver**, porque `--dry` no
escribe: `build_scoring.main(save=True)` **no podía escribir `features_upcoming.parquet`
nunca**. `out.attrs["cobertura"]` es un DataFrame y pandas vuelca `attrs` a los metadatos del
parquet como JSON, de modo que la escritura abortaba con «Object of type DataFrame is not
JSON serializable». Es el **mismo fallo que `models/build_models._escribe` ya evitaba**, y
aquí no se había visto porque la emisión nunca había llegado a escribirse: la puerta de
cuotas abortaba antes. Está en la ruta crítica exacta de lo que Manuel pidió. Corregido con
`out.attrs.clear()` después de cosechar `cobertura` y `cuotas_manuales`.

Medido de punta a punta con una plantilla de usar y tirar, para no tocar la real: una fila
sembrada desde Sofascore atraviesa la construcción entera —**10 filas × 131 columnas**, todas
las puertas pasadas incluida `_coverage_gate`, 10 filas promocionadas con su sello—, que es
un camino que **nunca se había ejercitado**. Batería propia en
`scripts/features/test_manual_odds.py`: **30 comprobaciones** en cuatro bloques (siembra,
aplicación, prevalencia y la juntura), con las de b365-intacto, cierre-intacto y
sello-posterior-rechazado como las que guardan la decisión de alcance. `pytest -q` sube de
184 a **185 pruebas** en **36 ficheros**, salida 0.

**La plantilla real de la jornada 10 está escrita y vacía** —10 partidos del 23 al 26-oct— en
`data/01_raw/footballdata/footballdata_upcoming_manual_odds.csv`. Rellenar `odds_h/odds_d/
odds_a` y reanudar es lo único que separa el estado actual de una predicción publicada.

## El modelo publicado, medido (19-oct-2026)

El modelo publicado es la **mezcla logit** de `estimators.mezcla`: dos ajustes separados
—el precio y el juego— combinados como `p ∝ p_mercado^0.85 · p_juego^0.15` sobre las
**62 columnas** declaradas en `build_models.CONJUNTO` (las 3 de `sl.SEMILLA_MERCADO` más
las 59 de juego). Se adoptó al levantar Manuel la regla 4, y **su precio en RPS está
declarado**: 0.000527 peor que el modelo de solo precio al que sustituye.

**Son 62 pedidas y el modelo ajusta 61**, y por eso las tablas de interpretabilidad
suman 61: las tres probabilidades del modelo de goles suman uno, así que la poda por
colinealidad del preprocesado retira una de las tres en cada pliegue. Medido sobre los
255 pliegues de los dos regímenes, cae `strength_p_home` en 208 y `strength_p_away` en
los 47 restantes. **Las tres se declaran a propósito**: quitar la que cae más veces
dejaría 60 columnas en los 47 pliegues donde la poda elige otra. `conjunto_declarado`
cuenta lo pedido y su columna `presente` vigila la matriz de features, no la poda de un
pliegue; no son la misma pregunta y no tienen por qué dar el mismo número.

Dos fallos de la primera ejecución real que la pasada en seco **no podía ver**, porque
`--dry` no escribe, y que conviene no volver a introducir:

- `_escribe` **vacía `attrs`** antes de escribir. `walk_forward` deja en `pred.attrs`
  el `ProtocolConfig` y los estados ajustados, `backtest` los encadena a los libros, y
  pandas vuelca `attrs` a los metadatos del parquet como JSON: la cadena abortaba con
  «Object of type ProtocolConfig is not JSON serializable» en el primer artefacto.
- **La temporada en curso se caía de `metricas_por_temporada` en silencio.**
  `evaluate.compare` mide sobre la intersección de los partidos que cubren *todas* las
  referencias pedidas —correcto—, pero eso convierte una referencia sin cobertura en un
  veto sobre la temporada entera: **Pinnacle cubre 3037 de los 3040 partidos de
  validación y 0 de los 78 de 2026/27**. `_referencias_vivas` descarta la referencia
  muerta, no la temporada; en validación la cifra no se mueve ni un decimal, y la
  invariante 6 de `qa_models` se comprueba ahora también temporada a temporada, que es
  justo lo que esa concesión no debe romper.

Cifras del régimen **semanal** (el que publica), bloque de validación
**2017/18–2024/25**, **3037 partidos** —la intersección de los que cubren a la vez las
tres casas de referencia, de 3040 puntuados—: modelo **RPS 0.194494** [0.18977, 0.19923],
error típico 0.002523 · log-loss 0.977547 · acierto **53.38 %**, muy por encima de la
previa de frecuencias (0.225345 / 44.78 %).

**La comparación contra los precios se reparte, y hay que citarla entera.** En RPS el
modelo **queda por detrás de los tres**: pinnacle 0.193958 le gana por 0.000536, consenso
0.194116 por 0.000378 y b365 0.194237 por 0.000257 (`skill_rps = −0.001947`, relativo al
consenso). En **log-loss**, lo mismo: 0.977547 contra 0.975511, 0.976058 y 0.976742. Lo
que el modelo gana es el **acierto**, y a los tres: 53.38 % contra 53.11 %, 53.08 % y
53.01 %. La tabla publicada pone el modelo en la primera fila **porque ordena el candidato
antes que las referencias, no porque gane**: no la leas como un ranking. Ese reparto es el
precio declarado del criterio de adopción —la familia se eligió por acierto y por dinero,
no por RPS—, y aquí se ve entero.

**El daño en RPS está localizado, y saber dónde cambia la lectura.** Contra el consenso,
el modelo pierde **0.002142 en 2017/18** y **0.002322 en 2018/19**, y empata o gana en
las seis restantes. Son exactamente las dos temporadas sin cuota de cierre, las de precio
más pobre de la serie; **y en las dos el modelo gana en acierto** (55.53 % contra 54.74 %
y 48.95 % contra 48.68 %). Lo que pasa ahí no es que el modelo falle más: reparte peor la
probabilidad entre los tres signos.

**La economía, con la política publicada** (`POLITICA_ECONOMIA = "favorito"`: una unidad
plana al signo del modelo, en todos los partidos con precio, sin umbral), **y son dos
niveles con signos opuestos, no el mismo con otro precio**:

| eje del precio | n | cobertura | modelo | listón (favorito de b365) | diferencia | caída máx (listón) |
|---|---|---|---|---|---|---|
| cuota de cierre promediada | 2280 | 0.75 | **+0.79 %** | −0.04 % | +0.84 pp [−1.17, +3.08] | 37.60 % (41.84 %) |
| consenso del mercado | 3040 | 1.00 | **−0.35 %** | −1.74 % | +1.39 pp [−0.43, +3.33] | **72.61 % (98.51 %)** |

**Ninguno de los dos intervalos excluye el cero.** Margen del libro 4.52 % a cierre y
4.65 % a consenso. Y las dos filas **no son la misma medición**: la de consenso incluye
2017/18 y 2018/19 y la de cierre no puede incluirlas, porque football-data no publica
cuota de cierre antes de 2019/20. **Al entrar esas dos temporadas el nivel cruza a
negativo y la ventaja sobre el listón crece**, lo que suena contradictorio y no lo es:
2018/19 es un año en el que apostar al favorito perdió **13.03 %** y el modelo perdió
**10.53 %**. Ganarle dos puntos y medio a quien pierde trece sigue siendo perder diez. La
caída máxima a consenso es la columna que convierte un −0.35 % en una serie inasumible
—la del listón, 98.51 %, es un apostante que casi se arruina—, y es la razón de no leer
el nivel agregado sin la caída al lado.

**La lectura que hay que repetir siempre que se cite esa cifra.** La ventaja agregada
sale de las tres temporadas que eligieron el peso. Por temporada, la diferencia frente al
listón a cuota de cierre es **+3.20, −0.47 y +2.76 puntos en 2019/20–2021/22** —las que
lo eligieron, media ≈ +1.83— y **−2.79, +1.46 y +0.85 en 2022/23–2024/25** —las que no,
media ≈ −0.16—. Las dos medias coinciden con lo que midió `_politica.py` por separado, lo
que además verifica que el cableado es el que se midió.

**Y el censo de nueve añade dos temporadas citables sin cambiar el veredicto, medido el
21-oct-2026.** 2017/18 y 2018/19 son `confirmacion` —anteriores a cualquier elección, así
que la regla 5 las deja citar— y a consenso dan **+2.66 y +2.50 pp**, por encima de la
media del propio bloque que eligió. Agregando bien, que es por beneficio sobre importe y
no promediando temporadas: **fuera del bloque que eligió el peso, el modelo empata con el
listón y no lo bate**, en los dos ejes. A cuota de cierre son 3 temporadas y 1140 partidos
con **−0.16 pp** de diferencia, 0.05 errores típicos de cero; a consenso son 5 temporadas
y 1900 partidos con **+1.00 pp**, 0.44 errores típicos. El +2.50 de 2018/19 además no es
lo que parece: es ganarle al listón **perdiendo 10.53 %** frente a su 13.03 %.

En curso: **2026/27, 10 pliegues, 78 partidos**, con ROI **+0.18 %** contra **−3.24 %**
del listón a cierre promediado (**+1.26 %** contra −2.41 % a consenso). **No cites ese
+3.42 pp de ventaja sin lo que viene detrás**: el error típico del ROI sobre 78 partidos
es **10.93 pp**, medido sobre `matchlogs.parquet`, contra **2.08 pp** sobre los 2280 de
validación a cuota de cierre y **1.79 pp** sobre los 3040 a consenso. El nivel está a dos
centésimas de error típico de cero y la ventaja a tres décimas. Son pocos partidos para
una métrica estable, y «pocos» aquí quiere decir que la cifra no distingue un modelo
bueno de uno malo. **Esos «pliegues» son la jornada
EFECTIVA, no la nominal de Sofascore**: `protocol.effective_matchday` abre jornada nueva
cuando un partido se separa más de `_GAP_DIAS = 3` del anterior, porque la `round` nominal
no sirve como unidad de avance —un aplazamiento rotulado «jornada 5» y jugado en febrero
arrastraría hacia atrás todo el entrenamiento—. Por eso las **ocho** jornadas de Primera
consolidadas caen en **diez** pliegues, y por eso la columna `jornada` de los parquets
publicados no cuadra con la `round` del maestro: no son la misma cosa y
`protocol.matchday_report` existe para contrastarlas. **2025/26 no aparece a propósito**
(regla 7).

**Dos cosas mueven estas cifras y conviene no confundirlas, porque una es rutina y la
otra es una decisión.** Consolidar una jornada nueva solo puede mover el renglón de la
temporada en curso, que vive en `produccion`: el bloque de validación no la contiene, así
que sus cifras tienen que salir idénticas al sexto decimal. **Si al meter una jornada se
mueve otra cosa, no es la jornada: es el modelo.** Lo que sí mueve el bloque entero es
**cambiar el censo**, y es lo que hace que la validación sea de 3040 partidos y no de
2280: el arranque del protocolo gasta una sola temporada
(`seleccion.WARMUP_CONFIRMACION`), lo que mete 2017/18 y 2018/19 dentro de lo puntuado.
Esa ampliación es la que invierte el veredicto de RPS de arriba, y está medida: las dos
temporadas que entran son las dos donde el modelo pierde RPS contra el consenso.

Los dos regímenes dan la misma cifra hasta el cuarto decimal, incluido el desglose por
temporada, y eso es correcto por construcción. Medido el 19-oct-2026 sobre las 62
declaradas: **ninguna de las cinco columnas que solo existen en `retrospectivo`**
—`close_move_fav`, `flag_no_closing_odds`, `sharp_div_*`— está en el conjunto, porque el
conjunto se declaró desde el banco **semanal**; y de las 61 que viven en las dos matrices
de preprocesado, las 61 son **idénticas bit a bit**. Sin una sola columna que las
distinga, el techo retrospectivo colapsa sobre el semanal. Que el conjunto haya pasado de
3 columnas a 62 no cambia el argumento, pero sí lo cambiaría meter una columna de
movimiento de línea: entonces hay que volver a medirlo, no volver a afirmarlo.

**La cadena entera está ejecutada y auditada sobre la mezcla** (19-oct-2026): los
dieciséis artefactos escritos en los dos regímenes, `qa_models.py` en verde con las
**nueve invariantes duras** en los dos, y `publish_web.py` reescribiendo los catorce de
`outputs/web/`. La ficha declara `conjunto.n_variables: 62` y la familia con su peso.

`qa_models.py` y `publish_web.py` se estrenaron el 14-oct-2026, y cada uno traía un fallo
que solo podía aparecer al ejecutarlo:

- **`qa_models` enumeraba los regímenes listando subcarpetas de `outputs/models/`**, y
  ahí ya no vive solo lo publicado: están `laboratorio/` y `seleccion/`. Auditaba dos
  regímenes que no existen y abortaba con «falta el manifiesto» mientras `semanal` y
  `retrospectivo` pasaban las nueve invariantes. Ahora la lista sale de `columns.REGIMES`
  —la misma que recorre `build_models`— intersecada con lo que hay en disco, para que
  publicar un solo régimen siga siendo legítimo.
- **La ficha publicaba `variables.n_features: 0`** tres líneas por encima de
  `conjunto.n_variables`. Sin jornada pendiente no hay estado de preprocesado cargado, y
  el cero afirmaba que el modelo no usa ninguna variable justo al lado de la lista de las
  que usa. Ahora va `null`: no es lo mismo «no contado» que «vacío». Es lo que hoy
  publica la ficha, porque no hay jornada pendiente.

`outputs/web/` tiene los **quince** ficheros históricos —trece parquets y dos JSON,
**1 287 006 bytes**— y el traslado a la app añade el recibo `sincronizacion.json`, de modo
que allí son dieciséis. Recuento del 21-oct-2026, sobre el censo de nueve temporadas:
`metricas_temporada` **53** filas, `perfiles_equipo` **7756**, `economia_jornada` **610**,
`matchlogs` **5476**, `predicciones` **3118 × 24**, `economia_temporada` **16**,
`clasificacion_por_clase` **50**, `confusion` **90**, `fiabilidad` **400**, `roc_ovr`
**10**. Los cinco de la emisión faltan, y el `manifiesto_web.json` lo declara:
`emision.estado = "sin jornada pendiente"`.

**De esos recuentos, los únicos que se mueven al consolidar una jornada son los que
incluyen la temporada en curso**: `perfiles_equipo`, `economia_jornada`, `matchlogs` y
`predicciones`. `metricas_temporada` **no se mueve**, porque la temporada en curso ya
tiene su fila; si cambia el 53, ha cambiado el reparto de bloques y no la jornada.
`matchlogs` son 5476 filas para 3118 partidos y no el doble, porque la fila va **por
partido y por eje de precio** y 2017/18 y 2018/19 solo existen a consenso: football-data
no publica cuota de cierre antes de 2019/20. **El doblado es asimétrico**, y una vista sin
filtrar por eje no infla todas las temporadas por igual.

## El origen rodante, la undécima ruta y la que contesta la directriz D (20-oct-2026)

Manuel pidió el 20-oct **«el mejor modelo que mejore las métricas de ROI y accuracy en
todo el histórico de partidos desde 17/18 hasta hoy sin excepción»**. Las diez rutas
anteriores eligen un conjunto con 2019/20–2021/22 y lo puntúan fuera, así que dejan tres
temporadas sin cifra; ampliar el bloque de elección arreglaría la cobertura y rompería la
regla 5. Las dos exigencias solo son compatibles eligiendo un **procedimiento** y no un
conjunto: para cada temporada S se repite la búsqueda **con solo partidos anteriores a
S**, y con lo que salga se predice S. Lo hace `_rodante.py` —de la raíz, nueve orígenes,
dos criterios, 4.2 h de búsqueda— y su docstring lleva el detalle. Está en **C.9.6** del
cuaderno 05 y en el README; la décima, que es partir el censo en dos y de donde sale el
cuadro de ganadores que esta ruta rodante generaliza, en **C.9.5**.

**El veredicto, censo limpio** (las seis temporadas que no eligieron el peso de la mezcla
publicada, 1978 partidos, el único censo donde las cuatro columnas se comparan):

| pista | acierto | RPS | ROI b365 | ventaja sobre el listón |
|---|---|---|---|---|
| listón (favorito de b365) | 53.54 % | — | −2.78 % | — |
| semilla (3 de precio) | 53.44 % | 0.193962 | −2.97 % | −0.19 pp [−1.53, +1.20] |
| publicado (mezcla, 62) | 53.84 % | 0.194437 | −1.60 % | **+1.18 pp [−0.84, +3.33]** |
| rodante por acierto | 53.49 % | 0.194118 | −2.64 % | +0.14 pp [−1.64, +1.93] |
| rodante por ventaja | 53.59 % | 0.194335 | −1.96 % | +0.82 pp [−1.35, +3.01] |

**Ni un intervalo excluye el cero, ninguno de los cuatro**, y sobre los 3118 enteros
tampoco: el mayor es el de la mezcla publicada, +1.52 pp [−0.19, +3.31]. De las **doce
comparaciones emparejadas** entre pistas (seis pares en los dos censos) **solo una excluye
el cero**, `publicado` contra `semilla` sobre los 3118: +1.53 pp [+0.06, +3.03] —y en el
censo limpio ya no—. Esa es la respuesta honesta a la directriz: **cubiertas las nueve
temporadas sin excepción, ninguna ruta del proyecto produce una ventaja distinguible de
cero contra el favorito de Bet365, y la mejor de las cuatro sigue siendo la mezcla que ya
se publica.** Lo que la ruta rodante aporta no es un modelo mejor: es la primera cifra del
trabajo que **cubre las nueve temporadas sin que ninguna haya participado en elegir lo que
la predice**, y por tanto la primera citable entera. **No sustituyas el modelo publicado
por una pista rodante**: pierde, y además no hay nada que ganar.

Cuatro cosas de ese cuadro que no hay que volver a medir, y la primera es una corrección:

- **Predije que la pista de ventaja quedaría por debajo de la de acierto y queda por
  encima.** El argumento era que su tolerancia es casi tres veces más ancha (6.8e-3 contra
  2.5e-3) y compra ruido. Adopta más profundo, sí, pero da +0.82 pp contra +0.14 y más
  acierto. **Lo que cuesta está en el RPS** —0.194335 contra 0.194118, 0.000217 peor—, que
  es exactamente lo que la regla 4 obliga a seguir reportando aunque ya no arbitre.
- **Las dos pistas rodantes difieren en 91 partidos de 3118**, el 2.9 %; emparejadas, la
  diferencia es +0.36 pp [−1.09, +1.79]. Las cuatro pistas coinciden en el signo en más
  del 95 % de los partidos, así que **las cifras marginales de la tabla exageran las
  distancias** y la comparación que vale es la emparejada.
- **El precio vale más que el conjunto, medido ya en las nueve temporadas.** La misma
  pista liquidada a la mejor cuota entre casas en vez de a Bet365 pasa de −2.05 % a
  **+2.42 %**, y el listón de −2.87 % a +1.93 %: **4.5 puntos de ROI por elegir dónde se
  apuesta, contra 1.5 del mejor conjunto de once rutas de búsqueda.** No promete nada —hay
  que poder tomar ese precio en 3118 partidos— pero dimensiona el resto.
- **La celda más bonita es la única incitable**: la pista de acierto en 2026/27 da +6.18 %
  de ROI y +8.21 pp de ventaja, y son 78 partidos con un error típico de 10.93 pp. Está a
  **0.75 errores típicos de cero.**

Y un hallazgo lateral que va contra la directriz que lo produjo, de `_estabilidad.py`
(añádelo a la lista de guiones de sonda): **el orden de las candidatas por RPS es más
estable entre orígenes que el orden por acierto**, y la brecha se abre con la distancia
—+0.588 contra +0.371 de rho de Spearman en el par más lejano—. No cambia quién elige, que
lo fijó Manuel, pero es el mecanismo por el que una búsqueda guiada por acierto compra
daño en RPS sin anunciarlo.

## El máximo por ROI y acierto sobre el censo de nueve (21-oct-2026)

Manuel pidió **«el mejor modelo en términos de roi y accuracy… que maximice esas métricas
tanto en cómputo global como a nivel de temporada»** sobre el censo de nueve ya fijado. Lo
contesta `_nueve.py` —50 conjuntos distintos de las once rutas, los 345 pliegues, 24.3
min— y su docstring lleva el detalle. No es una ruta de búsqueda duodécima: no busca
nada, puntúa el acervo que las once dejaron. Está en **C.9.7** del cuaderno 05 y en el
README. **El resultado es que no hay conjunto que adoptar**, y las tres lecturas lo dicen
con tres fuerzas distintas.

**Global.** El máximo lo firma `maximo_roi` (k=10): acierto **54.11 %**, ROI **+1.35 %**,
ventaja **+4.22 pp [+1.72, +6.77]** sobre el listón de los 3118 (−2.87 %, 53.11 %). Ese
intervalo excluye el cero y **es exactamente por eso por lo que no es citable**: el
conjunto se buscó sobre partidos que están dentro de estos 3118, así que la cifra arrastra
el defecto de `_maximo.py` en vez de repararlo. El publicado de 62 queda en 53.50 % y
+1.52 pp [−0.19, +3.31].

**Meter el banco entero cuesta acierto y RPS, y no cuesta ventaja, y no hay que
confundirlo.** `todas65` cae a **50.58 %** de acierto y 0.202736 de RPS, y `juego59` a
**50.03 %** y 0.204590; pero sus ventajas sobre el listón siguen saliendo **positivas**
(+1.66 y +0.25 pp) y la peor de las 50 es `semilla+fuerza` con **−0.93 pp**. El ROI plano
sobre 3118 partidos tiene un error típico de 1.79 pp y **no distingue un modelo que
acierta el 50 % de uno que acierta el 53.5 %**. El daño existe y está medido; quien lo
mide es el acierto, no el dinero. **De ahí que la directriz pida dos métricas y haya que
seguir reportando las tres.**

**Y el censo LIMPIO no limpia lo que se buscó.** Son las seis temporadas que no eligieron
el peso de la mezcla, así que es censo limpio para `publicado` (+1.18 pp [−0.84, +3.33]) y
para `semilla` (−0.19 pp), y **no lo es para ningún conjunto de búsqueda**: `maximo_roi`
se buscó sobre 2019/20–2024/25 y pisa tres de las seis, y `rod_ve_202425` —que ahí firma
el máximo con +3.61 pp [+1.35, +5.97], intervalo que excluye el cero— se eligió con el
pasado de 2024/25, que contiene cuatro de las seis. **Ese +3.61 no es una cifra de fuera
de muestra**; las únicas de fuera de muestra del cuadro son las dos del censo limpio para
el publicado y la semilla, y la lectura 3.

**Por temporada no hay respuesta que dar, y eso es un hallazgo y no una evasiva.** Las
nueve temporadas las firman **nueve conjuntos distintos, en los dos criterios, y ninguno
repite**. El puesto medio del ganador en las otras ocho va de **12.2** a **40.6** de 50
—`escalera+clasificacion` gana 2023/24 con .5605 y promedia .5110 fuera—. Nueve máximos
sobre 380 partidos con un error típico de 5.0 pp son nueve sorteos: no existe «el conjunto
que maximiza cada temporada», existe el que ganó ahí.

**Dejar una fuera es la única lectura citable, y no excluye el cero.** Elegido con ocho
temporadas y puntuado en la novena: por acierto, 4 conjuntos distintos y **53.30 % contra
53.11 %**, ventaja **+1.46 pp [−0.42, +3.34]**; por ventaja, 3 conjuntos y **53.11 %
contra 53.11 %** —el mismo acierto al decimal— con **+1.45 pp [−0.84, +3.69]**. **Los dos
quedan por debajo del +1.52 pp del publicado**, y los vaivenes por vuelta dicen de dónde
sale la media: +8.41 pp en 2019/20 contra −4.55 pp en 2026/27.

**Y la voraz con los 3118 dentro del bloque de elección se agota en dos variables**, que
es el máximo de información que el proyecto puede darle. Para en la tolerancia y no en el
techo, a **k=5 por los dos criterios**: por acierto la mejor candidata del paso 3
(`form_ga_ewm_diff`) mejoraba **+0.000000** contra 0.00153093; por ventaja, +0.003211
contra 0.00416413. **No sustituyas el modelo publicado por nada de este cuadro.**

## Entorno y ejecución

Python 3.12, `pip install -r requirements.txt`. Las bibliotecas de scraping se importan
de forma perezosa dentro de las funciones que las usan, para que la lógica pura se pueda
validar con solo `pandas`, `numpy` y `pyarrow`.

```bash
python -m venv .venv && .venv/Scripts/activate
pip install -r requirements.txt
pytest -q                                      # testpaths = scripts
python scripts/rutina_semanal.py --listar      # el plan de la jornada, sin ejecutar
python scripts/rutina_semanal.py --dry         # ejecuta y reporta sin escribir
```

Las baterías viven junto al módulo que validan (`scripts/**/test_*.py`), no en un árbol
`tests/` aparte: se leen como el guion de comprobación de su propio módulo.

**Un guion de la raíz importa los módulos de modelado PLANOS, nunca por el paquete.**
`import laboratorio as lb`, con `scripts/models` metido en `sys.path`, que es como se
importan entre ellos (`seleccion.py` l. 237). Hacerlo por el paquete
—`import scripts.models.laboratorio as lb`— crea un **segundo objeto de módulo con su
propia caché de la matriz**, y entonces `seleccion._lote` ve una matriz que no es la de
su `lb._feat_cache`, concluye que no puede mandársela a un proceso hijo y **cae a la
serie sin avisar de nada**. La guarda hace lo que debe —perder velocidad antes que medir
otra cosa—, pero el síntoma es una búsqueda tres veces más lenta sin un solo mensaje:
medido, un paso de doce candidatas cuesta 42.2 s por la ruta del paquete y 29.8 s por la
plana. Si un guion de la raíz tarda mucho más de lo que su cuenta dice, **mira primero
cómo importa**, y confírmalo contando procesos: con reparto hay siete `python.exe` y no
uno. Estaba mal en `_rodante.py` y en `_censo.py`, y los dos están corregidos.

Los extractores hacen peticiones en vivo y se ejecutan **siempre en la máquina de
Manuel**. Sofascore atraviesa Cloudflare con una cascada de tres estrategias
(`curl_cffi` impersonando Chrome → espejo `api.sofavpn.com` → Selenium en Chrome real)
que escala sola tras cuatro fallos seguidos. Todo guardado grande usa el patrón
punto de control + escritura atómica + reanudación de `_common.py`, y limpia el atributo
'solo lectura' antes de escribir: el repo vive en el Escritorio, que OneDrive sincroniza
y marca los ficheros como de solo lectura.

## Notas de la migración desde Cowork

- La carpeta de trabajo de Manuel es `C:\Users\admin\Desktop\TFM 2026\TFM`; este repo
  está en `repos/laliga-predictor-engine`. Junto a él, `repos/laliga-predictor-portfolio`
  (app Streamlit) y las carpetas `material_*` con el material del máster.
- Esta copia local **no parece ser un clon de git** (no hay `.git`). Antes de hablar de
  ramas o commits, confírmalo.
- El antiguo entorno tenía un bug de montaje que servía ficheros `.py` truncados y
  obligaba a validar la lógica copiándola a scripts nuevos. **En Claude Code eso no
  aplica**: el sistema de ficheros es el real y los `pytest` se ejecutan de verdad.
  Si lees en el histórico avisos sobre "mount truncado", ignóralos.
