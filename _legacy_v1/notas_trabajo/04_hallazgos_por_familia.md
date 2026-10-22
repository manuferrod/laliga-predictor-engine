# Análisis exhaustivo por familia de features — hallazgos para el 04

**Alcance:** solo `dev` (3.420 partidos, 2016/17–2024/25). El holdout 25/26 sigue sellado.
**Métrica de señal:** información mutua (MI) entre cada feature discretizada (10 bins por cuantiles) y el target `y_num` (H=0, D=1, A=2). Es una medida *marginal* (una feature a la vez), no captura interacciones, pero sirve para rankear señal cruda y detectar peso muerto.
**Baseline a batir (mercado):** log-loss 0,9677 / accuracy 53,7 %.

---

## Ranking global (top señal marginal)

| # | Feature | Familia | MI | Cobertura |
|---|---------|---------|-----|-----------|
| 1 | `odds_close_max_h` | mercado | 0,106 | 66,7 % |
| — | (bloque de cuotas de cierre) | mercado | 0,103–0,106 | 66,7 % |
| 8 | `odds_b365_a` (apertura) | mercado | 0,102 | 100 % |
| — | `mkt_p_home` / `mkt_p_away` | mercado | 0,101 | 100 % |
| 19 | `squad_value_diff` | valor | 0,092 | 100 % |
| 20 | `elo_diff` = `elo_exp_home` | elo | 0,089 | 100 % |
| 22 | `squad_value_logratio` | valor | 0,087 | 100 % |
| 23 | `cum_xgd_pg_diff` | forma | 0,073 | 97,3 % |
| 24 | `cum_gd_pg_diff` | forma | 0,066 | 97,3 % |
| 25 | `points_above_drop_diff` | clasificación | 0,065 | 97,0 % |

**Mejor MI por familia:** mercado 0,106 · valor 0,092 · elo/derivadas 0,089 · forma 0,073 · clasificación 0,065 · bajas 0,052 · h2h 0,033 · fatiga 0,010 · psico 0,008.

**Lectura de tesis:** hay una jerarquía nítida de tres capas. (1) El **mercado** domina —ninguna feature propia se le acerca—; es el número a batir, no un input inocente. (2) Los **priores de fuerza estructural** (valor de plantilla, Elo) y la **forma/clasificación acumulada de temporada** forman la capa media aprovechable. (3) **Fatiga, h2h y psico** son casi ruido a nivel marginal. Esto no significa descartarlas (pueden aportar en interacciones o en la cola de la distribución), pero justifica no sobre-ponderarlas.

---

## Patrón transversal nº1: el diferencial vale ~2× que los niveles crudos

En todas las familias con versiones `home_*`, `away_*` y `*_diff`, la columna diferencia casi duplica la MI de cualquiera de sus componentes:

| Base | home | away | **diff** |
|------|------|------|----------|
| `form_gd_5` | 0,020 | 0,021 | **0,040** |
| `form_xgd_5` | 0,020 | 0,025 | **0,048** |
| `cum_points_pg` | 0,030 | 0,030 | **0,058** |
| `form_rating_5` | 0,028 | 0,028 | **0,055** |
| `prev_position` | 0,031 | 0,035 | **0,060** |

**Implicación para el 04:** el modelo quiere el *contraste* local–visitante, no los dos niveles absolutos. Los pares `home_*`/`away_*` son en gran medida redundantes una vez tienes el `_diff`. Candidato claro a reducción de dimensionalidad (quedarnos con el diff, o con diff + un nivel de referencia).

## Patrón transversal nº2: lo esperado (xG) > lo realizado (goles)

Dentro de forma, las features derivadas de xG baten sistemáticamente a sus equivalentes de resultado: `cum_xgd_pg_diff` (0,073) > `cum_gd_pg_diff` (0,066); `form_xgd_5_diff` (0,048) > `form_gd_5_diff` (0,040). Y lo **acumulado de temporada** (`cum_*`) bate a la **forma reciente de 3–5 partidos** (`form_*_3/5`). Dos mensajes de tesis: (a) la inversión en xG (Understat) está justificada empíricamente; (b) la "forma reciente" es más ruidosa que la acumulación larga —cuidado con narrativas de rachas cortas.

---

## Familia a familia

### Mercado (36 cols) — el techo de referencia
El bloque más fuerte y el rival a batir. Las **cuotas de cierre** son marginalmente más nítidas que las de apertura (0,106 vs 0,102): existe *closing-line value*, pero pequeño. Problema estructural clave: **las cuotas de cierre y derivados (`line_move_*`, hándicap asiático de cierre, over/under de cierre) solo existen desde 2019/20** → 66,7 % de cobertura, 100 % NaN en 2016/17–2018/19. Es el hueco dominante del dataset. Overround medio 4,67 % con tendencia a estrecharse (mercado cada vez más eficiente). Fuentes de cuotas (B365/Max/Avg/PS) redundantes entre sí a >0,99.
**Decisión 04:** cómo tratar el hueco de cierre 16–19 (imputar / flag de faltante / restringir esas features a post-2019). Y decidir si el mercado entra como feature o se reserva como baseline/benchmark (riesgo de que el modelo solo aprenda a copiar la cuota).

### Valor de plantilla (9 cols) — el mejor prior no-mercado
`squad_value_diff` (0,092) es la feature propia más fuerte de todo el dataset, por encima de Elo. `squad_value_logratio` (0,087) casi igual. `age_diff` en cambio es débil (0,012). El valor de mercado infla con el tiempo (202 M€ media 16/17 → 287 M€ 24/25) → hay **drift de nivel** que el `_diff`/`logratio` neutraliza (bien diseñado).
**⚠️ Decisión abierta D.0 (leak):** el valor se une a la *misma* temporada del partido (sin lag), y el scraper de Transfermarkt refresca en vivo intra-temporada (mercado de invierno + revalorizaciones). En v1 se laggeaba t−1. En el holdout 25/26 sería "valor vivo" → lookahead leve pero real. **Sin resolver — a decidir contigo.** Mi recomendación: laggear a t−1 tratando el cold-start de ascendidos.

### Elo / derivadas (6+5 cols)
`elo_diff` (0,089) es un prior sólido. **Hallazgo de redundancia:** `elo_exp_home` es una transformada determinista de `elo_diff` (MI idéntica, r=1) → aporta cero información nueva; es la probabilidad esperada logística, no una feature independiente. Su aparente "mala calibración" (predice 0,45 pero se observa 0,29 de victoria local) es porque mide *expected score* del formato Elo, **no** P(victoria local) — no es un bug, es semántica. Ojo si alguien lo interpreta como probabilidad.

### Forma (146 cols) — la familia grande, muy redundante
La más numerosa. Top: `cum_xgd_pg_diff` (0,073). **Alta colinealidad interna: 11 pares con |r|>0,90** entre los 48 features `_diff`, p.ej. `xg`≈`npxg` (0,96, el penalti pesa poco), `xgd`≈`xpts` (0,97, xPts es función de xGD), `gd_6`≈`gd_hybrid` (0,97). Hay margen amplio de reducción (quedarse con un representante por bloque, o PCA). En la cola, los **residuos de finalización/defensa** (`fin_resid`, `def_resid`) tienen MI ~0,003 —capturan suerte/varianza, no señal—; `matches_prev_diff` idem. Coberturas <95 %: `big_chances` (~82 %, es Understat, falta pre-2017) y `venue_form_5` (94,7 %, cold-start de 5 partidos como local/visitante).

### Clasificación (11 cols) — señal alta y consistente
Segunda MEDIANA de MI más alta tras el mercado. `points_above_drop_diff` (0,065) y `prev_position_diff` (0,060) están entre las features propias más fuertes del dataset. `position_zone` es **categórica** (descenso/mid_table/champions/europa) → decidir encoding en 04 (ordinal tiene sentido). Punto débil: `dynamic_pos_change` (73,5 % cobertura, diff solo 61,9 %, MI ~0,004) —cold-start de snapshots, 0 % en 2016/17— candidato a descartar o aceptar como débil.

### Bajas (18 cols) — solo el dinero pesa
Hallazgo limpio: **los conteos de lesionados no sirven** (`n_injury`, `n_missing`, `n_out`… MI ~0,003), pero **la baja ponderada por valor de mercado sí** (`out_value_diff` MI 0,052, ~15× los conteos). Perder un crack de 100 M€ ≠ perder un suplente: la señal está en el euro, no en el número de bajas. Valida la decisión de diseño de ponderar por valor. Matiz: `out_value_diff` (absoluto) bate a `out_value_pct` (relativo al valor del club) → parte de su señal puede ser proxy del tamaño del club (a controlar). Cobertura ~89 %; **0 % en 2016/17** (no había datos de bajas). Los conteos son candidatos a poda.

### Fatiga (12 cols) — débil y contraintuitiva
La familia (junto a psico) más floja a nivel marginal (mejor MI 0,010). **Hallazgo contraintuitivo:** el local que ha jugado MÁS partidos en 7 días GANA MÁS (38,5 % con 0 → 46,4 % con 1 → 54,1 % con 2). No es que la fatiga ayude: es confusión —jugar 2 partidos en 7 días implica competición europea entre semana, que solo tienen los clubes grandes—. La congestión correlaciona con calidad de plantilla; el efecto fatiga puro queda enmascarado. Buen caso de *confounding* para discutir. `rest_days` llega a 101 días: son inicios de temporada (75 de 92 casos son jornada 1) y el parón COVID de 2019/20 (jornada 28, marzo–junio 2020) — outliers estructurales, no errores.

### Head-to-head (21 cols) — masivamente sobre-parametrizada
De 21 columnas, los grados de libertad reales son ~6–7. **2 columnas constantes** (`h2h_draw_rate_ewm_diff`, `h2h_draw_rate_roll8_diff`) valen 0 siempre (un empate es empate para ambos → su diff es 0) → peso muerto, tirar. **Redundancia perfecta (r=1,0):** los 6 pares conocidos (home_win==away_loss, etc.) **más** el trío `gd_ewm` que colapsa a un solo grado (home==away==diff) y los pares win_diff==−loss_diff. Las tasas de **empate** no aportan señal (MI ~0,003) mientras win/loss/gd sí (0,02–0,03) → coherente con que el empate es la clase más difícil. `gd_ewm` tiene 88 % cobertura (cold-start de emparejamientos nuevos/ascendidos).

### Psico (7 cols) — hipótesis folk no soportada
La más débil (mejor MI 0,008). Las binarias `post_big_win` (resaca tras golear) y `post_big_fav_loss` (reacción tras caer siendo favorito) tienen MI ~0,003: **las narrativas psicológicas de rebote/complacencia no muestran asociación medible con el 1X2 en LaLiga 16–25.** `relative_perf` es casi continua (3.362 valores) pero débil, y —único caso del dataset— su `_diff` rinde PEOR que los niveles crudos (0,003 vs 0,008), señal de que probablemente es ruido.

---

## Implicaciones concretas para el 04 (preprocesado)

**Poda / peso muerto (candidatos a tirar):**
- 2 columnas h2h constantes (draw_rate diffs).
- 1 de cada par h2h perfectamente colineal (6 pares + colapso de `gd_ewm` a 1 grado).
- Residuos de forma `fin_resid`/`def_resid` y `matches_prev_diff` (MI ~ruido).
- Conteos de bajas crudos (`n_*`) si nos quedamos con `out_value_diff`.
- `elo_exp_home` (duplica a `elo_diff`).
- Evaluar `dynamic_pos_change` (cobertura baja + MI ~0).

**Colinealidad a resolver:** 11 pares de forma con |r|>0,90 (xg/npxg, xgd/xpts, gd/gd_hybrid…). Decidir representante por bloque o reducción.

**Redundancia home/away vs diff:** considerar quedarnos con los `_diff` como núcleo.

**Missing por causa (imputación con flag, no ciega):**
- Cierre de mercado + derivados: 100 % NaN 16–19 (estructural). Imputar/flag/restringir.
- Bajas: 0 % en 2016/17; ~89 % desde 2017/18.
- Understat (xG, big_chances, ppda, deep): falta pre-2017.
- Cold-starts: `venue_form_5` (94,7 %), `dynamic_pos_change` (~74 %), `h2h_gd_ewm` (88 %).

**Encoding:** `position_zone` (categórica ordinal) → ordinal.

**Escalado:** las magnitudes conviven en rangos muy distintos (Elo ±494, valor en M€, rest_days hasta 101, tasas 0–1) → estandarizar, con `fit` en dev y `apply` a holdout+upcoming.

**⚠️ Decisión de leak sin cerrar (D.0):** lag del valor Transfermarkt. A decidir contigo antes de tocar nada.
