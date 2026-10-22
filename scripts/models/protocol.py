"""Protocolo de validación temporal: pliegues walk-forward por jornada.

Este módulo define **cómo se mide** el sistema, y su única salida es un conjunto
de predicciones fuera de muestra. No conoce ningún estimador concreto —los recibe
como una fábrica— ni calcula ninguna métrica, que es trabajo de `evaluate.py`.
La separación es deliberada: el protocolo es la parte del trabajo que un tribunal
audita primero, y debe poder leerse sin saber qué modelo se entrenó.

Los cuatro bloques del calendario
---------------------------------
La muestra no se parte en dos sino en cuatro, porque las temporadas antiguas, las
recientes y la que se está jugando no admiten la misma disciplina.

    2016/17 – 2018/19   arranque. Carecen de cuota de cierre y de parte médico.
                        Entran en el entrenamiento —alimentan las ventanas de
                        forma, de Elo y de historial— pero no se evalúan: una
                        métrica calculada sobre ellas mezclaría el rendimiento
                        del modelo con la escasez de la fuente.
    2019/20 – 2024/25   validación temporal. Aquí se ejecuta el walk-forward, se
                        comparan modelos y se eligen hiperparámetros. Toda cifra
                        que sirva para decidir algo sale de este bloque.
    2025/26             holdout sellado (`columns.HOLDOUT_SEASONS`). Se puntúa una
                        sola vez, con el modelo ya cerrado, y su resultado se
                        reporta sin reajustar nada después.
    2026/27 →           producción: la temporada EN CURSO, la que el sistema
                        predice cada semana. Se puntúa jornada a jornada con el
                        modelo ya cerrado y da la tercera cifra que el trabajo
                        necesita —cómo va el modelo en la temporada que se está
                        jugando—, pero **no decide nada**.

El cuarto bloque no es un adorno: es el que impide una fuga concreta. La temporada
en curso es posterior al holdout, así que la ventana de entrenamiento de cualquiera
de sus jornadas contiene las 380 filas de la temporada sellada —necesariamente, y
eso está bien: en octubre de 2026 el modelo que de verdad se publica ha visto
2025/26, y negarse a usarla empobrecería la predicción sin ganar nada de método—.
Lo que no puede pasar es que esas predicciones entren en una decisión: si el banco
que compara familias las evaluase, la familia se elegiría en parte mirando
predicciones de un modelo que vio el holdout, y el holdout dejaría de ser una
prueba independiente sin que nada diera error. De ahí que `produccion` sea un
bloque aparte y que la temporada en curso quede FUERA de `validacion`, `seleccion`
y `medicion`.

Conviene no confundir dos actos que la palabra «abrir» mezcla: **usar** 2025/26
como pasado de entrenamiento de 2026/27 es producción corriente, y **puntuarla**
para leer su métrica es el acto único que `build_models.py --holdout` sella. El
primero no consume el segundo.

El bloque de validación se parte además en dos mitades por `CORTE_SELECCION`, y
esa partición es la que sostiene la selección de variables:

    2019/20 – 2021/22   selección. La búsqueda voraz de `seleccion.py` recorre
                        candidatas aquí. Es el bloque cuyas cifras están
                        contaminadas por haber elegido mirándolas, y por eso no se
                        citan.
    2022/23 – 2024/25   medición. El conjunto llega cerrado y se puntúa una vez.
                        Es de aquí de donde sale la cifra que la memoria publica
                        antes de abrir el holdout.

Las dos mitades no son bloques nuevos sino una lectura del mismo desarrollo:
`validacion` sigue existiendo y sigue siendo lo que usa el banco manual para
comparar familias, porque comparar dos familias con el conjunto fijo no elige
variables y no necesita reservar temporadas.

La jornada como unidad de avance
--------------------------------
El protocolo avanza por jornada y no por partido. La razón no es de comodidad:
avanzar partido a partido daría al modelo el resultado de los encuentros del
sábado para predecir los del domingo, que es una ventaja que en producción no
existe salvo que se re-ejecute la cadena a mitad de fin de semana.

Ahora bien, la jornada nominal que publica Sofascore (`round`) no sirve como
unidad, porque un aplazamiento la desordena: un partido de la jornada 5 disputado
en febrero seguiría rotulado como jornada 5 y arrastraría hacia atrás todo el
pliegue. Lo que el protocolo necesita no es el rótulo administrativo sino el
**conjunto de partidos que se puntuarían juntos**, que es exactamente lo que
`effective_matchday` reconstruye: bloques contiguos en el calendario, separados
cuando media un hueco de varios días o cuando un equipo aparecería dos veces en
el mismo bloque. Esa segunda condición es la que garantiza la propiedad que de
verdad importa —dentro de un pliegue ningún equipo juega dos veces—, y por tanto
que la evaluación conjunta no esconda información de un partido en otro.

`matchday_report` contrasta la jornada efectiva con la nominal y deja por escrito
en qué medida coinciden, que es el modo de mostrar que la reconstrucción no
inventa un calendario propio.

La ventana de entrenamiento
---------------------------
Se ofrecen las dos variantes que la memoria compara, porque cuál es mejor no es
obvio a priori y la respuesta es un resultado por sí misma:

    expansiva     todo el pasado disponible. Máximo tamaño de muestra.
    deslizante    las últimas `sliding_seasons` temporadas. Apuesta por que el
                  fútbol reciente es más informativo que el antiguo.

Sobre cualquiera de las dos se superpone un **peso de recencia** con decaimiento
exponencial en días: un partido disputado una semivida antes del pliegue cuenta
la mitad que uno de la jornada anterior. La pregunta que responde es la que
cualquier lector se hace al ver una ventana de nueve temporadas —¿de verdad un
partido de 2017 informa igual que el del fin de semana pasado?— y la respuesta no
se supone: la semivida se fija al abrir un ensayo en el banco manual
(`laboratorio.py`) y se compara midiendo, de modo que dos ensayos que solo
difieren en ella son dos cifras enfrentadas y no una preferencia declarada. «Sin
peso» es uno de esos ensayos, no un caso aparte: ponderar se adopta si la
medición lo respalda, y quien lee la comparación decide.

Las dos formas de ventana no son, por tanto, dos hipótesis en competencia sino
una sola con dos parametrizaciones: la deslizante es un peso escalonado —uno
hasta el corte, cero después— y el decaimiento exponencial es su versión suave.
El trabajo recorre la expansiva y deja que la semivida module cuánto pesa el
pasado, que es la formulación continua de la misma decisión y la única de las dos
que puede tomarse dentro del pliegue sin rehacer los pliegues.

El preprocesado dentro del pliegue
----------------------------------
`preprocess.fit` aprende medianas de imputación, parámetros λ de simetrización y
—sobre todo— la selección de variables de la poda colineal, que es supervisada.
Ajustarlo una sola vez sobre todo el desarrollo y usarlo después en cada pliegue
no filtra el resultado del partido que se predice, pero sí filtra la
**distribución futura**: el pliegue de 2019 sabría qué variables acabarían siendo
informativas en 2024. Es un optimismo pequeño en las medianas, moderado en los λ
y potencialmente notable en la selección, y es exactamente el tipo de detalle que
un tribunal busca.

El protocolo lo resuelve reajustando el estado dentro del pliegue, con una
cadencia declarada:

    "temporada"   (por defecto) el estado se reajusta al empezar cada temporada,
                  con datos estrictamente anteriores a ella. El estado es un
                  resumen distribucional que se mueve despacio, de modo que
                  reajustarlo cada jornada multiplica por treinta y ocho el coste
                  para mover unas medianas que apenas cambian. Usar el estado de
                  la temporada anterior nunca aporta información futura: es
                  conservador, no optimista.
    "jornada"     el estado se reajusta en cada pliegue. Es la lectura más
                  estricta y la que se usa para medir cuánto importaba la
                  cadencia; su coste es de cómputo, no de código.
    "unico"       un solo ajuste sobre todo el desarrollo. **No es un protocolo
                  válido**: se conserva únicamente como término de comparación,
                  para poder publicar la diferencia entre hacerlo bien y hacerlo
                  mal en lugar de afirmar que importa.

El estado se aprende siempre sobre las filas de desarrollo de la ventana. En los
pliegues de validación eso no cambia nada, porque la ventana es desarrollo
entera; en la pasada del holdout es lo que mantiene el sello, porque el estado
que transforma el holdout se ha aprendido sin mirarlo.

Como efecto secundario, esto corrige también la deriva de nivel de la imputación
neutral: la mediana deja de ser la de nueve temporadas y pasa a ser la de la
ventana que precede al pliegue, de modo que un hueco de 2024 se rellena con un
valor de 2024 y no con uno intermedio entre 2016 y 2024. `imputation_drift`
cuantifica esa deriva sobre los estados realmente ajustados, que es la
comprobación empírica que la decisión pedía.

Lo que el pliegue entrega al estimador
--------------------------------------
La matriz de variables es deliberadamente estrecha: contiene las columnas del
estado congelado y nada más. No lleva la fecha, que no es una variable sino el
eje sobre el que se ordena todo lo demás, ni lleva la probabilidad implícita del
mercado en escala cruda, que se conserva sin transformar precisamente para que
siga siendo una probabilidad y no un valor estandarizado.

Ninguna de las dos cabe en la matriz y las dos hacen falta. La red neuronal
decide cuántas épocas entrenar reservando el **último tramo temporal de la
ventana**, y saber cuál es ese tramo exige la fecha de cada fila; confiar en que
el marco venga ordenado sería una suposición silenciosa que un reordenamiento
inocente rompería sin dejar rastro. El precio del mercado, por su parte, se
ofrece en su escala original porque una probabilidad estandarizada ya no es una
probabilidad: quien quiera condicionar sobre él —hoy la evaluación económica,
mañana una familia que lo pida— necesita el número tal cual sale de la cuota.

El protocolo se los pasa en un `Contexto`, y solo a quien lo pide: un estimador
cuya firma no lo declara se entrena exactamente igual que antes. La regla es la
misma que gobierna el peso de recencia —se inspecciona la firma y se llama en
consecuencia—, de modo que el protocolo sigue sin saber qué modelo entrena.

El pliegue ajustado como unidad reutilizable
--------------------------------------------
`run_protocol` solo publica predicciones y descarta todo lo demás, pero la
interpretabilidad necesita justamente lo que se descarta: el modelo entrenado, la
matriz de evaluación sobre la que se mide la importancia por permutación y la
ventana de la que se extraen los valores de reemplazo. `fit_fold` aísla ese paso
elemental —congelar el preprocesado, comprobar la matriz, pesar y entrenar— y
devuelve un `PliegueAjustado` con todo ello. `run_protocol` lo llama pliegue a
pliegue y `explain.py` lo invoca sobre los pliegues que quiere interpretar, de
modo que lo que se explica es por construcción lo mismo que se evaluó y no una
reconstrucción parecida.

Uso
---
    from protocol import ProtocolConfig, run_protocol

    cfg = ProtocolConfig(regime="semanal", window="expansiva")
    pred = run_protocol(feat, fabrica_de_estimador, cfg)
"""
from __future__ import annotations

import inspect
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import columns as C  # noqa: E402
import preprocess as pp  # noqa: E402

# --- bloques del calendario ---------------------------------------------------
# El arranque son tres temporadas, y eso está medido, no heredado. La duda es legítima
# porque solo la primera lo necesita por construcción —2016/17 es la primera temporada
# del histórico y no tiene pasado del que sacar forma, racha ni enfrentamiento directo,
# y su cobertura media de variables es del 88.00 %—, mientras que 2017/18 y 2018/19
# están al 96.42 % y al 96.69 %, indistinguibles del 96.70 % de 2019/20 y del
# 96.96-97.19 % de las temporadas de medición. Por cobertura, dos de las tres sobran.
#
# Se midió lo que costaría recuperarlas, porque la búsqueda de variables fracasó por
# falta de potencia y agrandar el bloque de selección era el primer candidato a
# remedio. El bloque de MEDICIÓN no cambia con el arranque —sigue siendo
# 2022/23-2024/25, 125 pliegues y 1140 partidos—, así que la decisión es solo del lado
# de la selección:
#
#   arranque        pliegues      n   error típico   el k5 gana al k3      t
#   3 temporadas         130   1140      0.000381          +0.000636    1.67
#   2 temporadas         170   1520      0.000311          +0.000350    1.13
#   1 temporada          210   1900      0.000266          +0.000195    0.73
#
# El ruido baja como tiene que bajar: 0.70x al pasar de 1140 a 1900 partidos, cerca de
# la raíz de la proporción (0.775). Pero el efecto que habría que medir baja MÁS
# deprisa, y la discriminación empeora de 1.67 a 0.73 errores típicos. Agrandar el
# bloque no compra potencia.
#
# La razón de que el efecto se desplome es el hallazgo, y vale más que la decisión del
# arranque: las dos temporadas que se añadirían NO participaron en elegir el conjunto,
# y en ellas el conjunto PIERDE (+0.000424 en 17/18, +0.000507 en 18/19, de RPS, que es
# pérdida). Mezclarlas con el bloque de selección desperdiciaría su mejor uso, que es
# precisamente el de lectura limpia de fuera de la elección, anterior a la selección y
# por tanto independiente del bloque de medición.
#
# Se queda en tres temporadas, y un arranque de una sola se usa como configuración de
# DIAGNÓSTICO: es la que abre 2017/18-2018/19 para juzgar un conjunto con partidos que
# no lo eligieron, sin tocar el holdout.
WARMUP_SEASONS = ["2016/17", "2017/18", "2018/19"]

# Corte que parte el bloque de validación en dos mitades cronológicas. La primera
# —de la primera temporada evaluable a la anterior a este corte— es donde se ELIGE:
# la búsqueda voraz recorre variables ahí y solo ahí. La segunda es donde se MIDE,
# con el conjunto ya cerrado y sin volver atrás.
#
# La razón de partirlo es que una selección voraz elige la variable que más sube la
# cifra sobre los partidos que mira, de modo que esa cifra deja de ser una estimación
# del rendimiento futuro y pasa a ser el máximo de un ruido con doscientas
# oportunidades de parecer señal. Medir después sobre temporadas que la búsqueda no
# ha visto devuelve a la cifra su significado. Es el mismo argumento que sella el
# holdout, aplicado un escalón antes: el holdout protege del sobreajuste de la
# decisión final, y este corte protege del sobreajuste de la selección.
#
# El corte va en 2022/23 y no a mitad exacta porque las tres temporadas de medición
# son el mínimo con el que la tabla por temporada dice algo —una temporada afortunada
# se distingue de una tendencia mirando varias—, y porque la mitad antigua necesita
# ser la más larga: es la que paga el coste de recorrer candidatas.
CORTE_SELECCION = "2022/23"

# --- reconstrucción de la jornada efectiva ------------------------------------
_GAP_DIAS = 3          # hueco de calendario que separa dos jornadas consecutivas

# --- ventana de entrenamiento -------------------------------------------------
# La ventana por defecto es EXPANSIVA y SIN peso de recencia, y eso está medido, no
# heredado. Las dos preguntas naturales —«¿sobra el fútbol de hace seis temporadas?»
# y «¿debe pesar más el partido reciente?»— se contestaron recorriendo las treinta
# combinaciones de ventana (expansiva, o deslizante de 2 a 6 temporadas) por semivida
# de recencia (sin peso, 180, 365, 730, 1460 días), sobre el bloque de selección y con
# dos conjuntos distintos para que la respuesta no dependa de uno. Comparación
# emparejada partido a partido, intervalo remuestreando jornadas.
#
# Gana la vigente en los dos conjuntos, y ninguna de las veintinueve alternativas la
# bate: RPS 0.193174 con las tres variables del mercado y 0.192538 con el conjunto de
# cinco. Los dos ejes, por separado:
#
#   · El peso de recencia hace daño de forma ordenada, y cuanto más corto más daño:
#     semivida de 180 días cuesta +0.000732 de RPS (k3) y +0.001179 (k5, con intervalo
#     [-0.00242, -0.000005] que EXCLUYE el cero); 365 días, +0.000234 y +0.000369; 730,
#     +0.000083 y +0.000120; 1460, +0.000033 y +0.000044. La serie converge a cero
#     desde abajo, que es exactamente lo que tiene que pasar si la mejor semivida es
#     infinita. El partido de hace seis temporadas todavía informa.
#   · Acortar la ventana tampoco paga: deslizante de 3 temporadas es la peor de las
#     largas (-0.000441 y -0.000627) y la de 2 pierde -0.000067 y -0.000500.
#
# `deslizante` de 6 da cifras IDÉNTICAS a `expansiva` a seis decimales, y eso no es
# casualidad ni un fallo: en este bloque ningún pliegue tiene más de seis temporadas
# de historia detrás, así que una ventana de seis es todo el pasado. Sirve de
# comprobación de que la ventana hace lo que dice.
#
# Queda una tensión que conviene dejar escrita porque apunta en contra: la deslizante
# de 2 temporadas SUBE el acierto —53.77 % frente al 52.81 % del conjunto de cinco con
# la vigente— mientras empeora el RPS. Acierta más favoritos y calibra peor. No se
# elige por ahí, y no por dogma: esos 0.96 puntos son once partidos de mil ciento
# cuarenta, del tamaño del error típico del propio acierto, mientras que la pérdida de
# RPS se mide. Es la regla 4 otra vez.
WINDOWS = ("expansiva", "deslizante")
SLIDING_SEASONS = 4    # el valor de la referencia; hiperparámetro, no dogma

# --- cadencia de reajuste del preprocesado ------------------------------------
# Lo que esta cadencia gobierna es el estado del PREPROCESADO —valores de imputación,
# lambdas de Yeo-Johnson, medias y desviaciones de escalado, poda por correlación—, no
# el reajuste del modelo: el estimador se entrena desde cero en cada pliegue siempre, y
# eso no es un parámetro de nada.
#
# La cadencia vigente es `temporada`, y está medida. La duda es razonable: con un estado
# por temporada, la jornada 38 se preprocesa con constantes aprendidas antes de la
# jornada 1, de modo que si las distribuciones se mueven dentro del curso, `jornada`
# tendría que ganar. Sobre el bloque de selección y con los dos conjuntos, comparación
# emparejada partido a partido e intervalo por jornadas:
#
#   conjunto   cadencia    RPS        gana a la vigente   intervalo
#   k3         jornada     0.193174      +0.0000003       [-0.0000004, +0.0000010]
#   k3         unico       0.193174      +0.0000006       [-0.0000026, +0.0000035]
#   k5         jornada     0.192538      +0.0000005       [-0.0000004, +0.0000013]
#   k5         unico       0.192536      +0.0000024       [-0.0000021, +0.0000066]
#
# Son diferencias de orden 1e-6, MIL VECES menores que el ruido del propio bloque
# (0.00026 de error típico en una comparación emparejada), y el acierto no se mueve ni
# en la segunda cifra decimal. La cadencia es irrelevante para los conjuntos en juego y
# se queda como está por medición, no por herencia.
#
# El dato que cierra la pregunta por arriba es `unico`, que no es un protocolo válido
# —ajusta el estado sobre TODO el desarrollo, incluido el futuro del pliegue— y existe
# precisamente como término de comparación: su fuga compra 0.0000024 de RPS. Es decir
# que la mejora M1, que era exactamente esta fuga, estaba bien corregida por método y
# no por coste: lo que se ganaba con ella es indistinguible de cero.
#
# El mecanismo, que es inferencia del resultado y no una medición aparte: la logística
# del banco va penalizada (`C=0.3`, cresta), así que NO es invariante a reescalado y la
# explicación no puede ser la invariancia afín. Lo que ocurre es que las constantes se
# estiman de ventanas solapadas de 1140 partidos o más de la misma distribución, apenas
# se mueven de una cadencia a otra, y un cambio diminuto en la tipificación desplaza la
# solución penalizada en segundo orden. Con un conjunto grande, donde la poda por
# correlación sí retira columnas y podría retirar columnas DISTINTAS en cada estado, la
# cadencia podría pesar más; con tres y cinco variables no se poda ninguna.
REFIT_CADENCES = ("temporada", "jornada", "unico")

# --- guarda de tamaño mínimo --------------------------------------------------
MIN_TRAIN_MATCHES = 380   # una temporada completa de Primera División

_CLASES = (0, 1, 2)       # y_num: 0=H, 1=D, 2=A
_PROB_COLS = ["p_home", "p_draw", "p_away"]


# ===========================================================================
# 1. La jornada efectiva
# ===========================================================================
def effective_matchday(feat: pd.DataFrame) -> pd.Series:
    """Índice de jornada reconstruido desde el calendario, por temporada.

    Devuelve una serie entera alineada con `feat`, numerada desde 1 dentro de
    cada temporada. Un partido abre jornada nueva cuando su fecha se separa de la
    última del bloque en curso más de `_GAP_DIAS`, o cuando alguno de sus dos
    equipos ya juega en ese bloque.

    La segunda condición es la que hace la construcción defendible. El corte por
    hueco de calendario, solo, agruparía un aplazamiento disputado entre semana
    con los partidos de la jornada siguiente; si en ese mismo bloque estuviera el
    otro partido de uno de los dos equipos, el pliegue evaluaría dos encuentros
    consecutivos del mismo equipo a la vez y la forma del segundo tendría que
    haberse calculado con el resultado del primero, que en producción todavía no
    se habría jugado. Exigir que ningún equipo se repita dentro del bloque cierra
    esa puerta por construcción.
    """
    faltan = [c for c in ("season", "date", "home_key", "away_key")
              if c not in feat.columns]
    if faltan:
        raise ValueError(f"effective_matchday necesita {faltan} en el frame")

    fechas = pd.to_datetime(feat["date"])
    orden = feat.assign(_fecha=fechas).sort_values(
        ["season", "_fecha", "home_key"], kind="mergesort")

    jornada = pd.Series(0, index=feat.index, dtype="int64")
    for _, bloque in orden.groupby("season", sort=False):
        j = 1
        ultimo = None
        equipos: set[str] = set()
        for idx, fila in bloque.iterrows():
            fecha = fila["_fecha"]
            par = {fila["home_key"], fila["away_key"]}
            corta = (ultimo is not None
                     and ((fecha - ultimo).days > _GAP_DIAS or bool(par & equipos)))
            if corta:
                j += 1
                equipos = set()
            jornada.at[idx] = j
            equipos |= par
            ultimo = fecha
    return jornada


def matchday_report(feat: pd.DataFrame) -> pd.DataFrame:
    """Contraste entre la jornada efectiva y la nominal de Sofascore.

    Una fila por temporada con el número de jornadas reconstruidas, el número de
    jornadas nominales y la proporción de partidos en que ambas coinciden tras
    alinear el desfase. No es una comprobación dura —el calendario real no tiene
    por qué coincidir con el rótulo— sino la evidencia de que la reconstrucción
    reproduce el calendario en lugar de inventarse uno.
    """
    j = effective_matchday(feat)
    dat = pd.DataFrame({"season": feat["season"].to_numpy(), "jornada": j.to_numpy()})
    dat["round"] = (pd.to_numeric(feat["round"], errors="coerce").to_numpy()
                    if "round" in feat.columns else np.nan)

    filas = []
    for temporada, sub in dat.groupby("season", sort=True):
        con_round = sub.dropna(subset=["round"])
        coincide = float((con_round["jornada"] == con_round["round"]).mean()) \
            if len(con_round) else float("nan")
        filas.append({
            "season": temporada,
            "partidos": len(sub),
            "jornadas_efectivas": int(sub["jornada"].max()),
            "jornadas_nominales": (int(con_round["round"].max())
                                   if len(con_round) else 0),
            "coincidencia": coincide,
            "partidos_por_jornada": round(len(sub) / int(sub["jornada"].max()), 2),
        })
    return pd.DataFrame(filas)


# ===========================================================================
# 2. Configuración y pliegues
# ===========================================================================
# Los dos valores por omisión que más se discuten —`window = "expansiva"` y
# `halflife_days = None`— están medidos, y la hipótesis que los ponía en duda era
# razonable: si el fútbol de hace nueve temporadas no informa sobre la jornada que
# viene, una ventana corta con peso de recencia tiene que ganar a la expansiva.
#
# Se midieron las 30 combinaciones de ventana (expansiva y deslizante de 2 a 6
# temporadas) por semivida (sin peso, 180, 365, 730 y 1460 días) sobre el bloque de
# SELECCIÓN —2019/20-2021/22, 1140 partidos, regla 6— y con dos conjuntos, para que la
# respuesta no dependiese de uno: la semilla de mercado de tres variables y el conjunto
# de cinco. Comparación emparejada partido a partido contra la configuración vigente,
# con el intervalo remuestreando jornadas enteras.
#
# La hipótesis no sobrevive, y en ninguna de las dos lecturas: **expansiva sin peso es
# la mejor de las treinta**, y no hay una sola configuración que la bata.
#
#   conjunto      configuración              RPS        gana a la vigente
#   k3 semilla    expansiva · sin peso       0.193174    (vigente)
#   k3 semilla    deslizante-5 · sin peso    0.193202   -0.000028
#   k3 semilla    expansiva · hl=1460d       0.193207   -0.000033
#   k3 semilla    expansiva · hl=180d        0.193906   -0.000732  [-0.00163, +0.00015]
#   k5 conjunto   expansiva · sin peso       0.192538    (vigente)
#   k5 conjunto   deslizante-5 · sin peso    0.192554   -0.000015
#   k5 conjunto   expansiva · hl=180d        0.193717   -0.001179  [-0.00242, -0.00001]
#
# Dos lecturas, y la segunda es la que importa. Acortar la ventana sin ponderar es
# indiferente —deslizante-5 pierde tres cienmilésimas, y deslizante-6 da exactamente la
# cifra de la expansiva porque con el arranque dentro no hay más pasado que recortar—,
# así que acortar no hace daño: simplemente no sirve de nada. **Ponderar sí hace daño**,
# de forma monótona y cuanto más corta la semivida: la de 180 días es la peor de las
# cinco en las dos lecturas, y con el conjunto de cinco su intervalo al 95 % excluye el
# cero. El acierto se mueve en dirección contraria en algunas filas (deslizante-2 sin
# peso llega al 53.77 %) y por eso no decide: es la regla 4, y la C.1 del cuaderno.
#
# El motivo mecánico es que este es un modelo dominado por el precio, y el precio ya es
# reciente por construcción: cada cuota es la lectura del mercado de esa semana. Lo que
# el pasado antiguo aporta no es información sobre la forma de los equipos sino partidos
# con los que estimar la relación entre cuota y resultado, y para eso cuantos más,
# mejor. Ponderar tira muestra —cae el tamaño efectivo— sin ganar actualidad.
#
# Conclusión operativa: **no se cambia nada**, y queda escrito que no se cambia porque
# se midió y no por omisión. `SEMIVIDAS` sigue declarado como espacio (§3) porque un
# conjunto distinto podría dar otra respuesta; lo que no se hace es ponderar de oficio.
@dataclass
class ProtocolConfig:
    """Todas las decisiones del protocolo, en un objeto que se puede imprimir.

    Que la configuración sea un dato y no una colección de argumentos sueltos es
    lo que permite adjuntarla a las predicciones y responder, meses después, con
    qué protocolo se obtuvo una cifra concreta.
    """
    regime: str = pp.DEFAULT_REGIME
    window: str = "expansiva"
    sliding_seasons: int = SLIDING_SEASONS
    refit_cadence: str = "temporada"
    halflife_days: float | None = None      # None = sin peso de recencia
    corr_threshold: float = 0.95
    transform: bool = True
    min_train_matches: int = MIN_TRAIN_MATCHES
    warmup_seasons: list = field(default_factory=lambda: list(WARMUP_SEASONS))
    corte_seleccion: str = CORTE_SELECCION

    def __post_init__(self) -> None:
        if self.regime not in C.REGIMES:
            raise ValueError(f"régimen desconocido: {self.regime!r}; "
                             f"use uno de {C.REGIMES}")
        if self.window not in WINDOWS:
            raise ValueError(f"ventana desconocida: {self.window!r}; "
                             f"use una de {WINDOWS}")
        if self.refit_cadence not in REFIT_CADENCES:
            raise ValueError(f"cadencia desconocida: {self.refit_cadence!r}; "
                             f"use una de {REFIT_CADENCES}")
        if self.halflife_days is not None and self.halflife_days <= 0:
            raise ValueError("la semivida del peso de recencia debe ser positiva")
        # Un corte que caiga dentro del arranque dejaría vacío el bloque de
        # selección sin decirlo: `make_folds` devolvería una lista vacía y la
        # búsqueda parecería no encontrar nada cuando lo que no hay es dónde buscar.
        if str(self.corte_seleccion) <= max(map(str, self.warmup_seasons or [""])):
            raise ValueError(
                f"corte_seleccion={self.corte_seleccion!r} cae dentro del arranque "
                f"({self.warmup_seasons}); el bloque de selección quedaría vacío")

    def describe(self) -> str:
        peso = ("sin peso de recencia" if self.halflife_days is None
                else f"peso de recencia con semivida de {self.halflife_days:g} días")
        ventana = (self.window if self.window == "expansiva"
                   else f"{self.window} de {self.sliding_seasons} temporadas")
        return (f"régimen {self.regime} · ventana {ventana} · "
                f"preprocesado por {self.refit_cadence} · {peso} · "
                f"corte de selección en {self.corte_seleccion}")


@dataclass
class Fold:
    """Un pliegue: una jornada que se predice y la ventana con la que se entrena."""
    idx: int
    season: str
    jornada: int
    fecha_ini: pd.Timestamp
    fecha_fin: pd.Timestamp
    train_idx: pd.Index
    eval_idx: pd.Index

    @property
    def n_train(self) -> int:
        return len(self.train_idx)

    @property
    def n_eval(self) -> int:
        return len(self.eval_idx)

    def __repr__(self) -> str:  # pragma: no cover - cosmético
        return (f"<Fold {self.idx:>3} {self.season} j{self.jornada:>2} "
                f"train={self.n_train} eval={self.n_eval}>")


BLOQUES: tuple[str, ...] = ("validacion", "seleccion", "medicion", "holdout",
                            "produccion")


def temporadas_en_curso(feat: pd.DataFrame) -> list[str]:
    """Las temporadas POSTERIORES a la sellada: las de producción.

    Se deducen del propio dato comparando con `columns.HOLDOUT_SEASONS` y no se
    escriben a mano en ninguna lista. El motivo es el de siempre en este proyecto:
    una lista literal de temporadas envejece en silencio —el primer fin de semana
    de agosto de 2027 la temporada nueva se colaría en el bloque de desarrollo y
    ninguna comprobación lo diría—, mientras que el criterio «va después del
    holdout» sigue siendo verdad sin que nadie lo toque.

    El rótulo de temporada ordena como texto: '2026/27' > '2025/26' y
    '2016/17' < '2025/26', de modo que la comparación lexicográfica es la
    cronológica mientras el formato no cambie.
    """
    ultima_sellada = max(map(str, C.HOLDOUT_SEASONS))
    return sorted(s for s in feat["season"].astype(str).unique()
                  if s > ultima_sellada)


def _bloque_evaluable(feat: pd.DataFrame, cfg: ProtocolConfig,
                      evaluar: str) -> pd.Series:
    """Máscara de las filas que un pliegue puede evaluar, según el bloque pedido.

    `validacion` es el bloque de desarrollo entero, que es lo que usa el banco
    manual cuando compara familias o hiperparámetros. `seleccion` y `medicion` son
    sus dos mitades a un lado y otro de `cfg.corte_seleccion`, y existen para que la
    búsqueda voraz elija en una y publique en la otra. Las tres máscaras se escriben
    aquí juntas para que sea evidente que las dos mitades particionan la primera:
    ninguna jornada de desarrollo queda fuera de las dos y ninguna cae en ambas.

    `produccion` es la temporada en curso y no pertenece a ninguna de las tres. La
    columna `split` no la distingue —vale «dev» en ella, porque lo que esa columna
    materializa es el sello del holdout y nada más—, así que el descuento se hace
    aquí, que es donde vive el calendario. Dejarla dentro de `validacion` llevaría
    la temporada sellada a una decisión por la puerta de atrás: cualquier jornada
    posterior al holdout se entrena con él.
    """
    dev = ((feat[C.SPLIT] == "dev")
           & (~feat["season"].isin(cfg.warmup_seasons))
           & (~feat["season"].astype(str).isin(temporadas_en_curso(feat))))
    if evaluar == "validacion":
        return dev
    if evaluar == "seleccion":
        return dev & (feat["season"].astype(str) < str(cfg.corte_seleccion))
    if evaluar == "medicion":
        return dev & (feat["season"].astype(str) >= str(cfg.corte_seleccion))
    if evaluar == "holdout":
        return feat[C.SPLIT] == "holdout"
    if evaluar == "produccion":
        # Solo los partidos ya jugados: la jornada que viene se puntúa por la ruta
        # de inferencia (`features/build_predict.py`), que no tiene resultado con
        # el que comparar y por tanto no es un pliegue.
        return (feat["season"].astype(str).isin(temporadas_en_curso(feat))
                & feat[C.TARGET_NUM].notna())
    raise ValueError(f"bloque desconocido: {evaluar!r}; "
                     f"use uno de {list(BLOQUES)}")


def make_folds(feat: pd.DataFrame, cfg: ProtocolConfig | None = None, *,
               evaluar: str = "validacion") -> list[Fold]:
    """Construye los pliegues walk-forward en orden cronológico.

    Para cada jornada evaluable, la ventana de entrenamiento son los partidos
    **estrictamente anteriores** a su primer encuentro. El corte se hace por fecha
    y no por índice de jornada precisamente porque un aplazamiento desordena los
    índices: un partido rotulado como jornada 5 pero disputado en febrero no puede
    entrar en el entrenamiento de la jornada 6 de octubre.

    Los pliegues cuya ventana no alcanza `min_train_matches` se descartan en
    silencio: entrenar un multinomial de doscientas variables con media temporada
    produce una cifra que solo mide la falta de datos.
    """
    cfg = cfg or ProtocolConfig()
    dat = feat.copy()
    dat["_fecha"] = pd.to_datetime(dat["date"])
    dat["_jornada"] = effective_matchday(dat)

    evaluable = _bloque_evaluable(dat, cfg, evaluar)
    claves = (dat.loc[evaluable, ["season", "_jornada", "_fecha"]]
              .groupby(["season", "_jornada"], sort=False)["_fecha"]
              .agg(["min", "max"]).reset_index()
              .sort_values("min", kind="mergesort"))

    folds: list[Fold] = []
    for _, fila in claves.iterrows():
        temporada, jornada = fila["season"], int(fila["_jornada"])
        ini, fin = fila["min"], fila["max"]

        es_pliegue = (evaluable
                      & (dat["season"] == temporada)
                      & (dat["_jornada"] == jornada))
        anteriores = dat["_fecha"] < ini
        if cfg.window == "deslizante":
            corte = ini - pd.DateOffset(years=cfg.sliding_seasons)
            anteriores &= dat["_fecha"] >= corte
        # el objetivo tiene que existir: un partido sin resultado no enseña nada
        entrena = anteriores & dat[C.TARGET_NUM].notna()

        if int(entrena.sum()) < cfg.min_train_matches:
            continue
        folds.append(Fold(idx=len(folds), season=str(temporada), jornada=jornada,
                          fecha_ini=ini, fecha_fin=fin,
                          train_idx=dat.index[entrena],
                          eval_idx=dat.index[es_pliegue]))
    return folds


def folds_report(folds: list[Fold]) -> pd.DataFrame:
    """Tabla descriptiva de los pliegues: tamaño de ventana y de jornada."""
    return pd.DataFrame([{
        "fold": f.idx, "season": f.season, "jornada": f.jornada,
        "fecha_ini": f.fecha_ini, "fecha_fin": f.fecha_fin,
        "n_train": f.n_train, "n_eval": f.n_eval,
        "dias": int((f.fecha_fin - f.fecha_ini).days) + 1,
    } for f in folds])


# ===========================================================================
# 3. Peso de recencia
# ===========================================================================
# Espacio declarado de la semivida, escrito aquí —junto a la función que la
# aplica y antes de buscarla— por el mismo motivo por el que los espacios de los
# estimadores viven junto a los estimadores: un rango que se retoca después de
# ver resultados deja de ser una búsqueda.
#
# `None` es un valor del espacio y no la ausencia de él: significa «todo el
# pasado pesa igual», que es una hipótesis con defensores y la que el trabajo
# tomaría por omisión. Que compita como un candidato más es lo que convierte la
# ponderación en una decisión medida en lugar de en un supuesto.
#
# Los valores cubren de media temporada a cinco años. El extremo corto es la
# apuesta por que solo el pasado inmediato informa; el largo es prácticamente
# indistinguible de no ponderar sobre una muestra de nueve temporadas, y está
# para que el espacio contenga esa vecindad y no haya que extrapolarla.
SEMIVIDAS: tuple = (None, 180.0, 365.0, 730.0, 1095.0, 1825.0)

# Marca de «no se ha decidido», distinta de `None`. Son dos estados que la tabla
# de selección tiene que poder separar: `None` es «se midió y ponderar no mejora»
# y `HEREDADA` es «no hubo con qué medir, rige el valor del protocolo». Sin esta
# distinción, la primera temporada de validación —que no selecciona nada— sería
# indistinguible de una temporada en la que la búsqueda descartó el peso.
HEREDADA: str = "heredada"


def recency_weights(dates, ref, halflife_days: float | None) -> np.ndarray | None:
    """Peso exponencial que decae hacia el pasado con la semivida indicada.

    Un partido disputado `halflife_days` antes de `ref` pesa la mitad que uno
    disputado en `ref`. Se devuelve `None` cuando no hay semivida, de modo que la
    ausencia de peso sea explícita y no un vector de unos que el estimador tenga
    que multiplicar sin motivo.
    """
    if halflife_days is None:
        return None
    d = (pd.Timestamp(ref) - pd.to_datetime(pd.Series(dates))).dt.days
    d = d.clip(lower=0).to_numpy(dtype="float64")
    return np.exp(-np.log(2.0) * d / float(halflife_days))


def semivida_de(factory, fold: "Fold", cfg: ProtocolConfig) -> float | None:
    """La semivida que rige en un pliegue: la de la fábrica si la declara.

    La ponderación del pasado es una decisión del protocolo, y por eso su valor
    por defecto vive en `ProtocolConfig`. Pero es también una decisión que puede
    tomarse dentro del pliegue, y entonces no puede vivir en la configuración: una
    sola `cfg` recorre todas las temporadas y todas las familias, mientras que la
    semivida elegida cambia de una temporada a la siguiente y de una familia a
    otra. Quien la conoce es la fábrica, que es el objeto que ya transporta las
    decisiones tomadas por temporada.

    De ahí este punto de enganche, gemelo del que permite a una fábrica pedir el
    pliegue: si el invocable expone `semivida(fold)`, manda su respuesta; si no,
    manda `cfg.halflife_days`. El pliegue entra como rótulo —su temporada—, nunca
    como acceso a sus partidos, exactamente igual que en la construcción del
    estimador.
    """
    fn = getattr(factory, "semivida", None)
    if fn is None:
        return cfg.halflife_days
    valor = fn(fold) if callable(fn) else fn
    if valor is HEREDADA or valor == HEREDADA:
        return cfg.halflife_days
    if valor is not None and float(valor) <= 0:
        raise ValueError(f"semivida no positiva ({valor!r}) en el pliegue "
                         f"{fold.season} j{fold.jornada}")
    return None if valor is None else float(valor)


# ===========================================================================
# 4. El preprocesado dentro del pliegue
# ===========================================================================
class _StateCache:
    """Ajusta y reutiliza el estado del preprocesado según la cadencia declarada.

    La clave de reutilización es lo que distingue las tres cadencias: la temporada
    del pliegue, el pliegue entero o una constante. El estado se ajusta siempre
    sobre las filas de desarrollo de la ventana, nunca sobre el holdout.
    """

    def __init__(self, feat: pd.DataFrame, cfg: ProtocolConfig):
        self._feat = feat
        self._cfg = cfg
        self._cache: dict = {}
        self._aplicado: tuple = ()   # (clave, marco transformado) del estado en uso
        self.states: dict = {}       # clave -> PreprocessState, para diagnóstico

    def _clave(self, fold: Fold):
        if self._cfg.refit_cadence == "temporada":
            return ("temporada", fold.season)
        if self._cfg.refit_cadence == "jornada":
            return ("jornada", fold.idx)
        return ("unico",)

    def get(self, fold: Fold):
        clave = self._clave(fold)
        if clave in self._cache:
            return self._cache[clave]

        if self._cfg.refit_cadence == "unico":
            # término de comparación: TODO el desarrollo, incluido el futuro del
            # pliegue. No es un protocolo válido y por eso se aísla aquí.
            ventana = self._feat.index[self._feat[C.SPLIT] == "dev"]
        else:
            ventana = fold.train_idx

        dev = self._feat.loc[ventana]
        dev = dev[dev[C.SPLIT] == "dev"]
        if dev.empty:
            raise ValueError(
                f"el pliegue {fold.idx} no tiene filas de desarrollo con las que "
                f"ajustar el preprocesado; el estado no puede aprenderse del holdout")
        st = pp.fit(dev, corr_threshold=self._cfg.corr_threshold,
                    transform=self._cfg.transform, regime=self._cfg.regime)
        self._cache[clave] = st
        self.states[clave] = st
        return st

    def aplicado(self, fold: Fold, feat: pd.DataFrame):
        """El marco entero ya transformado con el estado de este pliegue, o `None`.

        Transformar una ventana y transformar el marco completo para quedarse
        luego con esa ventana dan lo mismo: `preprocess.apply` es fila a fila
        —los flags salen de `isna()`, y los mapas de categorías, los valores de
        imputación, las λ de Yeo-Johnson y las medias y desviaciones de escalado
        vienen todos congelados en el estado—, de modo que ninguna fila influye
        en el valor de otra. Lo que cambia es el número de veces que se hace: la
        cadencia `temporada` comparte un estado entre las treinta y ocho jornadas
        de su temporada y la ventana es creciente, así que cada pliegue volvía a
        transformar las mismas filas que el anterior y una más. Aquí se
        transforma una vez por estado y el pliegue se queda con sus dos trozos.

        Esto no mira hacia adelante. El estado sigue ajustándose solo con las
        filas de entrenamiento del pliegue —eso lo decide `get`, que no se toca—
        y qué filas ve el modelo lo siguen decidiendo `fold.train_idx` y
        `fold.eval_idx`. Transformar de más no es entrenar de más.

        Devuelve `None` —y quien llama transforma por su cuenta— en los dos casos
        en los que el memo no aplica: si el marco no es aquel con el que se
        construyó la caché, porque entonces no hay nada que afirmar sobre su
        contenido; y con cadencia `jornada`, donde cada pliegue estrena estado y
        el memo solo añadiría el coste de las filas que ese pliegue no usa.
        """
        if feat is not self._feat or self._cfg.refit_cadence == "jornada":
            return None
        clave = self._clave(fold)
        if self._aplicado and self._aplicado[0] == clave:
            return self._aplicado[1]
        todo = pp.apply(self._feat, self.get(fold), keep_meta=True)
        # Una sola entrada: las claves se recorren en orden y la anterior ya no
        # vuelve, así que guardar el histórico solo gastaría memoria.
        self._aplicado = (clave, todo)
        return todo


def imputation_drift(states: dict) -> pd.DataFrame:
    """Deriva de los valores de imputación neutral a lo largo de los estados.

    Una fila por columna imputada con estrategia `neutral`, el primer y el último
    valor aprendidos, y la deriva relativa entre ambos. Es la comprobación
    empírica que justifica reajustar dentro del pliegue en lugar de heredar una
    mediana global: si la deriva es despreciable la corrección es cosmética, y si
    no lo es —el valor de plantilla en euros es el caso evidente— un hueco de la
    última temporada se estaría rellenando con el nivel de hace cuatro años.
    """
    claves = list(states.keys())
    if not claves:
        return pd.DataFrame(columns=["columna", "primero", "ultimo", "deriva_rel"])

    filas = []
    primero, ultimo = states[claves[0]], states[claves[-1]]
    for col, estrategia in primero.impute_strat.items():
        if estrategia != "neutral" or col not in ultimo.impute_value:
            continue
        a = float(primero.impute_value[col])
        b = float(ultimo.impute_value[col])
        base = max(abs(a), 1e-12)
        filas.append({"columna": col, "primero": a, "ultimo": b,
                      "deriva_rel": (b - a) / base})
    out = pd.DataFrame(filas)
    if not out.empty:
        out = out.reindex(out.deriva_rel.abs().sort_values(ascending=False).index)
    return out.reset_index(drop=True)


# ===========================================================================
# 5. Ejecución del protocolo
# ===========================================================================
@dataclass(frozen=True)
class Contexto:
    """Lo que el pliegue sabe de sus filas y la matriz de variables no lleva.

    `fechas` es la fecha de cada partido, en el mismo orden que las filas de la
    matriz. Permite a un estimador reservar el último tramo temporal de su ventana
    sin depender de que el marco venga ordenado.

    `mercado` y `mercado_sharp` son las probabilidades implícitas des-margen del
    consenso y de Pinnacle, en escala cruda y en el orden canónico H, D, A. Valen
    `None` cuando el artefacto no las trae, que es lo que ocurre en las temporadas
    de arranque, y un estimador que las use debe contemplar ese caso en lugar de
    suponer que siempre están.
    """
    fechas: pd.Series
    mercado: np.ndarray | None = None
    mercado_sharp: np.ndarray | None = None

    def __len__(self) -> int:
        return len(self.fechas)


def _acepta(fn, nombre: str) -> bool:
    """¿Declara `fn` un parámetro llamado `nombre`?

    La inspección de la firma es el mecanismo por defecto y basta para cualquier
    estimador escrito a mano. No basta, en cambio, para un invocable envuelto: una
    fábrica que cierra sobre una tabla de configuraciones y recibe lo que le pasen
    a través de `**kwargs` tiene una firma que no declara nada, y suponer que
    entonces no acepta nada sería tan arbitrario como suponer lo contrario.

    Por eso se consulta primero el atributo `ACEPTA`, una colección de nombres con
    la que un invocable declara explícitamente qué está dispuesto a recibir. Es una
    declaración y no una deducción, que es justo lo que se necesita cuando la firma
    ha dejado de ser informativa.
    """
    declarado = getattr(fn, "ACEPTA", None)
    if declarado is not None:
        return nombre in declarado
    try:
        return nombre in inspect.signature(fn).parameters
    except (TypeError, ValueError):  # pragma: no cover - firmas exóticas
        return False


def _contexto(marco: pd.DataFrame, fechas: pd.Series) -> Contexto:
    """Arma el contexto de una ventana a partir del marco ya preprocesado."""
    def _terna(cols):
        if not all(c in marco.columns for c in cols):
            return None
        m = marco[list(cols)].to_numpy(dtype="float64")
        return m if np.isfinite(m).any() else None

    return Contexto(fechas=fechas.reset_index(drop=True),
                    mercado=_terna(pp.MARKET_RAW_COLS),
                    mercado_sharp=_terna(pp.SHARP_RAW_COLS))


def _proba_ordenada(modelo, X: pd.DataFrame, contexto: Contexto | None = None
                    ) -> np.ndarray:
    """Probabilidades en el orden canónico H, D, A, venga como venga del estimador.

    `scikit-learn` ordena las columnas de `predict_proba` según `classes_`, que
    depende de qué clases aparecieron en el entrenamiento. Confiar en que el orden
    sea el natural es la clase de suposición que funciona hasta el día en que una
    ventana de entrenamiento no contiene algún empate.
    """
    if contexto is not None and _acepta(modelo.predict_proba, "contexto"):
        p = np.asarray(modelo.predict_proba(X, contexto=contexto), dtype="float64")
    else:
        p = np.asarray(modelo.predict_proba(X), dtype="float64")
    clases = list(getattr(modelo, "classes_", _CLASES))
    out = np.zeros((len(X), 3), dtype="float64")
    for j, c in enumerate(clases):
        out[:, int(c)] = p[:, j]
    suma = out.sum(axis=1, keepdims=True)
    if not np.allclose(suma, 1.0, atol=1e-6):
        raise ValueError("el estimador ha devuelto probabilidades que no suman 1")
    return out


def _guardas_matriz(X: pd.DataFrame, cfg: ProtocolConfig) -> None:
    """Comprobaciones sobre la matriz que entra al estimador."""
    if cfg.regime == "semanal":
        prohibidas = C.weekly_unavailable(list(X.columns))
        if prohibidas:
            raise AssertionError(
                f"el régimen semanal ha recibido {len(prohibidas)} columnas que no "
                f"existen al predecir: {prohibidas[:6]}. La cifra que produjese esta "
                f"matriz no sería reproducible el día de la jornada")
    finito = np.isfinite(X.to_numpy(dtype="float64"))
    if not finito.all():
        malas = [c for c, ok in zip(X.columns, finito.all(axis=0)) if not ok]
        raise AssertionError(
            f"tras el preprocesado quedan valores no finitos en {len(malas)} columnas: "
            f"{malas[:6]}. Ocurre cuando una columna está entera vacía en la ventana "
            f"de entrenamiento y su valor de imputación es a su vez un hueco; la poda "
            f"por constancia debería haberla retirado, revísela antes de seguir")


@dataclass(frozen=True)
class PliegueAjustado:
    """Un pliegue ya entrenado: el modelo y todo el estado con el que se obtuvo.

    Contiene el estimador ajustado, el estado de preprocesado que se le aplicó,
    los marcos transformados con metadatos (`tr`, `ev`), las dos matrices de
    variables, el objetivo de la ventana y los dos contextos. Es lo que
    `run_protocol` construye y descarta en cada iteración, y lo que la
    interpretabilidad necesita conservar.
    """
    fold: Fold
    modelo: object
    estado: object
    tr: pd.DataFrame
    ev: pd.DataFrame
    Xtr: pd.DataFrame
    Xev: pd.DataFrame
    ytr: pd.Series
    ctx_tr: Contexto
    ctx_ev: Contexto


def fit_fold(feat: pd.DataFrame, factory, cfg: ProtocolConfig, fold: Fold, *,
             cache: "_StateCache | None" = None) -> PliegueAjustado:
    """Entrena un estimador sobre la ventana de un pliegue, con todas sus guardas.

    El orden de las operaciones es el que hace válida la cifra y por eso vive en
    una sola función: primero se comprueba que ninguna fila de la ventana alcance
    la fecha del pliegue, después se congela el preprocesado con datos anteriores
    a él, luego se verifica que la matriz resultante sea admisible en el régimen
    declarado y solo entonces se entrena. Invertir cualquiera de esos pasos
    produciría un modelo aparentemente correcto y una validación inválida.

    `cache` permite compartir el estado de preprocesado entre pliegues según la
    cadencia configurada; si no se pasa, cada llamada ajusta el suyo.
    """
    cache = cache if cache is not None else _StateCache(feat, cfg)
    fechas = pd.to_datetime(feat["date"])

    # --- guarda de orden temporal: nada de la ventana puede ser posterior ---
    if fold.n_train and fechas.loc[fold.train_idx].max() >= fold.fecha_ini:
        raise AssertionError(
            f"pliegue {fold.idx}: la ventana de entrenamiento alcanza la fecha del "
            f"pliegue. Un solo partido mal ordenado invalida toda la validación")

    st = cache.get(fold)
    todo = cache.aplicado(fold, feat)
    if todo is None:
        tr = pp.apply(feat.loc[fold.train_idx], st, keep_meta=True)
        ev = pp.apply(feat.loc[fold.eval_idx], st, keep_meta=True)
    else:
        tr, ev = todo.loc[fold.train_idx], todo.loc[fold.eval_idx]
    Xtr, Xev = tr[st.feature_cols], ev[st.feature_cols]
    _guardas_matriz(Xtr, cfg)
    _guardas_matriz(Xev, cfg)

    ytr = feat.loc[fold.train_idx, C.TARGET_NUM].astype("int64")

    # La fábrica puede pedir el pliegue. La que lo hace es `fabrica_rodante`, de la
    # búsqueda de origen rodante, porque el bloque de variables no es el mismo en
    # todas las temporadas: la configuración que se aplica aquí es la que se eligió
    # con partidos ANTERIORES a este pliegue. El pliegue entra, por
    # tanto, como identificador de qué configuración toca —su temporada, su
    # jornada, su fecha de inicio—, nunca como acceso a sus partidos: la ventana
    # de entrenamiento y la de evaluación las construye esta función y ninguna
    # fábrica las recibe.
    modelo = factory(fold=fold) if _acepta(factory, "fold") else factory()
    w = recency_weights(fechas.loc[fold.train_idx], fold.fecha_ini,
                        semivida_de(factory, fold, cfg))
    if w is not None and not _acepta(modelo.fit, "sample_weight"):
        raise TypeError(
            f"se ha pedido peso de recencia pero {type(modelo).__name__}.fit no "
            f"admite `sample_weight`; elimine el peso o cambie de estimador")

    ctx_tr = _contexto(tr, fechas.loc[fold.train_idx])
    ctx_ev = _contexto(ev, fechas.loc[fold.eval_idx])
    extra = {"contexto": ctx_tr} if _acepta(modelo.fit, "contexto") else {}
    if w is None:
        modelo.fit(Xtr, ytr, **extra)
    else:
        modelo.fit(Xtr, ytr, sample_weight=w, **extra)

    return PliegueAjustado(fold=fold, modelo=modelo, estado=st, tr=tr, ev=ev,
                           Xtr=Xtr, Xev=Xev, ytr=ytr,
                           ctx_tr=ctx_tr, ctx_ev=ctx_ev)


def run_protocol(feat: pd.DataFrame, factory, cfg: ProtocolConfig | None = None, *,
                 evaluar: str = "validacion", folds: list[Fold] | None = None,
                 cache: "_StateCache | None" = None,
                 verbose: bool = False) -> pd.DataFrame:
    """Ejecuta el walk-forward y devuelve las predicciones fuera de muestra.

    `factory` es un invocable que devuelve un estimador nuevo con la interfaz de
    `scikit-learn` (`fit`, `predict_proba`). El protocolo no sabe qué modelo es, de
    modo que comparar dos familias o dos conjuntos de variables no puede introducir
    sin querer una diferencia de protocolo.

    Una fábrica puede declarar que acepta `fold` —por firma o por el atributo
    `ACEPTA`— y entonces se la llama con el pliegue. Es lo que necesita una
    configuración que cambia de temporada en temporada: el pliegue le dice cuál de
    las configuraciones ya decididas le corresponde. La decisión en sí se toma
    fuera y con datos anteriores al pliegue; aquí solo se aplica, y la fábrica no
    recibe en ningún caso los partidos evaluados.

    `cache` permite reutilizar el estado de preprocesado entre ejecuciones que
    comparten pliegues y configuración. El estado no depende del estimador —lo
    aprenden las mismas filas y lo definen las mismas reglas—, de modo que al
    comparar decenas de candidatos sobre los mismos pliegues, reajustarlo para cada
    uno multiplica el coste sin cambiar ninguna cifra. Es lo que hace el módulo de
    selección; en un recorrido normal se omite y cada ejecución ajusta el suyo.

    La salida tiene una fila por partido evaluado, con la identidad del encuentro,
    el pliegue del que salió, las tres probabilidades y —cuando el artefacto las
    trae— las del mercado en escala cruda, para que la comparación con la
    referencia se haga sobre exactamente el mismo conjunto de partidos.
    """
    cfg = cfg or ProtocolConfig()
    folds = folds if folds is not None else make_folds(feat, cfg, evaluar=evaluar)
    if not folds:
        raise ValueError("el protocolo no ha generado ningún pliegue; revise el "
                         "bloque evaluado y `min_train_matches`")
    cache = cache if cache is not None else _StateCache(feat, cfg)
    fechas = pd.to_datetime(feat["date"])
    jornadas = effective_matchday(feat)

    salidas = []
    for f in folds:
        aj = fit_fold(feat, factory, cfg, f, cache=cache)
        p = _proba_ordenada(aj.modelo, aj.Xev, aj.ctx_ev)
        bloque = pd.DataFrame({
            "fold": f.idx, "season": feat.loc[f.eval_idx, "season"].to_numpy(),
            "jornada": jornadas.loc[f.eval_idx].to_numpy(),
            "date": fechas.loc[f.eval_idx].to_numpy(),
            "match_key": feat.loc[f.eval_idx, "match_key"].to_numpy(),
            "home_key": feat.loc[f.eval_idx, "home_key"].to_numpy(),
            "away_key": feat.loc[f.eval_idx, "away_key"].to_numpy(),
            "split": feat.loc[f.eval_idx, C.SPLIT].to_numpy(),
            "y_num": feat.loc[f.eval_idx, C.TARGET_NUM].to_numpy(),
            "n_train": f.n_train,
        })
        for j, col in enumerate(_PROB_COLS):
            bloque[col] = p[:, j]
        # las tres lecturas del precio viajan en escala cruda junto a la predicción.
        # No alimentan al modelo —el consenso ya entra por su versión escalada y las
        # otras dos ni siquiera son variables—, pero sin ellas la evaluación no podría
        # comparar el pronóstico con el mercado sobre exactamente estos partidos.
        for col in pp.PRICE_RAW_COLS:
            if col in aj.ev.columns:
                bloque[col] = aj.ev[col].to_numpy()
        salidas.append(bloque)

        if verbose and f.idx % 20 == 0:
            print(f"  pliegue {f.idx:>3}/{len(folds)}  {f.season} j{f.jornada:>2}  "
                  f"train={f.n_train:>5}  eval={f.n_eval:>2}")

    pred = pd.concat(salidas, ignore_index=True)
    if pred.match_key.duplicated().any():
        raise AssertionError(
            "hay partidos predichos por más de un pliegue: los pliegues deben "
            "particionar el bloque evaluado, no solaparse")
    pred.attrs["config"] = cfg
    pred.attrs["states"] = cache.states
    pred.attrs["n_folds"] = len(folds)
    return pred


def protocol_report(pred: pd.DataFrame, verbose: bool = True) -> dict:
    """Resumen de una ejecución: cobertura, pliegues y configuración."""
    cfg = pred.attrs.get("config")
    res = {
        "protocolo": cfg.describe() if cfg is not None else "(sin configuración)",
        "pliegues": pred.attrs.get("n_folds", pred.fold.nunique()),
        "partidos": len(pred),
        "temporadas": sorted(pred.season.unique().tolist()),
        "ventana_media": float(pred.groupby("fold").n_train.first().mean()),
        "estados_ajustados": len(pred.attrs.get("states", {})),
    }
    if verbose:
        print("=" * 72)
        print("PROTOCOLO —", res["protocolo"])
        print(f"  pliegues: {res['pliegues']}   partidos fuera de muestra: "
              f"{res['partidos']}")
        print(f"  temporadas evaluadas: {', '.join(res['temporadas'])}")
        print(f"  ventana de entrenamiento media: {res['ventana_media']:.0f} partidos")
        print(f"  estados de preprocesado ajustados: {res['estados_ajustados']}")
        print("=" * 72)
    return res
