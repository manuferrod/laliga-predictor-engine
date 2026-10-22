"""Utilidad económica: qué le ocurre a un banco que apuesta lo que el modelo dice.

Este módulo cierra la evaluación por el lado que un lector no técnico entiende
sin traducción. `evaluate.py` responde a «¿son buenas estas probabilidades?»;
aquí se responde a «¿sirven para algo contra un precio real?», que es una
pregunta distinta y bastante más exigente: una terna puede ganar en RPS y perder
dinero, porque el RPS premia acertar en promedio y el mercado solo cobra donde
se equivoca.

El resultado se presenta con una advertencia que conviene leer antes que las
cifras: **el retorno es la métrica más ruidosa de todas las que publica el
trabajo**. Depende de un puñado de aciertos a cuota alta, su varianza es enorme
frente al número de apuestas disponible y una diferencia de dos puntos de
rendimiento entre dos estrategias casi nunca es distinguible del azar. Por eso
todo lo que sale de aquí se acompaña de intervalo por remuestreo y ninguna
conclusión del trabajo descansa exclusivamente en este bloque.

Contra qué precio se apuesta
----------------------------
La cuota no es un detalle de implementación: es la definición del experimento.
El módulo trabaja con conjuntos de cuotas declarados en `CUOTAS`, y la elección
tiene consecuencias que hay que dejar escritas.

    cierre_avg  media de las casas al cierre. Es el precio más eficiente que
                existe: incorpora toda la información hasta el pitido inicial,
                incluidas las alineaciones. Batirlo es la afirmación fuerte.
    cierre_max  la mejor cuota del mercado al cierre. Es el precio que consigue
                un apostante que tiene cuenta en varias casas, y por eso el
                rendimiento contra él es sistemáticamente más alto: parte del
                beneficio no viene del modelo, sino de comparar precios.
    consenso    media de las casas en el momento de apertura. Es el precio que
                encontraría de verdad una apuesta emitida el jueves.
    pinnacle    la casa de referencia informada, la que mueve el mercado.
    b365        una casa concreta y popular, para ilustrar el efecto de no poder
                elegir dónde se apuesta.

De aquí sale la nota de honestidad que la memoria tiene que recoger. El sistema
publica su predicción **antes** de la jornada, de modo que la apuesta real iría
contra las cuotas de apertura; pero la serie histórica está mucho mejor cubierta
al cierre, y el cierre es además el listón más difícil. La lectura defendible es
reportar **las dos**: contra el cierre para no exagerar la habilidad del modelo,
y contra la apertura para describir lo que ocurriría de verdad. Es el mismo
desajuste temporal que documenta la mejora del bloque de mercado, y se reporta,
no se esconde.

El margen del conjunto de cuotas —la suma de las tres probabilidades implícitas
menos uno— se publica junto al rendimiento, porque es el listón exacto que hay
que superar: apostar siguiendo el propio precio del mercado pierde, en promedio,
justo ese margen. Esa comprobación es la calibración del banco de pruebas.

Qué se apuesta y cuánto
-----------------------
Dos decisiones, deliberadamente separadas.

**Selección.** `favorito` apuesta a la clase más probable según el modelo; es la
regla ingenua y sirve de referencia. `valor` apuesta a la clase con mayor valor
esperado y solo si ese valor supera un umbral. La segunda es la que tiene sentido
económico: el modelo no necesita saber quién gana, necesita encontrar precios
equivocados, y esas dos cosas coinciden menos de lo que parece. El umbral existe
porque un valor esperado ligeramente positivo es indistinguible del error de
estimación del propio modelo.

**Importe.** `plana` arriesga una unidad por apuesta; el rendimiento resultante
es el clásico *yield* sobre importe arriesgado y no depende del orden de los
partidos, lo que lo hace la cifra comparable entre estrategias. `kelly` arriesga
la fracción del banco que maximiza el crecimiento logarítmico esperado si las
probabilidades del modelo fueran ciertas —y no lo son—, así que se aplica
**fraccionada** y con tope. Kelly completo sobre probabilidades estimadas es una
receta conocida para la ruina: sobreestima la ventaja justo donde el modelo está
peor calibrado, que es en las cuotas altas.

El tamaño de Kelly se calcula una vez por **jornada**, con el banco al inicio de
la jornada, y no partido a partido. Liquidar en cadena los partidos de un mismo
fin de semana supondría poder reinvertir el sábado lo ganado el sábado en otro
partido del sábado, lo que no ocurre: las apuestas de una jornada son
simultáneas a efectos prácticos. Es una diferencia pequeña en el retorno y
grande en la caída máxima, que es precisamente la cifra que se quiere honesta.

Contra quién se mide: el apostante que sigue a Bet365
-----------------------------------------------------
Un rendimiento no significa nada sin algo con lo que compararlo, y el cero no es
esa comparación: perder poco donde el mercado pierde mucho es un resultado, y
ganar poco donde el mercado gana más no lo es. Por eso el módulo define un
**apostante de referencia** que ignora por completo el modelo y respalda siempre
al favorito de Bet365, es decir, la más baja de sus tres cuotas.

Esa referencia es el contendiente natural del trabajo. La pregunta que la memoria
plantea no es «¿gana dinero el modelo?» —a la que casi siempre hay que responder
que no, porque el margen de la casa está diseñado exactamente para eso— sino
«¿lo hace mejor o peor que la casa de apuestas cuyo precio se está usando?». La
referencia se construye sustituyendo la terna del modelo por la terna implícita
de Bet365 y pasando el resultado por la misma selección, la misma simulación y
el mismo resumen: lo único que cambia entre las dos estrategias es **quién
opina**, de modo que la diferencia de rendimiento es atribuible al pronóstico y
no al procedimiento.

Las dos apuestan contra el mismo conjunto de cuotas. Pagar a la referencia con
el precio de Bet365 y al modelo con otro mezclaría en una sola cifra dos efectos
distintos —la calidad del pronóstico y la calidad del precio conseguido— y la
comparación dejaría de decir lo que dice.

La comparación se publica **temporada a temporada y jornada a jornada**, no solo
en agregado, porque el retorno es ruidoso y una ventaja que solo existe en el
total suele ser una ventaja que solo existe en una temporada.

Qué se reporta
--------------
Retorno acumulado, rendimiento sobre importe arriesgado, tasa de acierto, cuota
media, número de apuestas y cobertura; y junto a ellos la **caída máxima**, que
es la peor racha medida desde un máximo del banco. Un retorno del ocho por
ciento con una caída del cuarenta no es una estrategia, es una anécdota, y
publicar solo la primera cifra sería un error de presentación grave.

Todas esas cifras se emiten dos veces, una por el modelo y otra por la
referencia, y con ellas la diferencia de rendimiento y su intervalo por
remuestreo de jornadas completas.

El barrido de umbral cierra el bloque. Muestra el rendimiento en función de la
exigencia mínima de valor esperado, y su lectura correcta no es «elíjase el
máximo» —que sería ajustar la política a los datos de evaluación— sino observar
si la curva tiene forma: si el rendimiento crece con el umbral, el modelo está
ordenando de verdad las oportunidades; si es plana o errática, lo que se está
midiendo es ruido.

Uso
---
    from backtest import attach_odds, simulate, compara_con_mercado, backtest_report

    p   = attach_odds(pred, feat, cuotas="cierre_avg")
    lib = simulate(p, politica="valor", umbral=0.03, importe="kelly")
    cmp = compara_con_mercado(p)            # modelo frente al favorito de Bet365
    backtest_report(p)                      # informe legible de la estrategia
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import evaluate as ev  # noqa: E402

# --- conjuntos de cuotas declarados -------------------------------------------
# Cada entrada nombra las tres columnas de cuota decimal (local, empate,
# visitante) tal como las publica el bloque de mercado del máster.
CUOTAS = {
    "cierre_avg": ("odds_close_avg_h", "odds_close_avg_d", "odds_close_avg_a"),
    "cierre_max": ("odds_close_max_h", "odds_close_max_d", "odds_close_max_a"),
    "consenso": ("odds_avg_h", "odds_avg_d", "odds_avg_a"),
    "maxima": ("odds_max_h", "odds_max_d", "odds_max_a"),
    "pinnacle": ("odds_ps_h", "odds_ps_d", "odds_ps_a"),
    "b365": ("odds_b365_h", "odds_b365_d", "odds_b365_a"),
    "b365_cierre": ("odds_b365_close_h", "odds_b365_close_d", "odds_b365_close_a"),
}
CUOTA_COLS = ["cuota_home", "cuota_draw", "cuota_away"]

# --- parámetros por defecto de la política ------------------------------------
FRACCION_KELLY = 0.25      # cuarto de Kelly: el compromiso habitual entre
                           # crecimiento y supervivencia con probabilidades estimadas
TOPE_KELLY = 0.05          # ninguna apuesta arriesga más del 5 % del banco
UMBRAL_VALOR = 0.02        # dos por ciento de ventaja estimada mínima
BANCA_INICIAL = 100.0
_EPS = 1e-12


# ===========================================================================
# 1. Cuotas: incorporación y validación
# ===========================================================================
def attach_odds(pred: pd.DataFrame, feat: pd.DataFrame, *,
                cuotas: str = "cierre_avg") -> pd.DataFrame:
    """Añade a las predicciones las tres cuotas decimales del conjunto elegido.

    La unión se hace aquí y no en el protocolo a propósito. El protocolo entrega
    lo que hace falta para **medir probabilidades**, y una cuota no es una
    probabilidad: lleva dentro el margen de la casa. Mantenerla fuera evita que
    entre por descuido en la matriz de variables, y obliga a que el estudio
    económico declare explícitamente contra qué precio se está midiendo.

    La unión es por `match_key`, que es única por partido, de modo que el
    resultado tiene exactamente las mismas filas y en el mismo orden que
    `pred`. Los partidos sin cuota conservan `NaN` y quedan luego fuera de la
    simulación, contabilizados como cobertura incompleta y no como apuestas
    perdidas.
    """
    if cuotas not in CUOTAS:
        raise ValueError(f"conjunto de cuotas desconocido: {cuotas!r}; "
                         f"disponibles {sorted(CUOTAS)}")
    if "match_key" not in pred.columns or "match_key" not in feat.columns:
        raise ValueError("attach_odds necesita match_key en ambos marcos")

    cols = list(CUOTAS[cuotas])
    faltan = [c for c in cols if c not in feat.columns]
    if faltan:
        raise ValueError(f"el marco de variables no trae {faltan}")

    tabla = (feat[["match_key"] + cols]
             .drop_duplicates("match_key")
             .rename(columns=dict(zip(cols, CUOTA_COLS))))
    out = pred.merge(tabla, on="match_key", how="left", validate="one_to_one")
    for c in CUOTA_COLS:
        out[c] = pd.to_numeric(out[c], errors="coerce")
        # Una cuota decimal por debajo de 1 no paga ni la puesta: es un dato
        # corrupto, no una apuesta mala, y se trata como ausencia.
        out.loc[out[c] <= 1.0, c] = np.nan
    out.attrs = dict(pred.attrs)
    out.attrs["cuotas"] = cuotas
    return out


def _cuotas(pred: pd.DataFrame) -> np.ndarray:
    """Matriz N×3 de cuotas decimales, exigida como tal."""
    faltan = [c for c in CUOTA_COLS if c not in pred.columns]
    if faltan:
        raise ValueError(f"faltan las cuotas {faltan}: usa attach_odds primero")
    return pred[CUOTA_COLS].to_numpy(dtype="float64")


def overround(pred: pd.DataFrame) -> float:
    """Margen medio del conjunto de cuotas: cuánto hay que batir solo para empatar.

    Es la suma de las tres probabilidades implícitas menos uno. Un margen del
    seis por ciento significa que una apuesta al azar pierde, en promedio, cerca
    de un seis por ciento del importe arriesgado; ese es el listón real, no el
    cero.
    """
    O = _cuotas(pred)
    fila = np.isfinite(O).all(axis=1)
    if not fila.any():
        return float("nan")
    return float((1.0 / O[fila]).sum(axis=1).mean() - 1.0)


# ===========================================================================
# 2. Valor esperado y tamaño de la apuesta
# ===========================================================================
def expected_value(P: np.ndarray, O: np.ndarray) -> np.ndarray:
    """Valor esperado por unidad arriesgada: `p·c − 1`.

    Cero significa apuesta justa. Positivo significa que, **si la probabilidad
    del modelo es correcta**, la apuesta gana dinero a la larga; toda la
    dificultad del problema está contenida en esa condición.
    """
    P = np.asarray(P, dtype="float64")
    O = np.asarray(O, dtype="float64")
    if P.shape != O.shape:
        raise ValueError("probabilidades y cuotas deben tener la misma forma")
    return P * O - 1.0


def kelly_fraction(p: np.ndarray, o: np.ndarray) -> np.ndarray:
    """Fracción del banco que maximiza el crecimiento logarítmico esperado.

    Para una apuesta binaria a cuota decimal `c` con probabilidad `p`, la
    fracción óptima es `(p·c − 1)/(c − 1)`: el valor esperado dividido por la
    ganancia neta por unidad. Se recorta en cero porque una fracción negativa
    equivaldría a apostar en contra, que en el mercado 1X2 no está disponible
    —vender un resultado no es lo mismo que comprar los otros dos, y confundirlo
    inflaría la estrategia con operaciones imposibles.
    """
    p = np.asarray(p, dtype="float64")
    o = np.asarray(o, dtype="float64")
    neta = np.clip(o - 1.0, _EPS, None)
    f = (p * o - 1.0) / neta
    return np.clip(np.nan_to_num(f, nan=0.0, posinf=0.0, neginf=0.0), 0.0, 1.0)


# ===========================================================================
# 3. Selección: a qué se apuesta en cada partido
# ===========================================================================
def select_bets(pred: pd.DataFrame, *, politica: str = "valor",
                umbral: float = UMBRAL_VALOR) -> pd.DataFrame:
    """Una fila por partido con la selección propuesta, o ninguna selección.

    Se apuesta **como mucho una vez por partido**. Repartir el importe entre dos
    resultados del mismo encuentro es una cobertura, no una predicción, y
    mezclarla con el resto haría ilegible el rendimiento.

    `favorito` toma la clase más probable y `valor` la de mayor valor esperado.
    La diferencia entre ambas es la tesis del bloque: el favorito del modelo casi
    siempre coincide con el del mercado, así que apostar al favorito compra el
    margen de la casa una y otra vez; el valor solo aparece donde el modelo y el
    precio discrepan, que son pocos partidos y son los únicos que importan.
    """
    if politica not in ("valor", "favorito"):
        raise ValueError(f"política desconocida: {politica!r}")

    P = ev._terna(pred, ev.PROB_COLS)
    O = _cuotas(pred)
    VE = expected_value(P, O)

    disponible = np.isfinite(O) & (O > 1.0)
    hay = disponible.any(axis=1)

    if politica == "favorito":
        crit = np.where(disponible, P, -np.inf)
    else:
        crit = np.where(disponible, VE, -np.inf)
    sel = np.argmax(crit, axis=1)
    idx = np.arange(len(pred))

    out = pred.copy()
    out["seleccion"] = np.where(hay, np.array(ev.CLASES)[sel], None)
    out["p_sel"] = np.where(hay, P[idx, sel], np.nan)
    out["cuota_sel"] = np.where(hay, O[idx, sel], np.nan)
    out["ve_sel"] = np.where(hay, VE[idx, sel], np.nan)
    out["kelly_sel"] = np.where(hay, kelly_fraction(P[idx, sel], O[idx, sel]), np.nan)
    out["clase_sel"] = np.where(hay, sel, -1)

    # El umbral solo tiene sentido sobre la política de valor: exigirle ventaja
    # positiva al favorito lo convertiría en una política de valor peor definida.
    apuesta = hay & np.isfinite(out["p_sel"].to_numpy())
    if politica == "valor":
        apuesta &= out["ve_sel"].to_numpy() > umbral
    out["apuesta"] = apuesta
    out.attrs = dict(pred.attrs)
    out.attrs["politica"] = politica
    out.attrs["umbral"] = float(umbral)
    return out


# ===========================================================================
# 4. La referencia: el apostante que sigue al mercado
# ===========================================================================
# La casa cuya opinión define al apostante de referencia. Es Bet365 y no el
# consenso porque la comparación tiene que ser contra un contendiente
# identificable —una casa con nombre, aquella cuyo precio el lector reconoce— y
# no contra un promedio que ninguna apuesta real podría haber seguido: nadie
# apuesta al favorito de la media de las casas, porque esa media no cotiza.
REFERENCIA = "b365"


def referencia_mercado(pred: pd.DataFrame, *,
                       casa: str = REFERENCIA) -> pd.DataFrame:
    """Las mismas filas con la terna del modelo sustituida por la de una casa.

    Es todo lo que hace falta para construir el contendiente. La opinión de una
    casa de apuestas **es** una terna de probabilidades —la implícita en sus
    cuotas, ya sin margen, que el bloque de mercado adjunta a cada predicción en
    escala cruda—, de modo que ponerla en el lugar de la del modelo convierte
    automáticamente toda la maquinaria de selección y simulación en el apostante
    que sigue al mercado. No hay una segunda implementación de la estrategia de
    referencia y esa ausencia es deliberada: dos implementaciones distintas
    podrían diferir en algún detalle y ese detalle acabaría dentro de la
    diferencia de rendimiento que el trabajo publica.

    Las **cuotas no se tocan**. El apostante de referencia opina como Bet365 pero
    cobra al mismo precio que el modelo, porque el experimento aísla el
    pronóstico y no el acceso al mercado.

    La terna de la casa procede de sus cuotas de **apertura**, que son las
    disponibles cuando el sistema publica su predicción. Las dos estrategias
    deciden, por tanto, con información del mismo momento.
    """
    if casa not in ev.REFERENCIAS:
        raise ValueError(f"referencia de mercado desconocida: {casa!r}; "
                         f"disponibles {sorted(ev.REFERENCIAS)}")
    cols = list(ev.REFERENCIAS[casa])
    faltan = [c for c in cols if c not in pred.columns]
    if faltan:
        raise ValueError(
            f"la predicción no trae la terna de «{casa}» ({faltan}). El protocolo "
            "la adjunta cuando el artefacto preprocesado la contiene, de modo que "
            "esto indica un artefacto sin bloque de mercado y no un fallo aquí.")

    out = pred.copy()
    out[ev.PROB_COLS] = pred[cols].to_numpy(dtype="float64")
    out.attrs = dict(pred.attrs)
    out.attrs["referencia"] = casa
    return out


def simula_referencia(pred: pd.DataFrame, *, casa: str = REFERENCIA,
                      importe: str = "plana", **kw) -> pd.DataFrame:
    """Libro del apostante de referencia: siempre al favorito de la casa.

    La política es `favorito` y no se admite otra. Buscar valor contra el propio
    precio del que se deriva la opinión es imposible por construcción: el valor
    esperado de las tres opciones es negativo y exactamente igual al margen, así
    que una política de valor no encontraría ninguna apuesta y la referencia se
    quedaría vacía. Lo que la referencia hace es respaldar su propia opinión
    partido a partido, que es la conducta que el lector imagina cuando piensa en
    «apostar con la casa».

    Por la misma razón el importe es plano por defecto: Kelly asigna cero a una
    apuesta sin ventaja estimada y dejaría a la referencia sin ninguna operación.
    """
    if "politica" in kw or "umbral" in kw:
        raise TypeError("la referencia no admite política ni umbral: apuesta "
                        "siempre al favorito de su casa")
    ref = referencia_mercado(pred, casa=casa)
    libro = simulate(ref, politica="favorito", importe=importe, **kw)
    libro.attrs["referencia"] = casa
    libro.attrs["politica"] = f"favorito de {casa}"
    return libro


# ===========================================================================
# 5. Simulación del banco
# ===========================================================================
def _liquidacion(bloques: np.ndarray) -> tuple[np.ndarray, list[slice]]:
    """El orden en que se liquidan las apuestas y los cortes de jornada sobre él.

    Sustituye a un `groupby` cuyo único cometido era iterar: agrupar un marco de
    pandas construye un objeto por grupo, y cada uno de esos objetos arrastra una
    copia profunda de los `attrs` del original —donde el protocolo deja los
    estados de preprocesado—. Aquí solo hacen falta los límites.

    Lo que no se puede dar por hecho es que cada jornada venga seguida. La
    unidad de liquidación es la jornada completa y un partido aplazado se juega
    semanas más tarde sin dejar de pertenecer a la suya, de modo que ordenar por
    fecha parte esa jornada en dos. Se reordena por tanto por jornada —en orden
    de primera aparición, que es el que tenía el recorrido por grupos— y es sobre
    ese orden donde los cortes son contiguos.
    """
    if not len(bloques):
        return np.empty(0, dtype="int64"), []
    codigos = pd.factorize(bloques)[0]          # en orden de primera aparición
    orden = np.argsort(codigos, kind="stable")  # estable: dentro, el de llegada
    cortes = np.flatnonzero(np.diff(codigos[orden])) + 1
    bordes = np.concatenate([[0], cortes, [len(orden)]])
    return orden, [slice(int(a), int(b)) for a, b in zip(bordes[:-1], bordes[1:])]


def _importes_kelly(d: pd.DataFrame, acierto: np.ndarray, cuota: np.ndarray,
                    tramos: list[slice], *, fraccion: float, tope: float,
                    banca: float) -> np.ndarray:
    """El importe de cada apuesta con Kelly fraccionado, jornada a jornada.

    El tamaño se fija con el banco al **inicio** de la jornada y todas las
    apuestas de esa jornada se liquidan después, de modo que el bucle recorre
    jornadas —unas pocas decenas— y no partidos. Dentro de cada una el cálculo es
    vectorial, que es lo que hace asequible el remuestreo: un intervalo de
    doscientas réplicas vuelve a liquidar las dos estrategias doscientas veces.
    """
    kelly = d["kelly_sel"].to_numpy(dtype="float64")
    importes = np.zeros(len(d), dtype="float64")
    b = float(banca)
    for tramo in tramos:
        s = np.minimum(fraccion * kelly[tramo], tope) * b
        # Kelly asigna importe cero —o negativo— a una selección sin ventaja
        # estimada: esa apuesta no se hace y por tanto no mueve el banco.
        s = np.where(s > 0, s, 0.0)
        g = np.where(acierto[tramo], s * (cuota[tramo] - 1.0), -s)
        b += float(g.sum())
        importes[tramo] = s
    return importes


def _orden_temporal(sel: pd.DataFrame) -> pd.DataFrame:
    """Ordena por fecha y agrupa por jornada, que es la unidad de liquidación."""
    d = sel.copy()
    if "date" not in d.columns:
        raise ValueError("la simulación necesita la fecha del partido")
    d["_fecha"] = pd.to_datetime(d["date"])
    if "season" in d.columns and "jornada" in d.columns:
        d["_bloque"] = d.season.astype(str) + "|" + d.jornada.astype(str)
    else:
        d["_bloque"] = d["_fecha"].dt.strftime("%Y-%m-%d")
    # `mergesort` es estable: dentro de la misma fecha se conserva el orden de
    # llegada, de modo que la simulación es reproducible fila a fila.
    return d.sort_values(["_fecha", "match_key"], kind="mergesort")


def simulate(pred: pd.DataFrame, *, politica: str = "valor",
             umbral: float = UMBRAL_VALOR, importe: str = "plana",
             fraccion: float = FRACCION_KELLY, tope: float = TOPE_KELLY,
             banca: float = BANCA_INICIAL, unidad: float = 1.0) -> pd.DataFrame:
    """Libro de apuestas partido a partido, con el banco después de cada jornada.

    Con importe **plano** cada apuesta arriesga `unidad` y el banco es el
    acumulado de ganancias; el orden de los partidos no altera el resultado
    final, lo que hace de esta variante la comparable entre estrategias.

    Con **Kelly fraccionado** el importe depende del banco, así que el orden sí
    importa y la simulación se vuelve una trayectoria. El tamaño se fija con el
    banco al **inicio de la jornada** y todas las apuestas de esa jornada se
    liquidan después: es lo que ocurre de verdad un fin de semana, y evita el
    espejismo de reinvertir dentro de la propia jornada lo que aún no se ha
    cobrado.

    Conviene notar una consecuencia del propio criterio: Kelly asigna importe
    cero a una selección sin ventaja estimada, de modo que combinarlo con la
    política de favorito filtra por valor aunque la política no lo pida. No es
    un efecto lateral que haya que corregir —arriesgar en una apuesta de valor
    esperado negativo no tiene defensa— pero sí explica que esa combinación
    apueste en menos partidos, y por eso la cobertura se publica siempre.

    El libro devuelto tiene una fila por apuesta efectivamente realizada y lleva
    en `attrs` la política, el conjunto de cuotas y la cobertura, para que
    ninguna cifra publicada quede sin su procedencia.
    """
    if importe not in ("plana", "kelly"):
        raise ValueError(f"modo de importe desconocido: {importe!r}")
    if not 0 < fraccion <= 1:
        raise ValueError("la fracción de Kelly debe estar en (0, 1]")

    sel = select_bets(pred, politica=politica, umbral=umbral)
    y = ev._objetivo(sel)
    sel = sel.assign(_y=y)
    ordenado = _orden_temporal(sel)

    # Un partido sin resultado conocido no puede liquidarse. No debería haber
    # ninguno en un bloque evaluable, pero el filtro por bloque se hace por
    # temporada y no por objetivo, de modo que un encuentro anulado o aún sin
    # acta llegaría hasta aquí. Contarlo como apuesta perdida falsearía el
    # rendimiento y detener la simulación entera sería peor: se omite, y la
    # cobertura publicada lo refleja.
    viva = (ordenado["apuesta"].to_numpy(dtype=bool)
            & np.isfinite(ordenado["_y"].to_numpy(dtype="float64")))
    d = ordenado.loc[viva]

    orden, tramos = _liquidacion(d["_bloque"].to_numpy())
    d = d.take(orden)

    cuota = d["cuota_sel"].to_numpy(dtype="float64")
    acierto = (d["_y"].to_numpy(dtype="int64")
               == d["clase_sel"].to_numpy(dtype="int64"))

    if importe == "plana":
        importes = np.full(len(d), float(unidad), dtype="float64")
    else:
        importes = _importes_kelly(d, acierto, cuota, tramos,
                                   fraccion=fraccion, tope=tope, banca=banca)

    # Una apuesta de importe nulo no se hizo, así que no figura en el libro. Con
    # importe plano eso no descarta nada; con Kelly, las selecciones sin ventaja.
    hecha = importes > 0
    d, cuota, acierto, importes = (d.loc[hecha], cuota[hecha], acierto[hecha],
                                   importes[hecha])

    # Cuota decimal: acertar devuelve puesta por cuota, fallar pierde la puesta.
    # La ganancia neta es por tanto `stake·(c−1)` o `−stake`, y el banco es su
    # acumulado en el orden de liquidación.
    ganancia = np.where(acierto, importes * (cuota - 1.0), -importes)
    datos = {
        "date": d["date"].to_numpy(),
        "match_key": d["match_key"].to_numpy(),
        "seleccion": d["seleccion"].to_numpy(),
        "y_num": d["_y"].to_numpy(dtype="int64"),
        "acierto": acierto,
        "p_sel": d["p_sel"].to_numpy(dtype="float64"),
        "cuota_sel": cuota,
        "ve_sel": d["ve_sel"].to_numpy(dtype="float64"),
        "importe": importes,
        "ganancia": ganancia,
        "banca": float(banca) + np.cumsum(ganancia),
    }
    for col in ("season", "jornada"):
        datos[col] = (d[col].to_numpy() if col in d.columns
                      else np.full(len(d), None, dtype="object"))

    libro = pd.DataFrame(datos)[[
        "date", "season", "jornada", "match_key", "seleccion", "y_num",
        "acierto", "p_sel", "cuota_sel", "ve_sel", "importe", "ganancia", "banca"]]
    libro.attrs = {
        "politica": politica, "umbral": float(umbral), "importe": importe,
        "fraccion": float(fraccion), "tope": float(tope),
        "banca_inicial": float(banca), "unidad": float(unidad),
        "cuotas": pred.attrs.get("cuotas"),
        "n_partidos": int(len(pred)),
        "n_con_cuota": int(np.isfinite(_cuotas(pred)).all(axis=1).sum()),
        "overround": overround(pred),
    }
    return libro


# ===========================================================================
# 6. Lectura del resultado
# ===========================================================================
def max_drawdown(banca: np.ndarray, inicial: float) -> dict:
    """Peor caída desde un máximo del banco, en unidades y en proporción.

    La proporción se mide contra el máximo alcanzado, no contra el banco
    inicial: perder veinte unidades después de haber ganado cincuenta no es lo
    mismo que perderlas de salida, y el apostante que vivió la racha lo
    experimenta como una caída sobre lo que llegó a tener.
    """
    serie = np.concatenate([[float(inicial)], np.asarray(banca, dtype="float64")])
    pico = np.maximum.accumulate(serie)
    caida = pico - serie
    rel = caida / np.clip(pico, _EPS, None)
    i = int(np.argmax(caida)) if len(caida) else 0
    return {"caida_max": float(caida.max()) if len(caida) else 0.0,
            "caida_max_rel": float(rel.max()) if len(rel) else 0.0,
            "pico_previo": float(pico[i]) if len(pico) else float(inicial),
            "valle": float(serie[i]) if len(serie) else float(inicial)}


def summary(libro: pd.DataFrame) -> dict:
    """Las cifras que resumen una estrategia, con su cobertura y su caída.

    `rendimiento` es el beneficio dividido por el **importe arriesgado**, que es
    la cifra comparable entre estrategias que apuestan cantidades distintas.
    `retorno` es lo que le pasó al banco, que es lo que se percibe. Se publican
    las dos porque responden a preguntas distintas y confundirlas es el error de
    lectura más frecuente en este terreno.
    """
    a = libro.attrs
    inicial = float(a.get("banca_inicial", BANCA_INICIAL))
    if libro.empty:
        return {"n_apuestas": 0, "n_partidos": int(a.get("n_partidos", 0)),
                "cobertura": 0.0, "acierto": float("nan"),
                "importe_total": 0.0, "beneficio": 0.0,
                "rendimiento": float("nan"), "retorno": 0.0,
                "banca_final": inicial, "cuota_media": float("nan"),
                "ve_medio": float("nan"), "caida_max": 0.0,
                "caida_max_rel": 0.0, "overround": a.get("overround", float("nan"))}

    imp = float(libro.importe.sum())
    ben = float(libro.ganancia.sum())
    dd = max_drawdown(libro.banca.to_numpy(), inicial)
    n_part = int(a.get("n_partidos", len(libro)))
    return {
        "n_apuestas": int(len(libro)),
        "n_partidos": n_part,
        "cobertura": float(len(libro) / n_part) if n_part else float("nan"),
        "acierto": float(libro.acierto.mean()),
        "importe_total": imp,
        "beneficio": ben,
        "rendimiento": ben / imp if imp > 0 else float("nan"),
        "retorno": (float(libro.banca.iloc[-1]) - inicial) / inicial,
        "banca_final": float(libro.banca.iloc[-1]),
        "cuota_media": float(libro.cuota_sel.mean()),
        "ve_medio": float(libro.ve_sel.mean()),
        "caida_max": dd["caida_max"],
        "caida_max_rel": dd["caida_max_rel"],
        "overround": float(a.get("overround", float("nan"))),
    }


def by_season(libro: pd.DataFrame) -> pd.DataFrame:
    """El mismo resumen por temporada, que es donde se ve si el retorno es estable.

    Una estrategia cuyo beneficio entero procede de una temporada no es una
    estrategia: es una temporada. Este corte es el que lo revela y por eso
    acompaña siempre a la cifra global.
    """
    if libro.empty or "season" not in libro.columns:
        return pd.DataFrame(columns=["season", "n", "acierto", "beneficio",
                                     "rendimiento", "cuota_media"])
    g = libro.groupby("season", sort=True)
    tab = pd.DataFrame({
        "n": g.size(),
        "acierto": g.acierto.mean(),
        "importe": g.importe.sum(),
        "beneficio": g.ganancia.sum(),
        "cuota_media": g.cuota_sel.mean(),
    })
    tab["rendimiento"] = tab.beneficio / tab.importe.replace(0.0, np.nan)
    return tab.reset_index()[["season", "n", "acierto", "beneficio",
                              "rendimiento", "cuota_media"]]


COLS_JORNADA = ["n", "aciertos", "importe", "beneficio", "rendimiento",
                "importe_acum", "beneficio_acum", "rendimiento_acum"]


def by_matchday(libro: pd.DataFrame) -> pd.DataFrame:
    """El mismo resumen jornada a jornada, con el acumulado hasta esa jornada.

    Es el corte que la evaluación automática necesita. La cifra global se calcula
    una vez al cerrar el recorrido, pero el sistema puntúa cada semana y en ese
    momento la pregunta es cómo va el rendimiento **hasta hoy**, no cómo quedó al
    final de un histórico que todavía no ha terminado. Por eso la tabla lleva las
    dos lecturas: el rendimiento de la jornada, que es puro ruido y solo sirve
    para ver la dispersión, y el acumulado, que es la serie que se publica.

    La jornada es además la unidad honesta de agregación: los partidos de una
    misma semana comparten el estado del mundo y no son observaciones
    independientes, de modo que es también el bloque sobre el que se remuestrea
    cuando hay que poner un intervalo a cualquiera de estas cifras.

    El orden es el temporal, dado por la primera fecha de cada jornada, y no el
    alfabético de la etiqueta: la jornada 10 va antes que la 2 si se ordenara
    como texto, y un acumulado calculado en ese orden no sería un acumulado.
    """
    vacia = pd.DataFrame(columns=["season", "jornada", "fecha", "cuota_media"]
                         + COLS_JORNADA)
    if libro.empty or not {"season", "jornada"} <= set(libro.columns):
        return vacia

    d = libro.copy()
    d["_fecha"] = pd.to_datetime(d["date"])
    g = d.groupby(["season", "jornada"], sort=False, dropna=False)
    tab = pd.DataFrame({
        "fecha": g["_fecha"].min(),
        "n": g.size(),
        "aciertos": g.acierto.sum(),
        "importe": g.importe.sum(),
        "beneficio": g.ganancia.sum(),
        "cuota_media": g.cuota_sel.mean(),
    }).reset_index().sort_values("fecha", kind="mergesort")

    tab["rendimiento"] = tab.beneficio / tab.importe.replace(0.0, np.nan)
    tab["importe_acum"] = tab.importe.cumsum()
    tab["beneficio_acum"] = tab.beneficio.cumsum()
    tab["rendimiento_acum"] = (tab.beneficio_acum
                               / tab.importe_acum.replace(0.0, np.nan))
    return tab.reset_index(drop=True)[["season", "jornada", "fecha",
                                       "cuota_media"] + COLS_JORNADA]


def equity_curve(libro: pd.DataFrame) -> pd.DataFrame:
    """Trayectoria del banco apuesta a apuesta, con su máximo y su caída viva."""
    inicial = float(libro.attrs.get("banca_inicial", BANCA_INICIAL))
    if libro.empty:
        return pd.DataFrame(columns=["n", "date", "banca", "pico", "caida"])
    banca = libro.banca.to_numpy(dtype="float64")
    pico = np.maximum.accumulate(np.concatenate([[inicial], banca]))[1:]
    return pd.DataFrame({"n": np.arange(1, len(libro) + 1),
                         "date": libro.date.to_numpy(),
                         "banca": banca, "pico": pico, "caida": pico - banca})


# ===========================================================================
# 7. La comparación con la referencia
# ===========================================================================
def rendimiento(libro: pd.DataFrame) -> float:
    """Beneficio sobre importe arriesgado, que es la cifra comparable.

    Se separa de `summary` porque es lo único que el remuestreo necesita
    recalcular miles de veces, y porque es la única cifra de este módulo que no
    depende del orden de los partidos ni del tamaño del banco: dos estrategias
    que apuestan cantidades distintas siguen siendo comparables por aquí.
    """
    imp = float(libro.importe.sum()) if not libro.empty else 0.0
    return float(libro.ganancia.sum() / imp) if imp > 0 else float("nan")


def comparacion_por_temporada(libro_modelo: pd.DataFrame,
                              libro_referencia: pd.DataFrame) -> pd.DataFrame:
    """Rendimiento de las dos estrategias en cada temporada, y su diferencia.

    Es el corte que decide si hay algo que contar. Una ventaja concentrada en una
    temporada es una temporada afortunada; una ventaja pequeña pero presente en
    casi todas es un resultado, aunque el agregado sea menos vistoso. Publicar
    solo la cifra global impediría distinguir esos dos casos, que es justo la
    distinción que el lector necesita para juzgar el trabajo.
    """
    cols = ["n", "acierto", "beneficio", "rendimiento", "cuota_media"]
    a = by_season(libro_modelo)
    b = by_season(libro_referencia)
    m = a.merge(b, on="season", how="outer", suffixes=("_modelo", "_ref"))
    m = m.sort_values("season", kind="mergesort")
    m["diferencia"] = m["rendimiento_modelo"] - m["rendimiento_ref"]
    orden = (["season"]
             + [f"{c}_{q}" for q in ("modelo", "ref") for c in cols]
             + ["diferencia"])
    return m.reset_index(drop=True)[orden]


def comparacion_por_jornada(libro_modelo: pd.DataFrame,
                            libro_referencia: pd.DataFrame) -> pd.DataFrame:
    """Serie jornada a jornada de las dos estrategias, con su acumulado.

    Esta es la tabla que la evaluación automática emite cada semana y de la que
    sale la curva que se publica: dos rendimientos acumulados frente a frente a
    lo largo del recorrido, y la distancia entre ambos. Lo que hay que mirar en
    ella no es el punto final —que ya está en el agregado— sino si la distancia
    se mantiene o si nace entera de un tramo corto.

    Las columnas acumuladas se arrastran hacia adelante cuando una de las dos
    estrategias no apuesta en alguna jornada. Un acumulado no vuelve a cero
    porque esa semana no hubiera operaciones: se queda donde estaba, que es lo
    que de verdad le pasa al banco.
    """
    a = by_matchday(libro_modelo)
    b = by_matchday(libro_referencia)
    llaves = ["season", "jornada"]
    cols = ["fecha", "cuota_media"] + COLS_JORNADA
    m = (a[llaves + cols].merge(b[llaves + cols], on=llaves, how="outer",
                                suffixes=("_modelo", "_ref")))
    if m.empty:
        m["fecha"] = pd.Series(dtype="datetime64[ns]")
        m["diferencia"] = pd.Series(dtype="float64")
    else:
        m["fecha"] = m["fecha_modelo"].fillna(m["fecha_ref"])
        m = m.sort_values("fecha", kind="mergesort")
        for q in ("modelo", "ref"):
            for c in ("importe_acum", "beneficio_acum", "rendimiento_acum"):
                m[f"{c}_{q}"] = m[f"{c}_{q}"].ffill()
        m["diferencia"] = m["rendimiento_acum_modelo"] - m["rendimiento_acum_ref"]
    m = m.drop(columns=["fecha_modelo", "fecha_ref"])
    orden = (llaves + ["fecha"]
             + [f"{c}_{q}" for q in ("modelo", "ref")
                for c in ["cuota_media"] + COLS_JORNADA]
             + ["diferencia"])
    return m.reset_index(drop=True)[orden]


def compara_con_mercado(pred: pd.DataFrame, *, casa: str = REFERENCIA,
                        politica: str = "valor", umbral: float = UMBRAL_VALOR,
                        importe: str = "plana", n_boot: int = 400,
                        seed: int = 7, nivel: float = 0.95,
                        intervalos_temporada: bool = True,
                        verbose: bool = False) -> dict:
    """El modelo contra el apostante que sigue a la casa, sobre los mismos partidos.

    Se simulan las dos estrategias, se resumen con el mismo `summary`, se cortan
    por temporada y por jornada, y se acompaña la diferencia de rendimiento de un
    intervalo por remuestreo de jornadas completas.

    El remuestreo es **pareado**: cada réplica elige un conjunto de jornadas y
    vuelve a simular las dos estrategias sobre él, de modo que ambas ven los
    mismos partidos en cada réplica. Remuestrear cada una por su lado sumaría dos
    varianzas que en realidad están correlacionadas —una jornada de resultados
    sorprendentes castiga a los dos contendientes a la vez— y produciría un
    intervalo demasiado ancho para detectar una diferencia que sí existe.

    Las temporadas llevan su propio intervalo, calculado sobre sus propias
    jornadas. Saldrán muy anchos, y eso es información: con unas cuarenta
    jornadas por temporada, ninguna diferencia de rendimiento razonable es
    distinguible del azar dentro de una sola temporada, y esa es exactamente la
    razón por la que la conclusión del trabajo no descansa en este bloque.
    """
    libro_m = simulate(pred, politica=politica, umbral=umbral, importe=importe)
    libro_r = simula_referencia(pred, casa=casa, importe=importe)

    def _dif(sub: pd.DataFrame) -> float:
        lm = simulate(sub, politica=politica, umbral=umbral, importe=importe)
        lr = simula_referencia(sub, casa=casa, importe=importe)
        if lm.empty or lr.empty:
            raise ValueError("réplica sin apuestas en alguna de las dos estrategias")
        return rendimiento(lm) - rendimiento(lr)

    def _intervalo(sub: pd.DataFrame):
        try:
            return ev.block_bootstrap(sub, _dif, n_boot=n_boot, seed=seed,
                                      nivel=nivel)
        except (ValueError, KeyError, ZeroDivisionError):
            return None

    # El remuestreo vuelve a liquidar las dos estrategias en cada réplica, y nada
    # de lo que viaja en `attrs` entra en esa cuenta. Se remuestrea por tanto
    # sobre un marco con los `attrs` vacíos, porque pandas los propaga con copia
    # profunda a todo marco derivado y el protocolo deja ahí los estados de
    # preprocesado: sobre el marco original, cada réplica pagaría esa copia
    # varias veces y el intervalo costaría más que el propio walk-forward. El
    # conjunto de cuotas se conserva porque es procedencia, no dato de cálculo.
    ligero = pred.copy()
    ligero.attrs = {"cuotas": pred.attrs.get("cuotas")}

    intervalo = (_intervalo(ligero)
                 if not (libro_m.empty or libro_r.empty) else None)

    temporadas = comparacion_por_temporada(libro_m, libro_r)
    if intervalos_temporada and not temporadas.empty and "season" in pred.columns:
        filas = []
        for s in temporadas["season"]:
            iv = _intervalo(ligero[ligero["season"].astype(str) == str(s)])
            filas.append({"season": s,
                          "dif_inf": np.nan if iv is None else iv["inf"],
                          "dif_sup": np.nan if iv is None else iv["sup"]})
        temporadas = temporadas.merge(pd.DataFrame(filas), on="season", how="left")

    res = {
        "casa": casa,
        "cuotas": pred.attrs.get("cuotas"),
        "politica": politica,
        "umbral": float(umbral),
        "importe": importe,
        "modelo": summary(libro_m),
        "referencia": summary(libro_r),
        "diferencia": rendimiento(libro_m) - rendimiento(libro_r),
        "intervalo": intervalo,
        "por_temporada": temporadas,
        "por_jornada": comparacion_por_jornada(libro_m, libro_r),
        "libros": {"modelo": libro_m, "referencia": libro_r},
    }

    if verbose:
        imprime_comparacion(res)
    return res


def imprime_comparacion(res: dict) -> None:
    """Lectura en voz alta de la comparación, para el cuaderno y para la consola."""
    m, r = res["modelo"], res["referencia"]
    print(f"MODELO frente al favorito de {res['casa']} — cuotas «{res['cuotas']}», "
          f"política «{res['politica']}», importe {res['importe']}")
    print(f"  modelo      {m['n_apuestas']:>5} apuestas  "
          f"acierto {m['acierto']:.1%}  cuota media {m['cuota_media']:.2f}  "
          f"rendimiento {m['rendimiento']:+.2%}")
    print(f"  {res['casa']:<11} {r['n_apuestas']:>5} apuestas  "
          f"acierto {r['acierto']:.1%}  cuota media {r['cuota_media']:.2f}  "
          f"rendimiento {r['rendimiento']:+.2%}")
    if res["intervalo"] is not None:
        iv = res["intervalo"]
        print(f"  diferencia  {iv['estimacion']:+.2%} "
              f"[{iv['inf']:+.2%}, {iv['sup']:+.2%}] por bloques de jornada"
              + ("  (compatible con cero)" if iv["inf"] <= 0 <= iv["sup"]
                 else "  (el intervalo no contiene el cero)"))
    else:
        print(f"  diferencia  {res['diferencia']:+.2%} (sin intervalo: no hay "
              "réplicas evaluables)")
    if not res["por_temporada"].empty:
        vista = ["season", "n_modelo", "rendimiento_modelo", "n_ref",
                 "rendimiento_ref", "diferencia"]
        vista += [c for c in ("dif_inf", "dif_sup")
                  if c in res["por_temporada"].columns]
        print(res["por_temporada"][vista].round(4).to_string(index=False))


# ===========================================================================
# 8. Sensibilidad e incertidumbre
# ===========================================================================
def sweep_umbral(pred: pd.DataFrame, *,
                 umbrales=(0.0, 0.01, 0.02, 0.03, 0.05, 0.08, 0.12),
                 **kw) -> pd.DataFrame:
    """Rendimiento y número de apuestas en función de la exigencia de valor.

    La tabla no se usa para elegir el umbral —eso sería ajustar la política
    sobre los mismos partidos con los que se la juzga— sino para ver si tiene
    forma. Un modelo que ordena bien las oportunidades produce una curva
    creciente hasta que el número de apuestas se hace demasiado pequeño; un
    modelo que no las ordena produce una curva sin patrón, y esa lectura es tan
    informativa como la primera.
    """
    filas = []
    for u in umbrales:
        lib = simulate(pred, umbral=float(u), politica="valor", **kw)
        s = summary(lib)
        filas.append({"umbral": float(u), "n_apuestas": s["n_apuestas"],
                      "acierto": s["acierto"], "cuota_media": s["cuota_media"],
                      "beneficio": s["beneficio"], "rendimiento": s["rendimiento"],
                      "caida_max_rel": s["caida_max_rel"]})
    return pd.DataFrame(filas)


def bootstrap_rendimiento(pred: pd.DataFrame, *, n_boot: int = 1000,
                          seed: int = 7, nivel: float = 0.95, **kw) -> dict:
    """Intervalo del rendimiento por remuestreo de **jornadas completas**.

    Se remuestrea sobre los partidos, no sobre las apuestas, y la simulación se
    rehace entera en cada réplica. Remuestrear el libro ya construido daría por
    fija la selección, que es justamente una de las fuentes de variabilidad: en
    otra realización de la temporada el modelo habría apostado a otras cosas.
    """
    def fn(sub: pd.DataFrame) -> float:
        lib = simulate(sub, **kw)
        if lib.empty:
            raise ValueError("réplica sin apuestas")
        return float(lib.ganancia.sum() / lib.importe.sum())

    return ev.block_bootstrap(pred, fn, n_boot=n_boot, seed=seed, nivel=nivel)


# ===========================================================================
# 9. Informe
# ===========================================================================
def backtest_report(pred: pd.DataFrame, *, politicas=("favorito", "valor"),
                    importes=("plana", "kelly"), umbral: float = UMBRAL_VALOR,
                    casa: str = REFERENCIA, n_boot: int = 400, seed: int = 7,
                    verbose: bool = True) -> dict:
    """Informe económico completo de una terna de probabilidades.

    Recorre el producto de políticas e importes, resume cada combinación,
    acompaña la principal —valor con importe plano, que es la comparable— de su
    intervalo por remuestreo, añade el corte por temporada, el barrido de umbral
    y la **comparación con el apostante que sigue a la casa**, que es la lectura
    que responde a la pregunta del trabajo. Devuelve todo en un diccionario para
    que el cuaderno lo represente sin recalcular nada.

    La comparación no es opcional. Publicar el rendimiento del modelo a solas
    invitaría a leerlo contra el cero, que es el listón equivocado: lo que hay
    que superar es el margen de la casa, y la referencia lo encarna apostando
    contra ese mismo precio con la opinión de la propia casa.
    """
    filas, libros = [], {}
    for pol in politicas:
        for imp in importes:
            lib = simulate(pred, politica=pol, importe=imp, umbral=umbral)
            libros[(pol, imp)] = lib
            filas.append({"politica": pol, "importe": imp, **summary(lib)})
    tabla = pd.DataFrame(filas)

    principal = libros.get(("valor", "plana"))
    intervalo = None
    if principal is not None and not principal.empty:
        try:
            intervalo = bootstrap_rendimiento(pred, n_boot=n_boot, seed=seed,
                                              politica="valor", importe="plana",
                                              umbral=umbral)
        except ValueError:
            intervalo = None

    comparacion = compara_con_mercado(pred, casa=casa, politica="valor",
                                      umbral=umbral, importe="plana",
                                      n_boot=n_boot, seed=seed)

    res = {"tabla": tabla, "libros": libros, "intervalo": intervalo,
           "por_temporada": by_season(principal) if principal is not None
           else pd.DataFrame(),
           "barrido": sweep_umbral(pred, importe="plana"),
           "comparacion": comparacion,
           "casa": casa,
           "referencia": comparacion["referencia"],
           "por_jornada": comparacion["por_jornada"],
           "overround": overround(pred),
           "cuotas": pred.attrs.get("cuotas")}

    if verbose:
        print("=" * 78)
        print(f"UTILIDAD ECONÓMICA — cuotas «{res['cuotas']}», "
              f"margen medio {res['overround']:.2%}")
        print("  Aviso: el retorno es la métrica más ruidosa del trabajo; "
              "léase con su intervalo.")
        print("-" * 78)
        vista = ["politica", "importe", "n_apuestas", "acierto", "cuota_media",
                 "beneficio", "rendimiento", "retorno", "caida_max_rel"]
        print(tabla[vista].round(4).to_string(index=False))
        if intervalo is not None:
            print("-" * 78)
            print(f"Rendimiento de la política de valor con importe plano: "
                  f"{intervalo['estimacion']:+.2%} "
                  f"[{intervalo['inf']:+.2%}, {intervalo['sup']:+.2%}] "
                  f"al 95 % por bloques de jornada")
        if not res["por_temporada"].empty:
            print("-" * 78)
            print("Por temporada:")
            print(res["por_temporada"].round(4).to_string(index=False))
        print("-" * 78)
        imprime_comparacion(comparacion)
        print("-" * 78)
        print("Barrido de umbral de valor esperado:")
        print(res["barrido"].round(4).to_string(index=False))
        print("=" * 78)
    return res
