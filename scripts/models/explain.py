"""INTERPRETABILIDAD: por qué el modelo dice lo que dice.

Un pronóstico que solo publica tres números no es defendible ante un tribunal ni
utilizable por un lector. Este módulo produce las dos lecturas que lo hacen
defendible, y las produce con la misma disciplina temporal que el resto del
trabajo: nada se mide sobre el tramo con el que el modelo se entrenó.

    GLOBAL   qué variables mueven el pronóstico en el conjunto de la muestra.
    LOCAL    qué ha movido ESTE pronóstico, en este partido concreto.

Tres relatos globales, y el interés está en dónde discrepan
-----------------------------------------------------------
Ninguna medida de importancia es la medida de importancia. Cada una responde a
una pregunta distinta y las tres se publican juntas precisamente para que el
lector vea la diferencia:

  1. COEFICIENTES ESTANDARIZADOS del modelo lineal. Responden a «¿en qué
     dirección y con cuánta fuerza entra esta variable en la ecuación?». Son
     comparables entre sí porque el preprocesado deja la matriz estandarizada, de
     modo que un coeficiente se lee como el efecto de una desviación típica. Su
     límite es conocido: con variables correlacionadas, el reparto entre ellas es
     arbitrario, y un coeficiente pequeño no significa que la variable no importe
     sino que su compañera se ha llevado el crédito.

  2. IMPORTANCIA POR PERMUTACIÓN, medida SOBRE EL BLOQUE DE EVALUACIÓN. Responde
     a «¿cuánta capacidad predictiva se pierde si esta variable deja de informar?».
     Es agnóstica al modelo y por eso permite comparar el lineal con el de árboles
     en la misma escala. Calcularla sobre el entrenamiento sería medir cuánto se
     apoya el ajuste en cada variable, que es otra cosa y casi siempre una cifra
     mayor.

  3. VALORES DE SHAPLEY POR BLOQUE, calculados de forma EXACTA. Responden a
     «¿cuánta probabilidad aporta este grupo de variables, repartida de manera
     que el reparto sea el único justo?». Se calculan enumerando las 2^B
     coaliciones posibles de los B bloques explicativos, sin aproximación
     muestral y sin dependencia externa: con menos de una decena de bloques la
     enumeración completa es trivial, y la exactitud es preferible a una
     estimación con error de muestreo que habría que explicar aparte.

Que coincidan es una confirmación; que discrepen es un resultado. Un bloque con
coeficiente grande e importancia por permutación nula es un bloque redundante
—otro le sustituye sin pérdida—, y decirlo es más informativo que ocultarlo.
`agreement` pone los tres órdenes uno al lado del otro y mide su concordancia.

Por qué la unidad de análisis es el BLOQUE y no la columna
-----------------------------------------------------------
La matriz tiene doscientas columnas y la mayoría son variantes de la misma
magnitud: la diferencia de goles de un equipo aparece acumulada, en ventana de
tres, de cinco, de diez, exponencial, ajustada al rival y por escenario. Una
tabla de importancia por columna reparte una señal única entre siete casillas y
concluye que ninguna importa. Repartirla entre bloques —donde el bloque se
permuta o se retira ENTERO, conservando la correlación interna— mide lo que de
verdad se quiere medir: cuánto aporta la forma reciente, no cuánto aporta la
ventana de cinco frente a la de diez.

Los bloques son además los que el lector entiende: mercado, fuerza, forma,
historial, clasificación, fatiga, bajas y contexto. La traducción desde la
taxonomía analítica de `lineage.py` está declarada en `BLOQUES_EXPLICATIVOS` y es
la única pieza escrita a mano; los cientos de nombres de columna se resuelven
consultando la tabla de procedencia, de modo que una variable nueva queda
clasificada sola.

La lectura local
----------------
`explain_match` reparte la probabilidad de cada resultado entre los bloques y se
queda con los tres o cuatro que dominan. La suma de las aportaciones es
exactamente la diferencia entre el pronóstico del partido y el pronóstico de
referencia —la propiedad de eficiencia de Shapley—, lo que permite escribir la
explicación como una frase aritméticamente cierta y no como una glosa.

Sobre el punto de partida
-------------------------
La coalición vacía necesita un valor para cada variable ausente, y aquí es la
MEDIANA DE LA VENTANA de entrenamiento del pliegue. La lectura resultante es
«cuánto se aparta este partido del partido mediano», que es la referencia natural
cuando la matriz está estandarizada. Conviene enunciar esa referencia al publicar
la descomposición: sin ella, un reparto de ±0,04 en probabilidad no se sabe
respecto a qué se mide, y la frase deja de ser aritméticamente cierta para el
lector aunque lo siga siendo para el método.

Uso
---
    import explain as ex

    ajustados = [pt.fit_fold(feat, fabrica, cfg, f) for f in folds_elegidos]
    imp = ex.permutation_importance_blocks(ajustados)
    phi = ex.shapley_blocks(aj.modelo, aj.Xev, aj.ctx_ev, base, mapa)
    ex.explain_match(phi, mapa, fila=0)
"""
from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import evaluate as ev  # noqa: E402
import protocol as pt  # noqa: E402

_CLASES = ("home", "draw", "away")
_ETIQUETA_CLASE = {"home": "victoria local", "draw": "empate",
                   "away": "victoria visitante"}

# Tope de bloques que la enumeración exacta admite sin volverse absurda. Con 12
# bloques son 4.096 coaliciones y sigue siendo cómodo; el tope existe para que un
# error de agrupación —pasar columnas sueltas como si fueran bloques— falle con un
# mensaje claro en lugar de agotar la memoria.
MAX_BLOQUES_SHAPLEY = 14


# ===========================================================================
# 1. Los bloques explicativos
# ===========================================================================
@dataclass(frozen=True)
class BloqueExplicativo:
    """Un grupo de variables que el lector reconoce como una sola idea."""
    id: str
    titulo: str
    bloques_lineage: tuple[str, ...]
    nota: str


BLOQUES_EXPLICATIVOS: tuple[BloqueExplicativo, ...] = (
    BloqueExplicativo(
        "mercado", "lo que dice el mercado",
        ("cuotas", "estructura_precio", "movimiento_linea"),
        "Precio 1X2 de las casas, su estructura —margen, dispersión entre casas, "
        "divergencia del dinero informado— y su movimiento entre apertura y "
        "cierre. Es el bloque de referencia: la opinión agregada de quienes "
        "arriesgan dinero sobre el partido."),
    BloqueExplicativo(
        "fuerza", "la fuerza de las plantillas",
        ("elo", "valor_plantilla", "fuerza_latente", "modelo_goles"),
        "Nivel estructural de cada club: rating Elo, valor de mercado de la "
        "plantilla y las capacidades latentes de ataque y defensa que estima el "
        "modelo de goles. Cambia despacio y no depende del último partido."),
    BloqueExplicativo(
        "forma", "el momento de forma",
        ("forma", "psicologicas_mercado"),
        "Rendimiento reciente en todas sus lecturas: puntos, goles, goles "
        "esperados, presión, nota y su versión ajustada a la dificultad del "
        "calendario. Incluye el rendimiento relativo a lo que el mercado esperaba "
        "en cada partido, que es forma reciente medida con la vara del precio."),
    BloqueExplicativo(
        "historial", "el historial entre los dos",
        ("head_to_head",),
        "Resultado de los enfrentamientos anteriores entre estos dos clubes. Es "
        "el bloque que el aficionado invoca primero y el que menos muestra tiene: "
        "dos partidos por temporada."),
    BloqueExplicativo(
        "clasificacion", "la situación en la tabla",
        ("clasificacion", "trascendencia"),
        "Posición, zona y distancia a los cortes que importan, junto con lo que "
        "cada equipo se juega: si la salvación ya es matemática o si Europa es "
        "inalcanzable, el partido puede no valer nada para uno de los dos."),
    BloqueExplicativo(
        "fatiga", "el desgaste del calendario",
        ("fatiga", "rotacion"),
        "Días de descanso, acumulación de partidos en una y dos semanas, "
        "desplazamiento europeo reciente y parón de selecciones, calculados sobre "
        "el calendario multicompetición y no solo sobre la Liga. Junto a ellos, "
        "cómo ha absorbido el equipo esa carga: continuidad del once, minutos "
        "acumulados por los titulares y concentración de los minutos en pocos "
        "futbolistas. El desgaste y el reparto del desgaste son la misma idea "
        "vista desde el calendario y desde la alineación."),
    BloqueExplicativo(
        "bajas", "quién no puede jugar",
        ("bajas",),
        "Ausencias por lesión, sanción y duda, ordenadas por las dos medidas de "
        "importancia del ausente: el valor de mercado que se queda fuera y la "
        "cuota de minutos que venía disputando. No pesa igual perder a tres "
        "suplentes que a un titular indiscutible."),
    BloqueExplicativo(
        "contexto", "el contexto del encuentro",
        ("contexto_calendario", "contexto_geografia"),
        "Hora de saque, franja de emisión, fin de semana, distancia que recorre "
        "el visitante y si el partido es un derbi territorial."),
)

_ORDEN_BLOQUES = tuple(b.id for b in BLOQUES_EXPLICATIVOS)
_BLOQUE_POR_ID = {b.id: b for b in BLOQUES_EXPLICATIVOS}

# lineage.bloque -> bloque explicativo
_DESDE_LINEAGE: dict[str, str] = {
    origen: b.id for b in BLOQUES_EXPLICATIVOS for origen in b.bloques_lineage}

_lineage_cache: dict[str, str] | None = None


def _tabla_lineage() -> dict[str, str]:
    """Columna -> bloque analítico, leída una sola vez de la tabla de procedencia."""
    global _lineage_cache
    if _lineage_cache is None:
        from lineage import feature_lineage
        lin = feature_lineage()
        _lineage_cache = dict(zip(lin["columna"], lin["bloque"]))
    return _lineage_cache


def bloque_de(columna: str) -> str:
    """Bloque explicativo al que pertenece una columna de la matriz.

    Las columnas de features se resuelven por la tabla de procedencia. Los
    indicadores de faltante que fabrica el preprocesado —`flag_no_absences` y
    compañía— no son features de ninguna fuente, pero tampoco son huérfanos: cada
    uno señala el hueco de un bloque concreto, así que heredan el bloque de la
    columna cuya ausencia declaran. Agruparlos aparte separaría «no hay parte
    médico» de «hay parte médico y dice esto», que son la misma pregunta.
    """
    tabla = _tabla_lineage()
    if columna in tabla:
        origen = tabla[columna]
        if origen not in _DESDE_LINEAGE:
            raise KeyError(
                f"explain: el bloque analítico {origen!r} de la columna {columna!r} "
                f"no está asignado a ningún bloque explicativo; declárelo en "
                f"BLOQUES_EXPLICATIVOS")
        return _DESDE_LINEAGE[origen]

    import preprocess as pp
    if columna in pp.FLAG_BLOCKS:
        return bloque_de(pp.FLAG_BLOCKS[columna])

    raise KeyError(
        f"explain: la columna {columna!r} no aparece en la tabla de procedencia ni "
        f"es un indicador de faltante conocido; no se puede explicar una variable "
        f"cuyo origen no está documentado")


def mapa_bloques(columnas) -> dict[str, list[str]]:
    """Bloque explicativo -> columnas presentes, en el orden canónico.

    Solo se devuelven los bloques con al menos una columna: la poda por
    colinealidad puede retirar un bloque entero, y un bloque vacío en la
    descomposición sería una casilla a cero que el lector interpretaría como
    «no aporta» cuando lo cierto es «no está».
    """
    grupos: dict[str, list[str]] = {b: [] for b in _ORDEN_BLOQUES}
    for c in columnas:
        grupos[bloque_de(c)].append(str(c))
    return {b: cols for b, cols in grupos.items() if cols}


def block_registry(columnas=None) -> pd.DataFrame:
    """Los bloques explicativos, con su glosa y —si se pasan— sus columnas.

    Es la tabla que acompaña a cualquier gráfico de importancia: sin ella, el
    lector no sabe qué hay dentro de la barra rotulada «forma».
    """
    mapa = mapa_bloques(columnas) if columnas is not None else {}
    return pd.DataFrame([{
        "bloque": b.id,
        "titulo": b.titulo,
        "bloques_lineage": ", ".join(b.bloques_lineage),
        "n_columnas": len(mapa.get(b.id, [])) if columnas is not None else np.nan,
        "nota": b.nota,
    } for b in BLOQUES_EXPLICATIVOS])


# ===========================================================================
# 2. Coeficientes estandarizados
# ===========================================================================
def linear_coefficients(modelo, nombres) -> pd.DataFrame:
    """Coeficientes del modelo lineal, en la escala en la que son comparables.

    Devuelve una fila por variable con su coeficiente en cada clase y la magnitud
    con la que se ordena la tabla. La comparabilidad no la aporta esta función
    sino el preprocesado: como la matriz llega estandarizada, un coeficiente ya
    expresa el efecto de una desviación típica de la variable y no depende de sus
    unidades.

    Se admiten dos formas. La primera es el multinomial de `scikit-learn`, cuyas
    tres filas de `coef_` se reordenan al orden canónico H, D, A porque
    `classes_` depende de qué clases vio la ventana. La segunda es cualquier
    envoltorio que publique su propia tabla mediante un método `coeficientes`,
    que tiene prioridad: si un modelo declara sus coeficientes, es porque miden
    algo distinto de lo que mediría `coef_` en crudo y solo él sabe cómo
    nombrarlos.
    """
    if hasattr(modelo, "coeficientes"):
        return modelo.coeficientes(nombres)

    coef = getattr(modelo, "coef_", None)
    if coef is None:
        raise TypeError(
            f"{type(modelo).__name__} no expone coeficientes lineales; la lectura "
            f"por coeficientes solo aplica a la familia lineal, y para las "
            f"demás está la importancia por permutación y los valores de Shapley")

    coef = np.asarray(coef, dtype="float64")
    nombres = list(nombres)
    if coef.shape[1] != len(nombres):
        raise ValueError(f"el modelo tiene {coef.shape[1]} coeficientes y se han "
                         f"pasado {len(nombres)} nombres de variable")

    clases = list(getattr(modelo, "classes_", range(coef.shape[0])))
    tab = pd.DataFrame(index=nombres)
    for j, c in enumerate(clases):
        tab[_CLASES[int(c)]] = coef[j]
    for c in _CLASES:                       # una clase ausente de la ventana
        if c not in tab.columns:
            tab[c] = 0.0
    tab = tab[list(_CLASES)]
    tab["magnitud"] = tab.abs().max(axis=1)
    return tab.sort_values("magnitud", ascending=False)


def coefficients_by_block(tab: pd.DataFrame,
                          mapa: dict[str, list[str]] | None = None) -> pd.DataFrame:
    """Agrega la tabla de coeficientes por bloque explicativo.

    Se publican dos agregados porque miden cosas distintas y confundirlos es el
    error habitual: la SUMA de valores absolutos crece con el número de columnas
    del bloque y responde a «cuánto peso lineal hay aquí en total», mientras que
    el MÁXIMO responde a «cuál es la variable más fuerte de este bloque» y no
    premia al bloque por ser numeroso. Un bloque con setenta variantes de la misma
    magnitud domina el primero y no el segundo.
    """
    filas = list(tab.index)
    mapa = mapa if mapa is not None else mapa_bloques(
        [f for f in filas if f != "(constante)"])
    inverso = {c: b for b, cols in mapa.items() for c in cols}

    out = []
    for b, cols in mapa.items():
        sub = tab.loc[[c for c in cols if c in tab.index]]
        if sub.empty:
            continue
        mag = sub["magnitud"] if "magnitud" in sub.columns else sub.abs().max(axis=1)
        out.append({"bloque": b, "n_columnas": len(sub),
                    "suma_abs": float(mag.sum()), "max_abs": float(mag.max()),
                    "media_abs": float(mag.mean()),
                    "variable_dominante": str(mag.idxmax())})
    sin_bloque = [c for c in filas if c not in inverso and c != "(constante)"]
    if sin_bloque:
        raise KeyError(f"explain: variables sin bloque en la tabla de "
                       f"coeficientes: {sin_bloque[:6]}")
    return pd.DataFrame(out).sort_values("suma_abs", ascending=False) \
                            .reset_index(drop=True)


# ===========================================================================
# 3. Importancia por permutación sobre el bloque de evaluación
# ===========================================================================
def _predice(modelo, X: pd.DataFrame, contexto) -> np.ndarray:
    """Terna en el orden canónico H, D, A, con el contexto si el estimador lo pide.

    Se reutiliza la función del protocolo en lugar de reimplementarla: si el
    reordenamiento por `classes_` cambiara, la interpretación seguiría hablando
    del mismo modelo que la evaluación.
    """
    return pt._proba_ordenada(modelo, X, contexto)


def _perdida(nombre: str):
    if nombre not in ev.PERDIDAS:
        raise ValueError(f"pérdida desconocida: {nombre!r}; use una de "
                         f"{sorted(ev.PERDIDAS)}")
    fn, sentido = ev.PERDIDAS[nombre]
    if sentido != "min":
        raise ValueError(
            f"{nombre!r} se maximiza, y la importancia por permutación se define "
            f"como el DETERIORO de una pérdida; use rps, logloss o brier")
    return fn


def _y_eval(aj: pt.PliegueAjustado, feat: pd.DataFrame | None = None) -> np.ndarray:
    """Objetivo de las filas evaluadas del pliegue."""
    import columns as C
    if C.TARGET_NUM in aj.ev.columns:
        return aj.ev[C.TARGET_NUM].to_numpy(dtype="float64")
    if feat is None:
        raise KeyError(
            "el marco preprocesado del pliegue no conserva el objetivo; pase "
            "`feat` para poder leerlo de la matriz original")
    return feat.loc[aj.fold.eval_idx, C.TARGET_NUM].to_numpy(dtype="float64")


def _perdida_base(ajustados, fn, feat=None) -> tuple[float, int]:
    """Pérdida agrupada sobre todas las filas evaluadas, sin perturbar nada."""
    total, n = 0.0, 0
    for aj in ajustados:
        y = _y_eval(aj, feat)
        P = _predice(aj.modelo, aj.Xev, aj.ctx_ev)
        v = fn(y, P)
        total += float(np.nansum(v))
        n += int(np.isfinite(v).sum())
    if n == 0:
        raise ValueError("ninguna fila evaluable en los pliegues ajustados")
    return total / n, n


def _importancia(ajustados, grupos: dict[str, list[str]], *,
                 metrica: str = "rps", n_repeticiones: int = 5, seed: int = 7,
                 feat: pd.DataFrame | None = None) -> pd.DataFrame:
    """Deterioro de la pérdida al romper la información de cada grupo.

    El grupo se sustituye ENTERO por los valores de filas tomadas al azar de la
    VENTANA DE ENTRENAMIENTO del propio pliegue. Las dos decisiones tienen motivo:

      * sustituir el grupo entero, tomando todas sus columnas de la misma fila
        sorteada, conserva la correlación interna del bloque. Barajar cada columna
        por separado construiría partidos imposibles —un equipo con la forma de
        uno y los goles esperados de otro— y mediría la sensibilidad del modelo a
        combinaciones que nunca ocurren.

      * sortear en la ventana de entrenamiento, y no dentro de las filas
        evaluadas, es lo único viable cuando el pliegue es una jornada de diez
        partidos: barajar diez valores entre sí no rompe la asociación, la
        desplaza. La ventana comparte el estado de preprocesado del pliegue, de
        modo que el valor de reemplazo procede exactamente de la misma
        distribución transformada.

    La importancia es la diferencia entre la pérdida perturbada y la de partida.
    Positiva significa que romper el grupo empeora el pronóstico; en torno a cero,
    que el modelo no lo estaba usando o que otro grupo lo sustituye sin pérdida.
    Valores ligeramente negativos son ruido de sorteo, no una mejora.
    """
    fn = _perdida(metrica)
    base, n_filas = _perdida_base(ajustados, fn, feat)
    rng = np.random.default_rng(seed)

    ys = [_y_eval(aj, feat) for aj in ajustados]
    filas = []
    for nombre, cols in grupos.items():
        muestras = np.empty(n_repeticiones, dtype="float64")
        for r in range(n_repeticiones):
            total, n = 0.0, 0
            for aj, y in zip(ajustados, ys):
                presentes = [c for c in cols if c in aj.Xev.columns]
                if not presentes:
                    continue
                X = aj.Xev.copy()
                sorteo = rng.integers(0, len(aj.Xtr), size=len(X))
                X[presentes] = aj.Xtr[presentes].to_numpy()[sorteo]
                v = fn(y, _predice(aj.modelo, X, aj.ctx_ev))
                total += float(np.nansum(v))
                n += int(np.isfinite(v).sum())
            muestras[r] = total / n if n else np.nan
        filas.append({
            "grupo": nombre, "n_columnas": len(cols),
            "perdida_base": base, "perdida_rota": float(np.nanmean(muestras)),
            "importancia": float(np.nanmean(muestras)) - base,
            "ee": float(np.nanstd(muestras, ddof=1)) if n_repeticiones > 1 else np.nan,
        })

    out = pd.DataFrame(filas).sort_values("importancia", ascending=False)
    out.attrs["metrica"] = metrica
    out.attrs["n_filas"] = n_filas
    out.attrs["n_pliegues"] = len(ajustados)
    out.attrs["n_repeticiones"] = n_repeticiones
    return out.reset_index(drop=True)


def permutation_importance_blocks(ajustados, *, columnas=None,
                                  metrica: str = "rps",
                                  n_repeticiones: int = 5, seed: int = 7,
                                  feat: pd.DataFrame | None = None) -> pd.DataFrame:
    """Importancia por permutación con el BLOQUE como unidad. Es la lectura principal.

    `columnas` restringe el mapa a un subconjunto de la matriz. Es lo que necesita
    un modelo entrenado sobre un conjunto elegido a mano: romper bloques que el
    estimador no mira produciría una tabla llena de ceros que no dice nada y
    multiplicaría el coste por el número de bloques inútiles.
    """
    if not ajustados:
        raise ValueError("no se ha pasado ningún pliegue ajustado")
    cols = (list(ajustados[0].Xev.columns) if columnas is None
            else [c for c in columnas if c in set(ajustados[0].Xev.columns)])
    if not cols:
        raise ValueError("ninguna de las columnas pedidas está en la matriz evaluada")
    return _importancia(ajustados, mapa_bloques(cols), metrica=metrica,
                        n_repeticiones=n_repeticiones, seed=seed, feat=feat)


def permutation_importance(ajustados, *, columnas=None, metrica: str = "rps",
                           n_repeticiones: int = 5, seed: int = 7,
                           feat: pd.DataFrame | None = None) -> pd.DataFrame:
    """Importancia por permutación columna a columna.

    Es la lectura de detalle, y hay que leerla sabiendo lo que hace: con
    doscientas columnas fuertemente correlacionadas, romper una sola casi nunca
    empeora nada porque sus seis variantes siguen ahí. Una columna con importancia
    nula aquí y un bloque con importancia alta en la tabla anterior no es una
    contradicción sino la descripción exacta de una señal redundante.

    El coste crece con el número de columnas, de pliegues y de repeticiones, de
    modo que `columnas` permite restringirla a las que interesen —las de mayor
    coeficiente, típicamente— en lugar de recorrer la matriz entera.
    """
    if not ajustados:
        raise ValueError("no se ha pasado ningún pliegue ajustado")
    cols = list(columnas) if columnas is not None else list(ajustados[0].Xev.columns)
    return _importancia(ajustados, {c: [c] for c in cols}, metrica=metrica,
                        n_repeticiones=n_repeticiones, seed=seed, feat=feat)


# ===========================================================================
# 4. Valores de Shapley exactos por bloque
# ===========================================================================
def baseline_median(X: pd.DataFrame) -> pd.Series:
    """Partido de referencia: la mediana de cada variable en la ventana.

    Es el valor que toma un bloque cuando la coalición no lo incluye. La mediana,
    y no la media, porque sobre una matriz con colas —el valor de plantilla, el
    valor de las bajas— la media describe un partido que no se parece a ninguno.
    """
    return X.median(numeric_only=False)


def _pesos_shapley(B: int) -> np.ndarray:
    """Peso de una coalición de tamaño s en el reparto de Shapley: s!(B−s−1)!/B!."""
    return np.array([math.factorial(s) * math.factorial(B - s - 1)
                     / math.factorial(B) for s in range(B)], dtype="float64")


def shapley_blocks(modelo, X: pd.DataFrame, contexto, base: pd.Series,
                   mapa: dict[str, list[str]] | None = None) -> dict:
    """Reparto EXACTO de la probabilidad entre bloques, por enumeración completa.

    Para cada subconjunto S de bloques se evalúa el modelo sobre una matriz en la
    que los bloques de S llevan el valor observado del partido y los demás el
    valor de referencia. La aportación del bloque j es la media ponderada de lo
    que añade al entrar en cada coalición posible, con los pesos que hacen del
    reparto el único que cumple simetría, nulidad, aditividad y eficiencia.

    La escala del reparto es la PROBABILIDAD, no su logaritmo. Es una decisión de
    legibilidad con una consecuencia exacta: las aportaciones de un partido suman,
    clase a clase, la diferencia entre su pronóstico y el de referencia, de modo
    que la explicación se puede escribir en puntos porcentuales sin aproximar
    nada. `verifica_eficiencia` lo comprueba sobre la salida.

    Devuelve un diccionario con la matriz de aportaciones `phi` de forma
    (n_partidos, n_bloques, 3), los nombres de bloque en su orden, el pronóstico
    de referencia y el pronóstico completo.
    """
    mapa = mapa if mapa is not None else mapa_bloques(X.columns)
    bloques = list(mapa.keys())
    B = len(bloques)
    if B == 0:
        raise ValueError("no hay ningún bloque con columnas en la matriz")
    if B > MAX_BLOQUES_SHAPLEY:
        raise ValueError(
            f"la enumeración exacta cubre hasta {MAX_BLOQUES_SHAPLEY} bloques y se "
            f"han pedido {B} ({2 ** B} coaliciones). Agrupe más: la unidad de esta "
            f"descomposición es el bloque, no la columna")

    faltan = [c for cols in mapa.values() for c in cols if c not in X.columns]
    if faltan:
        raise KeyError(f"columnas del mapa ausentes en la matriz: {faltan[:6]}")
    sin_base = [c for cols in mapa.values() for c in cols if c not in base.index]
    if sin_base:
        raise KeyError(f"columnas sin valor de referencia: {sin_base[:6]}")

    n = len(X)
    Xn = X.to_numpy(dtype="float64")
    pos = {c: i for i, c in enumerate(X.columns)}
    idx_bloque = [np.array([pos[c] for c in mapa[b]], dtype="int64") for b in bloques]
    Bn = np.tile(base.reindex(X.columns).to_numpy(dtype="float64"), (n, 1))

    # v[m] = pronóstico con los bloques del mapa de bits `m` en su valor observado
    v = np.empty((1 << B, n, 3), dtype="float64")
    for m in range(1 << B):
        M = Bn.copy()
        for j in range(B):
            if m >> j & 1:
                M[:, idx_bloque[j]] = Xn[:, idx_bloque[j]]
        v[m] = _predice(modelo, pd.DataFrame(M, columns=X.columns, index=X.index),
                        contexto)

    pesos = _pesos_shapley(B)
    tam = np.array([bin(m).count("1") for m in range(1 << B)], dtype="int64")
    phi = np.zeros((n, B, 3), dtype="float64")
    for j in range(B):
        bit = 1 << j
        sin_j = np.flatnonzero((np.arange(1 << B) & bit) == 0)
        phi[:, j, :] = np.einsum("m,mnk->nk", pesos[tam[sin_j]],
                                 v[sin_j | bit] - v[sin_j])

    return {"phi": phi, "bloques": bloques, "base": v[0], "completo": v[-1],
            "columnas": {b: list(mapa[b]) for b in bloques}}


def verifica_eficiencia(sh: dict, tol: float = 1e-9) -> float:
    """Comprueba la propiedad que hace publicable el reparto y devuelve el residuo.

    La suma de las aportaciones de todos los bloques tiene que ser exactamente la
    diferencia entre el pronóstico del partido y el de referencia. Si no lo es, la
    explicación que se publique al lector no cuadra con el número que se le
    muestra, y eso no es un detalle numérico sino un fallo de honestidad.
    """
    resid = np.abs(sh["phi"].sum(axis=1) - (sh["completo"] - sh["base"])).max()
    if resid > tol:
        raise AssertionError(
            f"el reparto de Shapley no cierra: residuo máximo {resid:.3e} > {tol:.0e}. "
            f"Las aportaciones publicadas no sumarían el pronóstico publicado")
    return float(resid)


def shapley_global(sh: dict) -> pd.DataFrame:
    """Importancia global: media de la aportación absoluta de cada bloque.

    Se promedia el valor ABSOLUTO porque un bloque que empuja hacia el local en
    la mitad de los partidos y hacia el visitante en la otra mitad es muy
    importante y su aportación media sería cero. Se publica además la aportación
    con signo, que responde a la pregunta complementaria: si el bloque tiende a
    favorecer sistemáticamente a un resultado.
    """
    phi = sh["phi"]
    filas = []
    for j, b in enumerate(sh["bloques"]):
        fila = {"bloque": b, "n_columnas": len(sh["columnas"][b]),
                "importancia": float(np.abs(phi[:, j, :]).mean(axis=0).sum())}
        for k, clase in enumerate(_CLASES):
            fila[f"abs_{clase}"] = float(np.abs(phi[:, j, k]).mean())
            fila[f"medio_{clase}"] = float(phi[:, j, k].mean())
        filas.append(fila)
    return pd.DataFrame(filas).sort_values("importancia", ascending=False) \
                              .reset_index(drop=True)


# ===========================================================================
# 5. La lectura local: un partido concreto
# ===========================================================================
def explain_match(sh: dict, fila: int = 0, *, clase: str | None = None,
                  top: int = 4) -> pd.DataFrame:
    """Descomposición del pronóstico de un partido, bloque a bloque.

    Devuelve una fila por bloque con su aportación en puntos de probabilidad a
    cada resultado, ordenada por la magnitud con la que interviene. Si se declara
    una `clase`, el orden es el de la aportación a ese resultado, que es lo que
    se quiere cuando la explicación acompaña a una apuesta concreta.

    `top` no recorta la tabla: marca en la columna `dominante` los bloques que la
    frase debe mencionar. Que la tabla completa siga disponible es deliberado —el
    anexo de la memoria la necesita entera— y que la frase mencione tres o cuatro
    también, porque una explicación de ocho términos no explica.
    """
    phi = sh["phi"][fila]
    tab = pd.DataFrame(phi, index=sh["bloques"], columns=list(_CLASES))
    tab.insert(0, "titulo", [_BLOQUE_POR_ID[b].titulo for b in sh["bloques"]])
    if clase is not None:
        if clase not in _CLASES:
            raise ValueError(f"clase desconocida: {clase!r}; use una de {_CLASES}")
        tab["orden"] = tab[clase]
        tab = tab.sort_values("orden", key=lambda s: s.abs(), ascending=False)
    else:
        tab["orden"] = tab[list(_CLASES)].abs().max(axis=1)
        tab = tab.sort_values("orden", ascending=False)
    tab["dominante"] = False
    tab.iloc[:top, tab.columns.get_loc("dominante")] = True
    tab.attrs["base"] = sh["base"][fila]
    tab.attrs["completo"] = sh["completo"][fila]
    return tab


def narrate(sh: dict, fila: int = 0, *, clase: str | None = None,
            top: int = 3, umbral: float = 0.01) -> str:
    """Convierte la descomposición en la frase que lee un usuario.

    El resultado explicado es, por defecto, el más probable. Solo se mencionan los
    bloques cuya aportación supera `umbral` —un punto porcentual— porque enumerar
    aportaciones de dos milésimas da una falsa impresión de precisión, y si
    ninguno lo supera la frase lo dice: el pronóstico es el del partido medio y no
    hay nada que destacar.
    """
    p = sh["completo"][fila]
    k = int(np.argmax(p)) if clase is None else _CLASES.index(clase)
    nombre = _CLASES[k]
    aporta = sh["phi"][fila, :, k]
    orden = np.argsort(-np.abs(aporta))

    piezas = []
    for j in orden[:top]:
        if abs(aporta[j]) < umbral:
            continue
        signo = "a favor" if aporta[j] > 0 else "en contra"
        piezas.append(f"{_BLOQUE_POR_ID[sh['bloques'][j]].titulo} "
                      f"{signo} ({aporta[j]:+.1%})")

    cabeza = (f"{_ETIQUETA_CLASE[nombre].capitalize()} con probabilidad "
              f"{p[k]:.1%} (referencia {sh['base'][fila][k]:.1%})")
    if not piezas:
        return (f"{cabeza}. Ningún bloque se aparta apreciablemente del partido de "
                f"referencia: el pronóstico es prácticamente el del equipo medio en "
                f"la situación media.")
    return f"{cabeza}. Pesan sobre todo: " + "; ".join(piezas) + "."


# ===========================================================================
# 6. Concordancia entre los tres relatos
# ===========================================================================
def agreement(relatos: dict[str, pd.Series]) -> dict:
    """Pone los tres órdenes de importancia uno al lado del otro y los compara.

    `relatos` es un diccionario de nombre -> serie indexada por bloque. Se
    devuelve la tabla de rangos —1 es el bloque más importante según ese relato—
    y la matriz de correlación de Spearman entre ellos.

    La concordancia alta significa que la conclusión no depende del método y puede
    afirmarse sin matizar. La baja no invalida nada: significa que los métodos
    responden a preguntas distintas y que hay que decir cuál se está respondiendo.
    El caso instructivo es el bloque de mercado, que suele encabezar coeficientes
    y Shapley y quedar más atrás en permutación cuando otro bloque puede
    reconstruir su información.

    Solo se comparan los bloques que aparecen en todos los relatos: un rango
    calculado sobre conjuntos distintos de bloques no mide lo mismo en cada
    columna y la correlación entre ellos no significaría nada. Las filas se
    publican en el orden canónico de los bloques, el mismo con el que se
    imprimen todas las tablas del módulo, para que la comparación visual entre
    relatos no dependa del orden alfabético de sus nombres.
    """
    if len(relatos) < 2:
        raise ValueError("la concordancia necesita al menos dos relatos")
    tab = pd.DataFrame(relatos)
    faltan = tab.isna().any(axis=1)
    if faltan.any():
        tab = tab.loc[~faltan]
    if tab.empty:
        raise ValueError("los relatos no comparten ningún bloque")
    presentes = set(tab.index)
    orden = [b for b in _ORDEN_BLOQUES if b in presentes]
    orden += [b for b in tab.index if b not in _BLOQUE_POR_ID]
    tab = tab.loc[orden]

    rangos = tab.rank(ascending=False, method="average")
    rangos.columns = [f"rango_{c}" for c in rangos.columns]
    concordancia = tab.corr(method="spearman")
    return {"valores": tab, "rangos": rangos, "spearman": concordancia,
            "orden": {c: list(tab[c].sort_values(ascending=False).index)
                      for c in tab.columns}}


# ===========================================================================
# 7. Informe
# ===========================================================================
def explain_report(ajustados, *, columnas=None, metrica: str = "rps",
                   n_repeticiones: int = 5, seed: int = 7,
                   feat: pd.DataFrame | None = None,
                   verbose: bool = True) -> dict:
    """Los tres relatos globales sobre un mismo conjunto de pliegues ajustados.

    La descomposición de Shapley se calcula sobre el ÚLTIMO pliegue de la lista
    —el más reciente— y no sobre todos: es una descripción del modelo tal y como
    queda al final del recorrido, y promediar repartos calculados con modelos
    distintos mezclaría lo que cada uno hace. La importancia por permutación, en
    cambio, sí agrupa todos los pliegues, porque ahí lo que se promedia es una
    pérdida y no un coeficiente.

    `columnas` restringe los tres relatos a un subconjunto de la matriz, que es lo
    que necesita un modelo entrenado sobre un conjunto elegido a mano. Sin esa
    restricción, el mapa de bloques incluiría bloques que el estimador ni siquiera
    mira: la permutación les daría importancia cero y la enumeración de coaliciones
    de Shapley crecería en potencias de dos por cada bloque vacío. Una tabla de
    ceros no se lee como «ese bloque no está» sino como «ese bloque no importa», y
    las dos cosas son distintas.
    """
    if not ajustados:
        raise ValueError("no se ha pasado ningún pliegue ajustado")
    ult = ajustados[-1]
    presentes = list(map(str, ult.Xev.columns))
    cols = (presentes if columnas is None
            else [c for c in presentes if c in set(map(str, columnas))])
    if not cols:
        raise ValueError("ninguna de las columnas pedidas está en la matriz evaluada")
    mapa = mapa_bloques(cols)

    perm = permutation_importance_blocks(
        ajustados, columnas=cols, metrica=metrica,
        n_repeticiones=n_repeticiones, seed=seed, feat=feat)

    sh = shapley_blocks(ult.modelo, ult.Xev, ult.ctx_ev,
                        baseline_median(ult.Xtr), mapa)
    verifica_eficiencia(sh)
    shap = shapley_global(sh)

    relatos = {"permutacion": perm.set_index("grupo")["importancia"],
               "shapley": shap.set_index("bloque")["importancia"]}
    try:
        coef = coefficients_by_block(
            linear_coefficients(ult.modelo, list(ult.Xev.columns)), mapa)
        relatos["coeficientes"] = coef.set_index("bloque")["suma_abs"]
    except TypeError:
        coef = None            # la familia no es lineal: dos relatos, no tres

    conc = agreement(relatos)
    res = {"permutacion": perm, "shapley": shap, "coeficientes": coef,
           "concordancia": conc, "bloques": block_registry(cols),
           "descomposicion": sh}

    if verbose:
        print("=" * 72)
        print("INTERPRETABILIDAD")
        print("=" * 72)
        print(f"  pliegues interpretados: {len(ajustados)}   "
              f"partidos evaluados: {perm.attrs['n_filas']}")
        print(f"  bloques con columnas: {len(mapa)}   "
              f"coaliciones enumeradas: {2 ** len(mapa)}")
        print(f"\nImportancia por permutación sobre el bloque de evaluación "
              f"({metrica}):")
        for _, r in perm.iterrows():
            print(f"  {r.grupo:16s} {r.importancia:+.5f}  ±{r.ee:.5f}  "
                  f"({int(r.n_columnas):>3} cols)")
        print("\nAportación media de Shapley (probabilidad):")
        for _, r in shap.iterrows():
            print(f"  {r.bloque:16s} {r.importancia:.4f}")
        print("\nConcordancia entre relatos (Spearman):")
        print(conc["spearman"].round(3).to_string())
        print("=" * 72)
    return res


if __name__ == "__main__":       # pragma: no cover - guía de uso
    print(__doc__)
    print(block_registry().to_string(index=False))
