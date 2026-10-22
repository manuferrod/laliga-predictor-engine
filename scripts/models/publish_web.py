"""Proyección de lo evaluado al contrato de salidas de la aplicación.

`build_models.py` produce evidencia: tablas de métricas, predicciones evaluadas,
repartos de importancia. Este módulo la convierte en el material que consume la
web —predicciones de la próxima jornada, series por temporada y por equipo,
explicaciones de partido, ejes de radar, ficha del modelo— sin volver a decidir
nada. La separación es deliberada y responde a una regla del trabajo: **quien
mide no publica y quien publica no mide**. Si la proyección pudiera reelegir el
modelo o recalcular una métrica, la cifra que ve el lector dejaría de ser la que
el protocolo produjo, y ninguna auditoría posterior podría distinguirlo.

De ahí que aquí solo ocurran tres clases de operación: leer lo publicado, derivar
magnitudes que son función determinista de una terna de probabilidades —clase
predicha, confianza, entropía, margen— y **emitir** la jornada que aún no se ha
jugado, que es lo único que no existía antes y que por tanto hay que calcular.

La emisión merece un párrafo. El modelo que puntúa la próxima jornada se ajusta
con el estado de preprocesado **congelado** que escribió `build_preprocess.py`
para el régimen semanal, que es exactamente el mismo con el que
`build_predict.py` construyó la matriz de inferencia. Ajustar con otro estado
—aunque fuese «mejor»— produciría una predicción cuyo rendimiento histórico no
sería el medido, porque el objeto evaluado y el objeto ejecutado serían
distintos. Por eso la paridad de columnas se comprueba antes de puntuar y no
después: es una condición de emisión, no un diagnóstico.

El mismo argumento se aplica a la **configuración** del estimador. El modelo
publicado no es solo una familia: es esa familia sobre el conjunto de variables
cerrado y con los hiperparámetros afinados sobre él. Emitir con otras variables o
con otros valores publicaría una predicción distinta de la que se midió, con la
agravante de que el nombre sería el mismo y nadie lo notaría. Por eso aquí no se
elige nada: las tres piezas se **leen** de `build_models.py`, que es donde el
cuaderno 05 las dejó escritas, y viajan hasta la ficha del modelo para que quien
lea la aplicación vea exactamente sobre qué se pronuncia.

Normalización de los gráficos
-----------------------------
Los ejes del radar se normalizan con rangos **declarados en el código y
versionados** (`VERSION_NORMALIZACION`). No se normaliza contra el mínimo y el
máximo observados, que es lo cómodo, porque entonces el mismo equipo con el mismo
rendimiento aparecería distinto según la temporada que se estuviese mostrando y
los gráficos históricos dejarían de ser comparables entre sí sin que nada lo
advirtiese. Cambiar un rango obliga a subir la versión, y la versión viaja en el
artefacto.

El radar y la línea temporal comparten una restricción que no es estética: ambos
dibujan **un equipo**, no un partido, y por tanto solo pueden alimentarse de
magnitudes que lleguen a la matriz por separado para el local y para el
visitante. Desde que el catálogo decide lado por lado, la mayoría de los
atributos de equipo viaja únicamente como diferencia, de la que no se puede
recuperar el valor de ninguno de los dos. Declarar aquí uno de esos atributos no
rompería nada de forma visible: la columna `home_...` sencillamente no existiría
y el gráfico se publicaría vacío. Por eso las dos declaraciones se contrastan
contra el catálogo al importar el módulo, y un eje que el catálogo no emita por
lados detiene la publicación en vez de vaciarla.

Artefactos (bajo `outputs/web/`)
--------------------------------
Los cinco primeros solo se escriben cuando hay jornada pendiente; el resto se
publica siempre, porque describen lo ya medido y no dependen de que queden
partidos por jugar.

    proxima_jornada.csv / .json      predicciones emitidas, con cuota, valor
                                     esperado, Kelly y cobertura por bloque
    explicaciones_jornada.parquet    aportación por bloque de cada partido emitido
    narrativa_jornada.parquet        la frase que acompaña a cada predicción
    radar_jornada.parquet            ejes normalizados de los dos equipos
    ejes_radar.parquet               rango declarado y sentido de cada eje
    metricas_temporada.parquet       modelo y mercado en las mismas filas
    matchlogs.parquet                registro de partido con retorno acumulado
    predicciones.parquet             una fila por partido: terna del modelo, terna
                                     de las tres referencias, signo y resultado
    economia_temporada.parquet       rendimiento del modelo y del favorito de
                                     Bet365 en cada temporada, y su diferencia
    economia_jornada.parquet         las dos curvas de rendimiento acumulado
                                     jornada a jornada, y la distancia entre ellas
    clasificacion_por_clase.parquet  precisión, cobertura y F1 por resultado
    roc_ovr.parquet                  áreas bajo la curva uno-contra-resto
    confusion.parquet                aciertos y confusiones entre clases
    fiabilidad.parquet               probabilidad declarada frente a observada
    desacuerdo_mercado.parquet       dónde y cuánto se separa el modelo del precio
    importancia_bloques.parquet      reparto de importancia por bloque explicativo
    perfiles_equipo.parquet          serie por equipo y jornada
    ficha_modelo.json                especificación, protocolo, límites
    manifiesto_web.json              qué se publicó, cuándo y con qué ejecución

Uso
---
    python scripts/models/publish_web.py           # publica
    python scripts/models/publish_web.py --dry     # calcula y audita, no escribe
    python scripts/models/publish_web.py --sin-emision   # solo la parte histórica
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
_EXTRACTORS = _HERE.parent / "extractors"
for _p in (_HERE, _FEATURES, _EXTRACTORS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from _common import write_df_required  # noqa: E402
import backtest as bt  # noqa: E402
import build_models as bm  # noqa: E402
import catalogo as cat  # noqa: E402
import columns as C  # noqa: E402
import evaluate as ev  # noqa: E402
import explain as ex  # noqa: E402
import laboratorio as lb  # noqa: E402
import lineage as lg  # noqa: E402
import preprocess as pp  # noqa: E402
import protocol as pt  # noqa: E402
import qa_models as qa  # noqa: E402

_ROOT = _HERE.parent.parent
PROC = _ROOT / "data" / "02_processed"
FEATURES = PROC / "features_matches.parquet"
MASTER = PROC / "master_matches.parquet"
PREDICT = PROC / "features_predict.parquet"
UPCOMING = PROC / "features_upcoming.parquet"
WEB = _ROOT / "outputs" / "web"

# El único régimen publicable: el que no mira nada que no exista al predecir.
REGIMEN = "semanal"

# Conjunto de cuotas que acompaña a la predicción emitida. Tiene que ser el de
# APERTURA y no el de cierre: es el precio que un lector encontraría al leer la
# predicción. El histórico se calcula además contra el cierre, y la diferencia
# entre ambas lecturas se publica en la ficha del modelo como limitación.
CUOTAS_EMISION = "consenso"

# Versión de la normalización de los ejes. Se sube cuando cambia cualquier rango
# o cualquier eje; los artefactos la llevan dentro para que un gráfico antiguo
# nunca se compare con uno nuevo sin que se note. La 2.0 es la curación del
# catálogo: los ejes pasaron a leerse de las medias exponenciales, que son las
# que sobreviven por lados, y las escalas cambiaron con ellos.
VERSION_NORMALIZACION = "2.0"


def _columnas_del_catalogo() -> set[str]:
    """Los nombres de columna que el catálogo declara para la matriz de partido.

    Son dos poblaciones y hay que unirlas porque una base puede pertenecer a
    cualquiera de las dos. Unas columnas se copian del máster con el nombre ya
    puesto —`home_elo_z`, `home_squad_value`, `home_fat_rest_days`— y viven en
    `columns.PREMATCH_PASSTHROUGH`; otras son atributos de equipo que el catálogo
    proyecta a los lados que declara, y sus nombres solo existen al pedírselos a
    `catalogo.nombres()`. Consultar solo la segunda —que es el error natural,
    porque `catalogo.lados()` está a mano— daría por inexistente al Elo, que sí
    llega a la matriz.
    """
    return set(C.PREMATCH_PASSTHROUGH) | {
        n for b in cat.LADOS for n in cat.nombres(b)}


def _exige_por_lado(bases: list[str], donde: str) -> None:
    """Aborta si alguna base declarada no llega a la matriz separada por lados.

    Es una comprobación de declaración, no de datos: se hace al importar, contra
    el catálogo, y no contra la matriz que llegue en tiempo de ejecución. La
    distinción importa porque los dos fallos posibles no son el mismo. Que una
    columna falte en una temporada concreta —porque la fuente no cubría ese
    tramo— es una circunstancia legítima, y ahí el gráfico prefiere omitir el eje
    a dibujarlo a cero. Que una base esté declarada sin que el catálogo la emita
    por lados es, en cambio, un error de quien escribió la lista: no hay dato que
    esperar, ninguna ejecución lo arreglará, y el único síntoma sería un
    artefacto silenciosamente vacío.
    """
    declaradas = _columnas_del_catalogo()
    malas = [b for b in bases
             if not {f"home_{b}", f"away_{b}"} <= declaradas]
    if malas:
        raise ValueError(
            f"{donde}: el catálogo no emite estas bases por lados, de modo que "
            f"no pueden dibujar a un equipo: {malas}. Si hacen falta, hay que "
            f"declararlas como PAR en catalogo.py; si no, sobran aquí.")


@dataclass(frozen=True)
class EjeRadar:
    """Un eje del radar pre-partido, con su rango declarado.

    `invertido` señala las magnitudes en las que menos es mejor —la posición en
    la tabla, el valor de mercado de las bajas, el número de partidos recientes—
    para que el radar se lea siempre igual: más área, mejor situación. Sin esa
    marca, un equipo con muchas bajas dibujaría un pico y el lector lo
    interpretaría como una fortaleza.
    """
    id: str
    titulo: str
    base: str                  # nombre sin el prefijo home_/away_
    minimo: float
    maximo: float
    invertido: bool = False


# Los ocho ejes cubren las cuatro preguntas que un lector se hace antes de un
# partido —cuánto vale este equipo, en qué forma llega, con qué gente y con
# cuánto descanso— y dentro de la forma enfrentan lo conseguido con lo merecido:
# `puntos` contra `merito` y el gol esperado a favor contra el esperado en
# contra. Ese contraste es lo que distingue al equipo que va bien del que va
# acertado, y es información que un radar con una sola medida de rendimiento no
# puede dar.
#
# La forma se lee de las medias exponenciales y no de las ventanas rectangulares
# porque son las únicas que el catálogo emite por lados: la ventana de tres
# viaja solo como diferencia, y una diferencia no dibuja a un equipo.
EJES_RADAR = (
    # La fuerza de ClubElo se dibuja TIPIFICADA (desviaciones respecto a la media
    # de Primera ese día) y no en puntos de rating: un eje 1300-2000 era el de una
    # escala que la fuente cambió en 2022, de modo que los equipos de antes y de
    # después del cambio dibujarían áreas distintas por un motivo ajeno al fútbol
    # (ver `consolidate/spine.add_clubelo`).
    #
    # El rango declarado es el observado en el histórico completo, redondeado hacia
    # fuera por el lado que lo necesita: sobre las 7.600 filas equipo-partido del
    # maestro la fuerza tipificada va de -1.869 a 3.015. El suelo se baja a -2.0
    # para que el eje no empiece justo en el mínimo histórico; el techo se queda en
    # 3.0, y la única fila que lo rebasa la recorta `_normaliza` a 1.0 sin que el
    # dibujo cambie —es el líder destacado de una jornada, que ya ocupa el eje
    # entero—. No es un intervalo redondo porque la escala tampoco lo es.
    EjeRadar("elo", "Fuerza relativa (Elo)", "elo_z", -2.0, 3.0),
    EjeRadar("plantilla", "Valor de plantilla", "squad_value", 20.0, 900.0),
    EjeRadar("puntos", "Puntos por partido", "form_points_ewm", 0.0, 3.0),
    EjeRadar("merito", "Puntos esperados", "form_xpts_ewm", 0.0, 3.0),
    EjeRadar("ataque", "Goles esperados a favor", "form_npxg_ewm", 0.0, 2.5),
    EjeRadar("defensa", "Goles esperados en contra", "form_npxga_ewm",
             0.0, 2.5, True),
    EjeRadar("bajas", "Plantilla ausente", "out_value_pct", 0.0, 0.35, True),
    EjeRadar("descanso", "Días de descanso", "fat_rest_days", 2.0, 10.0),
)
_exige_por_lado([e.base for e in EJES_RADAR], "EJES_RADAR")


# ===========================================================================
# 1. Magnitudes derivadas de una terna
# ===========================================================================
def derivadas_terna(P: np.ndarray) -> pd.DataFrame:
    """Clase predicha, confianza, entropía y margen a partir de las tres probabilidades.

    Las cuatro son función determinista de la terna y por eso se derivan aquí en
    lugar de guardarse: recalcularlas siempre desde la misma fuente elimina la
    posibilidad de que una tabla publique una confianza que no case con sus
    propias probabilidades.

    La entropía se expresa en base tres —normalizada por `log 3`— para que valga
    uno en el máximo desconocimiento y cero en la certeza, independientemente del
    número de clases. La lectura resultante es directamente un porcentaje de
    incertidumbre, que es lo que la interfaz quiere mostrar. El margen entre las
    dos clases más probables dice algo distinto y complementario: un pronóstico
    puede tener poca entropía y poco margen —dos resultados muy probables y uno
    descartado— y esa situación merece señalarse.
    """
    P = np.asarray(P, dtype="float64")
    if P.ndim != 2 or P.shape[1] != 3:
        raise ValueError("se espera una matriz N×3 de probabilidades")
    orden = np.sort(P, axis=1)
    k = P.argmax(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        ent = -(P * np.log(np.where(P > 0, P, 1.0))).sum(axis=1) / np.log(3.0)
    return pd.DataFrame({
        "clase": [ev.CLASES[i] for i in k],
        "confianza": P.max(axis=1),
        "entropia": np.clip(ent, 0.0, 1.0),
        "margen": orden[:, 2] - orden[:, 1],
    })


# ===========================================================================
# 2. Ajuste de emisión
# ===========================================================================
def configuracion_emision(manifiesto: dict, temporada: str, *,
                          sello: dict | None = None) -> tuple:
    """El conjunto y los valores con los que se emite, y de dónde salen.

    Aquí no se busca nada. El conjunto ya viene cerrado en `build_models` —escrito
    a mano o recogido de `seleccion.carga()` al importar, que para esta función es
    lo mismo: en ambos casos es una decisión tomada antes y fuera de la emisión—, y
    lo único que hace esta función es comprobar que lo declarado coincide con lo
    que dice el artefacto que se está publicando. La comprobación no es ceremonia.
    Un conjunto que se toca en el código después de haber ejecutado el recorrido
    dejaría en disco unas métricas que describen a un modelo y una emisión que
    ejecuta a otro, y como el nombre de la familia sería el mismo, nada en la
    aplicación lo delataría.

    Devuelve las columnas efectivas y el motivo, que viaja hasta la ficha del
    modelo.
    """
    temporada = str(temporada)
    declarado = list(bm.CONJUNTO)
    fuentes = [("el sello del holdout", (sello or {}).get("conjunto")),
               ("el manifiesto", (manifiesto.get("conjunto") or {}).get("variables"))]
    for etiqueta, cols in fuentes:
        if not cols:
            continue
        if sorted(map(str, cols)) != sorted(map(str, declarado)):
            raise ValueError(
                f"el conjunto que declara build_models.py ({len(declarado)} "
                f"variables) no es el que produjo las cifras según {etiqueta} "
                f"({len(cols)} variables). Publicar así emitiría un modelo distinto "
                f"del medido: vuelve a ejecutar build_models.py")
        return declarado, (f"conjunto declarado en build_models.py, verificado "
                           f"contra {etiqueta} ({len(declarado)} variables) para "
                           f"la temporada {temporada}")
    if not declarado:
        raise ValueError(
            "build_models.py no declara ningún conjunto —ni escrito ni recogido de "
            "la búsqueda— y el manifiesto tampoco trae variables: no hay modelo "
            "que emitir")
    return declarado, ("conjunto declarado en build_models.py; el artefacto no "
                       "trae la lista con la que compararlo")


def ajusta_emision(feat: pd.DataFrame, nombre: str, *,
                   columnas=None, state_path: Path | None = None,
                   verbose: bool = True):
    """Entrena el modelo publicado sobre todo el pasado disponible.

    A diferencia de un pliegue de validación, aquí no hay nada que reservar: los
    partidos ya disputados —los de desarrollo y los del holdout que ya se han
    jugado— son pasado legítimo en el momento de emitir, y excluirlos empeoraría
    la predicción sin proteger nada, porque la cifra publicada del holdout ya se
    calculó y no se recalcula.

    El estado del preprocesado no se reajusta: se carga el congelado. Es la
    condición que hace que la matriz de inferencia y la de entrenamiento hablen
    el mismo idioma, y comprobarla es más importante que optimizarla.

    El estimador se arma exactamente igual que en el recorrido medido —familia,
    valores y conjunto de variables de `build_models`— porque cualquiera de las
    tres piezas que se desviara publicaría un modelo distinto del que la tabla
    dice haber medido, y la diferencia no daría error: el ajuste funcionaría y
    predeciría otra cosa.
    """
    from build_preprocess import state_path as _ruta_estado
    st = pp.PreprocessState.load(state_path or _ruta_estado(REGIMEN))
    if st.regime != REGIMEN:
        raise ValueError(f"el estado congelado es del régimen «{st.regime}» y la "
                         f"emisión exige «{REGIMEN}»")

    if nombre != bm.FAMILIA:
        raise ValueError(
            f"el artefacto dice que el modelo publicado es «{nombre}» y "
            f"build_models.py declara «{bm.FAMILIA}»; emitir así publicaría un "
            f"rendimiento histórico que no corresponde al modelo emitido")
    cols = list(columnas) if columnas else list(bm.CONJUNTO)
    if not cols:
        raise ValueError(
            "no hay conjunto con el que emitir: build_models.py no declara "
            "CONJUNTO, la búsqueda no ha dejado uno utilizable y tampoco se ha "
            "pasado ninguno a mano")

    jugados = feat[feat[C.TARGET_NUM].notna()].copy()
    tr = pp.apply(jugados, st, keep_meta=True)
    Xtr = tr[st.feature_cols]
    ytr = jugados[C.TARGET_NUM].astype("int64")
    fechas = pd.to_datetime(jugados["date"])

    cfg = replace(bm.PROTOCOLO, regime=REGIMEN)
    modelo = lb.fabrica(nombre, cols, bm.hiper_efectivo())()
    # La semivida es la del protocolo con el que se midió, no una elegida aquí:
    # ajustar con otra publicaría un modelo distinto del que la tabla describe, y
    # la diferencia sería invisible, porque el ajuste no falla, solo pondera otra
    # cosa.
    semivida = cfg.halflife_days
    w = pt.recency_weights(fechas, fechas.max() + pd.Timedelta(days=1), semivida)
    ctx = pt._contexto(tr, fechas)
    extra = {"contexto": ctx} if pt._acepta(modelo.fit, "contexto") else {}
    if w is None:
        modelo.fit(Xtr, ytr, **extra)
    else:
        modelo.fit(Xtr, ytr, sample_weight=w, **extra)

    ausentes = getattr(modelo, "columnas_ausentes_", ())
    if ausentes:
        raise KeyError(
            f"{len(ausentes)} variables del conjunto declarado no existen en la "
            f"matriz preprocesada de emisión: {list(ausentes)[:8]}. En un pliegue "
            f"esto se tolera porque la poda de colinealidad varía de ventana en "
            f"ventana, pero al emitir significaría publicar un modelo recortado "
            f"sin decirlo")

    if verbose:
        peso = "sin peso de recencia" if semivida is None else \
            f"semivida de {float(semivida):g} días"
        print(f"emisión: «{nombre}» sobre {len(modelo.columnas_)} variables "
              f"declaradas, ajustado con {len(Xtr)} partidos disputados "
              f"({fechas.min().date()} -> {fechas.max().date()}) · {peso}")
    return modelo, st, Xtr


def puntua_jornada(modelo, st, X: pd.DataFrame, up: pd.DataFrame,
                   Xtr: pd.DataFrame) -> pd.DataFrame:
    """Puntúa la matriz de inferencia tras comprobar la paridad de esquema.

    La comprobación va delante del cálculo por una razón operativa: si el orden
    de columnas no coincide, el estimador no falla —aplica cada coeficiente a la
    columna equivocada y devuelve números plausibles—, de modo que un error
    detectado después ya se habría publicado.
    """
    cols = list(st.feature_cols)
    fallos = qa.check_orden_columnas(list(Xtr.columns), [c for c in cols
                                                         if c in X.columns])
    fallos += qa.check_regimen(cols, REGIMEN)
    if fallos:
        raise qa.PublicacionError("emisión abortada:\n  - " + "\n  - ".join(fallos))

    Xev = X[cols]
    ctx = pt._contexto(X, pd.to_datetime(up["date"]))
    P = pt._proba_ordenada(modelo, Xev, ctx)
    return P


# ===========================================================================
# 3. Cobertura por bloque de cada predicción emitida
# ===========================================================================
def cobertura_por_partido(up: pd.DataFrame) -> pd.DataFrame:
    """Qué bloques de información llegaron completos para cada partido emitido.

    Es la mejora de trazabilidad llevada hasta la interfaz: permite que la ficha
    de un partido diga «esta predicción se emitió sin parte médico» en lugar de
    presentar una probabilidad indistinguible de las demás. Un modelo que imputa
    un bloque ausente sigue devolviendo un número, y sin este indicador ese
    número parecería tan informado como cualquier otro.
    """
    lin = lg.feature_lineage()
    presentes = set(up.columns)
    filas = {"match_key": up["match_key"].to_numpy()}
    bloques = []
    for bloque, grupo in lin.groupby("bloque", sort=True):
        cols = [c for c in grupo["columna"] if c in presentes]
        if not cols:
            continue
        filas[f"bloque_{bloque}"] = up[cols].notna().all(axis=1).to_numpy()
        bloques.append(f"bloque_{bloque}")
    cov = pd.DataFrame(filas)
    cov["n_bloques"] = cov[bloques].sum(axis=1).astype("int64")
    cov["cobertura"] = cov["n_bloques"] / max(len(bloques), 1)
    return cov


# ===========================================================================
# 4. Predicciones de la próxima jornada
# ===========================================================================
def proxima_jornada(up: pd.DataFrame, P: np.ndarray, *,
                    cuotas: str = CUOTAS_EMISION) -> pd.DataFrame:
    """La tabla que la aplicación publica como predicción pendiente.

    Lleva la cuota disponible **en el momento de emitir** y, a partir de ella, el
    valor esperado y la fracción de Kelly de la selección. Que el valor esperado
    viaje junto a la probabilidad y no en otro fichero es intencionado: son la
    misma afirmación leída de dos maneras, y separarlas invita a publicar una sin
    la otra, que es como se acaba mostrando una probabilidad alta como si fuese
    una oportunidad de apuesta.
    """
    der = derivadas_terna(P)
    cols_cuota = list(bt.CUOTAS[cuotas])
    hay_cuota = all(c in up.columns for c in cols_cuota)

    out = pd.DataFrame({
        "match_key": up["match_key"].to_numpy(),
        "date": pd.to_datetime(up["date"]).to_numpy(),
        "season": up["season"].to_numpy(),
        "jornada": up["round"].to_numpy() if "round" in up.columns else np.nan,
        "home_key": up["home_key"].to_numpy(),
        "away_key": up["away_key"].to_numpy(),
        "home_team": up["home_team"].to_numpy(),
        "away_team": up["away_team"].to_numpy(),
        "p_home": P[:, 0], "p_draw": P[:, 1], "p_away": P[:, 2],
    })
    out = pd.concat([out, der], axis=1)

    if hay_cuota:
        O = up[cols_cuota].to_numpy(dtype="float64")
        O = np.where(O > 1.0, O, np.nan)
        ve = bt.expected_value(P, O)
        k = np.arange(len(out)), out["clase"].map(
            {c: i for i, c in enumerate(ev.CLASES)}).to_numpy()
        out["cuota"] = O[k]
        out["valor_esperado"] = ve[k]
        out["kelly"] = bt.kelly_fraction(P[k], O[k]) * bt.FRACCION_KELLY
        out["kelly"] = np.minimum(out["kelly"], bt.TOPE_KELLY)
        # La selección de la política de valor: la clase de MAYOR valor esperado,
        # que no tiene por qué ser la más probable. Se publica aparte de la clase
        # predicha porque son dos preguntas distintas —qué va a pasar y a qué
        # merece la pena apostar— y confundirlas es el error de lectura más común.
        alguna = np.isfinite(ve).any(axis=1)
        j = np.argmax(np.where(np.isfinite(ve), ve, -np.inf), axis=1)
        mejor = np.arange(len(out)), j
        # Un partido sin ningún precio publicado no tiene selección: se deja
        # vacía en lugar de heredar el argumento máximo de una fila de NaN, que
        # apuntaría siempre a la victoria local y parecería una recomendación.
        out["seleccion"] = np.where(alguna, [ev.CLASES[i] for i in j], None)
        out["seleccion_ve"] = np.where(alguna, ve[mejor], np.nan)
        out["seleccion_cuota"] = np.where(alguna, O[mejor], np.nan)
        # Apostable es lo que la política publicada apostaría, y la publicada es
        # `build_models.POLITICA_ECONOMIA = "favorito"`: una unidad plana al signo
        # que el modelo señala, en todos los partidos con precio. Marcar aquí el
        # umbral de valor pondría al lado de la cifra histórica de rentabilidad una
        # recomendación que esa cifra no mide —y que, medida, rinde peor: pierde
        # 5.75 puntos contra el listón de Bet365 donde se eligió el peso y 1.84
        # fuera, con el mismo orden en las dos ventanas—. El valor esperado y la
        # fracción de Kelly siguen publicándose como lectura descriptiva del
        # precio, que es otra pregunta y va rotulada como tal.
        out["apostable"] = np.isfinite(out["cuota"].to_numpy())
        out["cuotas_origen"] = cuotas
    else:
        for c in ("cuota", "valor_esperado", "kelly", "seleccion_ve",
                  "seleccion_cuota"):
            out[c] = np.nan
        out["seleccion"] = None
        out["apostable"] = False
        out["cuotas_origen"] = "sin precio"

    cov = cobertura_por_partido(up)
    out = out.merge(cov, on="match_key", how="left", validate="one_to_one")
    return out.sort_values(["date", "home_key"]).reset_index(drop=True)


# ===========================================================================
# 5. Explicaciones y narrativa de la jornada emitida
# ===========================================================================
def explicaciones_jornada(modelo, Xev: pd.DataFrame, ctx, base: pd.Series,
                          claves: pd.Series) -> tuple:
    """Aportación por bloque de cada partido emitido, más su frase.

    El reparto se calcula sobre la escala de probabilidad y con enumeración
    exacta de coaliciones, de modo que las aportaciones de un partido suman
    exactamente la distancia entre su pronóstico y el del partido de referencia.
    Esa igualdad es la que permite enseñar la descomposición al lector sin
    acompañarla de una advertencia: lo que se le muestra suma lo que se le dice.
    """
    mapa = ex.mapa_bloques(Xev.columns)
    sh = ex.shapley_blocks(modelo, Xev, ctx, base, mapa)
    error = ex.verifica_eficiencia(sh)

    filas, frases = [], []
    for i, clave in enumerate(claves):
        tab = ex.explain_match(sh, i)
        for bloque, r in tab.iterrows():
            filas.append({"match_key": clave, "bloque": bloque,
                          "titulo": r["titulo"],
                          "aporta_home": float(r["home"]),
                          "aporta_draw": float(r["draw"]),
                          "aporta_away": float(r["away"]),
                          "dominante": bool(r["dominante"])})
        frases.append({"match_key": clave, "narrativa": ex.narrate(sh, i)})
    aporta = pd.DataFrame(filas)
    # El error de eficiencia viaja con la tabla, no en un mensaje de consola: es
    # la prueba de que lo que se dibuja suma lo que se afirma, y la interfaz
    # puede citarlo sin recalcularlo.
    aporta.attrs["error_eficiencia"] = float(error)
    return aporta, pd.DataFrame(frases)


# ===========================================================================
# 6. Radar y barras pre-partido
# ===========================================================================
def _normaliza(valores: pd.Series, eje: EjeRadar) -> pd.Series:
    """Lleva una magnitud a [0,1] con el rango declarado, no con el observado."""
    ancho = eje.maximo - eje.minimo
    z = (pd.to_numeric(valores, errors="coerce") - eje.minimo) / ancho
    z = z.clip(0.0, 1.0)
    return 1.0 - z if eje.invertido else z


def radar(up: pd.DataFrame) -> pd.DataFrame:
    """Ejes normalizados de los dos equipos de cada partido emitido.

    Formato largo —una fila por partido, lado y eje— porque es el que permite
    dibujar el radar, las barras comparadas y la tabla de detalle desde un único
    artefacto. El valor crudo viaja junto al normalizado: sin él, el gráfico no
    podría etiquetar «1834 Elo» y el lector solo vería una forma.
    """
    filas = []
    for lado, prefijo in (("local", "home"), ("visitante", "away")):
        clave_equipo = (f"{prefijo}_key" if f"{prefijo}_key" in up.columns
                        else f"{prefijo}_team")
        for eje in EJES_RADAR:
            col = f"{prefijo}_{eje.base}"
            if col not in up.columns or clave_equipo not in up.columns:
                continue
            crudo = pd.to_numeric(up[col], errors="coerce")
            filas.append(pd.DataFrame({
                "match_key": up["match_key"].to_numpy(),
                "lado": lado,
                "equipo": up[clave_equipo].to_numpy(),
                "eje": eje.id,
                "titulo": eje.titulo,
                "valor": crudo.to_numpy(),
                "normalizado": _normaliza(crudo, eje).to_numpy(),
                "invertido": eje.invertido,
                "rango_min": eje.minimo,
                "rango_max": eje.maximo,
            }))
    if not filas:
        return pd.DataFrame(columns=["match_key", "lado", "equipo", "eje"])
    out = pd.concat(filas, ignore_index=True)
    out["version_normalizacion"] = VERSION_NORMALIZACION
    return out.sort_values(["match_key", "lado", "eje"]).reset_index(drop=True)


def ejes_declarados() -> pd.DataFrame:
    """La declaración de los ejes, publicada como artefacto propio.

    Que los rangos vivan también fuera del código permite que la aplicación
    dibuje las escalas sin replicarlos y, sobre todo, que un cambio de rango sea
    visible en un diff del repositorio de publicación y no solo aquí.
    """
    return pd.DataFrame([{
        "eje": e.id, "titulo": e.titulo, "columna_base": e.base,
        "rango_min": e.minimo, "rango_max": e.maximo, "invertido": e.invertido,
        "version_normalizacion": VERSION_NORMALIZACION,
    } for e in EJES_RADAR])


# ===========================================================================
# 7. Perfiles de equipo y línea temporal
# ===========================================================================
# Las magnitudes de la línea temporal son las mismas que dibuja el radar, y no
# por comodidad: el radar es una foto del estado en un partido y la serie es ese
# mismo estado a lo largo de la temporada, de modo que el lector que pasa de uno
# a otro tiene que reconocer la misma magnitud. Publicar aquí una lista distinta
# obligaría a explicar por qué el equipo que aparece fuerte en la foto decae en
# la serie, cuando la única causa sería que se están midiendo cosas distintas.
_PERFIL = [(e.id, e.base) for e in EJES_RADAR]
_exige_por_lado([c for _, c in _PERFIL], "_PERFIL")


def perfiles_equipo(feat: pd.DataFrame) -> pd.DataFrame:
    """Serie por equipo y jornada con las magnitudes que dibujan su evolución.

    Se construye desdoblando cada partido en sus dos equipos, que es la forma en
    que el dominio piensa una temporada: la trayectoria es del equipo, no del
    encuentro. Todas las magnitudes son las que el modelo vio ANTES de cada
    partido, de modo que la línea temporal describe lo que se sabía en cada
    momento y no lo que acabó ocurriendo; una serie construida con el estado
    posterior contaría otra historia y además sería inconsistente con las
    predicciones que la acompañan.
    """
    trozos = []
    for lado, prefijo in (("local", "home"), ("visitante", "away")):
        base = pd.DataFrame({
            "season": feat["season"].to_numpy(),
            "jornada": feat["round"].to_numpy() if "round" in feat else np.nan,
            "date": pd.to_datetime(feat["date"]).to_numpy(),
            "match_key": feat["match_key"].to_numpy(),
            "equipo": feat[f"{prefijo}_key"].to_numpy(),
            "rival": feat[f"{'away' if prefijo == 'home' else 'home'}_key"].to_numpy(),
            "lado": lado,
        })
        # El hueco se rellena con NaN en lugar de omitir la columna, al revés que
        # en el radar, porque aquí el artefacto es una tabla y la aplicación la
        # lee por nombre: una columna que aparece y desaparece según la temporada
        # que se publique rompería al consumidor, mientras que una serie vacía se
        # dibuja como una línea que no empieza. Que el hueco no pueda deberse a un
        # nombre mal declarado —el fallo que este rescate ocultaría— ya lo
        # garantiza la comprobación contra el catálogo al importar el módulo.
        for nombre, col in _PERFIL:
            fuente = f"{prefijo}_{col}"
            base[nombre] = (pd.to_numeric(feat[fuente], errors="coerce").to_numpy()
                            if fuente in feat.columns else np.nan)
        if C.TARGET in feat.columns:
            res = feat[C.TARGET].to_numpy()
            base["resultado"] = np.where(
                res == "D", "E", np.where((res == "H") == (prefijo == "home"),
                                          "V", "P"))
            base.loc[pd.isna(feat[C.TARGET]).to_numpy(), "resultado"] = None
        trozos.append(base)
    out = pd.concat(trozos, ignore_index=True)
    return out.sort_values(["equipo", "date"]).reset_index(drop=True)


# ===========================================================================
# 8. Diagnósticos de clasificación
# ===========================================================================
def _puntuados(base: Path, publicado: str) -> pd.DataFrame:
    """Los partidos ya puntuados por el modelo publicado, con su bloque rotulado.

    Dos ficheros y no uno porque `build_models` los separa a propósito: el nombre
    del artefacto es lo primero que lee quien lo abre, y «validacion» no puede
    contener la temporada en curso sin desmentirse. Al juntarlos aquí para
    diagnosticar, lo que allí decía el nombre del fichero tiene que decirlo una
    columna, y esa columna es la que `bm._agregable` consulta.

    La temporada en curso puede no existir —`build_models` solo escribe su parquet
    si tiene partidos—, así que su ausencia no es un error. El holdout no entra: su
    parquet solo aparece cuando se rompe el sello, y la web no lo publica (regla 7).
    """
    trozos = []
    for nombre, bloque in (("predicciones_validacion", "validacion"),
                           ("predicciones_produccion", "produccion")):
        ruta = base / f"{nombre}.parquet"
        if not ruta.exists():
            continue
        d = pd.read_parquet(ruta)
        d = d[d["modelo"] == publicado].copy()
        d[bm.BLOQUE] = bloque
        trozos.append(d)
    if not trozos:
        raise FileNotFoundError(
            f"no hay predicciones del modelo «{publicado}» en {base}. "
            "Ejecuta primero `python scripts/models/build_models.py`.")
    return pd.concat(trozos, ignore_index=True)


def diagnosticos(pred: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Confusión, informe por clase y ROC uno-contra-resto, por temporada y total.

    Se publican desagregados porque agregados no diagnostican nada: el empate es
    la clase que todos los modelos de este dominio predicen mal, y su fallo queda
    sepultado bajo el acierto de las otras dos en cualquier cifra global. La
    matriz de confusión y el informe por clase son las dos lecturas que lo hacen
    visible, y la curva ROC uno-contra-resto la que dice si el problema es de
    ordenación o de umbral.

    Recibe validación Y temporada en curso, y reparte como reparte el motor: las
    filas por temporada traen todas las temporadas, y la fila `total` solo las de
    validación. El reparto no se decide aquí ni se copia: se delega en
    `bm._agregable`, que es donde está escrita la regla —una copia podría divergir
    de ella sin que nada lo notase—.

    Esto corrige una incoherencia medida del contrato publicado. `confusion` y
    `fiabilidad` llegan ya repartidas así desde `build_models` —sus filas por
    temporada incluyen 2026/27, con 78 partidos y 312 tramos, y sus totales son
    2280 y 9120, es decir validación sola—, mientras que estas dos tablas se
    construían sobre `predicciones_validacion` y **se quedaban sin la temporada en
    curso**: la misma web ofrecía matriz de confusión y curva de fiabilidad de la
    temporada que se está jugando, y ROC e informe por clase vacíos para ella. Con
    la directriz de repasar temporada por temporada sin excepción, ese hueco pasa
    de rareza a defecto.

    **Se publica el área y NO los puntos de la curva, y está medido por qué.** La
    aplicación anterior pedía un `roc_curves_by_season.json` con la curva de cada
    temporada, y la tentación es dársela: el área resume la ordenación entera y
    esconde la región de confianza alta, que es la única donde actúa un apostante a
    unidad plana. Y en efecto esconde algo —el área parcial al 10 % de falsos
    positivos REORDENA las temporadas respecto al área total en las tres clases,
    con correlación de rangos de solo +0.32 (H), +0.75 (D) y +0.18 (A); 2021/22 es
    cuarta de siete por área en la clase local (0.7271) y última por área parcial
    (0.1111)—. Pero ese reordenamiento no se sostiene: remuestreando, la dispersión
    del área parcial ENTRE temporadas es 1.46, 1.54 y 1.08 veces su propio error
    típico de medición, es decir que en la clase visitante la diferencia entre
    temporadas es indistinguible del ruido de medirla, y en las otras dos es débil.
    El motivo es de tamaño: el 10 % inicial del eje de una temporada se apoya en
    unas 28 observaciones negativas. Publicar la curva por temporada sería publicar
    la forma del ruido con aspecto de hallazgo, así que no se publica.

    Agregada sí está determinada (2280 partidos, error típico 0.0188, 0.0103 y
    0.0184), y de ahí sale una cifra que el contrato actual no podía mostrar y que
    sostiene con números lo que este módulo afirma arriba sobre el empate: frente al
    0.05 que sacaría una moneda, el decil de más confianza coloca al local a **8.3**
    errores típicos del azar y al visitante a **7.2**, pero al empate solo a **1.8**.
    El empate no es que se prediga mal en promedio: es que donde el modelo dice
    estar seguro, apenas ordena mejor que el azar.
    """
    informes, rocs = [], []
    for etiqueta, sub in ([("total", bm._agregable(pred))]
                          + list(pred.groupby("season", sort=True))):
        y = ev._objetivo(sub)
        P = ev._terna(sub, ev.PROB_COLS)
        m = np.isfinite(P).all(axis=1) & np.isfinite(y)
        if not m.any():
            continue
        inf = ev.classification_report(y[m], P[m])
        inf.insert(0, "season", etiqueta)
        informes.append(inf.reset_index() if inf.index.name else inf)
        auc = ev.roc_auc_ovr(y[m], P[m])
        rocs.append({"season": etiqueta,
                     **{k: float(v) for k, v in auc.items()}})
    return {"informe": pd.concat(informes, ignore_index=True),
            "roc": pd.DataFrame(rocs)}


# Las ternas de probabilidad del precio que se publican junto a la del modelo, y
# por qué estas tres: son exactamente los tres `contendiente` que ya compara
# `metricas_temporada`. Publicar en el partido una referencia que la tabla por
# temporada no contiene —o al revés— dejaría la web sin poder explicar de dónde
# sale una diferencia agregada mirando los partidos que la componen.
_REFERENCIAS_TERNA = {"b365": "mkt_p_b365_{}_raw",
                      "consenso": "mkt_p_{}_raw",
                      "pinnacle": "mkt_p_ps_{}_raw"}


def predicciones_por_partido(pred: pd.DataFrame) -> pd.DataFrame:
    """Una fila por partido puntuado: qué dijo el modelo, qué decía el precio, qué pasó.

    Es el artefacto que permite repasar una temporada partido a partido, y no lo
    cubría ninguno de los publicados. `matchlogs` es la LIBRETA DE APUESTAS —una
    fila por partido y por fuente de cuota, con importe, ganancia y banca— y de la
    predicción conserva solo `p_sel`, la probabilidad del signo elegido. Para un
    modelo 1X2 eso esconde justamente la clase de la que trata el problema: con
    `p_sel` no hay forma de saber qué probabilidad daba el modelo al empate, que es
    la que todos los modelos del dominio predicen mal y la que el trabajo discute.
    La terna completa existía en `predicciones_validacion`, dentro del motor, y no
    llegaba a la web.

    El `signo` es el argumento máximo de la terna del modelo, y coincide con el que
    apuesta la economía publicada porque `POLITICA_ECONOMIA = "favorito"` juega al
    signo del modelo. Se recalcula aquí en vez de traerlo de `matchlogs` para no
    heredar su duplicación por fuente de cuota: el signo es del modelo y no del
    precio, así que es uno por partido.

    **Aviso para quien calcule una métrica con esta tabla.** Son TODOS los partidos
    puntuados, 3040 de validación, y `metricas_temporada` no: aquella se mide sobre
    la intersección de los partidos que cubren todas las referencias pedidas, y
    Pinnacle cubre 3037 de los 3040. De ahí que el acierto de validación salga
    0.534211 contado aquí (1624 de 3040) y 0.533750 en la tabla de métricas (1621
    de 3037) —los tres partidos sin Pinnacle son aciertos los tres—. No es una
    discrepancia que arreglar sino dos preguntas distintas: esta tabla responde
    «qué hizo el modelo en cada partido» y aquella «cómo se compara con el precio
    donde hay precio con el que compararlo». Una web que enseñe las dos cifras como
    la misma estará enseñando un error de 0.0006 sin saberlo.
    """
    P = ev._terna(pred, ev.PROB_COLS)
    sel = np.argmax(P, axis=1)
    y = pd.to_numeric(pred["y_num"], errors="coerce")
    clases = np.array(ev.CLASES)

    out = pd.DataFrame({
        "season": pred["season"].to_numpy(),
        "jornada": pred["jornada"].to_numpy(),
        "date": pd.to_datetime(pred["date"]).to_numpy(),
        "match_key": pred["match_key"].to_numpy(),
        "local": pred["home_key"].to_numpy(),
        "visitante": pred["away_key"].to_numpy(),
        "bloque": pred[bm.BLOQUE].to_numpy(),
        # El resultado como letra y como número: la letra es la que se lee en una
        # tabla y el número el que permite recalcular cualquier métrica sin volver
        # a acordar el orden de las clases.
        "resultado": np.where(y.notna(), clases[y.fillna(0).astype(int)], None),
        "y_num": y.to_numpy(),
        "p_home": P[:, 0], "p_draw": P[:, 1], "p_away": P[:, 2],
        "signo": clases[sel],
        "p_signo": P[np.arange(len(P)), sel],
        "acierto": np.where(y.notna(), sel == y.to_numpy(), None),
    })
    for etiqueta, patron in _REFERENCIAS_TERNA.items():
        for lado in ("home", "draw", "away"):
            col = patron.format(lado)
            # Una referencia sin cobertura se publica como columna de nulos y no se
            # omite: Pinnacle cubre 3037 de los 3040 partidos de validación y 0 de
            # la temporada en curso, y una columna que aparece y desaparece según la
            # temporada obligaría a la web a comprobar el esquema en cada filtro.
            out[f"{etiqueta}_p_{lado}"] = (pred[col].to_numpy() if col in pred
                                           else np.nan)
    return out.sort_values(["date", "match_key"]).reset_index(drop=True)


# ===========================================================================
# 9. Ficha del modelo
# ===========================================================================
def ficha_modelo(manifiesto: dict, st, met: pd.DataFrame,
                 emision: dict | None) -> dict:
    """La especificación de lo publicado, con sus límites al mismo nivel que sus cifras.

    Las limitaciones no van en un apéndice sino en el mismo documento y con la
    misma jerarquía que las métricas. La razón es de honestidad expositiva: una
    web que muestra un retorno del ocho por ciento sin decir que su intervalo
    cruza el cero no está informando, está persuadiendo.
    """
    pub = manifiesto.get("publicado", {})
    metricas: dict[str, float] = {}
    referencias: dict[str, dict] = {}
    if {"contendiente", "modelo"}.issubset(met.columns):
        suyas = met[met["modelo"] == pub.get("modelo")]
        fila = suyas[suyas["contendiente"] == "modelo"]
        if len(fila):
            metricas = {k: float(v) for k, v in fila.iloc[0].items()
                        if isinstance(v, (int, float, np.floating))
                        and not isinstance(v, bool) and pd.notna(v)}
        for _, r in suyas[suyas["contendiente"] != "modelo"].iterrows():
            referencias[str(r["contendiente"])] = {
                k: float(r[k]) for k in ("n", "rps", "logloss", "accuracy")
                if k in suyas.columns and pd.notna(r[k])}

    return {
        "modelo": pub.get("modelo"),
        "motivo_de_eleccion": pub.get("motivo"),
        "regimen": REGIMEN,
        "protocolo": manifiesto.get("protocolo"),
        # El papel de cada temporada frente a la elección del modelo, que es lo que la
        # aplicación necesita para que su repaso temporada por temporada no presente
        # como resultado las que eligieron el peso (regla 5). Lo construye
        # `build_models.temporadas_rol` y aquí solo viaja.
        "temporadas_rol": manifiesto.get("temporadas_rol"),
        "run_code": manifiesto.get("run_code"),
        "generado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        # El recuento de la matriz con la que se emite, que es distinto del
        # conjunto del modelo: cuenta las columnas del estado de preprocesado
        # congelado, indicadores de faltante incluidos. Sin jornada pendiente no
        # hay estado cargado (`st = None` en `publica`) y entonces se publica
        # `null`, no cero: la primera ficha escrita decía «n_features: 0» tres
        # líneas por encima de «conjunto.n_variables: 3», y un cero es una
        # afirmación —el modelo no usa ninguna variable— mientras que la verdad
        # era que nadie había contado. Un `null` lo dice sin mentir, y la web
        # puede distinguir «no emitido» de «vacío».
        "variables": {
            "n_features": (len(st.feature_cols) if st is not None else None),
            "n_indicadores_faltante": (len(st.flag_cols or {})
                                       if st is not None else None),
            "bloques": ex.block_registry().to_dict("records"),
        },
        "metricas_validacion": metricas,
        "referencias": referencias,
        # Qué especifica al modelo publicado, más allá del nombre de la familia:
        # el conjunto de variables con el que se midió, los valores con los que se
        # ajusta y de dónde salió esa elección. Va en la ficha porque una misma
        # familia sobre dos conjuntos distintos son dos modelos distintos, y el
        # lector no puede distinguirlos por el nombre.
        "conjunto": manifiesto.get("conjunto"),
        "familia": manifiesto.get("familia", bm.FAMILIA),
        "hiperparametros": manifiesto.get("hiperparametros"),
        "economia": manifiesto.get("economia", {}),
        "holdout": manifiesto.get("holdout"),
        "emision": emision or {"estado": "sin jornada pendiente"},
        "normalizacion_graficos": {
            "version": VERSION_NORMALIZACION,
            "ejes": ejes_declarados().to_dict("records"),
        },
        "limitaciones": manifiesto.get("limitaciones", bm.LIMITACIONES),
    }


# ===========================================================================
# 10. Orquestación
# ===========================================================================
ESCRITOS: list[str] = []


def _escribe(df: pd.DataFrame, nombre: str, *, save: bool,
             fmt: str = "parquet") -> Path:
    destino = WEB / f"{nombre}.{fmt}"
    ESCRITOS.append(destino.name)
    if not save:
        print(f"  [DRY] {destino.relative_to(_ROOT)}  ({len(df)} filas)")
        return destino
    salida = df.reset_index() if (df.index.name is not None or
                                  isinstance(df.index, pd.MultiIndex)) else df
    write_df_required(salida, destino, fmt=fmt, etiqueta="publish_web")
    print(f"  [OK ] {destino.relative_to(_ROOT)}  ({len(salida)} filas)")
    return destino


def _escribe_json(obj, nombre: str, *, save: bool) -> Path:
    destino = WEB / f"{nombre}.json"
    ESCRITOS.append(destino.name)
    if not save:
        print(f"  [DRY] {destino.relative_to(_ROOT)}")
        return destino
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str),
                       encoding="utf-8")
    print(f"  [OK ] {destino.relative_to(_ROOT)}")
    return destino


def publica(*, save: bool = True, con_emision: bool = True,
            verbose: bool = True) -> dict:
    """Lee lo evaluado, emite la jornada pendiente y escribe el contrato completo."""
    base = bm.OUT / REGIMEN
    man_path = base / "manifiesto.json"
    if not man_path.exists():
        raise FileNotFoundError(
            f"no hay nada evaluado en {base}. Ejecuta primero "
            "`python scripts/models/build_models.py`, que es quien decide qué "
            "modelo se publica; este módulo no elige, proyecta.")
    manifiesto = json.loads(man_path.read_text(encoding="utf-8"))
    publicado = manifiesto["publicado"]["modelo"]
    # El sello del holdout, si existe, es la fuente más reciente de configuración:
    # contiene la decidida para la temporada sellada, que es la que se está
    # jugando cuando se emite. Su ausencia no impide publicar —hasta que el
    # holdout se puntúa, la última configuración conocida es la del manifiesto—,
    # de modo que se lee sin exigirlo.
    sello_path = bm.sello_path(REGIMEN)
    sello = (json.loads(sello_path.read_text(encoding="utf-8"))
             if sello_path.exists() else None)
    if verbose:
        print(f"proyectando la ejecución {manifiesto['run_code']} "
              f"(modelo «{publicado}», régimen {REGIMEN})")

    met = pd.read_parquet(base / "metricas_modelos.parquet")
    por_temporada = pd.read_parquet(base / "metricas_por_temporada.parquet")
    matchlogs = pd.read_parquet(base / "matchlogs.parquet")
    # Las dos caras de la comparación económica. Se proyectan tal cual porque la
    # web no tiene que recalcular nada que la evaluación ya haya decidido: el
    # rendimiento del modelo y el del apostante que sigue al favorito de Bet365
    # llegan medidos sobre los mismos partidos y pagados al mismo precio.
    eco_temporada = pd.read_parquet(base / "economia_por_temporada.parquet")
    eco_jornada = pd.read_parquet(base / "economia_por_jornada.parquet")
    # Las dos poblaciones de partido puntuado, juntas y rotuladas. El rótulo lo
    # pone aquí quien lee el fichero, porque `predicciones_*.parquet` no lo trae:
    # `build_models` lo estampa dentro de su propio proceso y no lo persiste. Es la
    # columna de la que depende `bm._agregable`, de modo que sin estamparla el
    # recorte del agregado no fallaría —su guarda devuelve la tabla entera— sino
    # que metería la temporada en curso en el total, que es el único sitio donde no
    # puede estar.
    pred = _puntuados(base, publicado)

    feat = pd.read_parquet(FEATURES)
    diag = diagnosticos(pred)
    por_partido = predicciones_por_partido(pred)
    perfiles = perfiles_equipo(feat)

    salidas: dict[str, object] = {}
    emision_meta = None
    st = None                      # sin jornada pendiente no hay estado cargado
    ESCRITOS.clear()

    if con_emision and PREDICT.exists() and UPCOMING.exists():
        up = pd.read_parquet(UPCOMING)
        X = pd.read_parquet(PREDICT)
        # La temporada de la jornada pendiente solo sirve para rotular: el
        # conjunto con el que se emite es uno y el mismo, el declarado, y lo que
        # se comprueba es que el artefacto que se está publicando se midió con él.
        temporada = str(up["season"].dropna().max())
        columnas, motivo = configuracion_emision(manifiesto, temporada,
                                                 sello=sello)
        if verbose:
            print(f"configuración de emisión: {motivo}")
        modelo, st, Xtr = ajusta_emision(feat, publicado, columnas=columnas,
                                         verbose=verbose)
        P = puntua_jornada(modelo, st, X, up, Xtr)

        jornada = proxima_jornada(up, P)
        fallos = qa.audita_prediccion(jornada, feat[["match_key"]],
                                      list(Xtr.columns),
                                      [c for c in st.feature_cols if c in X.columns],
                                      regimen=REGIMEN)
        if fallos:
            raise qa.PublicacionError("la jornada emitida no supera el QA:\n  - "
                                      + "\n  - ".join(fallos))

        Xev = X[list(st.feature_cols)]
        ctx = pt._contexto(X, pd.to_datetime(up["date"]))
        aporta, frases = explicaciones_jornada(
            modelo, Xev, ctx, ex.baseline_median(Xtr), up["match_key"])
        ejes = radar(up)

        salidas.update({"proxima_jornada": jornada, "explicaciones": aporta,
                        "narrativa": frases, "radar": ejes})
        emision_meta = {
            "partidos": int(len(jornada)),
            "desde": str(pd.to_datetime(jornada["date"]).min().date()),
            "hasta": str(pd.to_datetime(jornada["date"]).max().date()),
            "cuotas": CUOTAS_EMISION,
            "con_precio": int(jornada["cuota"].notna().sum()),
            "cobertura_media": float(jornada["cobertura"].mean()),
            "n_features": len(st.feature_cols),
            "temporada": temporada,
            "familia": bm.FAMILIA,
            "hiperparametros": bm.hiper_efectivo(),
            "n_variables": len(columnas),
            "variables": list(columnas),
            "motivo_de_configuracion": motivo,
        }
        if verbose:
            print(f"jornada emitida: {emision_meta['partidos']} partidos "
                  f"({emision_meta['desde']} -> {emision_meta['hasta']}), "
                  f"{emision_meta['con_precio']} con precio, cobertura media "
                  f"{emision_meta['cobertura_media']:.1%}")
    elif con_emision and verbose:
        print("sin jornada pendiente: no existen features_upcoming/features_predict; "
              "se publica solo la parte histórica")

    # Sin jornada pendiente la ficha se publica igual: la especificación del
    # modelo y sus límites no dependen de que haya partidos por jugar, y omitirla
    # dejaría la web sin la única página que explica qué se está mirando.
    ficha = ficha_modelo(manifiesto, st, met, emision_meta)

    if verbose:
        print("\nartefactos:")
    if "proxima_jornada" in salidas:
        _escribe(salidas["proxima_jornada"], "proxima_jornada", save=save, fmt="csv")
        _escribe_json(salidas["proxima_jornada"].to_dict("records"),
                      "proxima_jornada", save=save)
        _escribe(salidas["explicaciones"], "explicaciones_jornada", save=save)
        _escribe(salidas["narrativa"], "narrativa_jornada", save=save)
        _escribe(salidas["radar"], "radar_jornada", save=save)
    _escribe(ejes_declarados(), "ejes_radar", save=save)
    _escribe(por_temporada, "metricas_temporada", save=save)
    _escribe(matchlogs, "matchlogs", save=save)
    _escribe(por_partido, "predicciones", save=save)
    _escribe(eco_temporada, "economia_temporada", save=save)
    _escribe(eco_jornada, "economia_jornada", save=save)
    _escribe(diag["informe"], "clasificacion_por_clase", save=save)
    _escribe(diag["roc"], "roc_ovr", save=save)
    _escribe(pd.read_parquet(base / "confusion.parquet"), "confusion", save=save)
    _escribe(pd.read_parquet(base / "fiabilidad.parquet"), "fiabilidad", save=save)
    _escribe(pd.read_parquet(base / "desacuerdo_mercado.parquet"),
             "desacuerdo_mercado", save=save)
    _escribe(pd.read_parquet(base / "importancia_bloques.parquet"),
             "importancia_bloques", save=save)
    _escribe(perfiles, "perfiles_equipo", save=save)
    _escribe_json(ficha, "ficha_modelo", save=save)

    manifiesto_web = {
        "generado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_code": manifiesto["run_code"],
        "regimen": REGIMEN,
        "modelo": publicado,
        "version_normalizacion": VERSION_NORMALIZACION,
        "emision": emision_meta or {"estado": "sin jornada pendiente"},
        # Los artefactos son los que ESTA ejecución ha escrito, no los que hay en
        # el directorio: un fichero sobrante de una publicación anterior figuraría
        # en el manifiesto como si perteneciese a la ejecución actual, y quien
        # leyese la web creería estar viendo un conjunto coherente.
        "artefactos": sorted(set(ESCRITOS) | {"manifiesto_web.json"}),
        "error_eficiencia": (
            float(salidas["explicaciones"].attrs.get("error_eficiencia", float("nan")))
            if "explicaciones" in salidas else None),
    }
    _escribe_json(manifiesto_web, "manifiesto_web", save=save)

    if not save:
        print("\n[DRY] no se ha escrito nada.")
    salidas["ficha"] = ficha
    salidas["manifiesto"] = manifiesto_web
    return salidas


if __name__ == "__main__":
    publica(save="--dry" not in sys.argv,
            con_emision="--sin-emision" not in sys.argv)
