"""QA de lo publicado por la fase de modelado.

`qa_features.py` responde a la pregunta «¿el conjunto de variables está limpio?».
Este módulo responde a la siguiente, que es la que decide si el trabajo puede
defenderse: «¿lo que sale publicado dice lo que parece decir?». Son preguntas
distintas y por eso son dos ficheros: un set de variables impecable puede acabar
produciendo una web incoherente si dos artefactos proceden de ejecuciones
distintas, si el mercado se mide sobre más partidos que el modelo o si una
predicción «de la próxima jornada» corresponde a un partido ya jugado.

Todas las comprobaciones son funciones puras sobre tablas: reciben lo publicado y
devuelven la lista de problemas encontrados. Esa forma es deliberada. Permite que
el banco de pruebas las ejerza sin tocar disco ni red, con casos construidos a
mano en los que se sabe de antemano qué tiene que fallar, y evita el patrón
habitual de comprobación que solo se puede validar ejecutando el pipeline
completo —es decir, que nadie valida.

  DUROS (rompen la publicación):
   1. Probabilidades legítimas: las tres columnas suman uno y caen en el
      intervalo unidad, en validación, en holdout y en la próxima jornada.
   2. La próxima jornada es futura: ninguna predicción publicada como pendiente
      corresponde a un partido ya disputado, ni por fecha ni por aparecer con
      resultado en el histórico.
   3. Paridad de esquema: el orden y el conjunto de columnas de la matriz de
      inferencia coinciden exactamente con los de la matriz de entrenamiento.
   4. Coherencia del régimen: ninguna columna de `WEEKLY_UNAVAILABLE` alimenta
      el modelo semanal, ni como variable ni como indicador de faltante.
   5. Pasada única del holdout: existe un sello, uno solo, y la tabla publicada
      es exactamente la que el sello acredita.
   5b. Separación del bloque en curso: la temporada que se está jugando se
      publica en su propio artefacto, es posterior al holdout y no comparte
      ninguna temporada con validación. Lleva el número del holdout porque es su
      consecuencia: todo lo que viene después de la temporada sellada se entrena
      con ella dentro y por eso no puede decidir nada.
   6. Cobertura común: dentro de cada comparación, modelo y referencias están
      medidos sobre el mismo número de partidos. Se comprueba en el agregado y
      también temporada a temporada, porque la tabla por temporada es la única
      donde el juego de referencias cambia —una casa que deja de publicar precio
      desaparece de sus filas— y ahí es donde una ventaja de cobertura se
      colaría disfrazada de ventaja de pronóstico.
   7. Conjunto publicado: está cerrado, dice de dónde sale, y las variables que
      declara el manifiesto son las mismas que publica `conjunto_declarado`.
   8. Rendimiento con su listón: ninguna cifra económica se publica sin la del
      apostante que sigue al favorito de Bet365 —ni la del conjunto ni la de la
      temporada en curso—, la diferencia entre ambas es la resta de las dos, y la
      serie de validación por jornada cierra donde dice el manifiesto. Que el
      cuadre sea contra validación y no contra la tabla entera es lo que exige el
      rótulo de bloque: sin él, la serie terminaría en la última jornada jugada y
      la comprobación fallaría cada semana por el motivo equivocado.
   9. Una sola ejecución por régimen: todos los artefactos comparten código de
      ejecución y ese código es el que declara el manifiesto.

  REPORTE (no rompe): modelo publicado con su motivo, familia y valores, tamaño y
  procedencia del conjunto, cobertura del precio, limitaciones declaradas y edad
  de los artefactos.

Uso
---
    python scripts/models/qa_models.py                 # audita outputs/models
    python scripts/models/qa_models.py --regimen semanal
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import columns as C  # noqa: E402

_ROOT = _HERE.parent.parent
OUT = _ROOT / "outputs" / "models"

PROB_COLS = ["p_home", "p_draw", "p_away"]

# Tolerancia de la suma de la terna. Es holgada respecto al error de coma
# flotante y estricta respecto a cualquier error real: una terna que suma 0.999
# no viene de redondear, viene de haber normalizado mal o de haber recortado una
# clase después de normalizar.
TOL_SUMA = 1e-6


class PublicacionError(AssertionError):
    """Un artefacto publicado incumple un invariante de la fase de modelado."""


# ===========================================================================
# 1. Probabilidades legítimas
# ===========================================================================
def check_probabilidades(tab: pd.DataFrame, etiqueta: str = "tabla", *,
                         cols: list[str] = PROB_COLS,
                         tol: float = TOL_SUMA) -> list[str]:
    """Las tres probabilidades forman una distribución en todas las filas.

    Se comprueba lo que se publica, no lo que el estimador devuelve: entre una
    cosa y la otra hay un guardado en parquet, una posible conversión de tipo y
    una eventual reordenación de columnas, y es ahí donde la terna se estropea.
    Un valor ausente cuenta como fallo y no se ignora, porque una probabilidad
    que falta en la tabla de la web se pinta como cero y se lee como certeza.
    """
    fallos = []
    faltan = [c for c in cols if c not in tab.columns]
    if faltan:
        return [f"{etiqueta}: faltan las columnas de probabilidad {faltan}"]
    if tab.empty:
        return [f"{etiqueta}: no hay ninguna fila publicada"]

    P = tab[cols].to_numpy(dtype="float64")
    n_nan = int((~np.isfinite(P)).any(axis=1).sum())
    if n_nan:
        fallos.append(f"{etiqueta}: {n_nan} filas con probabilidad ausente o no finita")

    fin = np.isfinite(P).all(axis=1)
    if fin.any():
        Pf = P[fin]
        fuera = int(((Pf < -tol) | (Pf > 1 + tol)).any(axis=1).sum())
        if fuera:
            fallos.append(f"{etiqueta}: {fuera} filas con alguna probabilidad "
                          f"fuera del intervalo unidad "
                          f"(min={Pf.min():.4g}, max={Pf.max():.4g})")
        desv = np.abs(Pf.sum(axis=1) - 1.0)
        malas = int((desv > tol).sum())
        if malas:
            fallos.append(f"{etiqueta}: {malas} filas cuya terna no suma uno "
                          f"(desviación máxima {desv.max():.3g})")
    return fallos


# ===========================================================================
# 2. La próxima jornada es futura
# ===========================================================================
def check_jornada_futura(pred: pd.DataFrame, disputados: pd.DataFrame, *,
                         hoy: pd.Timestamp | None = None) -> list[str]:
    """Ninguna predicción pendiente corresponde a un partido ya jugado.

    Se exigen las dos lecturas porque fallan en momentos distintos. La fecha
    detecta el caso obvio —la ventana de scoring se quedó abierta y arrastró la
    jornada anterior—, pero no el partido aplazado que se juega antes de lo que
    figura en el calendario. La presencia en el histórico detecta ese segundo
    caso, que es justamente el que produciría una predicción con acierto perfecto
    y contaminaría cualquier cifra que la incluyese.
    """
    fallos = []
    if pred.empty:
        return ["próxima jornada: no hay ninguna predicción publicada"]
    if "match_key" not in pred.columns:
        fallos.append("próxima jornada: la tabla no lleva `match_key` y no puede "
                      "cotejarse con el histórico")

    hoy = pd.Timestamp(hoy).normalize() if hoy is not None \
        else pd.Timestamp.utcnow().normalize()
    if "date" in pred.columns:
        fechas = pd.to_datetime(pred["date"], errors="coerce")
        if fechas.isna().any():
            fallos.append(f"próxima jornada: {int(fechas.isna().sum())} filas sin "
                          "fecha legible")
        pasados = int((fechas.dt.normalize() < hoy).sum())
        if pasados:
            fallos.append(f"próxima jornada: {pasados} predicciones con fecha "
                          f"anterior a {hoy.date()}")
    else:
        fallos.append("próxima jornada: la tabla no lleva `date`")

    if "match_key" in pred.columns and "match_key" in disputados.columns:
        ya = set(disputados["match_key"].dropna())
        repetidos = sorted(set(pred["match_key"].dropna()) & ya)
        if repetidos:
            fallos.append(f"próxima jornada: {len(repetidos)} partidos ya "
                          f"disputados figuran como pendientes "
                          f"(p. ej. {repetidos[:3]})")

    if "match_key" in pred.columns and not pred["match_key"].is_unique:
        dup = pred["match_key"].value_counts()
        fallos.append(f"próxima jornada: {int((dup > 1).sum())} partidos "
                      "publicados más de una vez")
    return fallos


# ===========================================================================
# 3. Paridad de esquema entre entrenamiento e inferencia
# ===========================================================================
def check_orden_columnas(entrenamiento: list[str],
                         inferencia: list[str]) -> list[str]:
    """El orden y el conjunto de columnas coinciden bit a bit.

    La igualdad se pide sobre la lista y no sobre el conjunto. Un estimador
    lineal ajustado sobre una matriz de doscientas columnas aplica el coeficiente
    de la columna i al valor de la columna i: permutar dos columnas no lanza
    ninguna excepción, no deja ningún hueco y produce predicciones sistemáticamente
    equivocadas que además parecen razonables. Es la avería más cara de detectar a
    posteriori y la más barata de impedir aquí.
    """
    if list(entrenamiento) == list(inferencia):
        return []
    fallos = []
    sobran = [c for c in inferencia if c not in set(entrenamiento)]
    faltan = [c for c in entrenamiento if c not in set(inferencia)]
    if sobran:
        fallos.append(f"esquema: la inferencia trae {len(sobran)} columnas que el "
                      f"entrenamiento no tenía: {sobran[:8]}")
    if faltan:
        fallos.append(f"esquema: a la inferencia le faltan {len(faltan)} columnas "
                      f"del entrenamiento: {faltan[:8]}")
    if not sobran and not faltan:
        pos = next(i for i, (a, b) in enumerate(zip(entrenamiento, inferencia))
                   if a != b)
        fallos.append(f"esquema: mismo conjunto de columnas pero distinto orden; "
                      f"primera discrepancia en la posición {pos} "
                      f"(entrenamiento «{entrenamiento[pos]}», "
                      f"inferencia «{inferencia[pos]}»)")
    return fallos


# ===========================================================================
# 4. Coherencia del régimen semanal
# ===========================================================================
def check_regimen(columnas, regimen: str) -> list[str]:
    """Bajo el régimen semanal no entra nada que solo exista tras el saque.

    La comprobación se repite aquí aunque el preprocesado ya la haga porque el
    objeto auditado es otro: allí se verifica el estado que se congela, y aquí lo
    que efectivamente alimentó al modelo publicado. Que ambos coincidan es
    justamente lo que hay que demostrar y no lo que se puede suponer.
    """
    if regimen != "semanal":
        return []
    intrusas = C.weekly_unavailable(list(columnas))
    if intrusas:
        return [f"régimen semanal: {len(intrusas)} columnas no conocibles al "
                f"predecir alimentan el modelo: {sorted(intrusas)[:8]}"]
    return []


# ===========================================================================
# 5. Pasada única del holdout
# ===========================================================================
def check_sello_holdout(sello: dict | None, pred: pd.DataFrame | None, *,
                        temporadas_selladas=None) -> list[str]:
    """El holdout se puntuó una vez y la tabla publicada es la de esa pasada.

    El sello por sí solo no prueba nada: acredita que hubo una pasada, no que la
    tabla que acompaña sea la suya. Por eso se coteja el recuento de partidos y
    el código de ejecución. Sin ese cotejo, repetir la puntuación y sobrescribir
    solo la tabla dejaría el sello intacto y la propiedad —una única lectura—
    aparentemente satisfecha.
    """
    if sello is None and pred is None:
        return []                       # el holdout aún no se ha puntuado: correcto
    fallos = []
    if sello is None:
        return ["holdout: hay predicciones publicadas sin sello de pasada única"]
    if pred is None:
        return ["holdout: hay sello pero no la tabla de predicciones que acredita"]

    n_sello = sello.get("partidos")
    if n_sello is not None and int(n_sello) != len(pred):
        fallos.append(f"holdout: el sello acredita {n_sello} partidos y la tabla "
                      f"publicada tiene {len(pred)}")
    if sello.get("run_code") and "run_code" in pred.columns:
        codigos = set(pred["run_code"].dropna().unique())
        if codigos != {sello["run_code"]}:
            fallos.append(f"holdout: la tabla procede de {sorted(codigos)} y el "
                          f"sello de «{sello['run_code']}»")
    temporadas_selladas = list(temporadas_selladas or C.HOLDOUT_SEASONS)
    if "season" in pred.columns:
        intrusas = sorted(set(pred["season"].dropna()) - set(temporadas_selladas))
        if intrusas:
            fallos.append(f"holdout: la tabla incluye temporadas que no son del "
                          f"holdout: {intrusas}")
    return fallos


# ===========================================================================
# 5b. Separación del bloque de producción
# ===========================================================================
def check_produccion(curso: pd.DataFrame | None,
                     validacion: pd.DataFrame | None,
                     holdout: pd.DataFrame | None, *,
                     temporadas_selladas=None) -> list[str]:
    """La temporada en curso se publica aparte y no se mezcla con lo que decide.

    El bloque de producción es la tercera cifra del trabajo —cómo va el modelo en
    la temporada que se está jugando— y es la única que no puede volver sobre
    ninguna decisión: sus ventanas de entrenamiento contienen necesariamente el
    holdout, porque está después. Mientras viva en su propio artefacto, esa cifra
    es descriptiva y se lee bien; en cuanto una temporada suya aparezca también en
    la tabla de validación, la elección de familia, de hiperparámetros o de
    variables habría visto el holdout a través de ella y la independencia del
    sello se habría perdido sin que nada fallase.

    Que el artefacto NO exista es correcto y frecuente: en agosto la temporada en
    curso no tiene todavía jornadas puntuables. Lo que no puede ocurrir es que
    exista y comparta temporada con validación o con el propio holdout.
    """
    if curso is None:
        return []                       # aún no hay jornadas en curso: correcto
    fallos = []
    if curso.empty:
        return ["producción: el artefacto existe pero está vacío; no se publica "
                "una tabla sin filas, se omite"]
    if "season" not in curso.columns:
        return ["producción: la tabla no declara temporada, así que no se puede "
                "comprobar que no se mezcla con validación"]

    suyas = set(curso["season"].dropna())
    for nombre, otra in (("validación", validacion), ("holdout", holdout)):
        if otra is None or "season" not in getattr(otra, "columns", []):
            continue
        solapa = sorted(suyas & set(otra["season"].dropna()))
        if solapa:
            fallos.append(f"producción: comparte temporada con {nombre} ({solapa}); "
                          "la cifra en curso no puede haber entrado en ninguna "
                          "decisión y aquí lo habría hecho")

    selladas = set(temporadas_selladas or C.HOLDOUT_SEASONS)
    anteriores = sorted(s for s in suyas if s <= max(selladas)) if selladas else []
    if anteriores:
        fallos.append(f"producción: incluye temporadas que no son posteriores al "
                      f"holdout ({anteriores}); el bloque en curso es, por "
                      "definición, lo que viene después de la temporada sellada")
    return fallos


# ===========================================================================
# 6. Cobertura común entre modelo y referencias
# ===========================================================================
def check_cobertura_comun(tab: pd.DataFrame, *, por=("modelo",),
                          columna_n: str = "n") -> list[str]:
    """Dentro de cada comparación, todos los contendientes miden lo mismo.

    Es el error que más fácilmente produce una mejora ficticia: el mercado solo
    tiene precio en parte de la muestra, de modo que evaluarlo sobre sus partidos
    y al modelo sobre todos regala una diferencia que no mide calidad de
    pronóstico sino cobertura del precio. `evaluate.compare` restringe a la
    intersección precisamente para evitarlo; aquí se comprueba que lo publicado
    conserve esa propiedad, que es lo que un tribunal puede verificar.
    """
    fallos = []
    if columna_n not in tab.columns:
        return [f"cobertura: la tabla no lleva la columna «{columna_n}»"]
    claves = [c for c in por if c in tab.columns]
    grupos = tab.groupby(claves, sort=True) if claves else [((), tab)]
    for clave, sub in grupos:
        enes = sorted(set(int(v) for v in sub[columna_n].dropna()))
        if len(enes) > 1:
            quien = clave if claves else "la tabla"
            fallos.append(f"cobertura: en {quien} los contendientes están medidos "
                          f"sobre distinto número de partidos: {enes}")
    return fallos


# ===========================================================================
# 7. La configuración publicada es la que se midió
# ===========================================================================
def check_conjunto(manifiesto: dict, tabla: "pd.DataFrame | None") -> list[str]:
    """El conjunto que el manifiesto declara es el que se midió y el que se emite.

    Es la comprobación que protege la emisión. Quien publica la predicción de la
    próxima jornada no vuelve a ejecutar el recorrido: lee del manifiesto la
    familia, los valores y las variables del modelo publicado, y ajusta con ellos.
    Si esas tres piezas no fueran las que produjeron las cifras de validación
    —porque una ejecución escribió el manifiesto y otra los parquets, o porque el
    conjunto se tocó en el código después de medir—, la web mostraría un
    rendimiento histórico que no corresponde al modelo que está emitiendo, y nada
    en la página lo delataría: el nombre de la familia sería el mismo.

    Un conjunto sin cerrar no es un fallo de programación sino de procedimiento, y
    se trata como fallo duro porque el recorrido provisional existe para explorar,
    no para publicar.
    """
    fallos = []
    publicado = manifiesto.get("publicado", {}).get("modelo")
    con = manifiesto.get("conjunto")
    if not publicado:
        return ["conjunto: el manifiesto no declara modelo publicado"]
    if con is None:
        return [f"conjunto: el manifiesto no declara con qué variables se midió "
                f"«{publicado}»"]

    familia = manifiesto.get("familia")
    if familia and familia != publicado:
        fallos.append(f"conjunto: el manifiesto publica «{publicado}» y declara la "
                      f"familia «{familia}»")
    if not con.get("cerrado"):
        fallos.append(
            "conjunto: el recorrido se hizo con un conjunto provisional. Cierra la "
            "busqueda —`python scripts/models/seleccion.py`, que deja el conjunto en "
            "outputs/models/seleccion/ y build_models.py recoge de ahi— o escribe "
            "CONJUNTO, FAMILIA, HIPER y PROCEDENCIA a mano en build_models.py, y "
            "vuelve a ejecutarlo antes de publicar")
    if not con.get("procedencia") or con.get("procedencia") == "sin cerrar":
        fallos.append("conjunto: no consta de dónde salió la elección de variables; "
                      "un conjunto elegido a mano sin su procedencia no es "
                      "reproducible")

    variables = list(con.get("variables") or [])
    if not variables:
        fallos.append("conjunto: el manifiesto no publica la lista de variables")
    elif con.get("n_variables") not in (None, len(variables)):
        fallos.append(f"conjunto: el manifiesto dice {con.get('n_variables')} "
                      f"variables y publica {len(variables)}")

    if variables and tabla is not None and "variable" in tabla.columns:
        declaradas = set(map(str, variables))
        publicadas = set(tabla["variable"].astype(str))
        if declaradas != publicadas:
            solo_man = sorted(declaradas - publicadas)[:5]
            solo_tab = sorted(publicadas - declaradas)[:5]
            fallos.append(
                f"conjunto: el manifiesto y «conjunto_declarado.parquet» no listan "
                f"las mismas variables (solo en el manifiesto: {solo_man}; solo en "
                f"la tabla: {solo_tab})")
    return fallos


# ===========================================================================
# 8. El rendimiento se publica con su listón
# ===========================================================================
def check_economia(manifiesto: dict,
                   por_jornada: "pd.DataFrame | None") -> list[str]:
    """Toda cifra económica va acompañada de la del apostante de referencia.

    Un rendimiento a solas se lee contra el cero, que es el listón equivocado en un
    mercado con margen: perder poco donde el mercado pierde mucho es un resultado y
    ganar poco donde el mercado gana más no lo es. Por eso el manifiesto no puede
    publicar el retorno del modelo sin el de la referencia y sin la diferencia
    entre ambos.

    Se comprueba además que la serie por jornada cierra donde dice el manifiesto.
    Es la avería silenciosa de este bloque: si el agregado y el detalle vinieran de
    ejecuciones distintas, la web dibujaría una curva que no termina en la cifra que
    ella misma muestra al lado, y ningún error saltaría.

    El cuadre es contra la serie de VALIDACIÓN, porque validación es lo que el
    manifiesto resume en `economia`. La temporada en curso viaja en la misma
    tabla y con su propio rótulo —se dibuja al lado y se mide contra el mismo
    listón—, pero sumarla a la serie movería el cierre cada semana y la
    comprobación fallaría por el motivo equivocado. Por eso aquí el rótulo se
    EXIGE en cuanto hay más de un bloque: sin él las dos series son
    indistinguibles y esta función no tiene forma de saber cuál cuadrar.
    """
    fallos = []
    eco = manifiesto.get("economia") or {}
    if not eco:
        return ["economía: el manifiesto no publica rendimiento económico"]

    # Las dos economías se auditan con la misma regla: la del conjunto, que es la
    # que cita la memoria, y la de la temporada en curso, que es la que lee quien
    # abre la aplicación un lunes. Una cifra sin listón no es publicable en
    # ninguna de las dos, y la segunda es justamente la que más tienta a
    # publicarse sola porque es la que todo el mundo quiere ver.
    en_curso = (manifiesto.get("en_curso") or {}).get("economia") or {}
    for rotulo, tabla in (("", eco), ("en curso/", en_curso)):
        for cual, bloque in tabla.items():
            for campo in ("casa_referencia", "rendimiento_referencia",
                          "diferencia"):
                if campo not in bloque:
                    fallos.append(f"economía [{rotulo}{cual}]: publica el "
                                  f"rendimiento del modelo sin «{campo}», es "
                                  "decir, sin su listón")
            if "rendimiento" in bloque and "rendimiento_referencia" in bloque \
                    and "diferencia" in bloque:
                esperada = bloque["rendimiento"] - bloque["rendimiento_referencia"]
                if np.isfinite(esperada) \
                        and abs(bloque["diferencia"] - esperada) > 1e-9:
                    fallos.append(
                        f"economía [{rotulo}{cual}]: la diferencia publicada "
                        f"({bloque['diferencia']:+.6f}) no es la resta de los "
                        f"dos rendimientos ({esperada:+.6f})")

    if por_jornada is None or por_jornada.empty:
        return fallos
    if "cuotas" not in por_jornada.columns:
        fallos.append("economía: la serie por jornada no dice a qué precio "
                      "corresponde cada fila")
        return fallos

    if "bloque" in por_jornada.columns:
        por_jornada = por_jornada[por_jornada["bloque"] == "validacion"]
        if por_jornada.empty:
            return fallos + ["economía: la serie por jornada no trae ninguna fila "
                             "de validación, que es lo que el manifiesto resume"]
    elif len(eco) and len(en_curso):
        return fallos + ["economía: se publican dos economías —el conjunto y la "
                         "temporada en curso— y la serie por jornada no dice a "
                         "cuál pertenece cada fila"]

    for cual, sub in por_jornada.groupby("cuotas"):
        bloque = eco.get(str(cual))
        if not bloque:
            fallos.append(f"economía: hay serie por jornada de «{cual}» que el "
                          "manifiesto no resume")
            continue
        for lado, campo in (("modelo", "rendimiento"),
                            ("ref", "rendimiento_referencia")):
            col = f"rendimiento_acum_{lado}"
            if col not in sub.columns or campo not in bloque:
                continue
            serie = sub[col].dropna()
            if serie.empty or not np.isfinite(bloque[campo]):
                continue
            if abs(float(serie.iloc[-1]) - float(bloque[campo])) > 1e-9:
                fallos.append(
                    f"economía [{cual}]: la serie de «{lado}» cierra en "
                    f"{float(serie.iloc[-1]):+.6f} y el manifiesto declara "
                    f"{float(bloque[campo]):+.6f}")
    return fallos


# ===========================================================================
# 9. Una sola ejecución por régimen
# ===========================================================================
def check_run_code(artefactos: dict[str, pd.DataFrame],
                   manifiesto: dict) -> list[str]:
    """Todos los artefactos de un régimen vienen de la misma pasada.

    Una web que mezcla la tabla de métricas de una ejecución con las
    explicaciones de otra no falla: muestra números que no se contradicen a
    simple vista y que sin embargo no describen el mismo modelo. El código de
    ejecución convierte esa avería silenciosa en un error ruidoso.
    """
    fallos = []
    esperado = manifiesto.get("run_code")
    if not esperado:
        return ["manifiesto: no declara código de ejecución"]
    for nombre, df in artefactos.items():
        if df is None or "run_code" not in getattr(df, "columns", []):
            fallos.append(f"ejecución: «{nombre}» no lleva código de ejecución")
            continue
        codigos = set(df["run_code"].dropna().unique())
        if codigos != {esperado}:
            fallos.append(f"ejecución: «{nombre}» procede de {sorted(codigos)} y el "
                          f"manifiesto declara «{esperado}»")
    return fallos


# ===========================================================================
# 10. Auditoría de un régimen publicado
# ===========================================================================
_TABLAS = ["predicciones_validacion", "metricas_modelos", "conjunto_declarado",
           "metricas_por_temporada",
           "desacuerdo_mercado", "fiabilidad", "confusion", "matchlogs",
           "economia_por_temporada", "economia_por_jornada",
           "importancia_bloques", "permutacion_detalle", "registro_bloques"]


def _lee(directorio: Path, nombre: str) -> pd.DataFrame | None:
    ruta = directorio / f"{nombre}.parquet"
    return pd.read_parquet(ruta) if ruta.exists() else None


def audita_regimen(regimen: str, *, base: Path = OUT,
                   verbose: bool = True) -> list[str]:
    """Aplica las comprobaciones duras a lo publicado bajo un régimen."""
    directorio = base / regimen
    if not directorio.exists():
        return [f"{regimen}: no hay nada publicado en {directorio}"]

    manifiesto_path = directorio / "manifiesto.json"
    if not manifiesto_path.exists():
        return [f"{regimen}: falta el manifiesto, sin el cual nada es trazable"]
    manifiesto = json.loads(manifiesto_path.read_text(encoding="utf-8"))

    tablas = {n: _lee(directorio, n) for n in _TABLAS}
    presentes = {n: t for n, t in tablas.items() if t is not None}
    fallos = [f"{regimen}: falta el artefacto «{n}»"
              for n, t in tablas.items() if t is None]

    publicado = manifiesto.get("publicado", {}).get("modelo")
    val = presentes.get("predicciones_validacion")
    if val is not None:
        fallos += check_probabilidades(val, f"{regimen}/validación")
        if publicado and "modelo" in val.columns:
            del_publicado = val[val["modelo"] == publicado]
            if del_publicado.empty:
                fallos.append(f"{regimen}: el modelo publicado «{publicado}» no "
                              "aparece en la tabla de predicciones")

    met = presentes.get("metricas_modelos")
    if met is not None:
        fallos += check_cobertura_comun(met)

    # La misma invariante, temporada a temporada. Hace falta por separado porque
    # la tabla por temporada es la única donde el juego de referencias cambia de
    # fila a fila: Pinnacle cubre 3037 de los 3040 partidos de validación y 0 de
    # los 78 de 2026/27 —football-data dejó de publicar su precio—, de modo que
    # `build_models` le pide a `compare` solo las referencias vivas en cada
    # temporada para no perder la temporada en curso entera. Esa concesión es
    # legítima mientras la intersección se respete DENTRO de cada temporada, y
    # esto es lo que lo comprueba: una temporada donde el modelo midiera sobre
    # más partidos que su referencia regalaría una ventaja de cobertura, no de
    # pronóstico.
    met_t = presentes.get("metricas_por_temporada")
    if met_t is not None:
        fallos += check_cobertura_comun(met_t, por=("bloque", "season"))

    fallos += check_conjunto(manifiesto, presentes.get("conjunto_declarado"))
    fallos += check_economia(manifiesto, presentes.get("economia_por_jornada"))

    hold = _lee(directorio, "predicciones_holdout")
    sello_path = directorio / "holdout_sello.json"
    sello = json.loads(sello_path.read_text(encoding="utf-8")) \
        if sello_path.exists() else None
    fallos += check_sello_holdout(sello, hold)
    if hold is not None:
        fallos += check_probabilidades(hold, f"{regimen}/holdout")

    curso = _lee(directorio, "predicciones_produccion")
    fallos += check_produccion(curso, val, hold)
    if curso is not None:
        fallos += check_probabilidades(curso, f"{regimen}/producción")
        presentes["predicciones_produccion"] = curso

    fallos += check_run_code(presentes, manifiesto)

    if not manifiesto.get("limitaciones"):
        fallos.append(f"{regimen}: el manifiesto no declara limitaciones; una "
                      "cifra sin sus límites se lee como más firme de lo que es")

    if verbose:
        _reporte(regimen, manifiesto, presentes, sello)
    return fallos


def _reporte(regimen: str, manifiesto: dict, tablas: dict,
             sello: dict | None) -> None:
    pub = manifiesto.get("publicado", {})
    print("-" * 72)
    print(f"régimen {regimen}   ejecución {manifiesto.get('run_code')}   "
          f"generado {manifiesto.get('generado')}")
    cifra = f"  ({pub['rps']:.4f})" if isinstance(pub.get("rps"), (int, float)) else ""
    print(f"  modelo publicado  : {pub.get('modelo')}{cifra}")
    if pub.get("motivo"):
        print(f"  motivo            : {pub['motivo']}")
    print(f"  pliegues          : {manifiesto.get('pliegues')}")
    print(f"  familia           : {manifiesto.get('familia', '—')}   "
          f"{manifiesto.get('hiperparametros') or {}}")
    con = manifiesto.get("conjunto") or {}
    estado = "cerrado" if con.get("cerrado") else "PROVISIONAL, no publicable"
    print(f"  conjunto          : {con.get('n_variables', '—')} variables "
          f"({estado})")
    print(f"  procedencia       : {con.get('procedencia', '—')}")
    for nombre, df in sorted(tablas.items()):
        print(f"  {nombre:26s}: {len(df):>6} filas")
    print(f"  holdout           : "
          f"{'puntuado ' + str(sello.get('generado')) if sello else 'sin puntuar'}")
    print(f"  limitaciones      : {len(manifiesto.get('limitaciones', []))} declaradas")
    print("-" * 72)


# ===========================================================================
# 11. Auditoría de la predicción emitida
# ===========================================================================
def audita_prediccion(pred: pd.DataFrame, disputados: pd.DataFrame,
                      entrenamiento: list[str], inferencia: list[str], *,
                      regimen: str = "semanal",
                      hoy: pd.Timestamp | None = None) -> list[str]:
    """Las tres comprobaciones que solo tienen sentido sobre lo que se emite.

    Se separan de `audita_regimen` porque su objeto es distinto: aquella audita el
    histórico ya evaluado y esta la jornada que todavía no se ha jugado. Son los
    dos momentos en que la publicación puede mentir, y mienten de formas
    diferentes —el histórico exagerando una ventaja, la emisión anticipando un
    resultado ya conocido—, así que se comprueban por separado.
    """
    fallos = check_probabilidades(pred, f"{regimen}/próxima jornada")
    fallos += check_jornada_futura(pred, disputados, hoy=hoy)
    fallos += check_orden_columnas(entrenamiento, inferencia)
    fallos += check_regimen(inferencia, regimen)
    return fallos


def run_qa(regimenes: list[str] | None = None, *, base: Path = OUT,
           verbose: bool = True) -> bool:
    """Audita todo lo publicado. Devuelve True si pasa; lanza si algo falla."""
    # Los regímenes se enumeran desde `columns.REGIMES` —la misma lista que
    # recorre `build_models`— y no desde el disco. Listar subcarpetas daba por
    # hecho que todo lo que vive bajo `outputs/models/` es un régimen, y no lo
    # es: ahí dejan su rastro `laboratorio.py` (`laboratorio/registro.parquet`)
    # y `seleccion.py` (`seleccion/`, con el conjunto cerrado, la traza de la
    # búsqueda y sus bitácoras). Al ejecutar este QA por primera vez, esas dos
    # carpetas se auditaron como si fueran publicaciones y abortaron con «falta
    # el manifiesto» dos regímenes que no existen, mientras `semanal` y
    # `retrospectivo` pasaban las nueve invariantes. Intersecar con lo que hay
    # en disco mantiene legítima la publicación de un solo régimen
    # (`--regimen semanal`), que es el caso de la rutina semanal.
    conocidos = tuple(regimenes) if regimenes else C.REGIMES
    if regimenes is None:
        conocidos = tuple(r for r in conocidos if (base / r).is_dir())
    regimenes = list(conocidos)
    if not regimenes:
        raise PublicacionError(f"no hay ningún régimen publicado en {base}")

    fallos = []
    for regimen in regimenes:
        fallos += audita_regimen(regimen, base=base, verbose=verbose)

    if fallos:
        raise PublicacionError("QA de modelado: " + str(len(fallos)) +
                               " problemas\n  - " + "\n  - ".join(fallos))
    if verbose:
        print("=" * 72)
        print("QA MODELOS — invariantes DUROS: OK")
        print("  1. probabilidades: la terna suma uno y cae en [0,1]")
        print("  2. próxima jornada futura y sin partidos ya disputados")
        print("  3. paridad de esquema entrenamiento <-> inferencia")
        print("  4. régimen semanal sin columnas posteriores al saque")
        print("  5. holdout puntuado una sola vez, tabla acorde al sello, y la "
              "temporada en curso separada de validación y del holdout")
        print("  6. modelo y referencias sobre el mismo conjunto de partidos")
        print("  7. el conjunto publicado está cerrado y es el que se midió")
        print("  8. el rendimiento va con el del apostante de referencia")
        print("  9. todos los artefactos de la misma ejecución")
        print("=" * 72)
    return True


if __name__ == "__main__":
    _reg = None
    if "--regimen" in sys.argv:
        _reg = [sys.argv[sys.argv.index("--regimen") + 1]]
    run_qa(_reg)
