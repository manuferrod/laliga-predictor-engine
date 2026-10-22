"""QA anti-fuga del set de features (Fase A).

Comprueba invariantes duros (rompen si fallan) y reporta cobertura. El objetivo
es poder afirmar en la memoria, con prueba, que el set de features NO contiene
información del propio partido:

  DUROS (raise):
   1. Ninguna columna de fuga (goles, sofa_*, xG/rating crudos) en el set.
   2. match_key único (1 fila por partido).
   3. Holdout sellado: las temporadas de HOLDOUT_SEASONS están 100% en split
      'holdout' y ninguna otra temporada lo está.
   4. Prueba del shift: en la 1ª jornada de cada equipo (matches_prev==0) TODAS
      las columnas de forma que el catálogo emite —lados y diferencias— son NaN.
      Si alguna tuviera valor, estaría viendo su propio partido -> fuga.
   5. Rangos plausibles: tasas dentro del dominio que corresponde al lado en que
      se emiten ([0,1] por lado, [-1,1] en diferencia), margen de mercado en
      rango de mercado real, zonas de tabla válidas y contexto coherente (franja
      de saque, indicadores binarios, distancia constante por par).
   6. Head-to-head computado: las tasas del historial no son íntegramente nulas.
      La igualdad entre lados de la tasa del par la exige `h2h.py` en
      construcción, y el dominio lo cubre el punto 5.
   7. Contexto completo: todo club del máster tiene estadio en la tabla, y el
      sello de saque no se ha colado como feature.
   8. Fuerza latente coherente: la terna 1X2 del modelo de goles es una
      distribución legítima (suma 1, dentro de [0,1]) y el bloque entero aparece
      o falta a la vez, porque describe un enfrentamiento y no a un equipo suelto.
   9. Procedencia completa: toda columna del set está declarada en la tabla de
      trazabilidad y toda columna declarada está en el set.
  10. Mercado y estados: el rendimiento relativo es un cociente definido y no
      negativo, y los arrastres psicológicos son indicadores binarios.

  REPORTE (no rompe): cobertura por bloque, distribución del objetivo.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import catalogo  # noqa: E402
import columns as C  # noqa: E402
from form import form_feature_names  # noqa: E402
from h2h import h2h_feature_names  # noqa: E402
from context import SLOT_ORDER, unknown_stadiums  # noqa: E402
from lineage import (  # noqa: E402
    BLOQUES_FUENTE, check_lineage, coverage_matrix, feature_lineage)


class LeakageError(AssertionError):
    pass


def _form_numeric_names() -> list[str]:
    """Nombres numéricos de forma (form_feature_names ahora devuelve dict)."""
    names = form_feature_names()
    if isinstance(names, dict):
        return list(names.get("numeric", []))
    return list(names)


def _check_no_leakage(feat: pd.DataFrame) -> None:
    leaks = [c for c in C.leakage_columns(feat.columns) if c in feat.columns]
    if leaks:
        raise LeakageError(f"columnas de fuga en el set de features: {leaks[:12]}"
                           f"{' ...' if len(leaks) > 12 else ''}")


def _check_unique(feat: pd.DataFrame) -> None:
    if not feat["match_key"].is_unique:
        raise AssertionError("match_key no es único en el set de features")


def _check_holdout(feat: pd.DataFrame) -> None:
    hold = feat["season"].isin(C.HOLDOUT_SEASONS)
    bad_in = feat.loc[hold, C.SPLIT].ne("holdout").sum()
    bad_out = feat.loc[~hold, C.SPLIT].eq("holdout").sum()
    if bad_in or bad_out:
        raise AssertionError(
            f"split holdout mal etiquetado: dentro_no_holdout={bad_in} "
            f"fuera_holdout={bad_out}")


def _mascara_debut(feat: pd.DataFrame) -> dict[str, pd.Series]:
    """Para cada lado, qué filas son el primer partido de ese equipo en la temporada.

    Se cuenta desde el calendario y no desde ninguna feature: se apila el set en
    formato largo (una fila por equipo y partido), se ordena por fecha dentro de
    cada temporada y se toma el ordinal. El primer partido es el ordinal cero.
    """
    faltan = [c for c in ("season", "date", "home_key", "away_key", "match_key")
              if c not in feat.columns]
    if faltan:
        raise AssertionError(
            f"faltan identificadores {faltan}: sin ellos no puede recalcularse "
            f"quién debuta y la prueba del shift no puede ejecutarse")
    largo = pd.concat(
        [feat[["season", "date", "match_key"]].assign(
            lado=lado, team=feat[f"{lado}_key"])
         for lado in ("home", "away")], ignore_index=True)
    largo["date"] = pd.to_datetime(largo["date"])
    largo = largo.sort_values(["season", "team", "date", "match_key"])
    largo["orden"] = largo.groupby(["season", "team"], sort=False).cumcount()
    debut = largo.loc[largo["orden"] == 0, ["match_key", "lado"]]
    return {lado: feat["match_key"].isin(
                set(debut.loc[debut["lado"] == lado, "match_key"]))
            for lado in ("home", "away")}


def _check_shift_first_matchday(feat: pd.DataFrame) -> None:
    """La prueba del shift, sobre las columnas que el catálogo emite de verdad.

    Es la comprobación anti-fuga central del módulo: la forma se reinicia con la
    temporada, así que en el primer partido de un equipo no hay pasado del que
    promediar y toda su forma tiene que ser hueco. Si alguna tuviera valor estaría
    mirando el partido que se quiere predecir.

    Quién debuta se RECALCULA aquí desde el calendario —temporada, fecha y claves
    de club, que el set arrastra como identificadores— en lugar de leerse de
    `matches_prev`. El motivo es doble. Uno, que el catálogo emite el conteo solo
    como diferencia, y una resta no dice cuál de los dos equipos debuta. Y dos,
    más de fondo: `matches_prev` sale del mismo recorrido que la forma que se
    quiere auditar, de modo que un fallo en ese recorrido desplazaría a la vez la
    forma y el testigo, y la prueba pasaría precisamente en el caso que debe
    detectar. Contando partidos desde el calendario, el testigo es independiente.

    Se recorren los nombres FINALES —los que devuelve `catalogo.nombres`— y no
    solo la variante `home_x`/`away_x`. La razón es que el catálogo curado emite
    la mayoría de la forma como diferencia, de modo que comprobar únicamente los
    lados dejaría fuera casi todo el bloque y la prueba pasaría por vacía sin que
    nada lo advirtiese. Una diferencia hereda el hueco de cualquiera de sus dos
    sumandos, así que basta con que uno de los dos equipos debute para exigirla
    nula; por eso la máscara de la columna `_diff` es la unión de los dos debuts.

    La única excepción declarada es la memoria que cruza el corte de temporada a
    propósito: el momento de Elo no se reinicia en agosto porque el Elo tampoco
    lo hace, y ahí un valor en la primera jornada es la conducta correcta.
    """
    mp = _mascara_debut(feat)
    cross_season = {"elo_mom_5_z"}
    offenders = []
    for base in _form_numeric_names():
        if base == "matches_prev" or base in cross_season:
            continue
        for col in catalogo.nombres(base):
            if col not in feat.columns:
                continue
            if col.endswith("_diff"):
                primera = np.logical_or.reduce(list(mp.values()))
            else:
                lado = col.split("_", 1)[0]
                if lado not in mp:
                    continue
                primera = mp[lado]
            if feat.loc[primera, col].notna().any():
                offenders.append(col)
    if offenders:
        raise LeakageError(
            f"features con valor en la 1ª jornada (fuga por falta de shift): "
            f"{offenders[:8]}{' ...' if len(offenders) > 8 else ''}")


def _check_ranges(feat: pd.DataFrame) -> None:
    """Rangos plausibles: probabilidades y tasas en [0,1], zonas categóricas válidas."""
    # Una tasa vive en [0,1] mientras se emita por lado o por partido; en cuanto
    # sale como DIFERENCIA de dos tasas su dominio pasa a ser [-1,1]. La cota se
    # deduce del nombre final que declara el catálogo, no se fija a mano, para que
    # cambiar el lado de emisión de un atributo no deje aquí una cota equivocada
    # —que es el modo de fallo peor: la comprobación seguiría pasando.
    prob_like: list[tuple[str, float]] = []
    for base in h2h_feature_names():
        if "_rate_" not in base:
            continue
        for col in catalogo.nombres(base, "h2h_"):
            prob_like.append((col, 1.0 if not col.endswith("_diff") else -1.0))
    prob_like += [(c, 0.0) for c in ("mkt_p_home", "mkt_p_draw", "mkt_p_away",
                                     "mkt_p_b365_home", "mkt_p_b365_draw",
                                     "mkt_p_b365_away")]
    for c, cota_inf in prob_like:
        if c not in feat.columns:
            continue
        lo = -1.0 if cota_inf < 0 else 0.0
        s = feat[c].dropna()
        if not s.empty and (s.min() < lo - 1e-9 or s.max() > 1 + 1e-9):
            raise AssertionError(
                f"{c} fuera de [{lo:.0f},1]: min={s.min():.4f} max={s.max():.4f}")

    # Estructura del precio. El margen de la casa es una suma de inversas sin
    # normalizar: por debajo de 1 la casa perdería dinero seguro, y por encima de
    # 1.6 el margen no corresponde a un mercado 1X2 real, así que un valor fuera de
    # ese rango delata un trío de cuotas corrupto que se ha colado como señal. La
    # dispersión, por su parte, compara la MEJOR cuota con el consenso y por
    # definición es no negativa: un valor negativo significa que las columnas `max`
    # y `avg` se han cruzado en la fuente.
    if "mkt_overround_open" in feat.columns:
        s = feat["mkt_overround_open"].dropna()
        if not s.empty and (s.min() < 0.95 - 1e-9 or s.max() > 1.60 + 1e-9):
            raise AssertionError(
                f"mkt_overround_open fuera del margen plausible [0.95, 1.60]: "
                f"min={s.min():.4f} max={s.max():.4f}")
    if "mkt_spread_mean" in feat.columns:
        s = feat["mkt_spread_mean"].dropna()
        if not s.empty and s.min() < -1e-9:
            raise AssertionError(
                f"mkt_spread_mean con valores negativos: min={s.min():.6f}")

    valid_zones = {"champions", "europa", "mid_table", "descenso"}
    for side in ("home", "away"):
        c = f"{side}_position_zone"
        if c in feat.columns:
            bad = set(feat[c].dropna().unique()) - valid_zones
            if bad:
                raise AssertionError(f"{c} con zonas no válidas: {bad}")

    # Trascendencia. Los partidos que restan salen de una resta contra el total
    # declarado de la liga, de modo que un valor negativo delataría un equipo con
    # más partidos jugados de los que tiene el calendario —típicamente una
    # temporada con jornadas duplicadas por un cruce de claves— y un valor por
    # encima de 38, una tabla previa contada de más.
    for side in ("home", "away"):
        c = f"{side}_matches_left"
        if c in feat.columns:
            s = feat[c].dropna()
            if not s.empty and (s.min() < -1e-9 or s.max() > 38 + 1e-9):
                raise AssertionError(
                    f"{c} fuera de [0, 38]: min={s.min():.1f} max={s.max():.1f}")

    # Indicadores binarios. Todos nacen de una comparación booleana y se guardan
    # como float para admitir el hueco; cualquier valor distinto de 0 y 1 sería
    # una media, un conteo o una suma que se ha colado en su lugar. Cuando el
    # catálogo emite el indicador como DIFERENCIA el conjunto admisible deja de
    # ser {0,1} y pasa a ser {-1,0,1}: restar dos banderas da tres estados, no
    # dos, y exigir aquí los dos originales haría fallar una columna correcta.
    _BASES_BINARIAS = ("safe_from_drop", "post_big_win", "fat_euro_trip_recent")
    binarias: list[str] = ["is_weekend", "is_derby", "stakes_none_match"]
    for base in _BASES_BINARIAS:
        binarias += catalogo.nombres(base)
    for c in binarias:
        if c not in feat.columns:
            continue
        admisible = {-1.0, 0.0, 1.0} if c.endswith("_diff") else {0.0, 1.0}
        vals = set(pd.unique(feat[c].dropna()))
        if not vals <= admisible:
            raise AssertionError(
                f"{c} no es binaria (admisible {sorted(admisible)}): "
                f"valores {sorted(vals)[:6]}")

    # Coherencia lógica de la trascendencia. El catálogo emite el indicador
    # conjunto —`stakes_none_match`, el producto de los dos lados— y descarta los
    # dos indicadores por equipo, así que la implicación «no jugarse nada exige
    # estar salvado» ya no puede comprobarse contra columnas del set. Lo que sí
    # queda comprobable, y es lo que importa aquí, es que el indicador conjunto no
    # se active donde el local aún puede caer: si el margen sobre el descenso está
    # a favor del visitante por más de lo que quedan por jugar, alguien ha
    # multiplicado banderas de partidos distintos. Esa igualdad la verifica
    # `test_standings.py` sobre el long, que es donde viven los dos lados; aquí
    # basta con la binariedad del producto, ya cubierta más arriba.

    # Contexto de calendario. La franja solo admite las etiquetas declaradas: una
    # distinta significaría que el sello de saque se ha leído en la unidad
    # equivocada (milisegundos en lugar de segundos) o sin convertir de huso. La
    # hora numérica ya no se emite —respondía a la misma pregunta que la franja
    # con más resolución de la que el fenómeno tiene—, de modo que la franja es
    # ahora el único testigo de que el sello se ha interpretado bien.
    if "kickoff_slot" in feat.columns:
        bad = set(feat["kickoff_slot"].dropna().unique()) - set(SLOT_ORDER)
        if bad:
            raise AssertionError(f"kickoff_slot con franjas no válidas: {bad}")

    # Geografía. La distancia ortodrómica es no negativa y está acotada por el
    # diámetro del país; además es una CONSTANTE del par de clubes, porque cada
    # club tiene un único emplazamiento. Que varíe dentro de un mismo par sería
    # señal de que la distancia se ha calculado con coordenadas de fila y no de
    # tabla, es decir, de que algo del propio partido se ha mezclado en ella.
    if "away_travel_km" in feat.columns:
        s = feat["away_travel_km"].dropna()
        if not s.empty and (s.min() < -1e-9 or s.max() > 2500.0):
            raise AssertionError(
                f"away_travel_km fuera de [0, 2500] km: "
                f"min={s.min():.1f} max={s.max():.1f}")
        if {"home_key", "away_key"} <= set(feat.columns):
            var = (feat.dropna(subset=["away_travel_km"])
                       .groupby(["home_key", "away_key"])["away_travel_km"]
                       .nunique(dropna=True))
            if not var.empty and var.max() > 1:
                pares = var[var > 1].index.tolist()[:4]
                raise AssertionError(
                    f"away_travel_km no es constante por par de clubes: {pares}")


def _check_strength(feat: pd.DataFrame) -> None:
    """Fuerza latente: distribución legítima y hueco íntegro.

    Las dos comprobaciones responden a los dos modos de fallo del bloque que
    quedan vivos, y ninguno se detecta de otra manera porque ambos producen
    números creíbles.

    Que la terna sume uno no es una formalidad: es lo que permite tratarla como
    el pronóstico de un modelo —se evalúa con log-loss en la fase 5 y compite
    con el resto de estimadores— y lo que delataría un truncamiento de la
    rejilla de marcadores aplicado sin renormalizar.

    Los goles esperados por lado ya no salen del módulo: son el paso intermedio
    del que nace la terna, y emitirlos además como variables era describir el
    mismo ajuste dos veces. Su rango futbolístico se comprueba ahora donde vive,
    en `test_strength.py` sobre la salida interna del ajuste, y no aquí.

    Y que el bloque aparezca o falte entero es la comprobación de que el hueco
    se propaga como debe. Si un lado tuviera fuerza y el otro no, la
    probabilidad del partido se habría calculado con media descripción, o —peor—
    el preprocesado imputaría el lado ausente con la mediana de la liga y el
    pronóstico resultante parecería una medición.
    """
    probs = ["strength_p_home", "strength_p_draw", "strength_p_away"]
    if all(c in feat.columns for c in probs):
        sub = feat[probs].dropna()
        if not sub.empty:
            if sub.to_numpy().min() < -1e-9 or sub.to_numpy().max() > 1 + 1e-9:
                raise AssertionError("strength_p_* fuera de [0,1]")
            err = float((sub.sum(axis=1) - 1).abs().max())
            if err > 1e-6:
                raise AssertionError(
                    f"strength_p_* no suma 1 (error máximo {err:.2e}): la terna "
                    f"no es una distribución y no puede evaluarse como pronóstico")

    bloque = [c for c in feat.columns if "strength_" in c]
    if bloque:
        n_nan = feat[bloque].isna().sum(axis=1)
        if not n_nan.isin([0, len(bloque)]).all():
            parcial = int((~n_nan.isin([0, len(bloque)])).sum())
            raise AssertionError(
                f"{parcial} partidos con el bloque de fuerza a medias: sus "
                f"columnas deben estar todas presentes o todas ausentes")


def _check_h2h(feat: pd.DataFrame) -> None:
    """Head-to-head: coherencia de la tasa del par, dominio y bloque computado.

    El bloque sale de un long DIRIGIDO —cada partido produce la fila del par
    (equipo, rival) y la del par (rival, equipo), recorriendo la misma secuencia
    de enfrentamientos desde lados opuestos—, y de ahí nacen los tres modos de
    fallo que se comprueban aquí. Ninguno se detecta leyendo los números, porque
    los tres producen valores perfectamente creíbles.

    El primero es de agrupamiento, y es el grave. Si las claves de club no
    estuvieran canonizadas, el agrupamiento mezclaría pares y el historial que
    recibiría el modelo sería el de una rivalidad que no existe. La comprobación
    aprovecha que la tasa de EMPATE es una magnitud del par y no del equipo: un
    empate lo es para los dos a la vez, así que los dos lados tienen que llevar
    exactamente el mismo número. Esa igualdad la exige `h2h.py` al construir la
    columna —falla en construcción si se rompe— y por eso aquí no queda nada que
    volver a comprobar sobre ella salvo su dominio.

    El segundo es de dominio. Las tasas viven en [0,1] y su diferencia en [-1,1];
    salirse significa que el relleno neutral de los pares sin historia previa se
    ha aplicado a unas columnas sí y a otras no, o que alguna se ha calculado con
    otra parametrización. Esa parte la cubre `_check_ranges`, que deduce la cota
    del lado en que el catálogo emite cada tasa.

    El tercero es de cobertura, y es lo único que se comprueba en esta función:
    una columna íntegramente nula significaría que el bloque no llegó a
    computarse, y como el preprocesado la imputaría sin protestar, el modelo se
    entrenaría sin historial creyendo que lo tiene.
    """
    testigos = [c for base in h2h_feature_names()
                for c in catalogo.nombres(base, "h2h_")
                if "_rate_" in base and c in feat.columns]
    vacias = [c for c in testigos if feat[c].notna().sum() == 0]
    if vacias:
        raise AssertionError(
            f"tasas h2h íntegramente nulas {vacias}: el bloque no se computó, y "
            f"la imputación de la fase 4 lo taparía sin protestar")


def _check_market_form(feat: pd.DataFrame) -> None:
    """Mercado y estados: cociente definido y arrastres binarios.

    `relative_perf` es la única feature del set que nace de una DIVISIÓN, y esa
    forma admite un fallo que ninguna media admite: un denominador nulo produce
    un infinito que sobrevive al preprocesado disfrazado de valor extremo,
    porque el escalado robusto lo arrastra y la imputación no lo toca. El módulo
    lo evita convirtiendo el cero en hueco; aquí se verifica que la guarda sigue
    puesta. El signo se comprueba por la misma razón: el numerador es una media
    de indicadores y el denominador una media de probabilidades, así que un
    valor negativo solo podría venir de haber restado donde había que dividir.

    Los dos arrastres psicológicos son indicadores del partido ANTERIOR y por
    tanto valen cero, uno o hueco. Un valor intermedio significaría que se ha
    promediado una ventana en lugar de desplazar una bandera, y la hipótesis que
    el bloque pone a prueba —el bajón posterior a una victoria inesperada— dejaría
    de ser la que la memoria describe.
    """
    for c in catalogo.nombres("relative_perf"):
        if c not in feat.columns:
            continue
        s = feat[c].dropna()
        if s.empty:
            continue
        if not np.isfinite(s.to_numpy()).all():
            raise AssertionError(
                f"{c} contiene valores infinitos: la expectativa de mercado nula "
                f"debe producir hueco, no una división sin definir")
        if not c.endswith("_diff") and s.min() < -1e-9:
            raise AssertionError(
                f"{c} con valores negativos (min={s.min():.4f}): es un cociente "
                f"de dos medias no negativas")


def _check_precios(feat: pd.DataFrame) -> None:
    """Las dos lecturas del precio son distribuciones, y son dos lecturas distintas.

    Consenso y Bet365 se des-marginan con el mismo procedimiento y por eso cada
    terna tiene que sumar exactamente uno. La comprobación importa más aquí que en
    cualquier otro bloque porque estas ternas no son variables sino el NÚMERO A
    BATIR: un log-loss calculado sobre una terna que suma 1.02 saldría
    artificialmente alto y el modelo parecería superar al mercado sin haberlo
    hecho. La conclusión central del trabajo depende de esta igualdad.

    Son dos y no tres porque el listón declarado del trabajo es Bet365, y el
    consenso es el contexto en que se lee. Pinnacle respondía a la misma pregunta
    con una tercera cotización —informativa, pero no citable como referencia
    porque no cubre el histórico completo— y salió del catálogo con el resto de
    duplicados de concepto.

    La segunda comprobación es de identidad: las dos deben diferir entre sí en
    algún partido. Si una fuera copia exacta de la otra, el cuadro de referencias
    estaría enseñando el mismo precio dos veces bajo dos nombres y la lectura «lo
    batimos contra el mercado» dejaría de significar nada. Como el consenso
    promedia casas y Bet365 es una de ellas, el parecido es esperable; la
    igualdad literal, no.

    Antes que ninguna de las dos va una comprobación más basta: que las seis
    columnas estén. Se separa de la cobertura porque son cosas distintas. Que una
    terna venga vacía en parte del histórico es un hecho del mercado, se informa y
    no se exige; de restringir a las filas comunes ya se encarga la evaluación.
    Que la terna no exista como columna es otra cosa: significa que el artefacto
    se construyó antes de que esa referencia se declarase, y entonces el
    preprocesado no puede adjuntar su copia cruda y la fase de modelado se queda
    sin listón. El aviso tiene que llegar aquí, con el nombre de la referencia que
    falta, y no seis pasos después en forma de `KeyError`.
    """
    ternas = {
        "consenso": ("mkt_p_home", "mkt_p_draw", "mkt_p_away"),
        "b365": tuple(catalogo.SERVICIO_B365),
    }
    ausentes = {n: [c for c in cols if c not in feat.columns]
                for n, cols in ternas.items()}
    ausentes = {n: c for n, c in ausentes.items() if c}
    if ausentes:
        raise AssertionError(
            f"faltan columnas de las referencias de precio {ausentes}. Es la "
            "firma de un artefacto construido antes de que la referencia se "
            "declarase: hay que rehacer `build_features.py`, porque sin estas "
            "columnas el preprocesado no puede adjuntar su copia cruda y la "
            "fase de modelado se queda sin listón contra el que medirse")

    presentes = {}
    for nombre, cols in ternas.items():
        sub = feat[list(cols)].dropna()
        if sub.empty:
            continue
        presentes[nombre] = sub
        err = float((sub.sum(axis=1) - 1).abs().max())
        if err > 1e-9:
            raise AssertionError(
                f"la terna de precio '{nombre}' no suma 1 (error máximo {err:.2e}): "
                f"no es una distribución y no puede servir de referencia")

    nombres = list(presentes)
    for i, a in enumerate(nombres):
        for b in nombres[i + 1:]:
            comun = presentes[a].index.intersection(presentes[b].index)
            if len(comun) < 2:
                continue
            A = feat.loc[comun, list(ternas[a])].to_numpy(dtype="float64")
            B = feat.loc[comun, list(ternas[b])].to_numpy(dtype="float64")
            if np.allclose(A, B, atol=1e-12):
                raise AssertionError(
                    f"los precios '{a}' y '{b}' son idénticos en las {len(comun)} "
                    f"filas comunes: no son dos referencias, son una repetida")


def _check_context(feat: pd.DataFrame) -> None:
    """Contexto completo: cobertura geográfica total y sello de saque no filtrado.

    Son dos comprobaciones de naturaleza distinta y por eso van juntas aquí en
    lugar de con los rangos. La primera es de COBERTURA y se hace dura a
    propósito: un club sin estadio deja `away_travel_km` e `is_derby` en NaN
    para todos sus partidos, y la imputación posterior sustituiría ese hueco por
    una distancia mediana perfectamente creíble e inventada; el fallo, si no se
    detiene aquí, se vuelve invisible más abajo.

    La segunda es de FUGA por otra vía que la de la información del resultado:
    `kickoff_ts` entra en el máster como metadato y su función termina al
    derivar la hora y la franja. Como entero de segundos desde la época es un
    índice temporal casi único por partido, de modo que un estimador con
    capacidad suficiente podría memorizar el calendario a través de él en lugar
    de aprender del juego. Que no aparezca en el set de features no es una
    cuestión de estilo sino la garantía de que la hora se usa como hora.
    """
    if {"home_key", "away_key"} <= set(feat.columns):
        sin_sede = unknown_stadiums(feat)
        if sin_sede:
            raise AssertionError(
                f"clubes sin estadio en la tabla de `context.py`: {sin_sede}. "
                f"`away_travel_km` e `is_derby` quedan NaN en sus partidos")

    meta = [c for c in C.MASTER_METADATA if c in feat.columns]
    if meta:
        raise LeakageError(
            f"metadato del máster en el set de features: {meta}. No debe viajar "
            f"al modelo: el contexto de calendario se expresa con `kickoff_slot` "
            f"e `is_weekend`, no con el sello en crudo")


def _check_lineage(feat: pd.DataFrame) -> None:
    """Procedencia completa: la tabla de trazabilidad y el set describen lo mismo.

    La comprobación es una IGUALDAD DE CONJUNTOS, y lo es en los dos sentidos
    porque cada dirección detecta un fallo distinto.

    Una columna del set SIN procedencia declarada es una columna que la memoria
    no puede justificar: no se sabría decir de qué fuente viene, con qué
    parametrización se calcula ni si está disponible en el momento de predecir.
    Al ser dura esta comprobación, la trazabilidad deja de ser documentación
    —que envejece en silencio— y pasa a ser un requisito de construcción: quien
    añada una feature sin declararla verá fallar el QA en la misma pasada.

    Una columna DECLARADA que no aparece en el set es el síntoma opuesto y suele
    tener una causa más grave: no falta una línea de documentación, falta el
    dato. Que `form_rating_ewm_diff` esté declarada y ausente significa que el
    bloque de rating no llegó al máster, y el modelo se entrenaría con menos
    información de la que el trabajo afirma usar sin que nada lo advirtiera.

    El fallo se detiene aquí, antes del preprocesado, porque la imputación
    posterior no distingue una columna ausente de una columna vacía y borraría
    el rastro de ambas.
    """
    problema = check_lineage(feat)
    partes = []
    if problema["sin_procedencia"]:
        faltan = problema["sin_procedencia"]
        partes.append(
            f"{len(faltan)} columnas del set sin procedencia declarada "
            f"{faltan[:8]}{' ...' if len(faltan) > 8 else ''}: decláralas en "
            f"`features/lineage.py` indicando bloque, fuentes y naturaleza")
    if problema["declaradas_ausentes"]:
        sobran = problema["declaradas_ausentes"]
        partes.append(
            f"{len(sobran)} columnas declaradas que NO están en el set "
            f"{sobran[:8]}{' ...' if len(sobran) > 8 else ''}: no es un desajuste "
            f"de documentación sino de datos, revise qué fuente no llegó al máster")
    if partes:
        raise AssertionError("procedencia incompleta -> " + " | ".join(partes))


def _check_testigos(master: pd.DataFrame) -> None:
    """Los testigos de cada bloque de fuente existen y su indicador puede variar.

    `coverage_matrix` resume el estado de cada fuente partido a partido mirando
    las columnas TESTIGO que el bloque declara. Esa lista está escrita a mano
    —es el único conocimiento que no se puede deducir de ningún sitio— y por eso
    es el punto por el que la matriz de cobertura se degrada en silencio: un
    testigo que no existe en el máster, por un renombrado o por una errata, no
    produce ningún error. Produce un indicador que vale cero en los 3800
    partidos, que la rutina semanal imprime cada jornada y que el ojo aprende a
    saltarse, de modo que la fuente podría caerse de verdad sin que la tabla
    cambiara de aspecto.

    Se comprueba lo mínimo que hace informativo al indicador: que los testigos
    están en el máster y que el indicador se enciende en algún partido. Lo que
    delata al testigo roto es el cero PERPETUO, no la constancia: un bloque al
    100 % en las diez temporadas también tiene un indicador constante, y ahí la
    constancia es la respuesta correcta. Y valer cero en una temporada entera
    es legítimo —las cuotas de cierre no existen antes de 2019/20—; valerlo en
    los 3800 partidos no, porque entonces la fuente no habría llegado nunca y el
    bloque no debería estar declarado.
    """
    faltan = {b.id: [c for c in b.columnas if c not in master.columns]
              for b in BLOQUES_FUENTE if not b.sin_master}
    faltan = {k: v for k, v in faltan.items() if v}
    if faltan:
        raise AssertionError(
            "testigos de cobertura que no existen en el máster -> "
            + " | ".join(f"bloque '{k}': {v}" for k, v in faltan.items())
            + ". El indicador de esos bloques valdría cero en todos los partidos "
              "sin que nada más fallara; corrige los nombres en "
              "`features/lineage.py`")

    cov = coverage_matrix(master)
    mudos = [c[7:] for c in cov.columns
             if c.startswith("fuente_") and not cov[c].any()]
    if mudos:
        raise AssertionError(
            f"bloques cuyo indicador de cobertura vale cero en los "
            f"{len(cov)} partidos: {mudos}. No es cobertura, es un testigo que "
            f"no mide: o las columnas declaradas nunca llegan juntas o el listón "
            f"es inalcanzable")


def _report(feat: pd.DataFrame) -> None:
    """Cobertura por bloque sobre la partición de desarrollo.

    Los bloques no se recomponen aquí con patrones sobre los nombres: se leen de
    la tabla de procedencia, que es quien declara a qué bloque pertenece cada
    columna. Así el informe no puede discrepar del anexo de la memoria, y una
    familia nueva aparece en los dos sitios sin tocar este fichero.

    Conviene leer la cobertura recordando de qué depende cada bloque. La forma
    depende de que la fuente cubra el atributo, pero también de que el equipo
    tenga partidos previos, de modo que su hueco en la primera jornada es el
    comportamiento correcto y no una carencia. La fuerza latente no depende de
    ninguna fuente externa —sale del marcador, que el máster siempre tiene— y
    mide por tanto otra cosa: en cuántos partidos llegan los DOS clubes con
    historia suficiente. Y la geografía es completa por construcción salvo que
    ascienda un club sin estadio en la tabla, caso que `_check_context`
    convierte en fallo duro antes de llegar aquí.
    """
    print("-" * 72)
    print("COBERTURA por bloque (% no nulo, media sobre filas dev):")
    dev = feat[feat[C.SPLIT] == "dev"]

    lin = feature_lineage()
    presentes = set(feat.columns)
    for nombre, grupo in lin.groupby("bloque", sort=True):
        cols = [c for c in grupo["columna"] if c in presentes]
        if not cols:
            print(f"  {nombre:22s}: (sin columnas)")
            continue
        cov = dev[cols].notna().mean().mean()
        print(f"  {nombre:22s}: {cov:6.1%}  ({len(cols)} cols)")

    dist = feat[C.TARGET].value_counts(normalize=True)
    print("\nobjetivo 1X2:  " +
          "  ".join(f"{k}={dist.get(k, 0):.1%}" for k in ("H", "D", "A")))
    print("-" * 72)


def run_qa(feat: pd.DataFrame, master: pd.DataFrame | None = None,
           *, verbose: bool = True) -> bool:
    """Ejecuta todos los checks. Devuelve True si pasa; raise en fallo duro."""
    _check_no_leakage(feat)
    _check_unique(feat)
    _check_holdout(feat)
    _check_shift_first_matchday(feat)
    _check_ranges(feat)
    _check_h2h(feat)
    _check_context(feat)
    _check_strength(feat)
    _check_lineage(feat)
    _check_market_form(feat)
    _check_precios(feat)
    if master is not None:
        _check_testigos(master)
    if verbose:
        print("=" * 72)
        print("QA FEATURES — invariantes DUROS: OK")
        print("  1. sin columnas de fuga")
        print("  2. match_key único")
        print("  3. holdout sellado")
        print("  4. forma NaN en la 1ª jornada (prueba del shift), lados y difs")
        print("  5. rangos plausibles: tasas dentro del dominio de su lado,")
        print("     zonas de tabla válidas, franja de saque, indicadores")
        print("     binarios y distancia constante por par de clubes")
        print("  6. h2h computado: las tasas del historial no son todo NaN")
        print("  7. contexto completo: todo club tiene estadio y el sello de")
        print("     saque no se ha colado como feature")
        print("  8. fuerza latente coherente: la terna 1X2 suma 1 y el bloque")
        print("     no queda a medias")
        print("  9. procedencia completa: toda columna del set está declarada en")
        print("     la tabla de trazabilidad y toda declarada está en el set")
        print(" 10. mercado y estados: el rendimiento relativo es un cociente")
        print("     definido y no negativo, y los arrastres son binarios")
        print(" 11. precios: las ternas de consenso y Bet365 suman 1 y son dos")
        print("     lecturas distintas, no la misma repetida")
        if master is not None:
            print(" 12. testigos de cobertura: existen en el máster y su")
            print("     indicador se enciende en algún partido, no es un cero")
            print("     perpetuo que la rutina imprime sin medir nada")
        print("=" * 72)
        _report(feat)
    return True


def _selfcheck() -> None:
    from build_features import _load_master, build_features
    master = _load_master()
    run_qa(build_features(master), master)


if __name__ == "__main__":
    _selfcheck()
