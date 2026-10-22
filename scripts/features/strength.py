"""Fuerzas latentes de ATAQUE y DEFENSA estimadas con un modelo de goles.

El Elo resume la fuerza de un equipo en un único escalar y, al hacerlo, borra la
distinción entre las dos maneras de ser bueno: marcar mucho y encajar poco. Dos
clubes con el mismo rating pueden llegar a él por caminos opuestos —uno que gana
3-2 y otro que gana 1-0— y esa diferencia importa para un pronóstico 1X2, porque
el empate es mucho más probable en el encuentro de dos equipos que apenas
generan que en el de dos que se desbordan mutuamente.

Este módulo estima esas dos componentes por separado con el modelo clásico de
goles del fútbol (Maher, 1982; Dixon y Coles, 1997). Cada equipo recibe un
parámetro de ataque `a` y uno de defensa `d`, y la liga entera un nivel medio
`mu` y una ventaja de campo `h`, de modo que los goles esperados de un partido
entre el local *i* y el visitante *j* son

    λ_local     = mu · a_i · d_j · h
    λ_visitante = mu · a_j · d_i

y los goles observados se modelan como dos Poisson independientes de esas
medias. De ahí salen dos lecturas por equipo —cuánto ataca y cuánto defiende— y
una distribución completa sobre el marcador, que agregada por regiones da la
probabilidad de victoria local, empate y victoria visitante. Las dos medias λ
son el paso intermedio entre ambas cosas y no llegan a la matriz: lo que dicen
está ya en la terna de probabilidades.

DECISIONES DE MODELO
--------------------
*Independencia de los dos marcadores.* Dixon y Coles añaden un factor `τ` que
corrige la probabilidad conjunta de los cuatro resultados de pocos goles (0-0,
1-0, 0-1, 1-1), donde la independencia se queda corta. Aquí no se incluye, y la
omisión es deliberada: `τ` mejora la probabilidad de un MARCADOR concreto, que
no es lo que se pide al bloque —sus columnas son fuerzas y goles esperados—, y a
cambio obliga a una optimización numérica general en lugar del ajuste cerrado
que se describe abajo, multiplicando por dos órdenes de magnitud el coste de un
reajuste que se repite en cada fecha del histórico. La probabilidad 1X2 que sí
se publica agrega regiones enteras del marcador, donde el efecto de `τ` es de
segundo orden.

*Memoria exponencial.* Un partido pesa `0.5 ** (Δdías / HALF_LIFE_DAYS)`, de
modo que la fuerza estimada es un promedio ponderado que olvida el pasado de
forma suave en lugar de a corte. La semivida se fija en medio año: media
temporada atrás un partido vale la mitad y la temporada anterior entra completa
pero pesando en torno a un cuarto, que es la manera de reconocer que un equipo
no se reinicia cada agosto sin dejar que arrastre indefinidamente lo que fue.
El valor cae dentro del rango que Dixon y Coles estiman por máxima verosimilitud
para el fútbol inglés. Se declara como constante del módulo y NO se ajusta
contra el holdout: es un parámetro de construcción del dato, no de selección.

*Encogimiento hacia la media.* Cada parámetro se estima con `PRIOR_MATCHES`
partidos ficticios de rendimiento exactamente medio. Sin esa referencia, un
equipo que en su muestra ponderada no hubiera marcado todavía obtendría `a = 0`
—y por tanto un logaritmo infinito—, y uno con dos partidos afortunados
obtendría una fuerza extrema sostenida por nada. Con ella, la estimación se
desplaza desde la media de la liga hacia el dato propio a medida que ese dato
existe, que es el comportamiento razonable y el que hace utilizable la primera
parte de cada temporada.

*Historia mínima.* Aun con encogimiento, un equipo sin recorrido no tiene
fuerza estimable, y devolver la media de la liga como si fuera una medición
sería sustituir ignorancia por un valor creíble e inventado. Por eso el bloque
exige `MIN_EFFECTIVE_MATCHES` partidos EFECTIVOS —la suma de los pesos, no el
recuento bruto— y por debajo de ese umbral deja las columnas del equipo en NaN.
El umbral se expresa en las mismas unidades que el encogimiento para que la
relación entre ambos sea legible: con dos partidos ficticios de referencia,
exigir tres efectivos garantiza que el dato propio pesa más que esa referencia.
El hueco lo trata después el preprocesado, que lo marca con su flag.

DISCIPLINA ANTI-FUGA
--------------------
Los parámetros de un partido se estiman con los partidos disputados en fechas
ESTRICTAMENTE ANTERIORES a la suya. El reajuste es por tanto diario: hay tantos
ajustes como fechas distintas tiene el calendario. Dos partidos del mismo día no
se informan entre sí, ni un partido se informa a sí mismo, que es la misma regla
—y el mismo corte por fecha— que aplica `standings.py` a la clasificación
previa. Las filas sin resultado (partidos futuros de la ruta de inferencia) no
entran nunca en el ajuste, así que el módulo funciona igual sobre el máster
jugado y sobre el máster extendido con la jornada por venir.

DOBLE PAPEL
-----------
La pieza sirve para dos cosas y por eso se escribe una sola vez. Como bloque de
features aporta las columnas declaradas en `strength_feature_names`. Como modelo
autónomo, la terna `strength_p_home/draw/away` es ya un pronóstico 1X2 completo
—construido sin mirar el mercado y sin ningún estimador entrenado— y entra en la
comparación de la fase 5 como referencia de conocimiento de dominio puro, por
encima del azar y por debajo de lo que se le exige a un modelo ajustado.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_EXTRACTORS = _HERE.parent / "extractors"
if str(_EXTRACTORS) not in sys.path:
    sys.path.insert(0, str(_EXTRACTORS))

from _common import assert_unique  # noqa: E402

import catalogo  # noqa: E402

_NEED = ["match_key", "date", "home_key", "away_key", "fthg", "ftag"]

# --- parámetros de construcción (constantes del módulo, no hiperparámetros) ---
HALF_LIFE_DAYS = 180.0        # semivida de la memoria exponencial (medio año)
WINDOW_DAYS = 1095            # se ignora lo anterior a 3 años (peso < 0.015)
PRIOR_MATCHES = 2.0           # partidos ficticios de rendimiento medio
MIN_EFFECTIVE_MATCHES = 3.0   # historia mínima para publicar una fuerza

_MAX_GOALS = 10               # truncamiento de la rejilla de marcadores
_MAX_ITER = 200               # tope de barridos del ajuste alternado
_TOL = 1e-8                   # convergencia en escala logarítmica

# Valores iniciales del primer ajuste. Son los del fútbol español en el periodo:
# ~1.35 goles por equipo y partido y una ventaja de campo en torno al 25 %. Solo
# afectan a la velocidad de convergencia, no al punto en que converge.
_MU0 = 1.35
_H0 = 1.25

_NUMERIC = list(catalogo.FUERZA_ATRIBUTOS)
_MATCH_LEVEL = list(catalogo.FUERZA_PRONOSTICO)


# --------------------------------------------------------------------------- #
# ajuste
# --------------------------------------------------------------------------- #
def _solve(att: np.ndarray, dfn: np.ndarray, es_local: np.ndarray,
           goles: np.ndarray, peso: np.ndarray, n_equipos: int,
           init: tuple) -> tuple:
    """Máxima verosimilitud del modelo de goles por ascenso alternado en bloques.

    Cada fila de la entrada es un EQUIPO ANOTANDO en un partido: quién ataca,
    quién defiende, si lo hace en casa, cuántos goles marcó y cuánto pesa ese
    partido. Un partido produce por tanto dos filas, una por dirección.

    El ajuste no necesita un optimizador general. Fijados los demás parámetros, el
    máximo de la verosimilitud de Poisson en el que se busca es una razón entre lo
    OBSERVADO y lo ESPERADO, de forma que cada bloque se resuelve en forma cerrada
    y el barrido completo se repite hasta que nada cambia:

        a_i = (goles marcados por i) / (goles que se le esperarían siendo medio)

    con el numerador y el denominador aumentados en `PRIOR_MATCHES` partidos de
    rendimiento medio, que es lo que impide que un equipo sin muestra obtenga una
    fuerza infinita o nula. La función es cóncava en los logaritmos y cada bloque
    se lleva a su óptimo exacto, así que la sucesión es monótona y converge.

    El modelo está sobre-parametrizado —multiplicar todos los ataques por una
    constante y dividir el nivel medio por ella da exactamente las mismas medias—,
    de modo que al final de cada barrido se normaliza a media geométrica 1 en los
    ataques y en las defensas y el factor se traslada al nivel medio. Sin esa
    normalización los parámetros derivarían sin cambiar el ajuste y las columnas
    dejarían de ser comparables entre fechas.
    """
    a, d, h, mu = init
    a, d = a.copy(), d.copy()
    local = es_local == 1
    presente = np.bincount(att, weights=peso, minlength=n_equipos) > 0
    w_gol = peso * goles
    suma_gol = float(w_gol.sum())
    suma_gol_local = float(w_gol[local].sum())

    for _ in range(_MAX_ITER):
        a_prev, d_prev, h_prev, mu_prev = a.copy(), d.copy(), h, mu
        k = PRIOR_MATCHES * mu                      # el encogimiento, en goles

        # ataque: observado / esperado, con el rival y el campo ya descontados
        hpow = np.where(local, h, 1.0)
        marcados = np.bincount(att, weights=w_gol, minlength=n_equipos)
        esperados = np.bincount(att, weights=peso * mu * d[dfn] * hpow,
                                minlength=n_equipos)
        a = (marcados + k) / (esperados + k)

        # defensa: lo mismo leído del otro lado (encajado / esperado encajar)
        encajados = np.bincount(dfn, weights=w_gol, minlength=n_equipos)
        esperados = np.bincount(dfn, weights=peso * mu * a[att] * hpow,
                                minlength=n_equipos)
        d = (encajados + k) / (esperados + k)

        # ventaja de campo: el exceso del anotador local sobre lo que ya explican
        # las fuerzas de los dos equipos. Si en la muestra ponderada no se ha
        # marcado todavía ningún gol en casa, la razón no está definida —su
        # máximo se iría a cero, que no es una estimación sino la ausencia de
        # una— y el parámetro se deja donde estaba: no hay con qué moverlo. El
        # caso solo aparece en las primerísimas fechas del histórico, cuando el
        # bloque no publica nada todavía por falta de historia mínima.
        base = mu * a[att] * d[dfn]
        den = float(np.sum(peso[local] * base[local]))
        if den > 0 and suma_gol_local > 0:
            h = suma_gol_local / den

        # nivel medio de la liga, con la misma cautela
        hpow = np.where(local, h, 1.0)
        den = float(np.sum(peso * a[att] * d[dfn] * hpow))
        if den > 0 and suma_gol > 0:
            mu = suma_gol / den

        # identificabilidad: media geométrica 1 en ataques y defensas
        for vec in (a, d):
            g = float(np.exp(np.mean(np.log(vec[presente]))))
            vec /= g
            mu *= g
        a[~presente] = 1.0
        d[~presente] = 1.0

        delta = max(float(np.max(np.abs(np.log(a[presente] / a_prev[presente])))),
                    float(np.max(np.abs(np.log(d[presente] / d_prev[presente])))),
                    abs(np.log(h / h_prev)), abs(np.log(mu / mu_prev)))
        if delta < _TOL:
            break
    return a, d, h, mu


def _prepare(master: pd.DataFrame) -> dict:
    """Índices, fechas y marcadores del máster en forma de vectores."""
    faltan = [c for c in _NEED if c not in master.columns]
    if faltan:
        raise KeyError(f"strength: faltan columnas en el máster: {faltan}")

    fecha = pd.to_datetime(master["date"]).dt.normalize()
    equipos = pd.Index(sorted(set(master["home_key"].dropna())
                              | set(master["away_key"].dropna())))
    gh = pd.to_numeric(master["fthg"], errors="coerce").to_numpy(dtype="float64")
    ga = pd.to_numeric(master["ftag"], errors="coerce").to_numpy(dtype="float64")
    local = equipos.get_indexer(master["home_key"].astype("object"))
    visit = equipos.get_indexer(master["away_key"].astype("object"))
    return {
        "equipos": equipos,
        "dia": ((fecha - fecha.min()).dt.days.to_numpy(dtype="float64")),
        "local": local, "visit": visit, "gh": gh, "ga": ga,
        # un partido entra en el ajuste solo si tiene marcador y los dos clubes
        # identificados: las filas futuras de la ruta de inferencia no lo tienen y
        # quedan fuera por construcción, sin ninguna regla adicional
        "jugado": np.isfinite(gh) & np.isfinite(ga) & (local >= 0) & (visit >= 0),
    }


def _snapshots(P: dict) -> dict:
    """Parámetros vigentes en cada fecha del calendario, estimados con lo anterior.

    Devuelve `día -> (a, d, h, mu, partidos efectivos por equipo)`. Las fechas se
    recorren en orden y cada ajuste arranca del anterior, que ahorra la mayor
    parte de los barridos: entre dos jornadas consecutivas las fuerzas se mueven
    poco y el punto de partida está ya casi en el óptimo.
    """
    n = len(P["equipos"])
    dias = np.sort(np.unique(P["dia"]))
    jugado, dia = P["jugado"], P["dia"]
    estado: dict[float, tuple] = {}
    init = (np.ones(n), np.ones(n), _H0, _MU0)

    for D in dias:
        prev = jugado & (dia < D) & (D - dia <= WINDOW_DAYS)
        if not prev.any():
            continue
        w = 0.5 ** ((D - dia[prev]) / HALF_LIFE_DAYS)
        att = np.concatenate([P["local"][prev], P["visit"][prev]])
        dfn = np.concatenate([P["visit"][prev], P["local"][prev]])
        es_local = np.concatenate([np.ones(int(prev.sum())),
                                   np.zeros(int(prev.sum()))])
        goles = np.concatenate([P["gh"][prev], P["ga"][prev]])
        peso = np.concatenate([w, w])

        a, d, h, mu = _solve(att, dfn, es_local, goles, peso, n, init)
        # partidos EFECTIVOS de cada equipo: la suma de los pesos de sus partidos,
        # que es la muestra real de la que sale su fuerza
        n_ef = np.bincount(att, weights=peso, minlength=n)
        estado[float(D)] = (a, d, h, mu, n_ef)
        init = (a, d, h, mu)
    return estado


# --------------------------------------------------------------------------- #
# probabilidad del resultado
# --------------------------------------------------------------------------- #
def _outcome_probs(lam_h: np.ndarray, lam_a: np.ndarray) -> np.ndarray:
    """Probabilidad 1X2 agregando la rejilla de marcadores de dos Poisson.

    La rejilla se trunca en `_MAX_GOALS` goles por equipo y se RENORMALIZA sobre
    lo que queda. El truncamiento deja fuera una masa del orden de 10⁻⁷ con las
    medias de esta liga, pero renormalizar tiene un efecto que va más allá de esa
    cifra: garantiza que las tres columnas suman exactamente 1 y son por tanto
    probabilidades legítimas, condición que `qa_features` comprueba y de la que
    depende que la terna pueda evaluarse como pronóstico con log-loss.
    """
    k = np.arange(_MAX_GOALS + 1, dtype="float64")
    log_fact = np.concatenate([[0.0], np.cumsum(np.log(k[1:]))])
    with np.errstate(divide="ignore", invalid="ignore"):
        ph = np.exp(-lam_h[:, None] + k * np.log(lam_h[:, None]) - log_fact)
        pa = np.exp(-lam_a[:, None] + k * np.log(lam_a[:, None]) - log_fact)
    conj = ph[:, :, None] * pa[:, None, :]          # [partido, goles local, visit]
    gl, gv = np.meshgrid(k, k, indexing="ij")
    total = conj.sum(axis=(1, 2))
    out = np.stack([conj[:, gl > gv].sum(axis=1),
                    conj[:, gl == gv].sum(axis=1),
                    conj[:, gl < gv].sum(axis=1)], axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        return out / total[:, None]


# --------------------------------------------------------------------------- #
# ensamblado
# --------------------------------------------------------------------------- #
def build_strength_features(master: pd.DataFrame) -> pd.DataFrame:
    """Máster -> features de fuerza latente, una fila por partido.

    Las dos fuerzas se publican en escala LOGARÍTMICA y con el mismo signo
    positivo-es-mejor:

      * `strength_attack`  = log(a). Cero es el ataque medio de la liga; +0.3
        significa marcar un 35 % más de lo que le correspondería a un equipo
        medio contra ese mismo rival y en ese mismo campo.
      * `strength_defence` = −log(d). Cero es la defensa media; POSITIVO es mejor
        defensa —encaja menos de lo esperable—, que invierte el signo natural del
        parámetro para que las dos columnas se lean en la misma dirección y su
        diferencial no obligue a recordar cuál de las dos iba al revés.
    Los goles esperados de cada lado (λ) se calculan —son el insumo de la
    probabilidad 1X2— pero no se publican como columna: la terna
    `strength_p_home/draw/away` es exactamente el resumen de esas dos medias, y
    entregar además las medias sería decir dos veces lo mismo. El catálogo lo
    declara así en `FUERZA_PRONOSTICO` y `FUERZA_ATRIBUTOS`.
    """
    P = _prepare(master)
    estado = _snapshots(P)
    n_filas = len(master)
    nan = np.full(n_filas, np.nan)

    a_h, d_h, a_a, d_a = nan.copy(), nan.copy(), nan.copy(), nan.copy()
    lam_h, lam_a = nan.copy(), nan.copy()

    for D, (a, d, h, mu, n_ef) in estado.items():
        fila = P["dia"] == D
        hi, ai = P["local"][fila], P["visit"][fila]
        # un equipo sin historia suficiente no tiene fuerza estimable, y el
        # partido entero queda sin bloque: sus dos lados y la probabilidad que
        # sale de ellos dependen de los dos equipos a la vez
        ok = (hi >= 0) & (ai >= 0) & (n_ef[hi] >= MIN_EFFECTIVE_MATCHES) \
            & (n_ef[ai] >= MIN_EFFECTIVE_MATCHES)
        idx = np.where(fila)[0][ok]
        hi, ai = hi[ok], ai[ok]
        a_h[idx], d_h[idx] = a[hi], d[hi]
        a_a[idx], d_a[idx] = a[ai], d[ai]
        lam_h[idx] = mu * a[hi] * d[ai] * h
        lam_a[idx] = mu * a[ai] * d[hi]

    probs = _outcome_probs(lam_h, lam_a)

    out = pd.DataFrame({"match_key": master["match_key"].values})
    out["home_strength_attack"] = np.log(a_h)
    out["home_strength_defence"] = -np.log(d_h)
    out["away_strength_attack"] = np.log(a_a)
    out["away_strength_defence"] = -np.log(d_a)
    for c in _NUMERIC:
        if catalogo.emite(c, "diff"):
            out[f"{c}_diff"] = out[f"home_{c}"] - out[f"away_{c}"]
    for i, c in enumerate(_MATCH_LEVEL):
        out[c] = probs[:, i]

    cols = ["match_key"]
    for c in _NUMERIC:
        cols += catalogo.nombres(c)
    cols += _MATCH_LEVEL

    out = out.reset_index(drop=True)
    assert_unique(out, ["match_key"], "strength")
    return out[cols]


def strength_history(master: pd.DataFrame) -> pd.DataFrame:
    """Trayectoria de las fuerzas: una fila por (fecha, equipo) con su estimación.

    No entra en el set de features —no está indexada por partido— y existe para
    que el análisis pueda mirar el interior del bloque: cómo evoluciona el ataque
    de un club a lo largo de las temporadas, cuánto tarda un ascendido en salir
    del encogimiento y si la ventaja de campo de la liga se mueve con los años,
    que es la lectura descriptiva que el notebook 03 documenta.
    """
    P = _prepare(master)
    estado = _snapshots(P)
    origen = pd.to_datetime(master["date"]).dt.normalize().min()
    filas = []
    for D, (a, d, h, mu, n_ef) in estado.items():
        vivo = n_ef >= MIN_EFFECTIVE_MATCHES
        for i in np.where(vivo)[0]:
            filas.append((origen + pd.Timedelta(days=float(D)),
                          P["equipos"][i], float(np.log(a[i])),
                          float(-np.log(d[i])), float(n_ef[i]), h, mu))
    return pd.DataFrame(filas, columns=["date", "team_key", "strength_attack",
                                        "strength_defence", "effective_matches",
                                        "home_advantage", "league_mean_goals"])


def strength_feature_names() -> dict[str, list[str]]:
    """Contrato del módulo: atributos base y pronóstico a nivel de partido.

    `build_strength_features` selecciona por esta lista al devolver, expandiendo
    cada atributo a los lados que declara `catalogo.nombres`, de modo que la
    declaración no es una descripción aproximada de lo que ocurre dentro sino la
    definición de la salida.
    """
    return {"numeric": list(_NUMERIC), "categorical": [],
            "match_level": list(_MATCH_LEVEL)}
