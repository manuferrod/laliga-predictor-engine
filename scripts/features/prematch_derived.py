"""Features DERIVADAS PRE-PARTIDO (se conocen antes del pitido, no hay fuga).

A diferencia de form/h2h/standings, aquí no hay ventanas temporales: cada feature
se calcula a partir de escalares que ya existen en el máster ANTES del partido
(Elo previo, cuotas de apertura, cuotas de cierre). Por eso no necesitan shift(1):
no describen el resultado, sino el estado del mercado y de la fuerza relativa al
entrar al encuentro. Son match-level (una sola columna por partido, no home/away).

Régimen de información
----------------------
El módulo produce columnas de los DOS regímenes que define `columns.py`, y el
nombre es el que decide a cuál pertenece cada una:

  * Estructura del precio en la APERTURA (dispersión entre casas, margen de la
    casa, divergencia de la casa afilada). Se conoce con días de antelación, así
    que alimenta el régimen semanal —el modelo que se publica—.
  * Movimiento APERTURA -> CIERRE (`close_move_*`). Solo existe minutos antes del
    saque. Lleva el token `close` en el nombre, de modo que
    `columns.weekly_unavailable` lo excluye del régimen semanal por construcción y
    queda reservado al retrospectivo, donde sirve para cuantificar cuánta
    información añade el mercado en las últimas horas.

Bloques
-------
  * ESTRUCTURA DEL PRECIO (apertura; régimen semanal)
      - mkt_overround_open : suma de las inversas de las tres cuotas medias ANTES
          de normalizar, es decir, el margen que se reserva la casa. La
          probabilidad implícita `mkt_p_*` lo divide y por tanto lo pierde: es
          información que la normalización destruye. Un margen alto señala un
          partido sobre el que el mercado se cubre, y entra directamente en el
          retorno de cualquier apuesta.
      - mkt_spread_mean : media de las distancias relativas entre la MEJOR cuota
          ofertada por alguna casa (`odds_max_*`) y el consenso (`odds_avg_*`).
          Mide el desacuerdo entre casas, y es adimensional para que resulte
          comparable entre favoritos y tapados, cuyas cuotas viven en escalas muy
          distintas. Un partido con dispersión alta es un partido sobre el que el
          propio mercado no tiene una opinión firme.
      - sharp_div_home/draw/away : Pinnacle —la casa de referencia por precio
          informado— menos consenso, resultado a resultado. Mide dónde el dinero
          informado se separa del minorista, y a diferencia de la dispersión
          conserva el signo, que es lo que dice en qué dirección discrepa.

    Este bloque calcula además, como pasos intermedios, la dispersión por signo,
    el margen y la probabilidad de Pinnacle y la distancia de variación total
    entre ambas distribuciones. Ninguno sale a la matriz: son la aritmética de la
    que salen las columnas de arriba, no lecturas distintas del precio.

  * SERVICIO (apertura; no es feature)
      - mkt_p_b365_home/draw/away : probabilidad des-margen de una sola casa
          minorista de gran difusión, Bet365. No es una variable del modelo —el
          consenso ya recoge su precio, y duplicarlo solo añadiría colinealidad—
          sino el patrón de comparación contra el que se mide el pronóstico. Se
          calcula aquí, con el mismo procedimiento que las otras dos lecturas,
          precisamente para que la diferencia entre ellas mida el precio y no el
          método con que se convirtió en probabilidad.

  * MOVIMIENTO DE LÍNEA (cierre; régimen retrospectivo)
      - close_move_fav : desplazamiento de la probabilidad implícita (des-margen)
          entre la cuota de APERTURA y la de CIERRE, medido siempre en términos
          del que salía favorito para que el signo sea estable: positivo = el
          mercado reforzó al favorito. El desplazamiento por signo se calcula como
          paso intermedio y no se publica, porque los tres suman cero y el resumen
          orientado al favorito es la lectura que responde a la pregunta.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import catalogo

# Ventaja de campo en puntos Elo (equivalente a ~+0.09 de prob. a 0 de diferencia).
# Es pública porque el análisis la usa para leer el Elo en escala de probabilidad;
# ninguna columna de la matriz depende de ella (ver `elo_expected_score`).
HOME_ADV_ELO = 65.0

# --- guardas de validez de las cuotas decimales -------------------------------
# Una cuota decimal es un precio: siempre > 1 (a 1.00 el apostante no gana nada) y
# nunca 0 o negativa. Sin esta guarda, un 0 en la fuente produce una probabilidad
# infinita y un valor <=1 produce una probabilidad >1; ambos se propagarían a
# `close_move_*` como si fueran señal de mercado. La suma de inversas (`overround`)
# tiene ademas un rango plausible: por debajo de 1 la casa perderia dinero seguro y
# por encima de ~1.6 el margen no corresponde a un mercado 1X2 real, asi que fuera
# de ese rango el trio de cuotas se considera corrupto y se anula por completo.
_MIN_DEC_ODD = 1.01
_MIN_OVERROUND = 0.95
_MAX_OVERROUND = 1.60

# Dispersion entre casas: `odds_max_*` es la mejor cuota ofertada, de modo que por
# definicion es >= la media. Un valor por debajo del consenso no es una dispersion
# pequena sino una incoherencia de la fuente (columnas cruzadas, coalesce de eras
# distintas), y se anula en lugar de entrar como un desacuerdo negativo. Se tolera
# un margen de redondeo porque el fichero publica dos decimales.
_SPREAD_TOL = 1e-6
# Techo de plausibilidad: una casa que ofrezca mas del doble del consenso sobre el
# mismo resultado no esta discrepando, esta publicando un precio erroneo.
_MAX_SPREAD = 1.0


def _col(master: pd.DataFrame, name: str) -> pd.Series:
    """Columna como float64 si existe, si no Serie de NaN (partido futuro/fuente ausente)."""
    if name in master.columns:
        return master[name].astype("float64")
    return pd.Series(np.nan, index=master.index, dtype="float64")


def elo_expected_score(elo_diff, home_advantage: float = HOME_ADV_ELO):
    """Puntuación esperada por el Elo (logística estándar de 400 puntos).

    `elo_diff` es la ventaja Elo del equipo cuya expectativa se calcula sobre su
    rival, y `home_advantage` los puntos que se le suman por jugar en su campo
    (negativos si juega fuera, cero en terreno neutral). El resultado se
    interpreta como la puntuación media esperada en la escala del ajedrez, donde
    la victoria vale 1, el empate 0.5 y la derrota 0; en fútbol se lee como
    "cuota de puntos esperada".

    Es la ÚNICA implementación de la logística Elo del proyecto y NO PRODUCE
    NINGUNA COLUMNA. Ni la probabilidad que implica el Elo (`elo_exp_home`) ni el
    residuo entre lo esperado y lo obtenido (`elo_surprise`) entran ya en la
    matriz: la primera es una transformación monótona de la diferencia de fuerza,
    que el par `home_elo_z`/`away_elo_z` ya entrega, y darla sería decir dos veces
    lo mismo; el segundo medía el rendimiento ponderado por la dificultad del
    rival, que es exactamente lo que la forma ya dice al viajar junto a
    `form_opp_elo_z_*`. La función se conserva porque el análisis la necesita para
    leer el Elo en escala de probabilidad y comprobar su calibración frente al
    resultado observado, que es lectura de informe y no variable de entrada.

    Trabaja sobre el NIVEL de rating y no sobre la fuerza tipificada que consume
    el modelo, porque los 400 puntos de la logística son una constante de esa
    escala y no significan nada en desviaciones. Para el análisis eso no estorba
    —se lee dentro de una temporada, donde la escala no cambia— pero es la razón
    de que esta función no pueda alimentar una columna del histórico completo.
    """
    return 1.0 / (1.0 + np.power(10.0, -(elo_diff + home_advantage) / 400.0))


def _sane_odd(x: pd.Series) -> pd.Series:
    """Cuota decimal saneada: lo que no es un precio válido pasa a NaN."""
    v = pd.to_numeric(x, errors="coerce").astype("float64")
    return v.where(v >= _MIN_DEC_ODD)


def _overround(h: pd.Series, d: pd.Series, a: pd.Series) -> pd.Series:
    """Suma de inversas de un trío de cuotas: el margen bruto de la casa.

    Devuelve NaN fuera del rango plausible, con el mismo criterio que
    `_implied_probs`, para que el margen y la probabilidad que se derivan del
    mismo trío estén definidos exactamente sobre las mismas filas.
    """
    h, d, a = _sane_odd(h), _sane_odd(d), _sane_odd(a)
    s = 1.0 / h + 1.0 / d + 1.0 / a
    return s.where(s.notna() & (s >= _MIN_OVERROUND) & (s <= _MAX_OVERROUND))


def _implied_probs(h: pd.Series, d: pd.Series, a: pd.Series) -> pd.DataFrame:
    """Probabilidades des-margen (normalizadas a 1) a partir de 3 cuotas decimales.

    Un trío de cuotas se convierte en probabilidad solo si las tres son precios
    válidos y su overround cae en el rango plausible; si no, las tres salidas son
    NaN. Anular el trío completo (y no solo la cuota mala) es lo correcto porque la
    normalización acopla los tres resultados: con una cuota corrupta, las otras dos
    probabilidades tampoco son interpretables.
    """
    h, d, a = _sane_odd(h), _sane_odd(d), _sane_odd(a)
    ih, idr, ia = 1.0 / h, 1.0 / d, 1.0 / a
    s = ih + idr + ia
    valido = s.notna() & (s >= _MIN_OVERROUND) & (s <= _MAX_OVERROUND)
    out = pd.DataFrame({"home": ih / s, "draw": idr / s, "away": ia / s})
    return out.where(valido, other=np.nan)


def _opening_consensus(master: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Trío de apertura: media de casas con retorno a Bet365 si la media no está."""
    def pick(avg: str, b365: str) -> pd.Series:
        a = _col(master, avg)
        return a.where(a.notna(), _col(master, b365))
    return (pick("odds_avg_h", "odds_b365_h"),
            pick("odds_avg_d", "odds_b365_d"),
            pick("odds_avg_a", "odds_b365_a"))


def _closing_consensus(master: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Trío de cierre: media de casas con retorno a Bet365 de cierre."""
    def pick(avg: str, b365: str) -> pd.Series:
        a = _col(master, avg)
        return a.where(a.notna(), _col(master, b365))
    return (pick("odds_close_avg_h", "odds_b365_close_h"),
            pick("odds_close_avg_d", "odds_b365_close_d"),
            pick("odds_close_avg_a", "odds_b365_close_a"))


def _price_structure(master: pd.DataFrame) -> pd.DataFrame:
    """Estructura del precio de APERTURA: margen, dispersión y casa afilada.

    Todo lo que sale de aquí está disponible en el momento de emitir la predicción
    —`load_upcoming_processed` publica el mapa de apertura completo, incluidas las
    columnas `max` y las de Pinnacle— y por tanto pertenece al régimen semanal.
    """
    oh, od, oa = _opening_consensus(master)
    po = _implied_probs(oh, od, oa)

    out = pd.DataFrame(index=master.index)
    out["mkt_overround_open"] = _overround(oh, od, oa)

    # dispersion entre casas: mejor precio ofertado frente al consenso, en
    # fraccion del consenso para que sea comparable entre favoritos y tapados.
    for lado, avg_c, max_c in (("home", "odds_avg_h", "odds_max_h"),
                               ("draw", "odds_avg_d", "odds_max_d"),
                               ("away", "odds_avg_a", "odds_max_a")):
        avg = _sane_odd(_col(master, avg_c))
        mx = _sane_odd(_col(master, max_c))
        spread = (mx - avg) / avg
        out[f"mkt_spread_{lado}"] = spread.where(
            spread.notna() & (spread >= -_SPREAD_TOL) & (spread <= _MAX_SPREAD)).clip(lower=0.0)
    out["mkt_spread_mean"] = out[["mkt_spread_home", "mkt_spread_draw",
                                  "mkt_spread_away"]].mean(axis=1, skipna=False)

    # casa afilada (Pinnacle) frente al consenso
    ph, pd_, pa = (_col(master, "odds_ps_h"), _col(master, "odds_ps_d"),
                   _col(master, "odds_ps_a"))
    pps = _implied_probs(ph, pd_, pa)
    out["mkt_overround_ps"] = _overround(ph, pd_, pa)
    for lado in ("home", "draw", "away"):
        out[f"mkt_p_ps_{lado}"] = pps[lado]
        out[f"sharp_div_{lado}"] = pps[lado] - po[lado]
    out["sharp_div_tv"] = out[["sharp_div_home", "sharp_div_draw",
                               "sharp_div_away"]].abs().mean(axis=1, skipna=False)

    # una casa minorista concreta, leída con el mismo procedimiento. No entra en el
    # modelo (el consenso ya contiene este precio); existe para que la evaluación
    # pueda contrastar el pronóstico con las tres lecturas del mercado —consenso,
    # dinero informado y minorista— sobre exactamente las mismas filas.
    bh, bd, ba = (_col(master, "odds_b365_h"), _col(master, "odds_b365_d"),
                  _col(master, "odds_b365_a"))
    pb = _implied_probs(bh, bd, ba)
    for lado in ("home", "draw", "away"):
        out[f"mkt_p_b365_{lado}"] = pb[lado]
    return out


def _line_movement(master: pd.DataFrame) -> pd.DataFrame:
    """Deriva de prob. implícita apertura->cierre por resultado, + resumen del favorito.

    Pertenece al régimen retrospectivo: el nombre `close_move_*` lleva el token que
    `columns.weekly_unavailable` reconoce, de modo que estas columnas no pueden
    colarse en el modelo que se publica. La distinción importa porque en inferencia
    llegarían SIEMPRE vacías —el artefacto de próximos partidos no trae cierre— y
    `preprocess.apply` las rellenaría con la mediana de entrenamiento sin dar
    ninguna señal de error.
    """
    open_h, open_d, open_a = _opening_consensus(master)
    clos_h, clos_d, clos_a = _closing_consensus(master)

    po = _implied_probs(open_h, open_d, open_a)
    pc = _implied_probs(clos_h, clos_d, clos_a)

    move = pd.DataFrame({
        "close_move_home": pc["home"] - po["home"],
        "close_move_draw": pc["draw"] - po["draw"],
        "close_move_away": pc["away"] - po["away"],
    })
    # movimiento neto hacia el favorito de apertura (signo estable): + = el mercado
    # reforzó al que ya salía favorito; - = derivó hacia el underdog o el empate.
    fav_home = po["home"] >= po["away"]
    move["close_move_fav"] = np.where(fav_home, move["close_move_home"],
                                      move["close_move_away"])
    move.loc[po["home"].isna() | po["away"].isna(), "close_move_fav"] = np.nan
    return move


# Estructura del precio de APERTURA. Lo declara el catálogo, y de las columnas
# intermedias que este módulo calcula para llegar a él —los tres diferenciales por
# signo de los que sale la dispersión media, el margen y la probabilidad de
# Pinnacle, la divergencia media— no sobrevive ninguna: son pasos del cálculo, no
# lecturas distintas del precio.
#
# El bloque NO es homogéneo de régimen, y conviene no leerlo como si lo fuera. El
# margen y la dispersión salen del consenso y de la minorista, que el feed semanal
# publica, y entran al modelo que se emite. La divergencia con la casa afilada sale
# de Pinnacle, que es apertura y es pre-partido pero que ese feed no trae, de modo
# que `columns.SHARP_ODDS` la deja fuera del régimen semanal. Ser de apertura no es
# lo mismo que ser servible: tres de estas cinco columnas solo existen en el
# retrospectivo, y el reparto lo decide `columns.py`, no este módulo.
PRICE_STRUCTURE = list(catalogo.MERCADO_ESTRUCTURA)

# La probabilidad implícita de UNA casa minorista concreta. No es una feature —el
# consenso ya contiene este precio— sino una columna de SERVICIO: es el patrón de
# comparación contra el que se mide el modelo, y viaja en la matriz para que la
# evaluación lo tenga sobre exactamente las mismas filas que el pronóstico.
SERVICIO_B365 = list(catalogo.SERVICIO_B365)

# Bloque retrospectivo: el token `close` lo mantiene fuera del régimen semanal.
# Del movimiento por signo se publica solo el resumen orientado al favorito, que
# es la lectura con signo estable de hacia dónde se ha ido el dinero.
LINE_MOVEMENT = list(catalogo.MERCADO_MOVIMIENTO)

_NUMERIC = PRICE_STRUCTURE + SERVICIO_B365 + LINE_MOVEMENT


def build_prematch_derived_features(master: pd.DataFrame) -> pd.DataFrame:
    """Máster -> features derivadas pre-partido, keyed por match_key.

    La salida se ordena SIEMPRE según `_NUMERIC`, no según el orden en que los
    bloques internos van creando sus columnas. Así la lista declarada es el contrato
    del módulo y no una descripción aproximada de lo que ocurre dentro: cualquier
    columna que se añada a un bloque sin declararla desaparece de la salida, y
    cualquiera declarada que un bloque deje de producir sale como NaN en vez de
    provocar un desajuste de esquema aguas abajo. `build_features` ensambla por
    posición de columna, de modo que un orden inestable sería un fallo silencioso.
    """
    out = pd.DataFrame({"match_key": master["match_key"]})
    for bloque in (_price_structure(master), _line_movement(master)):
        for c in bloque.columns:
            out[c] = bloque[c].to_numpy()
    for c in _NUMERIC:                      # declarada pero no producida -> NaN
        if c not in out.columns:
            out[c] = np.nan
    return out[["match_key"] + _NUMERIC].reset_index(drop=True)


def prematch_derived_feature_names() -> list[str]:
    return list(_NUMERIC)
