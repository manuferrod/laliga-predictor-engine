"""Validacion offline del bloque de MERCADO y estados (market_form.py). Sin red.

Este bloque no mide como juega un equipo sino la distancia entre lo que hace y
lo que el mercado esperaba que hiciera. Esa naturaleza relativa cambia lo que hay
que comprobar respecto a `form.py`.

Lo primero es la definicion: `relative_perf` es un COCIENTE de dos medias
exponenciales —victorias reales sobre victorias esperadas por la cuota— y un
cociente admite fallos que una media no admite. Un denominador nulo produciria un
infinito que despues sobrevive a la imputacion disfrazado de valor extremo, y
desplazar solo uno de los dos terminos daria una ratio que compara el presente
con el pasado y por tanto filtra el resultado del propio partido. Ambos se
comprueban aqui, junto con la lectura que la memoria hace de la escala: un equipo
que gana siempre partiendo de una probabilidad de mercado del cincuenta por
ciento debe salir exactamente en dos, y no en cualquier numero mayor que uno.

Lo segundo es que la memoria cruza el corte de temporada A PROPOSITO, al reves
que la forma: el rendimiento relativo al mercado y el arrastre psicologico no se
reinician en agosto. Es una decision de diseno y se comprueba como tal.

Se comprueba:

  1. ANTIFUGA     el primer partido no tiene rendimiento relativo, y alterar el
                  marcador de t deja intactas las columnas de t.
  2. ARITMETICA   la ratio vale el cociente de las dos medias exponenciales, con
                  el minimo de observaciones declarado.
  3. ESCALA       la ratio se lee como se afirma en la memoria (uno = el equipo
                  rinde como dicta el mercado; dos = el doble) y nunca es
                  infinita.
  4. ESTADOS      el indicador describe el partido ANTERIOR y solo ese.
  5. MEMORIA      el bloque cruza el corte de temporada.
  6. NAN-SAFE     un partido futuro no cuenta como derrota ni apaga los estados.
  7. CONTRATO     lo producido coincide con lo que el catalogo declara, y todo el
                  bloque es de regimen semanal.

Una advertencia sobre DONDE se comprueba cada cosa. De `relative_perf` el
catalogo emite solo la diferencia entre los dos equipos, de modo que
`home_relative_perf` no llega a la matriz y de la resta no se recupera el
rendimiento de ninguno de los dos. Por eso toda la aritmetica de la ratio —los
cuatro grupos primeros— se comprueba sobre `market_form_history`, que es la
trayectoria por equipo y el sitio donde la ratio se calcula. La proyeccion a
fila de partido se comprueba aparte, contra el catalogo, en [7] CONTRATO.
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
import market_form as M  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []
_TOL = 1e-9


def check(cond: bool, msg: str, detalle: object = None) -> None:
    """Registra una comprobacion; `detalle` solo se imprime cuando falla.

    Un fallo de conjunto —«lo producido no es lo declarado»— no se diagnostica
    con un booleano: hace falta ver QUE sobra y que falta. Se imprime solo al
    fallar para no ensuciar la salida de la bateria verde.
    """
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")
    if not cond and detalle is not None:
        print(f"         -> {detalle}")


# --------------------------------------------------------------------------- #
# generador de masters sinteticos
# --------------------------------------------------------------------------- #
def _master(partidos: list[dict]) -> pd.DataFrame:
    """Master minimo: claves, marcador, cuota media de casas y prob. implicita.

    El local es siempre el equipo bajo observacion, de modo que su secuencia
    coincide con el orden de la lista. Cada partido admite `gh`/`ga` (marcador,
    NaN si esta por jugar), `odds` (cuota de victoria del local) y `p` (su
    probabilidad implicita).
    """
    filas = []
    for i, p in enumerate(partidos):
        fecha = pd.Timestamp(p.get("date", "2020-08-15")) + pd.Timedelta(
            days=7 * i if "date" not in p else 0)
        gh, ga = p.get("gh", np.nan), p.get("ga", np.nan)
        filas.append({
            "season_year": p.get("season_year", 2020),
            "date": fecha,
            "round": i + 1,
            "home_key": p.get("home", "aaa"),
            "away_key": p.get("away", f"b{i}"),
            "fthg": float(gh) if not pd.isna(gh) else np.nan,
            "ftag": float(ga) if not pd.isna(ga) else np.nan,
            "odds_avg_h": p.get("odds", 2.0),
            "odds_avg_a": p.get("odds_away", 3.0),
            "mkt_p_home": p.get("p", 0.5),
            "mkt_p_away": p.get("p_away", 0.3),
        })
    df = pd.DataFrame(filas)
    df["match_key"] = (df["date"].dt.strftime("%Y-%m-%d") + "|"
                       + df["home_key"] + "|" + df["away_key"])
    return df


def _serie(master: pd.DataFrame, col: str) -> list[float]:
    """Valor de `col` para el LOCAL en cada partido, en el orden declarado.

    Se lee de la trayectoria por equipo y no de la fila de partido porque de
    `relative_perf` la fila de partido solo contiene la diferencia, de la que no
    se recupera el valor del local. En estos masters sinteticos el local es
    siempre el mismo equipo, asi que la serie que devuelve es su historia.
    """
    h = M.market_form_history(master)
    h = h[h["is_home"] == 1].set_index("match_key")
    return [h.loc[k, col] for k in master["match_key"]]


def _ewm_adjust_false(vals: list[float], halflife: float) -> list[float]:
    """Media exponencial recursiva de pandas con `adjust=False`, saltando huecos."""
    alpha = 1.0 - 0.5 ** (1.0 / halflife)
    y, out = None, []
    for x in vals:
        if not pd.isna(x):
            y = x if y is None else (1 - alpha) * y + alpha * x
        out.append(y)
    return out


def _ratio_esperada(won: list[float], p: list[float]) -> list[float]:
    """`relative_perf` calculada a mano: cociente de las dos exponenciales."""
    ew, ep = _ewm_adjust_false(won, M._HALFLIFE), _ewm_adjust_false(p, M._HALFLIFE)
    out = [np.nan]                      # el primer partido no tiene pasado
    for i in range(1, len(won)):
        vistos = sum(1 for v in won[:i] if not pd.isna(v))
        if vistos < M._MINP_EWM or ew[i - 1] is None or not ep[i - 1]:
            out.append(np.nan)
        else:
            out.append(ew[i - 1] / ep[i - 1])
    return out


# --------------------------------------------------------------------------- #
# 1. ANTIFUGA
# --------------------------------------------------------------------------- #
def test_antifuga() -> None:
    print("\n[1] ANTIFUGA — la fila del partido t no ve el partido t")
    m = _master([{"gh": 2, "ga": 0}, {"gh": 1, "ga": 0}, {"gh": 0, "ga": 1},
                 {"gh": 3, "ga": 1}, {"gh": 0, "ga": 0}])
    w = M.build_market_form_features(m)

    rp = _serie(m, "relative_perf")
    check(pd.isna(rp[0]), "el primer partido no tiene rendimiento relativo")
    check(all(pd.isna(v) for v in rp[:M._MINP_EWM]),
          f"tampoco lo tiene hasta acumular {M._MINP_EWM} partidos previos")
    check(not pd.isna(rp[M._MINP_EWM]),
          "y aparece exactamente en el primero que cumple el minimo")

    m2 = m.copy()
    m2.loc[m2.index[4], ["fthg", "ftag"]] = [0.0, 5.0]
    w2 = M.build_market_form_features(m2)
    k = m["match_key"].iloc[4]
    cols = [c for c in w.columns if c != "match_key"]
    a, b = w.set_index("match_key").loc[k], w2.set_index("match_key").loc[k]
    igual = all((pd.isna(a[c]) and pd.isna(b[c]))
                or abs(float(a[c]) - float(b[c])) < _TOL for c in cols)
    check(igual, "alterar el marcador de t deja intactas TODAS las columnas de t")


# --------------------------------------------------------------------------- #
# 2. ARITMETICA
# --------------------------------------------------------------------------- #
def test_aritmetica() -> None:
    print("\n[2] ARITMETICA — la ratio es el cociente de las dos exponenciales")
    won = [1.0, 1.0, 0.0, 1.0, 0.0, 1.0]
    prob = [0.50, 0.40, 0.30, 0.60, 0.25, 0.55]
    # el marcador y la probabilidad se declaran por separado para que la victoria
    # y la expectativa del mercado puedan discrepar, que es justo lo que la ratio
    # mide: un pleno de victorias no significa lo mismo si el mercado lo esperaba.
    m = _master([{"gh": 2, "ga": 0, "p": q} if v == 1.0
                 else {"gh": 0, "ga": 1, "p": q}
                 for v, q in zip(won, prob)])

    obs = _serie(m, "relative_perf")
    esp = _ratio_esperada(won, prob)
    ok = all((pd.isna(o) and pd.isna(e)) or abs(float(o) - float(e)) < 1e-9
             for o, e in zip(obs, esp))
    check(ok, f"relative_perf = EWM(victorias) / EWM(prob. de mercado), semivida "
              f"{M._HALFLIFE:g}")
    check(all(pd.isna(v) for v in obs[:M._MINP_EWM]),
          f"el minimo de {M._MINP_EWM} observaciones se respeta en los dos terminos")


# --------------------------------------------------------------------------- #
# 3. ESCALA
# --------------------------------------------------------------------------- #
def test_escala() -> None:
    print("\n[3] ESCALA — la ratio se lee como afirma la memoria")
    # un equipo que gana siempre y al que el mercado daba por ganador seguro
    cierto = _master([{"gh": 1, "ga": 0, "p": 1.0}] * 5)
    rp = _serie(cierto, "relative_perf")
    check(abs(rp[4] - 1.0) < 1e-9,
          "rendir exactamente lo que el mercado dicta da uno")

    # el mismo pleno de victorias, pero partiendo de un cincuenta por ciento
    doble = _master([{"gh": 1, "ga": 0, "p": 0.5}] * 5)
    rp = _serie(doble, "relative_perf")
    check(abs(rp[4] - 2.0) < 1e-9,
          "ganar siempre partiendo de una probabilidad de 0.5 da exactamente dos")

    peor = _master([{"gh": 0, "ga": 1, "p": 0.5}] * 5)
    rp = _serie(peor, "relative_perf")
    check(abs(rp[4]) < 1e-12, "perder siempre da cero, no un negativo")

    # denominador nulo: sin la guarda seria un infinito que la imputacion
    # posterior no sabria distinguir de un valor extremo legitimo.
    cero = _master([{"gh": 1, "ga": 0, "p": 0.0}] * 5)
    rp = _serie(cero, "relative_perf")
    check(all(pd.isna(v) or np.isfinite(v) for v in rp),
          "una expectativa de mercado nula produce hueco, no infinito")


# --------------------------------------------------------------------------- #
# 4. ESTADOS
# --------------------------------------------------------------------------- #
def test_estados() -> None:
    print("\n[4] ESTADOS — el indicador describe el partido ANTERIOR y solo ese")
    # De los dos arrastres psicologicos que el bloque llego a tener, el catalogo
    # conserva solo la victoria inesperada. La derrota del gran favorito se cayo
    # porque su informacion sobre el resultado siguiente era del orden del ruido
    # de muestreo, y aqui se comprueba tambien esa ausencia: un indicador retirado
    # del catalogo pero seguido calculando en el modulo seria trabajo muerto que
    # nadie volveria a mirar.
    m = _master([{"gh": 2, "ga": 0, "odds": 6.0},     # 0: victoria inesperada
                 {"gh": 1, "ga": 1, "odds": 2.0},     # 1: empate corriente
                 {"gh": 0, "ga": 2, "odds": 1.20},    # 2: tropiezo de gran favorito
                 {"gh": 1, "ga": 0, "odds": 2.0},     # 3
                 {"gh": 1, "ga": 0, "odds": 2.0}])    # 4
    pbw = _serie(m, "post_big_win")

    check(pd.isna(pbw[0]),
          "el primer partido no tiene anterior: hueco, no cero")
    check(pbw[1] == 1.0, "tras ganar con cuota alta el indicador se enciende")
    check(pbw[2] == 0.0, "y se apaga en el partido siguiente (describe solo el previo)")
    check(pbw[3] == 0.0 and pbw[4] == 0.0,
          "y no lo enciende ningun otro desenlace: mide una victoria inesperada, "
          "no una jornada movida")
    check("post_big_fav_loss" not in M.market_form_history(m).columns,
          "el arrastre simetrico —la derrota del gran favorito— ya no se calcula: "
          "el catalogo no lo declara y el modulo no lo produce a escondidas")

    # los umbrales son los declarados en el modulo, no valores redondeados
    justo = _master([{"gh": 2, "ga": 0, "odds": M._BIG_WIN_ODDS},
                     {"gh": 1, "ga": 0, "odds": 2.0}])
    check(_serie(justo, "post_big_win")[1] == 0.0,
          f"la cuota de corte ({M._BIG_WIN_ODDS:g}) no cuenta: el umbral es estricto")


# --------------------------------------------------------------------------- #
# 5. MEMORIA ENTRE TEMPORADAS
# --------------------------------------------------------------------------- #
def test_memoria() -> None:
    print("\n[5] MEMORIA — el bloque cruza el corte de temporada a proposito")
    filas = [{"gh": 2, "ga": 0, "p": 0.5} for _ in range(4)]
    filas.append({"gh": 1, "ga": 0, "p": 0.5, "season_year": 2021,
                  "date": "2021-08-15"})
    m = _master(filas)

    rp = _serie(m, "relative_perf")
    check(not pd.isna(rp[4]),
          "el primer partido de la temporada siguiente conserva el rendimiento relativo")
    check(abs(rp[4] - 2.0) < 1e-9,
          "y conserva su valor: la memoria del mercado no se reinicia en agosto")

    # el contraste con form.py es deliberado y conviene dejarlo escrito: alli la
    # primera jornada es NaN por diseno, aqui no lo es.
    check(pd.isna(rp[0]) and not pd.isna(rp[4]),
          "el hueco de la primera jornada es el del arranque, no el del cambio de anio")


# --------------------------------------------------------------------------- #
# 6. NAN-SAFE
# --------------------------------------------------------------------------- #
def test_nan_safe() -> None:
    print("\n[6] NAN-SAFE — un partido futuro no es una derrota ni un estado apagado")
    filas = [{"gh": 1, "ga": 0, "p": 0.5} for _ in range(4)]
    filas.append({"p": 0.5})                       # jornada por jugar
    filas.append({"gh": 1, "ga": 0, "p": 0.5})
    m = _master(filas)

    rp = _serie(m, "relative_perf")
    check(abs(rp[4] - 2.0) < 1e-9,
          "la fila por jugar recibe el rendimiento relativo acumulado hasta ella")
    check(abs(rp[5] - 2.0) < 1e-9,
          "y no lo degrada: el hueco no entra en la media como una derrota")

    pbw = _serie(m, "post_big_win")
    check(pd.isna(pbw[5]),
          "tras una jornada sin marcador el estado es hueco, no cero: no se sabe")


# --------------------------------------------------------------------------- #
# 7. CONTRATO Y REGIMEN
# --------------------------------------------------------------------------- #
def test_contrato() -> None:
    print("\n[7] CONTRATO — lo declarado es exactamente lo producido")
    m = _master([{"gh": 1, "ga": 0}, {"gh": 0, "ga": 0}, {"gh": 0, "ga": 2},
                 {"gh": 2, "ga": 1}])
    w = M.build_market_form_features(m)
    base = M.market_form_feature_names()

    # De un atributo de equipo caben tres columnas en la fila de partido —los dos
    # lados y su resta— y las tres son linealmente dependientes, asi que el
    # catalogo elige cuales salen. Aqui elige por naturaleza de la senal: la ratio
    # es continua y su diferencia resume el enfrentamiento, mientras que el
    # arrastre es una bandera y restar banderas confunde el partido en que ninguno
    # viene de una sorpresa con aquel en que vienen los dos. Lo que se comprueba
    # es que lo producido y lo declarado sean el MISMO conjunto en las dos
    # direcciones: que no falte nada que el catalogo emita —una feature calculada
    # y tirada a la basura— y que no sobre nada que no emita, porque una columna
    # que llega a la matriz sin estar declarada se salta la curacion entera.
    esperadas = ["match_key"] + [c for b in base for c in catalogo.nombres(b)]
    check(set(w.columns) == set(esperadas),
          "lo producido coincide exactamente con lo que el catalogo declara",
          sorted(set(w.columns) ^ set(esperadas)))
    check("relative_perf_diff" in w.columns
          and "home_relative_perf" not in w.columns,
          "la senal continua viaja como diferencia y no por lados")
    check({"home_post_big_win", "away_post_big_win"} <= set(w.columns)
          and "post_big_win_diff" not in w.columns,
          "y el arrastre al reves, como par sin resta: restar banderas confunde "
          "el partido en que ninguno viene de una sorpresa con aquel en que "
          "vienen los dos")

    todas = [c for c in w.columns if c != "match_key"]
    check(not C.weekly_unavailable(todas),
          "ninguna columna queda fuera del regimen semanal: la cuota que usa el "
          "bloque es la de apertura, disponible al predecir")
    check(w["match_key"].is_unique and len(w) == len(m),
          "la salida es una fila por partido, con match_key unica")


def _run() -> None:
    print("=" * 72)
    print("TEST — bloque de mercado y estados (rendimiento relativo y arrastres)")
    print("=" * 72)
    test_antifuga()
    test_aritmetica()
    test_escala()
    test_estados()
    test_memoria()
    test_nan_safe()
    test_contrato()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_market_form() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
