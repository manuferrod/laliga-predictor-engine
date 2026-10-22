"""Validacion offline del bloque HEAD-TO-HEAD (h2h.py). Sin red y sin datos reales.

El historial de enfrentamientos directos comparte con la forma el riesgo de fuga
—son resultados pasados calculados sobre la misma tabla que contiene el presente—
pero anade dos dificultades propias que este banco comprueba por separado.

La primera es que el long del que se parte es DIRIGIDO: cada partido genera la
fila del par (equipo, rival) y la del par (rival, equipo), y las dos recorren
exactamente la misma secuencia de enfrentamientos vista desde lados opuestos. De
ahi la comprobacion que da sentido al bloque: el empate historico es la misma
cantidad en las dos filas, la diferencia de goles de una es la de la otra
cambiada de signo, y las dos tasas de victoria mas el empate suman uno. Si esas
identidades se rompieran, el agrupamiento estaria mezclando pares.

La segunda es que la memoria cruza el corte de temporada A PROPOSITO, al reves
que en `form.py`: la historia de un clasico no se reinicia cada agosto. Esa
decision se comprueba aqui de forma explicita porque es una eleccion de diseno
que la memoria debe poder defender, no un efecto colateral del codigo.

Se comprueba, sobre pares construidos con el resultado conocido:

  1. ANTIFUGA     el primer enfrentamiento de un par no tiene historial; alterar
                  el marcador de t deja intactas las features h2h de t.
  2. SIMPLEX      las tasas viven en el simplex: cada una en [0, 1] y las dos
                  publicadas sumando como mucho uno, tambien donde el relleno
                  neutral las sustituye.
  3. SIMETRIA     las dos filas dirigidas del mismo partido son coherentes entre
                  si, que es lo que autoriza a publicar el empate una sola vez.
  4. MEMORIA      el historial cruza temporadas, y no se contamina con partidos
                  contra OTROS rivales.
  5. ARITMETICA   la media exponencial vale exactamente lo que dice su definicion.
  6. NAN-SAFE     un partido futuro no entra en el historial como si fuera empate.
  7. CONTRATO     lo producido coincide con lo que el catalogo declara, y todo el
                  bloque pertenece al regimen semanal.

Una advertencia sobre DONDE se comprueba cada cosa. De las dos magnitudes que
miden al EQUIPO —la tasa de victoria y la diferencia de goles— el catalogo emite
solo la diferencia entre los dos, de modo que `home_h2h_win_rate_ewm` no llega a
la matriz y de la resta no se recupera el historial de ninguno. Por eso toda la
aritmetica se comprueba sobre `h2h_history`, que es el long dirigido y el sitio
donde esa aritmetica vive. La proyeccion a fila de partido —la resta, la columna
de enfrentamiento y el conjunto de nombres— se comprueba aparte, contra el
catalogo, en [3] SIMETRIA y [7] CONTRATO.
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
import h2h as H  # noqa: E402

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
def _master(partidos: list[tuple], fechas: list[str] | None = None) -> pd.DataFrame:
    """Master minimo con lo que h2h exige: claves, fecha, marcador y resultado.

    Cada partido se declara como (local, visitante, goles_local, goles_visitante).
    Un marcador NaN representa una jornada por jugar, que es como llega la fila
    de la ruta de inferencia.
    """
    filas = []
    for i, (h, a, gh, ga) in enumerate(partidos):
        fecha = (pd.Timestamp(fechas[i]) if fechas
                 else pd.Timestamp("2020-08-15") + pd.Timedelta(days=7 * i))
        if pd.isna(gh) or pd.isna(ga):
            ftr = pd.NA
        else:
            ftr = "H" if gh > ga else ("D" if gh == ga else "A")
        filas.append({"date": fecha, "home_key": h, "away_key": a,
                      "fthg": float(gh) if not pd.isna(gh) else np.nan,
                      "ftag": float(ga) if not pd.isna(ga) else np.nan,
                      "ftr": ftr})
    df = pd.DataFrame(filas)
    df["ftr"] = df["ftr"].astype("string")
    df["match_key"] = (df["date"].dt.strftime("%Y-%m-%d") + "|"
                       + df["home_key"] + "|" + df["away_key"])
    return df


def _hist(master: pd.DataFrame) -> pd.DataFrame:
    """Long dirigido indexado por (match_key, is_home)."""
    return H.h2h_history(master).set_index(["match_key", "is_home"])


def _lado(hist: pd.DataFrame, master: pd.DataFrame, i: int,
          lado: str = "home") -> pd.Series:
    """La fila dirigida del partido `i` vista desde el lado pedido."""
    return hist.loc[(master["match_key"].iloc[i], 1 if lado == "home" else 0)]


def _fila(wide: pd.DataFrame, master: pd.DataFrame, i: int) -> pd.Series:
    return wide.set_index("match_key").loc[master["match_key"].iloc[i]]


def _ewm_adjust_false(vals: list[float], halflife: float) -> list[float]:
    """Media exponencial recursiva de pandas con `adjust=False`, saltando huecos."""
    alpha = 1.0 - 0.5 ** (1.0 / halflife)
    y = None
    out = []
    for x in vals:
        if pd.isna(x):
            out.append(y)
            continue
        y = x if y is None else (1 - alpha) * y + alpha * x
        out.append(y)
    return out


# --------------------------------------------------------------------------- #
# 1. ANTIFUGA
# --------------------------------------------------------------------------- #
def test_antifuga() -> None:
    print("\n[1] ANTIFUGA — el enfrentamiento t no figura en su propio historial")
    m = _master([("aaa", "bbb", 3, 0), ("bbb", "aaa", 0, 1), ("aaa", "bbb", 0, 0)])
    w = H.build_h2h_features(m)
    h = _hist(m)

    f0 = _lado(h, m, 0)
    check(abs(f0["win_rate_ewm"] - 1 / 3) < _TOL
          and abs(f0["draw_rate_ewm"] - 1 / 3) < _TOL,
          "el primer enfrentamiento del par recibe el reparto neutral (1/3 cada "
          "desenlace)")
    check(pd.isna(f0["gd_ewm"]),
          "y la diferencia de goles historica queda NaN, no cero: el reparto "
          "neutral tiene sentido para una probabilidad y ninguno para un marcador")

    f1 = _lado(h, m, 1)
    check(abs(f1["win_rate_ewm"]) < _TOL,
          "el segundo enfrentamiento ve solo el primero (bbb habia perdido: tasa 0)")

    # prueba de intervencion: el marcador de t no puede alterar las features de t.
    m2 = m.copy()
    m2.loc[m2.index[2], ["fthg", "ftag", "ftr"]] = [4.0, 0.0, "H"]
    w2 = H.build_h2h_features(m2)
    cols = [c for c in w.columns if c != "match_key"]
    a, b = _fila(w, m, 2), _fila(w2, m, 2)
    igual = all((pd.isna(a[c]) and pd.isna(b[c]))
                or abs(float(a[c]) - float(b[c])) < _TOL for c in cols)
    check(igual, "alterar el marcador de t deja intactas TODAS las features h2h de t")


# --------------------------------------------------------------------------- #
# 2. SIMPLEX
# --------------------------------------------------------------------------- #
def test_simplex() -> None:
    print("\n[2] SIMPLEX — las tasas son una distribucion, no medias sueltas")
    m = _master([("aaa", "bbb", 3, 0), ("bbb", "aaa", 0, 1), ("aaa", "bbb", 0, 0),
                 ("bbb", "aaa", 2, 1), ("aaa", "bbb", 1, 1)])
    h = H.h2h_history(m)

    tasas = ["win_rate_ewm", "draw_rate_ewm"]
    dentro = all(bool(h[c].dropna().between(-1e-9, 1 + 1e-9).all()) for c in tasas)
    check(dentro, "ninguna tasa se sale de [0, 1]")
    check(bool((h[tasas].sum(axis=1) <= 1 + 1e-9).all()),
          "victoria y empate suman como mucho uno: lo que sobra es la derrota")
    check(not h[tasas].isna().any().any(),
          "el relleno neutral no deja huecos en las tasas (el hueco se reserva al gd)")

    # La derrota no se calcula: es exactamente uno menos las otras dos, de modo
    # que anadirla no aportaria informacion y si una columna mas que arrastrar por
    # todo el pipeline. Se comprueba la ausencia porque una magnitud retirada del
    # catalogo pero seguida calculando en el modulo es trabajo muerto.
    check("loss_rate_ewm" not in h.columns,
          "la tasa de derrota no se produce: es uno menos las otras dos y no "
          "anade nada que el estimador no pueda reconstruir")


# --------------------------------------------------------------------------- #
# 3. SIMETRIA DEL PAR DIRIGIDO
# --------------------------------------------------------------------------- #
def test_simetria() -> None:
    print("\n[3] SIMETRIA — las dos filas dirigidas describen el mismo historial")
    m = _master([("aaa", "bbb", 3, 0), ("bbb", "aaa", 0, 1), ("aaa", "bbb", 0, 0),
                 ("bbb", "aaa", 2, 1), ("aaa", "bbb", 1, 2)])
    w = H.build_h2h_features(m)
    h = H.h2h_history(m).set_index(["match_key", "is_home"])
    loc = h.xs(1, level="is_home")
    vis = h.xs(0, level="is_home").loc[loc.index]

    check(bool((loc["draw_rate_ewm"] - vis["draw_rate_ewm"]).abs().max() < 1e-12),
          "el empate es la misma cantidad vista desde los dos lados, que es lo "
          "que autoriza a publicarlo una sola vez y sin rotulo de lado")
    suma = loc["win_rate_ewm"] + vis["win_rate_ewm"] + loc["draw_rate_ewm"]
    check(bool((suma - 1.0).abs().max() < 1e-12),
          "y las dos tasas de victoria mas el empate suman uno: lo que uno gana "
          "es lo que el otro pierde")
    gd = (loc["gd_ewm"] + vis["gd_ewm"]).dropna()
    check(bool(gd.abs().max() < 1e-12) if len(gd) else False,
          "la diferencia de goles historica de uno es la del otro cambiada de signo")
    check(bool((w.set_index("match_key")["h2h_gd_ewm_diff"]
                - 2 * loc["gd_ewm"]).abs().max() < 1e-12),
          "y por tanto la columna de diferencia duplica el valor del local")
    check(bool((w.set_index("match_key")["h2h_draw_rate_ewm"]
                - loc["draw_rate_ewm"]).abs().max() < 1e-12),
          "mientras que la columna de enfrentamiento copia el valor comun")

    # El modulo no se fia de esa coincidencia: la exige. Si el empate difiriera
    # entre lados, el agrupamiento estaria mezclando pares —una clave de club sin
    # canonizar— y la columna dejaria de describir el enfrentamiento que dice
    # describir. Se comprueba que el guardian salta, porque un guardian que no
    # salta cuando debe es peor que no tenerlo: ademas tranquiliza.
    trucado = H.h2h_history(m)
    trucado.loc[trucado.index[0], "draw_rate_ewm"] += 0.5
    try:
        H._to_wide(trucado, ["win_rate_ewm", "draw_rate_ewm", "gd_ewm"])
        salto = False
    except AssertionError:
        salto = True
    check(salto, "y un empate que difiere entre lados detiene la construccion en "
                 "vez de publicar el del local sin mas")


# --------------------------------------------------------------------------- #
# 4. MEMORIA: cruza temporadas, no cruza rivales
# --------------------------------------------------------------------------- #
def test_memoria() -> None:
    print("\n[4] MEMORIA — la rivalidad no se reinicia en agosto ni se mezcla con otras")
    m = _master([("aaa", "bbb", 3, 0), ("aaa", "ccc", 0, 3), ("aaa", "ddd", 0, 4),
                 ("aaa", "bbb", 0, 0)],
                fechas=["2020-09-01", "2021-02-01", "2021-03-01", "2021-09-01"])
    h = _hist(m)

    f3 = _lado(h, m, 3)
    check(abs(f3["win_rate_ewm"] - 1.0) < _TOL,
          "el enfrentamiento de la temporada siguiente conserva el historial del par")
    check(abs(f3["gd_ewm"] - 3.0) < _TOL,
          "y arrastra tambien la diferencia de goles del enfrentamiento anterior")
    check(abs(f3["draw_rate_ewm"]) < _TOL,
          "las dos derrotas contra OTROS rivales no entran en el historial de este "
          "par: el unico precedente fue una victoria, asi que el empate es cero")

    f1 = _lado(h, m, 1)
    check(abs(f1["win_rate_ewm"] - 1 / 3) < _TOL,
          "y el primer cruce con un rival nuevo sigue partiendo del reparto neutral")


# --------------------------------------------------------------------------- #
# 5. ARITMETICA
# --------------------------------------------------------------------------- #
def test_aritmetica() -> None:
    print("\n[5] ARITMETICA — la media exponencial vale su definicion")
    # cuatro enfrentamientos del par con gd conocido desde la perspectiva de aaa
    m = _master([("aaa", "bbb", 3, 0), ("bbb", "aaa", 2, 0), ("aaa", "bbb", 1, 0),
                 ("bbb", "aaa", 1, 1)])
    h = _hist(m)
    gd_aaa = [3.0, -2.0, 1.0, 0.0]

    esperado = [np.nan] + _ewm_adjust_false(gd_aaa[:-1], H._HALFLIFE)
    obtenido = [_lado(h, m, i, "home" if m["home_key"].iloc[i] == "aaa" else "away")
                ["gd_ewm"] for i in range(4)]
    ok = all((pd.isna(o) and pd.isna(e)) or abs(float(o) - float(e)) < 1e-9
             for o, e in zip(obtenido, esperado))
    check(ok, f"gd_ewm = media exponencial de semivida {H._HALFLIFE:g} sobre lo anterior")

    # La semivida es la que declara el catalogo y no un numero escrito a mano en
    # el modulo: si las dos se separasen, el historial se olvidaria a un ritmo
    # distinto del que el catalogo documenta.
    check(H._HALFLIFE == catalogo.H2H_SEMIVIDA,
          "y la semivida que aplica el modulo es la que declara el catalogo")

    # con un solo precedente, la media exponencial es ese precedente: la primera
    # observacion entra entera porque no hay nada previo con lo que promediarla.
    f1 = _lado(h, m, 1, "away")
    check(abs(f1["gd_ewm"] - 3.0) < _TOL,
          "con un unico precedente la media vale ese precedente, sin amortiguar")


# --------------------------------------------------------------------------- #
# 6. NAN-SAFE
# --------------------------------------------------------------------------- #
def test_nan_safe() -> None:
    print("\n[6] NAN-SAFE — un partido futuro no cuenta como enfrentamiento jugado")
    m = _master([("aaa", "bbb", 3, 0), ("bbb", "aaa", np.nan, np.nan),
                 ("aaa", "bbb", np.nan, np.nan)])
    w = H.build_h2h_features(m)
    h = _hist(m)

    f2 = _lado(h, m, 2)
    check(abs(f2["win_rate_ewm"] - 1.0) < _TOL,
          "la jornada sin marcador se ignora en vez de entrar como empate")
    check(abs(f2["gd_ewm"] - 3.0) < _TOL,
          "y la diferencia de goles sigue siendo la del unico enfrentamiento jugado")
    check(w["match_key"].is_unique and len(w) == len(m),
          "la fila del partido por jugar recibe igualmente su historial")


# --------------------------------------------------------------------------- #
# 7. CONTRATO Y REGIMEN
# --------------------------------------------------------------------------- #
def test_contrato() -> None:
    print("\n[7] CONTRATO — lo declarado es exactamente lo producido")
    m = _master([("aaa", "bbb", 1, 0), ("bbb", "aaa", 2, 2), ("aaa", "ccc", 0, 1)])
    w = H.build_h2h_features(m)
    base = H.h2h_feature_names()

    # De un atributo caben tres columnas en la fila de partido —los dos lados y su
    # resta— y las tres son linealmente dependientes, asi que el catalogo elige
    # cuales salen. Aqui elige por naturaleza de la magnitud: lo que mide al
    # EQUIPO sale como diferencia, y el empate, que mide al PAR y vale lo mismo en
    # los dos lados, sale una sola vez sin rotulo, porque restarlo daria una
    # columna de ceros. Lo que se comprueba es que lo producido y lo declarado
    # sean el MISMO conjunto en las dos direcciones: que no falte nada que el
    # catalogo emita —una feature calculada y tirada a la basura— y que no sobre
    # nada que no emita, porque una columna que llega a la matriz sin estar
    # declarada se salta la curacion entera.
    esperadas = ["match_key"] + [c for b in base
                                 for c in catalogo.nombres(b, prefijo="h2h_")]
    check(set(w.columns) == set(esperadas),
          "lo producido coincide exactamente con lo que el catalogo declara",
          sorted(set(w.columns) ^ set(esperadas)))
    check(len(base) == len(set(base)), "la lista declarada no tiene nombres repetidos")
    check("h2h_draw_rate_ewm" in w.columns
          and "h2h_draw_rate_ewm_diff" not in w.columns
          and "home_h2h_draw_rate_ewm" not in w.columns,
          "el empate sale como magnitud del enfrentamiento: ni por lados ni "
          "restado, que daria una columna de ceros")
    check(not [b for b in catalogo.H2H_ATRIBUTOS if f"h2h_{b}_diff" not in w.columns],
          "y lo que mide al equipo sale como diferencia entre los dos")

    todas = [c for c in w.columns if c != "match_key"]
    check(not C.weekly_unavailable(todas),
          "ninguna columna h2h queda fuera del regimen semanal (no hay token de cierre)")
    check(w["match_key"].is_unique and len(w) == len(m),
          "la salida es una fila por partido, con match_key unica")


def _run() -> None:
    print("=" * 72)
    print("TEST — bloque head-to-head (historial dirigido por par de clubes)")
    print("=" * 72)
    test_antifuga()
    test_simplex()
    test_simetria()
    test_memoria()
    test_aritmetica()
    test_nan_safe()
    test_contrato()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_h2h() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
