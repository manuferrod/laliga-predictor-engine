"""Validacion offline de las features de contexto. Sin red y sin datos reales.

El bloque de contexto tiene una propiedad incomoda: todas sus columnas salen
siempre con un valor plausible. Una hora leida en UTC en vez de en hora local
sigue estando entre 0 y 24; una distancia calculada con la latitud y la longitud
intercambiadas sigue siendo un numero de kilometros positivo; un derbi definido
por un umbral kilometrico sigue siendo un indicador binario. Ninguno de esos
errores levanta una excepcion ni desentona en un histograma, de modo que la
unica forma de descartarlos es comprobar contra valores conocidos de antemano.

Se comprueba, sobre casos sinteticos construidos a mano:

  1. GEOMETRIA   la distancia ortodromica reproduce separaciones conocidas, es
                 simetrica, se anula sobre si misma y conserva precision en el
                 caso corto, que es donde la ley de cosenos fallaria.
  2. CALENDARIO  el sello de saque se lee en hora peninsular y no en UTC, tanto
                 en horario de invierno como de verano; los minutos entran como
                 fraccion; los cortes de franja caen donde estan declarados y el
                 fin de semana son sabado y domingo.
  3. GEOGRAFIA   el derbi es un hecho administrativo y no un umbral de
                 distancia: se verifica con los dos casos que separan ambos
                 criterios (el derbi gallego, lejano, y Vitoria-Pamplona,
                 cercano), ademas del derbi corto de una misma ciudad.
  4. COBERTURA   un club fuera de la tabla de estadios deja la geografia en NaN
                 en lugar de inventar una distancia, y `unknown_stadiums` lo
                 declara para que el QA pueda detenerse.
  5. CONTRATO    `context_feature_names` describe exactamente lo que
                 `build_context_features` produce, y la ausencia del sello de
                 saque no altera el esquema de salida.
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
import context as ctx  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK ' if cond else 'FAIL'}] {msg}")


def _ts(local: str) -> float:
    """Sello de saque (segundos desde la epoca) de una hora LOCAL peninsular.

    El master recibe el sello en UTC, asi que el caso de prueba se escribe en la
    hora que ve el espectador y se convierte aqui. De ese modo la comprobacion
    falla si el modulo omite la conversion de huso, que es justo el error que se
    busca: leer el sello en UTC adelanta una hora en invierno y dos en verano, y
    desplaza a la vispera todo saque nocturno.
    """
    return (pd.Timestamp(local, tz=ctx._TZ_LOCAL).tz_convert("UTC")
            - pd.Timestamp("1970-01-01", tz="UTC")).total_seconds()


def _master(filas: list[tuple[str, str, str | None]]) -> pd.DataFrame:
    """Master sintetico minimo: (local, visitante, hora local de saque o None)."""
    return pd.DataFrame({
        "match_key": [f"k{i}" for i in range(len(filas))],
        "home_key": [f[0] for f in filas],
        "away_key": [f[1] for f in filas],
        "kickoff_ts": [np.nan if f[2] is None else _ts(f[2]) for f in filas],
    })


# ---------------------------------------------------------------------------
# 1. geometria
# ---------------------------------------------------------------------------
def test_geometria() -> None:
    print("\n1. GEOMETRIA — la distancia reproduce separaciones conocidas")
    tabla = ctx.stadium_table().set_index("team_key")

    def d(a: str, b: str) -> float:
        ra, rb = tabla.loc[a], tabla.loc[b]
        return float(ctx.haversine_km(ra["lat"], ra["lon"], rb["lat"], rb["lon"]))

    # Referencias verificables: los dos estadios de Sevilla estan a poco mas de
    # tres kilometros y el trayecto Las Palmas-Barcelona es el mas largo de la
    # competicion. Un intercambio de latitud y longitud, o un radio terrestre
    # equivocado, rompe alguna de las dos.
    check(3.0 <= d("sevilla", "real_betis") <= 3.5,
          f"Sevilla-Betis ~3.2 km (obtenido {d('sevilla', 'real_betis'):.2f})")
    check(2150 <= d("las_palmas", "barcelona") <= 2200,
          f"Las Palmas-Barcelona ~2174 km (obtenido {d('las_palmas', 'barcelona'):.0f})")
    check(490 <= d("real_madrid", "barcelona") <= 510,
          f"Madrid-Barcelona ~499 km (obtenido {d('real_madrid', 'barcelona'):.0f})")

    check(abs(d("sevilla", "real_betis") - d("real_betis", "sevilla")) < 1e-9,
          "la distancia es simetrica: el orden de los estadios no la altera")
    check(d("sevilla", "sevilla") < 1e-9,
          "la distancia de un estadio a si mismo es cero (el local no se desplaza)")

    # El caso corto es el que motiva el semiverseno: con la ley de cosenos el
    # coseno del angulo es indistinguible de uno en coma flotante y la distancia
    # colapsa a cero o a un valor ruidoso.
    cerca = ctx.haversine_km(40.4531, -3.6883, 40.4532, -3.6883)
    check(0.005 < float(cerca) < 0.02,
          f"una diferencia de una diezmilesima de grado da ~11 m y no cero "
          f"(obtenido {float(cerca) * 1000:.1f} m)")

    todas = [d(a, b) for a in tabla.index for b in tabla.index if a != b]
    check(min(todas) > 0 and max(todas) < 2500,
          f"todas las distancias posibles caen en (0, 2500] km "
          f"(max {max(todas):.0f})")


# ---------------------------------------------------------------------------
# 2. calendario
# ---------------------------------------------------------------------------
def test_calendario() -> None:
    print("\n2. CALENDARIO — hora local, franja y fin de semana")
    # invierno (UTC+1) y verano (UTC+2), ambos a las 21:00 locales: leer el sello
    # en UTC daria 20:00 y 19:00 respectivamente, y el segundo caso ademas
    # cambiaria de franja.
    m = _master([
        ("real_madrid", "barcelona", "2021-01-09 21:00"),   # sabado, invierno
        ("real_madrid", "barcelona", "2021-08-15 21:00"),   # domingo, verano
        ("real_madrid", "barcelona", "2021-01-09 14:00"),   # mediodia
        ("real_madrid", "barcelona", "2021-01-09 16:15"),   # tarde, con minutos
        ("real_madrid", "barcelona", "2021-01-09 19:00"),   # corte exacto: noche
        ("real_madrid", "barcelona", "2021-01-09 15:00"),   # corte exacto: tarde
        ("real_madrid", "barcelona", "2021-01-13 21:00"),   # miercoles
        ("real_madrid", "barcelona", "2021-01-08 21:00"),   # viernes
    ])
    out = ctx.build_context_features(m)

    # La hora local ya no es una columna de la matriz: el catalogo emite la
    # franja, que es la lectura de escalon de esa misma magnitud. Pero la
    # conversion de huso sigue siendo el error mas caro del bloque —desplaza de
    # franja y de dia todo saque nocturno— y una franja correcta no la demuestra,
    # porque las 20:00 y las 21:00 caen en la misma franja. Asi que se comprueba
    # donde la conversion vive, que es `_local_kickoff`, y aparte se comprueba
    # que la franja publicada se corresponda con esa hora.
    hora = ctx._local_kickoff(m)
    hora = (hora.dt.hour + hora.dt.minute / 60.0).astype("float64")
    check(abs(hora.iloc[0] - 21.0) < 1e-9,
          "saque de invierno a las 21:00 locales se lee como 21.0 y no como 20.0")
    check(abs(hora.iloc[1] - 21.0) < 1e-9,
          "saque de verano a las 21:00 locales se lee como 21.0 y no como 19.0")
    check(abs(hora.iloc[3] - 16.25) < 1e-9,
          "los minutos entran como fraccion de hora (16:15 -> 16.25)")

    franjas = list(out["kickoff_slot"])
    check(all(f == ctx._classify_slot(h) for f, h in zip(franjas, hora)),
          "y la franja publicada es la clasificacion de esa hora local, no de "
          "una hora leida por segunda vez y por otro camino")
    check(franjas[2] == "mediodia" and franjas[3] == "tarde" and franjas[0] == "noche",
          "las tres franjas se asignan segun la hora local")
    check(franjas[5] == "tarde",
          "el corte de las 15:00 es cerrado por arriba: 15:00 ya es 'tarde'")
    check(franjas[4] == "noche",
          "el corte de las 19:00 es cerrado por arriba: 19:00 ya es 'noche'")
    check(set(out["kickoff_slot"].dropna()) <= set(ctx.SLOT_ORDER),
          "no aparece ninguna franja fuera de las tres declaradas")

    fs = list(out["is_weekend"])
    check(fs[0] == 1.0 and fs[1] == 1.0,
          "sabado y domingo cuentan como fin de semana")
    check(fs[6] == 0.0 and fs[7] == 0.0,
          "miercoles y viernes no: el saque entre semana lo recoge `fat_is_midweek`")

    # sin sello no se inventa una hora
    m2 = _master([("real_madrid", "barcelona", None)])
    o2 = ctx.build_context_features(m2).iloc[0]
    check(pd.isna(o2["is_weekend"]) and pd.isna(o2["kickoff_slot"]),
          "sin sello de saque las dos columnas de calendario quedan NaN, "
          "no a cero ni a la franja mas frecuente")
    check(ctx._classify_slot(np.nan) is pd.NA,
          "y la clasificacion de una hora ausente devuelve el hueco en vez de "
          "caer en la ultima rama, que la mandaria a 'noche'")

    # La hora en bruto se calcula pero no se publica, y eso hay que afirmarlo:
    # una columna intermedia que se olvidara en el frame llegaria a la matriz sin
    # pasar por el catalogo, que es exactamente lo que la curacion evita.
    check("kickoff_hour" not in out.columns,
          "la hora en bruto no sale a la matriz: la hipotesis de dominio es de "
          "escalon y la franja es su lectura correcta")


# ---------------------------------------------------------------------------
# 3. geografia y derbi
# ---------------------------------------------------------------------------
def test_derbi() -> None:
    print("\n3. GEOGRAFIA — el derbi es administrativo, no metrico")
    casos = [
        ("celta_vigo", "deportivo_la_coruna", 1.0, "derbi gallego (~131 km): lejano y derbi"),
        ("alaves", "osasuna", 0.0, "Vitoria-Pamplona (~86 km): cercano y NO derbi"),
        ("real_madrid", "atletico_madrid", 1.0, "derbi madrileno (~8 km)"),
        ("barcelona", "espanyol", 1.0, "derbi barcelones (~5 km)"),
        ("athletic_bilbao", "real_sociedad", 1.0, "derbi vasco (~79 km)"),
        ("real_madrid", "barcelona", 0.0, "Madrid-Barcelona: rivalidad, pero no derbi"),
        ("las_palmas", "mallorca", 0.0, "dos islas de comunidades distintas"),
    ]
    m = _master([(a, b, "2021-01-09 21:00") for a, b, _, _ in casos])
    out = ctx.build_context_features(m)
    for i, (_, _, esperado, nombre) in enumerate(casos):
        check(out.at[i, "is_derby"] == esperado, f"is_derby={esperado:.0f} — {nombre}")

    # la pareja que separa los dos criterios: si el derbi se derivara de un umbral
    # kilometrico, no existiria ningun corte que acertara en ambos casos a la vez.
    d_gal = out.at[0, "away_travel_km"]
    d_vit = out.at[1, "away_travel_km"]
    check(d_gal > d_vit,
          f"el derbi gallego ({d_gal:.0f} km) es mas largo que un no-derbi "
          f"({d_vit:.0f} km): ningun umbral de distancia reproduce el criterio")

    check(out["away_travel_km"].min() > 0,
          "toda distancia entre dos clubes distintos es estrictamente positiva")


# ---------------------------------------------------------------------------
# 4. cobertura de la tabla de estadios
# ---------------------------------------------------------------------------
def test_cobertura() -> None:
    print("\n4. COBERTURA — un club sin estadio se declara, no se rellena")
    m = _master([("real_madrid", "club_inventado", "2021-01-09 21:00"),
                 ("real_madrid", "barcelona", "2021-01-09 21:00")])
    faltan = ctx.unknown_stadiums(m)
    check(faltan == ["club_inventado"],
          f"`unknown_stadiums` declara el club ausente (obtenido {faltan})")

    out = ctx.build_context_features(m)
    check(pd.isna(out.at[0, "away_travel_km"]) and pd.isna(out.at[0, "is_derby"]),
          "sus dos columnas de geografia quedan NaN: no se imputa una distancia "
          "mediana que resultaria creible y seria inventada")
    check(not pd.isna(out.at[1, "away_travel_km"]),
          "y el resto de partidos conserva su valor: el hueco no se contagia")

    check(ctx.unknown_stadiums(_master([("real_madrid", "barcelona", None)])) == [],
          "con todos los clubes en la tabla la lista sale vacia")

    tabla = ctx.stadium_table()
    check(tabla["team_key"].is_unique and len(tabla) == len(ctx._ESTADIOS),
          f"la tabla de estadios tiene {len(tabla)} clubes y ninguna clave repetida")
    check(tabla["lat"].between(27.0, 44.0).all()
          and tabla["lon"].between(-19.0, 5.0).all(),
          "todas las coordenadas caen dentro del territorio espanol "
          "(descarta latitud y longitud intercambiadas o de signo cambiado)")


# ---------------------------------------------------------------------------
# 5. contrato del modulo
# ---------------------------------------------------------------------------
def test_contrato() -> None:
    print("\n5. CONTRATO — lo declarado y lo producido coinciden")
    nombres = ctx.context_feature_names()
    esperadas = ["match_key"] + nombres["numeric"] + nombres["categorical"]

    # El modulo no es la ultima palabra sobre lo que emite: `_NUMERIC` y
    # `_CATEGORICAL` se leen del catalogo, y es el catalogo quien decide. Se
    # comprueba que esa lectura siga siendo fiel, porque si el modulo se pusiera
    # a declarar por su cuenta, la curacion tendria una via de escape.
    check(nombres["numeric"] == list(catalogo.CONTEXTO)
          and nombres["categorical"] == list(catalogo.CONTEXTO_CATEGORICO),
          "lo que el modulo declara es literalmente lo que el catalogo declara")
    check("kickoff_hour" not in catalogo.CONTEXTO
          and "kickoff_hour" not in catalogo.CONTEXTO_CATEGORICO,
          "y la hora en bruto no figura en el catalogo bajo ninguno de los dos tipos")

    m = _master([("real_madrid", "barcelona", "2021-01-09 21:00"),
                 ("sevilla", "real_betis", "2021-01-10 14:00")])
    out = ctx.build_context_features(m)
    check(list(out.columns) == esperadas,
          "las columnas producidas son exactamente las declaradas, en el mismo orden")
    check(len(out) == len(m) and out["match_key"].is_unique,
          "una fila por partido y match_key unico")
    check("kickoff_ts" not in out.columns,
          "el sello de saque no viaja a la salida: es materia prima, no feature")

    # un master sin sello de saque —el caso de la ruta de inferencia cuando el
    # calendario aun no publica la hora— no puede alterar el esquema.
    sin_ts = m.drop(columns=["kickoff_ts"])
    o2 = ctx.build_context_features(sin_ts)
    check(list(o2.columns) == esperadas,
          "sin la columna `kickoff_ts` el esquema de salida se mantiene (calendario a NaN)")
    check(o2["kickoff_slot"].isna().all() and o2["is_weekend"].isna().all()
          and not o2["away_travel_km"].isna().any(),
          "y la geografia sigue calculandose: los dos bloques son independientes")

    faltan = m.drop(columns=["home_key"])
    try:
        ctx.build_context_features(faltan)
        ok = False
    except KeyError:
        ok = True
    check(ok, "sin las claves de los equipos el modulo falla en vez de devolver NaN")

    check(set(nombres["numeric"]) & set(nombres["categorical"]) == set(),
          "ninguna columna se declara a la vez numerica y categorica")


def _run() -> None:
    print("=" * 72)
    print("TEST — features de contexto (calendario y geografia)")
    print("=" * 72)
    test_geometria()
    test_calendario()
    test_derbi()
    test_cobertura()
    test_contrato()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    for f in FAIL:
        print(f"  FALLO: {f}")
    print("=" * 72)


def test_context() -> None:
    """Punto de entrada para pytest."""
    _run()
    assert not FAIL, f"{len(FAIL)} comprobaciones fallidas: {FAIL}"


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
