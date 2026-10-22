"""Banco de pruebas de qa_models.py (QA de lo publicado por la fase de modelado).

Una comprobacion de calidad que nunca se ha visto fallar no es una comprobacion:
es una linea de codigo. Por eso cada prueba de este banco construye a mano el
defecto exacto que la funcion debe cazar y verifica DOS cosas —que el caso sano
pasa limpio y que el caso averiado produce un fallo—, porque una funcion que
siempre devuelve lista vacia superaria la mitad de las pruebas que solo miran el
caso sano.

Todo es sintetico y offline: ni red, ni artefactos, ni pipeline. Las tablas se
arman con la forma exacta que publica `build_models.py` y con el numero de filas
justo para que el resultado esperado se pueda calcular a mano.

  1. Probabilidades: terna legitima, suma distinta de uno, valor fuera de [0,1],
     ausente, columnas que faltan y tabla vacia.
  2. Proxima jornada: fecha pasada, partido ya disputado, clave duplicada y
     tabla sin las columnas necesarias.
  3. Paridad de esquema: identidad, columna de mas, columna de menos y —el caso
     que importa— mismo conjunto en distinto orden.
  4. Regimen: columna posterior al saque bajo el regimen semanal, y el mismo
     caso bajo el retrospectivo, donde es legitimo.
  5. Sello del holdout: sin puntuar, sello sin tabla, tabla sin sello, recuento
     discrepante, codigo de ejecucion distinto y temporada intrusa.
  6. Cobertura comun: contendientes con la misma n y con n distinta, y el
     agrupamiento por modelo. En la seccion 10, la misma invariante dentro de la
     tabla por temporada, que es la unica donde el juego de referencias cambia de
     fila a fila.
  7. Conjunto publicado: correspondencia entre lo que declara el manifiesto y lo
     que publica `conjunto_declarado`, familia cambiada, conjunto sin cerrar, sin
     procedencia, con el recuento descuadrado y manifiesto mudo.
  8. Rendimiento con su liston: manifiesto sin la cifra de la referencia, resta
     que no cuadra, serie por jornada que cierra donde no debe y serie de un
     precio que el manifiesto no resume.
  9. Codigo de ejecucion: artefactos coherentes, uno de otra pasada y uno sin la
     columna.
 10. Auditoria de un regimen completo sobre un directorio sintetico en disco
     temporal, incluida la deteccion de un artefacto ausente.
 11. Auditoria de la prediccion emitida: caso sano y caso con las tres averias.
 12. Contrato: `run_qa` lanza `PublicacionError` cuando algo falla y devuelve
     True cuando todo pasa.
 13. Contrato de `build_models`: conjunto, familia y valores declarados en la
     cabecera, y la reserva cerrada mientras el conjunto sea provisional.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_FEATURES = _HERE.parent / "features"
for _p in (_HERE, _FEATURES):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import laboratorio as lb  # noqa: E402
import qa_models as qa  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(cond: bool, msg: str) -> None:
    if cond:
        PASS.append(msg)
        print(f"  [OK ] {msg}")
    else:
        FAIL.append(msg)
        print(f"  [FAIL] {msg}")


# ---------------------------------------------------------------------------
# utilidades de construccion
# ---------------------------------------------------------------------------
def _terna(n: int, seed: int = 3) -> np.ndarray:
    """n ternas legitimas, normalizadas de forma exacta."""
    r = np.random.default_rng(seed)
    P = r.dirichlet([4.0, 3.0, 3.0], size=n)
    return P / P.sum(axis=1, keepdims=True)


def _pred(n: int = 10, *, seed: int = 3, fecha: str = "2026-05-01",
          season: str = "2025/26", run: str = "abc123abc123") -> pd.DataFrame:
    P = _terna(n, seed)
    return pd.DataFrame({
        "match_key": [f"k{i:03d}" for i in range(n)],
        "date": pd.to_datetime(fecha) + pd.to_timedelta(np.arange(n), unit="D"),
        "season": season,
        "jornada": 30,
        "p_home": P[:, 0], "p_draw": P[:, 1], "p_away": P[:, 2],
        "run_code": run,
    })


def _disputados(claves) -> pd.DataFrame:
    return pd.DataFrame({"match_key": list(claves)})


# ---------------------------------------------------------------------------
# 1. Probabilidades
# ---------------------------------------------------------------------------
def test_1_probabilidades() -> None:
    print("\n1. probabilidades legitimas")
    ok = _pred(12)
    check(qa.check_probabilidades(ok) == [], "terna legitima no produce fallos")

    mal = ok.copy()
    mal.loc[0, "p_home"] = float(mal.loc[0, "p_home"]) + 0.1
    f = qa.check_probabilidades(mal)
    check(len(f) == 1 and "no suma uno" in f[0], "detecta una terna que no suma uno")

    fuera = ok.copy()
    fuera.loc[1, "p_draw"] = -0.2
    fuera.loc[1, "p_away"] = float(fuera.loc[1, "p_away"]) + 0.2
    f = qa.check_probabilidades(fuera)
    check(any("fuera del intervalo unidad" in x for x in f),
          "detecta una probabilidad negativa aunque la terna siga sumando uno")

    nan = ok.copy()
    nan.loc[2, "p_home"] = np.nan
    f = qa.check_probabilidades(nan)
    check(any("ausente" in x for x in f), "detecta una probabilidad ausente")

    f = qa.check_probabilidades(ok.drop(columns=["p_draw"]))
    check(len(f) == 1 and "faltan las columnas" in f[0],
          "detecta que falta una columna de probabilidad")

    f = qa.check_probabilidades(ok.iloc[:0])
    check(len(f) == 1 and "ninguna fila" in f[0], "detecta una tabla vacia")

    # El margen de tolerancia no puede tragarse un error real: 1e-3 es cinco
    # ordenes de magnitud mayor que el ruido de coma flotante.
    casi = ok.copy()
    casi.loc[3, "p_home"] = float(casi.loc[3, "p_home"]) + 1e-3
    check(qa.check_probabilidades(casi) != [],
          "una desviacion de 1e-3 se considera error y no redondeo")

    # ...y el ruido genuino de coma flotante si tiene que pasar.
    ruido = ok.copy()
    ruido["p_home"] = ruido["p_home"] + 1e-12
    check(qa.check_probabilidades(ruido) == [],
          "una desviacion de 1e-12 se considera redondeo y no error")


# ---------------------------------------------------------------------------
# 2. Proxima jornada
# ---------------------------------------------------------------------------
def test_2_jornada_futura() -> None:
    print("\n2. la proxima jornada es futura")
    hoy = pd.Timestamp("2026-05-01")
    fut = _pred(6, fecha="2026-05-02")
    check(qa.check_jornada_futura(fut, _disputados([]), hoy=hoy) == [],
          "una jornada enteramente futura no produce fallos")

    pas = _pred(6, fecha="2026-04-25")     # 6 fechas: 25,26,27,28,29,30 abril
    f = qa.check_jornada_futura(pas, _disputados([]), hoy=hoy)
    check(len(f) == 1 and "6 predicciones con fecha anterior" in f[0],
          "cuenta exactamente las seis predicciones con fecha pasada")

    mixta = _pred(6, fecha="2026-04-29")   # 29,30 abril pasados; 1..4 mayo no
    f = qa.check_jornada_futura(mixta, _disputados([]), hoy=hoy)
    check(len(f) == 1 and "2 predicciones con fecha anterior" in f[0],
          "la fecha de hoy no cuenta como pasada")

    f = qa.check_jornada_futura(fut, _disputados(["k002", "k004"]), hoy=hoy)
    check(any("ya disputados" in x for x in f),
          "detecta un partido ya disputado aunque su fecha sea futura")

    dup = pd.concat([fut, fut.iloc[[0]]], ignore_index=True)
    f = qa.check_jornada_futura(dup, _disputados([]), hoy=hoy)
    check(any("mas de una vez" in x.replace("á", "a") for x in f),
          "detecta un partido publicado dos veces")

    f = qa.check_jornada_futura(fut.drop(columns=["date"]), _disputados([]), hoy=hoy)
    check(any("no lleva `date`" in x for x in f), "exige la columna de fecha")

    f = qa.check_jornada_futura(fut.drop(columns=["match_key"]), _disputados([]),
                                hoy=hoy)
    check(any("no lleva `match_key`" in x for x in f), "exige la clave de partido")

    f = qa.check_jornada_futura(fut.iloc[:0], _disputados([]), hoy=hoy)
    check(len(f) == 1 and "ninguna prediccion" in f[0].replace("ó", "o"),
          "una jornada sin predicciones es un fallo, no un caso trivial")


# ---------------------------------------------------------------------------
# 3. Paridad de esquema
# ---------------------------------------------------------------------------
def test_3_orden_columnas() -> None:
    print("\n3. paridad de esquema entrenamiento <-> inferencia")
    base = ["elo_diff", "forma_h", "forma_a", "mkt_p_home"]
    check(qa.check_orden_columnas(base, list(base)) == [],
          "esquemas identicos no producen fallos")

    f = qa.check_orden_columnas(base, base + ["extra"])
    check(len(f) == 1 and "columnas que el" in f[0], "detecta una columna de mas")

    f = qa.check_orden_columnas(base, base[:-1])
    check(len(f) == 1 and "le faltan" in f[0], "detecta una columna de menos")

    permutado = [base[0], base[2], base[1], base[3]]
    f = qa.check_orden_columnas(base, permutado)
    check(len(f) == 1 and "distinto orden" in f[0] and "posición 1" in f[0],
          "detecta la permutacion y senala la primera posicion discrepante")

    # La permutacion es el caso que ninguna comprobacion por conjuntos veria.
    check(set(base) == set(permutado) and qa.check_orden_columnas(base, permutado),
          "la permutacion tiene el mismo conjunto de columnas y aun asi falla")


# ---------------------------------------------------------------------------
# 4. Regimen
# ---------------------------------------------------------------------------
def test_4_regimen() -> None:
    print("\n4. coherencia del regimen")
    import columns as C

    limpias = ["elo_diff", "forma_h", "mkt_p_home"]
    check(qa.check_regimen(limpias, "semanal") == [],
          "columnas conocibles al predecir pasan bajo el regimen semanal")

    intrusa = C.weekly_unavailable(
        ["odds_close_avg_h", "closing_p_home", "elo_diff"])
    if not intrusa:
        check(False, "la politica de columnas no reconoce ninguna intrusa de prueba")
        return
    sucias = limpias + list(intrusa)
    f = qa.check_regimen(sucias, "semanal")
    check(len(f) == 1 and "no conocibles al predecir" in f[0],
          "detecta una columna posterior al saque bajo el regimen semanal")

    check(qa.check_regimen(sucias, "retrospectivo") == [],
          "la misma columna es legitima bajo el regimen retrospectivo")


# ---------------------------------------------------------------------------
# 5. Sello del holdout
# ---------------------------------------------------------------------------
def test_5_sello_holdout() -> None:
    print("\n5. pasada unica del holdout")
    pred = _pred(8, season="2025/26", run="deadbeefcafe")
    sello = {"partidos": 8, "run_code": "deadbeefcafe", "modelo": "logistica",
             "generado": "2026-06-01T10:00:00+00:00"}

    check(qa.check_sello_holdout(None, None) == [],
          "no haber puntuado el holdout todavia es correcto")
    check(qa.check_sello_holdout(sello, pred, temporadas_selladas=["2025/26"]) == [],
          "sello y tabla coherentes no producen fallos")

    f = qa.check_sello_holdout(None, pred)
    check(len(f) == 1 and "sin sello" in f[0], "detecta predicciones sin sello")

    f = qa.check_sello_holdout(sello, None)
    check(len(f) == 1 and "sin la tabla" in f[0].replace("í", "i") or
          len(f) == 1 and "no la tabla" in f[0], "detecta sello sin tabla")

    f = qa.check_sello_holdout({**sello, "partidos": 9}, pred,
                               temporadas_selladas=["2025/26"])
    check(len(f) == 1 and "acredita 9 partidos" in f[0],
          "detecta que el sello y la tabla no cuentan los mismos partidos")

    f = qa.check_sello_holdout({**sello, "run_code": "otra00000000"}, pred,
                               temporadas_selladas=["2025/26"])
    check(any("y el" in x and "sello de" in x for x in f),
          "detecta que la tabla se regenero despues de sellar")

    intrusa = pred.copy()
    intrusa.loc[0, "season"] = "2024/25"
    f = qa.check_sello_holdout(sello, intrusa, temporadas_selladas=["2025/26"])
    check(any("temporadas que no son del holdout" in x for x in f),
          "detecta una temporada que no pertenece al holdout")


# ---------------------------------------------------------------------------
# 5b. Separacion del bloque en curso
# ---------------------------------------------------------------------------
def test_5b_produccion() -> None:
    print("\n5b. la temporada en curso no se mezcla con lo que decide")
    val = _pred(20, season="2024/25", fecha="2025-03-01")
    hold = _pred(8, season="2025/26", fecha="2026-05-01")
    curso = _pred(10, season="2026/27", fecha="2026-10-01")
    SELLADAS = ["2025/26"]

    check(qa.check_produccion(None, val, hold) == [],
          "que no haya temporada en curso puntuable todavia es correcto")
    check(qa.check_produccion(curso, val, hold,
                              temporadas_selladas=SELLADAS) == [],
          "en curso posterior al holdout y sin solape no produce fallos")

    # El fallo que importa: una temporada que aparece en los dos sitios. Si entrara
    # en validacion, habria participado en elegir familia, valores y variables con
    # el holdout dentro de su ventana de entrenamiento.
    f = qa.check_produccion(curso, pd.concat([val, curso], ignore_index=True),
                            hold, temporadas_selladas=SELLADAS)
    check(any("comparte temporada con validación" in x for x in f),
          "detecta que la temporada en curso tambien esta en validacion")

    f = qa.check_produccion(curso, val, pd.concat([hold, curso], ignore_index=True),
                            temporadas_selladas=SELLADAS)
    check(any("comparte temporada con holdout" in x for x in f),
          "y que esta en el holdout, que es la misma averia por el otro lado")

    # «En curso» significa posterior al sello. Una temporada anterior etiquetada
    # como produccion seria una temporada de desarrollo publicada sin haber pasado
    # por el protocolo que la evalua.
    vieja = _pred(10, season="2023/24", fecha="2024-02-01")
    f = qa.check_produccion(vieja, val, hold, temporadas_selladas=SELLADAS)
    check(any("no son posteriores al holdout" in x for x in f),
          "detecta una temporada anterior al holdout colada como en curso")

    f = qa.check_produccion(curso.iloc[0:0], val, hold,
                            temporadas_selladas=SELLADAS)
    check(len(f) == 1 and "vacío" in f[0],
          "un artefacto vacio es un artefacto que no debio escribirse")

    f = qa.check_produccion(curso.drop(columns=["season"]), val, hold,
                            temporadas_selladas=SELLADAS)
    check(len(f) == 1 and "no declara temporada" in f[0],
          "sin temporada no se puede comprobar nada, y eso se dice en vez de pasar")


# ---------------------------------------------------------------------------
# 6. Cobertura comun
# ---------------------------------------------------------------------------
def test_6_cobertura() -> None:
    print("\n6. cobertura comun entre modelo y referencias")
    tab = pd.DataFrame({
        "modelo": ["logistica"] * 3 + ["bosque"] * 3,
        "contendiente": ["modelo", "consenso", "pinnacle"] * 2,
        "n": [500, 500, 500, 480, 480, 480],
        "rps": [0.20, 0.21, 0.205, 0.21, 0.21, 0.206],
    })
    check(qa.check_cobertura_comun(tab) == [],
          "misma n dentro de cada familia no produce fallos")
    check(qa.check_cobertura_comun(tab, por=()) != [],
          "sin agrupar, dos familias con n distinta se leen como incoherencia")

    roto = tab.copy()
    roto.loc[1, "n"] = 620          # el mercado, medido sobre mas partidos
    f = qa.check_cobertura_comun(roto)
    check(len(f) == 1 and "500" in f[0] and "620" in f[0],
          "detecta que el mercado se midio sobre mas partidos que el modelo")

    f = qa.check_cobertura_comun(tab.drop(columns=["n"]))
    check(len(f) == 1 and "no lleva la columna" in f[0],
          "exige la columna con el numero de partidos")


# ---------------------------------------------------------------------------
# 7. Conjunto publicado
# ---------------------------------------------------------------------------
def _bloque_conjunto(**cambios) -> dict:
    """Conjunto cerrado, con procedencia y con su lista de variables."""
    base = {"cerrado": True, "procedencia": "C.4 · mercado + fuerza",
            "n_variables": 3, "variables": ["a", "b", "c"], "nota": ""}
    base.update(cambios)
    return base


def test_7_conjunto() -> None:
    print("\n7. el conjunto declarado es el que se midio y el que se emite")
    tabla = pd.DataFrame({"orden": [1, 2, 3], "variable": ["a", "b", "c"]})
    sano = {"publicado": {"modelo": "logistica"}, "familia": "logistica",
            "conjunto": _bloque_conjunto()}
    check(qa.check_conjunto(sano, tabla) == [],
          "manifiesto y tabla que listan las mismas variables pasan limpio")

    # El caso que la comprobacion existe para cazar: el manifiesto describe un
    # modelo y publica otro, de modo que la emision ajustaria un objeto distinto
    # del que produjo las cifras.
    otra = {"publicado": {"modelo": "bosque"}, "familia": "logistica",
            "conjunto": _bloque_conjunto()}
    f = qa.check_conjunto(otra, tabla)
    check(any("publica «bosque»" in x for x in f),
          "detecta que la familia declarada no es la publicada")

    # Un conjunto provisional es una pasada de exploracion, no una publicacion.
    provisional = {"publicado": {"modelo": "logistica"}, "familia": "logistica",
                   "conjunto": _bloque_conjunto(cerrado=False,
                                                procedencia="sin cerrar")}
    f = qa.check_conjunto(provisional, tabla)
    check(any("provisional" in x for x in f),
          "un conjunto sin cerrar no se publica")
    check(any("procedencia" in x for x in f),
          "un conjunto sin procedencia no es reproducible")

    descuadre = {"publicado": {"modelo": "logistica"}, "familia": "logistica",
                 "conjunto": _bloque_conjunto(n_variables=5)}
    f = qa.check_conjunto(descuadre, tabla)
    check(any("dice 5" in x for x in f),
          "detecta que el recuento declarado no cuadra con la lista")

    distinto = {"publicado": {"modelo": "logistica"}, "familia": "logistica",
                "conjunto": _bloque_conjunto(variables=["a", "b", "z"])}
    f = qa.check_conjunto(distinto, tabla)
    check(any("no listan" in x for x in f),
          "detecta que el manifiesto y el parquet no publican lo mismo")

    f = qa.check_conjunto({"publicado": {"modelo": "logistica"}}, tabla)
    check(len(f) == 1 and "no declara con que" in f[0].replace("é", "e"),
          "un manifiesto sin seccion de conjunto es un fallo")


# ---------------------------------------------------------------------------
# 8. El rendimiento se publica con su liston
# ---------------------------------------------------------------------------
def _eco_manifiesto(rend: float = 0.031, ref: float = -0.052) -> dict:
    """Bloque economico coherente: la diferencia es exactamente la resta."""
    return {"cierre_avg": {"rendimiento": rend, "casa_referencia": "b365",
                           "rendimiento_referencia": ref,
                           "diferencia": rend - ref}}


def _eco_jornada(rend: float = 0.031, ref: float = -0.052,
                 cuotas: str = "cierre_avg") -> pd.DataFrame:
    """Serie por jornada cuyo acumulado termina en esas dos cifras."""
    return pd.DataFrame({
        "cuotas": cuotas, "season": "2024/25", "jornada": [1, 2, 3],
        "rendimiento_acum_modelo": [0.10, -0.04, rend],
        "rendimiento_acum_ref": [-0.02, -0.09, ref],
    })


def test_8_economia() -> None:
    print("\n8. el rendimiento se publica con su liston")
    man = {"economia": _eco_manifiesto()}
    check(qa.check_economia(man, _eco_jornada()) == [],
          "agregado y serie que cierran en la misma cifra pasan limpio")

    check(qa.check_economia({}, None) != [],
          "un manifiesto sin bloque economico es un fallo")

    # El defecto que la comprobacion existe para cazar: publicar el retorno del
    # modelo a solas, que invita a leerlo contra el cero.
    solo = {"economia": {"cierre_avg": {"rendimiento": 0.031}}}
    f = qa.check_economia(solo, None)
    check(len(f) == 3 and all("sin su liston" in x.replace("ó", "o") for x in f),
          "detecta que se publica el rendimiento sin el de la referencia")

    torcida = {"economia": {"cierre_avg": {**man["economia"]["cierre_avg"],
                                           "diferencia": 0.5}}}
    f = qa.check_economia(torcida, None)
    check(len(f) == 1 and "no es la resta" in f[0],
          "detecta una diferencia que no es la resta de los dos rendimientos")

    # La averia silenciosa: agregado y detalle de ejecuciones distintas.
    f = qa.check_economia(man, _eco_jornada(rend=0.031, ref=0.20))
    check(len(f) == 1 and "«ref»" in f[0],
          "detecta que la serie de la referencia no cierra donde dice el manifiesto")

    f = qa.check_economia(man, _eco_jornada(cuotas="consenso"))
    check(len(f) == 1 and "no resume" in f[0],
          "detecta una serie de un precio que el manifiesto no resume")

    f = qa.check_economia(man, _eco_jornada().drop(columns=["cuotas"]))
    check(len(f) == 1 and "a que precio" in f[0].replace("é", "e"),
          "una serie que no dice a que precio corresponde es un fallo")

    # --- la temporada en curso trae su propia economia y su propio liston ------
    # Es la cifra que mas tienta a publicarse sola, porque es la que cualquiera
    # que abra la aplicacion un lunes quiere ver, y la que menos contexto tiene
    # para leerse contra el cero: media docena de jornadas.
    con_curso = {"economia": _eco_manifiesto(),
                 "en_curso": {"economia": _eco_manifiesto(rend=0.08, ref=-0.01)}}
    check(qa.check_economia(con_curso, None) == [],
          "la economia en curso con su liston no produce fallos")

    cojo = {"economia": _eco_manifiesto(),
            "en_curso": {"economia": {"cierre_avg": {"rendimiento": 0.08}}}}
    f = qa.check_economia(cojo, None)
    check(len(f) == 3 and all("en curso/" in x for x in f),
          "el liston se exige tambien a la temporada en curso, y se dice cual es")

    # El cuadre es contra VALIDACION. Si la serie de la jornada en curso entrara
    # en el cierre, la comprobacion fallaria cada semana por el motivo
    # equivocado: el manifiesto resume validacion y la ultima fila seria otra.
    mezclada = pd.concat(
        [_eco_jornada().assign(bloque="validacion"),
         _eco_jornada(rend=0.08, ref=-0.01).assign(bloque="produccion")],
        ignore_index=True)
    check(qa.check_economia(con_curso, mezclada) == [],
          "con el rotulo, la serie en curso no descuadra el cierre de validacion")

    check(any("ninguna fila de validación" in x for x in
              qa.check_economia(con_curso, mezclada[mezclada["bloque"]
                                                    == "produccion"])),
          "una serie que solo trae la temporada en curso no cuadra nada y lo dice")

    # Sin rotulo y con dos economias publicadas la funcion no puede saber que
    # filas cuadrar. Decirlo es mejor que elegir una y acertar la mitad de las
    # veces; con una sola economia no hay ambiguedad y se sigue como siempre.
    f = qa.check_economia(con_curso, mezclada.drop(columns=["bloque"]))
    check(len(f) == 1 and "no dice a cuál pertenece" in f[0],
          "sin rotulo y con dos economias, la ambiguedad se denuncia")
    check(qa.check_economia(man, _eco_jornada()) == [],
          "y con una sola economia el rotulo no hace falta")


# ---------------------------------------------------------------------------
# 9. Codigo de ejecucion
# ---------------------------------------------------------------------------
def test_9_run_code() -> None:
    print("\n9. una sola ejecucion por regimen")
    man = {"run_code": "aaaa1111bbbb"}
    a = pd.DataFrame({"x": [1, 2], "run_code": "aaaa1111bbbb"})
    b = pd.DataFrame({"x": [3], "run_code": "aaaa1111bbbb"})
    check(qa.check_run_code({"a": a, "b": b}, man) == [],
          "artefactos de la misma pasada no producen fallos")

    c = pd.DataFrame({"x": [4], "run_code": "cccc2222dddd"})
    f = qa.check_run_code({"a": a, "c": c}, man)
    check(len(f) == 1 and "«c»" in f[0], "detecta un artefacto de otra pasada")

    d = pd.DataFrame({"x": [5]})
    f = qa.check_run_code({"a": a, "d": d}, man)
    check(len(f) == 1 and "no lleva" in f[0],
          "detecta un artefacto sin codigo de ejecucion")

    f = qa.check_run_code({"a": a}, {})
    check(len(f) == 1 and "no declara" in f[0],
          "un manifiesto sin codigo de ejecucion es un fallo")


# ---------------------------------------------------------------------------
# 10. Auditoria de un regimen completo
# ---------------------------------------------------------------------------
def _met_temporada(run: str) -> pd.DataFrame:
    """La tabla por temporada, con el juego de referencias cambiando de fila.

    Reproduce a escala la propiedad que hace que esta tabla necesite su propia
    comprobacion de cobertura: cada temporada se mide contra las referencias que
    tenian precio ESA temporada. Aqui la de validacion lleva las dos y la de
    produccion solo una —Pinnacle dejo de publicar precio—, de modo que la n no
    es comparable entre temporadas y si lo es dentro de cada una.
    """
    filas = [
        ("validacion", "2024/25", "modelo", 38),
        ("validacion", "2024/25", "consenso", 38),
        ("validacion", "2024/25", "pinnacle", 38),
        ("produccion", "2026/27", "modelo", 9),
        ("produccion", "2026/27", "consenso", 9),
    ]
    return pd.DataFrame(filas, columns=["bloque", "season", "contendiente",
                                        "n"]).assign(rps=0.20, run_code=run)


def _monta_regimen(base: Path, regimen: str = "semanal", *,
                   run: str = "1111aaaa2222", omitir=(),
                   conjunto: dict | None = None) -> Path:
    """Escribe en disco un regimen publicado minimo pero coherente.

    `conjunto` permite alterar el bloque del conjunto declarado para comprobar
    que la auditoria propaga lo que la comprobacion detecta.
    """
    d = base / regimen
    d.mkdir(parents=True, exist_ok=True)

    val = _pred(20, run=run)
    val.insert(0, "modelo", "logistica")
    met = pd.DataFrame({
        "modelo": ["logistica"] * 3,
        "contendiente": ["modelo", "consenso", "pinnacle"],
        "tipo": ["candidato", "referencia", "referencia"],
        "n": [20, 20, 20], "rps": [0.20, 0.21, 0.205], "run_code": run,
    })
    # La tabla del conjunto tiene que decir lo mismo que el manifiesto: una fila
    # por variable declarada.
    bloque = conjunto if conjunto is not None else _bloque_conjunto()
    cols = list(bloque.get("variables") or [])
    decl = pd.DataFrame({"orden": range(1, len(cols) + 1), "variable": cols,
                         "bloque": ["mercado"] * len(cols),
                         "presente": [True] * len(cols), "run_code": run})
    # La serie economica tiene que cerrar exactamente donde el manifiesto dice.
    jor = _eco_jornada()
    jor.insert(0, "run_code", run)
    tablas = {n: pd.DataFrame({"x": [1], "run_code": run}) for n in qa._TABLAS}
    tablas["predicciones_validacion"] = val
    tablas["metricas_modelos"] = met
    tablas["conjunto_declarado"] = decl
    tablas["economia_por_jornada"] = jor
    tablas["metricas_por_temporada"] = _met_temporada(run)
    for nombre, df in tablas.items():
        if nombre in omitir:
            continue
        df.to_parquet(d / f"{nombre}.parquet", index=False)

    (d / "manifiesto.json").write_text(json.dumps({
        "run_code": run, "regimen": regimen, "generado": "2026-06-01T10:00:00+00:00",
        "medidos": ["logistica"], "pliegues": 120,
        "publicado": {"modelo": "logistica", "rps": 0.20, "motivo": "menor rps"},
        "familia": "logistica", "hiperparametros": {"C": 1.0},
        "conjunto": bloque,
        "economia": _eco_manifiesto(),
        "limitaciones": ["el retorno economico es la metrica mas ruidosa"],
    }, ensure_ascii=False), encoding="utf-8")
    return d


def test_10_audita_regimen() -> None:
    print("\n10. auditoria de un regimen publicado")
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _monta_regimen(base)
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(f == [], f"un regimen coherente pasa limpio ({f[:2]})")

        check(qa.run_qa(["semanal"], base=base, verbose=False) is True,
              "run_qa devuelve True sobre un regimen coherente")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _monta_regimen(base, omitir=("fiabilidad",))
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("falta el artefacto" in x for x in f),
              "detecta un artefacto que no se ha publicado")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        d = _monta_regimen(base)
        met = pd.read_parquet(d / "metricas_modelos.parquet")
        met.loc[1, "n"] = 40
        met.to_parquet(d / "metricas_modelos.parquet", index=False)
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("distinto numero de partidos" in x.replace("ú", "u") for x in f),
              "propaga el fallo de cobertura a la auditoria del regimen")

    # La misma invariante dentro de la tabla por temporada. Se comprueba aparte
    # porque es el unico sitio donde el juego de referencias cambia de fila a
    # fila: la averia es una temporada en la que el modelo mide sobre mas
    # partidos que su referencia, que regalaria ventaja de cobertura mientras el
    # agregado sigue cuadrando.
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        d = _monta_regimen(base)
        t = pd.read_parquet(d / "metricas_por_temporada.parquet")
        t.loc[(t.season == "2026/27") & (t.contendiente == "modelo"), "n"] = 9
        t.loc[(t.season == "2026/27") & (t.contendiente == "consenso"), "n"] = 4
        t.to_parquet(d / "metricas_por_temporada.parquet", index=False)
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("2026/27" in x for x in f),
              "caza la ventaja de cobertura dentro de una sola temporada")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        d = _monta_regimen(base)
        val = pd.read_parquet(d / "predicciones_validacion.parquet")
        val.loc[0, "p_home"] = 0.9
        val.to_parquet(d / "predicciones_validacion.parquet", index=False)
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("no suma uno" in x for x in f),
              "propaga el fallo de probabilidades a la auditoria del regimen")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        d = _monta_regimen(base)
        hold = _pred(5, season="2025/26", run="1111aaaa2222")
        hold.to_parquet(d / "predicciones_holdout.parquet", index=False)
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("sin sello" in x for x in f),
              "una tabla de holdout sin sello no pasa la auditoria")

        (d / "holdout_sello.json").write_text(json.dumps(
            {"partidos": 5, "run_code": "1111aaaa2222", "modelo": "logistica",
             "generado": "2026-06-02T09:00:00+00:00"}), encoding="utf-8")
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(f == [], f"con el sello coherente, el holdout pasa ({f[:2]})")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        d = _monta_regimen(base)
        man = json.loads((d / "manifiesto.json").read_text(encoding="utf-8"))
        man["limitaciones"] = []
        (d / "manifiesto.json").write_text(json.dumps(man), encoding="utf-8")
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("no declara limitaciones" in x for x in f),
              "publicar sin limitaciones declaradas es un fallo duro")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        d = _monta_regimen(base)
        man = json.loads((d / "manifiesto.json").read_text(encoding="utf-8"))
        man["familia"] = "bosque"
        (d / "manifiesto.json").write_text(json.dumps(man), encoding="utf-8")
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("conjunto:" in x for x in f),
              "propaga el fallo de conjunto a la auditoria del regimen")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        d = _monta_regimen(base)
        man = json.loads((d / "manifiesto.json").read_text(encoding="utf-8"))
        del man["economia"]["cierre_avg"]["rendimiento_referencia"]
        (d / "manifiesto.json").write_text(json.dumps(man), encoding="utf-8")
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("liston" in x.replace("ó", "o") for x in f),
              "propaga a la auditoria un rendimiento publicado sin su liston")

    # La pasada de exploracion es legitima como estudio y no como publicacion:
    # el manifiesto la declara provisional y la auditoria tiene que negarse.
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _monta_regimen(base, conjunto=_bloque_conjunto(cerrado=False,
                                                       procedencia="sin cerrar"))
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(any("provisional" in x for x in f),
              "propaga a la auditoria un conjunto que todavia no se ha cerrado")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        f = qa.audita_regimen("semanal", base=base, verbose=False)
        check(len(f) == 1 and "no hay nada publicado" in f[0],
              "un regimen sin publicar se reporta sin lanzar")


# ---------------------------------------------------------------------------
# 11. Auditoria de la prediccion emitida
# ---------------------------------------------------------------------------
def test_11_audita_prediccion() -> None:
    print("\n11. auditoria de la prediccion emitida")
    hoy = pd.Timestamp("2026-05-01")
    pred = _pred(6, fecha="2026-05-03")
    cols = ["elo_diff", "forma_h", "forma_a"]
    f = qa.audita_prediccion(pred, _disputados([]), cols, list(cols), hoy=hoy)
    check(f == [], f"una emision correcta pasa limpia ({f[:2]})")

    roto = pred.copy()
    roto.loc[0, "p_home"] = float(roto.loc[0, "p_home"]) + 0.05   # terna rota
    roto.loc[1, "date"] = pd.Timestamp("2026-04-20")              # fecha pasada
    f = qa.audita_prediccion(roto, _disputados(["k003"]),
                             cols, [cols[1], cols[0], cols[2]], hoy=hoy)
    check(len(f) == 4, f"las cuatro averias se reportan por separado ({len(f)})")
    check(any("no suma uno" in x for x in f) and
          any("fecha anterior" in x for x in f) and
          any("ya disputados" in x for x in f) and
          any("distinto orden" in x for x in f),
          "cada averia se identifica con su propio mensaje")


# ---------------------------------------------------------------------------
# 12. Contrato de run_qa
# ---------------------------------------------------------------------------
def test_12_contrato() -> None:
    print("\n12. contrato de run_qa")
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _monta_regimen(base, omitir=("confusion",))
        try:
            qa.run_qa(["semanal"], base=base, verbose=False)
            check(False, "run_qa lanza PublicacionError cuando algo falla")
        except qa.PublicacionError as exc:
            check("confusion" in str(exc),
                  "run_qa lanza PublicacionError y nombra el artefacto ausente")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        try:
            qa.run_qa(base=base, verbose=False)
            check(False, "run_qa lanza si no hay ningun regimen publicado")
        except qa.PublicacionError:
            check(True, "run_qa lanza si no hay ningun regimen publicado")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        _monta_regimen(base, "semanal")
        _monta_regimen(base, "retrospectivo", run="3333cccc4444")
        check(qa.run_qa(base=base, verbose=False) is True,
              "run_qa descubre los regimenes publicados y los audita todos")


# ---------------------------------------------------------------------------
# 13. El contrato que el cuaderno escribe y el script cierra
# ---------------------------------------------------------------------------
def test_13_contrato_build_models() -> None:
    """Las cuatro declaraciones que separan explorar de publicar.

    `build_models` no busca nada: lee de su cabecera el conjunto, la familia y
    los valores que el cuaderno 05 dejo elegidos, y los cierra. Lo que se
    comprueba aqui es que esa cabecera no pueda quedarse a medias sin que se
    note: un conjunto vacio tiene que declararse provisional en el manifiesto y
    cerrar el paso a la reserva, porque una pasada de exploracion publicada como
    definitiva seria indistinguible de la buena.
    """
    print("\n13. el contrato de build_models separa explorar de publicar")
    import build_models as bm

    check(isinstance(bm.CONJUNTO, tuple),
          "el conjunto se declara como tupla, que no se modifica sin querer")
    check(bm.conjunto_cerrado() is bool(bm.CONJUNTO),
          "«cerrado» significa exactamente que hay conjunto escrito")
    check(bm.FAMILIA in lb.FAMILIAS,
          f"la familia declarada es una de las cuatro del estudio ({bm.FAMILIA})")

    hiper = bm.hiper_efectivo()
    check(all(k in hiper for k in lb.HIPER[bm.FAMILIA]),
          "los valores efectivos parten de los fijos de la familia")
    check(all(hiper[k] == v for k, v in bm.HIPER.items()),
          "y lo que se declara aparte los sobrescribe")

    if not bm.conjunto_cerrado():
        check(bm.PROCEDENCIA == "sin cerrar",
              "sin conjunto, la procedencia lo dice en lugar de inventarse una")
        feat = pd.DataFrame({c: [0.0] for c in
                             ("cierre_avg_p_home", "elo_diff", "forma_h")})
        try:
            bm.puntua_holdout(feat, "semanal", save=False, verbose=False)
            check(False, "la reserva no se abre con un conjunto provisional")
        except PermissionError:
            check(True, "la reserva no se abre con un conjunto provisional")
    else:
        check(bm.PROCEDENCIA and bm.PROCEDENCIA != "sin cerrar",
              "un conjunto cerrado dice de que seccion del cuaderno sale")
        check(len(set(bm.CONJUNTO)) == len(bm.CONJUNTO),
              "y no repite ninguna variable")

    # De donde sale el conjunto cuando la cabecera no lo trae escrito a mano. La
    # busqueda hacia delante deja su resultado en disco y el modulo lo recoge de
    # ahi, pero no a ciegas: un conjunto elegido bajo otro corte de temporadas o
    # para otra familia responde a otra pregunta, y publicarlo bajo este manifiesto
    # afirmaria del modelo algo que no se ha medido sin que ninguna cifra lo delate.
    with tempfile.TemporaryDirectory() as tmp:
        ruta = Path(tmp) / "conjunto.json"
        base = {"conjunto": ["mkt_p_home", "elo_diff"],
                "familia": bm.FAMILIA,
                "corte_seleccion": str(bm.PROTOCOLO.corte_seleccion)}
        ruta.write_text(json.dumps(base), encoding="utf-8")
        cols, proc = bm.de_la_seleccion(ruta=ruta, verbose=False)
        check(cols == ("mkt_p_home", "elo_diff"),
              "una seleccion guardada con el corte y la familia de aqui se recoge")
        check(proc.startswith("medicion") and proc.endswith("2v"),
              "y su procedencia nombra al ensayo que la midio, no a un rotulo suelto")

        ruta.write_text(json.dumps({**base, "corte_seleccion": "1999/00"}),
                        encoding="utf-8")
        check(bm.de_la_seleccion(ruta=ruta, verbose=False) == ((), ""),
              "una elegida con otro corte se ignora en vez de publicarse")

        ruta.write_text(json.dumps({**base, "familia": "_inexistente"}),
                        encoding="utf-8")
        check(bm.de_la_seleccion(ruta=ruta, verbose=False) == ((), ""),
              "y una elegida para otra familia, tambien")

        check(bm.de_la_seleccion(ruta=Path(tmp) / "no_esta.json",
                                 verbose=False) == ((), ""),
              "sin ninguna seleccion guardada no falla: la fase corre provisional")


# ---------------------------------------------------------------------------
# 14. Las dos poblaciones de las tablas descriptivas
# ---------------------------------------------------------------------------
def test_14_descriptivas_no_mezclan() -> None:
    """Validacion y temporada en curso conviven en los artefactos, separadas.

    El reparto no es un detalle de presentacion. La temporada en curso se puntua
    con el holdout dentro de la ventana de entrenamiento, asi que una cifra de
    CONJUNTO que la incluya ya no es la respuesta a la pregunta del trabajo; y
    el fallo no se ve, porque no rompe ninguna columna ni cambia el numero de
    filas: solo desplaza el agregado unas decimas en la direccion favorable.

    La regla, entonces, tiene dos mitades y las dos importan. Lo que se desglosa
    por temporada lleva las dos poblaciones, rotuladas —la temporada en curso es
    una de las tres cifras que el proyecto necesita y tiene que publicarse—; lo
    que produce una sola cifra lleva solo validacion.
    """
    print("\n14. las tablas descriptivas separan validacion de temporada en curso")
    import build_models as bm
    import columns as C

    def _con_objetivo(df: pd.DataFrame, bloque: str) -> pd.DataFrame:
        # Las tres clases ciclicas: lo que se mide aqui es el reparto de filas
        # entre bloques, no la calidad del pronostico, y un objetivo degenerado
        # dejaria sin filas la tabla de confusion por motivos ajenos al reparto.
        return df.assign(**{C.TARGET_NUM: np.arange(len(df)) % 3,
                            bm.BLOQUE: bloque})

    mezcla = pd.concat(
        [_con_objetivo(_pred(30, season="2024/25", fecha="2025-03-01"),
                       "validacion"),
         _con_objetivo(_pred(10, season="2026/27", fecha="2026-10-01"),
                       "produccion")],
        ignore_index=True)

    agreg = bm._agregable(mezcla)
    check(set(agreg[bm.BLOQUE]) == {"validacion"},
          "lo agregable es validacion y nada mas")
    check(len(agreg) == 30, "y son todas sus filas, no una muestra")
    check(len(bm._agregable(mezcla.drop(columns=[bm.BLOQUE]))) == len(mezcla),
          "sin columna de bloque no hay nada que separar y se devuelve todo")

    # Las dos mitades de la regla, sobre la misma tabla de entrada.
    fiab = bm._fiabilidad_por_temporada(mezcla)
    total = fiab[fiab[C.SEASON] == "total"]
    check(not total.empty, "la fila de conjunto se sigue publicando")
    check(int(total["n"].sum()) == int(
        fiab[fiab[C.SEASON] == "2024/25"]["n"].sum()),
        "y cuenta los mismos partidos que validacion, no los cuarenta")
    check("2026/27" in set(fiab[C.SEASON]),
          "la temporada en curso si aparece desglosada, que es como se publica")

    conf = bm._confusion_por_temporada(mezcla)
    check(int(conf[conf[C.SEASON] == "total"]["n"].sum())
          == int(conf[conf[C.SEASON] == "2024/25"]["n"].sum()),
          "la confusion reparte igual: total es validacion, el curso va aparte")

    # Y el rotulo viaja. Agrupar por temporada ya separa las dos poblaciones
    # —ninguna temporada esta en las dos—, pero quien lee la tabla ve una serie
    # seguida y no tiene como saber que la ultima fila se midio de otra manera.
    pt_ = bm._por_temporada_con_mercado(mezcla)
    if not pt_.empty:
        check(bm.BLOQUE in pt_.columns,
              "cada fila por temporada dice de que bloque sale")
        porbloque = dict(pt_.groupby(bm.BLOQUE)[C.SEASON].agg(set))
        check(porbloque.get("produccion", set()) == {"2026/27"}
              and porbloque.get("validacion", set()) == {"2024/25"},
              "y el rotulo coincide con la temporada, no se reparte al azar")

    # La economia se apila rotulada. Es la tabla con la que `check_economia`
    # cuadra el cierre de la serie contra el manifiesto, y sin rotulo cuadraria
    # contra la ultima jornada jugada: fallaria cada semana.
    fake = {"cierre_avg": {"casa": "b365",
                           "por_jornada": pd.DataFrame({"jornada": [1, 2],
                                                        "x": [0.1, 0.2]})}}
    apilada = bm._apila_economia(fake, fake, "por_jornada")
    check(set(apilada[bm.BLOQUE]) == {"validacion", "produccion"},
          "las dos economias viajan juntas y rotuladas")
    check(len(apilada) == 4, "y ninguna de las dos se pierde al apilar")
    check(list(bm._apila_economia({}, {}, "por_jornada").columns)
          == ["cuotas", "casa", bm.BLOQUE],
          "sin economia que apilar el esquema se mantiene, no se devuelve vacio")


# ---------------------------------------------------------------------------
def _run() -> None:
    print("=" * 72)
    print("BANCO qa_models.py")
    print("=" * 72)
    test_1_probabilidades()
    test_2_jornada_futura()
    test_3_orden_columnas()
    test_4_regimen()
    test_5_sello_holdout()
    test_5b_produccion()
    test_6_cobertura()
    test_7_conjunto()
    test_8_economia()
    test_9_run_code()
    test_10_audita_regimen()
    test_11_audita_prediccion()
    test_12_contrato()
    test_13_contrato_build_models()
    test_14_descriptivas_no_mezclan()
    print("\n" + "=" * 72)
    print(f"RESULTADO: {len(PASS)} OK, {len(FAIL)} fallos")
    if FAIL:
        for m in FAIL:
            print(f"  - {m}")
    print("=" * 72)


def test_qa_models() -> None:
    """Entrada para pytest."""
    _run()
    assert not FAIL, FAIL


if __name__ == "__main__":
    _run()
    sys.exit(1 if FAIL else 0)
