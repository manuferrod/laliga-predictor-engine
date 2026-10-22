"""El mejor conjunto por ROI y acierto sobre el censo de nueve, global y por temporada.

Es lo que pidió Manuel el 21-oct-2026, literalmente: «con el censo que he decidido
ahora encuentres el mejor modelo en términos de roi y accuracy, quiero que encuentres
el modelo con las variables que maximice esas métricas tanto en cómputo global como a
nivel de temporada». El censo es el de nueve temporadas que acaba de quedar fijado:
2017/18–2024/25 y 2026/27, **3118 partidos en 345 pliegues**, sin 2025/26 (regla 7) y
sin 2016/17, que es el arranque.

La segunda mitad de la petición —«a nivel de temporada»— es lo que este módulo añade
al proyecto, y conviene decir por qué no es la misma pregunta que la primera. Pedir el
conjunto que maximiza el agregado es pedir UN número; pedir el que maximiza cada
temporada es pedir NUEVE, y nueve máximos sobre 380 partidos cada uno no son nueve
confirmaciones: son nueve sorteos. El error típico del ROI plano sobre 380 partidos es
**5.0 puntos porcentuales** —10.93 sobre los 78 de la temporada en curso, 2.08 sobre
los 2280 de validación, y escala con la raíz—, de modo que el mejor de K conjuntos en
una temporada suelta puede ganar varios puntos sin que debajo haya nada. Por eso aquí
el máximo por temporada no se entrega solo: se entrega con la medida de cuánto de él
es ruido, que es lo que permite leerlo.

Tres lecturas, en este orden y sin mezclarse:

**1. El acervo, puntuado entero.** Todos los conjuntos que el proyecto ha producido en
once rutas de búsqueda —la semilla de precio, el publicado de 62, las 65 juntas, los
siete dominios, los seis `semilla+topN`, los ganadores de `_maximo.py` y `_censo.py`,
los doce pasos del haz y los dieciocho rodantes— se puntúan sobre los MISMOS 3118
partidos, temporada a temporada, con acierto, RPS y ROI plano a tres precios con su
listón al lado (regla 3). Es la tabla que contesta a la petición tal como está escrita,
y es la primera del proyecto que pone todas las rutas en la misma cuadrícula.

**2. Si el máximo por temporada se repite.** La pregunta que decide si «maximizar por
temporada» es un objetivo o un espejismo, y se contesta gratis con el libro de la
lectura 1: para cada temporada, qué conjunto gana, y qué hace ESE conjunto en las otras
ocho. Si los nueve máximos los firman nueve conjuntos distintos y ninguno repite, el
máximo por temporada es el máximo de una nube y no una propiedad del conjunto.

**3. Dejar una temporada fuera.** La versión honesta de la pregunta, y la única cuya
cifra es citable: para cada temporada S, el conjunto que maximiza las OTRAS ocho se
elige sin ver S y se puntúa en S. No cuesta un solo ajuste más —se lee del mismo libro
partido a partido— y es lo que distingue «este conjunto es mejor» de «este conjunto fue
mejor ahí». Si el conjunto elegido con ocho temporadas no bate al listón en la novena,
no hay conjunto que elegir y la respuesta a la directriz es que el mejor modelo es el
que ya se publica.

**Y la voraz sobre el censo entero** (`--dentro`), que es la petición en su forma más
literal: buscar guiado por acierto y por ventaja con los 3118 partidos dentro del
bloque de elección. Va aparte y al final a propósito, porque elige y mide sobre los
mismos partidos y por tanto **su cifra no es citable** —es lo que dejó incitable a
`_maximo.py`, con el detalle en su docstring—. Se ejecuta igualmente: la directriz pide
el máximo, el máximo se calcula, y al lado va cuánto vale.

Lo que este módulo NO vuelve a medir, porque ya está medido y sigue valiendo:

  · `_rodante.py` puntuó cuatro pistas sobre este mismo censo y **ni un intervalo
    excluye el cero**, siendo el mayor el del modelo publicado, +1.52 pp [−0.19, +3.31]
    de ventaja sobre el favorito de Bet365 en los 3118. Las cuatro pistas coinciden en
    el signo en más del 95 % de los partidos.
  · el precio vale más que el conjunto: la misma pista liquidada a la mejor cuota entre
    casas en vez de a Bet365 gana **4.5 puntos de ROI**, contra 1.5 del mejor conjunto
    de once rutas. Por eso aquí cada fila trae los tres precios y no uno.

**Ejecutado (21-oct-2026): 50 conjuntos distintos sobre los 345 pliegues, 24.3 min, y
el veredicto es que no hay conjunto que adoptar.** Las tres lecturas dicen lo mismo con
tres fuerzas distintas, y la tercera es la única citable.

**Lectura 1, el agregado.** El máximo lo firma `maximo_roi` (k=10): acierto **54.11 %**,
RPS 0.195439, ROI **+1.35 %** y ventaja **+4.22 pp [+1.72, +6.77]** sobre el listón
—que en los 3118 va a −2.87 % y 53.11 %—. Ese intervalo excluye el cero y **no es
citable por eso**: `maximo_roi` se buscó sobre partidos que están dentro de estos 3118,
así que la cifra es la de `_maximo.py` y arrastra su defecto, no lo repara. El mejor
acierto global es de `rod_ve_202425` (k=7) con **54.14 %**. El publicado de 62 queda en
53.50 % y **+1.52 pp [−0.19, +3.31]**, y la semilla de precio en 53.08 % y −0.01 pp.

**El precio de meter el banco entero se ve en el acierto y en el RPS, y NO en la ventaja**,
que es un detalle que conviene no confundir. `todas65` cae a **50.58 %** de acierto y
0.202736 de RPS, `juego59` a **50.03 %** y 0.204590 —tres puntos y medio de acierto por
debajo del publicado— y `escalera+bajas` (k=58) a 50.71 %. Pero sus ventajas sobre el
listón siguen saliendo **positivas** (+1.66, +0.25 y +0.85 pp), y la peor de las 50 es
`semilla+fuerza` con solo **−0.93 pp**. El ROI plano sobre 3118 partidos tiene un error
típico de 1.79 pp y **no distingue un modelo que acierta el 50 % de uno que acierta el
53.5 %**: el daño está medido, pero quien lo mide es el acierto, no el dinero. Es la razón
por la que estas tablas llevan las tres columnas y no la que pidió la directriz sola.

**Y el censo LIMPIO no limpia lo que se buscó, aunque se llame así.** Son las seis
temporadas que no eligieron el peso de la mezcla, de modo que es censo limpio para
`publicado` (+1.18 pp [−0.84, +3.33]) y para `semilla` (−0.19 pp), y **no lo es para
ningún conjunto de búsqueda**: `maximo_roi` se buscó sobre 2019/20–2024/25 y pisa tres de
las seis, y `rod_ve_202425` —que ahí firma el máximo con **+3.61 pp [+1.35, +5.97]**,
intervalo que excluye el cero— se eligió con el pasado de 2024/25, que contiene cuatro de
las seis. Ese +3.61 **no es una cifra de fuera de muestra** y no se puede citar como si lo
fuera. Las únicas del cuadro que lo son son las dos del censo limpio para el publicado y
la semilla, y las de la lectura 3.

**Lectura 2, y es la que contesta la segunda mitad de la petición.** Las nueve
temporadas las firman **nueve conjuntos distintos, en los dos criterios, y ninguno
repite**. Lo que cada ganador hace en las otras ocho lo dice su puesto medio de 50:
**12.2** el mejor caso (`maximo_roi`, que gana 2022/23) y **40.6** el peor
(`escalera+clasificacion`, que gana 2023/24 con .5605 y promedia .5110 fuera). Nueve
máximos sobre 380 partidos con un error típico de 5.0 pp son nueve sorteos, y el reparto
lo confirma: **no existe «el conjunto que maximiza cada temporada»**, existe el conjunto
que ganó ahí.

**Lectura 3, dejar una fuera, la única citable.** Elegido con ocho temporadas y puntuado
en la novena: por acierto salen 4 conjuntos distintos y el agregado es **53.30 % contra
53.11 %** del listón, ventaja **+1.46 pp [−0.42, +3.34]**; por ventaja salen 3 y el
agregado es **53.11 % contra 53.11 %** —el mismo acierto, al decimal— con **+1.45 pp
[−0.84, +3.69]**. **Ninguno de los dos intervalos excluye el cero, y los dos quedan por
debajo del +1.52 pp del modelo publicado.** Los vaivenes por vuelta dicen de dónde
viene: +8.41 pp en 2019/20 contra −4.55 pp en 2026/27.

**Y la voraz sobre el censo entero (`--dentro`) para en la tolerancia, no en el techo**,
a **k=5 por los dos criterios**. Por acierto adopta `home_playstyle` y
`h2h_draw_rate_ewm` sobre la semilla y se detiene porque la mejor candidata del paso 3
(`form_ga_ewm_diff`) mejoraba **+0.000000** contra una tolerancia de 0.00153093; por
ventaja adopta `is_weekend` y `form_opp_elo_z_ewm_diff` y se detiene con +0.003211
contra 0.00416413. Con los 3118 partidos dentro del bloque de elección —el máximo de
información que el proyecto puede darle— la búsqueda se agota en dos variables.

**La respuesta a la directriz, entonces, es que el mejor modelo en ROI y acierto sigue
siendo la mezcla que ya se publica**, y no porque no se haya buscado: once rutas, 50
conjuntos y tres lecturas después, lo único que supera al publicado se mide donde se
eligió. Lo que este módulo añade al trabajo no es un modelo: es la primera medida de
cuánto del máximo por temporada es ruido, y la respuesta es casi todo.

Uso:
    python _nueve.py --acervo     # lectura 1: el acervo sobre los 3118, por temporada
    python _nueve.py --lee        # lecturas 2 y 3, del libro ya escrito
    python _nueve.py --dentro     # la voraz sobre el censo entero (no citable)
"""
from __future__ import annotations

import os

# Un hilo por proceso, antes de importar numpy. Con reparto entre procesos, cada hijo
# que abra su propio pool de BLAS compite con los demás por los mismos núcleos.
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

import argparse                                              # noqa: E402
import json                                                  # noqa: E402
import sys                                                   # noqa: E402
import time                                                  # noqa: E402
from pathlib import Path                                     # noqa: E402

import numpy as np                                           # noqa: E402
import pandas as pd                                          # noqa: E402

_RAIZ = Path(__file__).resolve().parent
for _sub in (".", "scripts/models", "scripts/features"):
    if str(_RAIZ / _sub) not in sys.path:
        sys.path.insert(0, str(_RAIZ / _sub))

# Planos, nunca por el paquete: `scripts.models.laboratorio` y `laboratorio` son dos
# objetos de módulo con dos cachés de la matriz, y `seleccion._lote` decide si puede
# repartir entre procesos preguntando `feat is lb._feat_cache`. Por la ruta del paquete
# la guarda cae a la serie sin avisar y la búsqueda tarda tres veces más (medido: 42.2 s
# contra 29.8 s un paso de doce candidatas). El detalle está en CLAUDE.md.
import _rodante as rd                                        # noqa: E402
import build_models as bm                                    # noqa: E402
import laboratorio as lb                                     # noqa: E402
import protocol as pt                                        # noqa: E402
import seleccion as sl                                       # noqa: E402

# ===========================================================================
# 1. Las decisiones, declaradas antes de buscar
# ===========================================================================
# El censo, la liquidación y el listón se heredan de `_rodante` y no se redeclaran.
# Es la condición para que las dos tablas se puedan leer juntas: si aquí se cambiase
# el precio, el listón o la política, las filas de este módulo y las de aquel medirían
# cosas distintas con los mismos nombres de columna, que es la forma más fácil de
# publicar una comparación falsa.
ORIGENES = rd.ORIGENES
EN_CURSO = rd.EN_CURSO
SUCIAS_PESO = rd.SUCIAS_PESO
CASA = rd.CASA
PRECIOS = rd.PRECIOS
FAMILIA = rd.FAMILIA
HIPER = rd.HIPER

# Los dominios que `_bloques.py` probó enteros, en el orden de mérito que midió:
# fuerza 0.841 -> forma 0.679 -> clasificacion 0.525 -> historial 0.317 -> fatiga
# 0.143 -> bajas 0.011 -> contexto -0.025 (fracción del hueco entre la previa de
# frecuencias y la semilla que recorre cada dominio solo, sin una columna de precio).
# El orden se declara aquí porque la escalera acumulativa de la lectura 1 lo recorre,
# y recorrerlo en otro orden mediría otra cosa.
DOMINIOS_ORDEN: tuple[str, ...] = ("fuerza", "forma", "clasificacion",
                                   "historial", "fatiga", "bajas", "contexto")

# Cuántas de las mejores por mérito propio se prueban con la semilla delante. Son las
# de `_mejores.py`, que ordenó las 59 de juego una a una SIN el precio dentro —el
# orden con el precio delante está dominado por lo que el precio ya sabe— y luego
# probó los prefijos. Se repiten aquí porque allí se midieron sobre 1140 partidos del
# bloque de selección y aquí se miden sobre los 3118 del censo entero.
TOPS: tuple[int, ...] = (3, 5, 8, 12, 20, 30)

# Las rutas las declara `seleccion.py`, igual que las de las once rutas que llenan el
# acervo; aquí solo se les pone nombre corto.
OUT = sl.NUEVE
ACERVO = sl.NUEVE_ACERVO
PRED = sl.NUEVE_PRED
TEMPORADA = sl.NUEVE_TEMPORADA
LECTURA = sl.NUEVE_LECTURA
DENTRO = sl.NUEVE_DENTRO


# ===========================================================================
# 2. El acervo: todos los conjuntos que el proyecto ha producido
# ===========================================================================
def _lee_json(ruta: Path) -> dict | None:
    """Lee un artefacto de una ruta anterior, o `None` si no se ejecutó."""
    if not ruta.exists():
        return None
    return json.loads(ruta.read_text(encoding="utf-8"))


def acervo(feat: pd.DataFrame | None = None, *,
           verbose: bool = True) -> dict[str, dict]:
    """Los conjuntos a puntuar, leídos del disco y del catálogo.

    Se leen de los artefactos de las rutas anteriores en vez de copiarse aquí a mano
    por una razón de método: una lista pegada en este fichero sería una **tercera**
    copia de la verdad —la primera es el `.json` que escribió la búsqueda, la segunda
    el cuadro de su docstring— y las tres se desincronizarían en cuanto una búsqueda
    se repitiese. Lo que no está en disco se omite nombrándolo, que es la diferencia
    entre un acervo incompleto y uno que miente sobre su tamaño.

    Cada entrada declara su familia y su penalización, no solo sus columnas: el
    publicado es la mezcla logit con su peso, y puntuarlo como una logística plana
    mediría otro modelo con el nombre del publicado.
    """
    feat = lb.datos() if feat is None else feat
    banco = sl.candidatas(feat, regime="semanal")
    mapa = lb.dominios(banco)
    base = list(sl.SEMILLA_MERCADO)
    # «Sin precio» es sin el dominio `mercado` ENTERO, que son seis columnas y no las
    # tres de la semilla: `ah_line`, `mkt_overround_open` y `mkt_spread_mean` también
    # son el precio dicho de otra manera, y dejarlas dentro llamaría «juego» a un
    # conjunto que sigue mirando la pizarra de la casa. Son 59 y son exactamente las
    # que midió `_sinprecio.py`.
    juego = [c for c in banco if c not in set(mapa.get("mercado", []))]
    sal: dict[str, dict] = {}

    def _pon(nombre: str, cols, *, familia: str = FAMILIA,
             hiper: dict | None = None, origen: str = "") -> None:
        cols = [c for c in dict.fromkeys(map(str, cols)) if c in set(banco)]
        if not cols:
            return
        clave = "|".join(sorted(cols)) + f"#{familia}"
        for n, d in sal.items():
            if d["clave"] == clave:
                d["origen"] += f" = {origen or nombre}"
                return
        # La penalización por omisión es la de la logística y **solo** la de la
        # logística. Pasarle `HIPER` a la mezcla sería puntuar otro modelo con el
        # nombre del publicado: `bm.HIPER` está vacío a propósito, porque la mezcla
        # lleva su peso en la configuración declarada de `laboratorio` y no en una
        # `C` que su estimador no usa.
        if not hiper:
            hiper = dict(HIPER) if familia == FAMILIA else None
        sal[nombre] = {"conjunto": cols, "familia": familia,
                       "hiper": dict(hiper) if hiper else None,
                       "origen": origen or nombre, "clave": clave}

    # Las dos referencias obligadas. La semilla es el techo declarado de antemano de
    # las once rutas; el publicado es lo que hay que batir para que cambiar algo tenga
    # sentido, y va con SU familia y SU peso.
    _pon("semilla", base, origen="sl.SEMILLA_MERCADO, 3 de precio")
    _pon("publicado", list(bm.CONJUNTO), familia=bm.FAMILIA,
         hiper=bm.HIPER or None, origen="bm.CONJUNTO, mezcla logit w=0.85")
    _pon("todas65", banco, origen="el banco entero")
    _pon("juego59", juego, origen="las 59 de juego, sin el dominio del precio")

    # Los siete dominios, cada uno con la semilla delante y luego acumulados. Los dos
    # recorridos miden cosas distintas y los dos están en `_bloques.py`: el primero
    # pregunta qué añade un dominio al precio, el segundo si la suma de todos ayuda.
    for dom in DOMINIOS_ORDEN:
        _pon(f"semilla+{dom}", base + mapa.get(dom, []),
             origen=f"_bloques.py, dominio {dom} entero")
    # El último peldaño de la escalera son **las mismas 62 columnas que `bm.CONJUNTO`**
    # —comprobado por igualdad de conjuntos: la semilla más los siete dominios es
    # exactamente lo que se publica, y lo que el banco tiene de más son las tres
    # columnas de mercado que no son probabilidad (`ah_line`, `mkt_overround_open`,
    # `mkt_spread_mean`)—. No es una coincidencia que convenga deshacer: deja en el
    # acervo el par que **aísla lo que aporta la mezcla**, porque `publicado` y
    # `escalera+contexto` tienen idénticas columnas y solo se distinguen en la familia.
    # Es la única comparación del proyecto que separa el conjunto del combinador.
    acum = list(base)
    for dom in DOMINIOS_ORDEN:
        acum = acum + mapa.get(dom, [])
        nota = (" = las 62 de bm.CONJUNTO, pero como logística plana"
                if set(acum) == set(bm.CONJUNTO) else "")
        _pon(f"escalera+{dom}", acum,
             origen=f"_bloques.py, escalera acumulativa hasta {dom}{nota}")

    # Las mejores por mérito propio, con la semilla delante. El orden lo escribió
    # `_mejores.py` midiendo cada columna SOLA, sin el precio dentro.
    orden = sl.MEJORES_ORDEN
    if orden.exists():
        rank = pd.read_csv(orden).sort_values("puesto")["variable"].tolist()
        for n in TOPS:
            _pon(f"semilla+top{n}", base + rank[:n],
                 origen=f"_mejores.py, las {n} mejores por mérito propio")
    elif verbose:
        print(f"aviso: no está {orden.name}; el acervo va sin los semilla+topN",
              flush=True)

    # Los ganadores de las dos rutas guiadas por acierto y ROI. Son los que la regla 5
    # dejó incitables por elegir y medir en los mismos partidos; aquí se puntúan sobre
    # un censo que incluye 760 partidos que ninguno de los dos vio, que es la lectura
    # que les faltaba.
    for fich, etiq in (("maximo_ganadores.json", "_maximo.py"),
                       ("censo_ganadores.json", "_censo.py")):
        d = _lee_json(sl.OUT / fich)
        if d is None:
            if verbose:
                print(f"aviso: no está {fich}; el acervo va sin sus ganadores",
                      flush=True)
            continue
        for cual in ("ganador_acierto", "ganador_roi"):
            g = d.get(cual) or {}
            if g.get("conjunto"):
                _pon(f"{etiq.strip('_.py')}_{cual.split('_')[1]}",
                     g["conjunto"], hiper=g.get("hiper"),
                     origen=f"{etiq}, ganador por {cual.split('_')[1]}")

    # Los doce pasos del haz, que es la ruta que mejor RPS alcanzó dentro del bloque.
    haz = sl.OUT / "haz.csv"
    if haz.exists():
        h = pd.read_csv(haz)
        for _, f in h.iterrows():
            _pon(f"haz_k{int(f['k'])}", str(f["conjunto"]).split(),
                 origen=f"_haz.py, el mejor de k={int(f['k'])}")

    # Los dieciocho rodantes, cada uno como conjunto FIJO. No es lo mismo que la pista
    # rodante de `_rodante.py`: allí cada temporada se predice con el conjunto elegido
    # con su propio pasado, y aquí el conjunto de un origen se aplica a las nueve. La
    # diferencia entre las dos lecturas es exactamente lo que mide la lectura 2.
    for crit in rd.CRITERIOS:
        for s in ORIGENES:
            d = _lee_json(rd._ruta_eleccion(crit, s))
            if d and d.get("conjunto"):
                _pon(f"rod_{crit[:2]}_{rd._slug(s)}", d["conjunto"],
                     origen=f"_rodante.py, elegido por {crit} con el pasado de {s}")

    for d in sal.values():
        d.pop("clave", None)
        d["k"] = len(d["conjunto"])
    if verbose:
        print(f"=== acervo: {len(sal)} conjuntos distintos, de {len(banco)} "
              f"candidatas ===", flush=True)
    return sal


# ===========================================================================
# 3. Lectura 1: el acervo sobre los mismos 3118 partidos
# ===========================================================================
def puntua(pool: dict[str, dict] | None = None, *,
           verbose: bool = True) -> pd.DataFrame:
    """Puntúa cada conjunto del acervo sobre el censo de nueve, temporada a temporada.

    Una sola `cfg` y una sola caché de estados de preprocesado para todos. El corte de
    selección no interviene: no parte los pliegues de desarrollo ni los de producción,
    solo la máscara evaluable, y aquí se evalúa TODO. Por eso vale cualquiera y se
    declara el del origen en curso, para que la elección esté a la vista.

    El libro partido a partido se guarda entero, y no solo el resumen, porque las
    lecturas 2 y 3 son comparaciones EMPAREJADAS: la ventaja de un conjunto sobre otro
    calculada desde dos resúmenes no es la misma cifra que la diferencia partido a
    partido, y la que vale es la segunda.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    feat = lb.datos()
    pool = acervo(feat, verbose=verbose) if pool is None else pool

    cfg = rd.cfg_de(EN_CURSO)
    dev = [f for f in pt.make_folds(feat, cfg, evaluar="validacion")
           if str(f.season) in set(ORIGENES)]
    prod = pt.make_folds(feat, cfg, evaluar="produccion")
    folds = dev + prod
    cache = pt._StateCache(feat, cfg)
    if verbose:
        print(f"=== puntuación: {len(pool)} conjuntos x {len(folds)} pliegues "
              f"({len(dev)} de desarrollo + {len(prod)} de producción) ===",
              flush=True)

    liqs: dict[str, pd.DataFrame] = {}
    t00 = time.time()
    for i, (nombre, d) in enumerate(pool.items(), 1):
        t0 = time.time()
        fab = lb.fabrica(d["familia"], d["conjunto"], d["hiper"] or None)
        pred = pt.run_protocol(feat, fab, cfg, folds=folds, cache=cache)
        # `attrs` lleva el `ProtocolConfig` y los estados ajustados, que pandas
        # intenta volcar a los metadatos del parquet como JSON y no sabe serializar.
        pred.attrs.clear()
        liqs[nombre] = rd.liquida(pred, feat)
        if verbose:
            print(f"  [{i:>2}/{len(pool)}] {nombre:<22} k={d['k']:>2} "
                  f"{len(pred):>5} partidos en {time.time() - t0:>5.1f} s",
                  flush=True)
    if verbose:
        print(f"=== {len(pool)} conjuntos en {(time.time() - t00) / 60:.1f} min ===",
              flush=True)

    # El listón es del precio y no del modelo, así que sus columnas `*_ref_*` tienen
    # que ser idénticas en todas las filas. Comprobarlo no es ceremonia: si no lo
    # fuesen, la ventaja de dos conjuntos se estaría midiendo sobre dos censos
    # distintos, que es el fallo que la invariante 6 de `qa_models` existe para
    # impedir un escalón más arriba.
    ref = liqs["semilla"]
    for nombre, liq in liqs.items():
        if not np.allclose(liq[f"ben_ref_{CASA}"].to_numpy(dtype=float),
                           ref[f"ben_ref_{CASA}"].to_numpy(dtype=float),
                           equal_nan=True):
            raise AssertionError(
                f"el listón de {nombre} no coincide con el de la semilla: todos "
                f"los conjuntos tienen que liquidarse sobre los mismos partidos")

    largo = pd.concat([liq.assign(conjunto=n) for n, liq in liqs.items()],
                      ignore_index=True)
    largo.to_parquet(PRED, index=False)

    filas = []
    for nombre, liq in liqs.items():
        d = pool[nombre]
        comun = {"conjunto": nombre, "k": d["k"], "familia": d["familia"],
                 "origen": d["origen"]}
        filas.append({**comun, "temporada": "TODAS",
                      **rd.resume(liq, con_intervalo=True)})
        for s, g in liq.groupby(liq["season"].astype(str)):
            filas.append({**comun, "temporada": s,
                          **rd.resume(g, con_intervalo=False)})
        limpio = liq[~liq["season"].astype(str).isin(SUCIAS_PESO)]
        filas.append({**comun, "temporada": "LIMPIO",
                      **rd.resume(limpio, con_intervalo=True)})
    tabla = pd.DataFrame(filas)
    tabla.to_csv(ACERVO, index=False, encoding="utf-8")
    (OUT / "nueve_acervo_pool.json").write_text(
        json.dumps(pool, ensure_ascii=False, indent=2), encoding="utf-8")

    por = tabla.loc[tabla["temporada"].isin(ORIGENES)]
    por.to_csv(TEMPORADA, index=False, encoding="utf-8")
    if verbose:
        informa_acervo(tabla)
    return tabla


# ===========================================================================
# 4. Lecturas 2 y 3: si el máximo por temporada vale algo
# ===========================================================================
def _libro() -> pd.DataFrame:
    """El libro partido a partido de la lectura 1, con el beneficio y el listón."""
    if not PRED.exists():
        raise FileNotFoundError(
            f"no está {PRED.name}: ejecuta primero `python _nueve.py --acervo`")
    return pd.read_parquet(PRED)


def _metricas(g: pd.DataFrame) -> dict:
    """Acierto, ventaja a los tres precios y RPS de un trozo del libro."""
    d = {"n": int(len(g)), "acierto": float(g["ok"].mean()),
         "rps": float(g["rps"].mean())}
    for nom in PRECIOS:
        d[f"roi_{nom}"] = float(g[f"ben_{nom}"].mean())
        d[f"vent_{nom}"] = float((g[f"ben_{nom}"] - g[f"ben_ref_{nom}"]).mean())
    d["acierto_ref"] = float(g[f"ok_ref_{CASA}"].mean())
    return d


def _mejor(tabla: pd.DataFrame, criterio: str) -> str:
    """El conjunto que maximiza `criterio`. Desempata por el otro, nunca al azar.

    El desempate importa y no es un detalle: sobre 380 partidos el acierto se mueve a
    saltos de 0.263 puntos porcentuales, de modo que dos conjuntos empatan a menudo, y
    un desempate ciego —el primero que aparezca— metería el orden del diccionario
    dentro del resultado.
    """
    otro = "vent_" + CASA if criterio == "acierto" else "acierto"
    d = tabla.sort_values([criterio, otro], ascending=False)
    return str(d.iloc[0]["conjunto"])


def lee(*, verbose: bool = True) -> dict:
    """Las lecturas 2 y 3, del libro ya escrito. No ajusta un solo modelo más."""
    libro = _libro()
    nombres = libro["conjunto"].unique().tolist()
    K = len(nombres)

    # --- global: el máximo del acervo, con su inflación declarada ---------------
    glob = pd.DataFrame([{"conjunto": n, **_metricas(g)}
                         for n, g in libro.groupby("conjunto")])
    res: dict = {"n_conjuntos": K, "n_partidos": int(len(libro) // K),
                 "global": {c: _mejor(glob, c)
                            for c in ("acierto", f"vent_{CASA}", "vent_maxima")}}

    # --- lectura 2: el máximo por temporada, y si repite -----------------------
    porsea = pd.DataFrame([{"conjunto": n, "temporada": str(s), **_metricas(g)}
                           for (n, s), g in libro.groupby(
                               ["conjunto", libro["season"].astype(str)])])
    gana: dict[str, dict] = {}
    for crit in ("acierto", f"vent_{CASA}"):
        quien = {s: _mejor(porsea.loc[porsea["temporada"] == s], crit)
                 for s in ORIGENES}
        cuenta = pd.Series(list(quien.values())).value_counts()
        # Qué hace el ganador de una temporada en las OTRAS ocho. Es la pregunta que
        # separa «este conjunto es mejor» de «este conjunto fue mejor ahí».
        fuera = {}
        for s, n in quien.items():
            otras = porsea.loc[(porsea["conjunto"] == n)
                               & (porsea["temporada"] != s)]
            fuera[s] = {"conjunto": n,
                        "en_la_suya": float(
                            porsea.loc[(porsea["conjunto"] == n)
                                       & (porsea["temporada"] == s),
                                       crit].iloc[0]),
                        "media_otras": float(otras[crit].mean()),
                        "puesto_medio_otras": float(
                            porsea.loc[porsea["temporada"] != s]
                            .groupby("temporada")[crit]
                            .rank(ascending=False, method="min")
                            [porsea.loc[porsea["temporada"] != s][
                                "conjunto"] == n].mean())}
        gana[crit] = {"por_temporada": quien,
                      "conjuntos_distintos": int(len(cuenta)),
                      "repite_mas": cuenta.head(3).to_dict(),
                      "fuera": fuera}
    res["por_temporada"] = gana

    # --- lectura 3: dejar una temporada fuera ----------------------------------
    # Para cada temporada S, el conjunto que maximiza las otras ocho se elige SIN ver
    # S y se puntúa en S. Es la única de las tres lecturas cuya cifra es citable, y es
    # la que contesta la directriz: si lo elegido con ocho no bate al listón en la
    # novena, no hay conjunto que elegir.
    dejar: dict[str, dict] = {}
    for crit in ("acierto", f"vent_{CASA}"):
        filas = []
        for s in ORIGENES:
            otras = libro.loc[libro["season"].astype(str) != s]
            tab = pd.DataFrame([{"conjunto": n, **_metricas(g)}
                                for n, g in otras.groupby("conjunto")])
            elegido = _mejor(tab, crit)
            en_s = libro.loc[(libro["conjunto"] == elegido)
                             & (libro["season"].astype(str) == s)]
            filas.append({"temporada": s, "elegido": elegido,
                          **_metricas(en_s)})
        d = pd.DataFrame(filas)
        # El agregado del procedimiento, partido a partido y no promediando nueve
        # medias: las temporadas tienen 380 partidos y la en curso 78, así que la
        # media de medias le daría a 2026/27 el mismo peso que a una entera.
        trozos = [libro.loc[(libro["conjunto"] == f["elegido"])
                            & (libro["season"].astype(str) == f["temporada"])]
                  for _, f in d.iterrows()]
        junto = pd.concat(trozos, ignore_index=True)
        bloque = (junto["season"].astype(str) + "|"
                  + junto["jornada"].astype(str)).to_numpy()
        agr = _metricas(junto)
        for nom in PRECIOS:
            lo, hi = rd._boot(
                (junto[f"ben_{nom}"] - junto[f"ben_ref_{nom}"]).to_numpy(
                    dtype=float), bloque)
            agr[f"vent_{nom}_inf"], agr[f"vent_{nom}_sup"] = lo, hi
        dejar[crit] = {"por_temporada": d.to_dict("records"),
                       "agregado": agr,
                       "conjuntos_distintos": int(d["elegido"].nunique())}
    res["dejar_una_fuera"] = dejar

    LECTURA.write_text(json.dumps(res, ensure_ascii=False, indent=2,
                                  default=float), encoding="utf-8")
    if verbose:
        informa_lectura(res, glob, porsea)
    return res


# ===========================================================================
# 5. La voraz sobre el censo entero: el máximo literal, y no citable
# ===========================================================================
def dentro(*, verbose: bool = True) -> dict:
    """El máximo literal sobre el censo entero, que **ya estaba calculado**.

    Esto empezó siendo una búsqueda nueva de cinco horas y no lo es, y el motivo
    merece quedar escrito porque es la clase de duplicado que un proyecto con once
    rutas de búsqueda produce sin querer. La voraz guiada por acierto y por ventaja
    **con los 3118 partidos dentro del bloque de elección** es, exactamente, el origen
    `2026/27` de `_rodante.py`: su `corte_seleccion` deja como bloque de selección todo
    lo anterior a la temporada en curso, y eso son los **3040 partidos de validación**
    —lo dice su propia tabla, `n_bloque = 3040` en la fila de 2026/27—. Volver a
    lanzarla reproduciría dos ficheros que ya están en disco.

    No se puede pedir de otra manera, además, y eso también conviene saberlo:
    `seleccion.adelante` llama a `_guarda_bloque(evaluar, BLOQUE_SELECCION, …)`, de
    modo que **se niega a buscar sobre `validacion`**. La única forma legítima de
    ensanchar el bloque de elección es mover el corte, que es lo que hace `cfg_de`. La
    guarda es la regla 6 escrita en código y hace lo que debe.

    Los dos conjuntos que salen son de **cinco variables** cada uno, la semilla de
    precio más dos, y no comparten las dos:

      · por acierto  -> `home_playstyle`, `h2h_draw_rate_ewm`
      · por ventaja  -> `is_weekend`, `form_opp_elo_z_ewm_diff`

    Y se paran por tolerancia, no por el tope: con 3040 partidos la tolerancia del
    acierto baja a 0.00153 y la de la ventaja a 0.00416, y la mejor candidata del paso
    cuatro no llega. **Eso no es el máximo que pidió la directriz**, que sería el de la
    adopción forzada; ese lo midió `_maximo.py` con un haz doble hasta k=10 y da 55.00 %
    de acierto y +4.56 % de ROI sobre 2280 partidos, cifras que su propia docstring
    declara incitables por elegir y medir en los mismos partidos. Los dos ganadores de
    aquella ruta y los dos de `_censo.py` **viajan en el acervo de la lectura 1**, de
    modo que esta vez se puntúan sobre un censo que incluye 760 partidos que ninguno de
    los cuatro vio: eso es la medida del optimismo, y es gratis.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    todo: dict[str, dict] = {}
    for crit in rd.CRITERIOS:
        d = _lee_json(rd._ruta_eleccion(crit, EN_CURSO))
        if d is None:
            raise FileNotFoundError(
                f"no está la elección de {crit} para el origen {EN_CURSO}: es la "
                f"voraz sobre el censo entero y la ejecuta `python _rodante.py "
                f"--elige`")
        todo[crit] = {k: v for k, v in d.items() if k != "traza"}
    DENTRO.write_text(json.dumps(todo, ensure_ascii=False, indent=2),
                      encoding="utf-8")
    if verbose:
        print("\n" + "=" * 100)
        print("EL MÁXIMO DE DENTRO: la voraz con los 3040 partidos en el bloque "
              "de elección")
        print("=" * 100)
        for crit, d in todo.items():
            print(f"  por {crit:<9} k={d.get('n_variables', '?'):<3} "
                  f"bloque={d.get('n_bloque', '?')} -> "
                  f"{' '.join(d.get('conjunto', []))}")
            print(f"    parada: {d.get('motivo', '')}")
        print("  Su cifra NO es citable: elige y mide sobre los mismos partidos.")
        print(f"  Lo que la acota es la lectura 3 de `{LECTURA.name}`.")
    return todo


# ===========================================================================
# 6. Lo que se enseña por pantalla
# ===========================================================================
def informa_acervo(tabla: pd.DataFrame, *, tope: int = 14) -> None:
    """El acervo ordenado por ventaja, con el listón arriba y la semilla marcada."""
    d = tabla.loc[tabla["temporada"] == "TODAS"].copy()
    d = d.sort_values(f"vent_{CASA}", ascending=False)
    print("\n" + "=" * 100)
    print(f"EL ACERVO SOBRE LOS MISMOS {int(d['n'].iloc[0])} PARTIDOS "
          f"(17/18-26/27), ORDENADO POR VENTAJA")
    print("=" * 100)
    print(f"{'conjunto':<22} {'k':>3} {'acierto':>8} {'RPS':>9} "
          f"{'ROI b365':>9} {'ROI max':>9} {'ventaja b365':>24}")
    lis = d.iloc[0]
    print(f"{'liston':<22} {'—':>3} {lis['acierto_ref']:>7.2%} {'—':>9} "
          f"{lis[f'roi_ref_{CASA}']:>8.2%} {lis['roi_ref_maxima']:>8.2%}")
    for _, f in d.head(tope).iterrows():
        print(f"{f['conjunto']:<22} {int(f['k']):>3} {f['acierto']:>7.2%} "
              f"{f['rps']:>9.6f} {f[f'roi_{CASA}']:>8.2%} "
              f"{f['roi_maxima']:>8.2%}  {f[f'vent_{CASA}']:>+7.2%} "
              f"[{f[f'vent_{CASA}_inf']:>+.2%}, {f[f'vent_{CASA}_sup']:>+.2%}]")
    if len(d) > tope:
        print(f"  ... y {len(d) - tope} más en {ACERVO.name}")


def informa_lectura(res: dict, glob: pd.DataFrame,
                    porsea: pd.DataFrame) -> None:
    """Las tres lecturas, por pantalla y en el orden en que hay que leerlas."""
    crits = ("acierto", f"vent_{CASA}")
    print("\n" + "=" * 100)
    print(f"1. EL MÁXIMO GLOBAL DEL ACERVO ({res['n_conjuntos']} conjuntos sobre "
          f"{res['n_partidos']} partidos)")
    print("=" * 100)
    for c in crits:
        n = res["global"][c]
        f = glob.loc[glob["conjunto"] == n].iloc[0]
        print(f"  por {c:<12} -> {n:<22} acierto {f['acierto']:.2%} · "
              f"ventaja {f['vent_' + CASA]:+.2%} · RPS {f['rps']:.6f}")

    print("\n" + "=" * 100)
    print("2. EL MÁXIMO POR TEMPORADA: ¿repite el que gana?")
    print("=" * 100)
    for c in crits:
        g = res["por_temporada"][c]
        print(f"\n  por {c}: {g['conjuntos_distintos']} conjuntos distintos "
              f"firman las {len(ORIGENES)} temporadas")
        print(f"    los que más repiten: {g['repite_mas']}")
        print(f"    {'temporada':<10} {'gana':<22} {'en la suya':>11} "
              f"{'media otras':>12} {'puesto medio':>13}")
        for s, d in g["fuera"].items():
            print(f"    {s:<10} {d['conjunto']:<22} "
                  f"{d['en_la_suya']:>+11.4f} {d['media_otras']:>+12.4f} "
                  f"{d['puesto_medio_otras']:>13.1f}")

    print("\n" + "=" * 100)
    print("3. DEJAR UNA TEMPORADA FUERA: elegir con ocho, puntuar en la novena")
    print("=" * 100)
    for c in crits:
        d = res["dejar_una_fuera"][c]
        a = d["agregado"]
        print(f"\n  por {c}: {d['conjuntos_distintos']} conjuntos distintos "
              f"elegidos en las nueve vueltas")
        print(f"    {'temporada':<10} {'elegido':<22} {'acierto':>8} "
              f"{'listón':>8} {'ventaja b365':>13}")
        for f in d["por_temporada"]:
            print(f"    {f['temporada']:<10} {f['elegido']:<22} "
                  f"{f['acierto']:>7.2%} {f['acierto_ref']:>7.2%} "
                  f"{f['vent_' + CASA]:>+12.2%}")
        print(f"    {'AGREGADO':<10} {'(el procedimiento)':<22} "
              f"{a['acierto']:>7.2%} {a['acierto_ref']:>7.2%} "
              f"{a['vent_' + CASA]:>+12.2%} "
              f"[{a['vent_' + CASA + '_inf']:>+.2%}, "
              f"{a['vent_' + CASA + '_sup']:>+.2%}]")


# ===========================================================================
def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--acervo", action="store_true",
                   help="lectura 1: puntúa el acervo sobre los 3118, por temporada")
    p.add_argument("--lee", action="store_true",
                   help="lecturas 2 y 3, del libro ya escrito")
    p.add_argument("--dentro", action="store_true",
                   help="la voraz sobre el censo entero (su cifra no es citable)")
    a = p.parse_args(argv)
    if not (a.acervo or a.lee or a.dentro):
        a.acervo = a.lee = True
    if a.acervo:
        puntua()
    if a.lee:
        lee()
    if a.dentro:
        dentro()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
