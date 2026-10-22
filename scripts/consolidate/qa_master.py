"""QA del master consolidado: invariantes duras + cobertura por bloque.

Construye el master en memoria (build_master) y comprueba:
  1. 1 fila por partido y `match_key` unico.
  2. sin fan-out: nº de filas == nº de partidos del spine.
  3. target `ftr` completo y en {H, D, A}.
  4. cobertura por temporada de cada bloque de fuentes (xG, Elo, sofascore,
     fatiga, disponibilidad, valor plantilla, bajas €, bajas por rol, rotacion,
     calidad de ocasion).
  5. ninguna columna 100 % nula, salvo los bloques declarados opcionales.
  6. sin arrastre de dato viejo: el Elo vigente de un partido no precede al
     partido anterior de ese equipo, y los bloques cubren la COLA del maestro
     --las ultimas jornadas-- y no solo la temporada en promedio.
  7. continuidad del empalme de ClubElo: el salto de fuerza tipificada que
     acusan los equipos al cruzar la union de las dos escalas de la fuente cabe
     dentro de lo que mueve una semana corriente de competicion.

Las dos averias que (6) persigue son las unicas de esta lista que no dan la cara.
Las demas rompen algo visible; estas publican una jornada entera con el dato de
la semana pasada. El Elo entra por `merge_asof`, asi que si ClubElo deja de
publicar la columna sigue llena y el numero sigue siendo plausible. El resto de
bloques entra por `match_key`, asi que un extractor que no corrio deja vacias
solo las filas nuevas, que es exactamente donde un promedio por temporada no las
ve.

Uso:
    python scripts/consolidate/qa_master.py
Sale con codigo !=0 si alguna invariante dura falla (apto para CI).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from build_master import build_master, _load  # noqa: E402
from spine import ELO_CORTE, build_spine  # noqa: E402

# Bloques que el master publica siempre: si falta alguno, la consolidacion esta
# rota y el QA debe fallar.
_BLOCKS = {
    "xg": ["home_xg", "away_xg"],
    "elo": ["home_elo_z", "away_elo_z"],
    "sofa_rating": ["home_sofa_rating", "away_sofa_rating"],
    "fatiga": ["home_fat_rest_days", "away_fat_rest_days"],
    "disponibilidad": ["home_n_missing", "away_n_missing"],
    "valor_plantilla": ["home_squad_value", "away_squad_value"],
    "bajas_euros": ["home_out_value", "away_out_value"],
}

# Bloques que nacen de un volcado propio, de una peticion por partido: las
# alineaciones y los minutos (`extractors/lineups.py`) y el detalle de tiro
# (`extractors/understat_shots.py`). Son reconstrucciones largas y separadas, y el
# master se construye igual sin ellas, con estos bloques enteros en NaN. Se
# declaran aparte para que el QA distinga un "hueco conocido y asumido" de una
# columna que deberia tener dato y no lo tiene: la primera situacion se reporta,
# la segunda invalida el master.
_BLOCKS_OPCIONALES = {
    "bajas_rol": ["home_out_minshare", "away_out_minshare"],
    "rotacion": ["home_xi_continuity", "away_xi_continuity"],
    "calidad_ocasion": ["home_shot_xg_mean", "away_shot_xg_mean"],
}

# Marcas de las columnas que dependen de esos mismos volcados, para la
# invariante 5.
_MARCAS_OPCIONALES = ("out_minshare", "xi_continuity", "starters_minutes",
                      "minutes_hhi", "n_players_used", "_shot_")

# Suelo de cobertura exigible a cada bloque. Baja donde el hueco es estructural
# y esta explicado: fuentes que arrancan mas tarde que el historico; el valor de
# plantilla, que se retrasa a proposito a la temporada s-1 para no mirar hacia
# adelante y por tanto no existe para el recien ascendido, que no jugo esa
# temporada; los dos bloques de alineaciones, que dejan NaN en las primeras
# jornadas de cada equipo porque aun no hay pasado del que derivar la rotacion ni
# la cuota; y la calidad de ocasion, que no existe para el equipo cuyos unicos
# remates de un partido fueron penaltis o goles en propia puerta --el agregado
# excluye ambos por construccion y prefiere no publicar fila a publicar un
# promedio calculado sobre lo que no es juego abierto--.
#
# El suelo del valor de plantilla --y el de las bajas en euros, que lo necesitan
# para convertir ausencias en dinero-- se fija POR DEBAJO de su techo
# estructural: tres ascendidos de veinte clubes no tienen valor de la temporada
# s-1, asi que la cobertura maxima alcanzable es 17/20 = 85 % exacto. Un suelo
# del 85 % no deja holgura y lo que mide es el redondeo: basta con que los
# ascendidos de una temporada en curso jueguen un par de partidos entre ellos
# --o ninguno-- para cruzar la linea sin que nada vaya mal. El 83 % deja dos
# puntos de margen y sigue denunciando lo que importa: que falte el valor de un
# club que SI jugo en Primera la temporada anterior.
_SUELO = {"sofa_rating": 0.85, "disponibilidad": 0.85, "bajas_euros": 0.83,
          "valor_plantilla": 0.83, "rotacion": 0.85, "bajas_rol": 0.80,
          "calidad_ocasion": 0.90}
_SUELO_DEFECTO = 0.98

# Cuantos partidos mira la invariante 6b. Tres jornadas: suficiente para que el
# porcentaje signifique algo y corto para que una jornada entera sin dato pese un
# tercio y no un 3 % diluido en la temporada.
_COLA = 30

# Tope absoluto del salto de fuerza tipificada en el empalme de ClubElo
# (invariante 7), en desviaciones. El presupuesto principal es RELATIVO --el
# empalme no puede moverse mas que el extremo de una semana corriente de
# competicion, medido sobre el mismo maestro-- porque asi sigue siendo valido
# cuando el historico crezca. Este tope esta encima como suelo de cordura: media
# desviacion es la mitad de la distancia que separa al equipo medio del cuarto
# clasificado, y un salto asi no lo produce una semana de futbol por mucho que la
# cola de la distribucion se ensanche.
_SALTO_TOPE = 0.50

# Techo de la proporcion de filas equipo-partido a las que el rating pegado le
# falta UN partido (invariante 6). ClubElo publica un tramo por partido, pero no
# con la regularidad de un reloj: sobre el maestro completo, 16 de 7600 filas
# --un 0.21 %-- heredan un rating que no incorpora el ultimo partido del club, y
# ninguna se salta dos. Este techo es cinco veces esa tasa: deja pasar el salto de
# refresco aislado, que es ruido de la fuente, y se queda muy por debajo de una
# degradacion sistematica. Faltar DOS partidos no tiene techo: aborta siempre.
_SALTADOS_TECHO = 0.01


def _fail(msg: str, errors: list[str]) -> None:
    print("  [FAIL] " + msg)
    errors.append(msg)


def _dias(s: pd.Series) -> pd.Series:
    """Columna de fechas en texto -> datetime a dia, sin hora."""
    return pd.to_datetime(s.astype("string").str.slice(0, 10), errors="coerce")


def elo_pegado(m: pd.DataFrame, elo: pd.DataFrame) -> pd.DataFrame:
    """Una fila por (equipo, partido) con la FECHA del Elo que se le pego.

    Repite el `merge_asof` de `spine.add_clubelo` quedandose con la fecha del
    tramo en lugar del valor, porque el valor es justo lo que no delata el
    arrastre: un Elo de hace tres meses es un numero tan plausible como el de
    ayer. Trae `_previo`, la fecha del partido anterior de ese mismo equipo, y
    dos lecturas del desfase:

    `_saltados` cuenta cuantos partidos de ese club quedan ENTRE la fecha del
    rating y el partido que se esta prediciendo: es el numero de resultados que al
    rating le faltan, y es la unidad en la que se mide esta invariante. El desfase
    en dias no sirve de liston --tiene maximos legitimos de tres meses por el paron
    de verano, y cualquier tolerancia que los admita deja pasar tambien un mes de
    fuente caida en plena temporada-- mientras que en partidos el paron de verano
    vale cero, igual que una semana corriente.

    `_arrastre` es `_saltados >= 1`, el caso que hay que mirar; cuanto hay que
    tolerar de el lo decide `veredicto_arrastre`.
    """
    e = elo.copy()
    e["_elo_d"] = _dias(e["date"])
    e = e.dropna(subset=["_elo_d"])
    e["_d"] = e["_elo_d"]

    sp = m[["match_key", "season", "home_key", "away_key", "date"]].copy()
    sp["_d"] = _dias(sp["date"])
    largo = pd.concat([sp.assign(team_key=sp["home_key"]),
                       sp.assign(team_key=sp["away_key"])], ignore_index=True)
    largo = (largo[["match_key", "season", "team_key", "_d"]]
             .dropna(subset=["_d"]).sort_values(["team_key", "_d"]))
    largo["_previo"] = largo.groupby("team_key")["_d"].shift(1)

    pegado = pd.merge_asof(largo.sort_values("_d"),
                           e[["_d", "team_key", "_elo_d"]].sort_values("_d"),
                           on="_d", by="team_key", direction="backward")

    # Partidos del propio club en el intervalo abierto (fecha del rating, fecha
    # del partido). Se cuentan por busqueda binaria sobre el calendario ya
    # ordenado de cada equipo, no partido a partido: el maestro trae 7600 filas
    # equipo-partido y el bucle anidado las recorreria enteras por cada una.
    # Las fechas viajan como entero de nanosegundos: `searchsorted` sobre
    # `datetime64` degrada a objeto y compara Timestamp con int.
    def _ns(s):
        return s.to_numpy("datetime64[ns]").astype("int64")
    cal = {k: np.sort(_ns(g["_d"])) for k, g in largo.groupby("team_key")}
    pegado["_saltados"] = 0
    for k, g in pegado.groupby("team_key"):
        f = cal.get(k)
        if f is None:
            continue
        n = (np.searchsorted(f, _ns(g["_d"]), "left")
             - np.searchsorted(f, _ns(g["_elo_d"]), "right"))
        pegado.loc[g.index, "_saltados"] = np.where(g["_elo_d"].isna(), 0, n)
    pegado["_arrastre"] = pegado["_saltados"] >= 1
    return pegado


def veredicto_arrastre(pegado: pd.DataFrame) -> tuple[str | None, dict]:
    """Decide si el desfase del Elo pegado es un salto de refresco o una averia.

    Recibe el largo de `elo_pegado` y responde con el motivo del fallo --o `None`
    si pasa-- y las cifras del desfase, para imprimirlas aunque no falle.

    La invariante existe para detectar que la fuente dejo de publicar y que
    `merge_asof` esta repitiendo el ultimo tramo que vio. Lo que distingue esa
    averia de un salto de refresco aislado no es el tamano del desfase en dias
    sino cuantos resultados le faltan al rating:

      1. a alguna fila le faltan DOS partidos o mas -> la fuente lleva al menos
         dos jornadas sin publicar para ese club. Aborta, sin techo que lo tolere.
      2. a mas de `_SALTADOS_TECHO` de las filas le falta UNO -> ya no es un salto
         de refresco, es una degradacion sistematica del tramo que se publica.

    Que falte un partido en una fila suelta no se puede evitar desde aqui: ClubElo
    publica un tramo por partido pero no siempre lo refresca a tiempo, y el dato
    que llega entonces es el rating de la vispera del partido anterior, que sigue
    siendo informacion pre-partido legitima --solo algo mas pobre--. Se cuenta y se
    informa; no se corrige inventando un valor que la fuente no dio.
    """
    if not len(pegado):
        return None, {}
    con_elo = pegado[pegado["_elo_d"].notna()]
    if not len(con_elo):
        return "ninguna fila equipo-partido recibio Elo", {}
    graves = con_elo[con_elo["_saltados"] >= 2]
    uno = con_elo[con_elo["_saltados"] == 1]
    desfase = (con_elo["_d"] - con_elo["_elo_d"]).dt.days
    mt = {"n": int(len(con_elo)), "n_uno": int(len(uno)),
          "n_graves": int(len(graves)),
          "share_uno": float(len(uno) / len(con_elo)),
          "techo": _SALTADOS_TECHO,
          "p50_d": float(desfase.median()), "p99_d": float(desfase.quantile(0.99))}
    if len(graves):
        peor = graves.nlargest(3, "_saltados")
        muestra = ", ".join(
            f"{r['team_key']} {r['_d'].date()} (Elo de {r['_elo_d'].date()}, "
            f"{int(r['_saltados'])} partidos suyos sin recoger)"
            for _, r in peor.iterrows())
        return (f"{len(graves)} filas equipo-partido con un Elo que se salta dos "
                f"partidos o mas del propio club: {muestra}. La fuente dejo de "
                f"publicar y el merge esta repitiendo el ultimo tramo que vio"), mt
    if mt["share_uno"] > _SALTADOS_TECHO:
        return (f"a {mt['share_uno']:.2%} de las filas le falta un partido "
                f"({mt['n_uno']} de {mt['n']}), por encima del techo declarado "
                f"({_SALTADOS_TECHO:.0%}): el retraso del refresco de ClubElo ha "
                f"dejado de ser un caso aislado"), mt
    return None, mt


def salto_elo_z(m: pd.DataFrame, *, corte: str = ELO_CORTE) -> pd.DataFrame:
    """Variacion de fuerza tipificada entre partidos consecutivos de un equipo.

    Devuelve el largo equipo-partido con `_salto` --el valor absoluto del cambio
    de `elo_z` respecto al partido ANTERIOR del mismo equipo en la MISMA
    temporada-- y `_cruza`, que marca el unico par de partidos que atraviesa el
    corte del empalme de ClubElo.

    El emparejamiento se hace dentro de la temporada y no a lo largo de todo el
    historico porque, de lo contrario, el recien ascendido contamina la medida: su
    partido anterior en el maestro es de dos temporadas atras, con otra plantilla y
    --sobre todo-- con otra base de tipificacion, de modo que su salto mezcla el
    empalme con un cambio de division. Medido sin esa separacion aparecen 25
    "cruces" y los tres peores son Leganes, Las Palmas y Alaves volviendo de
    Segunda; con ella quedan exactamente los veinte clubes de Primera de 2022/23,
    que son los que de verdad cruzan la union.
    """
    cols = [c for c in ("home_elo_z", "away_elo_z") if c in m.columns]
    if len(cols) < 2:
        return pd.DataFrame(columns=["team_key", "season", "_d", "elo_z",
                                     "_salto", "_cruza"])
    sp = m[["match_key", "season", "home_key", "away_key", "date",
            "home_elo_z", "away_elo_z"]].copy()
    sp["_d"] = _dias(sp["date"])
    largo = pd.concat([
        sp.rename(columns={"home_key": "team_key", "home_elo_z": "elo_z"})
          [["match_key", "season", "team_key", "_d", "elo_z"]],
        sp.rename(columns={"away_key": "team_key", "away_elo_z": "elo_z"})
          [["match_key", "season", "team_key", "_d", "elo_z"]],
    ], ignore_index=True)
    largo = largo.dropna(subset=["_d", "elo_z"])
    largo = largo.sort_values(["season", "team_key", "_d"])

    g = largo.groupby(["season", "team_key"], sort=False)
    largo["_previo_d"] = g["_d"].shift(1)
    largo["_salto"] = (largo["elo_z"] - g["elo_z"].shift(1)).abs()
    c = pd.Timestamp(corte)
    largo["_cruza"] = (largo["_previo_d"] < c) & (largo["_d"] >= c)
    return largo.dropna(subset=["_salto"]).reset_index(drop=True)


def veredicto_empalme(salto: pd.DataFrame) -> tuple[str | None, dict]:
    """Decide si el empalme de ClubElo cabe dentro de una semana de competicion.

    Recibe el largo que devuelve `salto_elo_z` y responde con el motivo del fallo
    --o `None` si pasa-- y las metricas de las dos distribuciones que se comparan,
    para que la invariante 7 las imprima aunque no falle: son las cifras que la
    memoria cita, y una comprobacion que solo dice "OK" no se puede citar.

    El liston principal es RELATIVO y sale del propio maestro: el salto del
    empalme no puede pasar del extremo de una semana corriente. Se escribe asi, y
    no como una constante, para que siga siendo valido cuando el historico crezca
    y la cola de la distribucion se ensanche. Se mira en tres pasos, de la averia
    mas grave a la mas sutil:

      1. el PEOR salto del empalme supera el peor de una semana normal -> el
         cambio de escala esta llegando al modelo como si fuera futbol;
      2. cabe en esa cola pero pasa de `_SALTO_TOPE` -> suelo de cordura, por si
         el historico se ensancha tanto que el liston relativo deja de apretar;
      3. el salto TIPICO del empalme pasa del p90 de una semana normal -> no hay
         un caso extremo, hay un desplazamiento sistematico de los veinte clubes
         a la vez, que es exactamente la firma de un cambio de escala.
    """
    cruces, normal = salto[salto["_cruza"]], salto[~salto["_cruza"]]
    if not len(cruces):
        return None, {}
    if not len(normal):
        return "sin semanas corrientes con las que comparar el salto del empalme", {}

    mt = {
        "n_cruces": int(len(cruces)), "n_normal": int(len(normal)),
        "p50_c": float(cruces["_salto"].median()),
        "p90_c": float(cruces["_salto"].quantile(0.90)),
        "max_c": float(cruces["_salto"].max()),
        "p50_n": float(normal["_salto"].median()),
        "p90_n": float(normal["_salto"].quantile(0.90)),
        "p99_n": float(normal["_salto"].quantile(0.99)),
        "max_n": float(normal["_salto"].max()),
    }
    if mt["max_c"] > mt["max_n"]:
        peor = cruces.nlargest(1, "_salto").iloc[0]
        return (f"el salto del empalme ({mt['max_c']:.3f} desviaciones, "
                f"{peor['team_key']} el {peor['_d'].date()}) supera el extremo de "
                f"una semana corriente ({mt['max_n']:.3f}): el cambio de escala de "
                f"ClubElo esta llegando al modelo como si fuera futbol"), mt
    if mt["max_c"] > _SALTO_TOPE:
        return (f"el salto del empalme ({mt['max_c']:.3f}) supera el tope "
                f"declarado ({_SALTO_TOPE:.2f} desviaciones) aunque quepa en la "
                f"cola de una semana normal: revisa el corte {ELO_CORTE}"), mt
    if mt["p50_c"] > mt["p90_n"]:
        return (f"el salto TIPICO del empalme ({mt['p50_c']:.3f}) esta por encima "
                f"del p90 de una semana corriente ({mt['p90_n']:.3f}): no es un "
                f"extremo aislado sino un desplazamiento sistematico de los veinte "
                f"clubes a la vez"), mt
    return None, mt


def cobertura_cola(m: pd.DataFrame, bloques: dict[str, list[str]],
                   n: int = _COLA) -> tuple[pd.DataFrame, dict[str, float]]:
    """Cobertura de cada bloque sobre los ultimos `n` partidos del maestro.

    Mirar la cola y no la temporada es lo que distingue "a esta fuente le falta
    algo disperso del historico" de "esta fuente no ha traido la jornada que
    estamos a punto de publicar".
    """
    cola = m.assign(_d=_dias(m["date"])).sort_values("_d").tail(n)
    cobertura = {}
    for name, cols in bloques.items():
        present = [c for c in cols if c in cola.columns]
        if present:
            cobertura[name] = 1 - cola[present].isna().mean().mean()
    return cola, cobertura


def run_qa() -> int:
    data = _load()
    m = build_master(data)
    spine = build_spine(data["fd"], data["us"], data["elo"])
    errors: list[str] = []

    print("=" * 72)
    print("QA MASTER —", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 72)
    print(f"partidos={len(m)}  columnas={m.shape[1]}")

    # 1) unicidad
    print("\n[1] 1 fila por partido / match_key unico")
    if not m["match_key"].is_unique:
        _fail("match_key duplicado", errors)
    else:
        print("  OK: match_key unico")

    # 2) sin fan-out
    print("\n[2] sin fan-out en los merges")
    if len(m) != len(spine):
        _fail(f"filas master ({len(m)}) != partidos spine ({len(spine)})", errors)
    else:
        print(f"  OK: {len(m)} filas == {len(spine)} partidos del spine")

    # 3) target
    print("\n[3] target ftr completo y valido")
    if m["ftr"].isna().any():
        _fail(f"ftr con {int(m['ftr'].isna().sum())} nulos", errors)
    elif not set(m["ftr"].dropna().unique()) <= {"H", "D", "A"}:
        _fail(f"ftr con valores raros: {set(m['ftr'].unique())}", errors)
    else:
        dist = m["ftr"].value_counts(normalize=True).to_dict()
        print("  OK: ftr en {H,D,A} | dist:",
              {k: round(v, 3) for k, v in dist.items()})

    # 4) cobertura por temporada
    print("\n[4] cobertura por temporada (no-nulos, media de las 2 columnas del bloque)")
    seasons = sorted(m["season"].dropna().unique())

    # Un bloque opcional cuenta como presente cuando alguna de sus columnas trae
    # dato: la mera existencia de la columna no basta, porque las bajas por rol
    # se publican siempre --con NaN de punta a punta si no hay minutos volcados--.
    vivos = {n: c for n, c in _BLOCKS_OPCIONALES.items()
             if any(x in m.columns and not m[x].isna().all() for x in c)}
    _VOLCADO = {"bajas_rol": "alineaciones y minutos",
                "rotacion": "alineaciones y minutos",
                "calidad_ocasion": "detalle de tiro de Understat"}
    for n in _BLOCKS_OPCIONALES:
        if n not in vivos:
            print(f"  (bloque '{n}' sin dato: falta el volcado de "
                  f"{_VOLCADO[n]}; el master es valido igualmente)")
    bloques = {**_BLOCKS, **vivos}

    print("  season    " + "".join(f"{b[:8]:>10}" for b in bloques))
    for s in seasons:
        sub = m[m["season"] == s]
        cells = []
        for cols in bloques.values():
            present = [c for c in cols if c in sub.columns]
            cov = 1 - sub[present].isna().mean().mean() if present else float("nan")
            cells.append(f"{cov:>9.0%} ")
        print(f"  {str(s):<9}" + "".join(cells))

    # 4b) cobertura por bloque sobre su rango REAL (algunos empiezan mas tarde:
    #     disponibilidad/bajas arrancan en 17/18, no en 16/17).
    print("\n[4b] cobertura por bloque sobre su rango real (auto-detecta inicio)")
    for name, cols in bloques.items():
        present = [c for c in cols if c in m.columns]
        if not present:
            _fail(f"bloque '{name}' sin columnas ({cols})", errors)
            continue
        per_season = {s: 1 - m.loc[m["season"] == s, present].isna().mean().mean()
                      for s in seasons}
        active = [s for s in seasons if per_season[s] > 0]
        start = active[0] if active else None
        cov_full = 1 - m[present].isna().mean().mean()
        cov_active = np.mean([per_season[s] for s in active]) if active else float("nan")
        note = "" if (start == seasons[0]) else f"  <- empieza en {start}"
        print(f"  {name:16s}: rango={cov_active:6.1%}  global={cov_full:6.1%}{note}")
        floor = _SUELO.get(name, _SUELO_DEFECTO)
        if not np.isnan(cov_active) and cov_active < floor:
            _fail(f"bloque '{name}' cobertura {cov_active:.1%} < {floor:.0%} "
                  f"en su rango activo", errors)

    # 5) columnas 100% nulas
    print("\n[5] columnas completamente nulas")
    allnull = [c for c in m.columns if m[c].isna().all()]
    # Una columna vacia es normalmente un merge que no ha casado. La excepcion
    # son las que dependen de un volcado propio --alineaciones y minutos, detalle
    # de tiro--: ahi el vacio esta declarado de antemano y el preprocesador lo
    # marca con su flag de ausencia.
    asumidas = [c for c in allnull if any(t in c for t in _MARCAS_OPCIONALES)]
    inesperadas = [c for c in allnull if c not in asumidas]
    if inesperadas:
        _fail(f"{len(inesperadas)} columnas 100% nulas: {inesperadas[:8]}", errors)
    elif asumidas:
        print(f"  OK: las {len(asumidas)} columnas vacias pertenecen a bloques "
              "de volcado propio, pendientes de ejecutar")
    else:
        print("  OK: ninguna columna 100% nula")

    # 6) arrastre de dato viejo: cuantos resultados del propio club le faltan al
    #    Elo que se le pego. Es el fallo semanal que ninguna otra invariante ve,
    #    porque el valor arrastrado es tan plausible como el bueno: si ClubElo
    #    deja de publicar, `merge_asof` repite el ultimo tramo y la columna sigue
    #    llena. Se mide en partidos y no en dias --ver `elo_pegado`-- y el
    #    veredicto lo da `veredicto_arrastre`.
    print("\n[6] arrastre de dato viejo (Elo)")
    pegado = elo_pegado(m, data["elo"])
    sin_elo = int(pegado["_elo_d"].isna().sum())
    motivo, mt = veredicto_arrastre(pegado)
    if mt:
        print(f"  {mt['n']} filas equipo-partido; desfase mediano "
              f"{mt['p50_d']:.0f} d, p99 {mt['p99_d']:.0f} d")
        print(f"  le falta un partido a {mt['n_uno']} ({mt['share_uno']:.2%}, "
              f"techo {mt['techo']:.0%}); dos o mas, a {mt['n_graves']}")
    if motivo:
        _fail(motivo, errors)
    elif mt:
        print("  OK: ningun rating se salta dos partidos del propio club y el "
              "retraso de refresco se queda dentro del techo")
    if sin_elo:
        print(f"  ({sin_elo} filas sin Elo: anteriores al primer tramo publicado)")

    # 6b) la cola: cobertura sobre los ultimos partidos, no sobre la temporada.
    #     Es la invariante que cubre el fallo semanal probable --un extractor que
    #     no corrio-- y que la cobertura por temporada no puede ver: un bloque al
    #     que le falten las dos ultimas jornadas de las treinta y ocho sigue
    #     declarando un 95 % y pasa todos los suelos de [4b].
    print(f"\n[6b] cobertura de la cola (ultimos {_COLA} partidos del maestro)")
    cola, cobertura = cobertura_cola(m, bloques)
    print(f"  desde {cola['_d'].min().date()} hasta {cola['_d'].max().date()}")
    for name, cov in cobertura.items():
        floor = _SUELO.get(name, _SUELO_DEFECTO)
        if cov < floor:
            _fail(f"bloque '{name}' solo cubre {cov:.0%} de los ultimos {_COLA} "
                  f"partidos (< {floor:.0%}): su extractor no ha traido la "
                  f"ultima jornada", errors)
        else:
            print(f"  {name:16s}: {cov:6.0%}")

    # 7) el empalme de ClubElo no puede verse como un acontecimiento deportivo.
    #    La serie de Elo publicada une dos escalas de la fuente, y lo que la hace
    #    legitima es que el modelo consume la fuerza TIPIFICADA dentro de la
    #    jornada, invariante a la transformacion afin que las separa. Eso es un
    #    argumento, y esta invariante es su comprobacion: el salto de fuerza que
    #    los equipos acusan al cruzar la union tiene que caber dentro de lo que
    #    mueve una semana corriente de competicion. El liston se mide sobre el
    #    propio maestro --y no se escribe como constante-- para que siga siendo
    #    valido cuando el historico crezca; _SALTO_TOPE esta encima solo como
    #    suelo de cordura. Si esta invariante cae, lo que falla no es un dato: es
    #    el argumento que sostiene el empalme, y habria que volver a decidirlo.
    print("\n[7] continuidad del empalme de ClubElo (fuerza tipificada)")
    motivo, mt = veredicto_empalme(salto_elo_z(m))
    if not mt:
        print(f"  (ningun partido cruza el corte {ELO_CORTE}: nada que medir)")
        if motivo:
            _fail(motivo, errors)
    else:
        print(f"  empalme  (n={mt['n_cruces']}): mediana {mt['p50_c']:.3f}  "
              f"p90 {mt['p90_c']:.3f}  max {mt['max_c']:.3f}")
        print(f"  semana normal (n={mt['n_normal']}): mediana {mt['p50_n']:.3f}  "
              f"p90 {mt['p90_n']:.3f}  p99 {mt['p99_n']:.3f}  "
              f"max {mt['max_n']:.3f}")
        if motivo:
            _fail(motivo, errors)
        else:
            print(f"  OK: el empalme cabe dentro de una semana de competicion "
                  f"(max {mt['max_c']:.3f} <= {mt['max_n']:.3f}; "
                  f"mediana {mt['p50_c']:.3f} <= p90 {mt['p90_n']:.3f})")

    # veredicto
    print("\n" + "=" * 72)
    if errors:
        print(f"QA FALLIDO: {len(errors)} problema(s)")
        for e in errors:
            print("   -", e)
        return 1
    print("QA OK: el master cumple todas las invariantes duras.")
    return 0


if __name__ == "__main__":
    sys.exit(run_qa())
