"""Features de CLASIFICACIÓN y TRASCENDENCIA (la tabla) leak-safe.

Reconstruye la tabla tal como estaba ANTES de cada partido y de ahí deriva la
presión contextual (zona de la tabla, colchón sobre el descenso, cambio respecto
a la temporada previa). Todo mirando solo partidos con fecha estrictamente
anterior al encuentro, así que no hay fuga.

Método = SNAPSHOT POR FECHA. Para cada fecha D de la temporada, se suman los
puntos/gd/gf de cada equipo en sus partidos JUGADOS con fecha < D y se ordena la
tabla. Esto es correcto también en la ruta de inferencia: un partido futuro en la
fecha D se clasifica contra la tabla real de los 20 equipos a esa fecha (no solo
contra el subconjunto que se está prediciendo, que es el error en el que caería
un enfoque por número de jornada sobre un máster extendido parcial).

Atributos que se calculan por equipo (el lado en que sale cada uno lo declara
`catalogo`; los marcados como INSUMO se calculan pero no llegan a la matriz):
  * prev_position          : puesto (1=líder) al entrar al partido
  * position_zone          : 'champions' (<=4) / 'europa' (5-6) / 'mid_table'
                             (7-17) / 'descenso' (>=18)   [categórica]
  * points_above_drop      : puntos por encima del 1er puesto de descenso (18º)
  * dynamic_pos_change     : posición final temporada previa − prev_position
                             (>0 mejora respecto a como acabó; <0 empeora)

  --- distancia a cada objetivo y trascendencia del partido ---
  * points_to_leader       : puntos que separan del líder
  * points_to_champions    : puntos hasta el 4º puesto (<=0 si ya se ocupa)
  * points_to_europa       : puntos hasta el 6º puesto (<=0 si ya se ocupa)
  * matches_left           : partidos que le restan al equipo por disputar
  * safe_from_drop         : ya hay tantos equipos incapaces de alcanzarle como
                             puestos de descenso, así que no puede descender
  * europe_unreachable     : INSUMO. Ya hay seis equipos a los que él no puede
                             alcanzar. No sale porque es el complemento casi
                             determinista de `points_to_europa`.
  * stakes_none            : INSUMO. Ambas cosas a la vez (nada en juego). Sale
                             solo como producto de los dos lados, ver abajo.

Y una columna de PARTIDO, sin desdoblar en local y visitante:
  * stakes_none_match      : los dos equipos sin nada en juego

La posición sola no dice cuánto se juega un equipo: un décimo a tres puntos de
Europa en la jornada treinta y cinco y un décimo con todo decidido ocupan el
mismo puesto y afrontan el partido de manera distinta. Lo que separa esos dos
casos es la DISTANCIA a cada objetivo medida contra los puntos que todavía se
pueden sumar, y de ahí salen tanto las cuatro distancias como los tres
indicadores de trascendencia.

Los indicadores son cotas conservadoras y no predicciones, y se construyen
CONTANDO EQUIPOS y no comparando contra un corte. Para cada equipo se calcula el
máximo al que puede aspirar —sus puntos actuales más tres por cada partido que
le resta— y a partir de ahí:

  * un equipo está salvado cuando el número de rivales cuyo máximo alcanzable no
    llega a sus puntos ACTUALES iguala o supera al de puestos de descenso: por
    pocos puntos que sume a partir de ahora, terminará por delante de todos
    ellos y por tanto fuera de la zona;
  * Europa le es inalcanzable cuando hay ya seis rivales cuyos puntos actuales
    superan su máximo alcanzable, porque los seis terminarán por delante.

Contar equipos y no medir distancias al corte es lo que hace la cota válida
cuando el calendario va desigual: un rival con un partido aplazado puede sumar
más puntos que los que restan al equipo que se evalúa, de modo que una regla
apoyada en los partidos pendientes PROPIOS declararía salvaciones que el
calendario todavía no garantiza. Aquí cada rival se acota con los partidos que
le restan a él.

Como el máximo alcanzable solo puede decrecer y los puntos actuales solo pueden
crecer, un objetivo declarado inalcanzable lo es con certeza; a cambio, algún
caso alcanzable sobre el papel pero imposible en la práctica no se marca. La
asimetría es deliberada: el indicador afirma solo lo que se puede demostrar con
la tabla a la vista.
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

_NEED = ["match_key", "season_year", "date", "home_key", "away_key",
         "ftr", "fthg", "ftag"]

_DROP_POS = 18       # 1er puesto de descenso en una liga de 20
_CHAMPIONS_POS = 4   # último puesto con plaza de Champions
_EUROPA_POS = 6      # último puesto con plaza europea
_POINTS_PER_WIN = 3

# Partidos de liga por equipo y temporada (20 equipos, ida y vuelta). Se declara
# como constante en vez de contarse sobre el máster a propósito: en la ruta de
# inferencia el máster extendido contiene los partidos ya jugados más la jornada
# que se va a predecir, de modo que un recuento por equipo daría un calendario
# más corto que el real y sobreestimaría lo decidido que está todo. El recuento
# de jugados, en cambio, sí se toma del dato, y así los partidos aplazados
# aparecen correctamente como partidos pendientes.
_TOTAL_MATCHES = 38

_NUMERIC = ["prev_position", "points_above_drop", "dynamic_pos_change",
            "points_to_leader", "points_to_champions", "points_to_europa",
            "matches_left", "safe_from_drop", "europe_unreachable",
            "stakes_none"]


def _team_long(master: pd.DataFrame) -> pd.DataFrame:
    """2 filas/partido con pts/gd/gf desde la perspectiva del equipo (NaN si futuro)."""
    frames = []
    for is_home in (True, False):
        tk = "home_key" if is_home else "away_key"
        gf = master["fthg"].astype("float64") if is_home else master["ftag"].astype("float64")
        ga = master["ftag"].astype("float64") if is_home else master["fthg"].astype("float64")
        ftr = master["ftr"].astype("string")
        pts = np.where(gf > ga, 3.0, np.where(gf == ga, 1.0, 0.0))
        played = ftr.notna().to_numpy() & gf.notna().to_numpy() & ga.notna().to_numpy()
        pts = np.where(played, pts, np.nan)
        frames.append(pd.DataFrame({
            "match_key": master["match_key"],
            "season_year": master["season_year"],
            "date": pd.to_datetime(master["date"]).dt.normalize(),
            "is_home": 1 if is_home else 0,
            "team_key": master[tk],
            "pts": pts,
            "gd": (gf - ga).where(pd.Series(played, index=master.index)),
            "gf": gf.where(pd.Series(played, index=master.index)),
            "played": played,
        }))
    return pd.concat(frames, ignore_index=True)


def _prev_season_final(long: pd.DataFrame) -> pd.DataFrame:
    """Posición final de cada equipo, desplazada +1 temporada."""
    played = long[long["played"]]
    tot = (played.groupby(["season_year", "team_key"], as_index=False)[["pts", "gd", "gf"]]
                 .sum())
    tot = tot.sort_values(["season_year", "pts", "gd", "gf"],
                          ascending=[True, False, False, False])
    tot["final_position"] = tot.groupby("season_year").cumcount() + 1
    tot["season_year"] = tot["season_year"] + 1   # aplica a la temporada siguiente
    return tot[["season_year", "team_key", "final_position"]]


def _corte(tbl: pd.DataFrame, pos: int) -> float:
    """Puntos del equipo que ocupa `pos` en la tabla, o NaN si aún no existe.

    Devuelve NaN —y no los puntos del último clasificado— cuando la tabla tiene
    menos equipos que el puesto pedido, que es lo que ocurre en las primeras
    fechas de la temporada, con parte de la jornada inaugural sin disputar. Un
    corte tomado de una tabla incompleta no es un corte bajo: es un corte que
    todavía no significa nada.
    """
    return float(tbl.iloc[pos - 1]["pts"]) if len(tbl) >= pos else np.nan


def _snapshots(long: pd.DataFrame) -> pd.DataFrame:
    """Foto de la tabla ANTES de cada fecha: puesto, cortes y partidos disputados.

    De aquí salen las dos lecturas de la clasificación. La estática es el puesto
    y el colchón sobre el descenso; la de trascendencia son las distancias a cada
    corte (líder, Champions, Europa), el número de partidos que el equipo aún
    tiene por delante y los dos RECUENTOS DE RIVALES que deciden si un objetivo
    sigue vivo. Esos recuentos se calculan aquí, y no más tarde, porque exigen
    tener delante la tabla entera: son una comparación de cada equipo contra
    todos los demás y no una lectura de su propia fila.
    """
    out_rows = []
    for season, sdf in long.groupby("season_year", sort=False):
        played = sdf[sdf["played"]]
        # el número de equipos se toma del CALENDARIO de la temporada y no de la
        # tabla del día, que en las primeras fechas todavía no los contiene a
        # todos; de él dependen los puestos de descenso, que en una liga de 20
        # son tres.
        n_teams = int(sdf["team_key"].nunique())
        dates = np.sort(sdf["date"].unique())
        for D in dates:
            prior = played[played["date"] < D]
            if prior.empty:
                continue
            g = prior.groupby("team_key")
            tbl = (g[["pts", "gd", "gf"]].sum()
                    .sort_values(["pts", "gd", "gf"], ascending=[False, False, False]))
            tbl["jugados"] = g.size()
            tbl["position"] = np.arange(1, len(tbl) + 1)
            drop_pts = _corte(tbl, _DROP_POS)
            lead_pts = _corte(tbl, 1)
            ucl_pts = _corte(tbl, _CHAMPIONS_POS)
            uel_pts = _corte(tbl, _EUROPA_POS)

            # Techo de cada equipo: lo que sumaría ganando todo lo que le resta.
            # Un equipo con un partido aplazado tiene un techo más alto que otro
            # con los mismos puntos, y es justo esa asimetría la que una regla
            # basada en los partidos pendientes propios pasaría por alto.
            pts_arr = tbl["pts"].to_numpy(dtype="float64")
            jugados = tbl["jugados"].to_numpy(dtype="float64")
            techo = pts_arr + _POINTS_PER_WIN * (_TOTAL_MATCHES - jugados)
            # Ambos recuentos son ESTRICTOS: el empate no demuestra nada, porque
            # un rival que llega justo a mis puntos puede acabar por delante por
            # los criterios de desempate. Un equipo nunca se cuenta a sí mismo,
            # ya que su propio techo nunca es menor que sus puntos actuales ni
            # sus puntos actuales mayores que su propio techo.
            n_below = np.searchsorted(np.sort(techo), pts_arr, side="left")
            n_above = len(pts_arr) - np.searchsorted(np.sort(pts_arr), techo,
                                                     side="right")
            # los equipos que aún no aparecen en la tabla no entran en ninguno de
            # los dos recuentos, lo que juega siempre del lado conservador: su
            # ausencia solo puede retrasar una declaración, nunca provocarla.

            for i, (team, r) in enumerate(tbl.iterrows()):
                pts = float(r["pts"])
                out_rows.append((
                    season, team, D, float(r["position"]), pts, float(r["jugados"]),
                    pts - drop_pts, lead_pts - pts, ucl_pts - pts, uel_pts - pts,
                    float(n_below[i]), float(n_above[i]), float(n_teams)))
    # `pts_prior`, `jugados`, los dos recuentos y `n_teams` no son features: son
    # la materia prima con la que se calculan `matches_left` y los indicadores de
    # trascendencia. `pts_prior` se nombra así para no colisionar con la columna
    # homónima de `_team_long`, que mide lo ocurrido EN el partido y no lo
    # acumulado ANTES de él.
    return pd.DataFrame(out_rows, columns=[
        "season_year", "team_key", "date", "prev_position", "pts_prior", "jugados",
        "points_above_drop", "points_to_leader", "points_to_champions",
        "points_to_europa", "rivals_unreachable_below", "rivals_out_of_reach_above",
        "n_teams"])


def _classify_zone(pos) -> object:
    if pd.isna(pos):
        return pd.NA
    if pos <= 4:
        return "champions"
    if pos <= 6:
        return "europa"
    if pos <= 17:
        return "mid_table"
    return "descenso"


def standings_history(master: pd.DataFrame) -> pd.DataFrame:
    """Trayectoria POR EQUIPO: dos filas por partido, con todo lo calculado.

    Es el frame donde vive de verdad la aritmética de este módulo. Se expone
    aparte porque la fila de partido ya no la contiene: el catálogo emite la
    mayoría de estos atributos solo como diferencia, y de una resta no se
    recupera ninguno de los dos sumandos. Sin este acceso, comprobar que el
    colchón sobre el descenso o el indicador de salvación valen lo que deben
    exigiría deducirlos de la diferencia, que es imposible, o mirar únicamente
    los pocos atributos que sobreviven por lados, que es mirar hacia otro lado.
    """
    long = _team_long(master)
    snaps = _snapshots(long)
    finals = _prev_season_final(long)

    long = long.merge(snaps, on=["season_year", "team_key", "date"], how="left")
    long = long.merge(finals, on=["season_year", "team_key"], how="left")
    long["dynamic_pos_change"] = long["final_position"] - long["prev_position"]
    long["position_zone"] = long["prev_position"].map(_classify_zone).astype("string")

    # Trascendencia: lo que queda por jugar acota lo que aún puede cambiar. El
    # máximo alcanzable no se publica como columna porque es 3·matches_left, una
    # transformación lineal que no añade nada a lo que ya dice `matches_left`.
    long["matches_left"] = _TOTAL_MATCHES - long["jugados"]
    # Puestos de descenso de la temporada. Salvarse es asegurarse de terminar por
    # delante de tantos equipos como plazas de descenso haya: con tres rivales
    # que no puedan alcanzarle, el peor puesto posible del equipo es el 17º.
    plazas_descenso = long["n_teams"] - _DROP_POS + 1
    long["safe_from_drop"] = (
        long["rivals_unreachable_below"] >= plazas_descenso).astype("float64")
    # Simétricamente, con seis rivales fuera de su alcance el mejor puesto
    # posible es el 7º y la última plaza europea queda descartada.
    long["europe_unreachable"] = (
        long["rivals_out_of_reach_above"] >= _EUROPA_POS).astype("float64")
    long["stakes_none"] = (long["safe_from_drop"] * long["europe_unreachable"])
    # Una comparación con NaN devuelve False, que aquí sería afirmar sin dato:
    # los indicadores se anulan donde no hay tabla previa de la que contar. Se
    # anulan también en una competición con menos equipos que el puesto de
    # referencia, por el mismo motivo por el que un corte inexistente es NaN y no
    # los puntos del último clasificado: en una liga de doce equipos no hay un
    # decimoctavo puesto del que salvarse, y afirmar la salvación equivaldría a
    # inventar la zona de descenso.
    _base = long["jugados"].notna()
    long["safe_from_drop"] = long["safe_from_drop"].where(
        _base & (long["n_teams"] >= _DROP_POS))
    long["europe_unreachable"] = long["europe_unreachable"].where(
        _base & (long["n_teams"] >= _EUROPA_POS))
    long["stakes_none"] = long["stakes_none"].where(
        long["safe_from_drop"].notna() & long["europe_unreachable"].notna())
    return long


def build_standings_features(master: pd.DataFrame) -> pd.DataFrame:
    """Máster -> features de clasificación, keyed por match_key."""
    long = standings_history(master)

    feats_num = _NUMERIC
    feats_cat = ["position_zone"]
    feats = feats_num + feats_cat
    keep = ["match_key", "is_home"] + feats
    home = (long.loc[long["is_home"] == 1, keep].drop(columns="is_home")
                .rename(columns={c: f"home_{c}" for c in feats}))
    away = (long.loc[long["is_home"] == 0, keep].drop(columns="is_home")
                .rename(columns={c: f"away_{c}" for c in feats}))
    wide = home.merge(away, on="match_key", how="outer")
    for c in feats_num:
        if catalogo.emite(c, "diff"):
            wide[f"{c}_diff"] = wide[f"home_{c}"] - wide[f"away_{c}"]

    # Partido intrascendente: los DOS equipos sin nada en juego. Es el producto
    # de los dos indicadores y no una combinación lineal de ellos ni de su
    # diferencia, así que un estimador aditivo no puede reconstruirlo y merece
    # columna propia. Es además la única forma en que `stakes_none` sale a la
    # matriz: por lado no aporta —lo que un equipo se juega ya lo describen las
    # distancias a cada objetivo—, y es la coincidencia de los dos la que la
    # literatura asocia a un sesgo de motivación.
    wide["stakes_none_match"] = wide["home_stakes_none"] * wide["away_stakes_none"]

    # y aquí se descarta todo lado que el catálogo no declare
    sobran = [f"{lado}_{c}" for c in feats for lado in ("home", "away")
              if not catalogo.emite(c, lado)]
    wide = wide.drop(columns=sobran)

    wide = wide.reset_index(drop=True)
    assert_unique(wide, ["match_key"], "standings")
    return wide


# columnas de partido (sin desdoblar en local/visitante) que emite el módulo
_MATCH_LEVEL = ["stakes_none_match"]


def standings_feature_names() -> dict[str, list[str]]:
    """Contrato del módulo: qué columnas emite y de qué tipo es cada una."""
    return {"numeric": _NUMERIC, "categorical": ["position_zone"],
            "match_level": _MATCH_LEVEL}
