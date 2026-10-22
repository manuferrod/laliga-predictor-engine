"""
Extractor Transfermarkt (valores de mercado) para laliga-predictor-engine.

Fuente: Transfermarkt (https://www.transfermarkt.com). Aporta el VALOR DE MERCADO,
que ninguna otra de las fuentes tiene y que sirve para dos cosas en el modelo 1X2:

  1) PRIOR DE FUERZA DE EQUIPO: el valor total de plantilla por club-temporada
     (`squad_value`) es una senal de calidad de plantilla independiente de la forma
     reciente; complementa a ClubElo (Elo mide resultados, el valor mide plantilla).
  2) PESO DE LAS BAJAS: el valor de cada jugador permite ponderar los ausentes de
     `availability.py`. No es lo mismo perder tres suplentes que al '9' de 80M. La
     tabla `player_values` deja los valores por jugador-temporada listos para ese
     cruce (que se hace en la capa de consolidacion, no aqui).

Que se extrae (y por que solo esto)
-----------------------------------
De cada pagina de PLANTILLA (kader) por club-temporada se saca, de una sola
peticion, TANTO el desglose por jugador COMO el total del equipo (= suma). Edad,
posicion y valor por jugador; el resto de Transfermarkt (fichajes, contratos,
nacionalidad detallada, ...) no aporta al 1X2 y se ignora. Los "Ausfalle"
(lesiones/sanciones) de TM son REDUNDANTES con Sofascore (`availability.py`), asi
que no se tocan.

Anti-leakage (importante para el TFM)
-------------------------------------
El valor se toma AS-OF-TEMPORADA (la pagina `saison_id/{year}` da el valor tal
como estaba catalogado esa temporada), NO el valor de hoy. Usar el valor actual
para un partido de 2018 seria mirar al futuro. Granularidad temporada = suficiente
para un prior de fuerza sin lookahead.

Ventana temporal
----------------
El universo de PARTIDOS del proyecto empieza en FIRST_SEASON/17 (2016/17), para
casar con las demas fuentes. El universo de VALORES es una temporada mas ancho
(`_VALUE_FIRST_SEASON = FIRST_SEASON - 1`) por una razon metodologica: la capa de
consolidacion asigna a cada partido el valor de plantilla de la temporada ANTERIOR
(lag t-1, ver `consolidate/squadvalue.py`) para que el mercado de invierno y las
revalorizaciones posteriores al partido no entren en la feature. Sin la temporada
2015/16 en el maestro, ese lag dejaria 2016/17 entera sin prior economico. Por eso
`backfill` arranca en `_VALUE_FIRST_SEASON` y `process` descarta lo anterior a esa
temporada, no a FIRST_SEASON.

Estructura del modulo (identica a los demas extractores)
--------------------------------------------------------
A) CAPA DE SCRAPING (corre en LOCAL, red abierta):
     backfill(seasons) -> descubre clubes por temporada (pagina de competicion
                          ES1) y baja cada plantilla al CSV crudo
                          `transfermarkt_squads.csv` (1 fila = jugador-club-temporada).
     update()          -> EJECUTABLE SEMANALMENTE: re-baja la temporada vigente.
   Idempotente por `row_id = year|club_id|player_id`: `backfill` salta los
   clubes-temporada ya presentes en el master, y vuelca al CSV cada ~200 filas
   (guardado atomico via `_common`), de modo que una interrupcion se reanuda
   sin re-bajar lo ya guardado. `update` re-baja la temporada vigente de forma
   NO destructiva (solo reemplaza los clubes que se bajan completos; ver
   `_run_refresh`). No hay fichero de checkpoint aparte: el propio master hace
   de marca de progreso.

B) CAPA DE ADAPTACION / NORMALIZACION (corre en cualquier sitio, solo pandas):
     process(master_csv) -> agrega a club-temporada (squad_value/size/avg) y escribe:
                            - `team_squad_value.parquet` (nivel club-temporada)
                            - `transfermarkt_player_values.parquet` (nivel jugador)
     to_match_level()   -> pivota a partido (home/away squad_value + diff) usando
                          el master de Sofascore como indice de partidos.

IMPORTANTE: la capa A exige salida a transfermarkt.com, por lo que se ejecuta
siempre en la maquina local. Requiere: pandas, curl_cffi y beautifulsoup4 (ya en
el .venv del repo). El parseo (capa B y las funciones `parse_*`) es puro y se
valida offline sobre HTML de ejemplo.

Uso tipico (en local)
---------------------
    python scripts\\extractors\\transfermarkt.py diag      # confirma el DOM (1 pagina)
    python scripts\\extractors\\transfermarkt.py backfill  # historico jugador-nivel
    python scripts\\extractors\\transfermarkt.py process    # agrega a los parquets
    python scripts\\extractors\\transfermarkt.py weekly     # update + process
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import pandas as pd

try:  # ejecucion como paquete
    from . import _common
    from . import sofascore as sofa
    from . import teamnames as tn
except ImportError:  # ejecucion como script suelto
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import _common  # type: ignore
    import sofascore as sofa  # type: ignore
    import teamnames as tn  # type: ignore


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
_COMP_ID = "ES1"                 # LaLiga en Transfermarkt (wettbewerb/ES1)
_BASE = "https://www.transfermarkt.com"
FIRST_SEASON = 2016              # 2016 = 16/17 (casa con las demas fuentes)
# El valor de plantilla entra en el modelo con lag t-1 (consolidate/squadvalue.py),
# de modo que los partidos de FIRST_SEASON necesitan la temporada anterior. Se baja
# y se conserva una temporada extra SOLO para alimentar ese desplazamiento.
_VALUE_FIRST_SEASON = FIRST_SEASON - 1

_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "transfermarkt"
# CSV crudo: una fila por jugador-club-temporada.
_MASTER_CSV = _RAW_DIR / "transfermarkt_squads.csv"
# Parquets procesados.
_OUT_TEAM = _REPO / "data" / "02_processed" / "team_squad_value.parquet"
_OUT_PLAYERS = _REPO / "data" / "02_processed" / "transfermarkt_player_values.parquet"

_KEY_COL = "row_id"

# Umbrales de cordura del scraping (Primera = 20 clubes; plantilla >= ~20):
# por debajo de esto, lo mas probable es un cambio de DOM o un bloqueo, y es
# mejor parar/preservar que escribir un master mutilado en silencio.
_MIN_CLUBS = 18
_MIN_SQUAD = 15

# Cabeceras de navegador (TM es mas quisquilloso que Understat).
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": f"{_BASE}/",
}

# Valores TM: "€80.00m", "€500k", "€1.20bn", "€250Th.", "-", "".  -> millones EUR.
_VALUE_RE = re.compile(
    r"€\s*([\d.,]+)\s*(bn|m|k|th\.?)?", re.I)


# =========================================================================== #
# A) CAPA DE SCRAPING  (LOCAL: curl_cffi impersonando Chrome, fallback urllib)
# =========================================================================== #
def _http_get(url: str, *, timeout: int = 30, retries: int = 3,
              wait: float = 3.0) -> tuple[int, str]:
    """GET -> (status, cuerpo). NO lanza por codigos HTTP; espera y reintenta en
    red, 429 y 5xx. Implementacion en `_common.http_get`."""
    return _common.http_get(url, headers=_HEADERS, timeout=timeout,
                            retries=retries, wait=wait)


def _comp_url(year: int) -> str:
    return f"{_BASE}/laliga/startseite/wettbewerb/{_COMP_ID}/plus/?saison_id={year}"


def _squad_url(club_slug: str, club_id, year: int) -> str:
    slug = club_slug or "verein"
    return f"{_BASE}/{slug}/kader/verein/{club_id}/saison_id/{year}/plus/1"


# --------------------------------------------------------------------------- #
#  Parseo (funciones PURAS: reciben HTML, no tocan la red -> testeables offline)
# --------------------------------------------------------------------------- #
def _parse_value(txt) -> float | None:
    """'€80.00m' -> 80.0 ; '€500k' -> 0.5 ; '€1.20bn' -> 1200.0 ; '-' -> None.

    Devuelve el valor en MILLONES de euros (columna `value_mio`).
    TM usa el punto como separador de miles en la web inglesa ('€1.20m',
    '€900k'); el numero antes del sufijo no lleva separador de miles problematico.
    """
    if txt is None:
        return None
    s = str(txt).strip()
    if not s or s in ("-", "–", "—"):
        return None
    m = _VALUE_RE.search(s)
    if not m:
        return None
    num = m.group(1).replace(",", "")
    try:
        val = float(num)
    except ValueError:
        return None
    suf = (m.group(2) or "").lower().rstrip(".")
    if suf == "bn":
        return val * 1000.0
    if suf == "m":
        return val
    if suf in ("k", "th"):
        return val / 1000.0
    # sin sufijo: TM da euros "planos" (raro en plantillas) -> a millones
    return val / 1_000_000.0


def _soup(html: str):
    from bs4 import BeautifulSoup
    return BeautifulSoup(html, "lxml")


_CLUB_HREF = re.compile(r"^/([^/]+)/(?:startseite|kader|spielplan)/verein/(\d+)")


def parse_competition_clubs(html: str, year: int) -> list[dict]:
    """Clubes de LaLiga de una temporada desde la pagina de competicion.

    Devuelve [{club_id, club_slug, club_name}], deduplicado por club_id. Busca los
    enlaces a /{slug}/.../verein/{id} dentro de la tabla principal `table.items`.
    """
    soup = _soup(html)
    table = soup.select_one("table.items") or soup
    seen: dict[str, dict] = {}
    for a in table.select("a[href*='/verein/']"):
        href = a.get("href", "")
        m = _CLUB_HREF.match(href)
        if not m:
            continue
        slug, cid = m.group(1), m.group(2)
        name = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
        if not name:  # enlaces de escudo (sin texto): se completan luego por el de texto
            seen.setdefault(cid, {"club_id": cid, "club_slug": slug, "club_name": ""})
            continue
        seen[cid] = {"club_id": cid, "club_slug": slug, "club_name": name}
    return [v for v in seen.values() if v["club_name"]] or list(seen.values())


_PLAYER_HREF = re.compile(r"/profil/spieler/(\d+)")
_AGE_RE = re.compile(r"\((\d{2})\)")


def parse_squad(html: str, year: int, club_id, club_slug: str) -> list[dict]:
    """Filas jugador de una pagina de plantilla (kader/verein).

    Una fila por jugador con: nombre, player_id, player_slug, posicion (best-effort),
    edad y valor de mercado (millones). Robusto a cambios menores de clase: identifica
    la fila de jugador por el enlace `/profil/spieler/{id}` y busca la celda de valor
    por patron '€...'.
    """
    soup = _soup(html)
    table = soup.select_one("table.items")
    if table is None:
        return []
    rows: list[dict] = []
    for tr in table.select("tbody > tr"):
        a = tr.select_one("a[href*='/profil/spieler/']")
        if a is None:
            continue
        href = a.get("href", "")
        mid = _PLAYER_HREF.search(href)
        pid = mid.group(1) if mid else None
        pslug_m = re.match(r"/([^/]+)/profil/spieler/", href)
        pslug = pslug_m.group(1) if pslug_m else None
        name = a.get("title") or a.get_text(" ", strip=True)

        tds = tr.find_all("td", recursive=False)
        # valor: la ultima celda que contenga un patron '€...'
        value = None
        for td in reversed(tds):
            txt = td.get_text(" ", strip=True)
            if "€" in txt:
                value = _parse_value(txt)
                break
        # edad: de la celda "Feb 5, 1992 (33)" o una celda entera 15-45
        age = None
        row_txt = tr.get_text(" ", strip=True)
        ma = _AGE_RE.search(row_txt)
        if ma:
            age = int(ma.group(1))
        # posicion (best-effort): sub-linea bajo el nombre en la tabla inline
        pos = None
        inline = tr.select_one("table.inline-table tr:nth-of-type(2) td")
        if inline is not None:
            pos = inline.get_text(" ", strip=True) or None

        rows.append({
            "row_id": f"{year}|{club_id}|{pid or tn.to_key(name)}",
            "season": year,
            "club_id": str(club_id),
            "club_slug": club_slug,
            "player": name,
            "player_id": pid,
            "player_slug": pslug,
            "position": pos,
            "age": age,
            "value_mio": value,
        })
    return rows


# --------------------------------------------------------------------------- #
#  Descarga de una temporada
# --------------------------------------------------------------------------- #
def _discover_clubs(year: int, *, verbose: bool = True) -> list[dict]:
    status, body = _http_get(_comp_url(year))
    if status != 200:
        if verbose:
            print(f"  [{year}] competicion HTTP {status}; se omite.")
        return []
    clubs = parse_competition_clubs(body, year)
    if verbose:
        print(f"  [{year}] clubes detectados: {len(clubs)}")
    return clubs


def scrape_club_squad(year: int, club: dict) -> list[dict]:
    status, body = _http_get(_squad_url(club["club_slug"], club["club_id"], year))
    if status != 200:
        print(f"    [{year}] {club.get('club_name') or club['club_id']}: HTTP {status}")
        return []
    return parse_squad(body, year, club["club_id"], club["club_slug"])


# --------------------------------------------------------------------------- #
#  Guardado + orquestacion (reutiliza el guardado robusto de sofascore.py)
# --------------------------------------------------------------------------- #
def _load_master(master_csv: Path):
    master_csv = Path(master_csv)
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        done = set(df[_KEY_COL].astype(str)) if _KEY_COL in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} filas.")
        return df, done
    print("[INFO] sin datos previos.")
    return pd.DataFrame(), set()


def _finalize_master(df_master, new_rows, master_csv: Path):
    if not new_rows:
        print("[INFO] no hay filas nuevas.")
        return df_master
    df_new = pd.DataFrame(new_rows)
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty],
                          ignore_index=True)
    if _KEY_COL in df_master.columns:
        df_master = df_master.drop_duplicates(_KEY_COL, keep="last")
    if sofa._write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] -> {master_csv} ({len(df_master)} filas)")
    return df_master


def _run(seasons, master_csv: Path, *, pause: float = 2.5) -> pd.DataFrame:
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)
    df_master, done = _load_master(master_csv)
    new_rows: list = []
    for year in sorted(set(int(y) for y in seasons)):
        print(f"\n=== Temporada {year}/{str(year + 1)[2:]} ===")
        clubs = _discover_clubs(year)
        if not clubs:
            # sin master previo de esta temporada, 0 clubes = fallo de transporte
            # (Transfermarkt no publica "0 equipos"); no hay nada que guardar.
            raise RuntimeError(
                f"[transfermarkt] descubrimiento de clubes VACIO para {year}: "
                "la pagina de competicion no devolvio equipos (bloqueo o cambio "
                "de DOM). Lo ya descargado esta guardado; re-ejecuta para reanudar."
            )
        if len(clubs) < _MIN_CLUBS:
            print(f"  [WARN] solo {len(clubs)} clubes descubiertos (<{_MIN_CLUBS}) "
                  f"en {year}; posible bloqueo parcial.")
        thin = 0
        for i, club in enumerate(clubs, 1):
            rid_prefix = f"{year}|{club['club_id']}|"
            # idempotencia por club-temporada: si ya hay filas de ese club-temporada, se salta
            if any(str(r).startswith(rid_prefix) for r in done):
                print(f"  [{i}/{len(clubs)}] {club.get('club_name')}: ya presente, salto.")
                continue
            rows = scrape_club_squad(year, club)
            if len(rows) < _MIN_SQUAD:
                thin += 1
                print(f"  [{i}/{len(clubs)}] [WARN] {club.get('club_name')}: solo "
                      f"{len(rows)} jugadores (<{_MIN_SQUAD}); posible pagina incompleta.")
            else:
                print(f"  [{i}/{len(clubs)}] {club.get('club_name')}: {len(rows)} jugadores")
            new_rows.extend(rows)
            if len(new_rows) >= 200:
                df_master = _finalize_master(df_master, new_rows, master_csv)
                done |= {r["row_id"] for r in new_rows}
                new_rows = []
            time.sleep(pause)
        if thin:
            print(f"  [WARN] {year}: {thin} club(es) con plantilla incompleta; "
                  "revisa si Transfermarkt estaba limitando peticiones.")
    return _finalize_master(df_master, new_rows, master_csv)


def backfill(master_csv: Path = _MASTER_CSV, *, seasons=None):
    if seasons is None:
        seasons = range(_VALUE_FIRST_SEASON, sofa_current_year() + 1)
    _run(seasons, master_csv)
    process(master_csv)


def update(master_csv: Path = _MASTER_CSV, *, recent_seasons: int = 1):
    cur = sofa_current_year()
    seasons = [cur - i for i in range(max(1, recent_seasons))
               if cur - i >= _VALUE_FIRST_SEASON]
    # re-scrape: para refrescar la temporada vigente, borramos sus filas del done
    # forzando su re-descarga (los valores cambian dentro de la temporada).
    _run_refresh(seasons, master_csv)
    process(master_csv)


def _run_refresh(seasons, master_csv: Path, *, pause: float = 2.5):
    """Refresca las temporadas indicadas SIN destruir datos (para el update semanal).

    El fallo que evita: la forma directa de refrescar -- borrar del master TODAS
    las filas de la temporada y escribir lo scrapeado -- destruye datos en cuanto
    el scrapeo sale incompleto. Si un club devuelve 0 jugadores (un 403 puntual, un
    cambio de DOM), sus filas desapareceran del master PARA SIEMPRE. Aqui, en cambio:
      * se scrapea PRIMERO, en memoria;
      * solo se reemplazan en el master las filas (season, club_id) de los
        clubes que se han scrapeado con plantilla COMPLETA (>= _MIN_SQUAD);
      * un club con pocas/0 filas conserva sus datos viejos y se avisa;
      * si el descubrimiento de una temporada da 0 clubes -> se corta (fallo de
        transporte/DOM, no "no hay equipos");
      * si NADA valido se scrapeo, no se escribe: el master queda intacto.
    """
    master_csv = Path(master_csv)
    df_master, _ = _load_master(master_csv)
    years = sorted(set(int(y) for y in seasons))

    scraped_rows: list = []
    refreshed: set = set()   # (season:int, club_id:str) refrescados con exito
    for year in years:
        print(f"\n=== (refresh) Temporada {year}/{str(year + 1)[2:]} ===")
        clubs = _discover_clubs(year)
        if not clubs:
            raise RuntimeError(
                f"[transfermarkt] descubrimiento de clubes VACIO para {year}: "
                "la pagina de competicion no devolvio equipos (bloqueo o cambio "
                "de DOM). No se toca el master; re-ejecuta el mismo comando mas "
                "tarde."
            )
        if len(clubs) < _MIN_CLUBS:
            print(f"  [WARN] solo {len(clubs)} clubes descubiertos (<{_MIN_CLUBS}); "
                  "posible bloqueo parcial. Los no vistos conservan sus datos.")
        for i, club in enumerate(clubs, 1):
            cid = str(club["club_id"])
            rows = scrape_club_squad(year, club)
            name = club.get("club_name") or cid
            if len(rows) >= _MIN_SQUAD:
                scraped_rows.extend(rows)
                refreshed.add((year, cid))
                print(f"  [{i}/{len(clubs)}] {name}: {len(rows)} jugadores (refrescado)")
            else:
                print(f"  [{i}/{len(clubs)}] [WARN] {name}: solo {len(rows)} "
                      f"jugadores (<{_MIN_SQUAD}); CONSERVO sus datos anteriores.")
            time.sleep(pause)

    if not refreshed:
        print("[ERROR] ningun club se refresco con plantilla completa en "
              f"{years}: el master NO se modifica (evita borrar datos buenos). "
              "Probablemente Transfermarkt esta bloqueando; reintenta mas tarde.")
        return df_master

    # reemplazo quirurgico: quita del master SOLO las (season, club_id) que de
    # verdad hemos vuelto a bajar; el resto (incluidos los clubes que fallaron)
    # se mantiene tal cual.
    if not df_master.empty and {"season", "club_id"}.issubset(df_master.columns):
        key = list(zip(df_master["season"].astype(int),
                       df_master["club_id"].astype(str)))
        mask_keep = [k not in refreshed for k in key]
        df_master = df_master[mask_keep]

    return _finalize_master(df_master, scraped_rows, master_csv)


def sofa_current_year(now=None) -> int:
    """Ano de inicio de la temporada vigente (LaLiga arranca en agosto).

    El nombre se conserva por compatibilidad; la logica vive en
    `_common.current_season_year` (misma para todos los extractores).
    """
    return _common.current_season_year(now)


# --------------------------------------------------------------------------- #
#  Agregacion a club-temporada
# --------------------------------------------------------------------------- #
def aggregate_players(df: pd.DataFrame) -> pd.DataFrame:
    """Filas jugador -> una fila por (season, team_key): squad_value/size/avg.

    Dos decisiones que separan "plantilla barata" de "no tenemos el dato":

    * `squad_value` se suma con `min_count=1`, asi que un club cuya pagina
      devuelva TODOS los valores como '-' da NaN y no 0,0. Un cero es un numero
      perfectamente plausible que ninguna comprobacion de nulos habria detectado y
      que aguas abajo se interpretaria como "plantilla sin valor de mercado".
    * `squad_size` cuenta jugadores CON valor conocido (no filas), de modo que
      `squad_value / squad_size` sea consistente: si se contaran las filas, un
      club con la mitad de los valores ausentes tendria un valor medio artificial-
      mente bajo. `value_avg_mio` ya ignora nulos por definicion de `mean`.
    """
    cols = ["season", "team_key", "squad_value", "squad_size", "value_avg_mio", "avg_age"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    d = df.copy()
    d["team_key"] = d["club_slug"].map(tn.to_key)
    d["value_mio"] = pd.to_numeric(d["value_mio"], errors="coerce")
    d["age"] = pd.to_numeric(d["age"], errors="coerce")
    g = d.groupby(["season", "team_key"])
    out = g.agg(
        squad_value=("value_mio", lambda s: s.sum(min_count=1)),
        squad_size=("value_mio", "count"),
        value_avg_mio=("value_mio", "mean"),
        avg_age=("age", "mean"),
    ).reset_index()
    return out[cols]


# Valor de plantilla (M€) por debajo del cual el dato es implausible para un club
# de Primera: el mas modesto de la ventana (2015 en adelante) ronda los 25-30 M€,
# asi que un club-temporada por debajo de este umbral senala una pagina de
# Transfermarkt mal parseada, no una plantilla barata.
_MIN_TEAM_VALUE = 20.0


def assert_team_values(team: pd.DataFrame, *, strict: bool = True) -> None:
    """Avisa (o corta) si algun club-temporada trae un valor implausible o nulo."""
    if team.empty or "squad_value" not in team.columns:
        return
    v = pd.to_numeric(team["squad_value"], errors="coerce")
    mal = team.loc[v.isna() | (v < _MIN_TEAM_VALUE),
                   ["season", "team_key", "squad_value", "squad_size"]]
    if mal.empty:
        return
    msg = (f"[transfermarkt] {len(mal)} club-temporada con valor de plantilla "
           f"nulo o inferior a {_MIN_TEAM_VALUE:.0f} M€ (pagina mal parseada o "
           f"valores '-'):\n{mal.head(10).to_string(index=False)}")
    if strict:
        raise _common.SchemaError(
            msg + "\nRe-ejecuta el backfill de esas temporadas; el parquet NO se "
            "ha regenerado. Si el valor es real, baja _MIN_TEAM_VALUE "
            "documentando por que.")
    print("[WARN] " + msg)


def process(master_csv: Path = _MASTER_CSV, *, out_team: Path = _OUT_TEAM,
            out_players: Path = _OUT_PLAYERS,
            verbose: bool = True, strict: bool = True) -> pd.DataFrame:
    """Agrega a club-temporada y escribe los dos parquets.

    `strict=True`: valida que los clubes scrapeados (aquellos cuyo alias es
    corregible en teamnames) tengan clave canonica; si no, corta con
    teamnames.assert_known. Ademas exige valores de plantilla plausibles y
    publica de forma NO REGRESIVA (ver `_common.publish_df`): un maestro ausente o
    truncado aborta en vez de sobrescribir los parquets buenos con cero filas.
    """
    if not Path(master_csv).exists():
        raise _common.RegressionError(
            f"[transfermarkt.process] no existe el maestro '{master_csv}'. Sin el "
            "no hay nada que agregar y publicar un parquet vacio destruiria el "
            "vigente. Ejecuta primero el backfill/update.")
    df = pd.read_csv(master_csv)

    # validacion de las temporadas scrapeadas (club_slug -> team_key)
    if strict and not df.empty and "club_slug" in df.columns:
        m = ~df["club_slug"].map(tn.to_key).isin(tn.CANONICAL)
        if m.any():
            tn.assert_known(df.loc[m, "club_slug"].dropna().unique(),
                            source="transfermarkt.process(scraped)")

    team = aggregate_players(df)
    if not team.empty:
        team["season"] = pd.to_numeric(team["season"], errors="coerce").astype("Int64")
        team = team.sort_values(["season", "team_key"]).reset_index(drop=True)

    # tabla jugador-nivel (para ponderar bajas mas adelante)
    players = pd.DataFrame(columns=["season", "team_key", "player", "player_key",
                                    "player_id", "value_mio"])
    if not df.empty:
        players = df.copy()
        players["team_key"] = players["club_slug"].map(tn.to_key)
        players["player_key"] = players["player"].map(_player_key)
        players["value_mio"] = pd.to_numeric(players["value_mio"], errors="coerce")
        players = players[["season", "team_key", "player", "player_key",
                           "player_id", "value_mio"]]

    if verbose:
        _report(df, team)

    # Ventana de VALORES: una temporada por delante de la ventana de partidos,
    # porque la consolidacion los consume con lag t-1 (ver cabecera del modulo).
    for _df in (team, players):
        _df.drop(_df.index[pd.to_numeric(_df["season"], errors="coerce")
                           < _VALUE_FIRST_SEASON],
                 inplace=True)
    team = team.reset_index(drop=True)
    players = players.reset_index(drop=True)

    # valores implausibles ANTES de publicar: si el parseo ha fallado, el parquet
    # vigente sigue siendo mejor que el que estabamos a punto de escribir.
    assert_team_values(team, strict=strict)

    for out, data, etiqueta in ((out_team, team, "team_squad_value"),
                                (out_players, players, "player_values")):
        _common.publish_df(data, out, source=f"transfermarkt.{etiqueta}",
                           fmt="parquet", verbose=verbose)
    return team


def _player_key(name) -> str:
    """Clave normalizada de jugador (deaccent + minusculas + guiones)."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = tn._deaccent(str(name)).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def to_match_level(team_parquet: Path = _OUT_TEAM,
                   master_csv: Path = sofa._MATCH_STATS_CSV) -> pd.DataFrame:
    """Pivota a partido: home/away squad_value + diff, usando el master de Sofascore.

    Cruza cada partido (event_id -> temporada + equipos) con el valor de plantilla
    de esa temporada. Devuelve una fila por partido con match_key para el merge.
    """
    team = pd.read_parquet(team_parquet)
    meta = pd.read_csv(master_csv, usecols=["match_id", "home", "away",
                                            "match_date", "season_name"])
    meta["season"] = meta["season_name"].map(_season_start_year)
    meta["home_key"] = meta["home"].map(tn.to_key)
    meta["away_key"] = meta["away"].map(tn.to_key)
    val = team[["season", "team_key", "squad_value"]]
    out = meta.merge(val.rename(columns={"team_key": "home_key",
                                         "squad_value": "home_squad_value"}),
                     on=["season", "home_key"], how="left")
    out = out.merge(val.rename(columns={"team_key": "away_key",
                                        "squad_value": "away_squad_value"}),
                    on=["season", "away_key"], how="left")
    out["squad_value_diff"] = out["home_squad_value"] - out["away_squad_value"]
    date10 = out["match_date"].astype("string").str.slice(0, 10)
    out["match_key"] = tn.match_key(date10, out["home"], out["away"])
    return out


def _season_start_year(season_name) -> "int | float":
    """'24/25' o '2024/2025' o '2024' -> 2024. NaN si no se puede."""
    if season_name is None or (isinstance(season_name, float) and pd.isna(season_name)):
        return float("nan")
    s = str(season_name).split("/")[0].strip()
    try:
        v = int(s)
    except ValueError:
        return float("nan")
    if v > 1000:
        return v
    return 2000 + v if v <= 50 else 1900 + v


def _report(df, team) -> None:
    print("\n===== INFORME TRANSFERMARKT =====")
    print(f"filas jugador (crudo)   : {len(df)}")
    if len(df) and "value_mio" in df:
        v = pd.to_numeric(df["value_mio"], errors="coerce")
        print(f"  con valor no nulo     : {v.notna().mean() * 100:.1f}%")
    print(f"club-temporada          : {len(team)}")
    if len(team):
        seasons = sorted(int(s) for s in team['season'].dropna().unique())
        print(f"temporadas cubiertas    : {seasons[:3]} ... {seasons[-3:]}")
        bad = sorted(set(team.loc[~team['team_key'].isin(tn.CANONICAL), 'team_key']
                         .dropna().unique()))
        print(f"claves NO canonicas     : {bad if bad else '[] (ok)'}")
        sv = pd.to_numeric(team["squad_value"], errors="coerce")
        print(f"valor plantilla min/max : {sv.min():.0f} / {sv.max():.0f} M€  "
              f"(umbral de plausibilidad {_MIN_TEAM_VALUE:.0f} M€; "
              f"nulos: {int(sv.isna().sum())})")
    print("=================================\n")


# --------------------------------------------------------------------------- #
#  Diagnostico: confirma el DOM con 1 pagina antes del backfill grande
# --------------------------------------------------------------------------- #
def diag(year: int | None = None) -> None:
    year = year or sofa_current_year()
    print(f"[DIAG] competicion {year}: {_comp_url(year)}")
    clubs = _discover_clubs(year)
    for c in clubs[:5]:
        print(f"   club_id={c['club_id']} slug={c['club_slug']!r} name={c['club_name']!r}")
    if not clubs:
        print("[DIAG] sin clubes -> revisa el DOM de la pagina de competicion.")
        return
    club = clubs[0]
    print(f"\n[DIAG] plantilla de {club['club_name']}: "
          f"{_squad_url(club['club_slug'], club['club_id'], year)}")
    rows = scrape_club_squad(year, club)
    print(f"[DIAG] jugadores parseados: {len(rows)}")
    for r in rows[:6]:
        print(f"   {r['player']!r:28s} pos={r['position']!r} edad={r['age']} "
              f"valor={r['value_mio']} id={r['player_id']}")
    vals = [r["value_mio"] for r in rows if r["value_mio"] is not None]
    if vals:
        print(f"[DIAG] total plantilla (suma): {sum(vals):.1f} M€  "
              f"(n con valor={len(vals)}/{len(rows)})")


# =========================================================================== #
# CLI
# =========================================================================== #
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "process"
    if cmd == "diag":
        diag(int(sys.argv[2]) if len(sys.argv) > 2 else None)
    elif cmd == "backfill":
        backfill()
    elif cmd == "update":
        update()
    elif cmd == "process":
        process()
    elif cmd in ("weekly", "schedule"):
        update()
    else:
        print("uso: python transfermarkt.py [diag|backfill|update|process|weekly]")
