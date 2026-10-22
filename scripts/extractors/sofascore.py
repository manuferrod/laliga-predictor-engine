"""
Extractor Sofascore para laliga-predictor-engine.

Fuente: Sofascore (API interna v1), una de las webs con estadisticas de partido
mas completas de LaLiga. Aporta, por partido, TODAS las tablas de estadisticas
del periodo "ALL" (posesion, tiros, pases, duelos, defensa, portero, ...) MAS
la valoracion media de jugador (`home_avg_rating` / `away_avg_rating`), que
resume el rendimiento individual agregado y no esta disponible en las demas
fuentes del proyecto.

Estructura del modulo
---------------------
A) CAPA DE SCRAPING (corre en LOCAL, red abierta):
     backfill(seasons)  -> descarga todo el historico al CSV maestro
                           `sofascore_laliga_match_stats.csv`.
     update()           -> EJECUTABLE SEMANALMENTE: anade solo los partidos
                           finalizados nuevos de la temporada en curso al maestro.
   El guardado esta pensado para ejecutarse una sola vez sin intervencion: cada
   partido se vuelca a un checkpoint segun se descarga, el CSV se publica de
   forma atomica y se reanuda solo si algo interrumpe la escritura (ver la
   seccion "Guardado a prueba de fallos").
   Transporte validado (julio 2026): un cliente con 3 estrategias EN CASCADA que
   cambia sola cuando una acumula fallos (403 de Cloudflare, red, ...):
     1) curl_cffi impersonando Chrome -> api.sofascore.com   (rapido, ~1 req/s)
     2) curl_cffi impersonando Chrome -> api.sofavpn.com     (espejo oficioso;
        suele responder cuando el host canonico devuelve 403)
     3) Selenium: fetch() DENTRO de un Chrome real en www.sofascore.com
        (ultimo recurso; un solo driver persistente + espera del challenge)
   Requiere una maquina con salida a los dominios de Sofascore, por lo que se
   ejecuta siempre en local.  Requiere:  pip install curl_cffi pandas
   (selenium solo hace falta si se llega a la estrategia 3).

B) CAPA DE ADAPTACION / NORMALIZACION (corre en cualquier sitio, solo pandas):
     load_processed(master_csv) -> lee el master, aplana/renombra las columnas
                           ALL__, normaliza equipos via `teamnames` (+ fallback
                           por slug de Sofascore), anade match_key y guarda un
                           parquet limpio para el cruce con las demas fuentes.

Uso tipico (en local, cada semana)
----------------------------------
    from scripts.extractors import sofascore
    sofascore.update()                 # 1) baja los partidos nuevos
    sofascore.load_processed()         # 2) regenera el parquet normalizado

O desde linea de comandos:
    python scripts/extractors/sofascore.py weekly     # update + process
    python scripts/extractors/sofascore.py backfill --seasons 2016 2017 ... 2025
    python scripts/extractors/sofascore.py process    # solo re-normaliza el CSV
"""

from __future__ import annotations

import glob
import json
import random
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

try:  # ejecucion como paquete
    from . import _common
    from . import teamnames
except ImportError:  # ejecucion como script suelto
    import _common  # type: ignore
    import teamnames  # type: ignore


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #
LALIGA_TOURNAMENT_ID = 8
LANG = "es"

# Primera temporada del proyecto (ano de inicio). Casa con `FIRST_SEASON` de las
# demas fuentes: 2016 = temporada 2016/17.
FIRST_SEASON = 2016


def _current_season_year(now: datetime | None = None) -> int:
    """Ano de inicio de la temporada vigente (LaLiga arranca en agosto).

    El rango de temporadas se deduce de aqui y nunca se escribe a mano. Un tope
    literal no falla cuando caduca: simplemente deja de cubrir la temporada mas
    reciente, y el extractor sigue diciendo que ha terminado bien.
    """
    now = now or datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def _todas_las_temporadas() -> list[int]:
    """Temporadas del proyecto, de la primera a la vigente, ambas incluidas."""
    return list(range(FIRST_SEASON, _current_season_year() + 1))

# raiz del repo: .../scripts/extractors/sofascore.py -> subir 3 -> repo/
_REPO = Path(__file__).resolve().parents[2]
_RAW_DIR = _REPO / "data" / "01_raw" / "sofascore"
# CSV maestro (crudo): una fila por partido de LaLiga con TODAS las tablas de
# estadisticas de Sofascore (periodo "ALL") + las valoraciones medias de jugador.
_MATCH_STATS_CSV = _RAW_DIR / "sofascore_laliga_match_stats.csv"
# Parquet limpio (normalizado) listo para cruzar con las demas fuentes.
_OUT_PARQUET = _REPO / "data" / "02_processed" / "sofascore_team_match.parquet"

# columnas meta (no estadisticas) del CSV maestro
_META_COLS = {
    "match_id", "round", "home", "away", "home_slug", "away_slug",
    "customId", "customid", "timestamp", "status", "url", "match_date",
    "season_id", "season_name", "home_avg_rating", "away_avg_rating",
}

# --- umbrales de la asercion de cobertura de estadisticas ------------------- #
# El master historico ronda las 200 columnas `sofa_*`; un desplome muy por debajo
# de este suelo delata deriva del formato de Sofascore o un renombrado roto, no
# una carencia de datos.
_MIN_STAT_COLS = 100
# Cobertura minima por columna entre los partidos que SI tienen estadisticas.
# No todas las metricas existen en todo el historico (p.ej. las de xG llegan mas
# tarde), asi que por debajo de este umbral solo se avisa; una columna
# COMPLETAMENTE vacia, en cambio, es un error: no aporta nada al modelo y suele
# indicar que la tabla de origen cambio de nombre.
_MIN_COL_COVERAGE = 0.60


# =========================================================================== #
# A) CAPA DE SCRAPING  (LOCAL: curl_cffi en cascada + navegador de reserva)
# =========================================================================== #
# Sofascore protege su API con Cloudflare (TLS/JA3 + a veces managed challenge).
# El transporte validado en julio de 2026 es un cliente con
# 3 estrategias EN CASCADA que escala sola tras N fallos consecutivos:
#   1) curl_cffi impersonando Chrome -> api.sofascore.com
#   2) curl_cffi impersonando Chrome -> api.sofavpn.com  (espejo oficioso; suele
#      responder 200 cuando el host canonico devuelve 403)
#   3) Selenium: fetch() DENTRO de un Chrome real en www.sofascore.com
# Se prueban varios perfiles de impersonate y se respeta un rate-limit suave.
# La ruta se pasa como path ('/api/v1/...') y el cliente decide el host.
#   Requiere (local):  pip install curl_cffi pandas   (+ selenium para la 3)

# rutas de la API v1 (el HOST lo elige la estrategia activa del cliente)
_API = "/api/v1"

# perfiles de curl_cffi a probar en orden (el primero valido se usa)
_IMPERSONATE_CANDIDATES = ["chrome", "chrome136", "chrome131", "chrome124", "chrome120"]

# cabeceras que usa el frontend real de Sofascore
_SOFA_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://www.sofascore.com/",
    "Origin": "https://www.sofascore.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

# estrategias en cascada: (nombre, tipo, base_url)
_STRATEGIES = [
    ("curl->api.sofascore.com", "curl", "https://api.sofascore.com"),
    ("curl->api.sofavpn.com", "curl", "https://api.sofavpn.com"),
    ("browser->www.sofascore.com", "browser", "https://www.sofascore.com"),
]

# defaults ajustables por CLI
_IMPERSONATE = None          # None -> autodetecta probando _IMPERSONATE_CANDIDATES
_RATE_LIMIT_RPS = 1.6
_BROWSER_HEADLESS = True      # solo afecta a la estrategia 3 (Selenium)

# script async: fetch() DENTRO de la pagina (contexto www.sofascore.com, mismo
# origen -> sin CORS) y devuelve {ok, status, body}.
_PAGE_FETCH_JS = """
const url = arguments[0];
const cb = arguments[arguments.length - 1];
fetch(url, {credentials: 'include'})
  .then(async r => cb({ok: true, status: r.status, body: await r.text()}))
  .catch(e => cb({ok: false, err: String(e)}));
"""


class NotFound(Exception):
    """404: el recurso no existe (p.ej. partido sin estadisticas publicadas)."""


class FetchError(Exception):
    """Fallo recuperable (403/429/red). Se reintenta y/o escala de estrategia."""


class SofaClient:
    """Cliente Sofascore con 3 estrategias en cascada (curl_cffi x2 + navegador).

    Uso: `c = SofaClient(); c.fetch_json('/api/v1/...'); ...; c.close()`.
    Escala automaticamente a la siguiente estrategia tras `ESCALATE_AFTER` fallos
    consecutivos, y nunca se rinde mientras quede una estrategia sin probar.
    `fetch_json` devuelve {ok, data, err} (compatible con `_fetch_json`).
    """

    ESCALATE_AFTER = 4  # fallos consecutivos antes de pasar a la siguiente

    def __init__(self, *, impersonate: str | None = None,
                 rate_limit_rps: float | None = None,
                 headless: bool | None = None):
        rps = _RATE_LIMIT_RPS if rate_limit_rps is None else rate_limit_rps
        self._min_interval = (1.0 / rps) if rps and rps > 0 else 0.0
        self.impersonate = impersonate      # fuerza un perfil; None -> autodetecta
        self._headless = _BROWSER_HEADLESS if headless is None else headless
        self._session = None                # sesion curl_cffi
        self._driver = None                 # driver Selenium (estrategia 3)
        self._impersonate_used = None
        self._consec_fail = 0
        self._last_req = 0.0
        self.strategies = list(_STRATEGIES)
        self.strategy_idx = 0

    # ---- utils ------------------------------------------------------------ #
    @staticmethod
    def _to_path(url: str) -> str:
        """Acepta URL completa o path; devuelve solo el path '/api/...'."""
        if url.startswith("http"):
            i = url.find("/api/")
            return url[i:] if i >= 0 else url
        return url

    def _throttle(self):
        if self._min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_req
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_req = time.monotonic()

    def _escalate(self, reason):
        if self.strategy_idx < len(self.strategies) - 1:
            old = self.strategies[self.strategy_idx][0]
            self.strategy_idx += 1
            new = self.strategies[self.strategy_idx][0]
            self._consec_fail = 0
            print(f"\n[HTTP] '{old}' fallando ({reason}). Cambio a '{new}'...\n")
        else:
            raise FetchError(f"Todas las estrategias agotadas. Ultimo motivo: {reason}")

    # ---- estrategia curl_cffi -------------------------------------------- #
    def _get_session(self):
        if self._session is None:
            from curl_cffi import requests as cf_requests
            targets = [self.impersonate] if self.impersonate else _IMPERSONATE_CANDIDATES
            last_exc = None
            for target in targets:
                try:
                    s = cf_requests.Session(impersonate=target)
                    s.headers.update(_SOFA_HEADERS)
                    self._session = s
                    self._impersonate_used = target
                    break
                except Exception as e:  # noqa: BLE001  perfil no soportado
                    last_exc = e
            if self._session is None:
                raise FetchError(f"curl_cffi sin targets validos: {last_exc}")
        return self._session

    def _curl_get(self, base, path):
        s = self._get_session()
        url = base + path
        r = s.get(url, timeout=30)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and data.get("error"):
                raise FetchError(f"API error {data.get('error')}")
            return data
        if r.status_code == 404:
            raise NotFound(url)
        if r.status_code == 429:
            wait = 20 + random.uniform(0, 10)
            print(f"[HTTP] 429 rate-limit. Esperando {wait:.0f}s...")
            time.sleep(wait)
            raise FetchError("429")
        raise FetchError(f"HTTP {r.status_code}")

    # ---- estrategia navegador (Selenium) --------------------------------- #
    def _get_driver(self):
        if self._driver is None:
            try:
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
            except ImportError:
                raise FetchError(
                    "Estrategia navegador requiere selenium: pip install selenium")
            opts = Options()
            if self._headless:
                opts.add_argument("--headless=new")
            opts.add_argument("--window-size=1920,1080")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--log-level=3")
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)
            d = webdriver.Chrome(options=opts)
            d.set_script_timeout(60)
            d.get("https://www.sofascore.com/es/")
            # esperar a que Cloudflare deje pasar (challenge = titulo "just a moment")
            for _ in range(30):
                title = (d.title or "").lower()
                if "just a moment" not in title and "un momento" not in title:
                    break
                time.sleep(2)
            else:
                d.quit()
                raise FetchError(
                    "Cloudflare no suelta el challenge en headless. "
                    "Ejecuta con --browser-visible")
            time.sleep(3)
            self._driver = d
        return self._driver

    def _browser_get(self, path):
        d = self._get_driver()
        res = d.execute_async_script(_PAGE_FETCH_JS, "https://www.sofascore.com" + path)
        if not res.get("ok"):
            raise FetchError(f"fetch JS: {res.get('err')}")
        status = res.get("status")
        if status == 200:
            try:
                data = json.loads(res["body"])
            except Exception:  # noqa: BLE001
                raise FetchError("Respuesta 200 no-JSON (posible challenge)")
            if isinstance(data, dict) and data.get("error"):
                raise FetchError(f"API error {data.get('error')}")
            return data
        if status == 404:
            raise NotFound(path)
        raise FetchError(f"HTTP {status} (browser)")

    # ---- API interna: cascada + reintentos ------------------------------- #
    def get_json(self, path, max_retries: int = 3):
        """Descarga JSON gestionando reintentos y escalado de estrategia.
        Devuelve el JSON (dict/list) o lanza NotFound / FetchError."""
        attempt = 0
        while True:
            attempt += 1
            name, kind, base = self.strategies[self.strategy_idx]
            self._throttle()
            try:
                data = (self._curl_get(base, path) if kind == "curl"
                        else self._browser_get(path))
                self._consec_fail = 0
                return data
            except NotFound:
                self._consec_fail = 0
                raise
            except Exception as e:  # noqa: BLE001
                err = e if isinstance(e, FetchError) else FetchError(str(e))
                self._consec_fail += 1
                last_strategy = self.strategy_idx >= len(self.strategies) - 1
                if self._consec_fail >= self.ESCALATE_AFTER and not last_strategy:
                    self._escalate(str(err))
                    attempt = 0
                elif attempt >= max_retries:
                    if not last_strategy:      # antes de rendirse, prueba la siguiente
                        self._escalate(str(err))
                        attempt = 0
                    else:
                        # el path en el mensaje hace el fallo ATRIBUIBLE en logs
                        # de cientos de peticiones
                        raise FetchError(f"{path}: {err}") from err
                else:
                    time.sleep(1.5 * attempt + random.uniform(0, 1))

    # ---- API publica (compatible con _fetch_json) ------------------------ #
    def fetch_json(self, url, *, retries: int = 3, wait: float = 1.5):
        """GET JSON con cascada. Devuelve {ok, data, err}. `wait` se ignora
        (la espera la gestiona `get_json`); se mantiene por compatibilidad."""
        path = self._to_path(url)
        try:
            data = self.get_json(path, max_retries=retries)
            return {"ok": True, "data": data}
        except NotFound:
            return {"ok": False, "err": "404", "data": None}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "err": str(e), "data": None}

    def close(self):
        if self._session is not None:
            try:
                self._session.close()
            except Exception:  # noqa: BLE001
                pass
            self._session = None
        if self._driver is not None:
            try:
                self._driver.quit()
            except Exception:  # noqa: BLE001
                pass
            self._driver = None


def _new_session(**kw) -> "SofaClient":
    """Crea un cliente Sofascore con cascada (arranca perezoso en la 1a peticion)."""
    defaults = dict(
        impersonate=_IMPERSONATE,
        rate_limit_rps=_RATE_LIMIT_RPS,
        headless=_BROWSER_HEADLESS,
    )
    defaults.update(kw)
    return SofaClient(**defaults)


def _fetch_json(session, url, *, retries: int = 3, wait: float = 1.5):
    """Delega en `SofaClient.fetch_json`. Devuelve {ok, data, err}.

    Se mantiene la firma para no tocar el resto de funciones (scrape_match,
    _fetch_seasons, _fetch_season_events, ...).
    """
    return session.fetch_json(url, retries=retries, wait=wait)


# ---- parseo (funciones puras, testeables sin red) ------------------------- #
def _safe_key(x) -> str:
    return re.sub(r"\s+", " ", str(x).strip()) if x else "NA"


def _coerce(v):
    """Convierte '58%', '12 (7)', '1.8' -> float; None si no aplica."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace("−", "-").replace("–", "-").replace("—", "-")
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except ValueError:
            return None
    m = re.match(r"^([-+]?\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else None


def parse_all_groups(stats_json: dict) -> dict:
    """Aplana el bloque period='ALL' -> {'ALL__grupo__item__home': v, ...}."""
    out = {}
    for blk in stats_json.get("statistics", []):
        if blk.get("period") != "ALL":
            continue
        for g in blk.get("groups", []):
            gname = _safe_key(g.get("groupName"))
            for item in g.get("statisticsItems", []):
                iname = _safe_key(item.get("name"))
                base = f"ALL__{gname}__{iname}"
                out[base + "__home"] = item.get("home")
                out[base + "__away"] = item.get("away")
    return out


def _extract_rating(p):
    if not isinstance(p, dict):
        return None
    for key in ("rating", "sofaScoreRating"):
        if key in (p.get("player") or {}):
            return p["player"][key]
    if "rating" in p:
        return p["rating"]
    stats = p.get("statistics") or {}
    return stats.get("rating") or stats.get("sofaScoreRating")


def _team_avg_rating(players) -> float | None:
    vals = []
    for p in players:
        r = _extract_rating(p)
        if r is not None:
            try:
                vals.append(float(r))
            except (TypeError, ValueError):
                pass
    return sum(vals) / len(vals) if vals else None


def _normalize_row(row: dict) -> dict:
    return {k: (v if k == "match_id" else _coerce(v)) for k, v in row.items()}


def _build_url(home_slug, away_slug, custom_id, match_id, lang=LANG) -> str:
    return f"https://www.sofascore.com/{lang}/football/match/{home_slug}-{away_slug}/{custom_id}#id:{match_id}"


def scrape_match(match_id, *, session=None, sleep_cf: float = 0.0) -> dict | None:
    """Descarga stats ALL + ratings de un partido.

    Devuelve:
      * dict con las stats  -> descarga correcta.
      * dict {'match_id', 'stats_missing': True} -> 404 REAL: Sofascore no tiene
        estadisticas publicadas para ese partido. Se persiste para no reintentar.
      * None -> fallo TRANSITORIO (403/red/estrategias agotadas). NO se persiste:
        el partido queda pendiente y se reintenta en la siguiente pasada.
    Antes ambos fallos devolvian una fila con solo match_id, que entraba al
    master y (por la idempotencia por match_id) convertia un 403 puntual en un
    HUECO PERMANENTE sin stats. Ver tambien `repair()`.

    `session`: SofaClient reutilizable; si es None crea (y cierra) uno.
    `sleep_cf`: pausa opcional antes de pedir (compat; dejalo en 0 salvo que
    quieras ralentizar por cortesia).
    """
    own = session is None
    s = session or _new_session()
    try:
        if sleep_cf:
            time.sleep(sleep_cf)
        stats_url = f"{_API}/event/{match_id}/statistics"
        lineups_url = f"{_API}/event/{match_id}/lineups"

        res_stats = _fetch_json(s, stats_url)
        if not res_stats.get("ok"):
            if res_stats.get("err") == "404":
                return {"match_id": match_id, "stats_missing": True}
            print(f"    [WARN] partido {match_id}: fallo transitorio bajando "
                  f"estadisticas ({res_stats.get('err')}); quedara pendiente.")
            return None
        stats_json = res_stats["data"]

        res_lineups = _fetch_json(s, lineups_url)
        if not res_lineups.get("ok") and res_lineups.get("err") != "404":
            # los ratings importan: si es fallo transitorio, mejor reintentar el
            # partido entero la proxima pasada que persistirlo sin ratings.
            print(f"    [WARN] partido {match_id}: fallo transitorio bajando "
                  f"alineaciones ({res_lineups.get('err')}); quedara pendiente.")
            return None
        lineups_json = res_lineups.get("data", {}) if res_lineups.get("ok") else {}
        if not isinstance(lineups_json, dict):
            lineups_json = {}

        row = parse_all_groups(stats_json)
        row["match_id"] = match_id
        home_players = (lineups_json.get("home") or {}).get("players") or []
        away_players = (lineups_json.get("away") or {}).get("players") or []
        row["home_avg_rating"] = _team_avg_rating(home_players)
        row["away_avg_rating"] = _team_avg_rating(away_players)
        return _normalize_row(row)
    finally:
        if own:
            s.close()


def _fetch_seasons(session) -> list[dict]:
    """Lista de temporadas de LaLiga. El cliente cascadea entre hosts solo."""
    url = f"{_API}/unique-tournament/{LALIGA_TOURNAMENT_ID}/seasons"
    res = _fetch_json(session, url)
    data = res.get("data") if res.get("ok") else None
    if isinstance(data, dict) and data.get("seasons"):
        return data["seasons"]
    # diagnostico util si el shape vuelve a cambiar / todo falla
    if isinstance(data, dict):
        shape = f"keys={list(data.keys())} error={data.get('error')!r}"
    else:
        shape = f"tipo={type(data).__name__} valor={str(data)[:200]!r}"
    raise RuntimeError(
        "No pude obtener 'seasons' de Sofascore (todas las estrategias fallaron). "
        f"Ultima respuesta: ok={res.get('ok')} {shape} err={res.get('err')}"
    )


def _fetch_season_events(session, season_id, max_pages: int = 80, *,
                         strict: bool = True) -> list[dict]:
    """Todos los eventos de una temporada, paginando hacia atras.

    La paginacion termina de cuatro formas LEGITIMAS: 404 (la pagina no existe),
    el sobre de error que la API usa con el mismo significado, una lista de
    eventos vacia, o `hasNextPage=False`. En cualquiera de ellas la cosecha esta
    completa y se devuelve lo acumulado.

    Un fallo de TRANSPORTE (`ok=False`: timeout, 403 del anti-bot, host caido) no
    es ninguna de las tres. Tratarlo como fin de paginacion -- un `break` indistinto
    que cortara la cosecha sin distinguir el motivo -- devolveria media temporada con
    la misma cara que una temporada entera: el llamante persiste lo que hay, el
    checkpoint marca la temporada como vista y los partidos que faltan no vuelven
    a pedirse nunca. Por eso aqui se propaga como `HarvestError`, con la pagina y el
    error concretos, y el llamante decide (reintentar mas tarde) sin publicar nada
    truncado. `strict=False` degrada el corte a aviso para exploracion manual.

    Agotar `max_pages` sin ninguna senal de fin tampoco es un final limpio: se
    avisa siempre, porque significa que la temporada tiene mas paginas de las
    previstas y la cosecha puede estar incompleta por arriba.
    """
    base = f"{_API}/unique-tournament/{LALIGA_TOURNAMENT_ID}/season/{season_id}"
    events: list[dict] = []
    fin_limpio = False
    paginas = 0
    for page in range(max_pages):
        paginas += 1
        res = _fetch_json(session, f"{base}/events/last/{page}")
        if not res.get("ok") and str(res.get("err")) == "404":
            fin_limpio = True                    # 404 = la pagina no existe -> fin
            break
        if not res.get("ok"):
            msg = (f"[sofascore] temporada {season_id}: fallo de transporte en la "
                   f"pagina {page} (err={res.get('err')!r}) tras acumular "
                   f"{len(events)} evento(s). NO es fin de paginacion: la cosecha "
                   "quedaria truncada. Re-ejecuta el mismo comando mas tarde.")
            if strict:
                raise _common.HarvestError(msg)
            print("[WARN] " + msg)
            break
        data = res["data"]
        if isinstance(data, dict) and data.get("error"):
            fin_limpio = True                    # sobre de error = pagina inexistente
            break
        page_events = data.get("events") if isinstance(data, dict) else None
        if not page_events:
            fin_limpio = True                    # pagina vacia = no queda historico
            break
        events.extend(page_events)
        if isinstance(data, dict) and data.get("hasNextPage") is False:
            fin_limpio = True                    # la propia API declara el final
            break
    if not fin_limpio and events and paginas == max_pages:
        print(f"[WARN] [sofascore] temporada {season_id}: agotadas las "
              f"{max_pages} paginas sin senal de fin ({len(events)} eventos). "
              "Puede faltar historico; sube max_pages si se repite.")
    return events


def _season_year(season: dict) -> int:
    """Ano de inicio de una season ('16/17'->2016, '99'->1999)."""
    raw = season.get("year", "0")
    if not raw:
        return 0
    s = str(raw).split("/")[0]
    try:
        val = int(s)
    except ValueError:
        return 0
    if val > 1000:
        return val
    return (1900 + val) if val > 50 else (2000 + val)


def _event_to_meta(e: dict, season_id) -> dict:
    """Meta de un evento. Lanza ValueError si faltan los campos IMPRESCINDIBLES
    (id, nombres de equipo); los opcionales (slug, customId, ...) degradan a
    None sin romper. El llamante decide que hacer con los eventos malformados."""
    home, away = e.get("homeTeam") or {}, e.get("awayTeam") or {}
    if e.get("id") is None or not home.get("name") or not away.get("name"):
        raise ValueError(
            f"evento malformado (id={e.get('id')!r}, "
            f"home={home.get('name')!r}, away={away.get('name')!r}, "
            f"claves presentes={sorted(e)[:12]})"
        )
    row = {
        "match_id": e["id"],
        "round": (e.get("roundInfo") or {}).get("round"),
        "home": home["name"],
        "away": away["name"],
        "home_slug": home.get("slug"),
        "away_slug": away.get("slug"),
        "customId": e.get("customId"),
        "timestamp": e.get("startTimestamp"),
        "status": (e.get("status") or {}).get("type"),
        "season_id": season_id,
    }
    row["url"] = _build_url(row["home_slug"], row["away_slug"], row["customId"], e["id"])
    return row


def _finished_metas(events, season_id, season_name, existing) -> list[dict]:
    """Metas de los eventos FINALIZADOS que no estan ya en el master.

    Los eventos malformados (Sofascore cambia el shape a veces) se saltan con un
    aviso ATRIBUIBLE (temporada + detalle) en vez de reventar toda la pasada."""
    metas: list[dict] = []
    for e in events:
        if (e.get("status") or {}).get("type") != "finished":
            continue
        if e.get("id") in existing:
            continue
        try:
            metas.append({**_event_to_meta(e, season_id), "season_name": season_name})
        except ValueError as ex:
            print(f"  [WARN] temporada {season_name}: {ex}; se omite ese evento.")
    return metas


_MAX_FAIL_STREAK = 5  # fallos transitorios consecutivos antes de abortar la pasada


def _abort_if_streak(fail_streak: int, df_master, new_rows, master_csv) -> None:
    """Si el transporte esta caido (N fallos seguidos), guarda lo descargado y
    corta con un mensaje accionable, en vez de quemar horas fallando en bucle."""
    if fail_streak < _MAX_FAIL_STREAK:
        return
    _save_checkpoint(new_rows, master_csv)
    _finalize_master(df_master, new_rows, master_csv)
    raise SystemExit(
        f"\n[ERROR] {fail_streak} partidos seguidos con fallo de descarga: el "
        "transporte a Sofascore esta caido (Cloudflare/red), no es un problema "
        "de datos. Lo descargado hasta ahora esta guardado. Prueba, por este "
        "orden: 1) esperar 30-60 min; 2) --browser-visible; 3) cambiar de red. "
        "Al re-ejecutar el mismo comando se reanuda solo (los partidos "
        "pendientes se reintentan)."
    )


def backfill(
    seasons=None,
    *,
    master_csv: Path | str = _MATCH_STATS_CSV,
    sleep_cf: float = 0.0,
    pause: float = 0.4,
) -> pd.DataFrame:
    """Descarga TODO el historico de temporadas indicadas al master CSV.

    `seasons`: anos de inicio (2016 = 16/17). Si no se pasa, se deducen de
    `FIRST_SEASON` a la temporada vigente. Idempotente: salta los match_id
    que ya esten en el master.
    """
    if seasons is None:
        seasons = _todas_las_temporadas()
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)
    df_master, existing = _load_master(master_csv)

    # reanudacion: los partidos ya descargados (checkpoint) no se vuelven a bajar
    df_ckpt, ckpt_ids = _load_checkpoint(master_csv)
    new_rows: list = df_ckpt.to_dict("records") if not df_ckpt.empty else []
    existing = existing | ckpt_ids

    session = _new_session()
    try:
        all_seasons = _fetch_seasons(session)
        want = set(int(y) for y in seasons)
        target = [s for s in all_seasons if _season_year(s) in want]

        fail_streak = 0
        for s in sorted(target, key=_season_year):
            sid, sname = s["id"], s.get("name")
            print(f"\n=== Temporada {sname} (ID={sid}) ===")
            # si el transporte cae aqui, _fetch_season_events lanza FetchError en
            # vez de devolver media temporada: preferimos abortar y reanudar (el
            # checkpoint guarda cada partido ya bajado) a persistir un hueco mudo.
            events = _fetch_season_events(session, sid)
            metas = _finished_metas(events, sid, sname, existing)
            # dedup partidos aplazados
            dfm = pd.DataFrame(metas)
            if not dfm.empty:
                dfm = (dfm.sort_values(["round", "timestamp", "match_id"])
                          .drop_duplicates("match_id", keep="last")
                          .drop_duplicates(["round", "home", "away"], keep="last"))
            print(f"  partidos nuevos: {len(dfm)}")
            for i, (_, r) in enumerate(dfm.iterrows(), 1):
                mid = int(r["match_id"])
                print(f"  [{i}/{len(dfm)}] {mid}")
                stat = scrape_match(mid, session=session, sleep_cf=sleep_cf)
                if stat is None:  # fallo transitorio -> pendiente, cuenta racha
                    fail_streak += 1
                    _abort_if_streak(fail_streak, df_master, new_rows, master_csv)
                    continue
                fail_streak = 0
                new_rows.append({**r.to_dict(), **stat, "season_name": sname})
                _save_checkpoint(new_rows, master_csv)  # persiste tras cada partido
                time.sleep(pause)
    finally:
        session.close()

    return _finalize_master(df_master, new_rows, master_csv)


def update(
    *,
    master_csv: Path | str = _MATCH_STATS_CSV,
    sleep_cf: float = 0.0,
    pause: float = 0.4,
    recent_seasons: int = 2,
) -> pd.DataFrame:
    """EJECUTABLE SEMANALMENTE. Anade al master los partidos finalizados nuevos
    (los que aun no estan en el CSV).

    Escanea las `recent_seasons` temporadas mas recientes (por defecto 2 =
    actual + anterior), NO solo la ultima. Esto cubre dos casos:
      1. Actualizacion semanal en plena temporada (partidos nuevos de la actual).
      2. Cambio de temporada: Sofascore crea la nueva temporada (p.ej. 26/27)
         con 0 partidos jugados antes de que la anterior (25/26) termine de
         volcarse; mirar solo `seasons[0]` dejaria huecos. Al escanear tambien
         la anterior, esos partidos finalizados pendientes se recuperan.
    """
    master_csv = Path(master_csv)
    master_csv.parent.mkdir(parents=True, exist_ok=True)
    df_master, existing = _load_master(master_csv)

    # reanudacion: los partidos ya descargados (checkpoint) no se vuelven a bajar
    df_ckpt, ckpt_ids = _load_checkpoint(master_csv)
    new_rows: list = df_ckpt.to_dict("records") if not df_ckpt.empty else []
    existing = existing | ckpt_ids

    session = _new_session()
    try:
        seasons = sorted(_fetch_seasons(session), key=_season_year, reverse=True)
        targets = seasons[:max(1, recent_seasons)]
        print(f"[INFO] Temporadas a escanear: {[s['name'] for s in targets]}")

        now = time.time()
        metas = []
        for s in targets:
            # idem backfill: un fallo de transporte aborta el update. Un update
            # que "no encuentra partidos nuevos" por culpa de una pagina caida es
            # indistinguible de una jornada sin jugar, y esa es la peor lectura.
            events = _fetch_season_events(session, s["id"])
            found = [m for m in _finished_metas(events, s["id"], s["name"], existing)
                     if (m.get("timestamp") or 0) < now]
            print(f"  [{s['name']}] partidos nuevos finalizados: {len(found)}")
            metas.extend(found)

        # de-dup por match_id por si un evento apareciese en dos temporadas
        seen_ids: set = set()
        metas_unique = []
        for m in metas:
            mid = m.get("match_id")
            if mid in seen_ids:
                continue
            seen_ids.add(mid)
            metas_unique.append(m)

        df_missing = pd.DataFrame(metas_unique)
        print(f"[INFO] partidos nuevos pendientes (total): {len(df_missing)}")

        fail_streak = 0
        for i, (_, r) in enumerate(df_missing.iterrows(), 1):
            mid = int(r["match_id"])
            print(f"  [{i}/{len(df_missing)}] {mid}")
            stat = scrape_match(mid, session=session, sleep_cf=sleep_cf)
            if stat is None:  # fallo transitorio -> pendiente, cuenta racha
                fail_streak += 1
                _abort_if_streak(fail_streak, df_master, new_rows, master_csv)
                continue
            fail_streak = 0
            new_rows.append({**r.to_dict(), **stat})
            _save_checkpoint(new_rows, master_csv)  # persiste tras cada partido
            time.sleep(pause)
    finally:
        session.close()

    return _finalize_master(df_master, new_rows, master_csv)


def _load_master(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el CSV maestro de estadisticas.

    Si aun no hay maestro, lo reconstruye a partir de ficheros por temporada
    `laliga_*.csv`. Devuelve (dataframe, conjunto de match_id ya presentes).
    """
    if master_csv.exists():
        df = pd.read_csv(master_csv)
        ids = set(df["match_id"].unique()) if "match_id" in df.columns else set()
        print(f"[INFO] master cargado: {len(df)} partidos.")
        return df, ids
    parts = [f for f in glob.glob(str(master_csv.parent / "laliga_*_sofascore.csv"))
             if "MASTER" not in f]
    if not parts:
        print("[INFO] sin datos previos; se empieza de cero.")
        return pd.DataFrame(), set()
    dfs = []
    for f in parts:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] part-file '{Path(f).name}' ilegible ({e}); se omite "
                  "(sus partidos se re-descargaran al no estar en el master).")
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    ids = set(df["match_id"].unique()) if "match_id" in df.columns else set()
    return df, ids


# --------------------------------------------------------------------------- #
# Guardado a prueba de fallos (disenado para "ejecutar una vez y olvidarse")
# --------------------------------------------------------------------------- #
# La descarga es larga (cientos de partidos a ~1.5 s), asi que el guardado esta
# pensado para no perder nunca nada y no exigir ninguna accion manual:
#   * checkpoint incremental: cada partido descargado se vuelca al instante a
#     `_ckpt_<nombre>.csv`, de modo que un corte o un reinicio no pierden trabajo.
#   * escritura atomica: el CSV maestro se escribe primero a un temporal y se
#     publica con `os.replace` (operacion atomica), dejando una copia `.bak`.
#   * reanudacion automatica: al re-ejecutar, los match_id del checkpoint se
#     tratan como ya presentes -> no se vuelven a descargar y se funden.
#   * escritura garantizada en Windows: antes de guardar se retira el atributo
#     "solo lectura" (que servicios de sincronizacion como OneDrive suelen poner)
#     y, si el destino esta ocupado un instante, se reintenta con espera.
# El resultado: una sola llamada a `update()`/`weekly` deja los datos guardados.
def _ckpt_path(master_csv: Path) -> Path:
    return _common.ckpt_path(master_csv)


def _load_checkpoint(master_csv: Path) -> tuple[pd.DataFrame, set]:
    """Carga el checkpoint pendiente (partidos ya descargados sin volcar al master)."""
    return _common.load_checkpoint(master_csv, key_col="match_id")


def _save_checkpoint(new_rows: list, master_csv: Path) -> None:
    """Reescribe el checkpoint con todas las filas descargadas hasta ahora."""
    _common.save_checkpoint(new_rows, master_csv)


# alias finos: la implementacion vive en _common (compartida por TODOS los
# extractores); estos nombres se conservan porque transfermarkt / fatigue /
# availability los importan como `sofa._write_df_safe`.
_clear_readonly = _common.clear_readonly
_write_df_safe = _common.write_df_safe


def _finalize_master(df_master: pd.DataFrame, new_rows: list,
                     master_csv: Path) -> pd.DataFrame:
    """Funde los partidos nuevos en el maestro, lo guarda y limpia el checkpoint."""
    if not new_rows:
        print("[INFO] no hay partidos nuevos: el maestro ya esta al dia.")
        return df_master
    df_new = pd.DataFrame(new_rows)
    if "timestamp" in df_new.columns:
        df_new["match_date"] = (pd.to_datetime(df_new["timestamp"], unit="s", utc=True)
                                  .dt.tz_convert("Europe/Madrid"))
    df_master = pd.concat([d for d in (df_master, df_new) if not d.empty],
                          ignore_index=True)
    if "match_id" in df_master.columns:
        # El maestro (primero en el concat) manda: las stats de un partido
        # FINALIZADO no cambian, y los extractores tipo upsert (footballdata,
        # clubelo, ...) son los que usan keep='last' porque su dato SI se
        # completa a posteriori. La unica excepcion es la fila incompleta —
        # capturada con el partido en juego o con el scraping interrumpido—, que
        # un "gana el primero" estricto congelaria para siempre; por eso el
        # desempate lo resuelve la completitud (ver `_common`).
        df_master = _common.dedupe_preferring_complete(df_master, "match_id")
    if "timestamp" in df_master.columns:
        df_master = df_master.sort_values("timestamp").reset_index(drop=True)
    if _write_df_safe(df_master, master_csv, fmt="csv"):
        print(f"[OK] estadisticas guardadas -> {master_csv}  ({len(df_master)} partidos)")
        ckpt = _ckpt_path(master_csv)
        if ckpt.exists():
            try:
                ckpt.unlink()
            except Exception:  # noqa: BLE001
                pass
    return df_master


# =========================================================================== #
# B) CAPA DE ADAPTACION / NORMALIZACION  (solo pandas, testeable offline)
# =========================================================================== #
def _snake(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("%", "pct").replace("+", "_plus_").replace("/", "_per_").replace("-", "_")
    s = re.sub(r"[^\w]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _rename_stat_col(col: str) -> str:
    """'ALL__Shots__Total shots__home' -> 'sofa_shots_total_shots_home'."""
    parts = col.split("__")
    if parts and parts[0] == "ALL":
        parts = parts[1:]
    return "sofa_" + "_".join(_snake(p) for p in parts if p)


def _key_from_slug_or_name(slug, name) -> str:
    """Normaliza usando primero el slug de Sofascore, luego el nombre display."""
    for cand in (slug, name):
        if cand is None or (isinstance(cand, float) and pd.isna(cand)):
            continue
        key = teamnames.to_key(cand)
        # to_key devuelve un slug de reserva si no reconoce; aceptamos solo
        # las claves canonicas reales (presentes en CANONICAL)
        if key in teamnames.CANONICAL:
            return key
    # ninguno reconocido -> mejor esfuerzo con el nombre display
    return teamnames.to_key(name if name is not None else slug)


def stats_missing_mask(df: pd.DataFrame) -> pd.Series:
    """Filas marcadas `stats_missing=True`: partidos que Sofascore reconoce pero
    para los que NO publica estadisticas (404 real en el endpoint de statistics).

    Estas filas se conservan a proposito en el parquet: son el unico puente
    `match_id -> match_key` que usan los reducers de fatiga y disponibilidad
    (`consolidate/reducers.event_to_match_key`), de modo que eliminarlas dejaria
    esos partidos sin fatiga ni bajas en el master. Lo que si se hace es
    excluirlas EXPLICITAMENTE del calculo de cobertura (no son un fallo de
    descarga) y mantener sus `sofa_*` a NaN, nunca a 0.
    """
    if "stats_missing" not in df.columns:
        return pd.Series(False, index=df.index)
    s = df["stats_missing"]
    if s.dtype == bool:
        return s
    return (s.astype(str).str.strip().str.lower()
             .isin(("true", "1", "1.0", "yes", "si")))


def assert_stats_coverage(df: pd.DataFrame, *, strict: bool = True,
                          min_stat_cols: int = _MIN_STAT_COLS,
                          min_col_coverage: float = _MIN_COL_COVERAGE) -> dict:
    """Comprueba que el bloque de estadisticas `sofa_*` esta REALMENTE poblado.

    Las validaciones de esquema previas cubren claves y nombres de equipo, pero
    un master con las ~200 columnas de estadistica vacias las superaria todas: el
    parquet se publicaria con `match_key` valido y NaN en todo, y el modelo se
    entrenaria sobre imputaciones sin que nada lo advirtiera. Se comprueban tres
    cosas:

      1. que el renombrado ha producido el bloque esperado de columnas;
      2. que ninguna fila es un HUECO (sin estadisticas y sin la marca
         `stats_missing`), que es sintoma de descarga fallida -> `repair()`;
      3. que ninguna columna esta vacia del todo, avisando de las que bajan del
         umbral de cobertura (normal en metricas que no cubren todo el historico).

    Devuelve un resumen con los recuentos para el informe.
    """
    stat_cols = [c for c in df.columns if c.startswith("sofa_")]
    faltan = stats_missing_mask(df)
    con_stats = ~faltan
    problemas: list[str] = []

    if len(stat_cols) < min_stat_cols:
        problemas.append(
            f"solo {len(stat_cols)} columnas 'sofa_*' (se esperan >= "
            f"{min_stat_cols}): el CSV maestro no trae las tablas de "
            "estadisticas o Sofascore cambio el formato de sus grupos.")

    huecos = pd.Series(False, index=df.index)
    vacias: list[str] = []
    pobres: list[tuple[str, float]] = []
    if stat_cols:
        sin_dato = df[stat_cols].isna().all(axis=1)
        huecos = sin_dato & con_stats
        if huecos.any():
            ids = df.loc[huecos, "match_id"].tolist() if "match_id" in df else []
            problemas.append(
                f"{int(huecos.sum())} partido(s) sin ninguna estadistica y SIN la "
                f"marca stats_missing {ids[:10]}{' ...' if len(ids) > 10 else ''}: "
                "son huecos de descarga, no ausencias reales. Ejecuta "
                "'sofascore repair' y despues 'backfill'/'update'.")
        base = df.loc[con_stats & ~sin_dato, stat_cols]
        if len(base):
            cov = base.notna().mean()
            vacias = sorted(cov[cov == 0].index)
            pobres = sorted(((c, float(v)) for c, v in cov.items()
                             if 0 < v < min_col_coverage), key=lambda t: t[1])
        if vacias:
            problemas.append(
                f"{len(vacias)} columna(s) de estadistica COMPLETAMENTE vacias "
                f"{vacias[:10]}{' ...' if len(vacias) > 10 else ''}: no aportan "
                "senal y suelen indicar que la tabla de origen cambio de nombre.")

    if problemas:
        msg = "[sofascore.load_processed] cobertura de estadisticas: " + \
              " | ".join(problemas)
        if strict:
            raise _common.SchemaError(msg)
        print("[WARN] " + msg)
    if pobres:
        detalle = ", ".join(f"{c} ({v:.0%})" for c, v in pobres[:8])
        print(f"[WARN] {len(pobres)} columna(s) por debajo del "
              f"{min_col_coverage:.0%} de cobertura: {detalle}"
              f"{' ...' if len(pobres) > 8 else ''}. Puede ser legitimo si la "
              "metrica no existe en todo el historico.")

    return {"n_stat_cols": len(stat_cols), "n_sin_stats": int(faltan.sum()),
            "n_huecos": int(huecos.sum()), "cols_vacias": vacias,
            "cols_pobres": [c for c, _ in pobres]}


def load_processed(
    master_csv: Path | str = _MATCH_STATS_CSV,
    *,
    out_parquet: Path | str | None = _OUT_PARQUET,
    verbose: bool = True,
    strict: bool = True,
) -> pd.DataFrame:
    """Lee el CSV de estadisticas, normaliza y devuelve/guarda el parquet limpio.

    Salida: una fila por partido con
        match_key, home_key, away_key, match_date, season, round,
        home_avg_rating, away_avg_rating, y todas las sofa_*_home/away.

    `strict=True` (defecto): falla RUIDOSAMENTE (SchemaError/ValueError) si el
    master no cumple el contrato minimo, si algun match_key queda nulo o si un
    equipo no tiene clave canonica. Pensado para el pipeline automatico: un
    fallo aqui significa "arregla teamnames/el master y re-ejecuta process",
    nunca corrompe el parquet publicado.
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        raise _common.RegressionError(
            f"[sofascore.load_processed] no existe el maestro '{master_csv}'. "
            "Publicar un parquet vacio destruiria el vigente (las aserciones de "
            "esquema no detectan un frame sin filas). Ejecuta antes el backfill.")
    df = pd.read_csv(master_csv)
    _common.assert_columns(df, ["match_id", "home", "away", "match_date"],
                           source="sofascore.load_processed")

    # 1) renombrar columnas de estadisticas ALL__ -> sofa_...
    stat_cols = [c for c in df.columns if c.startswith("ALL__")]
    df = df.rename(columns={c: _rename_stat_col(c) for c in stat_cols})

    # 2) normalizar equipos (slug primero, nombre display de reserva)
    home_slug = df["home_slug"] if "home_slug" in df.columns else pd.Series([None] * len(df))
    away_slug = df["away_slug"] if "away_slug" in df.columns else pd.Series([None] * len(df))
    df["home_key"] = [_key_from_slug_or_name(s, n) for s, n in zip(home_slug, df["home"])]
    df["away_key"] = [_key_from_slug_or_name(s, n) for s, n in zip(away_slug, df["away"])]

    # 3) fecha LOCAL (Europe/Madrid) + match_key.
    #    match_date es "YYYY-MM-DD HH:MM:SS+TZ" en hora de Madrid (igual que el
    #    dia de partido que usan las demas fuentes), asi que tomamos los 10
    #    primeros caracteres; solo si el formato no es ISO, recurrimos a parsear.
    raw = df["match_date"].astype("string")
    date = raw.str.slice(0, 10)
    bad = ~date.str.match(r"^\d{4}-\d{2}-\d{2}$").fillna(False)
    if bad.any():
        parsed = pd.to_datetime(df["match_date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
        date = date.mask(bad, parsed)
    df["date"] = date
    df["match_key"] = (date.astype("string")
                       .str.cat(df["home_key"].astype("string"), sep="|")
                       .str.cat(df["away_key"].astype("string"), sep="|"))

    # 4) temporada legible
    if "season_name" in df.columns:
        df["season"] = df["season_name"]

    # 5) validaciones ruidosas (antes de publicar nada)
    if strict:
        _common.assert_non_null(df, ["match_key", "home_key", "away_key"],
                                source="sofascore.load_processed")
        _common.assert_unique(df, "match_key", source="sofascore.load_processed")
        bad_names = set()
        for side in ("home", "away"):
            m = ~df[f"{side}_key"].isin(teamnames.CANONICAL)
            bad_names |= set(df.loc[m, side].dropna().unique())
        if bad_names:
            teamnames.assert_known(bad_names, source="sofascore.load_processed")

    # 6) cobertura REAL del bloque de estadisticas (se comprueba siempre; en modo
    #    aviso si strict=False). Un master con las ~200 columnas vacias supera
    #    todas las validaciones de esquema anteriores.
    cobertura = assert_stats_coverage(df, strict=strict)

    if verbose:
        _report(df, stat_cols, cobertura)

    if out_parquet is not None:
        _common.publish_df(df, out_parquet, source="sofascore.load_processed",
                           fmt="parquet", verbose=verbose)
    return df


def _report(df: pd.DataFrame, stat_cols, cobertura: dict | None = None) -> None:
    print("\n===== INFORME COBERTURA Sofascore =====")
    print(f"partidos              : {len(df)}")
    if "match_key" in df:
        print(f"match_key unicos      : {df['match_key'].nunique()}")
    print(f"columnas de stats ALL : {len(stat_cols)}")
    if cobertura:
        print(f"columnas 'sofa_*'     : {cobertura['n_stat_cols']} "
              f"(minimo exigido {_MIN_STAT_COLS})")
        print(f"sin stats en Sofascore: {cobertura['n_sin_stats']} "
              "(marcados stats_missing; ausencia REAL, no fallo de descarga)")
        print(f"huecos de descarga    : {cobertura['n_huecos']} "
              "(deben ser 0; si no, 'repair' + 'backfill')")
        print(f"columnas vacias       : {len(cobertura['cols_vacias'])} | "
              f"bajo cobertura: {len(cobertura['cols_pobres'])}")
    if "date" in df:
        d = pd.to_datetime(df["date"], errors="coerce")
        print(f"rango fechas          : {d.min()} -> {d.max()}")
    for col in ("home_avg_rating", "away_avg_rating"):
        if col in df:
            print(f"{col:20s}: {df[col].notna().mean() * 100:.1f}% no nulo")
    # equipos no reconocidos (clave no canonica)
    bad = set()
    for side in ("home", "away"):
        keycol, namecol = f"{side}_key", side
        if keycol in df and namecol in df:
            m = ~df[keycol].isin(teamnames.CANONICAL)
            bad |= set(df.loc[m, namecol].dropna().unique())
    print(f"equipos NO reconocidos: {sorted(bad) if bad else '[] (ok)'}")
    print("=======================================\n")



def repair(master_csv: Path | str = _MATCH_STATS_CSV) -> pd.DataFrame:
    """Purga del master las filas HUECO heredadas (meta sin ninguna stat ALL__
    y sin la marca stats_missing=True): son partidos cuya descarga fallo de
    forma transitoria en versiones anteriores del extractor y que la
    idempotencia por match_id dejaba bloqueados para siempre.

    Al eliminarlas, el siguiente `backfill`/`update` los re-descarga; los que
    de verdad no tienen stats en Sofascore quedaran marcados stats_missing=True
    y no se volveran a intentar. Ejecutar UNA VEZ tras actualizar el extractor.
    """
    master_csv = Path(master_csv)
    if not master_csv.exists():
        print("[INFO] no hay master; nada que reparar.")
        return pd.DataFrame()
    df = pd.read_csv(master_csv)
    stat_cols = [c for c in df.columns if c.startswith("ALL__")]
    no_stats = (df[stat_cols].isna().all(axis=1) if stat_cols
                else pd.Series(True, index=df.index))
    flagged = (df["stats_missing"].fillna(False).astype(bool)
               if "stats_missing" in df.columns
               else pd.Series(False, index=df.index))
    gaps = no_stats & ~flagged
    n = int(gaps.sum())
    if not n:
        print("[OK] sin huecos: todas las filas tienen stats o estan marcadas "
              "stats_missing.")
        return df
    ids = df.loc[gaps, "match_id"].tolist()
    print(f"[INFO] {n} fila(s) hueco detectadas (meta sin stats ni marca): "
          f"{ids[:15]}{' ...' if n > 15 else ''}")
    df_clean = df[~gaps].reset_index(drop=True)
    if _write_df_safe(df_clean, master_csv, fmt="csv"):
        print(f"[OK] master purgado -> {master_csv} ({len(df_clean)} filas). "
              "Ejecuta ahora 'backfill' (o 'update' si son recientes) para "
              "re-descargar esos partidos.")
    return df_clean


# =========================================================================== #
# CLI
# =========================================================================== #
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Extractor Sofascore LaLiga.")
    p.add_argument("cmd", choices=["backfill", "update", "process", "weekly", "repair"],
                   help="backfill=historico | update=nuevos | process=normaliza | "
                        "weekly=update+process | repair=purga huecos meta-sin-stats")
    p.add_argument("--seasons", nargs="+", type=int,
                   default=_todas_las_temporadas())
    p.add_argument("--master", default=str(_MATCH_STATS_CSV))
    p.add_argument("--recent", type=int, default=2,
                   help="update/weekly: n temporadas mas recientes a escanear (def. 2)")
    p.add_argument("--impersonate", default=None,
                   help="fuerza un perfil de curl_cffi (p.ej. chrome131). "
                        "Por defecto se autodetecta probando varios.")
    p.add_argument("--rate", type=float, default=_RATE_LIMIT_RPS,
                   help=f"peticiones/seg (def. {_RATE_LIMIT_RPS})")
    p.add_argument("--browser-visible", action="store_true",
                   help="estrategia 3 (Selenium) con Chrome VISIBLE; util si "
                        "Cloudflare se pone pesado en headless")
    args = p.parse_args()

    if args.impersonate:
        _IMPERSONATE = args.impersonate
    _RATE_LIMIT_RPS = args.rate
    _BROWSER_HEADLESS = not args.browser_visible

    if args.cmd == "backfill":
        backfill(seasons=args.seasons, master_csv=args.master)
    elif args.cmd == "update":
        update(master_csv=args.master, recent_seasons=args.recent)
    elif args.cmd == "process":
        load_processed(master_csv=args.master)
    elif args.cmd == "repair":
        repair(master_csv=args.master)
    elif args.cmd == "weekly":
        update(master_csv=args.master, recent_seasons=args.recent)
        # si el guardado del master fallo, su checkpoint sigue ahi: NO regeneres
        # el parquet (saldria con datos viejos). Avisa y sal sin re-procesar.
        if _ckpt_path(Path(args.master)).exists():
            print("[WARN] el master no se guardo (checkpoint pendiente); "
                  "NO regenero el parquet para no dejarlo desactualizado. "
                  "Resuelve el bloqueo y re-ejecuta 'weekly'.")
        else:
            load_processed(master_csv=args.master)
