"""
Maquinaria COMPARTIDA de los extractores de laliga-predictor-engine.

Modulo PRIVADO (no forma parte de la API publica de ningun extractor): concentra
el codigo que antes estaba copiado literalmente en cada modulo (understat,
clubelo, footballdata, fbref, sofascore), de modo que un arreglo en el guardado,
el checkpoint o el HTTP se aplique a TODAS las fuentes a la vez.

Cada extractor conserva sus nombres privados historicos (`_write_df_safe`,
`_ckpt_path`, `_http_get`, ...) como alias finos a estas funciones: ni la API
publica ni los tests cambian.

Contenido
---------
* http_get / http_get_bytes : GET simple con curl_cffi (impersonando Chrome) si
  esta instalado y urllib si no. NO lanza por codigos HTTP (el llamante decide).
  Reintenta con espera creciente en lo transitorio -red, 429 y 5xx- y devuelve
  los 4xx en el primer intento, porque un 403 es la senal de escalada de
  `understat.py` y `sofascore.py` y tiene que llegar pronto.
* write_df_safe / clear_readonly : escritura atomica (temporal + os.replace)
  con reintentos ante candados de OneDrive/antivirus y fallback in situ.
  NOTA: el fallback in situ NO es atomico (si matan el proceso a mitad, el
  fichero puede quedar truncado); por eso el CSV maestro se guarda con
  backup=True (queda un .bak) y los parquet, regenerables desde el CSV, con
  backup=False.
* write_df_required : el mismo guardado, pero levantando `WriteError` si no llega
  a disco. Es la via de los constructores (`build_*`), cuyo resultado solo existe
  en memoria: alli un booleano ignorado deja en disco el artefacto de la pasada
  anterior y el resto del pipeline se calcula sobre un dato viejo sin sintoma.
  Los extractores siguen con `write_df_safe`, porque su fuente de verdad -el CSV
  maestro- sobrevive al fallo y la siguiente pasada reanuda.
* ckpt_path / load_checkpoint / save_checkpoint : reanudacion de descargas.
* current_season_year : ano de inicio de la temporada vigente (agosto).
* SchemaError + assert_columns / assert_non_null / assert_unique : validaciones
  RUIDOSAS para las capas de normalizacion. Fallan con un mensaje atribuible
  (modulo, columna, filas de ejemplo) en vez de dejar pasar datos corruptos
  aguas abajo. El crudo descargado nunca se toca: arreglar y re-ejecutar
  'process' basta.
* RegressionError + publish_df : publicacion NO REGRESIVA de un artefacto
  procesado. Las aserciones de esquema son necias sobre un frame vacio (no hay
  columna que falte, ni nulo, ni duplicado), asi que un maestro ausente o
  truncado podia sobrescribir un parquet bueno con cero filas y pasar todos los
  controles. `publish_df` compara contra el artefacto VIGENTE antes de escribir.
"""
from __future__ import annotations

import os
import shutil
import stat
import time
from datetime import datetime
from pathlib import Path

import pandas as pd


class SchemaError(RuntimeError):
    """El dato procesado no cumple el contrato de esquema esperado."""


class RegressionError(RuntimeError):
    """La regeneracion publicaria un artefacto PEOR que el vigente."""


class WriteError(RuntimeError):
    """El artefacto no se pudo dejar en disco y el contenido se ha perdido.

    Se reserva para los constructores del pipeline (`build_*`), cuyo resultado es
    el producto de un calculo largo y determinista que no queda guardado en
    ningun otro sitio: si la escritura no llega a disco, la ejecucion ha
    fracasado y tiene que decirlo. Los extractores no la usan porque su fuente de
    verdad es el CSV maestro, que sigue intacto: alli basta con avisar y reanudar.
    """


class HarvestError(RuntimeError):
    """La cosecha se interrumpio por transporte, no por fin de datos.

    Se distingue del `FetchError` de `sofascore.py`, que es el error de UNA
    peticion HTTP y lo absorbe el propio cliente (`fetch_json` lo traduce a
    `{"ok": False}`). `HarvestError` opera un nivel por encima: la peticion ya
    fallo y lo que esta en juego es la integridad del conjunto descargado.

    Existe para separar dos situaciones que un bucle de paginacion confunde con
    facilidad: la API responde "no hay mas paginas" (fin legitimo, la cosecha
    esta completa) frente a la API no responde (la cosecha esta TRUNCADA). La
    segunda debe propagarse: si se trata como fin de datos, una temporada a
    medias se persiste como si fuera la temporada entera y el error solo aparece
    aguas abajo, como partidos que faltan sin motivo aparente.
    """


# --------------------------------------------------------------------------- #
# HTTP (curl_cffi impersonando Chrome si esta instalado; urllib si no)
# --------------------------------------------------------------------------- #
def http_get_bytes(url: str, *, headers: dict, timeout: int = 30,
                   retries: int = 3, wait: float = 2.0) -> tuple[int, bytes]:
    """GET -> (status, cuerpo en BYTES).

    NO lanza por codigos HTTP: el ultimo status se DEVUELVE tal cual y el
    llamante decide (escalar a una via alternativa, degradar, o abortar).

    Reintenta, con espera creciente, en los tres fallos que la experiencia dice
    que son transitorios: errores de RED (timeout, DNS, conexion), 429
    (rate-limit) y 5xx (la fuente esta caida o de mantenimiento). Los 4xx que no
    sean 429 vuelven en el primer intento, sin espera: un 404 es "no existe" y
    reintentarlo solo retrasa el recorrido del historico, y un 403 es la senal
    con la que `understat.py` y `sofascore.py` escalan a su via alternativa, asi
    que tiene que llegar pronto.

    Solo relanza (RuntimeError) si NINGUN intento obtuvo respuesta HTTP.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            status, body = _raw_get_bytes(url, headers=headers, timeout=timeout)
        except Exception as e:  # noqa: BLE001  error de red -> reintento
            last_exc = e
            if attempt < retries:
                time.sleep(wait * attempt)
            continue
        if status == 429 and attempt < retries:
            pause = 15 + 5 * attempt
            print(f"[HTTP] 429 rate-limit en {url}; espero {pause}s y reintento...")
            time.sleep(pause)
            continue
        if 500 <= status < 600 and attempt < retries:
            pause = wait * 2 ** attempt
            print(f"[HTTP] {status} en {url} (intento {attempt}/{retries}); "
                  f"espero {pause:.0f}s y reintento...")
            time.sleep(pause)
            continue
        return status, body
    raise RuntimeError(f"fallo de red en {url}: {last_exc}") from last_exc


def _raw_get_bytes(url: str, *, headers: dict, timeout: int) -> tuple[int, bytes]:
    try:  # ruta preferida: curl_cffi (mas robusta ante bloqueos/Cloudflare)
        from curl_cffi import requests as cf_requests
    except ImportError:
        cf_requests = None

    if cf_requests is not None:
        r = cf_requests.get(url, headers=headers, timeout=timeout,
                            impersonate="chrome")
        return int(r.status_code), r.content

    import urllib.error
    import urllib.request

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read()
    except urllib.error.HTTPError as e:  # 4xx/5xx: NO es error de red
        return int(e.code), e.read()


def http_get(url: str, *, headers: dict, timeout: int = 30,
             retries: int = 3, wait: float = 2.0) -> tuple[int, str]:
    """GET -> (status, cuerpo como TEXTO utf-8 con reemplazo)."""
    status, body = http_get_bytes(url, headers=headers, timeout=timeout,
                                  retries=retries, wait=wait)
    return status, body.decode("utf-8", errors="replace")


# --------------------------------------------------------------------------- #
# Temporada vigente
# --------------------------------------------------------------------------- #
def current_season_year(now: datetime | None = None) -> int:
    """Ano de inicio de la temporada vigente (LaLiga arranca en agosto)."""
    now = now or datetime.now()
    return now.year if now.month >= 7 else now.year - 1


# --------------------------------------------------------------------------- #
# Guardado a prueba de fallos (disenado para "ejecutar una vez y olvidarse")
# --------------------------------------------------------------------------- #
def clear_readonly(path: Path) -> None:
    """Quita el atributo 'solo lectura' de un fichero (Windows: OneDrive suele
    marcarlo asi, lo que hace fallar `os.replace`/`to_csv` con PermissionError
    aunque el fichero NO este abierto). Best-effort."""
    try:
        p = Path(path)
        if p.exists():
            os.chmod(p, stat.S_IWRITE | stat.S_IREAD)
    except Exception:  # noqa: BLE001
        pass


def write_df_safe(df: pd.DataFrame, dest: Path, *, fmt: str = "csv",
                  backup: bool = True, retries: int = 6,
                  wait: float = 2.0) -> bool:
    """Guarda un DataFrame de forma robusta y sin intervencion manual.

    Escribe primero a un temporal y lo publica con `os.replace` (atomico), tras
    retirar el atributo "solo lectura" del destino. Si el destino esta ocupado
    un instante (sincronizacion/antivirus), reintenta; si el reemplazo sigue sin
    poder aplicarse, escribe directamente in situ (NO atomico: ultimo recurso).
    Deja una copia `.bak` cuando `backup=True`. `fmt`: "csv" o "parquet".
    Devuelve True si se guardo.
    """
    dest = Path(dest)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    def _emit(target: Path) -> None:
        if fmt == "parquet":
            df.to_parquet(target, index=False)
        else:
            df.to_csv(target, index=False)

    try:
        _emit(tmp)
    except Exception as e:  # noqa: BLE001
        print(f"[ERROR] no pude escribir el temporal de '{dest.name}' ({e}).")
        return False
    if backup and dest.exists():
        try:
            shutil.copy2(dest, dest.with_suffix(dest.suffix + ".bak"))
        except Exception:  # noqa: BLE001  backup best-effort
            pass

    # 1) publicacion atomica, reintentando por si el destino esta ocupado
    for attempt in range(1, retries + 1):
        try:
            clear_readonly(dest)
            os.replace(tmp, dest)
            return True
        except PermissionError:
            if attempt < retries:
                print(f"[INFO] '{dest.name}' ocupado (intento {attempt}/{retries}); "
                      f"reintento en {wait:.0f}s...")
                time.sleep(wait)
            else:
                break

    # 2) alternativa: escritura directa in situ (no atomica), con reintentos
    for attempt in range(1, retries + 1):
        try:
            clear_readonly(dest)
            _emit(dest)
            try:
                tmp.unlink()
            except Exception:  # noqa: BLE001
                pass
            return True
        except PermissionError:
            if attempt < retries:
                time.sleep(wait)
            else:
                break

    # 3) no fue posible: el origen sigue intacto; se informa como reanudar
    print(
        f"\n[ERROR] no pude guardar '{dest.name}' tras varios reintentos porque "
        "sigue protegido contra escritura (atributo 'solo lectura' o candado de "
        "sincronizacion/antivirus). No se ha perdido nada. Cierra cualquier "
        "programa que use el fichero (o pausa la sincronizacion) y vuelve a "
        "ejecutar el mismo comando; el proceso reanuda sin re-descargar."
    )
    try:
        tmp.unlink()
    except Exception:  # noqa: BLE001
        pass
    return False


# --------------------------------------------------------------------------- #
# Publicacion NO REGRESIVA de artefactos procesados
# --------------------------------------------------------------------------- #
def _rows_on_disk(dest: Path) -> int | None:
    """Filas del artefacto vigente, o None si no existe / no se puede leer."""
    dest = Path(dest)
    if not dest.exists():
        return None
    try:
        if dest.suffix == ".parquet":
            import pyarrow.parquet as pq  # dep. transitiva de pandas.to_parquet
            return int(pq.ParquetFile(dest).metadata.num_rows)
        return int(len(pd.read_csv(dest)))
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] no pude leer el artefacto vigente '{dest.name}' ({e}); "
              "se publica sin comparar tamano.")
        return None


def assert_no_regression(df: pd.DataFrame, dest: Path, *, source: str,
                         min_ratio: float = 0.90,
                         allow_empty: bool = False) -> None:
    """Falla si publicar `df` en `dest` supondria PERDER datos.

    Motivo: las aserciones de esquema son vacuamente ciertas sobre un frame sin
    filas, de modo que un maestro crudo ausente, renombrado o truncado a mitad de
    una descarga interrumpida generaba un parquet de cero filas que sobrescribia
    en silencio uno bueno. El sintoma solo aparecia mucho mas abajo, como
    cobertura NaN en el master consolidado.

    Se rechaza en dos casos:
      1. `df` vacio (salvo `allow_empty=True`, para tablas legitimamente vacias).
      2. `df` con menos del `min_ratio` de las filas del artefacto vigente. El
         margen del 10 % absorbe depuraciones y recortes de ventana normales; una
         caida mayor indica un problema de descarga, no una mejora de calidad.

    El crudo no se toca nunca: basta corregir la causa y re-ejecutar `process`.
    """
    dest = Path(dest)
    n_new = int(len(df))
    if n_new == 0 and not allow_empty:
        raise RegressionError(
            f"[{source}] el resultado tiene CERO filas: no se publica "
            f"'{dest.name}' (el artefacto vigente se conserva intacto). "
            "Revisa que el CSV maestro exista y no este truncado, y re-ejecuta "
            "'process'; si el vacio es legitimo, llama con allow_empty=True."
        )
    n_old = _rows_on_disk(dest)
    if n_old and n_new < min_ratio * n_old:
        raise RegressionError(
            f"[{source}] el resultado tiene {n_new} filas frente a las {n_old} "
            f"del '{dest.name}' vigente (< {min_ratio:.0%}): parece una descarga "
            "incompleta, no una depuracion. No se publica nada. Recupera el "
            "maestro (queda un .bak junto al CSV) o completa la descarga y "
            "re-ejecuta 'process'."
        )


def write_df_required(df: pd.DataFrame, dest: Path, *, fmt: str = "parquet",
                      backup: bool = False, etiqueta: str = "") -> None:
    """`write_df_safe` para artefactos OBLIGATORIOS: si no se guarda, se levanta.

    `write_df_safe` devuelve un booleano en vez de propagar el fallo porque en un
    extractor la escritura fallida es recuperable: el CSV maestro sigue en disco y
    la siguiente pasada reanuda sin re-descargar nada. En un constructor la
    situacion es la contraria. `master_matches`, `features_matches` o
    `features_prep` son el unico resultado de una reconstruccion completa que solo
    vive en memoria, de modo que ignorar el booleano deja el peor de los
    desenlaces: el proceso imprime su `[OK]`, termina con codigo 0 y en disco
    permanece el artefacto de la ejecucion ANTERIOR. Todo lo que viene detras se
    calcula entonces sobre un dato viejo, sin ningun sintoma visible hasta que los
    numeros finales no cuadran con lo que dice el codigo.

    Por eso aqui el fallo es terminal: mejor cortar la cadena en el punto exacto
    en que ha ocurrido -con el nombre del fichero y la causa ya impresos por
    `write_df_safe`- que seguir adelante con una premisa falsa.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not write_df_safe(df, dest, fmt=fmt, backup=backup):
        raise WriteError(
            f"[{etiqueta or dest.stem}] no se pudo escribir '{dest}'. En disco "
            "sigue el artefacto de la ejecucion anterior, asi que la cadena se "
            "detiene aqui: continuar habria calculado el resto del pipeline sobre "
            "un dato desactualizado sin avisar. Resuelve la causa indicada arriba "
            "y repite este mismo comando."
        )


def publish_df(df: pd.DataFrame, dest: Path, *, source: str, fmt: str = "parquet",
               min_ratio: float = 0.90, allow_empty: bool = False,
               backup: bool = True, verbose: bool = True) -> bool:
    """`assert_no_regression` + `write_df_safe`: unica via para publicar procesados.

    `backup=True` por defecto tambien en los parquet: aunque son regenerables
    desde el CSV maestro, el .bak es la red que permite volver atras cuando la
    causa de la regresion es justamente que el maestro se ha perdido.
    """
    dest = Path(dest)
    assert_no_regression(df, dest, source=source, min_ratio=min_ratio,
                         allow_empty=allow_empty)
    dest.parent.mkdir(parents=True, exist_ok=True)
    ok = write_df_safe(df, dest, fmt=fmt, backup=backup)
    if ok and verbose:
        print(f"[OK] {fmt} -> {dest} ({len(df)} filas)")
    return ok


# --------------------------------------------------------------------------- #
# Checkpoint de descargas (reanudacion automatica)
# --------------------------------------------------------------------------- #
def ckpt_path(master_csv: Path) -> Path:
    master_csv = Path(master_csv)
    return master_csv.parent / f"_ckpt_{master_csv.stem}.csv"


_CKPT_MAX_AGE_DAYS = 7.0


def load_checkpoint(master_csv: Path, *, key_col: str | None = None,
                    max_age_days: float = _CKPT_MAX_AGE_DAYS
                    ) -> tuple[pd.DataFrame, set]:
    """Carga el checkpoint pendiente (filas descargadas sin volcar al master).

    Devuelve (dataframe, conjunto de claves ya presentes segun `key_col`).
    Un checkpoint ilegible se descarta CON AVISO (sus filas se re-descargan).

    Un checkpoint VIEJO tambien se descarta. El checkpoint existe para reanudar
    una descarga interrumpida hace minutos u horas; pasados `max_age_days` deja de
    ser una reanudacion y pasa a ser una mezcla de dos cosechas separadas en el
    tiempo. Sus claves suprimen la re-descarga de partidos que entretanto han
    cambiado de estado -- una alineacion provisional que ya es definitiva, un
    partido aplazado que ya se ha jugado -- y esas filas viejas se publican como
    si fueran de la pasada actual. Descartarlo solo cuesta volver a bajar lo
    pendiente, que es exactamente lo que hace el extractor cuando no hay
    checkpoint; conservarlo cuesta datos desactualizados sin ninguna senal.

    La antiguedad se mide por la fecha de modificacion del fichero, que aqui SI es
    fiable: el checkpoint lo escribe el propio extractor y es un artefacto
    temporal, no un fichero versionado que pueda venir de un clon o una copia.
    """
    ckpt = ckpt_path(master_csv)
    if not ckpt.exists():
        return pd.DataFrame(), set()
    try:
        edad = (time.time() - ckpt.stat().st_mtime) / 86400.0
    except OSError:
        edad = 0.0
    if max_age_days is not None and edad > max_age_days:
        print(f"[WARN] checkpoint '{ckpt.name}' con {edad:.1f} dias de antiguedad "
              f"(> {max_age_days:g}); se descarta y se re-descarga lo pendiente. "
              "Un checkpoint de una cosecha anterior impediria refrescar filas "
              "que ya han cambiado.")
        try:
            ckpt.unlink()
        except OSError as e:
            print(f"[WARN] no pude borrar el checkpoint caducado ({e}); "
                  "se ignora su contenido de todos modos.")
        return pd.DataFrame(), set()
    try:
        df = pd.read_csv(ckpt)
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] checkpoint '{ckpt.name}' ilegible ({e}); se descarta y "
              "se re-descarga lo pendiente.")
        return pd.DataFrame(), set()
    keys: set = set()
    if key_col and key_col in df.columns:
        keys = set(df[key_col].dropna().unique())
    if len(df):
        print(f"[INFO] checkpoint encontrado: {len(df)} filas "
              "(se reanuda sin volver a bajarlas).")
    return df, keys


def save_checkpoint(rows, master_csv: Path) -> None:
    """Reescribe el checkpoint con todo lo descargado hasta ahora.

    `rows`: lista de dicts (filas) O lista de DataFrames (tramos/temporadas).
    """
    if not rows:
        return
    if isinstance(rows[0], pd.DataFrame):
        frames = [d for d in rows if isinstance(d, pd.DataFrame) and not d.empty]
        if not frames:
            return
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.DataFrame(rows)
    try:
        df.to_csv(ckpt_path(master_csv), index=False)
    except Exception as e:  # noqa: BLE001  no romper la descarga por el checkpoint
        print(f"[WARN] no pude escribir el checkpoint ({e}); sigo en memoria.")


# --------------------------------------------------------------------------- #
# Fusion de maestro y descarga nueva
# --------------------------------------------------------------------------- #
def dedupe_preferring_complete(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Elimina duplicados por `key` conservando la fila con MAS informacion.

    Las fuentes de estadistica de partido (Sofascore, Understat) tratan el
    maestro como verdad: las metricas de un encuentro finalizado no cambian, asi
    que una segunda descarga del mismo `match_id` es redundante y se descarta.
    Ese razonamiento, sin embargo, presupone que la fila almacenada se capturo
    con el partido ya terminado. Si se guardo antes —el encuentro estaba en
    juego, la fuente aun no habia publicado el bloque de estadisticas, o el
    scraping se interrumpio a mitad— quedan celdas vacias que un criterio de
    "gana el primero" congelaria de forma permanente: ninguna descarga posterior
    podria completarlas y el hueco viajaria hasta el modelo.

    El desempate se hace por completitud, contando celdas no nulas. Cuando ambas
    filas estan igual de completas —el caso normal, dos capturas del mismo
    partido terminado— gana la del maestro, de modo que el comportamiento
    habitual coincide con el anterior y la fusion sigue siendo estable: solo
    cambia el resultado cuando la copia nueva aporta datos que faltaban.

    `df` debe llegar con el maestro PRIMERO en el `concat`.
    """
    if key not in df.columns or df.empty:
        return df
    orden = df.notna().sum(axis=1).to_numpy()
    aux = df.assign(_completitud=orden, _posicion=range(len(df)))
    aux = aux.sort_values(["_completitud", "_posicion"], ascending=[False, True],
                          kind="mergesort")
    aux = aux.drop_duplicates(key, keep="first").sort_values("_posicion",
                                                             kind="mergesort")
    return aux.drop(columns=["_completitud", "_posicion"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Validaciones RUIDOSAS de esquema (para las capas de normalizacion)
# --------------------------------------------------------------------------- #
def _sample_rows(df: pd.DataFrame, mask, n: int = 5) -> list[dict]:
    """Primeras n filas ofensoras, solo columnas identificativas, para el error."""
    id_cols = [c for c in ("match_id", "row_id", "match_key", "event_id", "date",
                           "season", "team", "home", "away", "home_team",
                           "away_team", "HomeTeam", "AwayTeam")
               if c in df.columns]
    sub = df.loc[mask, id_cols] if id_cols else df.loc[mask]
    return sub.head(n).to_dict("records")


def assert_columns(df: pd.DataFrame, required, source: str) -> None:
    """Falla si faltan columnas del contrato minimo de la fuente."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SchemaError(
            f"[{source}] faltan columnas requeridas: {missing}. "
            f"Presentes (primeras 30): {list(df.columns)[:30]}. "
            "La fuente ha cambiado de esquema o el master esta corrupto; "
            "el parquet NO se ha regenerado."
        )


def assert_non_null(df: pd.DataFrame, cols, source: str) -> None:
    """Falla si alguna columna clave contiene nulos (con filas de ejemplo)."""
    for c in cols:
        if c not in df.columns:
            continue
        m = df[c].isna()
        if m.any():
            raise SchemaError(
                f"[{source}] {int(m.sum())} filas con '{c}' NULO. Un nulo en una "
                f"clave de cruce descuelga esas filas del merge en silencio. "
                f"Ejemplos: {_sample_rows(df, m)}"
            )


def assert_unique(df: pd.DataFrame, cols, source: str) -> None:
    """Falla si hay duplicados en la(s) columna(s) clave (con ejemplos)."""
    cols = [cols] if isinstance(cols, str) else list(cols)
    if any(c not in df.columns for c in cols):
        return
    m = df.duplicated(subset=cols, keep=False)
    if m.any():
        raise SchemaError(
            f"[{source}] {int(m.sum())} filas con {cols} DUPLICADO (la "
            "idempotencia aguas arriba ha fallado). "
            f"Ejemplos: {_sample_rows(df, m)}"
        )
