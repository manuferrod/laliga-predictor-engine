"""Sincroniza el cuaderno 03 con el circuito de cuotas manuales (regla 9).

Cuatro celdas de la Parte B quedaron desincronizadas al sembrar la ventana desde
el calendario de Sofascore y al hacer prevalecer en el histórico lo tecleado:

- **B.1** declaraba football-data como fuente ÚNICA de «qué partidos vienen», que
  es precisamente la afirmación que la siembra deroga. Se reescribe a «principal»
  y se explica el recorte a una sola jornada (`_GAP_JORNADA`), porque sin él el
  horizonte de 10 días siembra diecinueve partidos y nueve no tienen mercado
  abierto en ninguna casa.
- **B.2** remitía el hueco a «re-ejecuta el extractor o mete cuota manual», cuando
  lo que hace el diagnóstico es informar del `fixture_src` de una fila que la
  siembra ya metió en la ventana.
- **B.4** describía la plantilla pero no la mitad que faltaba: que lo tecleado se
  guarda con su sello y vuelve a aplicarse en los reprocesos, con su alcance
  —solo `odds_avg_*`, nunca Bet365 ni el cierre— y su límite temporal.
- El **cierre** recomendaba lanzar `upcoming` varios días seguidos para acumular
  la jornada, que era el remedio manual del problema que la siembra resuelve.

Se escribió aparte y no se editó el `.ipynb` a mano por el mismo motivo que
`_nbins4.py`: el cuaderno es entregable y un parche a mano sobre el JSON deja
comillas y saltos de línea mal formados sin que nada falle hasta que alguien lo
abre. Es **idempotente**: cada parche busca su texto viejo, y si encuentra el
nuevo informa y no toca nada, de modo que ejecutarlo dos veces no duplica
párrafos. Respeta el ancho de línea del cuaderno (~104) y su `indent=1`.

`E501` se silencia para todo el fichero y no línea a línea: las cadenas `*_VIEJO`
tienen que coincidir **byte a byte** con lo que hay en el `.ipynb` o el parche no
encuentra nada, de modo que su ancho lo fija el cuaderno y no este módulo; y las
`*_NUEVO` se wrapean al ancho de los párrafos vecinos, porque un párrafo más
estrecho que los de al lado se ve en el cuaderno, que es el entregable.
"""
# flake8: noqa: E501
from __future__ import annotations

import json
from pathlib import Path

NB = Path(__file__).resolve().parent / "notebooks" / \
    "03_LIMPIEZA_VARIABLES_E_INFERENCIA.ipynb"

# --- B.1 ------------------------------------------------------------------- #
B1_VIEJO = """La fuente única de "qué partidos vienen" es el extractor `footballdata.py upcoming()` (se corre en
local, necesita red): descarga el `fixtures.csv` de football-data, filtra LaLiga (SP1) y lo normaliza a
**`footballdata_upcoming.parquet`** — claves (`match_key`, `home_key`, `away_key`, fecha), **cuotas de
apertura** y `mkt_p_*` (probabilidad implícita del mercado). No trae resultado ni cuotas de cierre: aún
no existen."""

B1_NUEVO = """La fuente **principal** de "qué partidos vienen" es el extractor `footballdata.py upcoming()` (se corre
en local, necesita red): descarga el `fixtures.csv` de football-data, filtra LaLiga (SP1) y lo normaliza
a **`footballdata_upcoming.parquet`** — claves (`match_key`, `home_key`, `away_key`, fecha), **cuotas de
apertura** y `mkt_p_*` (probabilidad implícita del mercado). No trae resultado ni cuotas de cierre: aún
no existen.

**Principal y no única, y la diferencia importa.** football-data sirve el fixture y el precio *juntos o
ninguno de los dos*, de modo que mientras no publica la jornada no hay ni fila que puntuar ni cuota con
la que hacerlo. Por eso la ventana se **siembra** además desde el calendario de SofaScore
(`build_scoring._seed_from_sofascore`), que conoce la jornada con semanas de antelación: aporta el
fixture **sin cuota** y marca su procedencia en `fixture_src`. football-data **manda cuando tiene el
fixture**, porque entonces trae el precio real de las casas; la siembra solo cubre lo que a él le falta.

La siembra se recorta a la **primera** jornada que viene (`_solo_proxima_jornada`, hueco de calendario
de 3 días, el mismo criterio que `protocol._GAP_DIAS`). El recorte no es cosmético: `fixtures.csv` se
autolimita a ~7 días, así que el horizonte de 10 días nunca llegaba a abarcar dos jornadas, pero
SofaScore conoce la temporada entera y sin recortar sembraría la jornada siguiente **y la de después**
—diecinueve partidos en lugar de diez—. Los nueve de más no tienen mercado abierto en ninguna casa:
teclear su cuota sería inventarla."""

# --- B.2 ------------------------------------------------------------------- #
B2_VIEJO = """- los que SofaScore **programa pero football-data aún no trae** (hueco probable: la ventana de 7 días
  todavía no los alcanza → re-ejecuta el extractor o mete cuota manual),"""

B2_NUEVO = """- los que SofaScore **programa pero football-data aún no trae**: no quedan fuera. La siembra los
  mete en la ventana sin cuota y el *gate* los pone en la plantilla, de modo que lo que el diagnóstico
  informa de ellos es su `fixture_src`, y no un hueco pendiente de que alguien reaccione,"""

# --- B.4 ------------------------------------------------------------------- #
B4_VIEJO = """- **Cuotas manuales + gate estricto.** Si football-data aún no publicó la cuota de algún partido,
  `build_scoring` escribe una **plantilla CSV** (`footballdata_upcoming_manual_odds.csv`) con los
  partidos que faltan; rellenas las cuotas 1X2 que veas en tu casa y re-ejecutas. Las manuales pasan por
  la **misma** normalización de-margen que las automáticas. Si tras auto + manual **algún** partido
  sigue sin cuota, se **para** y no se escribe `features_upcoming` (la cuota es una señal demasiado
  fuerte como para predecir sin ella). Fatiga/bajas sí pueden quedar `NaN`."""

B4_NUEVO = """- **Cuotas manuales + gate estricto.** Si football-data aún no publicó la cuota de algún partido,
  `build_scoring` escribe una **plantilla CSV** (`footballdata_upcoming_manual_odds.csv`) con los
  partidos que faltan; rellenas las cuotas 1X2 que veas en tu casa y re-ejecutas. Las manuales pasan por
  la **misma** normalización de-margen que las automáticas. Si tras auto + manual **algún** partido
  sigue sin cuota, se **para** y no se escribe `features_upcoming` (la cuota es una señal demasiado
  fuerte como para predecir sin ella). Fatiga/bajas sí pueden quedar `NaN`.
- **Y lo tecleado prevalece después en el histórico**, que es la mitad sin la cual el mecanismo sería de
  un solo uso: una semana más tarde el partido se ha jugado, football-data lo publica en el CSV
  histórico con *su* precio, el máster se reconstruye y toma ese — la predicción se emitió a un precio y
  el histórico la reentrena y la liquida a otro, sin que nada avise, porque las dos cifras son
  legítimas. `promote_manual_odds` guarda lo tecleado con su sello `entered_ts` en
  `footballdata_odds_manual.csv` y `footballdata.apply_manual_odds` lo vuelve a aplicar en cada
  reproceso. Teclear una vez basta.

  Su **alcance** es una decisión con criterio y no una omisión: toca exclusivamente `odds_avg_h/d/a`, el
  canal de entrada del modelo, de donde salen `mkt_p_home/draw/away`. **No toca `odds_b365_*`,
  `odds_ps_*`, `odds_max_*` ni ninguna columna de cierre**, porque las de Bet365 son el listón contra el
  que se publica toda cifra económica (`backtest.REFERENCIA`) y una cuota escrita a mano ahí haría que
  la pregunta del trabajo se contestara contra un precio que no publicó ninguna casa; y porque cuando
  football-data publica el partido ya jugado trae la cuota de **cierre**, mejor dato que la apertura que
  se vio al teclear, de modo que pisarla degradaría el histórico en lugar de corregirlo. Lo que
  prevalece es **con qué precio predijo** el modelo; lo que se conserva es **a qué precio se liquida**.

  El límite que lo hace defendible es la regla antifuga escrita en código: la cuota se aplica solo si su
  sello es **anterior al día del partido**, de modo que el override queda confinado por construcción a
  partidos que estaban por jugarse cuando se tecleó y los bloques de validación son intocables. Como las
  dos vías son indistinguibles aguas abajo —misma normalización, mismo rechazo de márgenes imposibles—,
  la procedencia se declara en `odds_src`."""

# --- Cierre ---------------------------------------------------------------- #
CIERRE_VIEJO = """**Cómo ejecutarlo bien el día de la jornada:**
- **No lances una sola vez.** La ventana de football-data son ~7 días; ejecuta `upcoming` varios días
  seguidos antes de la jornada para acumular *todos* los partidos.
- **Refresca también el calendario de SofaScore** antes de puntuar, para que el chequeo de completitud
  (B.2) compare contra una foto actual.
- Si el diagnóstico marca **huecos** (SofaScore los programa y football-data no), re-ejecuta más cerca
  de la fecha o rellena la **plantilla de cuotas manuales** y vuelve a lanzar.
- Si el **gate de cuotas** para el proceso, es a propósito: rellena
  `footballdata_upcoming_manual_odds.csv` y re-ejecuta."""

CIERRE_NUEVO = """**Cómo ejecutarlo bien el día de la jornada:**
- **Refresca el calendario de SofaScore** antes de puntuar. No es solo para que el chequeo de
  completitud (B.2) compare contra una foto actual: es también la fuente de la que se siembra la ventana
  cuando football-data va con retraso, así que una foto vieja del calendario deja la jornada fuera.
- **Una sola pasada de `upcoming` basta para que la jornada entre.** Acumular por upsert es lo que hace
  el extractor y conviene igual, porque cada pasada trae precios reales de las casas en lugar de huecos
  que teclear; pero el partido que su ventana de 7 días no alcanza entra por la siembra, con la cuota
  pendiente de teclado en vez de no entrar.
- Si el **gate de cuotas** para el proceso, es a propósito: rellena
  `footballdata_upcoming_manual_odds.csv` y re-ejecuta. Desde la rutina semanal se reanuda sin repetir
  la cadena, con `python scripts/rutina_semanal.py --desde predict`.
- **Teclea solo lo que hayas visto en una casa.** El gate acepta cualquier terna 1X2 plausible y no
  puede distinguir un precio real de uno inventado; lo que sí distingue es la procedencia, y por eso la
  guarda en `odds_src` y en el registro con su sello."""

PARCHES = (("B.1", B1_VIEJO, B1_NUEVO), ("B.2", B2_VIEJO, B2_NUEVO),
           ("B.4", B4_VIEJO, B4_NUEVO), ("cierre", CIERRE_VIEJO, CIERRE_NUEVO))


def parchea(ruta: Path = NB) -> int:
    nb = json.loads(ruta.read_text(encoding="utf-8"))
    hechos = 0
    for etiqueta, viejo, nuevo in PARCHES:
        destino = None
        for i, c in enumerate(nb["cells"]):
            if c["cell_type"] != "markdown":
                continue
            s = "".join(c["source"])
            if nuevo in s:
                print(f"[SKIP] {etiqueta}: ya estaba sincronizada")
                destino = -1
                break
            if viejo in s:
                destino = i
                break
        if destino is None:
            print(f"[WARN] {etiqueta}: no encuentro el texto viejo; reviso a mano")
            continue
        if destino < 0:
            continue
        c = nb["cells"][destino]
        s = "".join(c["source"]).replace(viejo, nuevo)
        c["source"] = [ln + "\n" for ln in s.split("\n")]
        c["source"][-1] = c["source"][-1].rstrip("\n")
        print(f"[OK]   {etiqueta}: celda {destino} reescrita")
        hechos += 1
    if hechos:
        ruta.write_text(json.dumps(nb, ensure_ascii=False, indent=1) + "\n",
                        encoding="utf-8")
    return hechos


if __name__ == "__main__":
    print(f"parcheadas {parchea()} celda(s)")
