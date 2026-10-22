"""El ROI con la definición de Manuel: una unidad plana a todos los partidos.

Hay dos ROI distintos en este proyecto y conviene no confundirlos, porque difieren en
un factor de tres y responden a preguntas diferentes.

El que **publica** `backtest` es apuesta de valor: solo entra donde la probabilidad del
modelo supera a la implícita en la cuota, y en el bloque de validación eso son 191 de
380 partidos por temporada. Es la cifra correcta para «¿hay valor explotable?».

El que Manuel pide es otro: **una unidad al signo que el modelo señala, en todos los
partidos**, sin umbral. Es la cifra correcta para «¿cuánto deja el modelo si lo sigues
sin pensar?», y es además la única que compara manzanas con manzanas contra el listón
del proyecto, que ya era de unidad plana (`backtest.REFERENCIA = "b365"`: su cuota más
baja, todos los partidos con precio, sin umbral). En la tabla publicada el lado del
modelo tiene `n_modelo = 191` y el lado del listón `n_ref = 380`; los dos números no son
comparables entre sí, y por eso esta medida existe.

Se liquida con cuota de Bet365 en los dos lados —el modelo cobra al precio de la misma
casa que fija el listón—, porque pagarle al modelo la cuota media del mercado y al
listón la de Bet365 mezclaría la calidad de la predicción con la de la casa elegida.

**La cuota sale del maestro, no de invertir las implícitas de las predicciones.** Esto
hay que decirlo porque el nombre invita al error y yo caí en él: las columnas
`mkt_p_b365_*_raw` del parquet de predicciones **suman exactamente 1.0**, o sea que el
margen de la casa ya está quitado, pese a llamarse `_raw`. Invertirlas da una cuota
ficticia sin margen y un ROI inflado en todo el margen: medí **+4.13 %** para el modelo
y **+4.55 %** para el favorito de Bet365, cuando seguir al favorito a ciegas no puede
dar positivo —si lo diera, la casa no tendría negocio—. La cuota media del favorito
salía 2.06 contra las 1.98 que publica `backtest`, y ese 4 % de diferencia es
exactamente el margen. La diferencia MODELO-LISTÓN sí sobrevivía al error, porque el
margen afecta igual a los dos lados; los niveles no.

Lee las probabilidades de los artefactos publicados (regla 8) —son las que auditó
`qa_models`— y las cuotas de `master_matches.parquet`, que es donde vive el precio.

Uso:
    python _roiplano.py
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

_RAIZ = Path(__file__).resolve().parent
PRED = _RAIZ / "outputs" / "models" / "semanal" / "predicciones_validacion.parquet"
PROD = _RAIZ / "outputs" / "models" / "semanal" / "predicciones_produccion.parquet"
MAESTRO = _RAIZ / "data" / "02_processed" / "master_matches.parquet"
SALIDA = _RAIZ / "outputs" / "models" / "semanal" / "roi_plano.csv"

SIGNOS = ("home", "draw", "away")
PROB = ["p_home", "p_draw", "p_away"]
CUOTA = ["odds_b365_h", "odds_b365_d", "odds_b365_a"]


def _liquida(df: pd.DataFrame, eleccion: np.ndarray) -> pd.DataFrame:
    """Una unidad a `eleccion` en cada partido, cobrada a cuota real de Bet365.

    El acierto devuelve `cuota - 1` de beneficio neto; el fallo pierde la unidad.
    """
    cuota = df[CUOTA].to_numpy(dtype=float)[np.arange(len(df)), eleccion]
    gana = (eleccion == df["y_num"].to_numpy()).astype(float)
    return pd.DataFrame({"season": df["season"].astype(str).to_numpy(),
                         "acierto": gana, "cuota": cuota,
                         "beneficio": np.where(gana > 0, cuota - 1.0, -1.0)})


def _agrupa(liq: pd.DataFrame, etiqueta: str) -> pd.DataFrame:
    g = liq.groupby("season", as_index=False).agg(
        n=("beneficio", "size"), acierto=("acierto", "mean"),
        beneficio=("beneficio", "sum"), cuota_media=("cuota", "mean"))
    g["roi"] = g["beneficio"] / g["n"]
    g["quien"] = etiqueta
    return g


def mide(ruta: Path, bloque: str, cuotas: pd.DataFrame) -> pd.DataFrame:
    p = pd.read_parquet(ruta).merge(cuotas, on="match_key", how="left")
    # Sin resultado no hay liquidación posible, y sin precio de Bet365 tampoco: los
    # dos lados tienen que puntuar EXACTAMENTE los mismos partidos o la diferencia
    # mezcla el mérito del modelo con el censo de partidos que cada uno cubre.
    p = p.loc[p["y_num"].notna() & p[CUOTA].notna().all(axis=1)].copy()
    p["y_num"] = p["y_num"].astype(int)
    modelo = _liquida(p, p[PROB].to_numpy().argmax(axis=1))
    # El listón: el favorito de Bet365 es su cuota MÁS BAJA.
    liston = _liquida(p, p[CUOTA].to_numpy(dtype=float).argmin(axis=1))
    tab = pd.concat([_agrupa(modelo, "modelo"), _agrupa(liston, "b365")])
    tab["bloque"] = bloque
    return tab


def main() -> int:
    cuotas = pd.read_parquet(MAESTRO, columns=["match_key"] + CUOTA)
    partes = [mide(PRED, "validacion", cuotas)]
    if PROD.exists():
        try:
            partes.append(mide(PROD, "produccion", cuotas))
        except Exception as exc:                            # noqa: BLE001
            print(f"produccion no liquidable: {type(exc).__name__}: {exc}")
    tab = pd.concat(partes, ignore_index=True)

    piv = tab.pivot_table(index=["bloque", "season"], columns="quien",
                          values=["n", "acierto", "roi", "cuota_media"])
    print("\n=== ROI de una unidad plana a todos los partidos, cuota de Bet365 ===\n")
    print(f"{'temporada':>10} {'n':>5} | {'modelo':>9} {'acierto':>8} {'cuota':>6}"
          f" | {'b365':>9} {'acierto':>8} {'cuota':>6} | {'dif':>9}")
    for (bloque, season) in piv.index:
        r = piv.loc[(bloque, season)]
        print(f"{season:>10} {int(r[('n', 'modelo')]):>5} | "
              f"{100 * r[('roi', 'modelo')]:>8.2f}% "
              f"{100 * r[('acierto', 'modelo')]:>7.2f}% "
              f"{r[('cuota_media', 'modelo')]:>6.2f} | "
              f"{100 * r[('roi', 'b365')]:>8.2f}% "
              f"{100 * r[('acierto', 'b365')]:>7.2f}% "
              f"{r[('cuota_media', 'b365')]:>6.2f} | "
              f"{100 * (r[('roi', 'modelo')] - r[('roi', 'b365')]):>+8.2f}%")

    for bloque, sub in tab.groupby("bloque"):
        tot = sub.groupby("quien").agg(n=("n", "sum"), beneficio=("beneficio", "sum"),
                                       ac=("acierto", "mean"))
        tot["roi"] = tot["beneficio"] / tot["n"]
        mo, re = tot.loc["modelo", "roi"], tot.loc["b365", "roi"]
        print(f"\nagregado {bloque}: modelo {100 * mo:+.3f}% "
              f"· b365 {100 * re:+.3f}% · diferencia {100 * (mo - re):+.3f}%"
              f"  ({int(tot.loc['modelo', 'n'])} partidos)")

    tab.to_csv(SALIDA, index=False, encoding="utf-8")
    print(f"\n{SALIDA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
