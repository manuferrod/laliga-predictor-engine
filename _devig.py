"""¿Se está quitando bien el margen de la casa? Cinco métodos sobre las mismas cuotas.

Todo lo medido hasta ahora da por bueno el ancla —`mkt_p_home/draw/away`— y pregunta qué
se le puede añadir. Esta medida va por debajo: pregunta si el ancla está bien hecha.

`footballdata._implied_probs` quita el margen por **normalización multiplicativa**:
divide cada inversa de cuota por la suma de las tres. Es el método más simple y el único
que el proyecto ha probado, y la literatura de precios de apuestas sostiene que
**sobreestima a los favoritos y subestima a los poco probables**, porque reparte el
margen en proporción a la propia probabilidad cuando la casa lo carga más sobre los
resultados improbables (el *favourite-longshot bias*). Si eso es cierto aquí, hay RPS
sobre la mesa **sin tocar una sola variable y sin ajustar un solo modelo**: es una
transformación determinista de tres columnas que ya están en el maestro.

Los cinco métodos, todos con la misma entrada (r_i = 1/cuota, s = Σr_i > 1):

1. **multiplicativo** — p_i = r_i / s. El actual.
2. **aditivo** — p_i = r_i − (s−1)/3. Reparte el margen a partes iguales, que es la
   hipótesis opuesta a la del multiplicativo.
3. **potencia** — p_i ∝ r_i^k, con k resuelto para que sumen uno. Interpola entre los
   dos anteriores sin comprometerse de antemano.
4. **razón de momios** — p_i/(1−p_i) = c · r_i/(1−r_i), con c resuelto. Mantiene la
   razón de momios, que es la escala en la que trabaja la propia regresión logística.
5. **Shin** — modelo con una fracción z de apostantes informados; la casa carga el
   margen para cubrirse de ellos, y z sale de exigir que las tres sumen uno.
   Štrumbelj (2014, *IJF* 30(4):934-943) mide que **bate a la normalización básica en
   RPS** en cinco deportes de equipo; es la cita que justifica probarlo aquí.

No se elige nada aquí. Se mide RPS y acierto de las cinco en el bloque de validación, y
si una gana se lleva a `confirma()`, que es la única puerta que no ha visto estos datos.

Uso:
    python _devig.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_RAIZ = Path(__file__).resolve().parent
for _sub in ("scripts", "scripts/models", "scripts/features"):
    sys.path.insert(0, str(_RAIZ / _sub))

import evaluate as ev           # noqa: E402

MAESTRO = _RAIZ / "data" / "02_processed" / "master_matches.parquet"
SALIDA = _RAIZ / "outputs" / "models" / "seleccion" / "devig.csv"

VALIDACION = ("2019/20", "2020/21", "2021/22", "2022/23", "2023/24", "2024/25")
SELECCION = ("2019/20", "2020/21", "2021/22")
MEDICION = ("2022/23", "2023/24", "2024/25")
CONFIRMA = ("2017/18", "2018/19")

TERNAS = {
    "avg": ("odds_avg_h", "odds_avg_d", "odds_avg_a"),
    "b365": ("odds_b365_h", "odds_b365_d", "odds_b365_a"),
    "max": ("odds_max_h", "odds_max_d", "odds_max_a"),
}


def _raw(o: pd.DataFrame) -> np.ndarray:
    return 1.0 / o.to_numpy(dtype="float64")


def _biseccion(f, lo: float, hi: float, it: int = 80) -> float:
    """Bisección escalar. Sin dependencias: f es monótona en todos los usos de aquí."""
    flo = f(lo)
    for _ in range(it):
        mid = 0.5 * (lo + hi)
        fm = f(mid)
        if (fm > 0) == (flo > 0):
            lo, flo = mid, fm
        else:
            hi = mid
    return 0.5 * (lo + hi)


def multiplicativo(r: np.ndarray) -> np.ndarray:
    return r / r.sum(axis=1, keepdims=True)


def aditivo(r: np.ndarray) -> np.ndarray:
    p = r - (r.sum(axis=1, keepdims=True) - 1.0) / r.shape[1]
    # Un aditivo puro puede dar negativo cuando el margen es grande y la cuota muy
    # alta; se recorta y se renormaliza. Es el defecto que Clarke et al. (2017) le
    # achacan al metodo y la razon por la que proponen el de potencia.
    p = np.clip(p, 1e-6, None)
    return p / p.sum(axis=1, keepdims=True)


def potencia(r: np.ndarray) -> np.ndarray:
    out = np.empty_like(r)
    for i in range(r.shape[0]):
        ri = r[i]
        if not np.isfinite(ri).all():
            out[i] = np.nan
            continue
        k = _biseccion(lambda k: (ri ** k).sum() - 1.0, 0.2, 3.0)
        p = ri ** k
        out[i] = p / p.sum()
    return out


def razon_momios(r: np.ndarray) -> np.ndarray:
    out = np.empty_like(r)
    for i in range(r.shape[0]):
        ri = np.clip(r[i], 1e-6, 1 - 1e-6)
        if not np.isfinite(r[i]).all():
            out[i] = np.nan
            continue
        om = ri / (1.0 - ri)

        def suma(c: float) -> float:
            p = c * om / (1.0 + c * om)
            return p.sum() - 1.0

        c = _biseccion(suma, 1e-4, 1.0)
        p = c * om / (1.0 + c * om)
        out[i] = p / p.sum()
    return out


def shin(r: np.ndarray) -> np.ndarray:
    out = np.empty_like(r)
    for i in range(r.shape[0]):
        ri = r[i]
        if not np.isfinite(ri).all():
            out[i] = np.nan
            continue
        s = ri.sum()

        def p_de(z: float) -> np.ndarray:
            num = np.sqrt(z * z + 4.0 * (1.0 - z) * ri * ri / s) - z
            return num / (2.0 * (1.0 - z))

        z = _biseccion(lambda z: p_de(z).sum() - 1.0, 1e-9, 0.9999)
        p = p_de(z)
        out[i] = p / p.sum()
    return out


METODOS = {
    "multiplicativo": multiplicativo,
    "aditivo": aditivo,
    "potencia": potencia,
    "razon_momios": razon_momios,
    "shin": shin,
}


def _mide(y: np.ndarray, p: np.ndarray) -> dict:
    ok = np.isfinite(p).all(axis=1) & np.isfinite(y)
    yy, pp = y[ok], p[ok]
    rps = ev.rps_vector(yy, pp)
    acc = (pp.argmax(axis=1) == yy).astype(float)
    return {"n": int(ok.sum()), "rps": float(rps.mean()),
            "accuracy": float(acc.mean()),
            "logloss": float(ev.logloss_vector(yy, pp).mean()),
            "_rps": rps, "_ok": ok}


def main(argv=None) -> int:
    m = pd.read_parquet(MAESTRO)
    # El maestro lleva el resultado como letra (`ftr`); el `y_num` que consume
    # `evaluate` lo construye `build_preprocess`. Aqui se deriva igual, con el mismo
    # orden ordinal H < D < A sobre el que esta definido el RPS.
    y = (m["ftr"].astype(str).map({"H": 0.0, "D": 1.0, "A": 2.0})
         .to_numpy(dtype="float64"))
    temp = m["season"].astype(str)

    filas: list[dict] = []
    guardado: dict[tuple[str, str], np.ndarray] = {}
    for casa, (ch, cd, ca) in TERNAS.items():
        if not {ch, cd, ca} <= set(m.columns):
            print(f"{casa}: faltan columnas, se salta", flush=True)
            continue
        r = _raw(m[[ch, cd, ca]])
        sobre = np.nansum(r, axis=1)
        print(f"\n=== {casa} · sobre-redondeo mediano "
              f"{100 * (np.nanmedian(sobre) - 1):.2f} % · "
              f"{int(np.isfinite(r).all(axis=1).sum())} partidos con precio ===",
              flush=True)
        for nombre, fn in METODOS.items():
            p = fn(r)
            guardado[(casa, nombre)] = p
            for bloque, temps in (("validacion", VALIDACION), ("seleccion", SELECCION),
                                  ("medicion", MEDICION), ("confirmacion", CONFIRMA)):
                sel = temp.isin(temps).to_numpy()
                res = _mide(y[sel], p[sel])
                filas.append({"casa": casa, "metodo": nombre, "bloque": bloque,
                              "n": res["n"], "rps": res["rps"],
                              "accuracy": res["accuracy"], "logloss": res["logloss"]})
            v = [f for f in filas if f["casa"] == casa and f["metodo"] == nombre
                 and f["bloque"] == "validacion"][0]
            print(f"  {nombre:<15} RPS {v['rps']:.6f}  "
                  f"acc {100 * v['accuracy']:5.2f} %  "
                  f"ll {v['logloss']:.4f}  n={v['n']}", flush=True)

    tab = pd.DataFrame(filas)
    val = tab[tab["bloque"] == "validacion"].sort_values("rps")
    print("\n=== validacion, las quince combinaciones ordenadas por RPS ===")
    print(val.to_string(index=False))

    # La diferencia emparejada contra el metodo actual, con intervalo por jornadas, que
    # es lo unico que distingue una mejora de un decimal de ruido.
    base = guardado.get(("avg", "multiplicativo"))
    if base is not None:
        sel = temp.isin(VALIDACION).to_numpy()
        # El maestro llama `round` a la jornada; `block_bootstrap` remuestrea por
        # jornadas enteras y necesita las dos columnas con el nombre que espera.
        clave = (m.loc[sel, ["season", "round"]]
                 .rename(columns={"round": "jornada"}).reset_index(drop=True))
        rb = _mide(y[sel], base[sel])
        print("\n=== contra el actual (avg x multiplicativo), diferencia "
              "emparejada ===")
        print(f"{'metodo':<24} {'gana RPS':>10}  intervalo")
        for (casa, nombre), p in guardado.items():
            if (casa, nombre) == ("avg", "multiplicativo"):
                continue
            ro = _mide(y[sel], p[sel])
            com = rb["_ok"] & ro["_ok"]
            a = ev.rps_vector(y[sel][com], base[sel][com])
            b = ev.rps_vector(y[sel][com], p[sel][com])
            d = clave.loc[com].copy()
            d["dif"] = a - b
            iv = ev.block_bootstrap(d, lambda x: float(x["dif"].mean()),
                                    n_boot=4000, seed=7)
            print(f"{casa + ' x ' + nombre:<24} {d['dif'].mean():+10.6f}  "
                  f"[{iv['inf']:+.6f}, {iv['sup']:+.6f}]  "
                  f"n={int(com.sum())}", flush=True)

    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    tab.to_csv(SALIDA, index=False, encoding="utf-8")
    print(f"\n{SALIDA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
