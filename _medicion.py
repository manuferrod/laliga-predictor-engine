"""La medición trae un RPS sin listón, y así no se puede leer.

`informe` empareja lo económico con Bet365 (regla 3) pero el RPS sale solo, y el RPS es
la métrica que manda (regla 4). El listón natural no necesita ajustar nada: son las
probabilidades del propio mercado, que viajan en `pred` como `mkt_p_*_raw`. Se puntúan
sobre EXACTAMENTE los mismos partidos.

Se añade el modelo de la semilla desnuda como segunda lectura: contesta a si las dos
variables extra pagan su sitio fuera del bloque que las eligió. Es LECTURA, no decisión
—el conjunto ya está cerrado y la regla 6 prohíbe reelegir aquí—; si sale mal, se dice.
"""
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, "scripts/models")
import protocol as pt            # noqa: E402
import laboratorio as lb         # noqa: E402
import evaluate as ev            # noqa: E402
import seleccion as sl           # noqa: E402

CONJ = list(sl.carga()["conjunto"])
SEM = list(sl.SEMILLA_MERCADO)
RAW = ["mkt_p_home_raw", "mkt_p_draw_raw", "mkt_p_away_raw"]

cfg = pt.ProtocolConfig(regime="semanal")
feat = lb.datos()
print("conjunto cerrado:", CONJ, flush=True)


def puntua(cols, nombre):
    e = lb.evalua(cols, "logistica", cfg=cfg, feat=feat,
                  evaluar=sl.BLOQUE_MEDICION, n_boot=0,
                  registrar=False, verbose=False)
    p = e.pred.loc[ev.evaluable(e.pred)].copy()
    p["rps"] = ev.rps_vector(ev._objetivo(p), ev._terna(p, ev.PROB_COLS))
    p["acierto"] = (np.asarray(ev._terna(p, ev.PROB_COLS)).argmax(1)
                    == np.asarray(ev._objetivo(p))).astype(float)
    return nombre, p


filas = []
for cols, nombre in ((CONJ, "modelo k5"), (SEM, "semilla k3")):
    nombre, p = puntua(cols, nombre)
    filas.append((nombre, p))
    print(f"{nombre}: n={len(p)}  RPS {p['rps'].mean():.5f}  "
          f"acierto {p['acierto'].mean():.4%}", flush=True)

# El listón del precio, sin ajustar nada: las columnas crudas del mercado.
p = filas[0][1]
falta = [c for c in RAW if c not in p.columns]
if falta:
    raise SystemExit(f"no hay columnas crudas de mercado: {falta}")
mkt = p[RAW].to_numpy(float)
mkt = mkt / mkt.sum(1, keepdims=True)
p["rps_mkt"] = ev.rps_vector(ev._objetivo(p), mkt)
p["acierto_mkt"] = (mkt.argmax(1)
                    == np.asarray(ev._objetivo(p))).astype(float)
print(f"precio      : n={len(p)}  RPS {p['rps_mkt'].mean():.5f}  "
      f"acierto {p['acierto_mkt'].mean():.4%}", flush=True)

print("\n--- RPS temporada a temporada: modelo frente al precio ---")
t = p.groupby("season").apply(lambda g: pd.Series({
    "n": len(g),
    "rps_modelo": g["rps"].mean(),
    "rps_precio": g["rps_mkt"].mean(),
    "gana_rps": g["rps_mkt"].mean() - g["rps"].mean(),
    "acierto_modelo": g["acierto"].mean(),
    "acierto_precio": g["acierto_mkt"].mean()}), include_groups=False)
print(t.to_string())

dif = pd.DataFrame({"dif": p["rps_mkt"] - p["rps"],
                    "season": p["season"], "jornada": p["jornada"]})
iv = ev.block_bootstrap(dif, lambda q: float(q["dif"].mean()),
                        n_boot=2000, seed=7)
print(f"\nganancia de RPS del modelo sobre el precio: {dif['dif'].mean():+.6f}")
print(f"intervalo por bloques de jornada: {iv}")

d2 = pd.DataFrame({"dif": filas[1][1]["rps"].to_numpy() - p["rps"].to_numpy(),
                   "season": p["season"].to_numpy(),
                   "jornada": p["jornada"].to_numpy()})
iv2 = ev.block_bootstrap(d2, lambda q: float(q["dif"].mean()),
                         n_boot=2000, seed=7)
print(f"\nganancia del k5 sobre la semilla k3: {d2['dif'].mean():+.6f}")
print(f"intervalo por bloques de jornada: {iv2}")
print(f"(positivo = el conjunto de cinco mejora a la semilla)")
