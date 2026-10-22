"""Sonda del reparto: ¿se reutilizan los procesos hijos entre pasos?

La medición anterior dejó una pregunta abierta y una afirmación heredada sospechosa.
La pregunta: un paso voraz completo cuesta 298 s repartido y 4.5 s por candidata en
el padre caliente, lo que no cuadra con doce núcleos. La afirmación: el comentario de
`seleccion.N_JOBS` dice que el reparto «es la diferencia entre una búsqueda de cinco
horas y una de media hora», y el comentario de `_lote` dice que «loky mantiene vivos
los procesos entre llamadas, así que la caché de cada hijo se calienta una vez en el
primer paso y sirve para los veinticinco».

Esto mide las dos cosas sobre el mismo lote, sin argumentar ninguna:

  1. coste de una candidata en el padre caliente, que es el patrón de la serie
  2. el mismo lote repartido DOS veces seguidas: si los hijos se reutilizan, la
     segunda pasada tiene que costar bastante menos que la primera
  3. el mismo lote dentro de un `parallel_backend` persistente, que es el candidato a
     remedio si la segunda pasada no baja
  4. cuántos procesos conviene con 5.1 GB libres, que no tiene por qué ser doce

No escribe nada.
"""
from __future__ import annotations

import os

for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"

import sys                                                   # noqa: E402
import time                                                  # noqa: E402
from pathlib import Path                                     # noqa: E402

_RAIZ = Path(__file__).resolve().parent
for _sub in (".", "scripts/models", "scripts/features"):
    if str(_RAIZ / _sub) not in sys.path:
        sys.path.insert(0, str(_RAIZ / _sub))

# Planos, y por el mismo motivo que los importa planos `_rodante.py`: importar
# `scripts.models.laboratorio` crea un segundo objeto de módulo con su propia caché, y
# entonces la guarda de `seleccion._lote` ve una matriz ajena y cae a la serie. Medir
# el reparto a través de una copia del módulo mediría otra cosa.
import laboratorio as lb                                     # noqa: E402
import seleccion as sl                                       # noqa: E402

import _rodante as rd                                        # noqa: E402

N_CAND = 12
KW = {"hiper": rd.HIPER, "cuotas": rd.CASA, "n_boot": 0,
      "politica": sl.POLITICA_SELECCION}


def _kw() -> dict:
    """Lo que `adelante` reenvía a `_lote`, sin la política que `_mide_uno` fija."""
    return {k: v for k, v in KW.items() if k != "politica"}


def main() -> int:
    feat = lb.datos()
    cfg = rd.cfg_de("2019/20")
    pool = [c for c in sl.candidatas(feat, regime="semanal")
            if c not in sl.SEMILLA_MERCADO][:N_CAND]
    semilla = list(sl.SEMILLA_MERCADO)
    conjuntos = [semilla + [c] for c in pool]
    print(f"bloque de elección de 2019/20 · {len(conjuntos)} candidatas")

    t0 = time.time()
    sl._lote([conjuntos[0]], rd.FAMILIA, cfg, kw=_kw(), n_jobs=1, feat=feat)
    frio = time.time() - t0
    t0 = time.time()
    sl._lote([conjuntos[1]], rd.FAMILIA, cfg, kw=_kw(), n_jobs=1, feat=feat)
    cal = time.time() - t0
    print(f"padre: 1ª candidata {frio:.1f} s · 2ª {cal:.1f} s")

    for n in (6, 4, 10):
        print(f"\n--- n_jobs={n} ---")
        for pasada in (1, 2):
            t0 = time.time()
            r = sl._lote(conjuntos, rd.FAMILIA, cfg, kw=_kw(), n_jobs=n)
            dt = time.time() - t0
            malas = sum(1 for x in r if isinstance(x, str))
            print(f"  lote suelto, pasada {pasada}: {dt:7.1f} s "
                  f"({dt / len(conjuntos):5.2f} s/cand · "
                  f"{cal * len(conjuntos) / dt:4.2f}x) fallos={malas}")
        from joblib import parallel_backend
        with parallel_backend("loky", n_jobs=n):
            for pasada in (1, 2):
                t0 = time.time()
                sl._lote(conjuntos, rd.FAMILIA, cfg, kw=_kw(), n_jobs=n)
                dt = time.time() - t0
                print(f"  contexto persistente, pasada {pasada}: {dt:7.1f} s "
                      f"({dt / len(conjuntos):5.2f} s/cand · "
                      f"{cal * len(conjuntos) / dt:4.2f}x)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
