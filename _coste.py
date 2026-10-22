"""¿Cuánto cuesta de verdad un protocolo, y por qué el reparto no amortizaba?

Antes de dimensionar una búsqueda hay que saber el precio de una evaluación. La medida
ingenua —lanzar un lote con `n_jobs` y dividir— daba **7.59 s por conjunto con diez
procesos**, prácticamente lo mismo que en serie, y un lote de 200 conjuntos tardó
**22 minutos cuando en serie habría tardado 28**. Eso no es que el reparto no amortice:
es que estorba.

La causa se ve en la contabilidad de CPU: diez hijos acumulaban 1140 s de CPU cada uno
en 1320 s de pared, o sea **11.400 segundos-núcleo para un trabajo de ~1.700**. Cada
hijo abre su propia piscina de hilos de BLAS —`numpy` la dimensiona por
`os.cpu_count()`, que no sabe nada de que hay otros nueve hermanos—, así que 10 × 12 da
**120 hilos peleándose por 12 núcleos**. El coste no está en el cálculo sino en los
cambios de contexto.

El arreglo son tres variables de entorno **antes de importar numpy**: un hilo por
proceso, y que el paralelismo lo ponga el reparto de conjuntos, que es donde el trabajo
sí es independiente. Esto hay que fijarlo en cualquier script que llame a `_lote`.

Mide tres cosas sobre los mismos conjuntos:
  1. serie con un hilo       — el precio limpio de un protocolo
  2. serie sin fijar hilos   — cuánto le daba BLAS a un proceso solo
  3. reparto con un hilo     — la amortización real, y de ahí el coste de un paso de haz

Uso:
    python _coste.py
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

N_CONJ = 50
_RAIZ = Path(__file__).resolve().parent


def _pin(n: str) -> None:
    for v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
              "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ[v] = n


def main() -> int:
    modo = sys.argv[1] if len(sys.argv) > 1 else "pin"
    _pin("1" if modo == "pin" else str(os.cpu_count() or 1))

    for sub in ("scripts", "scripts/models", "scripts/features"):
        sys.path.insert(0, str(_RAIZ / sub))
    import laboratorio as lb        # noqa: E402
    import protocol as pt           # noqa: E402
    import seleccion as sl          # noqa: E402

    cfg = pt.ProtocolConfig(regime="semanal", window="expansiva",
                            sliding_seasons=pt.SLIDING_SEASONS, halflife_days=None)
    feat = lb.datos()
    sem = list(sl.SEMILLA_MERCADO)
    pool = [c for c in sl.candidatas(feat, regime=cfg.regime) if c not in sem]
    conj = [tuple(sem + [v]) for v in pool[:N_CONJ]]
    print(f"[{modo}] {len(pool)} candidatas, semilla de {len(sem)}", flush=True)

    # Calentar: el primer protocolo paga la carga del parquet y de sklearn, y meterlo
    # en la media mentiria sobre el coste marginal, que es el que dimensiona la
    # busqueda.
    sl._lote(conj[:1], sl.FAMILIA, cfg, kw={}, n_jobs=1, feat=feat)

    t0 = time.time()
    sl._lote(conj[:6], sl.FAMILIA, cfg, kw={}, n_jobs=1, feat=feat)
    serie = (time.time() - t0) / 6
    print(f"[{modo}] serie      {serie:6.2f} s/conjunto", flush=True)

    if modo == "pin":
        # Sin `feat`: `_lote` solo reparte si la matriz es la canonica, porque un hijo
        # leeria del disco otra distinta. Pasar `lb.datos()` tambien valdria —es la
        # propia cache— pero omitirlo deja claro que aqui se quiere el reparto.
        #
        # El calentamiento tiene que cubrir a TODOS los hijos. Con dos conjuntos solo
        # dos de los diez quedaban calientes, y al temporizar diez tareas sobre diez
        # procesos los otros ocho pagaban la lectura del parquet DENTRO del cronometro:
        # medi 7.10 s/conjunto y 2.3x de amortizacion, que era el arranque disfrazado de
        # coste marginal. Con 2x nucleos de calentamiento y 50 tareas temporizadas cada
        # hijo hace cinco y el arranque se diluye.
        nuc = sl._nucleos(0)
        sl._lote(conj[:2 * nuc], sl.FAMILIA, cfg, kw={}, n_jobs=0)
        t0 = time.time()
        sl._lote(conj, sl.FAMILIA, cfg, kw={}, n_jobs=0)
        par = (time.time() - t0) / len(conj)
        print(f"[{modo}] reparto    {par:6.2f} s/conjunto  "
              f"({nuc} procesos, amortiza {serie / max(par, 1e-9):.1f}x)", flush=True)
        paso = 5 * 60 * par
        print(f"\nun paso del haz (5 x 60 = 300 conjuntos) = {paso / 60:.1f} min")
        print(f"20 pasos = {20 * paso / 3600:.1f} h")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
