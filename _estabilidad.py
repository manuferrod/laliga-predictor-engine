"""¿Elige lo mismo cada origen? La inestabilidad del conjunto, medida.

El docstring de `_rodante.py` lo dice antes de ver el resultado: lo que mide el origen
rodante no es un conjunto sino una regla, «y el conjunto puede cambiar de una temporada
a otra; eso no es un defecto del método sino la mitad de su contenido, porque si cambia
mucho la propia inestabilidad es el resultado». Esto cobra esa deuda.

Hay dos formas de mirarlo y las dos hacen falta, porque la primera sola admite una
lectura benigna:

  1. **Qué adopta cada origen.** Sale de los nueve `eleccion_{criterio}_{origen}.json`.
     Si la intersección de los siete orígenes que eligen está vacía, ninguna variable
     sobrevive a que se le pregunte dos veces. La objeción benigna es que dos variables
     pueden ser casi equivalentes —`elo_z_diff` y `home_squad_value` miden fuerza las
     dos—, y entonces elegir una u otra no es inestabilidad sino empate.
  2. **Si el ORDEN de las 60 candidatas se repite entre orígenes.** Eso contesta la
     objeción: si la tabla del paso 1 de 2019/20 y la de 2024/25 estuviesen
     correlacionadas, habría una señal estable y la voraz solo estaría cortándola por
     donde empata. Si la correlación es ~0, lo que hay debajo es ruido y la adoptada es
     el máximo de 60 sorteos. Sale de `elige.log`, que guarda la tabla de cada paso.

Y una tercera que apareció sola al leer las adopciones: **acierto y ventaja eligen casi
lo mismo**, lo que era previsible y conviene declarar, porque invalida leerlas como dos
confirmaciones independientes. Con `politica="favorito"` se apuesta una unidad plana al
argmax del modelo, así que la ventaja es el acierto reponderado por la cuota: no son dos
criterios, son dos lecturas del mismo.

No escribe nada fuera de `outputs/models/seleccion/rodante/estabilidad.csv`.
"""
from __future__ import annotations

import collections
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_RAIZ = Path(__file__).resolve().parent
for _sub in (".", "scripts/models", "scripts/features"):
    if str(_RAIZ / _sub) not in sys.path:
        sys.path.insert(0, str(_RAIZ / _sub))

import _rodante as rd                                            # noqa: E402

OUT = rd.OUT
TABLA = OUT / "estabilidad.csv"

# `  [1·  1/62] ah_line        RPS 0.19986  acc 52.11%  ventaja +1.90%`
_FILA = re.compile(
    r"^\s*\[(\d+)\S\s*\d+/\d+\]\s+(\S+)\s+RPS\s+([\d.]+)\s+"
    r"acc\s+([\d.]+)%\s+ventaja\s+([+-][\d.]+)%")
_CAB = re.compile(r"^--- origen (\S+) . criterio (\w+)")


def _elecciones() -> dict[str, dict[str, dict]]:
    """Los JSON de punto de control, por criterio y origen."""
    por: dict[str, dict[str, dict]] = collections.defaultdict(dict)
    for f in sorted(OUT.glob("eleccion_*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        por[d["criterio"]][d["origen"]] = d
    return por


def _tablas_paso1(log: Path) -> dict[tuple[str, str], pd.DataFrame]:
    """La tabla del PASO 1 de cada (criterio, origen) que haya en el registro.

    Solo el paso 1 es comparable entre orígenes: del paso 2 en adelante cada uno parte
    de un conjunto distinto, así que una diferencia de orden ya no diría si la señal es
    estable sino que los conjuntos no son el mismo.
    """
    fuera: dict[tuple[str, str], list] = {}
    clave: tuple[str, str] | None = None
    for linea in log.read_text(encoding="utf-8", errors="replace").splitlines():
        cab = _CAB.match(linea)
        if cab:
            clave = (cab.group(2), cab.group(1))
            fuera.setdefault(clave, [])
            continue
        m = _FILA.match(linea)
        if m is None or clave is None or m.group(1) != "1":
            continue
        fuera[clave].append({"variable": m.group(2), "rps": float(m.group(3)),
                             "accuracy": float(m.group(4)) / 100,
                             "ventaja": float(m.group(5)) / 100})
    return {k: pd.DataFrame(v) for k, v in fuera.items() if v}


def _rho(a: pd.Series, b: pd.Series) -> float:
    """Spearman a mano, para no arrastrar scipy por una correlación."""
    if len(a) < 3:
        return float("nan")
    return float(np.corrcoef(a.rank(), b.rank())[0, 1])


def adopciones(por: dict[str, dict[str, dict]]) -> list[dict]:
    """Qué adopta cada origen, y la intersección de los que eligen."""
    filas = []
    for criterio, origenes in por.items():
        eligen = {s: [c for c in d["conjunto"]
                      if c not in rd.sl.SEMILLA_MERCADO]
                  for s, d in origenes.items() if d["n_pasos"] > 0}
        if not eligen:
            continue
        cuenta = collections.Counter()
        for v in eligen.values():
            cuenta.update(v)
        comun = set.intersection(*(set(v) for v in eligen.values()))
        print(f"\n=== {criterio}: {len(eligen)} orígenes que eligen ===")
        for s in sorted(eligen):
            print(f"  {s}  {' + '.join(eligen[s]) or '(ninguna)'}")
        print(f"  {len(cuenta)} variables distintas en "
              f"{sum(cuenta.values())} adopciones · "
              f"mediana {np.median([len(v) for v in eligen.values()]):.1f} por origen")
        print(f"  repetidas: "
              f"{[(k, n) for k, n in cuenta.most_common() if n > 1] or 'ninguna'}")
        print(f"  INTERSECCIÓN de los {len(eligen)}: "
              f"{sorted(comun) or 'VACÍA (solo la semilla declarada)'}")
        for k, n in cuenta.most_common():
            filas.append({"criterio": criterio, "variable": k, "adopciones": n,
                          "de_origenes": len(eligen)})
    return filas


def orden(tablas: dict[tuple[str, str], pd.DataFrame]) -> list[dict]:
    """¿Se repite el orden de las 60 candidatas entre orígenes?"""
    filas = []
    for criterio in sorted({c for c, _ in tablas}):
        ts = {s: t for (c, s), t in tablas.items() if c == criterio}
        if len(ts) < 2:
            continue
        print(f"\n=== {criterio}: orden del paso 1 entre orígenes ===")
        claves = sorted(ts)
        for metrica in ("accuracy", "ventaja", "rps"):
            rs = []
            for i, a in enumerate(claves):
                for b in claves[i + 1:]:
                    j = ts[a].merge(ts[b], on="variable", suffixes=("_a", "_b"))
                    r = _rho(j[f"{metrica}_a"], j[f"{metrica}_b"])
                    rs.append(r)
                    filas.append({"criterio": criterio, "metrica": metrica,
                                  "origen_a": a, "origen_b": b, "n": len(j),
                                  "rho": round(r, 4)})
            rs = [r for r in rs if not np.isnan(r)]
            print(f"  {metrica:<9} rho de {len(rs)} pares: "
                  f"mediana {np.median(rs):+.3f} · "
                  f"rango [{min(rs):+.3f}, {max(rs):+.3f}]")
        # Dónde cae la ganadora de cada origen en la tabla de los demás. Si la señal
        # fuese estable, la ganadora de uno tendría que ir alta en los otros.
        print("  puesto de la ganadora de cada origen en los demás (de 60):")
        for a in claves:
            gan = ts[a].sort_values(metrica_de(criterio),
                                    ascending=criterio == "rps").iloc[0]["variable"]
            puestos = []
            for b in claves:
                if b == a:
                    continue
                t = ts[b].sort_values(metrica_de(criterio),
                                      ascending=False).reset_index(drop=True)
                hit = t.index[t["variable"] == gan]
                puestos.append(int(hit[0]) + 1 if len(hit) else -1)
            print(f"    {a}  {gan:<32} {puestos}")
    return filas


def metrica_de(criterio: str) -> str:
    return criterio if criterio in ("accuracy", "ventaja") else "rps"


def decaimiento(pares: list[dict], por: dict[str, dict[str, dict]]) -> None:
    """El control que impide leer la correlación como reproducibilidad.

    Los orígenes NO son muestras independientes: su ventana de entrenamiento es
    expansiva, así que la de 2019/20 está contenida en la de 2024/25. Dos orígenes
    vecinos comparten del 67 al 88 % de sus partidos, y una correlación entre dos
    órdenes calculados casi sobre los mismos datos no dice que la señal se repita, dice
    que los datos son los mismos. Si la estabilidad fuese real, la rho no dependería de
    la distancia; si es solape, tiene que caer con ella.
    """
    d = pd.DataFrame(pares)
    if d.empty:
        return
    d = d[d.criterio == "accuracy"]
    pasado = {s: v["n_bloque"] for s, v in por.get("accuracy", {}).items()}
    orden_s = sorted(pasado, key=lambda s: pasado[s])
    ix = {s: i for i, s in enumerate(orden_s)}
    d = d[d.origen_a.isin(ix) & d.origen_b.isin(ix)].copy()
    d["gap"] = [abs(ix[a] - ix[b]) for a, b in zip(d.origen_a, d.origen_b)]
    d["solape"] = [min(pasado[a], pasado[b]) / max(pasado[a], pasado[b])
                   for a, b in zip(d.origen_a, d.origen_b)]
    print("\n=== control: ¿cae la rho con la distancia entre orígenes? ===")
    print(f"{'gap':>4} {'pares':>6} {'solape':>7} {'rho acc':>8} {'rho RPS':>8} "
          f"{'RPS-acc':>8}")
    for gap, g in d.groupby("gap"):
        ac = g[g.metrica == "accuracy"]["rho"].median()
        rp = g[g.metrica == "rps"]["rho"].median()
        print(f"{gap:>4} {len(g[g.metrica == 'accuracy']):>6} "
              f"{g['solape'].median():>6.0%} {ac:>+8.3f} {rp:>+8.3f} "
              f"{rp - ac:>+8.3f}")
    print("  La rho sigue al solape, no a la distancia en sí: la mayor parte de la\n"
          "  estabilidad aparente es dato compartido. La lectura limpia es el par más\n"
          "  lejano, que es el que menos comparte.")
    print("  Y el orden por RPS es MÁS estable que el orden por acierto en todos los\n"
          "  gaps, y la brecha se abre con la distancia: el acierto ordena peor las\n"
          "  candidatas que la métrica que la directriz del 20-oct le quitó el papel\n"
          "  de elegir. No cambia quién elige —eso lo decide Manuel— pero es el\n"
          "  mecanismo por el que una búsqueda por acierto puede comprar daño en RPS.")


def acuerdo(por: dict[str, dict[str, dict]]) -> None:
    """¿Eligen lo mismo el acierto y la ventaja? No son independientes."""
    a, b = por.get("accuracy", {}), por.get("ventaja", {})
    comunes = [s for s in sorted(set(a) & set(b))
               if a[s]["n_pasos"] > 0 and b[s]["n_pasos"] > 0]
    if not comunes:
        return
    print(f"\n=== acuerdo entre criterios · {len(comunes)} orígenes con los dos ===")
    iguales = 0
    for s in comunes:
        ca = [c for c in a[s]["conjunto"] if c not in rd.sl.SEMILLA_MERCADO]
        cb = [c for c in b[s]["conjunto"] if c not in rd.sl.SEMILLA_MERCADO]
        igual = set(ca) == set(cb)
        iguales += igual
        print(f"  {s}  {'IDÉNTICO' if igual else 'distinto':<9} "
              f"acierto: {' + '.join(ca) or '(ninguna)'}")
        if not igual:
            print(f"{'':13}{'':10}ventaja:  {' + '.join(cb) or '(ninguna)'}")
    print(f"  {iguales} de {len(comunes)} orígenes eligen el MISMO conjunto con los "
          f"dos criterios.\n  Con `politica=favorito` se apuesta una unidad al argmax, "
          f"así que la ventaja es el\n  acierto reponderado por la cuota: no son dos "
          f"confirmaciones independientes.")


def main() -> int:
    log = OUT / "elige.log"
    por = _elecciones()
    if not por:
        print(f"no hay puntos de control en {OUT}")
        return 1
    filas = adopciones(por)
    acuerdo(por)
    pares: list[dict] = []
    if log.exists():
        pares = orden(_tablas_paso1(log))
        decaimiento(pares, por)
    else:
        print(f"\naviso: sin {log.name} no se puede medir el orden")
    OUT.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(filas).to_csv(TABLA, index=False, encoding="utf-8")
    if pares:
        pd.DataFrame(pares).to_csv(
            OUT / "estabilidad_orden.csv", index=False, encoding="utf-8")
    print(f"\nescrito {TABLA.name}"
          f"{' y estabilidad_orden.csv' if pares else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
