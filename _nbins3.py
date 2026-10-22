"""Inserta C.9.3 —el mercado más lo mejor del resto— detrás de C.9.2 en el 05."""
from __future__ import annotations

import json
from pathlib import Path

RUTA = Path(__file__).resolve().parent / "notebooks" / "05_MODELADO_Y_EVALUACION.ipynb"

MD = r"""### C.9.3 · ¿Y el mercado MÁS lo mejor del resto?

Es la objeción directa, y la que cualquiera haría en la defensa. C.9 contestó «nada
mejora al precio», pero ese conjunto —el mercado junto a las mejores del resto— **la
voraz no podía encontrarlo aunque existiera**, y por dos limitaciones que son del
procedimiento y no de los datos.

**Una: la penalización estaba clavada.** `laboratorio.HIPER["logistica"]` fija
`C=0.3, l1_ratio=0.0` para toda la búsqueda. El propio módulo declara el precio de esa
decisión —«un conjunto puede parecer peor que otro por llevar una configuración que
no le convenía»— y lo remite a `afina()`, pero `afina()` solo afina el conjunto que
**ya ganó**, así que la compensación nunca llegó a los conjuntos que la necesitaban.
Y la necesitaban:
las **mismas 65 columnas** pasan de 0.200669 con `C=0.3, L2` a **0.194557** con
`C=0.03, L1`. Son **0.0061 de RPS recuperados por la penalización sola, sin tocar una
variable**. Dicho de otro modo: la voraz comparaba k=3 contra k=12 con un castigo de
varianza calibrado para k=3, y buena parte de lo que medía era ese castigo y no la falta
de señal.

**Dos: la voraz es miope.** Añade de una en una y se queda con la que más mejora
sola. Si un BLOQUE ayuda y ninguno de sus miembros ayuda por separado, no lo encuentra
nunca. Y eso no es hipotético aquí: C.9.2 midió que `fuerza` —nueve columnas— **bate a
las 59 de juego juntas**.

Se corrigen las dos a la vez. Cada conjunto se mide con **su** mejor penalización de
cinco —incluida la semilla, para que la pelea sea limpia en los dos ejes— y los
conjuntos son dominios **enteros**, más la escalera acumulativa, que es la pregunta en
su forma literal: el mercado más los dominios de mejor a peor. El orden de la escalera
no se elige mirando estas cifras —eso sería la voraz otra vez— sino el que C.9.2 ya
midió **sin el precio delante**, donde el precio no puede haber tapado a nadie.

| conjunto | k | RPS (su mejor penalización) | gana a la semilla | intervalo |
|---|---|---|---|---|
| **semilla+`bajas`** | 7 | **0.192958** | **+0.000211** | [−0.000377, +0.000814] |
| semilla (listón) | 3 | 0.193169 | — | — |
| semilla+`historial` | 6 | 0.193225 | −0.000057 | [−0.000327, +0.000221] |
| semilla+`contexto` | 7 | 0.193647 | −0.000478 | [−0.001349, +0.000436] |
| semilla+`fatiga` | 9 | 0.193683 | −0.000514 | [−0.001471, +0.000457] |
| semilla+`clasificacion` | 10 | 0.193694 | −0.000525 | [−0.001146, +0.000112] |
| semilla+`fuerza` | 12 | 0.193949 | −0.000780 | [−0.001584, +0.000070] |
| semilla+`forma` | 29 | 0.194065 | −0.000897 | [−0.001737, −0.000099] |

**La corrección estrecha la distancia de verdad, y conviene decirlo en voz alta:**
`semilla+fuerza` pasa de perder 0.001081 a perder **0.000780**. La comparación original
era injusta y ahora no lo es. Pero el veredicto no se mueve, y la escalera acumulativa
lo dice del modo más crudo —**monótonamente peor**, 0.193949 con `fuerza`, 0.194226 con
`forma`, hasta **0.194399** con las 62, con intervalos que excluyen el cero desde
`forma` en adelante—. No es ruido, es daño.

Queda una ruta más, la cuarta, y es la más generosa con la hipótesis: ordenar las 59 de
juego **por mérito propio** (k=1, sin el precio delante, donde ninguna puede quedar
tapada por el mercado) y juntar la semilla con las N mejores. El orden sale como debe
—`elo_z_diff` 0.198131, `strength_attack_diff` 0.203888, `cum_xgd_pg_diff` 0.204455— y
**ningún corte gana**: top3 −0.000259, top5 −0.000252, top8 −0.000335, top12 −0.000593,
top20 −0.000813, top30 −0.000928. Ordenar 59 variables sobre los mismos 1140 partidos y
quedarse con la cabeza es sesgo de máximo en estado puro, y se hace a propósito: si ni
con esa ventaja gana a la semilla, la respuesta es firme.

**Los dos conjuntos que sí ganaron pasan por la cuarta puerta, y los dos caen con el
signo invertido**, que es la firma de haber escogido el máximo de treinta y cinco
configuraciones. `semilla+bajas` gana 0.000211 donde se eligió y **pierde 0.000312 donde
no** —y es, no por casualidad, el mismo dominio de `out_minshare_max_diff`, una de las
dos variables que la voraz adoptó y esta misma puerta devolvió—. `semilla+historial`
empata en RPS y gana **+0.61 pp de acierto**, que parecen algo hasta que se miden contra
su propio error típico: el del acierto sobre 1140 partidos es **1.48 pp**, así que esa
ganancia son **0.41 errores típicos**, es decir nada; y fuera pierde 0.000586 con
intervalo [+0.000075, +0.001115], que ya excluye el cero.

Para que la puerta pudiera juzgarlos hubo que darle un argumento que no tenía:
`seleccion.confirma()` acepta ahora `hiper`. Un conjunto adoptado bajo `C=0.1, L1` y
confirmado bajo `C=0.3, L2` se estaría puntuando con un castigo de varianza que su
elección no tuvo, y el rechazo no distinguiría la falta de señal del cambio de
configuración. La semilla y el conjunto se puntúan siempre con la misma, que es lo que
mantiene la diferencia emparejada interpretable.

> Ninguna cifra de esta sección es citable (regla 5): sale del bloque de selección, y
> además elige la penalización mirando los mismos partidos —cinco miradas por conjunto—.
> Ese sesgo es simétrico entre conjuntos y por tanto no favorece a los grandes, que es
> lo que se pone a prueba, pero sesgo es. La puerta de verdad es
> `seleccion.confirma()` sobre 2017/18–2018/19. Las tablas las producen `_bloques.py`
> y `_mejores.py`, y viven en `seleccion.BLOQUES`, `MEJORES` y `MEJORES_ORDEN`.
"""

CODE = r'''# Regla 8: las dos tablas se leen de disco, no se recalculan. Las produjeron
# `_bloques.py` (quince conjuntos x cinco penalizaciones) y `_mejores.py` (las 59 solas
# mas los cortes acumulados), y viven bajo `outputs/`, que no se versiona.
if not (sl.BLOQUES.exists() and sl.MEJORES.exists()):
    print(f"sin el cuadro de bloques en {sl.BLOQUES}")
else:
    bq = pd.read_csv(sl.BLOQUES)
    mj = pd.read_csv(sl.MEJORES)

    def _mejor(t):
        """Cada conjunto en SU mejor penalizacion, que es lo que la busqueda no hizo."""
        return (t.loc[t.groupby("conjunto")["rps"].idxmin()]
                 .sort_values("rps").reset_index(drop=True))

    bqm, mjm = _mejor(bq), _mejor(mj)
    sem = float(bqm.loc[bqm["conjunto"] == "semilla", "rps"].iloc[0])

    # El precio de haber clavado la penalizacion, medido en este mismo cuadro: cuanto
    # recupera cada conjunto al dejarle elegir la suya en vez de la de la busqueda.
    fijo = (bq.loc[(bq["C"] == 0.3) & (bq["l1_ratio"] == 0.0)]
              .set_index("conjunto")["rps"])
    rec = (fijo - bqm.set_index("conjunto")["rps"]).sort_values(ascending=False)
    print(f"semilla en su mejor penalizacion : {sem:.6f}")
    print(f"lo que recupera la penalizacion  : hasta {rec.max():.6f} "
          f"({rec.idxmax()}), y solo {rec.get('semilla', float('nan')):.6f} en la "
          f"semilla")
    print("   -> la rejilla ayuda a los conjuntos GRANDES y no al pequeno: por eso la "
          "comparacion original era injusta")

    gan = bqm.loc[bqm["gana"] > 0, "conjunto"].tolist()
    print(f"conjuntos que ganan a la semilla : {', '.join(gan) if gan else 'ninguno'}"
          f"  (y la cuarta puerta los rechaza, con el signo al reves)")
    peor = mjm.loc[mjm["conjunto"] != "semilla", "gana"].max()
    print(f"el mejor de los cortes top-N     : {peor:+.6f} de RPS, tampoco gana")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))

    # Izquierda: cada dominio entero contra la semilla, con su intervalo por jornadas.
    bl = bqm[bqm["conjunto"].str.startswith("semilla+")].sort_values("gana")
    col = [PALETA["modelo"] if g > 0 else PALETA["apoyo"] for g in bl["gana"]]
    ax1.barh(bl["conjunto"].str.replace("semilla+", ""), bl["gana"], color=col)
    ax1.errorbar(bl["gana"], range(len(bl)),
                 xerr=[bl["gana"] - bl["inf"], bl["sup"] - bl["gana"]],
                 fmt="none", ecolor="black", elinewidth=1, capsize=3)
    ax1.axvline(0, color=PALETA["mercado"], lw=1.2)
    ax1.set_xlabel("RPS que le quita a la semilla (positivo = gana)")
    ax1.set_title("El mercado mas cada dominio entero, afinado")

    # Derecha: las dos escaleras. La acumulativa por dominios y los cortes top-N, ambas
    # contra el liston de la semilla. Ninguna baja de la linea.
    ORD = ["fuerza", "forma", "clasificacion", "historial", "fatiga", "bajas",
           "contexto"]
    paso = {f"escalera+{d}": i for i, d in enumerate(ORD)}
    # `esc` y no `es`: el cuaderno importa `estimators as es` en la cabecera, y
    # `test_referencias` resuelve los atributos de los alias de modulo contra el .py
    # real. Una variable local llamada `es` convierte `es.iterrows()` en una
    # referencia rota a `estimators.iterrows`, y la bateria lo denuncia con razon.
    esc = (bqm[bqm["conjunto"].isin(paso)]
           .assign(paso=lambda t: t["conjunto"].map(paso)).sort_values("paso"))
    tn = mjm[mjm["conjunto"].str.startswith("semilla+top")].sort_values("k")
    ax2.axhline(sem, color=PALETA["mercado"], lw=1.2, ls="--",
                label=f"semilla sola ({sem:.6f})")
    ax2.plot(esc["k"], esc["rps"], "o-", color=PALETA["modelo"],
             label="escalera por dominios enteros")
    ax2.plot(tn["k"], tn["rps"], "s-", color=PALETA["apoyo"],
             label="semilla + las N mejores por merito propio")
    for _, r in esc.iterrows():
        ax2.annotate(str(r["conjunto"]).replace("escalera+", ""), (r["k"], r["rps"]),
                     fontsize=7.5, xytext=(3, 4), textcoords="offset points")
    ax2.set_xlabel("k (numero de columnas del conjunto)")
    ax2.set_ylabel("RPS en su mejor penalizacion")
    ax2.set_title("Anadir no baja la linea, por ninguna de las dos rutas")
    ax2.legend(fontsize=8)
    plt.tight_layout(); plt.show()
'''


def main() -> int:
    nb = json.loads(RUTA.read_text(encoding="utf-8"))
    cells = nb["cells"]

    # Ancla: C.9.2 es markdown en 53 y su celda de codigo en 54.
    assert "C.9.2" in "".join(cells[53]["source"]), "53 no es C.9.2"
    assert "SINPRECIO" in "".join(cells[54]["source"]), "54 no es el codigo de C.9.2"

    nuevas = [
        {"cell_type": "markdown", "metadata": {},
         "source": MD.splitlines(keepends=True)},
        {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
         "source": CODE.splitlines(keepends=True)},
    ]
    # Idempotente: si C.9.3 ya esta, se SUSTITUYE en su sitio en vez de no tocar nada.
    # Corregir una linea del texto o del codigo es lo normal mientras se redacta, y un
    # script que solo sabe insertar obliga a borrar celdas a mano en el JSON, que es
    # justo la operacion en la que se rompe un cuaderno.
    if "C.9.3" in "".join(cells[55]["source"]):
        assert "BLOQUES" in "".join(cells[56]["source"]), "56 no es el codigo de C.9.3"
        cells[55:57] = nuevas
        accion = "sustituidas"
    else:
        assert "C.10" in "".join(cells[55]["source"]), "55 no es C.10"
        cells[55:55] = nuevas
        accion = "insertadas"
    RUTA.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"{accion} 2 celdas en 55-56; el cuaderno tiene ahora {len(cells)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
