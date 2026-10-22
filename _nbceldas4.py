"""Ensayo de las tres celdas de código que C.9.5, C.9.6 y C.9.7 llevan al cuaderno 05.

Existe para probarlas FUERA del notebook antes de insertarlas: una celda que falla
dentro de un `.ipynb` deja el cuaderno con una traza guardada, y el cuaderno es
entregable. Se ejecuta con `python _nbceldas4.py` desde la raíz y no escribe nada; quien
las lleva al cuaderno es `_nbins4.py`, que importa de aquí lo que se probó.

Las tres celdas cumplen la regla 8 —leen de disco lo que `_censo.py`, `_rodante.py` y
`_nueve.py` dejaron escrito, sin recalcular una sola cifra— y la regla 5: la única
lectura citable del último cuadro es la de dejar-una-fuera, y la celda lo dice en su
propia salida en vez de dejarlo en la prosa de arriba.

El ámbito que se les pasa es el que el cuaderno tiene vivo en A.0 —`sl`, `pd`, `plt` y
`PALETA`—, y nada más: si una celda necesita `json`, se lo importa ella, porque A.0 no
lo importa.
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt                                  # noqa: E402

_RAIZ = Path(__file__).resolve().parent
for _sub in ("scripts/models", "scripts/features"):
    if str(_RAIZ / _sub) not in sys.path:
        sys.path.insert(0, str(_RAIZ / _sub))

import seleccion as sl                                           # noqa: E402

PALETA = {"modelo": "#1f4e79", "mercado": "#c0504d", "apoyo": "#7f7f7f"}

CELDA_95 = '''
# Regla 8: las dos tablas se leen de disco. Las produjo `_censo.py` —la misma busqueda de
# acierto y ROI de `_maximo.py`, pero con el censo PARTIDO: elige en `seleccion` y mide en
# `medicion` y en `confirmacion`— y viven bajo `outputs/`, que no se versiona.
import json

if not (sl.CENSO_GANADORES.exists() and sl.CENSO_FUERA.exists()):
    print(f"sin la decima ruta en {sl.CENSO_CURVA}")
else:
    gan = json.loads(sl.CENSO_GANADORES.read_text(encoding="utf-8"))
    fue = pd.read_csv(sl.CENSO_FUERA)
    sem = gan["semilla"]

    # DENTRO del bloque que eligio: el mayor salto medido en el proyecto, y por eso
    # mismo el mas sospechoso. Son 2314 evaluaciones y el optimismo crece con ellas.
    print(f"{gan['evaluaciones']:,} evaluaciones en {gan['minutos']:.0f} min, "
          f"eligiendo con '{gan['elige']}' sobre un banco de {gan['n_banco']}\\n")
    print(f"--- dentro de '{gan['elige']}' ({sem['n']:,} partidos) ---")
    print(f"   semilla          acierto {sem['acierto'] * 100:.2f}%  "
          f"ROI max {sem['roi_max'] * 100:+.2f}%  RPS {sem['rps']:.6f}")
    for cual in ("ganador_acierto", "ganador_roi"):
        g = gan[cual]
        print(f"   {cual:<16} acierto {g['acierto'] * 100:.2f}%  "
              f"ROI max {g['roi_max'] * 100:+.2f}%  RPS {g['rps']:.6f}  "
              f"(k={len(g['conjunto'])})")
    print("   -> el mayor salto del proyecto, y medido donde se eligio: incitable")

    # FUERA se parte en dos, y eso es el veredicto: gana en `medicion` y pierde en
    # `confirmacion`. Un signo que se invierte es el maximo de una nube, no una ventaja.
    print("\\n--- fuera, contra la SEMILLA sobre los mismos partidos ---")
    for b in ("medicion", "confirmacion", "ambos"):
        r = fue[(fue["bloque"] == b) & (fue["quien"] == "ganador_acierto")].iloc[0]
        print(f"   {b:<13} n={int(r['n']):>4}  acierto {r['dac_semilla'] * 100:+.2f} pp "
              f"[{r['inf_dac_semilla'] * 100:+.2f},{r['sup_dac_semilla'] * 100:+.2f}]"
              f"   ROI max {r['droimax_semilla'] * 100:+.2f} pp "
              f"[{r['inf_droimax_semilla'] * 100:+.2f},"
              f"{r['sup_droimax_semilla'] * 100:+.2f}]")
    amb = fue[(fue["bloque"] == "ambos") & (fue["quien"] == "ganador_acierto")].iloc[0]
    med = fue[(fue["bloque"] == "medicion") & (fue["quien"] == "ganador_acierto")].iloc[0]
    con = fue[(fue["bloque"] == "confirmacion")
              & (fue["quien"] == "ganador_acierto")].iloc[0]
    print(f"\\n   el signo del acierto repite fuera: "
          f"{bool(med['dac_semilla'] > 0) == bool(con['dac_semilla'] > 0)}"
          "   <- se parte en dos, asi que NO")

    # Y el dano que si es significativo: el RPS empeora en las tres ventanas con los
    # tres intervalos excluyendo el cero. Esto no es ruido de muestra.
    print("\\n--- el RPS, que es lo unico que sale significativo ---")
    rps = fue[fue["quien"] == "ganador_acierto"]
    for _, r in rps.iterrows():
        print(f"   {r['bloque']:<13} RPS {r['drps']:+.6f} "
              f"[{r['inf_drps']:+.6f},{r['sup_drps']:+.6f}]  excluye el cero: "
              f"{bool(r['inf_drps'] > 0)}")
    print("   -> frente a la mezcla de C.9.4, que pierde 0.000527 de RPS SIN "
          "significacion y cuyo\\n      ROI si repite signo fuera, este conjunto es peor "
          "en las dos cosas a la vez")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.6))

    # Izquierda: el ROI maximo por bloque. La barra de dentro es enorme y las dos de
    # fuera se cancelan; es la figura entera del argumento.
    BLO = ["medicion", "confirmacion", "ambos"]
    x = range(len(BLO))
    for i, (quien, col) in enumerate((("semilla", PALETA["apoyo"]),
                                      ("ganador_acierto", PALETA["modelo"]))):
        t = fue[fue["quien"] == quien].set_index("bloque").reindex(BLO)
        ax1.bar([v + i * 0.38 for v in x], t["roi_max"] * 100, 0.38,
                color=col, label=quien)
    ax1.axhline(0, color="black", lw=0.8)
    ax1.set_xticks([v + 0.19 for v in x])
    ax1.set_xticklabels(BLO)
    ax1.set_ylabel("ROI a la mejor cuota (%)")
    ax1.set_title("Fuera se parte en dos: gana en una ventana y pierde en la otra")
    ax1.legend(fontsize=8)

    # Derecha: el RPS emparejado con su intervalo. Las tres barras caen enteras a la
    # derecha del cero, que es lo que no hace ninguna otra cifra de esta ruta.
    t = rps.set_index("bloque").reindex(BLO)
    ax2.errorbar(t["drps"], range(len(t)),
                 xerr=[t["drps"] - t["inf_drps"], t["sup_drps"] - t["drps"]],
                 fmt="o", color=PALETA["mercado"], capsize=4)
    ax2.axvline(0, color="black", lw=1.2)
    ax2.set_yticks(range(len(t)))
    ax2.set_yticklabels(BLO)
    ax2.set_xlabel("RPS contra la semilla (mas es PEOR)")
    ax2.set_title("El dano en RPS si excluye el cero, en las tres ventanas")

    fig.tight_layout()
    plt.show()
'''

CELDA_96 = '''
# Regla 8: la tabla se lee de disco. La produjo `_rodante.py` —nueve origenes por dos
# criterios, cada temporada predicha con un conjunto elegido con solo su pasado— y vive
# bajo `outputs/`, que no se versiona.
import json

if not (sl.RODANTE_TEMPORADA.exists() and sl.RODANTE_RESUMEN.exists()):
    print(f"sin la ruta rodante en {sl.RODANTE}")
else:
    tmp = pd.read_csv(sl.RODANTE_TEMPORADA)
    res = json.loads(sl.RODANTE_RESUMEN.read_text(encoding="utf-8"))
    PISTAS = ["semilla", "publicado", "rodante_ac", "rodante_roi"]

    # El censo LIMPIO son las seis temporadas que no eligieron el peso de la mezcla, y
    # es el unico donde las cuatro pistas se comparan: `publicado` vio las otras tres.
    print(f"censo entero: {res['n_partidos']:,} partidos en {res['pliegues']} pliegues "
          f"de {len(res['origenes'])} temporadas")
    print(f"censo limpio: {res['limpio']['semilla']['n']:,} partidos "
          f"(sin {', '.join(res['sucias_peso'])}, que eligieron el peso)\\n")

    fil = []
    for cual, etiq in (("limpio", "censo limpio"), ("agregado", "las nueve")):
        for p in PISTAS:
            d = res[cual][p]
            fil.append({"censo": etiq, "pista": p, "n": d["n"],
                        "acierto": d["acierto"], "rps": d["rps"],
                        "roi": d["roi_b365"], "liston": d["roi_ref_b365"],
                        "vent": d["vent_b365"],
                        "inf": d["vent_b365_inf"], "sup": d["vent_b365_sup"]})
    tab = pd.DataFrame(fil)
    for etiq in ("censo limpio", "las nueve"):
        t = tab[tab["censo"] == etiq]
        ref = t.iloc[0]
        print(f"--- {etiq} ({int(ref['n']):,} partidos) · liston "
              f"{ref['liston'] * 100:+.2f}% ---")
        for _, r in t.iterrows():
            print(f"   {r['pista']:<12} acierto {r['acierto'] * 100:.2f}%  "
                  f"RPS {r['rps']:.6f}  ROI {r['roi'] * 100:+.2f}%  "
                  f"ventaja {r['vent'] * 100:+.2f} pp "
                  f"[{r['inf'] * 100:+.2f},{r['sup'] * 100:+.2f}]")

    # Lo que decide: ningun intervalo excluye el cero. Que una pista gane por nivel no
    # la hace distinguible del liston, y el intervalo es lo que hay que mirar. La
    # comprobacion va FILA A FILA: un intervalo excluye el cero cuando sus dos extremos
    # caen al mismo lado, y agregar los extremos por separado no contesta eso.
    cruza = (tab["inf"] <= 0) & (tab["sup"] >= 0)
    mejor = tab[tab["censo"] == "censo limpio"].nlargest(1, "vent")["pista"].iloc[0]
    print(f"\\n   alguna ventaja excluye el cero: {not bool(cruza.all())}")
    print(f"   la mejor del censo limpio es '{mejor}' <- es la que YA se publica, "
          "asi que no hay nada que adoptar")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.6))

    # Izquierda: la ventaja sobre el liston temporada a temporada. El vaiven es el
    # argumento: lo que la media agregada esconde.
    por = tmp[tmp["temporada"] != "TODAS"]
    for p, est in zip(PISTAS, (":", "-", "--", "-.")):
        t = por[por["pista"] == p].sort_values("temporada")
        ax1.plot(t["temporada"], t["vent_b365"] * 100, est, marker="o", ms=4, label=p)
    ax1.axhline(0, color=PALETA["apoyo"], lw=1.0)
    ax1.set_ylabel("ventaja sobre el liston (pp)")
    ax1.set_title("Temporada a temporada: el vaiven que la media esconde")
    ax1.tick_params(axis="x", rotation=60)
    ax1.legend(fontsize=8)

    # Derecha: nivel contra intervalo en el censo limpio. Lo que hay que ver es que las
    # cuatro barras de error cruzan el cero.
    t = tab[tab["censo"] == "censo limpio"]
    ax2.errorbar(t["vent"] * 100, range(len(t)),
                 xerr=[(t["vent"] - t["inf"]) * 100, (t["sup"] - t["vent"]) * 100],
                 fmt="o", color=PALETA["modelo"], capsize=4)
    ax2.axvline(0, color=PALETA["mercado"], lw=1.4)
    ax2.set_yticks(range(len(t)))
    ax2.set_yticklabels(t["pista"])
    ax2.set_xlabel("ventaja sobre el liston (pp)")
    ax2.set_title("Censo limpio: las cuatro cruzan el cero")

    fig.tight_layout()
    plt.show()
'''

CELDA_97 = '''
# Regla 8: las tres lecturas se leen de disco. Las produjo `_nueve.py`, que no busca
# nada: puntua los 50 conjuntos que han producido las once rutas sobre los MISMOS
# partidos del censo, temporada a temporada.
import json

if not (sl.NUEVE_ACERVO.exists() and sl.NUEVE_LECTURA.exists()):
    print(f"sin el acervo del censo de nueve en {sl.NUEVE}")
else:
    ace = pd.read_csv(sl.NUEVE_ACERVO)
    lec = json.loads(sl.NUEVE_LECTURA.read_text(encoding="utf-8"))
    print(f"{lec['n_conjuntos']} conjuntos distintos sobre {lec['n_partidos']:,} "
          "partidos\\n")

    # LECTURA 1, el agregado. El maximo excluye el cero, y es por eso mismo por lo que
    # no es citable: el conjunto se busco sobre partidos que estan dentro del censo.
    tod = ace[ace["temporada"] == "TODAS"].copy()
    liston = tod["roi_ref_b365"].iloc[0]
    print(f"--- lectura 1: el agregado (liston {liston * 100:+.2f}%, "
          f"acierto {tod['acierto_ref'].iloc[0] * 100:.2f}%) ---")
    for cual, col in (("ventaja", "vent_b365"), ("acierto", "acierto")):
        g = tod.nlargest(1, col).iloc[0]
        print(f"   maximo por {cual:<8}: {g['conjunto']:<22} k={int(g['k']):<3} "
              f"acierto {g['acierto'] * 100:.2f}%  RPS {g['rps']:.6f}  "
              f"ventaja {g['vent_b365'] * 100:+.2f} pp "
              f"[{g['vent_b365_inf'] * 100:+.2f},{g['vent_b365_sup'] * 100:+.2f}]")
    for p in ("publicado", "semilla", "todas65", "juego59"):
        r = tod[tod["conjunto"] == p]
        if not r.empty:
            r = r.iloc[0]
            print(f"   {p:<20} k={int(r['k']):<3} acierto {r['acierto'] * 100:.2f}%  "
                  f"RPS {r['rps']:.6f}  ventaja {r['vent_b365'] * 100:+.2f} pp "
                  f"[{r['vent_b365_inf'] * 100:+.2f},{r['vent_b365_sup'] * 100:+.2f}]")
    print("   -> meter el banco entero cuesta acierto y RPS y NO cuesta ventaja: el "
          "ROI plano\\n      sobre 3118 partidos no distingue el 50% del 53.5%")

    # LECTURA 2: por temporada no hay respuesta que dar, y eso es el hallazgo.
    pt2 = lec["por_temporada"]["acierto"]
    print(f"\\n--- lectura 2: por temporada ({pt2['conjuntos_distintos']} conjuntos "
          "distintos en nueve temporadas, ninguno repite) ---")
    for s, d in list(pt2["fuera"].items())[:3]:
        print(f"   {s}: gana '{d['conjunto']}' con {d['en_la_suya'] * 100:.2f}% y "
              f"promedia {d['media_otras'] * 100:.2f}% fuera "
              f"(puesto medio {d['puesto_medio_otras']:.1f} de 50)")
    print("   -> nueve maximos sobre 380 partidos con un error tipico de 5.0 pp son "
          "nueve sorteos")

    # LECTURA 3, la UNICA citable: se elige con ocho temporadas y se puntua en la
    # novena, que no participo en elegir.
    print("\\n--- lectura 3: dejar una fuera (la unica citable) ---")
    for cri in ("acierto", "vent_b365"):
        d = lec["dejar_una_fuera"][cri]
        v = d["agregado"]
        print(f"   guiado por {cri:<10} acierto {v['acierto'] * 100:.2f}% contra "
              f"{v['acierto_ref'] * 100:.2f}% del liston  ·  ventaja "
              f"{v['vent_b365'] * 100:+.2f} pp "
              f"[{v['vent_b365_inf'] * 100:+.2f},{v['vent_b365_sup'] * 100:+.2f}]")
    pub = tod[tod["conjunto"] == "publicado"]["vent_b365"]
    if not pub.empty:
        print(f"   -> las dos quedan por debajo del {pub.iloc[0] * 100:+.2f} pp del "
              "modelo publicado, y ninguna excluye el cero")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.6))

    # Izquierda: la nube de los 50. El eje x es el acierto y el y la ventaja, y lo que
    # se ve es que no hay frontera: la ventaja no ordena por acierto.
    ax1.scatter(tod["acierto"] * 100, tod["vent_b365"] * 100, s=18,
                color=PALETA["apoyo"], label="los 50 conjuntos")
    for p, col in (("publicado", PALETA["modelo"]), ("semilla", PALETA["mercado"])):
        r = tod[tod["conjunto"] == p]
        if not r.empty:
            ax1.scatter(r["acierto"] * 100, r["vent_b365"] * 100, s=90, color=col,
                        zorder=3, label=p)
    ax1.axhline(0, color=PALETA["apoyo"], lw=1.0)
    ax1.set_xlabel("acierto (%)")
    ax1.set_ylabel("ventaja sobre el liston (pp)")
    ax1.set_title("Los 50 conjuntos sobre el censo de nueve")
    ax1.legend(fontsize=8)

    # Derecha: el ganador de cada temporada y su puesto medio en las otras ocho. Una
    # barra alta es un conjunto que gano donde se le midio y es del monton fuera.
    pm = pd.DataFrame([
        {"temporada": s, "puesto": d["puesto_medio_otras"]}
        for s, d in pt2["fuera"].items()])
    ax2.bar(pm["temporada"], pm["puesto"], color=PALETA["modelo"])
    ax2.axhline(25.5, color=PALETA["mercado"], lw=1.4, label="el monton (25.5 de 50)")
    ax2.set_ylabel("puesto medio en las otras ocho")
    ax2.set_title("El ganador de cada temporada, fuera de su temporada")
    ax2.tick_params(axis="x", rotation=60)
    ax2.legend(fontsize=8)

    fig.tight_layout()
    plt.show()
'''

CELDAS = (("C.9.5", CELDA_95), ("C.9.6", CELDA_96), ("C.9.7", CELDA_97))


def main() -> int:
    ambito = {"sl": sl, "pd": pd, "plt": plt, "PALETA": PALETA}
    for nombre, cuerpo in CELDAS:
        print("=" * 72)
        print(f"=== {nombre}")
        print("=" * 72)
        exec(compile(cuerpo, f"<celda {nombre}>", "exec"), ambito)
        plt.close("all")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
