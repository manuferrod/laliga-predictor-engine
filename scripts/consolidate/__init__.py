"""Consolidacion del MASTER de partido (LaLiga 1X2).

Este paquete reduce cada fuente procesada a la granularidad "1 fila por partido"
(clave canonica `match_key` = "YYYY-MM-DD|home_key|away_key", via teamnames) y las
une sobre un spine unico. Reemplaza la consolidacion que hoy vive dispersa en el
notebook NB1 (EXTRACCION_DATOS) por funciones puras, testeables e idempotentes,
coherentes con el patron de los extractores.

Alcance temporal del master: temporada 2016 (16/17) en adelante, que es cuando
existe toda la familia SofaScore (ratings, fatiga, bajas).

API publica:
    reduce_sofascore(sofa)          -> DataFrame keyed by match_key
    reduce_fatigue(fat, event2key)  -> DataFrame keyed by match_key
    reduce_availability(av, event2key) -> DataFrame keyed by match_key
    event_to_match_key(sofa)        -> Series match_id -> match_key
"""
from .reducers import (  # noqa: F401
    reduce_sofascore,
    reduce_fatigue,
    reduce_availability,
    event_to_match_key,
)
