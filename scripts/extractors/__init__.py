"""
Paquete de extractores modulares para laliga-predictor-engine.

Cada fuente de datos (FBref, Understat, ClubElo, football-data, ...) vive en su
propio modulo, con una capa de normalizacion de nombres de equipo y clave de
partido compartida en `teamnames.py`.

Los extractores que hacen peticiones en vivo requieren una maquina con acceso
abierto a internet; su logica de parseo y agregacion, en cambio, es pura y se
valida sin red mediante las baterias `test_*.py` de este mismo paquete.
"""
