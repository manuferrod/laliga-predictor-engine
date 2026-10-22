"""Ingeniería de variables (Fase A) del predictor 1X2 de LaLiga.

Convierte el MASTER de partido (`master_matches.parquet`, 1 fila/partido, mezcla
de columnas pre-partido y post-partido) en un set de features estrictamente
PRE-PARTIDO y sin fuga de información (`features_matches.parquet`).

Dos principios:
  1. Solo entra al modelo lo que se conoce antes del pitido inicial. Las
     estadísticas del propio partido (goles, tiros, xG, rating...) se usan
     únicamente como materia prima para construir "forma" de partidos previos,
     siempre con shift().
  2. La temporada más reciente queda SELLADA como holdout ciego: no se mira para
     elegir features ni hiperparámetros, de modo que la métrica final no incorpore
     fuga de selección.

API pública:
    build_features(master=None) -> DataFrame keyed by match_key
    build_form_features(master) -> features de forma leak-safe
    to_team_long(master)        -> reshape a team-match long
    run_qa(feat, master)        -> invariantes anti-fuga (qa_features)
"""
from .build_features import build_features  # noqa: F401
from .form import build_form_features, form_feature_names  # noqa: F401
from .teamlong import to_team_long  # noqa: F401
