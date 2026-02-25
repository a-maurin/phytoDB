# Sources de données PPP et qualité de l'eau

from .naiades import fetch_naiades_stations_dep, fetch_naiades_analyses_dep
from .ades import fetch_ades_stations_dep, fetch_ades_analyses_dep

__all__ = [
    "fetch_naiades_stations_dep",
    "fetch_naiades_analyses_dep",
    "fetch_ades_stations_dep",
    "fetch_ades_analyses_dep",
]
