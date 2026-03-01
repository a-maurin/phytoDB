"""
Utilitaires partagés phytoDB.
"""
from __future__ import annotations


def resultat_to_ugl(resultat: float, unite: str | None) -> float | None:
    """
    Convertit un résultat d'analyse en µg/L si l'unité est reconnue.
    Retourne None si la conversion n'est pas possible.
    """
    if resultat is None or unite is None:
        return None
    try:
        val = float(resultat)
    except (TypeError, ValueError):
        return None
    unite_norm = str(unite).replace("µ", "u")
    if unite_norm in ("µg/L", "ug/L"):
        return val
    if unite_norm in ("mg/L",):
        return val * 1000.0
    return None
