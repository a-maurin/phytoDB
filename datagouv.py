"""
Récupération des métadonnées et des RIDs des jeux de données data.gouv.fr.
Utilisé pour découvrir les ressources (CSV) éligibles à l'API tabulaire.
"""
from __future__ import annotations

from typing import Any

import requests

DATAGOUV_API_BASE = "https://www.data.gouv.fr/api/1"


def get_dataset(dataset_id: str, session: requests.Session | None = None) -> dict[str, Any]:
    """Retourne les métadonnées d'un jeu de données (dont la liste des ressources)."""
    s = session or requests.Session()
    r = s.get(f"{DATAGOUV_API_BASE}/datasets/{dataset_id}/", timeout=30)
    r.raise_for_status()
    return r.json()


def get_resources_for_tabular(dataset_id: str) -> list[dict[str, Any]]:
    """
    Liste les ressources du jeu de données susceptibles d'être sur l'API tabulaire :
    format csv/xlsx, taille dans les limites (csv < 100 Mo, xlsx < 12.5 Mo).
    """
    ds = get_dataset(dataset_id)
    resources = ds.get("resources") or []
    out = []
    for res in resources:
        fmt = (res.get("format") or "").lower()
        if fmt not in ("csv", "xlsx", "xls", "parquet"):
            continue
        size = res.get("filesize") or 0
        if fmt == "csv" and size > 100 * 1024 * 1024:
            continue
        if fmt == "xlsx" and size > 12.5 * 1024 * 1024:
            continue
        out.append(
            {
                "id": res.get("id"),
                "title": res.get("title"),
                "format": fmt,
                "filesize": size,
            }
        )
    return out
