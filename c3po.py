"""
Récupération des données C3PO via l'API tabulaire data.gouv.fr.
Les RIDs des ressources sont configurés dans config.yaml (datasets.c3po.resource_ids).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from api_tabulaire import get_profile, fetch_full_resource

from config import load_config


def get_c3po_resource_ids() -> list[str]:
    config = load_config()
    return list(config.get("datasets", {}).get("c3po", {}).get("resource_ids") or [])


def fetch_substances_identification(rid: str, cache_dir: Path | None = None) -> list[dict[str, Any]]:
    """
    Charge la ressource « substances_identification » (identifiants BNV-D, E-phy, Agritox, etc.).
    Si cache_dir est fourni et qu'un fichier cache existe, on le charge en priorité.
    """
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"c3po_substances_identification_{rid[:8]}.json"
        if cache_file.exists():
            import json
            with open(cache_file, encoding="utf-8") as f:
                return json.load(f)
    data = fetch_full_resource(rid)
    if cache_dir:
        import json
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=0)
    return data


def fetch_all_c3po(cache: bool = True) -> dict[str, list[dict[str, Any]]]:
    """
    Récupère toutes les ressources C3PO configurées.
    Retourne un dictionnaire { "rid_short": [lignes] }.
    """
    config = load_config()
    rids = get_c3po_resource_ids()
    cache_dir = None
    if config.get("cache", {}).get("enabled") and cache:
        cache_dir = Path(config.get("cache", {}).get("dir", "data/cache"))
    out = {}
    for rid in rids:
        short = rid[:8] if len(rid) >= 8 else rid
        try:
            rows = fetch_substances_identification(rid, cache_dir=cache_dir)
            out[short] = rows
        except Exception as e:
            out[short] = []  # éviter de faire échouer tout le run
            raise RuntimeError(f"Erreur chargement C3PO resource {rid}: {e}") from e
    return out


def get_substances_identification() -> list[dict[str, Any]]:
    """
    Retourne la première ressource C3PO configurée (substances_identification).
    Pratique pour l'analyse.
    """
    rids = get_c3po_resource_ids()
    if not rids:
        return []
    config = load_config()
    cache_dir = Path(config.get("cache", {}).get("dir", "data/cache")) if config.get("cache", {}).get("enabled") else None
    return fetch_substances_identification(rids[0], cache_dir=cache_dir)


async def fetch_substances_identification_async(
    rid: str, cache_dir: Path | None = None
) -> list[dict[str, Any]]:
    """Charge la ressource substances_identification (async). Utilise le cache si présent."""
    if cache_dir:
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"c3po_substances_identification_{rid[:8]}.json"
        if cache_file.exists():
            import json
            with open(cache_file, encoding="utf-8") as f:
                return json.load(f)
    from api_tabulaire import fetch_full_resource_async
    data = await fetch_full_resource_async(rid)
    if cache_dir:
        import json
        cache_file = cache_dir / f"c3po_substances_identification_{rid[:8]}.json"
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=0)
    return data


async def fetch_all_c3po_async(cache: bool = True) -> dict[str, list[dict[str, Any]]]:
    """Récupère toutes les ressources C3PO en parallèle (async)."""
    import asyncio
    config = load_config()
    rids = get_c3po_resource_ids()
    if not rids:
        return {}
    cache_dir = None
    if config.get("cache", {}).get("enabled") and cache:
        cache_dir = Path(config.get("cache", {}).get("dir", "data/cache"))

    async def fetch_one(rid: str) -> tuple[str, list[dict[str, Any]]]:
        short = rid[:8] if len(rid) >= 8 else rid
        try:
            rows = await fetch_substances_identification_async(rid, cache_dir=cache_dir)
            return (short, rows)
        except Exception as e:
            raise RuntimeError(f"Erreur chargement C3PO resource {rid}: {e}") from e

    results = await asyncio.gather(*[fetch_one(rid) for rid in rids])
    return dict(results)
