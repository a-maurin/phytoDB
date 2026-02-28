"""
Client pour l'API tabulaire de data.gouv.fr.
Documentation : https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-fr-beta
"""
from __future__ import annotations

import time
from typing import Any, Iterator

import requests


TABULAR_API_BASE = "https://tabular-api.data.gouv.fr/api"
DEFAULT_PAGE_SIZE = 100
RATE_LIMIT_PER_SECOND = 100  # limite documentée


def _url(rid: str, path: str = "") -> str:
    p = path.rstrip("/")
    return f"{TABULAR_API_BASE}/resources/{rid}/{p}/" if p else f"{TABULAR_API_BASE}/resources/{rid}/"


def get_resource_meta(rid: str, session: requests.Session | None = None) -> dict[str, Any]:
    """Retourne les métadonnées d'une ressource (liens profile, data, swagger)."""
    s = session or requests.Session()
    r = s.get(_url(rid), timeout=30)
    r.raise_for_status()
    return r.json()


def get_profile(rid: str, session: requests.Session | None = None) -> dict[str, Any]:
    """Retourne le profil d'une ressource (en-têtes, types, stats)."""
    s = session or requests.Session()
    url = _url(rid, "profile")
    r = s.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_data(
    rid: str,
    *,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    filters: dict[str, str] | None = None,
    sort: dict[str, str] | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """
    Récupère une page de données.
    filters : { "nom_colonne__exact": "valeur", "nom_colonne__contains": "x" }
    sort : { "nom_colonne": "asc" | "desc" }
    """
    s = session or requests.Session()
    params: dict[str, Any] = {"page": page, "page_size": page_size}
    if filters:
        params.update(filters)
    if sort:
        for col, order in sort.items():
            params[f"{col}__sort"] = order
    url = _url(rid, "data")
    r = s.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def iter_data(
    rid: str,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    filters: dict[str, str] | None = None,
    sort: dict[str, str] | None = None,
    session: requests.Session | None = None,
    max_pages: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Itère sur toutes les lignes de la ressource (pagination automatique)."""
    s = session or requests.Session()
    page = 1
    total_seen = 0
    while True:
        data = get_data(
            rid,
            page=page,
            page_size=page_size,
            filters=filters,
            sort=sort,
            session=s,
        )
        rows = data.get("data") or []
        for row in rows:
            yield row
        total_seen += len(rows)
        meta = data.get("meta") or {}
        total = meta.get("total", 0)
        if total_seen >= total or not rows:
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break
        time.sleep(1.0 / RATE_LIMIT_PER_SECOND)


def fetch_full_resource(
    rid: str,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    filters: dict[str, str] | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Charge l'intégralité d'une ressource en mémoire."""
    return list(iter_data(rid, page_size=page_size, filters=filters, session=session))


async def fetch_full_resource_async(
    rid: str,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    filters: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Charge l'intégralité d'une ressource en mémoire (async, httpx)."""
    import asyncio
    import httpx
    url_base = _url(rid, "data")
    out: list[dict[str, Any]] = []
    page = 1
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            params: dict[str, Any] = {"page": page, "page_size": page_size}
            if filters:
                params.update(filters)
            r = await client.get(url_base, params=params)
            r.raise_for_status()
            data = r.json()
            rows = data.get("data") or []
            out.extend(rows)
            meta = data.get("meta") or {}
            total = meta.get("total", 0)
            if len(out) >= total or not rows:
                break
            page += 1
            await asyncio.sleep(1.0 / RATE_LIMIT_PER_SECOND)
    return out
