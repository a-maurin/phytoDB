"""
Client pour les API Hub'Eau (Eaufrance).
- Qualité des cours d'eau (Naïades) : https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
- Qualité des nappes (ADES) : https://hubeau.eaufrance.fr/page/api-qualite-nappes
"""
from __future__ import annotations

from typing import Any, Iterator
import requests

HUBEAU_BASE = "https://hubeau.eaufrance.fr/api"


def _url(api: str, version: str, path: str, endpoint: str) -> str:
    return f"{HUBEAU_BASE}/{version}/{path}/{endpoint}"


def _iter_pages(
    url: str,
    params: dict[str, Any],
    page_size: int = 1000,
    max_pages: int | None = 50,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    s = session or requests.Session()
    params = dict(params)
    params["size"] = page_size
    page = 1
    while True:
        params["page"] = page
        r = s.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        for item in data.get("data") or []:
            yield item
        if not data.get("data"):
            break
        next_url = data.get("next")
        if not next_url:
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break


# ---------- Naïades (qualité cours d'eau) ----------


def naiades_stations(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """Stations de mesure physico-chimiques (cours d'eau / plans d'eau) pour un département."""
    url = _url("hubeau", "v2", "qualite_rivieres", "station_pc")
    return _iter_pages(
        url,
        {"code_departement": code_departement},
        page_size=page_size,
        max_pages=max_pages,
        session=session,
    )


def naiades_analyses(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
    code_parametre: str | None = None,
    date_debut_prelevement: str | None = None,
    date_fin_prelevement: str | None = None,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Analyses physico-chimiques (dont pesticides) par département.
    code_parametre : code Sandre du paramètre (ex. groupe pesticides) pour filtrer.
    """
    params: dict[str, Any] = {"code_departement": code_departement}
    if code_parametre:
        params["code_parametre"] = code_parametre
    if date_debut_prelevement:
        params["date_debut_prelevement"] = date_debut_prelevement
    if date_fin_prelevement:
        params["date_fin_prelevement"] = date_fin_prelevement
    url = _url("hubeau", "v2", "qualite_rivieres", "analyse_pc")
    return _iter_pages(url, params, page_size=page_size, max_pages=max_pages, session=session)


# ---------- ADES (qualité nappes) ----------


def ades_stations(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """Stations de mesure de la qualité des eaux souterraines (points d'eau) pour un département."""
    url = _url("hubeau", "v1", "qualite_nappes", "stations")
    return _iter_pages(
        url,
        {"code_departement": code_departement},
        page_size=page_size,
        max_pages=max_pages,
        session=session,
    )


def ades_analyses(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
    code_parametre: int | str | None = None,
    date_debut_prelevement: str | None = None,
    date_fin_prelevement: str | None = None,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Analyses de qualité des nappes (dont pesticides) par département.
    code_parametre : code Sandre du paramètre pour filtrer (ex. pesticides).
    """
    params: dict[str, Any] = {"code_departement": code_departement}
    if code_parametre is not None:
        params["code_parametre"] = str(code_parametre)
    if date_debut_prelevement:
        params["date_debut_prelevement"] = date_debut_prelevement
    if date_fin_prelevement:
        params["date_fin_prelevement"] = date_fin_prelevement
    url = _url("hubeau", "v1", "qualite_nappes", "analyses")
    return _iter_pages(url, params, page_size=page_size, max_pages=max_pages, session=session)


# ---------- Versions asynchrones (httpx) ----------


async def _fetch_all_pages_async(
    url: str,
    params: dict[str, Any],
    *,
    page_size: int = 1000,
    max_pages: int | None = 50,
) -> list[dict[str, Any]]:
    """Charge toutes les pages d'un endpoint Hub'Eau en async ; retourne la liste des items."""
    import httpx
    out: list[dict[str, Any]] = []
    params = dict(params)
    params["size"] = page_size
    page = 1
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            params["page"] = page
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            items = data.get("data") or []
            out.extend(items)
            if not items or not data.get("next"):
                break
            page += 1
            if max_pages is not None and page > max_pages:
                break
    return out


async def naiades_stations_async(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
) -> list[dict[str, Any]]:
    """Stations Naïades (async)."""
    url = _url("hubeau", "v2", "qualite_rivieres", "station_pc")
    return await _fetch_all_pages_async(
        url, {"code_departement": code_departement},
        page_size=page_size, max_pages=max_pages,
    )


async def naiades_analyses_async(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
    code_parametre: str | None = None,
    date_debut_prelevement: str | None = None,
    date_fin_prelevement: str | None = None,
) -> list[dict[str, Any]]:
    """Analyses Naïades (async)."""
    params: dict[str, Any] = {"code_departement": code_departement}
    if code_parametre:
        params["code_parametre"] = code_parametre
    if date_debut_prelevement:
        params["date_debut_prelevement"] = date_debut_prelevement
    if date_fin_prelevement:
        params["date_fin_prelevement"] = date_fin_prelevement
    url = _url("hubeau", "v2", "qualite_rivieres", "analyse_pc")
    return await _fetch_all_pages_async(url, params, page_size=page_size, max_pages=max_pages)


async def ades_stations_async(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
) -> list[dict[str, Any]]:
    """Stations ADES (async)."""
    url = _url("hubeau", "v1", "qualite_nappes", "stations")
    return await _fetch_all_pages_async(
        url, {"code_departement": code_departement},
        page_size=page_size, max_pages=max_pages,
    )


async def ades_analyses_async(
    code_departement: str,
    page_size: int = 1000,
    max_pages: int | None = 50,
    code_parametre: int | str | None = None,
    date_debut_prelevement: str | None = None,
    date_fin_prelevement: str | None = None,
) -> list[dict[str, Any]]:
    """Analyses ADES (async)."""
    params: dict[str, Any] = {"code_departement": code_departement}
    if code_parametre is not None:
        params["code_parametre"] = str(code_parametre)
    if date_debut_prelevement:
        params["date_debut_prelevement"] = date_debut_prelevement
    if date_fin_prelevement:
        params["date_fin_prelevement"] = date_fin_prelevement
    url = _url("hubeau", "v1", "qualite_nappes", "analyses")
    return await _fetch_all_pages_async(url, params, page_size=page_size, max_pages=max_pages)
