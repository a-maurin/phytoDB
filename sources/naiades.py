"""
Source Naïades (qualité des eaux de surface) via API Hub'Eau.
Données : stations de mesure et analyses physico-chimiques (dont pesticides).
Réf. : https://naiades.eaufrance.fr/ — https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

from hubeau import naiades_stations, naiades_analyses
from ref_params import load_pesticide_codes

from config import load_config as _load_config, cache_path


def fetch_naiades_stations_dep(
    code_departement: str = "21",
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """
    Récupère les stations de mesure qualité (cours d'eau / plans d'eau) pour le département.
    Chaque station peut contenir une clé 'geometry' (Point GeoJSON).
    """
    config = _load_config()
    cfg = config.get("hubeau", {})
    page_size = cfg.get("page_size", 1000)
    max_pages = cfg.get("max_pages", 50)
    data = list(naiades_stations(code_departement, page_size=page_size, max_pages=max_pages))
    if cache_dir:
        import json
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path(cache_dir, "naiades_stations", code_departement).write_text(
            json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8"
        )
    return data


def fetch_naiades_analyses_dep(
    code_departement: str = "21",
    code_parametre: str | None = None,
    date_debut: str | None = None,
    date_fin: str | None = None,
    cache_dir: Path | None = None,
    max_pages: int = 20,
) -> list[dict[str, Any]]:
    """
    Récupère les analyses physico-chimiques (optionnellement filtrées par paramètre, ex. pesticides).
    Chaque analyse peut contenir 'geometry' (point de prélèvement).
    Limiter max_pages pour éviter des volumes trop importants (5 M+ analyses pour le dép. 21).
    """
    config = _load_config()
    cfg_hubeau = config.get("hubeau", {})
    cfg_ppp = config.get("ppp", {}).get("naiades", {}) or {}

    page_size = cfg_hubeau.get("page_size", 1000)

    # Fenêtre temporelle : 10 dernières années (date_fin = jour courant, date_debut = il y a 10 ans)
    today = date.today()
    _debut_default = (today - timedelta(days=365 * 10)).isoformat()
    date_debut_eff = date_debut or cfg_ppp.get("date_debut") or _debut_default
    date_fin_eff = date_fin or cfg_ppp.get("date_fin") or today.isoformat()

    # Double fetch pour chevauchement NQE (2019+) : avant 2019 + période NQE 2019-today
    nqe_debut = cfg_ppp.get("date_debut_nqe") or "2019-01-01"
    today_str = today.isoformat()
    split_nqe = (
        nqe_debut <= today_str
        and (date_debut_eff < nqe_debut or date_fin_eff >= nqe_debut)
    )

    data: list[dict[str, Any]] = []

    def _fetch(d_debut: str, d_fin: str, pages: int) -> list[dict[str, Any]]:
        if pages <= 0:
            return []
        if code_parametre:
            return list(
                naiades_analyses(
                    code_departement,
                    page_size=page_size,
                    max_pages=pages,
                    code_parametre=code_parametre,
                    date_debut_prelevement=d_debut,
                    date_fin_prelevement=d_fin,
                )
            )
        codes_ppp: list[str] = cfg_ppp.get("codes_parametre") or []
        if not codes_ppp:
            codes_ppp = sorted(load_pesticide_codes())
        if not codes_ppp:
            return list(
                naiades_analyses(
                    code_departement,
                    page_size=page_size,
                    max_pages=pages,
                    code_parametre=None,
                    date_debut_prelevement=d_debut,
                    date_fin_prelevement=d_fin,
                )
            )
        out: list[dict[str, Any]] = []
        pages_restantes = pages
        for idx, code_ppp in enumerate(codes_ppp):
            if pages_restantes <= 0:
                break
            n_codes = len(codes_ppp)
            max_pages_code = max(1, pages_restantes // max(1, n_codes - idx))
            part = list(
                naiades_analyses(
                    code_departement,
                    page_size=page_size,
                    max_pages=max_pages_code,
                    code_parametre=code_ppp,
                    date_debut_prelevement=d_debut,
                    date_fin_prelevement=d_fin,
                )
            )
            out.extend(part)
            pages_restantes -= max_pages_code
        return out

    if split_nqe and date_debut_eff < nqe_debut:
        # Priorité période récente (NQE 2019+) : 1/3 avant 2019, 2/3 période NQE
        pages_hist = max(1, max_pages // 3)
        pages_nqe = max(1, max_pages - pages_hist)
        fin_hist = "2018-12-31"
        data = _fetch(date_debut_eff, fin_hist, pages_hist)
        data.extend(_fetch(nqe_debut, date_fin_eff, pages_nqe))
    else:
        data = _fetch(date_debut_eff, date_fin_eff, max_pages)

    if cache_dir:
        import json
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path(cache_dir, "naiades_analyses", code_departement).write_text(
            json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8"
        )
    return data


async def fetch_naiades_stations_dep_async(
    code_departement: str = "21",
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Récupère les stations Naïades (async). Écrit le cache si cache_dir fourni."""
    from hubeau import naiades_stations_async
    config = _load_config()
    cfg = config.get("hubeau", {})
    page_size = cfg.get("page_size", 1000)
    max_pages = cfg.get("max_pages", 50)
    data = await naiades_stations_async(code_departement, page_size=page_size, max_pages=max_pages)
    if cache_dir:
        import json
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path(cache_dir, "naiades_stations", code_departement).write_text(
            json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8"
        )
    return data


async def fetch_naiades_analyses_dep_async(
    code_departement: str = "21",
    code_parametre: str | None = None,
    date_debut: str | None = None,
    date_fin: str | None = None,
    cache_dir: Path | None = None,
    max_pages: int = 20,
) -> list[dict[str, Any]]:
    """Récupère les analyses Naïades (async). Même logique que fetch_naiades_analyses_dep."""
    from hubeau import naiades_analyses_async
    config = _load_config()
    cfg_hubeau = config.get("hubeau", {})
    cfg_ppp = config.get("ppp", {}).get("naiades", {}) or {}
    page_size = cfg_hubeau.get("page_size", 1000)
    today = date.today()
    _debut_default = (today - timedelta(days=365 * 10)).isoformat()
    date_debut_eff = date_debut or cfg_ppp.get("date_debut") or _debut_default
    date_fin_eff = date_fin or cfg_ppp.get("date_fin") or today.isoformat()
    nqe_debut = cfg_ppp.get("date_debut_nqe") or "2019-01-01"
    split_nqe = nqe_debut <= date_fin_eff and date_debut_eff < nqe_debut

    async def _fetch_async(d_debut: str, d_fin: str, pages: int) -> list[dict[str, Any]]:
        if pages <= 0:
            return []
        if code_parametre:
            return await naiades_analyses_async(
                code_departement, page_size=page_size, max_pages=pages,
                code_parametre=code_parametre,
                date_debut_prelevement=d_debut, date_fin_prelevement=d_fin,
            )
        codes_ppp: list[str] = cfg_ppp.get("codes_parametre") or []
        if not codes_ppp:
            codes_ppp = sorted(load_pesticide_codes())
        if not codes_ppp:
            return await naiades_analyses_async(
                code_departement, page_size=page_size, max_pages=pages,
                date_debut_prelevement=d_debut, date_fin_prelevement=d_fin,
            )
        out: list[dict[str, Any]] = []
        pages_restantes = pages
        for idx, code_ppp in enumerate(codes_ppp):
            if pages_restantes <= 0:
                break
            n_codes = len(codes_ppp)
            max_pages_code = max(1, pages_restantes // max(1, n_codes - idx))
            part = await naiades_analyses_async(
                code_departement, page_size=page_size, max_pages=max_pages_code,
                code_parametre=code_ppp,
                date_debut_prelevement=d_debut, date_fin_prelevement=d_fin,
            )
            out.extend(part)
            pages_restantes -= max_pages_code
        return out

    if split_nqe and date_debut_eff < nqe_debut:
        pages_hist = max(1, max_pages // 3)
        pages_nqe = max(1, max_pages - pages_hist)
        data = await _fetch_async(date_debut_eff, "2018-12-31", pages_hist)
        data.extend(await _fetch_async(nqe_debut, date_fin_eff, pages_nqe))
    else:
        data = await _fetch_async(date_debut_eff, date_fin_eff, max_pages)
    if cache_dir:
        import json
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path(cache_dir, "naiades_analyses", code_departement).write_text(
            json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8"
        )
    return data
