"""
Source Naïades (qualité des eaux de surface) via API Hub'Eau.
Données : stations de mesure et analyses physico-chimiques (dont pesticides).
Réf. : https://naiades.eaufrance.fr/ — https://hubeau.eaufrance.fr/page/api-qualite-cours-deau
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from hubeau import naiades_stations, naiades_analyses
from ref_params import load_pesticide_codes

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def _load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


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
        (cache_dir / "naiades_stations_21.json").write_text(
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

    # Fenêtre temporelle par défaut issue de la config PPP si non fournie explicitement
    date_debut_eff = date_debut or cfg_ppp.get("date_debut")
    date_fin_eff = date_fin or cfg_ppp.get("date_fin")

    data: list[dict[str, Any]] = []

    # Si un code_parametre est fourni explicitement, on ne considère que celui-là
    if code_parametre:
        data = list(
            naiades_analyses(
                code_departement,
                page_size=page_size,
                max_pages=max_pages,
                code_parametre=code_parametre,
                date_debut_prelevement=date_debut_eff,
                date_fin_prelevement=date_fin_eff,
            )
        )
    else:
        # Sinon, on regarde si des codes PPP sont définis dans la config
        # ou, à défaut, dans le référentiel local construit (C3PO / Sandre).
        codes_ppp: list[str] = cfg_ppp.get("codes_parametre") or []
        if not codes_ppp:
            codes_ppp = sorted(load_pesticide_codes())
        if not codes_ppp:
            # Aucun filtrage PPP disponible : on récupère tout (comportement précédent).
            data = list(
                naiades_analyses(
                    code_departement,
                    page_size=page_size,
                    max_pages=max_pages,
                    code_parametre=None,
                    date_debut_prelevement=date_debut_eff,
                    date_fin_prelevement=date_fin_eff,
                )
            )
        else:
            # On boucle sur la liste des codes PPP, en répartissant grossièrement max_pages.
            pages_restantes = max_pages
            for idx, code_ppp in enumerate(codes_ppp):
                if pages_restantes <= 0:
                    break
                # Répartition naïve : on consomme au plus max_pages / n_codes restants par paramètre.
                n_codes = len(codes_ppp)
                max_pages_code = max(1, pages_restantes // max(1, n_codes - idx))
                part = list(
                    naiades_analyses(
                        code_departement,
                        page_size=page_size,
                        max_pages=max_pages_code,
                        code_parametre=code_ppp,
                        date_debut_prelevement=date_debut_eff,
                        date_fin_prelevement=date_fin_eff,
                    )
                )
                data.extend(part)
                pages_restantes -= max_pages_code

    if cache_dir:
        import json
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "naiades_analyses_21.json").write_text(
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
        (cache_dir / "naiades_stations_21.json").write_text(
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
    date_debut_eff = date_debut or cfg_ppp.get("date_debut")
    date_fin_eff = date_fin or cfg_ppp.get("date_fin")
    data: list[dict[str, Any]] = []
    if code_parametre:
        data = await naiades_analyses_async(
            code_departement, page_size=page_size, max_pages=max_pages,
            code_parametre=code_parametre,
            date_debut_prelevement=date_debut_eff, date_fin_prelevement=date_fin_eff,
        )
    else:
        codes_ppp: list[str] = cfg_ppp.get("codes_parametre") or []
        if not codes_ppp:
            codes_ppp = sorted(load_pesticide_codes())
        if not codes_ppp:
            data = await naiades_analyses_async(
                code_departement, page_size=page_size, max_pages=max_pages,
                date_debut_prelevement=date_debut_eff, date_fin_prelevement=date_fin_eff,
            )
        else:
            pages_restantes = max_pages
            for idx, code_ppp in enumerate(codes_ppp):
                if pages_restantes <= 0:
                    break
                n_codes = len(codes_ppp)
                max_pages_code = max(1, pages_restantes // max(1, n_codes - idx))
                part = await naiades_analyses_async(
                    code_departement, page_size=page_size, max_pages=max_pages_code,
                    code_parametre=code_ppp,
                    date_debut_prelevement=date_debut_eff, date_fin_prelevement=date_fin_eff,
                )
                data.extend(part)
                pages_restantes -= max_pages_code
    if cache_dir:
        import json
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "naiades_analyses_21.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8"
        )
    return data
