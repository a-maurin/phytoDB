"""
Source ADES (qualité des eaux souterraines) via API Hub'Eau.
Données : stations (points d'eau) et analyses de qualité des nappes (dont pesticides).
Réf. : https://ades.eaufrance.fr/ — https://hubeau.eaufrance.fr/page/api-qualite-nappes
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from hubeau import ades_stations, ades_analyses

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def _load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_ades_stations_dep(
    code_departement: str = "21",
    cache_dir: Path | None = None,
    max_pages: int = 100,
) -> list[dict[str, Any]]:
    """
    Récupère les stations (points d'eau) qualité des nappes pour le département.
    Certaines stations peuvent avoir longitude/latitude à null (données anciennes).
    """
    config = _load_config()
    cfg = config.get("hubeau", {})
    page_size = cfg.get("page_size", 1000)
    data = list(ades_stations(
        code_departement,
        page_size=page_size,
        max_pages=max_pages,
    ))
    if cache_dir:
        import json
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "ades_stations_21.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8"
        )
    return data


def fetch_ades_analyses_dep(
    code_departement: str = "21",
    code_parametre: int | str | None = None,
    date_debut: str | None = None,
    date_fin: str | None = None,
    cache_dir: Path | None = None,
    max_pages: int = 20,
) -> list[dict[str, Any]]:
    """
    Récupère les analyses de qualité des nappes (optionnellement filtrées par paramètre).
    Volume très important pour le dép. 21 ; utiliser max_pages pour limiter.
    """
    config = _load_config()
    cfg_hubeau = config.get("hubeau", {})
    cfg_ppp = config.get("ppp", {}).get("ades", {}) or {}

    page_size = cfg_hubeau.get("page_size", 1000)

    # Fenêtre temporelle par défaut issue de la config PPP si non fournie explicitement
    date_debut_eff = date_debut or cfg_ppp.get("date_debut")
    date_fin_eff = date_fin or cfg_ppp.get("date_fin")

    data: list[dict[str, Any]] = []

    # Si un code_parametre est fourni explicitement, on ne considère que celui-là
    if code_parametre is not None:
        data = list(
            ades_analyses(
                code_departement,
                page_size=page_size,
                max_pages=max_pages,
                code_parametre=code_parametre,
                date_debut_prelevement=date_debut_eff,
                date_fin_prelevement=date_fin_eff,
            )
        )
    else:
        # Sinon, on regarde si des codes PPP sont définis dans la config.
        codes_ppp: list[str] = cfg_ppp.get("codes_parametre") or []
        if not codes_ppp:
            # Aucun filtrage PPP configuré : on récupère tout (comportement précédent).
            data = list(
                ades_analyses(
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
                n_codes = len(codes_ppp)
                max_pages_code = max(1, pages_restantes // max(1, n_codes - idx))
                part = list(
                    ades_analyses(
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
        (cache_dir / "ades_analyses_21.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8"
        )
    return data
