"""
Analyse des données C3PO pour le département de la Côte-d'Or (21).
Produit des indicateurs et des jeux de données consultables par l'utilisateur.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from c3po import get_substances_identification
from config import load_config, get_code_departement, resolve_path, cache_path


def _seuil_10_ans() -> str:
    """Date limite (il y a 10 ans) au format ISO pour filtrer les 10 dernières années."""
    from datetime import date, timedelta
    return (date.today() - timedelta(days=365 * 10)).isoformat()[:10]


def _annee_from_date(date_val: Any) -> str | None:
    """Extrait l'année (AAAA) depuis une date ISO ou partielle."""
    if date_val is None:
        return None
    s = str(date_val).strip()
    if len(s) >= 4 and s[:4].isdigit():
        return s[:4]
    return None


def stats_prelevements_par_annee(
    cache_dir: str | Path | None = None,
    sig_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Compte le nombre d'analyses (prélèvements) par année pour la Côte-d'Or.

    Données utilisées (par ordre de priorité) :
    1. Cache local : data/cache/naiades_analyses_21.json et ades_analyses_21.json
    2. Sinon couche SIG exportée (GeoJSON) si sig_path fourni ou défaut data/sig/analyse_stations_ppp_cote_dor.geojson

    Retourne un dict avec :
      - "par_annee" : { "2008": n, "2009": n, ... } (toutes sources)
      - "par_annee_naïades" : idem pour Naïades uniquement
      - "par_annee_ades" : idem pour ADES uniquement
      - "total" : total analyses
      - "source" : "cache" | "geojson"
    """
    root = Path(__file__).resolve().parent
    cache_dir = Path(cache_dir) if cache_dir else resolve_path("data/cache")
    sig_path = Path(sig_path) if sig_path else resolve_path("data/sig/analyse_stations_ppp_cote_dor.geojson")
    code_dep = get_code_departement()

    par_annee: dict[str, int] = defaultdict(int)
    par_annee_naiades: dict[str, int] = defaultdict(int)
    par_annee_ades: dict[str, int] = defaultdict(int)

    # 1) Essayer le cache (données brutes API)
    naiades_path = cache_path(cache_dir, "naiades_analyses", code_dep)
    ades_path = cache_path(cache_dir, "ades_analyses", code_dep)
    if naiades_path.exists():
        try:
            seuil = _seuil_10_ans()
            data_n = json.loads(naiades_path.read_text(encoding="utf-8"))
            for rec in data_n:
                dt = rec.get("date_prelevement") or ""
                if str(dt)[:10] < seuil:
                    continue
                annee = _annee_from_date(dt)
                if annee:
                    par_annee_naiades[annee] += 1
                    par_annee[annee] += 1
        except Exception:
            pass
    if ades_path.exists():
        try:
            seuil = _seuil_10_ans()
            data_a = json.loads(ades_path.read_text(encoding="utf-8"))
            for rec in data_a:
                dt = rec.get("date_debut_prelevement") or ""
                if str(dt)[:10] < seuil:
                    continue
                annee = _annee_from_date(dt)
                if annee:
                    par_annee_ades[annee] += 1
                    par_annee[annee] += 1
        except Exception:
            pass

    if par_annee:
        result: dict[str, Any] = {
            "par_annee": dict(par_annee),
            "par_annee_naïades": dict(par_annee_naiades),
            "par_annee_ades": dict(par_annee_ades),
            "total": sum(par_annee.values()),
            "source": "cache",
        }
        # Enrichir avec la ventilation par usage si le GeoJSON existe (pour cohérence affichage)
        if sig_path.exists():
            try:
                fc = json.loads(sig_path.read_text(encoding="utf-8"))
                par_annee_usage: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
                for f in fc.get("features") or []:
                    props = f.get("properties") or {}
                    annee = props.get("annee")
                    if annee:
                        annee = str(annee).strip()[:4]
                        usage = props.get("ppp_usage") or "non_renseigné"
                        par_annee_usage[annee][str(usage)] += 1
                result["par_annee_usage"] = {a: dict(u) for a, u in par_annee_usage.items()}
            except Exception:
                pass
        return result

    # 2) Sinon : couche SIG exportée (une feature = une analyse, Côte-d'Or déjà filtrée)
    if sig_path.exists():
        try:
            seuil = _seuil_10_ans()[:4]  # année uniquement
            fc = json.loads(sig_path.read_text(encoding="utf-8"))
            par_annee_usage: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for f in fc.get("features") or []:
                props = f.get("properties") or {}
                annee = props.get("annee")
                if annee and str(annee)[:4] < seuil:
                    continue
                if annee:
                    annee = str(annee).strip()
                    if len(annee) >= 4:
                        annee = annee[:4]
                    par_annee[annee] += 1
                    src = props.get("source") or ""
                    if "Naïades" in src or "Naiades" in src:
                        par_annee_naiades[annee] += 1
                    elif "ADES" in src:
                        par_annee_ades[annee] += 1
                    # Ventilation par usage (famille) lorsque disponible
                    usage = props.get("ppp_usage") or "non_renseigné"
                    par_annee_usage[annee][str(usage)] += 1
            if par_annee:
                result: dict[str, Any] = {
                    "par_annee": dict(par_annee),
                    "par_annee_naïades": dict(par_annee_naiades),
                    "par_annee_ades": dict(par_annee_ades),
                    "total": sum(par_annee.values()),
                    "source": "geojson",
                }
                result["par_annee_usage"] = {a: dict(u) for a, u in par_annee_usage.items()}
                return result
        except Exception:
            pass

    return {
        "par_annee": {},
        "par_annee_naïades": {},
        "par_annee_ades": {},
        "total": 0,
        "source": "aucune",
    }


def run_analysis(
    out_dir: str | Path = "data/out",
) -> dict[str, Any]:
    """
    Analyse centrée sur C3PO (sans utiliser les données de ventes).
    Les sorties sont écrites dans out_dir.
    """
    config = load_config()
    code_dep = config.get("departement", {}).get("code", "21")
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Charger C3PO
    substances = get_substances_identification()
    result: dict[str, Any] = {
        "departement": {"code": code_dep, "nom": config.get("departement", {}).get("nom", "Côte-d'Or")},
        "c3po": {"nombre_substances": len(substances)},
        "analyse": {
            "resume": {
                "message": "Analyse centrée sur C3PO (données de ventes BNV-D non utilisées).",
                "nombre_substances_c3po": len(substances),
            }
        },
    }

    # Exporter un aperçu des substances C3PO disponibles pour le dép. 21
    with open(out_dir / "substances_c3po_disponibles.json", "w", encoding="utf-8") as f:
        json.dump({"nombre": len(substances), "apercu": substances[:1000]}, f, ensure_ascii=False, indent=2)

    return result
