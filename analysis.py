"""
Analyse des données C3PO et ventes pour le département de la Côte-d'Or (21).
Produit des indicateurs et des jeux de données consultables par l'utilisateur.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from collections import defaultdict

import yaml

from c3po import get_substances_identification
from ventes import fetch_ventes_cote_dor_from_api, load_ventes_dep_from_csv

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


def load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _norm_id(s: str) -> str:
    return (s or "").strip().lower()


def build_substance_lookup(substances: list[dict[str, Any]]) -> dict[str, dict]:
    """
    Index par id_bnvd (et variantes) pour joindre avec les ventes.
    Les clés de chaque ligne peuvent avoir des guillemets (ex. "id_bnvd").
    """
    lookup = {}
    for row in substances:
        key = None
        for k in ("id_bnvd", '"id_bnvd"'):
            if k in row and row[k]:
                key = _norm_id(str(row[k]))
                break
        if key:
            lookup[key] = row
    return lookup


def analyze_ventes_with_c3po(
    substances: list[dict[str, Any]],
    ventes: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Croise les ventes (département 21) avec les substances C3PO.
    Les ventes BNV-D peuvent être par produit (identifiant produit) ou par substance (id_bnvd).
    Retourne des agrégats et listes pour consultation.
    """
    lookup = build_substance_lookup(substances)
    # Colonnes possibles dans les ventes : id_bnvd, code_substance, nom_substance, quantité, année, etc.
    # On agrège par substance (id_bnvd ou équivalent) si présent
    by_substance: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "rows": [], "substance_info": {}})
    by_product: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "rows": []})
    unknown_refs = set()

    for v in ventes:
        # Essayer de trouver une clé substance (BNV-D)
        sub_id = None
        for col in ("id_bnvd", "id_substance", "code_substance", '"id_bnvd"'):
            if col in v and v[col]:
                sub_id = _norm_id(str(v[col]))
                break
        if sub_id:
            by_substance[sub_id]["count"] += 1
            by_substance[sub_id]["rows"].append(v)
            if sub_id in lookup:
                by_substance[sub_id]["substance_info"] = lookup[sub_id]
            else:
                unknown_refs.add(sub_id)
        else:
            # Agrégat par produit si pas de substance
            prod_id = v.get("id_produit") or v.get("code_produit") or v.get("amm") or ""
            by_product[prod_id]["count"] += 1
            by_product[prod_id]["rows"].append(v)

    # Résumé
    total_ventes = len(ventes)
    substances_trouvees = sum(1 for s in by_substance.values() if s["substance_info"])
    substances_sans_match = len(unknown_refs)

    return {
        "resume": {
            "nombre_lignes_ventes": total_ventes,
            "substances_distinctes_avec_ventes": len(by_substance),
            "substances_rapprochees_c3po": substances_trouvees,
            "substances_sans_correspondance_c3po": substances_sans_match,
        },
        "par_substance": {
            k: {
                "nombre_ventes": v["count"],
                "info_c3po": v["substance_info"],
            }
            for k, v in by_substance.items()
        },
        "top_substances_ventes": sorted(
            [{"id_bnvd": k, "nombre": v["count"], "info": v["substance_info"]} for k, v in by_substance.items()],
            key=lambda x: -x["nombre"],
        )[:50],
    }


def run_analysis(
    ventes_csv_path: str | Path | None = None,
    use_api_ventes: bool = True,
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
