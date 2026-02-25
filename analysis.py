"""
Analyse des données C3PO pour le département de la Côte-d'Or (21).
Produit des indicateurs et des jeux de données consultables par l'utilisateur.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from c3po import get_substances_identification

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


def load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


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
