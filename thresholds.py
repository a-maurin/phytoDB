"""
Seuils réglementaires / sanitaires pour les PPP.

Objectif :
- fournir un seuil (en µg/L) par paramètre Sandre (code_parametre) quand on le connaît ;
- sinon, appliquer un seuil sanitaire générique (directive eau potable) : 0,1 µg/L par pesticide.
"""
from __future__ import annotations

from typing import Any
from pathlib import Path
import csv
import yaml


# Seuil sanitaire générique (directive eau potable UE 2020/2184) pour un pesticide individuel : 0,1 µg/L
# (0,5 µg/L pour la somme des pesticides, non géré ici car on travaille substance par substance)
DEFAULT_SANITAIRE_PPP_UGL = 0.1

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


def _load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _default_thresholds_file() -> Path:
    """
    Fichier CSV optionnel permettant de définir des seuils spécifiques par code_parametre.
    Format minimal : colonnes 'code_parametre' et 'seuil_ugl' (en µg/L).

    Exemple d'entrée dans config.yaml :

    ref:
      thresholds_pesticides:
        file: data/ref/seuils_pesticides.csv
    """
    cfg = _load_config()
    ref_cfg = cfg.get("ref", {}).get("thresholds_pesticides", {})
    path = ref_cfg.get("file", "data/ref/seuils_pesticides.csv")
    return Path(path)


_THRESHOLDS_CACHE: dict[str, float] | None = None


def _load_thresholds() -> dict[str, float]:
    """
    Charge les seuils spécifiques (en µg/L) par code_parametre depuis un CSV optionnel.
    """
    global _THRESHOLDS_CACHE
    if _THRESHOLDS_CACHE is not None:
        return _THRESHOLDS_CACHE

    path = _default_thresholds_file()
    thresholds: dict[str, float] = {}

    if path.exists():
        try:
            with path.open(encoding="utf-8") as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=";,")
                    reader = csv.DictReader(f, dialect=dialect)
                except csv.Error:
                    f.seek(0)
                    reader = csv.DictReader(f, delimiter=";")
                    if not reader.fieldnames or len(reader.fieldnames) < 1:
                        f.seek(0)
                        reader = csv.DictReader(f, delimiter=",")

                for row in reader:
                    code = str(row.get("code_parametre") or "").strip()
                    val = str(row.get("seuil_ugl") or "").strip()
                    if not code or not val:
                        continue
                    try:
                        thresholds[code] = float(val)
                    except ValueError:
                        continue
        except Exception:
            thresholds = {}

    _THRESHOLDS_CACHE = thresholds
    return thresholds


def seuil_sanitaire_ugL(code_parametre: str | None, meta: dict[str, Any] | None = None) -> float:
    """
    Retourne un seuil sanitaire en µg/L pour un paramètre PPP.

    - code_parametre : code Sandre du paramètre (peut être None).
    - meta : éventuellement informations supplémentaires (libellé, source...) si besoin.

    Stratégie :
    - si un seuil spécifique est défini dans data/ref/seuils_pesticides.csv (ou fichier configuré),
      on l'utilise ;
    - sinon, on applique le seuil générique 0,1 µg/L par pesticide individuel, en cohérence
      avec la directive (UE) 2020/2184 sur la qualité des eaux destinées à la consommation humaine.
    """
    thresholds = _load_thresholds()
    if code_parametre is not None:
        code = str(code_parametre).strip()
        if code in thresholds:
            return thresholds[code]
    return DEFAULT_SANITAIRE_PPP_UGL

