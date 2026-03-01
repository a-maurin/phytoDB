"""
Référentiel AMM (autorisation de mise sur le marché) - décisions AMM intrants.

Charge le CSV « substance_active » du jeu decision AMM (format CSV opendata)
et fournit un lookup par numéro CAS : la substance bénéficie-t-elle d'une AMM
(état INSCRITE = inscrite sur la liste des substances autorisées).
"""
from __future__ import annotations

import csv
from pathlib import Path

from config import load_config, resolve_path

# Sous-dossier par défaut (décisions AMM, format CSV UTF-8)
DEFAULT_SUBSTANCE_ACTIVE_DIR = "sources/sources_dictionnaire/decisionamm-intrant-format-csv-20260224-utf8"
SUBSTANCE_ACTIVE_FILENAME = "substance_active_utf8.csv"

# États dans le CSV : INSCRITE = substance autorisée (AMM), NON_INSCRITE = retirée / non approuvée
ETAT_INSCRITE = "INSCRITE"

_AMM_BY_CAS: dict[str, bool] | None = None


def _default_substance_active_path() -> Path:
    cfg = load_config()
    ref = cfg.get("ref", {})
    amm_dir = ref.get("amm", {}).get("substance_active_dir") or ref.get("sources_dictionnaire_amm")
    if amm_dir:
        p = Path(amm_dir)
        if not p.is_absolute():
            p = resolve_path(str(p))
        return p / SUBSTANCE_ACTIVE_FILENAME
    return resolve_path(DEFAULT_SUBSTANCE_ACTIVE_DIR) / SUBSTANCE_ACTIVE_FILENAME


def load_amm_by_cas(
    csv_path: str | Path | None = None,
) -> dict[str, bool]:
    """
    Charge le référentiel substance_active et retourne un dict {numero_cas_normalise -> autorise}.
    autorise = True si état = INSCRITE, False si NON_INSCRITE.
    """
    global _AMM_BY_CAS
    if _AMM_BY_CAS is not None:
        return _AMM_BY_CAS

    path = Path(csv_path) if csv_path else _default_substance_active_path()
    index: dict[str, bool] = {}

    if not path.exists():
        _AMM_BY_CAS = index
        return index

    try:
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            headers = reader.fieldnames or []
            cas_key = next((h for h in headers if "cas" in h.lower() and "numero" in h.lower()), "Numero CAS")
            etat_key = next((h for h in headers if "autorisation" in h.lower()), "Etat d'autorisation")
            for row in reader:
                etat = (row.get(etat_key) or "").strip()
                if not etat or etat == etat_key:
                    continue
                cas = str(row.get(cas_key) or "").strip()
                if not cas:
                    continue
                cas_norm = cas.replace(" ", "")
                index[cas_norm] = etat == ETAT_INSCRITE
    except Exception:
        pass

    _AMM_BY_CAS = index
    return index


def get_amm_autorise(cas: str | None) -> bool | None:
    """
    Retourne True si la substance (numéro CAS) bénéficie d'une AMM (inscrite),
    False si non inscrite, None si inconnu.
    """
    if not cas:
        return None
    cas_str = str(cas).strip().replace(" ", "")
    index = load_amm_by_cas()
    if cas_str in index:
        return index[cas_str]
    if "-" in str(cas):
        alt = cas_str.replace("-", "")
        if alt in index:
            return index[alt]
    return None
