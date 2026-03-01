"""
Lien direct vers la fiche toxicologique INRS (Fichetox) par numéro CAS.

L'INRS expose les fiches à l'URL :
  https://www.inrs.fr/publications/bdd/fichetox/fiche.html?refINRS=FICHETOX_<numéro>

Une table de correspondance CAS → numéro de fiche (fichetox_cas_ref.csv) permet
de construire l'URL directe. Sinon, on renvoie la page d'accueil Fichetox pour recherche manuelle.
"""
from __future__ import annotations

import csv
from pathlib import Path

from config import load_config, resolve_path

BASE_URL_FICHE = "https://www.inrs.fr/publications/bdd/fichetox/fiche.html"
BASE_URL_ACCUEIL = "https://www.inrs.fr/publications/bdd/fichetox.html"

DEFAULT_CSV = "sources/sources_dictionnaire/fichetox_cas_ref.csv"

_CAS_TO_REF: dict[str, str] | None = None


def _normalize_cas(cas: str | None) -> str:
    """Normalise le numéro CAS pour la clé de recherche (sans espaces, tirets conservés)."""
    if not cas:
        return ""
    s = str(cas).strip().replace(" ", "")
    return s


def _load_cas_to_ref(csv_path: str | Path | None = None) -> dict[str, str]:
    """
    Charge le CSV de correspondance CAS → numéro de fiche Fichetox.
    Colonnes attendues : cas, ref_fichetox (numéro seul, ex. 286).
    """
    global _CAS_TO_REF
    if _CAS_TO_REF is not None:
        return _CAS_TO_REF

    if csv_path is None:
        cfg = load_config()
        path_cfg = (cfg.get("ref") or {}).get("fichetox_cas_ref")
        if path_cfg:
            p = Path(path_cfg)
            if not p.is_absolute():
                p = resolve_path(str(p))
            csv_path = p
        else:
            csv_path = resolve_path(DEFAULT_CSV)

    path = Path(csv_path)
    index: dict[str, str] = {}

    if path.exists():
        try:
            with open(path, encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    cas_raw = (row.get("cas") or "").strip()
                    ref = (row.get("ref_fichetox") or "").strip()
                    if not cas_raw or not ref:
                        continue
                    # Numéro seul (286) ou préfixe FICHETOX_286
                    ref_num = ref.replace("FICHETOX_", "").strip()
                    if not ref_num.isdigit():
                        continue
                    cas_norm = _normalize_cas(cas_raw)
                    if cas_norm:
                        index[cas_norm] = ref_num
                        # Garder aussi la forme avec tirets si différente
                        if "-" in cas_raw and cas_norm != cas_raw.replace(" ", ""):
                            index[cas_raw.replace(" ", "")] = ref_num
        except Exception:
            pass

    _CAS_TO_REF = index
    return index


def get_fichetox_url(cas: str | None) -> str:
    """
    Retourne l'URL de la fiche toxicologique INRS pour la substance identifiée par son CAS.
    Si une correspondance existe dans fichetox_cas_ref.csv, renvoie l'URL directe de la fiche ;
    sinon renvoie la page d'accueil Fichetox (recherche manuelle).
    """
    if not cas:
        return BASE_URL_ACCUEIL
    cas_norm = _normalize_cas(cas)
    if not cas_norm:
        return BASE_URL_ACCUEIL
    index = _load_cas_to_ref()
    ref = index.get(cas_norm)
    if ref is None and "-" in cas_norm:
        ref = index.get(cas_norm.replace("-", ""))
    if ref is None:
        return BASE_URL_ACCUEIL
    return f"{BASE_URL_FICHE}?refINRS=FICHETOX_{ref}"
