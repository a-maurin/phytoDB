"""
Source NQE (Normes de Qualité Environnementale) - Diagnostics territoriaux Ecophyto 2030.

Charge le CSV BFC (Bourgogne-Franche-Comté) des dépassements NQE-MA / NQE-CMA
(dataset OFB data.gouv.fr) et fournit un index (code_station, code_parametre, annee) -> statut.
Utilisé pour enrichir les entités Naïades (eaux de surface) et la définition des hotspots.
"""
from __future__ import annotations

import csv
import gzip
import io
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import yaml

from config import load_config as _load_config

DEFAULT_URL = (
    "https://data.ofb.fr/catalogue/Donnees-geographiques-OFB/api/records/"
    "d75dbccd-af6a-4468-bfa3-978e3da3c1b8/attachments/resume_sta_param_annee_reg_27_BFC.csv.gz"
)

_NQE_INDEX: dict[tuple[str, str, str], dict[str, bool]] | None = None


def _is_depassement(statut: str | None) -> bool:
    """True si le statut indique un dépassement (exclut 'non dépassement' et 'indéterminé')."""
    if not statut:
        return False
    s = str(statut).strip()
    return s == "dépassement"


def load_nqe_index(
    csv_path: str | Path | None = None,
    url: str | None = None,
    cache_dir: str | Path | None = None,
) -> dict[tuple[str, str, str], dict[str, bool]]:
    """
    Charge l'index NQE : (code_station, code_parametre, annee) -> {nqe_ma_depasse, nqe_cma_depasse}.

    Priorité :
    1. csv_path si fourni et existe (fichier .csv ou .csv.gz local)
    2. cache_dir / nqe_ecophyto_bfc.csv.gz si existe
    3. téléchargement depuis url (ou config nqe.url)
    """
    global _NQE_INDEX
    if _NQE_INDEX is not None:
        return _NQE_INDEX

    config = _load_config()
    cfg = config.get("nqe", {}) or {}
    path_cfg = cfg.get("csv_path") or csv_path
    url_cfg = cfg.get("url") or url or DEFAULT_URL
    cache = Path(cache_dir) if cache_dir else Path(cfg.get("cache_dir") or config.get("cache", {}).get("dir", "data/cache"))

    index: dict[tuple[str, str, str], dict[str, bool]] = {}
    content: str | bytes | None = None
    is_gzip = False

    # 1) Fichier local explicite
    if path_cfg:
        p = Path(path_cfg)
        if p.exists():
            content = p.read_bytes()
            is_gzip = p.suffix == ".gz"
            if not is_gzip:
                content = content.decode("utf-8")

    # 2) Cache
    if content is None:
        cache_file = cache / "nqe_ecophyto_bfc.csv.gz"
        if cache_file.exists():
            content = cache_file.read_bytes()
            is_gzip = True

    # 3) Téléchargement
    if content is None:
        try:
            with urlopen(url_cfg, timeout=120) as resp:
                content = resp.read()
            is_gzip = True
            cache.mkdir(parents=True, exist_ok=True)
            (cache / "nqe_ecophyto_bfc.csv.gz").write_bytes(content)
        except Exception:
            _NQE_INDEX = index
            return index

    # Décompression si besoin
    if is_gzip and isinstance(content, bytes):
        content = gzip.decompress(content).decode("utf-8")
    elif isinstance(content, bytes):
        content = content.decode("utf-8")

    # Parse CSV (séparateur ;)
    reader = csv.DictReader(io.StringIO(content), delimiter=";")
    for row in reader:
        code_station = str(row.get("code_station") or "").strip()
        code_param = str(row.get("code_parametre") or "").strip()
        annee = str(row.get("annee") or "").strip()[:4]
        if not code_station or not code_param or not annee:
            continue
        statut_ma = row.get("statut_nqe_ma_souple")
        statut_cma = row.get("statut_nqe_cma_souple")
        key = (code_station, code_param, annee)
        index[key] = {
            "nqe_ma_depasse": _is_depassement(statut_ma),
            "nqe_cma_depasse": _is_depassement(statut_cma),
        }

    _NQE_INDEX = index
    return index


def get_nqe_for_analyse(
    code_station: str | None,
    code_parametre: str | None,
    annee: str | None,
    index: dict[tuple[str, str, str], dict[str, bool]] | None = None,
) -> tuple[bool | None, bool | None]:
    """
    Retourne (nqe_ma_depasse, nqe_cma_depasse) pour une analyse donnée.
    None si pas de donnée NQE pour ce couple.
    """
    if not code_station or not code_parametre or not annee:
        return (None, None)
    annee = str(annee).strip()[:4]
    idx = index if index is not None else load_nqe_index()
    key = (str(code_station), str(code_parametre), annee)
    rec = idx.get(key)
    if not rec:
        return (None, None)
    return (rec["nqe_ma_depasse"], rec["nqe_cma_depasse"])
