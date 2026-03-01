"""
Configuration centralisée phytoDB.
Charge config.yaml depuis la racine du projet.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
_CACHE: dict[str, Any] | None = None


def load_config(reload: bool = False) -> dict[str, Any]:
    """Charge la configuration depuis config.yaml (mise en cache)."""
    global _CACHE
    if _CACHE is not None and not reload:
        return _CACHE
    if not _CONFIG_PATH.exists():
        _CACHE = {}
        return _CACHE
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        _CACHE = yaml.safe_load(f) or {}
    return _CACHE


def get_cache_dir(config: dict[str, Any] | None = None) -> Path | None:
    """Retourne le répertoire de cache si activé, sinon None."""
    cfg = config or load_config()
    c = cfg.get("cache", {})
    if not c.get("enabled", True):
        return None
    return resolve_path(c.get("dir", "data/cache"))


def get_code_departement(config: dict[str, Any] | None = None) -> str:
    """Retourne le code département (ex. 21 pour Côte-d'Or)."""
    cfg = config or load_config()
    return cfg.get("departement", {}).get("code", "21")


def cache_path(cache_dir: Path, prefix: str, code_dep: str | None = None) -> Path:
    """Construit le chemin d'un fichier de cache (ex. naiades_analyses_21.json)."""
    dep = code_dep or get_code_departement()
    return cache_dir / f"{prefix}_{dep}.json"


def project_root() -> Path:
    """Racine du projet (répertoire contenant config.yaml)."""
    return Path(__file__).resolve().parent


def resolve_path(rel_path: str) -> Path:
    """Résout un chemin relatif par rapport à la racine du projet."""
    p = Path(rel_path)
    return project_root() / p if not p.is_absolute() else p
