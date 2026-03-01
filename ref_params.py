"""
Référentiels de paramètres PPP (pesticides / phytosanitaires) basés sur les codes Sandre.

Objectif :
- charger une liste officielle (CSV) de codes de paramètres « pesticides » ;
- filtrer les analyses Naïades / ADES pour ne garder que ces paramètres dans la couche SIG.

Le fichier CSV attendu est configurable via config.yaml, section :

ref:
  parametres_pesticides:
    file: data/ref/parametres_pesticides.csv

Format minimal du CSV (séparateur ; ou ,) :
- une colonne contenant le code de paramètre Sandre, nommée par exemple "code_parametre" ou "code".
Toutes les autres colonnes sont ignorées.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import csv
import json

import yaml
import requests

from config import load_config, resolve_path


def _default_pesticide_file() -> Path:
    cfg = load_config() or {}
    ref_cfg = cfg.get("ref", {}).get("parametres_pesticides", {})
    path = ref_cfg.get("file", "data/ref/parametres_pesticides.csv")
    return resolve_path(path)


def _pesticide_remote_url() -> str | None:
    """
    URL distante (Sandre / Eaufrance / autre) d'un CSV de paramètres pesticides.

    Configurable dans config.yaml, par ex. :

    ref:
      parametres_pesticides:
        url: https://exemple.sandre.eaufrance.fr/parametres_pesticides.csv

    Si non renseignée, le programme pourra tenter d'autres stratégies
    (ex. croisement Sandre / C3PO).
    """
    cfg = load_config() or {}
    ref_cfg = cfg.get("ref", {}).get("parametres_pesticides", {})
    url = ref_cfg.get("url")
    if url:
        return str(url)
    return None


def _c3po_substances_path() -> Path:
    """
    Emplacement attendu du fichier JSON des substances C3PO agrégées,
    produit par l'analyse : data/out/substances_c3po_disponibles.json
    """
    cfg = load_config() or {}
    out_dir = cfg.get("analysis", {}).get("out_dir", "data/out")
    return resolve_path(out_dir) / "substances_c3po_disponibles.json"


def _build_pesticide_csv_from_c3po(csv_path: Path) -> bool:
    """
    Construit un CSV de codes paramètres pesticides à partir de C3PO.

    Logique :
    - lit data/out/substances_c3po_disponibles.json ;
    - récupère tous les \"code_parametre_sandre\" non nuls ;
    - écrit un CSV avec une colonne \"code_parametre\".

    Retourne True si au moins un code a été écrit, False sinon.
    """
    json_path = _c3po_substances_path()
    if not json_path.exists():
        return False

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return False

    items = data.get("apercu") or []
    codes: set[str] = set()
    for sub in items:
        raw = sub.get("code_parametre_sandre")
        if raw is None:
            continue
        code = str(raw).strip()
        if code:
            codes.add(code)

    if not codes:
        return False

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["code_parametre"])
        for c in sorted(codes):
            writer.writerow([c])

    print(f"Liste de paramètres pesticides construite à partir de C3PO → {csv_path} ({len(codes)} codes)")
    return True


def _download_pesticide_csv_from_url(csv_path: Path, url: str) -> bool:
    """
    Tente de télécharger un CSV de paramètres pesticides depuis une URL configurée.
    Retourne True en cas de succès, False sinon.
    """
    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        csv_path.write_text(resp.text, encoding="utf-8")
        print(f"Téléchargement des paramètres pesticides depuis {url} → {csv_path}")
        return True
    except Exception as e:
        print(f"Impossible de télécharger la liste de paramètres pesticides depuis {url}: {e}")
        return False


_PESTICIDE_CODES_CACHE: set[str] | None = None


def load_pesticide_codes(path: str | Path | None = None) -> set[str]:
    """
    Charge la liste des codes de paramètres considérés comme « pesticides ».

    Retourne un set de codes (chaînes).
    Si le fichier n'existe pas ou est vide, retourne un set vide (aucun filtrage appliqué).
    """
    global _PESTICIDE_CODES_CACHE

    if path is None and _PESTICIDE_CODES_CACHE is not None:
        return _PESTICIDE_CODES_CACHE

    csv_path = Path(path) if path is not None else _default_pesticide_file()
    codes: set[str] = set()

    if not csv_path.exists():
        # 1) Si une URL est configurée, on tente de télécharger le CSV depuis cette URL.
        url = _pesticide_remote_url()
        if url and _download_pesticide_csv_from_url(csv_path, url):
            pass
        else:
            # 2) Sinon, on tente de construire la liste automatiquement à partir de C3PO.
            if not _build_pesticide_csv_from_c3po(csv_path):
                # Aucun référentiel disponible : aucun filtrage ne sera appliqué.
                return set()

    # On accepte ; ou , comme séparateur
    with csv_path.open(encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            # Tentative de détection automatique du séparateur
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            reader = csv.DictReader(f, dialect=dialect)
        except csv.Error:
            # Cas typique : un seul champ par ligne (aucun caractère ';' ou ',')
            # → on essaie d'abord avec ';' puis avec ','.
            f.seek(0)
            reader = csv.DictReader(f, delimiter=";")
            if not reader.fieldnames or len(reader.fieldnames) < 1:
                f.seek(0)
                reader = csv.DictReader(f, delimiter=",")

        for row in reader:
            # On tolère plusieurs noms de colonnes possibles
            raw = (
                row.get("code_parametre")
                or row.get("Code_parametre")
                or row.get("code")
                or row.get("Code")
                or ""
            )
            code = str(raw).strip()
            if code:
                codes.add(code)

    if path is None:
        _PESTICIDE_CODES_CACHE = codes
    return codes


def filter_analyses_pesticides(
    analyses: Iterable[dict[str, Any]],
    code_field: str,
) -> list[dict[str, Any]]:
    """
    Filtre une liste d'analyses pour ne garder que celles dont le code de paramètre
    figure dans la liste des pesticides (Sandre).

    - analyses : itérable de dicts (enregistrements Naïades / ADES).
    - code_field : nom du champ contenant le code paramètre
      (ex. 'code_parametre' pour Naïades, 'code_param' pour ADES).

    Si aucun référentiel n'est chargé (set vide), retourne la liste telle quelle.
    """
    codes = load_pesticide_codes()
    analyses_list = list(analyses)
    if not codes:
        # Pas de référentiel → aucun filtrage PPP appliqué
        return analyses_list

    out: list[dict[str, Any]] = []
    for a in analyses_list:
        raw = a.get(code_field)
        if raw is None:
            continue
        code = str(raw).strip()
        if code in codes:
            out.append(a)
    return out

