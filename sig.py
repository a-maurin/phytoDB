"""
Couche SIG : schéma normalisé et export GeoJSON pour visualiser les zones impactées par les PPP.
- Uniquement les entités localisées en Côte-d'Or (département 21).
- Table attributaire fixe : localisation + toutes les données possibles sur présence/quantité/impact des PPP.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

# Département cible : seules les entités en Côte-d'Or sont conservées
CODE_DEPARTEMENT_COTE_DOR = "21"

# Schéma strict de la table attributaire (colonnes toujours présentes)
COLONNES_BASE = [
    "wkt_geom",
    "source",
    "type_donnee",
    "bss_id",
    "code_bss",
    "code_station",
    "libelle_station",
    "code_departement",
    "code_commune",
    "libelle_commune",
    "nom_cours_eau",
    "nom_masse_deau",
    "num_departement",
    "nom_commune",
]
# Colonnes relatives aux PPP et à l'impact (paramètre, résultat, unité, date, liens)
COLONNES_PPP_IMPACT = [
    "libelle_parametre",
    "code_parametre",
    "resultat",
    "symbole_unite",
    "date_prelevement",
    "annee",
    # Champs dérivés pour mieux décrire le PPP et pointer vers les bases externes
    "ppp_description",
    "ppp_url_inrs",
    "ppp_url_ephy",
]
COLONNES_ATTR = COLONNES_BASE + COLONNES_PPP_IMPACT


def load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_c3po_substances() -> dict[str, dict[str, Any]]:
    """
    Charge le fichier d'aperçu des substances C3PO et indexe par code_parametre_sandre.
    Retourne un dict {code_parametre_sandre -> enregistrement C3PO}.
    """
    root = Path(__file__).resolve().parent
    path = root / "data" / "out" / "substances_c3po_disponibles.json"
    if not path.exists():
        return {}
    try:
        import json

        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for sub in data.get("apercu") or []:
        raw = sub.get("code_parametre_sandre")
        if raw is None:
            continue
        code = str(raw).strip()
        if not code:
            continue
        out[code] = sub
    return out


_C3PO_BY_PARAM: dict[str, dict[str, Any]] = _load_c3po_substances()


def _geom_to_wkt(geom: dict[str, Any] | None) -> str | None:
    """Convertit une géométrie GeoJSON Point en WKT (ex. 'Point (4.72 47.52)')."""
    if not geom or geom.get("type") != "Point":
        return None
    coords = geom.get("coordinates")
    if not coords or len(coords) < 2:
        return None
    return f"Point ({coords[0]} {coords[1]})"


def _ppp_metadata_for_param(code_parametre: Any, libelle_parametre: Any) -> dict[str, Any]:
    """
    Construit une petite description du PPP et deux URL (INRS, e-phy) à partir
    du code de paramètre Sandre et, si possible, des métadonnées C3PO.
    """
    code = str(code_parametre) if code_parametre is not None else ""
    lib = str(libelle_parametre) if libelle_parametre is not None else ""

    c3po = _C3PO_BY_PARAM.get(code) if code else None

    # Description prioritaire : libellé Sandre du paramètre, sinon divers libellés C3PO
    base_label = lib
    if not base_label and c3po:
        base_label = (
            c3po.get("libelle_parametre_sandre")
            or c3po.get("libelle_ephy")
            or c3po.get("libelle_bnvd")
            or c3po.get("libelle_agritox")
            or ""
        )

    # Détection très simple du type de PPP à partir des libellés C3PO
    parts = [str(base_label or "")]
    if c3po:
        for k in ("libelle_ephy", "libelle_bnvd", "libelle_agritox"):
            parts.append(str(c3po.get(k) or ""))
    text_for_detection = " ".join(parts).lower()

    type_texte = None
    if "herbicide" in text_for_detection:
        type_texte = "herbicide"
    elif "insecticide" in text_for_detection:
        type_texte = "insecticide"
    elif "fongicide" in text_for_detection or "fongicide" in text_for_detection:
        type_texte = "fongicide"
    elif "acaricide" in text_for_detection:
        type_texte = "acaricide"
    elif "rodenticide" in text_for_detection:
        type_texte = "rodenticide"
    elif "nematicide" in text_for_detection or "nématicide" in text_for_detection:
        type_texte = "nématicide"
    elif "pheromone" in text_for_detection or "phéromone" in text_for_detection:
        type_texte = "phéromone de confusion sexuelle"

    if type_texte:
        phrase = f"Produit phytopharmaceutique de type {type_texte}, utilisé principalement pour la protection des cultures."
    else:
        phrase = "Substance phytopharmaceutique suivie dans les eaux ; consulter INRS et e-phy pour les usages détaillés."

    if base_label:
        desc = f"{phrase} (substance : {base_label})."
    elif lib:
        desc = f"{phrase} (paramètre : {lib})."
    elif code:
        desc = f"{phrase} (code paramètre Sandre {code})."
    else:
        desc = phrase

    # URL vers les sites officiels (pages d'entrée des bases)
    url_inrs = "https://www.inrs.fr/publications/bdd/fichetox.html"
    url_ephy = "https://ephy.anses.fr/"

    return {
        "ppp_description": desc or None,
        "ppp_url_inrs": url_inrs,
        "ppp_url_ephy": url_ephy,
    }


def _empty_attrs() -> dict[str, Any]:
    """Retourne un dictionnaire avec toutes les colonnes attributaires à vide."""
    return {c: None for c in COLONNES_ATTR}


def _in_cote_dor(code_dep: Any, num_dep: Any, code_insee: Any) -> bool:
    """True si l'entité est localisée en Côte-d'Or (21)."""
    if code_dep in ("21", 21):
        return True
    if num_dep in ("21", 21):
        return True
    if code_insee and str(code_insee).strip().startswith("21"):
        return True
    return False


def _feature_normalized(geom: dict | None, attrs: dict[str, Any]) -> dict[str, Any] | None:
    """Construit une feature GeoJSON avec table attributaire normalisée (Côte-d'Or uniquement)."""
    if not geom:
        return None
    wkt = _geom_to_wkt(geom)
    if not wkt:
        return None
    if not _in_cote_dor(
        attrs.get("code_departement"),
        attrs.get("num_departement"),
        attrs.get("code_commune") or attrs.get("code_insee"),
    ):
        return None
    out = _empty_attrs()
    out["wkt_geom"] = wkt
    for k, v in attrs.items():
        if k in out and v is not None and v != "":
            out[k] = v
    return {"type": "Feature", "geometry": geom, "properties": out}


def feature_naiades_station(station: dict[str, Any], source_label: str = "Naïades") -> dict[str, Any] | None:
    """Station Naïades → feature normalisée (Côte-d'Or uniquement)."""
    geom = station.get("geometry")
    if not geom:
        lon, lat = station.get("longitude"), station.get("latitude")
        if lon is not None and lat is not None:
            geom = {"type": "Point", "coordinates": [float(lon), float(lat)]}
    if not geom:
        return None
    code_dep = station.get("code_departement")
    if code_dep != CODE_DEPARTEMENT_COTE_DOR:
        return None
    attrs = {
        "source": source_label,
        "type_donnee": "impact_surface",
        "code_station": station.get("code_station"),
        "libelle_station": station.get("libelle_station"),
        "code_departement": station.get("code_departement"),
        "code_commune": station.get("code_commune"),
        "libelle_commune": station.get("libelle_commune"),
        "nom_cours_eau": station.get("nom_cours_eau"),
        "nom_masse_deau": station.get("nom_masse_deau"),
    }
    return _feature_normalized(geom, attrs)


def feature_naiades_analyse(analyse: dict[str, Any], source_label: str = "Naïades") -> dict[str, Any] | None:
    """Analyse Naïades → feature normalisée avec données PPP/impact (Côte-d'Or uniquement)."""
    geom = analyse.get("geometry")
    if not geom:
        lon, lat = analyse.get("longitude"), analyse.get("latitude")
        if lon is not None and lat is not None:
            geom = {"type": "Point", "coordinates": [float(lon), float(lat)]}
    if not geom:
        return None
    code_dep = analyse.get("code_departement")
    if code_dep is not None and code_dep != CODE_DEPARTEMENT_COTE_DOR:
        return None
    date_prel = analyse.get("date_prelevement")
    annee = str(date_prel)[:4] if date_prel else None
    meta_ppp = _ppp_metadata_for_param(analyse.get("code_parametre"), analyse.get("libelle_parametre"))
    attrs = {
        "source": source_label,
        "type_donnee": "impact_surface",
        "code_station": analyse.get("code_station"),
        "libelle_station": analyse.get("libelle_station"),
        "code_departement": code_dep or CODE_DEPARTEMENT_COTE_DOR,
        "code_commune": analyse.get("code_commune"),
        "libelle_commune": analyse.get("libelle_commune"),
        "nom_cours_eau": analyse.get("nom_cours_eau"),
        "nom_masse_deau": analyse.get("nom_masse_deau"),
        "libelle_parametre": analyse.get("libelle_parametre"),
        "code_parametre": analyse.get("code_parametre"),
        "resultat": analyse.get("resultat"),
        "symbole_unite": analyse.get("symbole_unite"),
        "date_prelevement": date_prel,
        "annee": annee,
        "ppp_description": meta_ppp.get("ppp_description"),
        "ppp_url_inrs": meta_ppp.get("ppp_url_inrs"),
        "ppp_url_ephy": meta_ppp.get("ppp_url_ephy"),
    }
    return _feature_normalized(geom, attrs)


def feature_ades_station(station: dict[str, Any], source_label: str = "ADES") -> dict[str, Any] | None:
    """Station ADES → feature normalisée (Côte-d'Or uniquement)."""
    geom = station.get("geometry")
    if not geom:
        lon, lat = station.get("longitude"), station.get("latitude")
        if lon is not None and lat is not None:
            geom = {"type": "Point", "coordinates": [float(lon), float(lat)]}
    if not geom:
        return None
    num_dep = station.get("num_departement")
    code_insee = station.get("code_insee")
    if not _in_cote_dor(None, num_dep, code_insee):
        return None
    attrs = {
        "source": source_label,
        "type_donnee": "impact_souterrain",
        "bss_id": station.get("bss_id"),
        "code_bss": station.get("code_bss"),
        "num_departement": str(num_dep) if num_dep is not None else None,
        "nom_commune": station.get("nom_commune"),
        "code_departement": CODE_DEPARTEMENT_COTE_DOR if num_dep in ("21", 21) else None,
    }
    return _feature_normalized(geom, attrs)


def feature_ades_analyse(analyse: dict[str, Any], source_label: str = "ADES") -> dict[str, Any] | None:
    """Analyse ADES → feature normalisée avec données PPP/impact (Côte-d'Or uniquement)."""
    lon, lat = analyse.get("longitude"), analyse.get("latitude")
    if lon is None or lat is None:
        return None
    geom = {"type": "Point", "coordinates": [float(lon), float(lat)]}
    num_dep = analyse.get("num_departement")
    code_insee = analyse.get("code_insee_actuel")
    if not _in_cote_dor(analyse.get("code_departement"), num_dep, code_insee):
        return None
    date_prel = analyse.get("date_debut_prelevement")
    annee = str(date_prel)[:4] if date_prel else None
    meta_ppp = _ppp_metadata_for_param(analyse.get("code_param"), analyse.get("nom_param"))
    attrs = {
        "source": source_label,
        "type_donnee": "impact_souterrain",
        "bss_id": analyse.get("bss_id"),
        "code_bss": analyse.get("code_bss"),
        "num_departement": str(num_dep) if num_dep is not None else None,
        "nom_commune": analyse.get("nom_commune_actuel"),
        "code_departement": CODE_DEPARTEMENT_COTE_DOR if num_dep in ("21", 21) else None,
        "libelle_parametre": analyse.get("nom_param"),
        "code_parametre": analyse.get("code_param"),
        "resultat": analyse.get("resultat"),
        "symbole_unite": analyse.get("symbole_unite"),
        "date_prelevement": date_prel,
        "annee": annee,
        "ppp_description": meta_ppp.get("ppp_description"),
        "ppp_url_inrs": meta_ppp.get("ppp_url_inrs"),
        "ppp_url_ephy": meta_ppp.get("ppp_url_ephy"),
    }
    return _feature_normalized(geom, attrs)


def build_geojson_features(
    naiades_stations: list[dict] | None = None,
    naiades_analyses: list[dict] | None = None,
    ades_stations: list[dict] | None = None,
    ades_analyses: list[dict] | None = None,
    max_analyses_per_source: int = 50000,
) -> list[dict[str, Any]]:
    """
    Construit les features GeoJSON avec table attributaire normalisée.
    Uniquement les entités localisées en Côte-d'Or (code_departement ou num_departement = 21).
    """
    features = []
    for s in (naiades_stations or []):
        f = feature_naiades_station(s)
        if f:
            features.append(f)
    for a in (naiades_analyses or [])[:max_analyses_per_source]:
        f = feature_naiades_analyse(a)
        if f:
            features.append(f)
    for s in (ades_stations or []):
        f = feature_ades_station(s)
        if f:
            features.append(f)
    for a in (ades_analyses or [])[:max_analyses_per_source]:
        f = feature_ades_analyse(a)
        if f:
            features.append(f)
    return features


def export_sig_geojson(
    out_path: str | Path = "data/sig/impact_ppp_cote_dor.geojson",
    naiades_stations: list[dict] | None = None,
    naiades_analyses: list[dict] | None = None,
    ades_stations: list[dict] | None = None,
    ades_analyses: list[dict] | None = None,
) -> Path:
    """
    Écrit la couche SIG (GeoJSON) :
    - uniquement entités en Côte-d'Or ;
    - table attributaire : wkt_geom, source, type_donnee, bss_id, code_bss, code_station,
      libelle_station, code_departement, code_commune, libelle_commune, nom_cours_eau,
      nom_masse_deau, num_departement, nom_commune, libelle_parametre, code_parametre,
      resultat, symbole_unite, date_prelevement, annee.
    Permet d'évaluer le degré d'impact (paramètres, quantités, résultats d'analyses).
    """
    features = build_geojson_features(
        naiades_stations=naiades_stations,
        naiades_analyses=naiades_analyses,
        ades_stations=ades_stations,
        ades_analyses=ades_analyses,
    )
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fc = {"type": "FeatureCollection", "features": features}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)
    return out_path
