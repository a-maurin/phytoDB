"""
Couche SIG : schéma normalisé et export GeoJSON pour visualiser les zones impactées par les PPP.
- Uniquement les entités localisées en Côte-d'Or (département 21).
- Table attributaire : champs utiles pour un utilisateur non spécialiste des PPP.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

from thresholds import seuil_sanitaire_ugL
from ppp_dict import lookup_ppp_usage
from utils import resultat_to_ugl

try:
    from sources.nqe_ecophyto import get_nqe_for_analyse
except ImportError:
    get_nqe_for_analyse = None
try:
    from sources.amm import get_amm_autorise
except ImportError:
    get_amm_autorise = None
try:
    from sources.fichetox import get_fichetox_url
except ImportError:
    get_fichetox_url = None

# Département cible : seules les entités en Côte-d'Or sont conservées
CODE_DEPARTEMENT_COTE_DOR = "21"

# Champs des entités (ordre d'affichage pour un utilisateur non spécialiste)
COLONNES_ATTR = [
    "lieu",
    "commune",
    "cours_eau",
    "masse_eau",
    "substance",
    "usage_ppp",
    "amm_autorise",
    "concentration_ugl",
    "depassement_seuil_sanitaire",
    "ratio_seuil_sanitaire",
    "depassement_seuil_nqe",
    "date_prelevement",
    "type_eau",
    "lien_fiche",
    "wkt_geom",
]


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


def _oui_non(value: bool | None) -> str | None:
    """Formate un booléen en 'oui' / 'non' pour l'affichage."""
    if value is True:
        return "oui"
    if value is False:
        return "non"
    return None


    return None


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

    # Nom lisible prioritaire : libellé Sandre du paramètre, sinon divers libellés C3PO
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

    # 1) Dictionnaire manuel d'usages PPP (sources de référence préparées en CSV)
    dict_meta = lookup_ppp_usage(
        code_parametre=code_parametre,
        cas_parametre=(c3po or {}).get("cas_parametre_sandre") if c3po else None,
    )
    usage = dict_meta.get("ppp_usage") if dict_meta else None
    usages_typiques = dict_meta.get("ppp_usages_typiques") if dict_meta else None

    # 2) Si rien dans le dictionnaire, on applique la détection heuristique par mots-clés
    if usage is None:
        if "herbicide" in text_for_detection:
            usage = "herbicide"
        elif "insecticide" in text_for_detection:
            usage = "insecticide"
        elif "fongicide" in text_for_detection or "fungicide" in text_for_detection:
            usage = "fongicide"
        elif "acaricide" in text_for_detection:
            usage = "acaricide"
        elif "rodenticide" in text_for_detection:
            usage = "rodenticide"
        elif "nematicide" in text_for_detection or "nématicide" in text_for_detection:
            usage = "nématicide"
        elif "pheromone" in text_for_detection or "phéromone" in text_for_detection:
            usage = "phéromone de confusion sexuelle"

    # 3) Cas d'usage typiques génériques, uniquement si non fournis par le dictionnaire
    if usages_typiques is None and usage is not None:
        if usage == "herbicide":
            usages_typiques = "Désherbage des cultures, bords de champs, talus ou voiries."
        elif usage == "insecticide":
            usages_typiques = "Lutte contre les insectes ravageurs des cultures ou des stockages."
        elif usage == "fongicide":
            usages_typiques = "Protection des cultures contre les maladies fongiques (mildiou, oïdium, etc.)."
        elif usage == "acaricide":
            usages_typiques = "Lutte contre les acariens sur les cultures."
        elif usage == "rodenticide":
            usages_typiques = "Lutte contre les rongeurs (bâtiments agricoles, stockages, etc.)."
        elif usage in ("nématicide", "nematicide"):
            usages_typiques = "Lutte contre les nématodes des cultures."
        elif usage == "phéromone de confusion sexuelle":
            usages_typiques = "Confusion sexuelle pour limiter les ravageurs, en viticulture ou arboriculture."

    if usage:
        phrase = f"Produit phytopharmaceutique de type {usage}, utilisé principalement pour la protection des cultures."
    else:
        phrase = "Substance phytopharmaceutique suivie dans les eaux ; consulter INRS et e-phy pour les usages détaillés."

    if base_label:
        desc = f"{phrase} (substance : {base_label})."
        nom_ppp = base_label
    elif lib:
        desc = f"{phrase} (paramètre : {lib})."
        nom_ppp = lib
    elif code:
        desc = f"{phrase} (code paramètre Sandre {code})."
        nom_ppp = f"Paramètre {code}"
    else:
        desc = phrase
        nom_ppp = None

    # Fiche INRS : lien direct vers la fiche toxicologique si CAS connu et présent dans fichetox_cas_ref.csv
    cas = (c3po or {}).get("cas_parametre_sandre") if c3po else None
    url_inrs = get_fichetox_url(cas) if get_fichetox_url else "https://www.inrs.fr/publications/bdd/fichetox.html"
    url_ephy = "https://ephy.anses.fr/"
    # Fiche Sandre du paramètre (référentiel EauFrance)
    url_sandre = f"http://id.eaufrance.fr/par/{code}" if code else None

    # AMM : bénéficie d'une autorisation de mise sur le marché (référentiel décision AMM, lookup par CAS)
    ppp_amm_autorise = get_amm_autorise(cas) if get_amm_autorise and cas else None

    return {
        "ppp_nom": nom_ppp,
        "ppp_usage": usage,
        "ppp_amm_autorise": ppp_amm_autorise,
        "ppp_usages_typiques": usages_typiques,
        "ppp_description": desc or None,
        "ppp_url_inrs": url_inrs,
        "ppp_url_ephy": url_ephy,
        "ppp_url_sandre": url_sandre,
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
    # Filtre Côte-d'Or (codes passés dans attrs pour le filtre, pas exposés dans les propriétés)
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
        "lieu": station.get("libelle_station"),
        "commune": station.get("libelle_commune"),
        "cours_eau": station.get("nom_cours_eau"),
        "masse_eau": station.get("nom_masse_deau"),
        "type_eau": "surface",
        "code_departement": code_dep,
        "code_commune": station.get("code_commune"),
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

    # Métadonnées PPP (nom, usage, usages typiques, description, liens)
    meta_ppp = _ppp_metadata_for_param(analyse.get("code_parametre"), analyse.get("libelle_parametre"))

    # Conversion du résultat en µg/L si possible
    resultat = analyse.get("resultat")
    unite = analyse.get("symbole_unite")
    conc_ugl = resultat_to_ugl(resultat, unite)
    seuil_ugl: float | None = None
    ratio: float | None = None
    depassement: bool | None = None

    if conc_ugl is not None:
        seuil_ugl = seuil_sanitaire_ugL(analyse.get("code_parametre"), analyse)
        if seuil_ugl and seuil_ugl > 0:
            ratio = conc_ugl / seuil_ugl
            depassement = ratio > 1.0

    # Enrichissement NQE (dépassements réglementaires Ecophyto 2030, eaux de surface uniquement)
    nqe_ma, nqe_cma = (None, None)
    if get_nqe_for_analyse:
        try:
            nqe_ma, nqe_cma = get_nqe_for_analyse(
                analyse.get("code_station"),
                analyse.get("code_parametre"),
                annee,
            )
        except Exception:
            pass
    depassement_nqe = (nqe_ma is True or nqe_cma is True) if (nqe_ma is not None or nqe_cma is not None) else None

    attrs = {
        "lieu": analyse.get("libelle_station"),
        "commune": analyse.get("libelle_commune") or analyse.get("code_commune"),
        "cours_eau": analyse.get("nom_cours_eau"),
        "masse_eau": analyse.get("nom_masse_deau"),
        "substance": meta_ppp.get("ppp_nom"),
        "usage_ppp": meta_ppp.get("ppp_usage"),
        "amm_autorise": _oui_non(meta_ppp.get("ppp_amm_autorise")),
        "concentration_ugl": conc_ugl,
        "depassement_seuil_sanitaire": _oui_non(depassement),
        "ratio_seuil_sanitaire": round(ratio, 2) if ratio is not None else None,
        "depassement_seuil_nqe": _oui_non(depassement_nqe),
        "date_prelevement": date_prel,
        "type_eau": "surface",
        "lien_fiche": meta_ppp.get("ppp_url_inrs"),
        # Pour filtre Côte-d'Or (non exposés)
        "code_departement": code_dep or CODE_DEPARTEMENT_COTE_DOR,
        "code_commune": analyse.get("code_commune"),
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
        "lieu": station.get("bss_id") or station.get("code_bss"),
        "commune": station.get("nom_commune"),
        "type_eau": "souterraine",
        "num_departement": str(num_dep) if num_dep is not None else None,
        "code_departement": CODE_DEPARTEMENT_COTE_DOR if num_dep in ("21", 21) else None,
        "code_commune": code_insee,
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

    # Métadonnées PPP (nom, usage, usages typiques, description, liens)
    meta_ppp = _ppp_metadata_for_param(analyse.get("code_param"), analyse.get("nom_param"))

    # Conversion du résultat en µg/L si possible
    resultat = analyse.get("resultat")
    unite = analyse.get("symbole_unite")
    conc_ugl = resultat_to_ugl(resultat, unite)
    seuil_ugl: float | None = None
    ratio: float | None = None
    depassement: bool | None = None

    if conc_ugl is not None:
        seuil_ugl = seuil_sanitaire_ugL(analyse.get("code_param"), analyse)
        if seuil_ugl and seuil_ugl > 0:
            ratio = conc_ugl / seuil_ugl
            depassement = ratio > 1.0

    attrs = {
        "lieu": analyse.get("bss_id") or analyse.get("code_bss"),
        "commune": analyse.get("nom_commune_actuel"),
        "cours_eau": None,
        "masse_eau": None,
        "substance": meta_ppp.get("ppp_nom"),
        "usage_ppp": meta_ppp.get("ppp_usage"),
        "amm_autorise": _oui_non(meta_ppp.get("ppp_amm_autorise")),
        "concentration_ugl": conc_ugl,
        "depassement_seuil_sanitaire": _oui_non(depassement),
        "ratio_seuil_sanitaire": ratio,
        "ratio_seuil_sanitaire_phrase": _ratio_seuil_phrase(ratio),
        "depassement_seuil_nqe": None,
        "date_prelevement": date_prel,
        "type_eau": "souterraine",
        "lien_fiche": meta_ppp.get("ppp_url_inrs"),
        "code_departement": CODE_DEPARTEMENT_COTE_DOR if num_dep in ("21", 21) else None,
        "num_departement": num_dep,
        "code_commune": code_insee,
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
    Construit les features GeoJSON avec table attributaire normalisée,
    en ne conservant que les entités issues d'analyses (pas les stations seules),
    et uniquement pour la Côte-d'Or (code_departement ou num_departement = 21).
    """
    features: list[dict[str, Any]] = []
    for a in (naiades_analyses or [])[:max_analyses_per_source]:
        f = feature_naiades_analyse(a)
        if f:
            features.append(f)
    for a in (ades_analyses or [])[:max_analyses_per_source]:
        f = feature_ades_analyse(a)
        if f:
            features.append(f)
    return features


def export_sig_geojson(
    out_path: str | Path = "data/sig/analyse_stations_ppp_cote_dor.geojson",
    naiades_stations: list[dict] | None = None,
    naiades_analyses: list[dict] | None = None,
    ades_stations: list[dict] | None = None,
    ades_analyses: list[dict] | None = None,
) -> Path:
    """
    Écrit la couche SIG (GeoJSON) :
    - uniquement entités en Côte-d'Or ;
    - table attributaire : lieu, commune, cours_eau, masse_eau, substance, usage_ppp,
      amm_autorise, concentration_ugl, depassement_seuil_sanitaire, depassement_seuil_nqe,
      date_prelevement, type_eau, lien_fiche, wkt_geom.
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
