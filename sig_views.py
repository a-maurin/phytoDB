"""
Vues dérivées de la couche SIG principale pour faciliter la lecture par l'utilisateur :

- top10_ppp_par_annee.geojson : analyses des 10 PPP les plus détectés par année ;
- hotspots_ppp.geojson : points chauds de dépassement des seuils sanitaires/réglementaires.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Tuple

from thresholds import seuil_sanitaire_ugL

# Ordre des propriétés pour les couches dérivées (utile en premier)
COLONNES_TOP10_ORDER = [
    "ppp_nom", "ppp_usage", "ppp_usages_typiques", "ppp_taux_ugl", "ppp_seuil_sanitaire_ugl",
    "ppp_ratio_seuil", "ppp_depassement", "top10_ppp_annee", "top10_rang",
    "libelle_station", "libelle_commune", "nom_commune", "code_station", "code_commune",
    "code_departement", "source", "type_donnee", "libelle_parametre", "code_parametre",
    "resultat", "symbole_unite", "date_prelevement", "annee",
]

COLONNES_HOTSPOTS_ORDER = [
    "ppp_nom", "ppp_usage", "ppp_usages_typiques", "n_depassements", "n_mesures",
    "max_ratio", "max_conc_ugl", "ppp_taux_ugl", "ppp_seuil_sanitaire_ugl", "ppp_ratio_seuil", "ppp_depassement",
    "libelle_station", "code_station", "libelle_commune", "nom_commune", "code_commune", "code_departement",
    "bss_id", "code_bss", "source", "identifiant_point", "code_parametre", "libelle_parametre",
    "annee_min", "annee_max",
]


def _load_features(sig_path: str | Path) -> list[dict[str, Any]]:
    p = Path(sig_path)
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        fc = json.load(f)
    return fc.get("features", [])


def export_top10_ppp_par_annee(
    sig_path: str | Path = "data/sig/analyse_stations_ppp_cote_dor.geojson",
    out_path: str | Path = "data/sig/top10_ppp_par_annee.geojson",
) -> Path:
    """
    Construit une couche ne contenant que les analyses des 10 PPP les plus détectés par année.
    """
    features = _load_features(sig_path)
    if not features:
        return Path(out_path)

    # Comptage des occurrences (analyses) par (année, code_parametre)
    counts: Dict[Tuple[str, str], int] = defaultdict(int)
    for f in features:
        props = f.get("properties") or {}
        code = props.get("code_parametre")
        annee = props.get("annee")
        if not code or not annee:
            continue
        key = (str(annee), str(code))
        counts[key] += 1

    # Détermination des top 10 par année
    top_ranks: Dict[str, Dict[str, int]] = defaultdict(dict)  # annee -> code_parametre -> rang (1..10)
    by_year: Dict[str, list[Tuple[str, int]]] = defaultdict(list)
    for (annee, code), n in counts.items():
        by_year[annee].append((code, n))

    for annee, lst in by_year.items():
        lst_sorted = sorted(lst, key=lambda x: x[1], reverse=True)[:10]
        for rank, (code, _) in enumerate(lst_sorted, start=1):
            top_ranks[annee][code] = rank

    # Filtrage des features et ajout du rang
    out_features: list[dict[str, Any]] = []
    for f in features:
        props = f.get("properties") or {}
        code = props.get("code_parametre")
        annee = props.get("annee")
        if not code or not annee:
            continue
        rk = top_ranks.get(str(annee), {}).get(str(code))
        if rk is None:
            continue
        props = dict(props)
        props["top10_ppp_annee"] = True
        props["top10_rang"] = rk
        # Réordonner les propriétés (champs utiles en premier)
        props_ordered = {k: props[k] for k in COLONNES_TOP10_ORDER if k in props}
        for k, v in props.items():
            if k not in props_ordered:
                props_ordered[k] = v
        f_out = dict(f)
        f_out["properties"] = props_ordered
        out_features.append(f_out)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": out_features}, f, ensure_ascii=False, indent=2)
    return out_path


def export_hotspots_ppp(
    sig_path: str | Path = "data/sig/analyse_stations_ppp_cote_dor.geojson",
    out_path: str | Path = "data/sig/hotspots_ppp.geojson",
) -> Path:
    """
    Construit une couche de points chauds de dépassement des seuils sanitaires :
    - agrégation par point de mesure (station / BSS) et paramètre ;
    - indicateurs : nombre de dépassements, nombre total de mesures, max_ratio, etc.
    """
    features = _load_features(sig_path)
    if not features:
        return Path(out_path)

    # Agrégation par clé (source, identifiant spatial, code_parametre)
    AggKey = Tuple[str, str, str]
    aggs: Dict[AggKey, Dict[str, Any]] = {}

    for f in features:
        props = f.get("properties") or {}
        geom = f.get("geometry")
        code_param = props.get("code_parametre")
        if not code_param:
            continue
        resultat = props.get("resultat")
        unite = props.get("symbole_unite")
        if resultat is None or unite is None:
            continue

        # Conversion en µg/L si possible
        try:
            val = float(resultat)
        except Exception:
            continue

        unite_norm = str(unite).replace("µ", "u")  # tolérance sur le symbole
        if unite_norm in ("µg/L", "ug/L"):
            conc_ugl = val
        elif unite_norm in ("mg/L",):
            conc_ugl = val * 1000.0
        else:
            # Unité inconnue pour l'instant : on ignore
            continue

        seuil = seuil_sanitaire_ugL(str(code_param), props)
        if seuil <= 0:
            continue
        ratio = conc_ugl / seuil

        source = props.get("source") or ""
        ident = (
            props.get("code_station")
            or props.get("bss_id")
            or props.get("code_bss")
            or ""
        )
        if not ident:
            continue

        key: AggKey = (str(source), str(ident), str(code_param))
        agg = aggs.get(key)
        if not agg:
            agg = {
                "source": source,
                "identifiant_point": ident,
                "code_parametre": str(code_param),
                "libelle_parametre": props.get("libelle_parametre"),
                # Champs PPP « lisibles » repris de la couche principale
                "ppp_nom": props.get("ppp_nom") or props.get("libelle_parametre"),
                "ppp_usage": props.get("ppp_usage"),
                "ppp_usages_typiques": props.get("ppp_usages_typiques"),
                "code_station": props.get("code_station"),
                "libelle_station": props.get("libelle_station"),
                "bss_id": props.get("bss_id"),
                "code_bss": props.get("code_bss"),
                "code_departement": props.get("code_departement"),
                "code_commune": props.get("code_commune"),
                "libelle_commune": props.get("libelle_commune"),
                "n_mesures": 0,
                "n_depassements": 0,
                "max_ratio": 0.0,
                "max_conc_ugl": 0.0,
                "seuil_ugl": seuil,
                # Champs PPP synthétiques alignés avec la couche principale
                "ppp_taux_ugl": 0.0,
                "ppp_seuil_sanitaire_ugl": seuil,
                "ppp_ratio_seuil": 0.0,
                "ppp_depassement": False,
                "annee_min": props.get("annee"),
                "annee_max": props.get("annee"),
                "_geometry": geom,
            }
            aggs[key] = agg

        agg["n_mesures"] += 1
        annee = props.get("annee")
        if annee is not None:
            if agg["annee_min"] is None or str(annee) < str(agg["annee_min"]):
                agg["annee_min"] = annee
            if agg["annee_max"] is None or str(annee) > str(agg["annee_max"]):
                agg["annee_max"] = annee

        if ratio > 1.0:
            agg["n_depassements"] += 1
            if ratio > agg["max_ratio"]:
                agg["max_ratio"] = ratio
                agg["max_conc_ugl"] = conc_ugl
                # Mettre à jour les champs PPP synthétiques sur la base du pire cas
                agg["ppp_taux_ugl"] = conc_ugl
                agg["ppp_seuil_sanitaire_ugl"] = seuil
                agg["ppp_ratio_seuil"] = ratio
                agg["ppp_depassement"] = True

    # On ne garde que les vrais "points chauds" (au moins un dépassement)
    out_features: list[dict[str, Any]] = []
    for agg in aggs.values():
        if agg["n_depassements"] <= 0:
            continue
        geom = agg.pop("_geometry", None)
        # Réordonner les propriétés (champs utiles en premier)
        props_ordered = {k: agg[k] for k in COLONNES_HOTSPOTS_ORDER if k in agg}
        for k, v in agg.items():
            if k not in props_ordered:
                props_ordered[k] = v
        f = {"type": "Feature", "geometry": geom, "properties": props_ordered}
        out_features.append(f)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": out_features}, f, ensure_ascii=False, indent=2)
    return out_path

