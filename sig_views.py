"""
Vues dérivées de la couche SIG principale pour faciliter la lecture par l'utilisateur :

- top10_ppp_par_annee.geojson : analyses des 10 PPP les plus détectés par année ;
- hotspots_ppp.geojson : points chauds de dépassement des seuils sanitaires/réglementaires ;
- agregations_ppp_par_annee.csv : agrégation station × paramètre × année (nb prélèvements, concentration moyenne µg/L).
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Tuple

COLONNES_TOP10_ORDER = [
    "substance", "usage_ppp", "amm_autorise", "concentration_ugl", "depassement_seuil_sanitaire",
    "ratio_seuil_sanitaire", "depassement_seuil_nqe",
    "top10_ppp_annee", "top10_rang",
    "lieu", "commune", "cours_eau", "masse_eau", "date_prelevement", "type_eau", "lien_fiche",
]

COLONNES_HOTSPOTS_ORDER = [
    "substance", "usage_ppp", "amm_autorise", "n_depassements", "n_mesures",
    "depassement_seuil_nqe",
    "max_conc_ugl", "concentration_ugl", "depassement_seuil_sanitaire",
    "ratio_seuil_sanitaire",
    "taille_mm",
    "taille_inner_mm",
    "classe_taille",
    "type_depassement",
    "date_prelevement",
    "lieu", "commune", "cours_eau", "masse_eau", "type_eau", "lien_fiche",
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
    Construit une couche de points chauds de dépassement :
    - seuils sanitaires (0,1 µg/L ou référentiel) ;
    - NQE réglementaires (NQE-MA, NQE-CMA, source Ecophyto 2030).
    Agrégation par point de mesure (station / BSS) et paramètre.
    """
    features = _load_features(sig_path)
    if not features:
        return Path(out_path)

    # Agrégation par clé (type_eau, lieu, substance)
    AggKey = Tuple[str, str, str]
    aggs: Dict[AggKey, Dict[str, Any]] = {}

    for f in features:
        props = f.get("properties") or {}
        geom = f.get("geometry")
        substance = props.get("substance")
        type_eau = props.get("type_eau") or ""
        lieu = props.get("lieu") or ""
        if not substance or not lieu:
            continue

        key: AggKey = (str(type_eau), str(lieu), str(substance).strip())
        agg = aggs.get(key)
        if not agg:
            agg = {
                "type_eau": type_eau,
                "lieu": lieu,
                "substance": substance,
                "usage_ppp": props.get("usage_ppp"),
                "amm_autorise": props.get("amm_autorise"),
                "commune": props.get("commune"),
                "cours_eau": props.get("cours_eau"),
                "masse_eau": props.get("masse_eau"),
                "lien_fiche": props.get("lien_fiche"),
                "n_mesures": 0,
                "n_depassements": 0,
                "depassement_seuil_nqe": False,
                "max_conc_ugl": 0.0,
                "concentration_ugl": 0.0,
                "depassement_seuil_sanitaire": False,
                "max_ratio_seuil_sanitaire": None,
                "annee_min": None,
                "annee_max": None,
                "date_prelevement": None,
                "_geometry": geom,
            }
            aggs[key] = agg

        date_prel = props.get("date_prelevement")
        annee = str(date_prel)[:4] if date_prel else None

        if props.get("depassement_seuil_nqe") == "oui":
            agg["depassement_seuil_nqe"] = True

        conc_ugl = props.get("concentration_ugl")
        ratio_sanitaire = props.get("ratio_seuil_sanitaire")
        if ratio_sanitaire is not None:
            try:
                r = float(ratio_sanitaire)
                if agg["max_ratio_seuil_sanitaire"] is None or r > agg["max_ratio_seuil_sanitaire"]:
                    agg["max_ratio_seuil_sanitaire"] = r
                    agg["date_prelevement"] = date_prel
            except (TypeError, ValueError):
                pass
        if conc_ugl is not None:
            try:
                c = float(conc_ugl)
                agg["n_mesures"] += 1
                if annee is not None:
                    if agg["annee_min"] is None or annee < agg["annee_min"]:
                        agg["annee_min"] = annee
                    if agg["annee_max"] is None or annee > agg["annee_max"]:
                        agg["annee_max"] = annee
                if c > agg["max_conc_ugl"]:
                    agg["max_conc_ugl"] = c
                    agg["concentration_ugl"] = c
                    if agg["date_prelevement"] is None:
                        agg["date_prelevement"] = date_prel
                if props.get("depassement_seuil_sanitaire") == "oui":
                    agg["n_depassements"] += 1
                    agg["depassement_seuil_sanitaire"] = True
            except (TypeError, ValueError):
                pass

    # Points chauds : dépassement seuil sanitaire OU NQE
    out_features: list[dict[str, Any]] = []
    for agg in aggs.values():
        if agg["n_depassements"] <= 0 and not agg["depassement_seuil_nqe"]:
            continue
        max_ratio = agg.pop("max_ratio_seuil_sanitaire", None)
        ratio = round(max_ratio, 2) if max_ratio is not None else None
        agg["ratio_seuil_sanitaire"] = ratio
        # Taille en mm pour la symbologie QGIS (4–12 mm) : évite les expressions dans le QML
        r = max_ratio if max_ratio is not None else 1.0
        part = min(max(r - 1, 0.0), 9.0)
        bonus_nqe = 1.5 if agg["depassement_seuil_nqe"] else 0.0
        agg["taille_mm"] = round(4 + 0.75 * part + bonus_nqe, 1)
        agg["taille_inner_mm"] = round(max(agg["taille_mm"] - 1.5, 1.5), 1)
        # Classe de taille 1–4 pour règles QGIS à taille fixe (évite propriétés dérivées)
        t = agg["taille_mm"]
        if t >= 9.5:
            agg["classe_taille"] = 4
        elif t >= 7.5:
            agg["classe_taille"] = 3
        elif t >= 5.5:
            agg["classe_taille"] = 2
        else:
            agg["classe_taille"] = 1
        # Type de dépassement : 1 = NQE+sanitaire, 2 = NQE seul, 3 = sanitaire seul
        if agg["depassement_seuil_sanitaire"] and agg["depassement_seuil_nqe"]:
            agg["type_depassement"] = 1
        elif agg["depassement_seuil_nqe"]:
            agg["type_depassement"] = 2
        else:
            agg["type_depassement"] = 3
        geom = agg.pop("_geometry", None)
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


def export_agregations_ppp_par_annee(
    sig_path: str | Path = "data/sig/analyse_stations_ppp_cote_dor.geojson",
    out_path: str | Path = "data/sig/agregations_ppp_par_annee.csv",
) -> Path:
    """
    Exporte un CSV d'agrégation par (type_eau, lieu, substance, année) :
    nombre de prélèvements et concentration moyenne en µg/L.
    """
    features = _load_features(sig_path)
    if not features:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow([
                "type_eau", "lieu", "commune", "substance", "usage_ppp", "annee",
                "n_prelevements", "concentration_moyenne_ugl",
            ])
        return out_path

    # Clé : (type_eau, lieu, substance, annee)
    AggKey = Tuple[str, str, str, str]
    aggs: Dict[AggKey, Dict[str, Any]] = {}

    for f in features:
        props = f.get("properties") or {}
        type_eau = props.get("type_eau") or ""
        lieu = props.get("lieu") or ""
        substance = props.get("substance")
        date_prel = props.get("date_prelevement")
        annee = str(date_prel)[:4] if date_prel else None
        if not lieu or not substance or not annee:
            continue
        annee = annee.strip()[:4]

        key: AggKey = (str(type_eau), str(lieu), str(substance).strip(), annee)
        if key not in aggs:
            aggs[key] = {
                "type_eau": type_eau,
                "lieu": lieu,
                "commune": props.get("commune"),
                "substance": substance,
                "usage_ppp": props.get("usage_ppp"),
                "annee": annee,
                "n_prelevements": 0,
                "sum_ugl": 0.0,
                "count_ugl": 0,
            }
        agg = aggs[key]
        agg["n_prelevements"] += 1
        conc = props.get("concentration_ugl")
        if conc is not None:
            try:
                agg["sum_ugl"] += float(conc)
                agg["count_ugl"] += 1
            except (TypeError, ValueError):
                pass

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([
            "type_eau", "lieu", "commune", "substance", "usage_ppp", "annee",
            "n_prelevements", "concentration_moyenne_ugl",
        ])
        for agg in sorted(aggs.values(), key=lambda x: (x["annee"], x["type_eau"], x["lieu"] or "", x["substance"])):
            moy = (agg["sum_ugl"] / agg["count_ugl"]) if agg["count_ugl"] else ""
            w.writerow([
                agg["type_eau"] or "",
                agg["lieu"] or "",
                agg["commune"] or "",
                agg["substance"] or "",
                agg["usage_ppp"] or "",
                agg["annee"],
                agg["n_prelevements"],
                f"{moy:.4f}" if isinstance(moy, float) else moy,
            ])
    return out_path

