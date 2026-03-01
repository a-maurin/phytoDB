"""
Point d'entrée CLI - phytoDB : récupération et analyse PPP pour la Côte-d'Or.
Usage :
  python main.py fetch     # récupère les données C3PO
  python main.py analyze   # lance l'analyse C3PO et écrit les sorties dans data/out
  python main.py run       # analyse complète : C3PO + Naïades + ADES + analyse + export SIG (fetch en parallèle)
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

# Permettre l'import des modules du projet (répertoire courant)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from c3po import fetch_all_c3po, fetch_all_c3po_async, get_c3po_resource_ids
from analysis import run_analysis, stats_prelevements_par_annee
from datagouv import get_resources_for_tabular
from sources.naiades import (
    fetch_naiades_stations_dep,
    fetch_naiades_analyses_dep,
    fetch_naiades_stations_dep_async,
    fetch_naiades_analyses_dep_async,
)
from sources.ades import (
    fetch_ades_stations_dep,
    fetch_ades_analyses_dep,
    fetch_ades_stations_dep_async,
    fetch_ades_analyses_dep_async,
)
from sig import export_sig_geojson
from sig_styles import write_sig_styles
from sig_views import export_top10_ppp_par_annee, export_hotspots_ppp, export_agregations_ppp_par_annee
from ref_params import filter_analyses_pesticides
from ppp_dict import build_ppp_usages_from_sources_dictionnaire
from config import load_config, get_cache_dir, get_code_departement, cache_path, resolve_path


def cmd_fetch(args) -> int:
    """Récupère les données C3PO et les met en cache."""
    print("Récupération des données C3PO via l'API tabulaire...")
    rids = get_c3po_resource_ids()
    if not rids:
        print("Aucun resource_id C3PO configuré dans config.yaml (datasets.c3po.resource_ids).")
        return 1
    try:
        data = fetch_all_c3po(cache=not args.no_cache)
        for rid_short, rows in data.items():
            print(f"  Ressource {rid_short}: {len(rows)} lignes.")
        return 0
    except Exception as e:
        print(f"Erreur: {e}")
        return 1


def cmd_analyze(args) -> int:
    """Lance l'analyse et produit les sorties dans data/out."""
    print("Lancement de l'analyse (Côte-d'Or, dép. 21)...")
    try:
        result = run_analysis(out_dir=getattr(args, "out_dir", "data/out"))
        print("Résumé:")
        print(f"  Département: {result['departement']['code']} - {result['departement']['nom']}")
        print(f"  Substances C3PO chargées: {result['c3po']['nombre_substances']}")
        if result.get("analyse") and result["analyse"].get("resume"):
            for k, v in result["analyse"]["resume"].items():
                print(f"  {k}: {v}")
        print("Sorties écrites dans data/out/")
        return 0
    except Exception as e:
        print(f"Erreur: {e}")
        return 1


def cmd_run(args) -> int:
    """Analyse complète : fetch C3PO, Naïades et ADES en parallèle (async), puis analyse + export SIG."""
    config = load_config()
    return asyncio.run(_run_async(args, config))


async def _run_async(args: argparse.Namespace, config: dict) -> int:
    """Coroutine exécutant fetch C3PO, Naïades et ADES en parallèle puis le reste en séquentiel."""
    code_dep = config.get("departement", {}).get("code", "21")
    cache = _cache_dir(config)
    out_dir = getattr(args, "out_dir", "data/out")
    out_sig = Path(getattr(args, "out_sig", "data/sig/analyse_stations_ppp_cote_dor.geojson"))
    max_pages_naiades = getattr(args, "max_pages_naiades", 20)
    max_pages_ades = getattr(args, "max_pages_ades", 20)
    max_pages_ades_analyses = getattr(args, "max_pages_ades_analyses", 20)
    # Aperçu : suffisamment de pages pour inclure les données les plus récentes (date_fin = aujourd'hui)
    apercu_naiades = min(max_pages_naiades, 15) if not getattr(args, "naiades_analyses", False) else max_pages_naiades
    apercu_ades = min(max_pages_ades_analyses, 10) if not getattr(args, "ades_analyses", False) else max_pages_ades_analyses

    # 1) Fetch en parallèle : C3PO, Naïades (stations + analyses), ADES (stations + analyses)
    print("=== 1/5 Fetch en parallèle (C3PO, Naïades, ADES) ===")
    rids = get_c3po_resource_ids()
    if not rids:
        print("Aucun resource_id C3PO configuré. Lancez sans async ou configurez config.yaml.")
        return 1

    async def do_c3po():
        data = await fetch_all_c3po_async(cache=not getattr(args, "no_cache", False))
        for rid_short, rows in data.items():
            print(f"  C3PO {rid_short}: {len(rows)} lignes.")
        return data

    async def do_naiades():
        stations = await fetch_naiades_stations_dep_async(code_dep, cache_dir=cache)
        analyses = await fetch_naiades_analyses_dep_async(
            code_dep, cache_dir=cache, max_pages=apercu_naiades
        )
        print(f"  Naïades: {len(stations)} stations, {len(analyses)} analyses brutes.")
        return (stations, analyses)

    async def do_ades():
        stations = await fetch_ades_stations_dep_async(
            code_dep, cache_dir=cache, max_pages=max_pages_ades
        )
        analyses = await fetch_ades_analyses_dep_async(
            code_dep, cache_dir=cache, max_pages=apercu_ades
        )
        print(f"  ADES: {len(stations)} stations, {len(analyses)} analyses brutes.")
        return (stations, analyses)

    try:
        _, (naiades_stations, naiades_analyses), (ades_stations, ades_analyses) = await asyncio.gather(
            do_c3po(), do_naiades(), do_ades()
        )
    except Exception as e:
        import traceback
        msg = str(e) or "(aucun message)"
        print(f"Erreur fetch: {type(e).__name__}: {msg}")
        traceback.print_exc()
        return 1

    # Filtrage PPP
    naiades_analyses = filter_analyses_pesticides(naiades_analyses, code_field="code_parametre")
    print(f"  Analyses Naïades pesticides retenues: {len(naiades_analyses)}")
    ades_analyses = filter_analyses_pesticides(ades_analyses, code_field="code_param")
    print(f"  Analyses ADES pesticides retenues: {len(ades_analyses)}")

    # 2) Analyse (C3PO)
    print("\n=== 2/5 Analyse (C3PO) ===")
    if cmd_analyze(args) != 0:
        return 1

    # 3) Export SIG
    if not getattr(args, "no_sig", False):
        print("\n=== 3/5 Export couche SIG ===")
        try:
            path = export_sig_geojson(
                out_path=out_sig,
                naiades_stations=naiades_stations,
                naiades_analyses=naiades_analyses,
                ades_stations=ades_stations,
                ades_analyses=ades_analyses,
            )
            write_sig_styles(out_sig.parent)
            print(f"  Couche SIG: {path} ({path.stat().st_size // 1024} Ko)")
            if getattr(args, "top10", False):
                top10_path = export_top10_ppp_par_annee(sig_path=path, out_path=Path("data/sig/top10_ppp_par_annee.geojson"))
                print(f"  Couche top10 PPP par année: {top10_path} ({top10_path.stat().st_size // 1024} Ko)")
            hotspots_path = export_hotspots_ppp(sig_path=path, out_path=Path("data/sig/hotspots_ppp.geojson"))
            print(f"  Couche points chauds PPP: {hotspots_path} ({hotspots_path.stat().st_size // 1024} Ko)")
            agg_path = export_agregations_ppp_par_annee(sig_path=path, out_path=Path("data/sig/agregations_ppp_par_annee.csv"))
            print(f"  Agrégations (CSV): {agg_path} ({agg_path.stat().st_size // 1024} Ko)")
        except Exception as e:
            print(f"  Erreur export SIG: {e}")
    else:
        print("\n=== 3/5 Export SIG (ignoré avec --no-sig) ===")

    print("\n--- Analyse complète terminée ---")
    print(f"  Sorties: {out_dir}/")
    if not getattr(args, "no_sig", False):
        print(f"  SIG: {out_sig}")
    return 0


def cmd_list_resources(args) -> int:
    """Liste les ressources du jeu C3PO (pour récupérer les RIDs à mettre dans config)."""
    config = load_config()
    c3po_id = config.get("datasets", {}).get("c3po", {}).get("id")
    if not c3po_id:
        print("datasets.c3po.id non renseigné dans config.yaml")
        return 1
    try:
        resources = get_resources_for_tabular(c3po_id)
        print(f"Ressources du jeu C3PO (id={c3po_id}) éligibles à l'API tabulaire:")
        for r in resources:
            print(f"  RID: {r.get('id')}  titre: {r.get('title')}  format: {r.get('format')}")
        return 0
    except Exception as e:
        print(f"Erreur: {e}")
        return 1


def cmd_build_ppp_dict(args) -> int:
    """Construit data/ref/ppp_usages.csv à partir de sources/sources_dictionnaire (C3PO/e-Phy)."""
    root = Path(__file__).resolve().parent
    sources_dir = getattr(args, "sources_dir", None)
    if sources_dir:
        sources_dir = Path(sources_dir)
        if not sources_dir.is_absolute():
            sources_dir = root / sources_dir
    out = getattr(args, "out", None)
    try:
        path = build_ppp_usages_from_sources_dictionnaire(
            sources_dir=sources_dir,
            output_path=out,
        )
        print(f"Dictionnaire PPP généré : {path} ({len(path.read_text(encoding='utf-8').splitlines()) - 1} entrées)")
        return 0
    except FileNotFoundError as e:
        print(f"Erreur: {e}")
        return 1
    except Exception as e:
        print(f"Erreur: {e}")
        return 1


def _filter_10_dernieres_annees(
    analyses: list[dict], date_field: str = "date_prelevement"
) -> list[dict]:
    """Filtre les analyses pour ne garder que les 10 dernières années (à partir du jour)."""
    seuil = (date.today() - timedelta(days=365 * 10)).isoformat()
    return [a for a in analyses if (a.get(date_field) or a.get("date_debut_prelevement") or "")[:10] >= seuil[:10]]


def _cache_dir(config: dict | None = None) -> Path | None:
    return get_cache_dir(config)


def cmd_fetch_naiades(args) -> int:
    """Récupère les données Naïades (qualité eaux de surface) pour le dép. 21."""
    config = load_config()
    code_dep = config.get("departement", {}).get("code", "21")
    cache = _cache_dir(config)
    print(f"Récupération Naïades (Hub'Eau) pour le département {code_dep}...")
    try:
        stations = fetch_naiades_stations_dep(code_dep, cache_dir=cache)
        print(f"  Stations: {len(stations)}")
        if getattr(args, "analyses", False):
            max_p = getattr(args, "max_pages_analyses", 20)
            analyses = fetch_naiades_analyses_dep(code_dep, cache_dir=cache, max_pages=max_p)
            print(f"  Analyses Naïades brutes (max_pages={max_p}): {len(analyses)}")
            analyses_ppp = filter_analyses_pesticides(analyses, code_field="code_parametre")
            print(f"  Analyses Naïades pesticides retenues: {len(analyses_ppp)}")
        return 0
    except Exception as e:
        print(f"Erreur: {e}")
        return 1


def cmd_fetch_ades(args) -> int:
    """Récupère les données ADES (qualité nappes) pour le dép. 21."""
    config = load_config()
    code_dep = config.get("departement", {}).get("code", "21")
    cache = _cache_dir(config)
    print(f"Récupération ADES (Hub'Eau) pour le département {code_dep}...")
    try:
        stations = fetch_ades_stations_dep(code_dep, cache_dir=cache, max_pages=getattr(args, "max_pages", 100))
        print(f"  Stations: {len(stations)}")
        if getattr(args, "analyses", False):
            max_p = getattr(args, "max_pages_analyses", 20)
            analyses = fetch_ades_analyses_dep(code_dep, cache_dir=cache, max_pages=max_p)
            print(f"  Analyses ADES brutes (max_pages={max_p}): {len(analyses)}")
            analyses_ppp = filter_analyses_pesticides(analyses, code_field="code_param")
            print(f"  Analyses ADES pesticides retenues: {len(analyses_ppp)}")
        return 0
    except Exception as e:
        print(f"Erreur: {e}")
        return 1


def cmd_export_sig(args) -> int:
    """Génère la couche SIG (GeoJSON) à partir des données Naïades/ADES (cache ou fetch)."""
    config = load_config()
    code_dep = get_code_departement(config)
    cache = get_cache_dir(config) or resolve_path("data/cache")
    out = Path(getattr(args, "out", "data/sig/analyse_stations_ppp_cote_dor.geojson"))

    naiades_stations, naiades_analyses, ades_stations, ades_analyses = [], [], [], []

    use_naiades = not getattr(args, "no_naiades", False)
    use_ades = not getattr(args, "no_ades", False)

    def load_json(path: Path) -> list:
        if not path.exists():
            return []
        import json
        return json.loads(path.read_text(encoding="utf-8"))

    if use_naiades:
        naiades_stations = load_json(cache_path(cache, "naiades_stations", code_dep))
        if not naiades_stations:
            print("Pas de cache Naïades stations. Lancement fetch...")
            naiades_stations = fetch_naiades_stations_dep(code_dep, cache_dir=cache)
        naiades_analyses = _filter_10_dernieres_annees(
            load_json(cache_path(cache, "naiades_analyses", code_dep)), date_field="date_prelevement"
        )
        if not naiades_analyses and not getattr(args, "no_fetch_analyses", False):
            print("Pas de cache Naïades analyses (paramètres PPP). Récupération jusqu'aux données les plus récentes...")
            naiades_analyses = fetch_naiades_analyses_dep(code_dep, cache_dir=cache, max_pages=15)
        # Filtre PPP si référentiel disponible
        naiades_analyses_ppp = filter_analyses_pesticides(naiades_analyses, code_field="code_parametre")
        print(f"Analyses Naïades pesticides retenues (export-sig): {len(naiades_analyses_ppp)}")
        naiades_analyses = naiades_analyses_ppp
    if use_ades:
        ades_stations = load_json(cache_path(cache, "ades_stations", code_dep))
        if not ades_stations:
            print("Pas de cache ADES stations. Lancement fetch...")
            ades_stations = fetch_ades_stations_dep(code_dep, cache_dir=cache, max_pages=50)
        ades_analyses = _filter_10_dernieres_annees(
            load_json(cache_path(cache, "ades_analyses", code_dep)), date_field="date_debut_prelevement"
        )
        if not ades_analyses and not getattr(args, "no_fetch_analyses", False):
            print("Pas de cache ADES analyses (paramètres PPP). Récupération jusqu'aux données les plus récentes...")
            ades_analyses = fetch_ades_analyses_dep(code_dep, cache_dir=cache, max_pages=10)
        # Filtre PPP si référentiel disponible
        ades_analyses_ppp = filter_analyses_pesticides(ades_analyses, code_field="code_param")
        print(f"Analyses ADES pesticides retenues (export-sig): {len(ades_analyses_ppp)}")
        ades_analyses = ades_analyses_ppp

    path = export_sig_geojson(
        out_path=out,
        naiades_stations=naiades_stations,
        naiades_analyses=naiades_analyses,
        ades_stations=ades_stations,
        ades_analyses=ades_analyses,
    )
    write_sig_styles(out.parent)
    print(f"Couche SIG exportée: {path} ({path.stat().st_size // 1024} Ko)")
    hotspots_path = export_hotspots_ppp(sig_path=path, out_path=out.parent / "hotspots_ppp.geojson")
    print(f"Couche points chauds: {hotspots_path} ({hotspots_path.stat().st_size // 1024} Ko)")
    agg_path = export_agregations_ppp_par_annee(sig_path=path, out_path=out.parent / "agregations_ppp_par_annee.csv")
    print(f"Agrégations (CSV): {agg_path} ({agg_path.stat().st_size // 1024} Ko)")
    return 0


def cmd_stats_annees(args) -> int:
    """Affiche le classement par année du nombre de prélèvements (analyses) en Côte-d'Or."""
    config = load_config()
    cache_dir = getattr(args, "cache_dir", None) or (get_cache_dir(config) or resolve_path("data/cache"))
    cache_dir = Path(cache_dir) if not isinstance(cache_dir, Path) else cache_dir
    sig_path = getattr(args, "sig", None)
    if sig_path and not Path(sig_path).is_absolute():
        sig_path = resolve_path(str(sig_path))

    stats = stats_prelevements_par_annee(cache_dir=cache_dir, sig_path=sig_path)
    if stats["total"] == 0:
        print("Aucune donnée disponible. Lancer d'abord :")
        print("  python main.py run --naiades-analyses --ades-analyses")
        print("  ou  python main.py export-sig  (sans --no-fetch-analyses)")
        print("pour générer le cache ou la couche SIG, puis relancer stats-annees.")
        return 1

    print("Prélèvements (analyses PPP) en Côte-d'Or — classement par année")
    print("Source des données:", stats["source"])
    print("Total:", stats["total"], "analyses")
    print()
    annees = sorted(stats["par_annee"].keys())
    for annee in annees:
        n = stats["par_annee"][annee]
        n_na = stats.get("par_annee_naïades", {}).get(annee, 0)
        n_ad = stats.get("par_annee_ades", {}).get(annee, 0)
        detail = ""
        if n_na or n_ad:
            detail = f"  (Naïades: {n_na}, ADES: {n_ad})"
        print(f"  {annee} : {n} analyses{detail}")
    # Ventilation par usage (famille) lorsque disponible (données depuis GeoJSON)
    par_usage = stats.get("par_annee_usage")
    if par_usage:
        print()
        print("Par année et usage (famille PPP) :")
        for annee in sorted(par_usage.keys()):
            usages = par_usage[annee]
            parts = [f"    {k}: {v}" for k, v in sorted(usages.items(), key=lambda x: -x[1])]
            print(f"  {annee} :")
            print("\n".join(parts))
    return 0


def cmd_export_agregations(args) -> int:
    """Exporte le CSV d'agrégations (station × paramètre × année)."""
    root = Path(__file__).resolve().parent
    sig_path = getattr(args, "sig", None) or root / "data" / "sig" / "analyse_stations_ppp_cote_dor.geojson"
    out_path = Path(getattr(args, "out", "data/sig/agregations_ppp_par_annee.csv"))
    if not out_path.is_absolute():
        out_path = root / out_path
    if not Path(sig_path).is_absolute():
        sig_path = root / sig_path
    p = export_agregations_ppp_par_annee(sig_path=sig_path, out_path=out_path)
    print(f"Agrégations exportées: {p} ({p.stat().st_size} octets)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="phytoDB - Analyse PPP Côte-d'Or")
    sub = parser.add_subparsers(dest="command", required=True)

    p_fetch = sub.add_parser("fetch", help="Récupérer les données C3PO (cache local)")
    p_fetch.add_argument("--no-cache", action="store_true", help="Désactiver le cache")
    p_fetch.set_defaults(func=cmd_fetch)

    p_analyze = sub.add_parser("analyze", help="Lancer l'analyse C3PO et écrire les sorties")
    p_analyze.add_argument("--out-dir", default="data/out", help="Répertoire de sortie")
    p_analyze.set_defaults(func=cmd_analyze)

    p_run = sub.add_parser("run", help="Analyse complète : C3PO + Naïades + ADES + analyse + export SIG")
    p_run.add_argument("--no-cache", action="store_true", help="Désactiver le cache")
    p_run.add_argument("--out-dir", default="data/out", help="Répertoire de sortie analyse")
    p_run.add_argument("--out-sig", default="data/sig/analyse_stations_ppp_cote_dor.geojson", help="Fichier GeoJSON couche SIG")
    p_run.add_argument("--naiades-analyses", action="store_true", help="Inclure les analyses Naïades (volume important)")
    p_run.add_argument("--max-pages-naiades", type=int, default=20, help="Max pages analyses Naïades")
    p_run.add_argument("--ades-analyses", action="store_true", help="Inclure les analyses ADES")
    p_run.add_argument("--max-pages-ades", type=int, default=20, help="Max pages stations ADES")
    p_run.add_argument("--max-pages-ades-analyses", type=int, default=20, help="Max pages analyses ADES")
    p_run.add_argument("--no-sig", action="store_true", help="Ne pas exporter la couche SIG")
    p_run.add_argument("--top10", action="store_true", help="Exporter aussi la couche top10 PPP par année")
    p_run.set_defaults(func=cmd_run)

    p_list = sub.add_parser("list-resources", help="Lister les RIDs des ressources C3PO")
    p_list.set_defaults(func=cmd_list_resources)

    p_build_dict = sub.add_parser("build-ppp-dict", help="Construire data/ref/ppp_usages.csv depuis sources_dictionnaire (C3PO/e-Phy)")
    p_build_dict.add_argument("--sources-dir", default=None, help="Répertoire des CSV (défaut: config ref.ppp_usages.sources_dictionnaire)")
    p_build_dict.add_argument("--out", default=None, help="Fichier CSV de sortie (défaut: data/ref/ppp_usages.csv)")
    p_build_dict.set_defaults(func=cmd_build_ppp_dict)

    p_naiades = sub.add_parser("fetch-naiades", help="Récupérer les données Naïades (qualité cours d'eau)")
    p_naiades.add_argument("--analyses", action="store_true", help="Récupérer aussi les analyses (volume important)")
    p_naiades.add_argument("--max-pages-analyses", type=int, default=20, help="Max pages pour les analyses")
    p_naiades.set_defaults(func=cmd_fetch_naiades)

    p_ades = sub.add_parser("fetch-ades", help="Récupérer les données ADES (qualité nappes)")
    p_ades.add_argument("--analyses", action="store_true", help="Récupérer aussi les analyses")
    p_ades.add_argument("--max-pages", type=int, default=100, help="Max pages pour les stations")
    p_ades.add_argument("--max-pages-analyses", type=int, default=20, help="Max pages pour les analyses")
    p_ades.set_defaults(func=cmd_fetch_ades)

    p_sig = sub.add_parser("export-sig", help="Exporter la couche SIG (GeoJSON) pour QGIS/ArcGIS")
    p_sig.add_argument("--out", default="data/sig/analyse_stations_ppp_cote_dor.geojson", help="Fichier GeoJSON de sortie")
    p_sig.add_argument("--no-naiades", action="store_true", dest="no_naiades", help="Exclure Naïades")
    p_sig.add_argument("--no-ades", action="store_true", dest="no_ades", help="Exclure ADES")
    p_sig.add_argument("--no-fetch-analyses", action="store_true", dest="no_fetch_analyses",
                        help="Ne pas récupérer les analyses si cache vide (couche sans paramètres PPP)")
    p_sig.set_defaults(func=cmd_export_sig)

    p_stats = sub.add_parser("stats-annees", help="Afficher le classement par année du nombre de prélèvements (Côte-d'Or)")
    p_stats.add_argument("--cache-dir", default=None, help="Répertoire cache (défaut: config cache.dir)")
    p_stats.add_argument("--sig", default=None, help="Fichier GeoJSON couche SIG (si pas de cache)")
    p_stats.set_defaults(func=cmd_stats_annees)

    p_agg = sub.add_parser("export-agregations", help="Exporter les agrégations station × paramètre × année (CSV)")
    p_agg.add_argument("--sig", default=None, help="Fichier GeoJSON couche SIG")
    p_agg.add_argument("--out", default="data/sig/agregations_ppp_par_annee.csv", help="Fichier CSV de sortie")
    p_agg.set_defaults(func=cmd_export_agregations)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
