"""
Point d'entrée CLI - phytoDB : récupération et analyse PPP pour la Côte-d'Or.
Usage :
  python main.py fetch     # récupère les données C3PO (et optionnellement ventes)
  python main.py analyze   # lance l'analyse et écrit les sorties dans data/out
  python main.py run       # analyse complète : C3PO + Naïades + ADES + analyse + export SIG
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Permettre l'import des modules du projet (répertoire courant)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from c3po import fetch_all_c3po, get_c3po_resource_ids
from analysis import run_analysis
from datagouv import get_resources_for_tabular
from sources.naiades import fetch_naiades_stations_dep, fetch_naiades_analyses_dep
from sources.ades import fetch_ades_stations_dep, fetch_ades_analyses_dep
from sig import export_sig_geojson
from sig_views import export_top10_ppp_par_annee, export_hotspots_ppp
from ref_params import filter_analyses_pesticides
import yaml


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
        if getattr(args, "ventes", False):
            print("Tentative de récupération des ventes par département (API)...")
            ventes = fetch_ventes_cote_dor_from_api()
            print(f"  Ventes Côte-d'Or (API): {len(ventes)} lignes.")
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
    """Analyse complète : fetch C3PO → Naïades → ADES → analyze → export SIG."""
    root = Path(__file__).resolve().parent
    with open(root / "config.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    code_dep = config.get("departement", {}).get("code", "21")
    cache = _cache_dir(config)
    out_dir = getattr(args, "out_dir", "data/out")
    out_sig = Path(getattr(args, "out_sig", "data/sig/impact_ppp_cote_dor.geojson"))

    # 1) C3PO
    print("=== 1/5 C3PO (API tabulaire) ===")
    if cmd_fetch(args) != 0:
        return 1

    # 2) Naïades (stations + analyses, filtrées sur paramètres PPP si référentiel disponible)
    print("\n=== 2/5 Naïades (qualité cours d'eau) ===")
    try:
        naiades_stations = fetch_naiades_stations_dep(code_dep, cache_dir=cache)
        print(f"  Stations: {len(naiades_stations)}")
        naiades_analyses = []
        max_pages_naiades = getattr(args, "max_pages_naiades", 20)
        if getattr(args, "naiades_analyses", False):
            # Mode complet : on respecte max_pages_naiades
            naiades_analyses = fetch_naiades_analyses_dep(
                code_dep, cache_dir=cache, max_pages=max_pages_naiades
            )
            print(f"  Analyses Naïades brutes (max_pages={max_pages_naiades}): {len(naiades_analyses)}")
        else:
            # Par défaut : on récupère au moins un aperçu limité pour alimenter la couche SIG
            apercu_pages = min(max_pages_naiades, 5)
            naiades_analyses = fetch_naiades_analyses_dep(
                code_dep, cache_dir=cache, max_pages=apercu_pages
            )
            print(f"  Analyses Naïades brutes (aperçu, max_pages={apercu_pages}): {len(naiades_analyses)}")

        # Filtrage PPP (pesticides) si un référentiel de codes est disponible
        naiades_analyses_ppp = filter_analyses_pesticides(naiades_analyses, code_field="code_parametre")
        print(f"  Analyses Naïades pesticides retenues: {len(naiades_analyses_ppp)}")
        naiades_analyses = naiades_analyses_ppp
    except Exception as e:
        print(f"  Erreur Naïades: {e}")
        naiades_stations, naiades_analyses = [], []

    # 3) ADES (stations + analyses, filtrées sur paramètres PPP si référentiel disponible)
    print("\n=== 3/5 ADES (qualité nappes) ===")
    try:
        ades_stations = fetch_ades_stations_dep(
            code_dep, cache_dir=cache, max_pages=getattr(args, "max_pages_ades", 20)
        )
        print(f"  Stations: {len(ades_stations)}")
        ades_analyses = []
        max_pages_ades_analyses = getattr(args, "max_pages_ades_analyses", 20)
        if getattr(args, "ades_analyses", False):
            # Mode complet : on respecte max_pages_ades_analyses
            ades_analyses = fetch_ades_analyses_dep(
                code_dep, cache_dir=cache, max_pages=max_pages_ades_analyses
            )
            print(f"  Analyses ADES brutes (max_pages={max_pages_ades_analyses}): {len(ades_analyses)}")
        else:
            # Par défaut : on récupère un échantillon limité pour alimenter la couche SIG
            apercu_pages_ades = min(max_pages_ades_analyses, 3)
            ades_analyses = fetch_ades_analyses_dep(
                code_dep, cache_dir=cache, max_pages=apercu_pages_ades
            )
            print(f"  Analyses ADES brutes (aperçu, max_pages={apercu_pages_ades}): {len(ades_analyses)}")

        # Filtrage PPP (pesticides) si un référentiel de codes est disponible
        ades_analyses_ppp = filter_analyses_pesticides(ades_analyses, code_field="code_param")
        print(f"  Analyses ADES pesticides retenues: {len(ades_analyses_ppp)}")
        ades_analyses = ades_analyses_ppp
    except Exception as e:
        print(f"  Erreur ADES: {e}")
        ades_stations, ades_analyses = [], []

    # 4) Analyse (C3PO + ventes)
    print("\n=== 4/5 Analyse (C3PO + ventes) ===")
    if cmd_analyze(args) != 0:
        return 1

    # 5) Export SIG
    if not getattr(args, "no_sig", False):
        print("\n=== 5/5 Export couche SIG ===")
        try:
            path = export_sig_geojson(
                out_path=out_sig,
                naiades_stations=naiades_stations,
                naiades_analyses=naiades_analyses,
                ades_stations=ades_stations,
                ades_analyses=ades_analyses,
            )
            print(f"  Couche SIG: {path} ({path.stat().st_size // 1024} Ko)")

            # Couches dérivées pour une symbologie plus parlante
            top10_path = export_top10_ppp_par_annee(sig_path=path, out_path=Path("data/sig/top10_ppp_par_annee.geojson"))
            print(f"  Couche top10 PPP par année: {top10_path} ({top10_path.stat().st_size // 1024} Ko)")
            hotspots_path = export_hotspots_ppp(sig_path=path, out_path=Path("data/sig/hotspots_ppp.geojson"))
            print(f"  Couche points chauds PPP: {hotspots_path} ({hotspots_path.stat().st_size // 1024} Ko)")
        except Exception as e:
            print(f"  Erreur export SIG: {e}")
    else:
        print("\n=== 5/5 Export SIG (ignoré avec --no-sig) ===")

    print("\n--- Analyse complète terminée ---")
    print(f"  Sorties: {out_dir}/")
    if not getattr(args, "no_sig", False):
        print(f"  SIG: {out_sig}")
    return 0


def cmd_list_resources(args) -> int:
    """Liste les ressources du jeu C3PO (pour récupérer les RIDs à mettre dans config)."""
    with open(Path(__file__).resolve().parent / "config.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
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


def _cache_dir(config: dict) -> Path | None:
    c = config.get("cache", {})
    if c.get("enabled"):
        return Path(c.get("dir", "data/cache"))
    return None


def cmd_fetch_naiades(args) -> int:
    """Récupère les données Naïades (qualité eaux de surface) pour le dép. 21."""
    with open(Path(__file__).resolve().parent / "config.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
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
    with open(Path(__file__).resolve().parent / "config.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
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
    with open(Path(__file__).resolve().parent / "config.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    code_dep = config.get("departement", {}).get("code", "21")
    cache = Path(config.get("cache", {}).get("dir", "data/cache"))
    out = Path(getattr(args, "out", "data/sig/impact_ppp_cote_dor.geojson"))

    naiades_stations, naiades_analyses, ades_stations, ades_analyses = [], [], [], []

    use_naiades = not getattr(args, "no_naiades", False)
    use_ades = not getattr(args, "no_ades", False)

    def load_json(path: Path) -> list:
        if not path.exists():
            return []
        import json
        return json.loads(path.read_text(encoding="utf-8"))

    if use_naiades:
        naiades_stations = load_json(cache / "naiades_stations_21.json")
        if not naiades_stations:
            print("Pas de cache Naïades stations. Lancement fetch...")
            naiades_stations = fetch_naiades_stations_dep(code_dep, cache_dir=cache)
        naiades_analyses = load_json(cache / "naiades_analyses_21.json")
        if not naiades_analyses and not getattr(args, "no_fetch_analyses", False):
            print("Pas de cache Naïades analyses (paramètres PPP). Récupération limitée (5 pages)...")
            naiades_analyses = fetch_naiades_analyses_dep(code_dep, cache_dir=cache, max_pages=5)
        # Filtre PPP si référentiel disponible
        naiades_analyses_ppp = filter_analyses_pesticides(naiades_analyses, code_field="code_parametre")
        print(f"Analyses Naïades pesticides retenues (export-sig): {len(naiades_analyses_ppp)}")
        naiades_analyses = naiades_analyses_ppp
    if use_ades:
        ades_stations = load_json(cache / "ades_stations_21.json")
        if not ades_stations:
            print("Pas de cache ADES stations. Lancement fetch...")
            ades_stations = fetch_ades_stations_dep(code_dep, cache_dir=cache, max_pages=50)
        ades_analyses = load_json(cache / "ades_analyses_21.json")
        if not ades_analyses and not getattr(args, "no_fetch_analyses", False):
            print("Pas de cache ADES analyses (paramètres PPP). Récupération limitée (3 pages)...")
            ades_analyses = fetch_ades_analyses_dep(code_dep, cache_dir=cache, max_pages=3)
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
    print(f"Couche SIG exportée: {path} ({path.stat().st_size // 1024} Ko)")
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
    p_run.add_argument("--out-sig", default="data/sig/impact_ppp_cote_dor.geojson", help="Fichier GeoJSON couche SIG")
    p_run.add_argument("--naiades-analyses", action="store_true", help="Inclure les analyses Naïades (volume important)")
    p_run.add_argument("--max-pages-naiades", type=int, default=20, help="Max pages analyses Naïades")
    p_run.add_argument("--ades-analyses", action="store_true", help="Inclure les analyses ADES")
    p_run.add_argument("--max-pages-ades", type=int, default=20, help="Max pages stations ADES")
    p_run.add_argument("--max-pages-ades-analyses", type=int, default=20, help="Max pages analyses ADES")
    p_run.add_argument("--no-sig", action="store_true", help="Ne pas exporter la couche SIG")
    p_run.set_defaults(func=cmd_run)

    p_list = sub.add_parser("list-resources", help="Lister les RIDs des ressources C3PO")
    p_list.set_defaults(func=cmd_list_resources)

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
    p_sig.add_argument("--out", default="data/sig/impact_ppp_cote_dor.geojson", help="Fichier GeoJSON de sortie")
    p_sig.add_argument("--no-naiades", action="store_true", dest="no_naiades", help="Exclure Naïades")
    p_sig.add_argument("--no-ades", action="store_true", dest="no_ades", help="Exclure ADES")
    p_sig.add_argument("--no-fetch-analyses", action="store_true", dest="no_fetch_analyses",
                        help="Ne pas récupérer les analyses si cache vide (couche sans paramètres PPP)")
    p_sig.set_defaults(func=cmd_export_sig)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
