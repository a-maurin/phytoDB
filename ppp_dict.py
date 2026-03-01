"""
Dictionnaire d'usages PPP (type et usages typiques) par substance / paramètre.

Objectif :
- permettre de renseigner de façon fiable les champs `ppp_usage` et `ppp_usages_typiques`
  à partir de sources de référence (E-phy, BNVD, autres bases officielles) préparées
  sous forme de CSV simple ;
- surcharger ou compléter la détection heuristique basée sur les libellés C3PO.

Le fichier CSV attendu est configurable via `config.yaml`, section :

ref:
  ppp_usages:
    file: data/ref/ppp_usages.csv

Format minimal du CSV (séparateur `;` ou `,`) :
- `code_parametre` : code paramètre Sandre (ex. 1105 pour Aminotriazole) ;
- `ppp_usage` : texte court (herbicide, fongicide, insecticide, etc.) ;
- `ppp_usages_typiques` : texte libre décrivant les usages typiques.

On peut aussi, en complément, fournir des lignes indexées par CAS :
- `cas` : numéro CAS de la substance ;

La priorité d'utilisation est :
1) correspondance par `code_parametre` si présent ;
2) à défaut, correspondance par `cas` ;
3) à défaut, on laisse la logique heuristique de `sig._ppp_metadata_for_param`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import csv
import json

import yaml

from config import load_config, resolve_path


def _default_ppp_usages_file() -> Path:
    cfg = load_config()
    ref_cfg = cfg.get("ref", {}).get("ppp_usages", {}) or {}
    path = ref_cfg.get("file", "data/ref/ppp_usages.csv")
    return resolve_path(path)


_PPP_USAGES_BY_CODE: Dict[str, Dict[str, str]] | None = None
_PPP_USAGES_BY_CAS: Dict[str, Dict[str, str]] | None = None


def _load_ppp_usages() -> tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """
    Charge le dictionnaire d'usages PPP depuis un CSV optionnel.

    Retourne deux dicts :
    - par code_parametre Sandre ;
    - par CAS (si renseigné).
    """
    global _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS
    if _PPP_USAGES_BY_CODE is not None and _PPP_USAGES_BY_CAS is not None:
        return _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS

    path = _default_ppp_usages_file()
    by_code: Dict[str, Dict[str, str]] = {}
    by_cas: Dict[str, Dict[str, str]] = {}

    if not path.exists():
        _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS = by_code, by_cas
        return by_code, by_cas

    with path.open(encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            reader = csv.DictReader(f, dialect=dialect)
        except csv.Error:
            f.seek(0)
            reader = csv.DictReader(f, delimiter=";")
            if not reader.fieldnames or len(reader.fieldnames) < 1:
                f.seek(0)
                reader = csv.DictReader(f, delimiter=",")

        for row in reader:
            code_raw = row.get("code_parametre") or row.get("Code_parametre") or ""
            cas_raw = row.get("cas") or row.get("CAS") or ""
            # Conserver toutes les colonnes non vides (pas seulement ppp_usage / ppp_usages_typiques)
            meta: Dict[str, str] = {}
            for k, v in row.items():
                if k and v is not None:
                    vstr = (v or "").strip()
                    if vstr and k not in ("code_parametre", "Code_parametre", "cas", "CAS"):
                        meta[k] = vstr
            usage = meta.get("ppp_usage") or (row.get("ppp_usage") or "").strip() or None
            usages_typiques = meta.get("ppp_usages_typiques") or (row.get("ppp_usages_typiques") or "").strip() or None
            if not usage and not usages_typiques and not meta:
                continue
            if usage:
                meta["ppp_usage"] = usage
            if usages_typiques:
                meta["ppp_usages_typiques"] = usages_typiques

            code = str(code_raw).strip()
            if code:
                by_code[code] = meta

            cas = str(cas_raw).strip()
            if cas:
                by_cas[cas] = meta

    _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS = by_code, by_cas
    return by_code, by_cas


def lookup_ppp_usage(
    code_parametre: Any | None = None,
    cas_parametre: Any | None = None,
) -> dict[str, str] | None:
    """
    Retourne, si disponible, un dict avec au moins l'une des clés :
    - 'ppp_usage'
    - 'ppp_usages_typiques'

    en cherchant d'abord par code_parametre, puis par CAS.
    """
    by_code, by_cas = _load_ppp_usages()

    if code_parametre is not None:
        code = str(code_parametre).strip()
        if code and code in by_code:
            return by_code[code]

    if cas_parametre is not None:
        cas = str(cas_parametre).strip()
        if cas and cas in by_cas:
            return by_cas[cas]

    return None


def build_ppp_usages_from_c3po(output_path: str | Path | None = None) -> Path:
    """
    Construit (ou remplace) un CSV d'usages PPP à partir du fichier C3PO agrégé
    `data/out/substances_c3po_disponibles.json`.

    Pour chaque substance ayant un `code_parametre_sandre` non nul, on tente de
    déterminer un usage (herbicide, fongicide, insecticide, etc.) à partir des
    libellés C3PO (Sandre, E-phy, BNVD, Agritox...). Quand un type est trouvé,
    on génère aussi un texte d'usages typiques générique.

    Le CSV produit est compatible avec `lookup_ppp_usage` :
    - colonnes : code_parametre ; cas ; ppp_usage ; ppp_usages_typiques
    """
    cfg = load_config()
    out_dir = cfg.get("analysis", {}).get("out_dir", "data/out")
    json_path = Path(out_dir) / "substances_c3po_disponibles.json"
    if not json_path.exists():
        raise FileNotFoundError(
            f"Fichier C3PO agrégé introuvable : {json_path}. "
            "Lancez d'abord `python main.py fetch` puis `python main.py analyze`."
        )

    data = json.loads(json_path.read_text(encoding="utf-8"))
    items = data.get("apercu") or []

    if output_path is None:
        output_path = _default_ppp_usages_file()
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["code_parametre", "cas", "ppp_usage", "ppp_usages_typiques"])

        for sub in items:
            code_raw = sub.get("code_parametre_sandre")
            code = str(code_raw).strip() if code_raw is not None else ""
            if not code:
                continue

            # Texte agrégé pour détecter le type d'usage
            parts = [
                str(sub.get("libelle_parametre_sandre") or ""),
                str(sub.get("libelle_ephy") or ""),
                str(sub.get("libelle_bnvd") or ""),
                str(sub.get("libelle_agritox") or ""),
                str(sub.get("libelle_eupdb") or ""),
            ]
            text = " ".join(parts).lower()

            usage = None
            if "herbicide" in text:
                usage = "herbicide"
            elif "insecticide" in text:
                usage = "insecticide"
            elif "fongicide" in text or "fungicide" in text:
                usage = "fongicide"
            elif "acaricide" in text:
                usage = "acaricide"
            elif "rodenticide" in text:
                usage = "rodenticide"
            elif "nematicide" in text or "nématicide" in text:
                usage = "nématicide"
            elif "pheromone" in text or "phéromone" in text:
                usage = "phéromone de confusion sexuelle"

            if not usage:
                # Rien de fiable détecté : on n'écrit pas de ligne pour ce code,
                # il restera géré par heuristique ou enrichi manuellement.
                continue

            usages_typiques = None
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

            cas = (
                sub.get("cas_parametre_sandre")
                or sub.get("cas_agritox")
                or sub.get("cas_ephy")
                or sub.get("cas_bnvd")
                or sub.get("cas_eupdb")
                or ""
            )
            cas_str = str(cas).strip() if cas is not None else ""

            writer.writerow([code, cas_str, usage or "", usages_typiques or ""])

    # Invalider le cache en mémoire pour que les prochaines consultations
    # prennent en compte les nouvelles valeurs
    global _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS
    _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS = None, None

    return out_path


def _generic_usages_typiques(usage: str) -> str:
    """Texte générique d'usages typiques selon le type PPP."""
    if usage == "herbicide":
        return "Désherbage des cultures, bords de champs, talus ou voiries."
    if usage == "insecticide":
        return "Lutte contre les insectes ravageurs des cultures ou des stockages."
    if usage == "fongicide":
        return "Protection des cultures contre les maladies fongiques (mildiou, oïdium, etc.)."
    if usage == "acaricide":
        return "Lutte contre les acariens sur les cultures."
    if usage == "bactericide":
        return "Lutte contre les bactéries (traitements des cultures ou des stockages)."
    if usage == "molluscicide":
        return "Lutte contre les limaces et mollusques."
    if usage in ("nématicide", "nematicide"):
        return "Lutte contre les nématodes des cultures."
    if usage == "régulateur de croissance":
        return "Régulation de la croissance des plantes (antigerminatif, etc.)."
    if usage == "rodenticide":
        return "Lutte contre les rongeurs (bâtiments agricoles, stockages, etc.)."
    if usage == "phéromone de confusion sexuelle":
        return "Confusion sexuelle pour limiter les ravageurs, en viticulture ou arboriculture."
    if usage == "autre":
        return "Autre usage phytopharmaceutique ; consulter e-phy/INRS pour le détail."
    return ""


def _usage_from_fonctions_row(row: dict) -> str | None:
    """
    Déduit ppp_usage depuis une ligne de substances_fonctions.csv
    (colonnes booléennes herbicide, insecticide, fongicide, etc.).
    """
    def is_true(key: str) -> bool:
        val = (row.get(key) or "").strip().lower()
        return val in ("true", "1", "oui", "yes")

    # Ordre de priorité pour un seul type affiché
    if is_true("herbicide"):
        return "herbicide"
    if is_true("insecticide"):
        return "insecticide"
    if is_true("fongicide"):
        return "fongicide"
    if is_true("acaricide"):
        return "acaricide"
    if is_true("bactericide"):
        return "bactericide"
    if is_true("molluscicide"):
        return "molluscicide"
    if is_true("nematicide"):
        return "nématicide"
    if is_true("regulateur_croissance"):
        return "régulateur de croissance"
    if is_true("rodenticide"):
        return "rodenticide"
    if is_true("autre_fonction"):
        return "autre"
    return None


def _read_csv_by_id_bnvd(path: Path, key_col: str = "id_bnvd") -> Dict[str, dict]:
    """Charge un CSV avec colonne id_bnvd ; retourne dict id_bnvd -> ligne (première occurrence)."""
    out: Dict[str, dict] = {}
    if not path.exists():
        return out
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            kid = (row.get(key_col) or "").strip()
            if kid:
                out[kid] = {k: (v or "").strip() for k, v in row.items() if v and (v or "").strip()}
    return out


def _pnec_to_ugl(valeur: str, unite: str) -> float | None:
    """Convertit valeur_pnec + unite_pnec en µg/L si possible."""
    if not valeur or not unite:
        return None
    try:
        # Gérer "> 0.776" ou "0.54"
        v = valeur.strip().replace(",", ".")
        if v.startswith(">"):
            v = v[1:].strip()
        num = float(v)
    except ValueError:
        return None
    u = (unite or "").strip().lower().replace("µ", "u")
    if u in ("ug/l", "µg/l"):
        return num
    if u in ("mg/l",):
        return num * 1000.0
    return None


def _load_ephy_usages_by_cas(base_dir: Path) -> Dict[str, list[str]]:
    """
    Si les CSV e-Phy sont présents (substance_active_utf8.csv, usages_des_produits_autorises_utf8.csv),
    dans base_dir ou un sous-dossier decision*intrant*, construit un dict CAS -> liste de libellés d'usages courts.
    Sinon retourne {}.
    """
    sub_name = "substance_active_utf8.csv"
    usages_name = "usages_des_produits_autorises_utf8.csv"
    ephy_dir: Path | None = None
    if (base_dir / sub_name).exists() and (base_dir / usages_name).exists():
        ephy_dir = base_dir
    else:
        try:
            for child in sorted(base_dir.iterdir()):
                if child.is_dir() and ("decision" in child.name.lower() or "intrant" in child.name.lower()):
                    if (child / sub_name).exists() and (child / usages_name).exists():
                        ephy_dir = child
                        break
        except OSError:
            pass
    if ephy_dir is None:
        return {}

    sub_path = ephy_dir / sub_name
    usages_path = ephy_dir / usages_name

    nom_to_cas: Dict[str, str] = {}
    with sub_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cas = (row.get("Numero CAS") or "").strip()
            nom = (row.get("Nom substance active") or "").strip()
            if cas and nom:
                key = nom.lower().replace("é", "e").replace("è", "e")
                nom_to_cas[key] = cas
                if " " in nom:
                    nom_to_cas[nom.split()[0].lower()] = cas

    cas_usages: Dict[str, list[str]] = {}
    with usages_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            substances = (row.get("Substances actives") or "").strip()
            ident_usage = (row.get("identifiant usage") or row.get("identifiant usage lib court") or "").strip()
            if not substances or not ident_usage:
                continue
            name_part = (substances.split("(")[1].split(")")[0].strip() if "(" in substances else (substances.split()[0] if substances else ""))
            if not name_part:
                continue
            key = name_part.lower().replace("é", "e").replace("è", "e")
            cas = nom_to_cas.get(key) or nom_to_cas.get(name_part.lower())
            if not cas:
                continue
            short = ident_usage.replace("*", ", ")[:80]
            if short and (cas not in cas_usages or short not in cas_usages[cas]):
                cas_usages.setdefault(cas, []).append(short)

    return {c: lst[:8] for c, lst in cas_usages.items()}


def build_ppp_usages_from_sources_dictionnaire(
    sources_dir: str | Path | None = None,
    output_path: str | Path | None = None,
) -> Path:
    """
    Construit le CSV d'usages PPP à partir de toutes les sources C3PO/e-Phy
    disponibles dans `sources_dictionnaire`.

    Utilise :
    - substances_identification.csv : jointure id_bnvd -> code_parametre_sandre, cas
    - substances_fonctions.csv : ppp_usage (herbicide, insecticide, ...)
    - substances_reglementation.csv : statut_ue, date_expiration, autorisation_france
    - substances_mentions_categories.csv : mentions (biocontrôle, faible risque, etc.)
    - substances_pnec.csv : pnec_ugl (PNEC en µg/L, valeur min par substance)
    - substances_classements_tox_ecotox.csv : mentions_danger (codes H, etc.)

    Fichiers optionnels : s'ils sont absents, les colonnes correspondantes sont vides.
    """
    cfg = load_config()
    ref_ppp = cfg.get("ref", {}).get("ppp_usages", {}) or {}
    default_sources = ref_ppp.get("sources_dictionnaire", "sources/sources_dictionnaire")
    root = CONFIG_PATH.parent
    base_dir = Path(sources_dir) if sources_dir else root / default_sources
    base_dir = base_dir if base_dir.is_absolute() else root / base_dir

    id_path = base_dir / "substances_identification.csv"
    fonc_path = base_dir / "substances_fonctions.csv"
    if not id_path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {id_path}. "
            "Vérifiez ref.ppp_usages.sources_dictionnaire dans config.yaml."
        )
    if not fonc_path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {fonc_path}. "
            "Vérifiez ref.ppp_usages.sources_dictionnaire dans config.yaml."
        )

    # id_bnvd -> (code_parametre_sandre, cas_parametre_sandre)
    id_map: Dict[str, tuple[str, str]] = {}
    with id_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            code = (row.get("code_parametre_sandre") or "").strip()
            if not code:
                continue
            cas = (row.get("cas_parametre_sandre") or row.get("cas_bnvd") or "").strip()
            id_bnvd = (row.get("id_bnvd") or "").strip()
            if id_bnvd:
                id_map[id_bnvd] = (code, cas)

    # Usages e-Phy par CAS (optionnel) pour enrichir ppp_usages_typiques
    ephy_usages_by_cas = _load_ephy_usages_by_cas(base_dir)

    # Chargement des sources optionnelles (une entrée par id_bnvd)
    regl = _read_csv_by_id_bnvd(base_dir / "substances_reglementation.csv")
    mentions_cat = _read_csv_by_id_bnvd(base_dir / "substances_mentions_categories.csv")
    classements = _read_csv_by_id_bnvd(base_dir / "substances_classements_tox_ecotox.csv")

    # PNEC : plusieurs lignes par id_bnvd, on garde le min en µg/L
    pnec_by_id: Dict[str, float] = {}
    pnec_path = base_dir / "substances_pnec.csv"
    if pnec_path.exists():
        with pnec_path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                kid = (row.get("id_bnvd") or "").strip()
                if not kid or kid not in id_map:
                    continue
                v = _pnec_to_ugl(
                    row.get("valeur_pnec") or "",
                    row.get("unite_pnec") or "",
                )
                if v is not None:
                    pnec_by_id[kid] = min(v, pnec_by_id[kid]) if kid in pnec_by_id else v

    if output_path is None:
        output_path = _default_ppp_usages_file()
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Colonnes de sortie (ordre fixe pour lisibilité)
    base_columns = ["code_parametre", "cas", "ppp_usage", "ppp_usages_typiques"]
    extra_columns = ["statut_ue", "date_expiration", "autorisation_france", "mentions", "pnec_ugl", "mentions_danger"]
    all_columns = base_columns + extra_columns

    seen_codes: set[str] = set()
    rows_out: list[Dict[str, str]] = []

    with fonc_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            id_bnvd = (row.get("id_bnvd") or "").strip()
            if id_bnvd not in id_map:
                continue
            code, cas = id_map[id_bnvd]
            if code in seen_codes:
                continue
            seen_codes.add(code)
            usage = _usage_from_fonctions_row(row)
            if not usage:
                continue
            usages_typiques = _generic_usages_typiques(usage)
            if cas and cas in ephy_usages_by_cas:
                usages_typiques = "; ".join(ephy_usages_by_cas[cas])

            out_row: Dict[str, str] = {
                "code_parametre": code,
                "cas": cas,
                "ppp_usage": usage,
                "ppp_usages_typiques": usages_typiques,
            }

            # Réglementation
            r = regl.get(id_bnvd) or {}
            out_row["statut_ue"] = r.get("etat_reg_1107_2009") or ""
            out_row["date_expiration"] = r.get("date_expiration_approbation") or ""
            out_row["autorisation_france"] = r.get("autorisation_france") or ""

            # Mentions / catégories (biocontrôle, faible risque, candidat substitution)
            m = mentions_cat.get(id_bnvd) or {}
            parts = []
            if (m.get("in_biocontrole") or "").lower() in ("true", "1", "oui"):
                parts.append("biocontrôle")
            if (m.get("faible_risque") or "").lower() in ("true", "1", "oui"):
                parts.append("faible risque")
            if (m.get("candidat_substitution") or "").lower() in ("true", "1", "oui"):
                parts.append("candidat à la substitution")
            out_row["mentions"] = "; ".join(parts) if parts else ""

            # PNEC (µg/L)
            pnec_val = pnec_by_id.get(id_bnvd)
            out_row["pnec_ugl"] = str(pnec_val) if pnec_val is not None else ""

            # Classements danger (codes H, mentions)
            c = classements.get(id_bnvd) or {}
            danger = c.get("codes_h_agritox") or c.get("mentions_danger_agritox") or ""
            danger = " ".join(danger.split())  # une seule ligne
            if len(danger) > 250:
                danger = danger[:247] + "..."
            out_row["mentions_danger"] = danger

            rows_out.append(out_row)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_columns, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        for out_row in rows_out:
            writer.writerow(out_row)

    global _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS
    _PPP_USAGES_BY_CODE, _PPP_USAGES_BY_CAS = None, None

    return out_path

