# phytoDB – Analyse des PPP en Côte-d'Or

Outil d’analyse de l’impact des produits phytopharmaceutiques (PPP) dans le département de la Côte-d'Or (21), en vue d’orienter les contrôles et les lieux potentiels d’infractions. Objectif à terme : **croiser le maximum de données PPP pour produire une couche SIG** visualisant les zones les plus impactées.

## Données utilisées

- **C3PO** (base de connaissances PPP) : [data.gouv.fr – C3PO](https://www.data.gouv.fr/datasets/base-de-connaissances-sur-les-produits-phytopharmaceutiques-a-partir-de-sources-ouvertes-c3po/)
- **API tabulaire data.gouv.fr** : [API tabulaire (beta)](https://www.data.gouv.fr/dataservices/api-tabulaire-data-gouv-fr-beta)
- **Naïades** (qualité des eaux de surface) : [naiades.eaufrance.fr](https://naiades.eaufrance.fr/) — accès via [API Hub'Eau Qualité des cours d'eau](https://hubeau.eaufrance.fr/page/api-qualite-cours-deau)
- **ADES** (qualité des eaux souterraines) : [ades.eaufrance.fr](https://ades.eaufrance.fr/) — accès via [API Hub'Eau Qualité des nappes](https://hubeau.eaufrance.fr/page/api-qualite-nappes)

## Prérequis

- Python 3.10+
- Accès internet pour l’API tabulaire et, le cas échéant, le téléchargement de jeux de données

## Installation

**Explication complète du fonctionnement** : [docs/Fonctionnement_complet.md](docs/Fonctionnement_complet.md).

```bash
cd phytoDB
pip install -r requirements.txt
```

## Configuration

- `config.yaml` : département cible (21), URLs des API, RIDs des ressources C3PO, cache.
- Les **RIDs** des ressources sont visibles sur la page du jeu de données data.gouv.fr, onglet *Métadonnées* de chaque ressource.
- Pour le filtrage PPP (pesticides), on peut renseigner une URL de référentiel Sandre ou un CSV local, par ex. :

```yaml
ref:
  parametres_pesticides:
    # Chemin local où sera enregistré le CSV de paramètres pesticides
    file: data/ref/parametres_pesticides.csv
    # (Optionnel) URL distante d'un CSV de paramètres pesticides (Sandre / Eaufrance / autre)
    # Si le fichier n'existe pas encore, le programme tentera de le télécharger automatiquement.
    url: https://exemple.sandre.eaufrance.fr/parametres_pesticides.csv
```

## Utilisation (CLI)

```bash
# Depuis le répertoire phytoDB
# Récupération des données C3PO (via API tabulaire)
python main.py fetch

# Analyse (C3PO uniquement)
python main.py analyze

# Tout en une fois (analyse complète : C3PO + Naïades + ADES + analyse + SIG)
python main.py run

# Avec options (analyses qualité eau, ou sans export SIG)
python main.py run --naiades-analyses --ades-analyses
python main.py run --no-sig

# --- Naïades (qualité cours d'eau) et ADES (qualité nappes) ---
# Les listes de paramètres PPP (codes Sandre) peuvent être fournies dans un CSV
# (voir `ref_params.py` et la section `ref.parametres_pesticides` de `config.yaml`).
python main.py fetch-naiades              # stations de mesure dép. 21
python main.py fetch-naiades --analyses   # + analyses (puis filtrage éventuel sur pesticides)
python main.py fetch-ades                 # stations ADES dép. 21
python main.py fetch-ades --analyses

# --- Couche SIG (GeoJSON pour QGIS / ArcGIS) ---
python main.py export-sig            # génère data/sig/impact_ppp_cote_dor.geojson
# Si le cache des analyses est vide, des analyses (Naïades + ADES) sont récupérées automatiquement
# puis filtrées sur les paramètres « pesticides » si un fichier de références Sandre est présent,
# afin que la couche contienne des paramètres PPP (filtre « code_parametre is not null » en SIG).
python main.py export-sig --no-fetch-analyses   # ne pas récupérer les analyses si cache vide (couche sans paramètres)
python main.py export-sig --no-ades  # uniquement Naïades
python main.py export-sig --no-naiades --out data/sig/ades_21.geojson
```

Les sorties (indicateurs, listes de substances/produits, agrégats) sont écrites dans `data/out/` (JSON et CSV). La couche SIG est écrite dans `data/sig/` (GeoJSON).

## Construction du programme et couche SIG

- **Référentiel de substances** : C3PO (liste de substances PPP, noms, identifiants, propriétés).
- **Impact** : Naïades (qualité des eaux de surface, stations + analyses) et ADES (qualité des nappes, stations + analyses), avec géométrie (point de mesure).
- **Normalisation** : chaque source est convertie en *features* GeoJSON avec un schéma commun (`source`, `type_donnee` = impact_surface | impact_souterrain, propriétés métier, géométrie).
- **Export** : un seul fichier GeoJSON (FeatureCollection) agrège stations et analyses pour le département 21, exploitable dans QGIS ou ArcGIS pour visualiser les zones impactées et les croiser avec d’autres couches (communes, bassins versants, parcelles, etc.).
- **Évolutions envisagées** : filtrage des paramètres « pesticides » (codes Sandre), agrégation par commune ou maille, indicateur de pression (ventes) géolocalisé (ex. achats par code postal), puis combinaison pression + impact dans une couche d’indicateurs de contrôle.

## Structure du projet

- `config.yaml` : configuration (département, API, RIDs, cache, référentiels PPP Naïades/ADES)
- `api_tabulaire.py` : client pour l’API tabulaire data.gouv.fr (pagination, filtres)
- `datagouv.py` : récupération des métadonnées et des RIDs des jeux de données
- `c3po.py` : chargement des données C3PO via l’API tabulaire
- `analysis.py` : analyse C3PO et indicateurs pour le département 21
- `main.py` : point d’entrée CLI
- `hubeau.py` : client API Hub'Eau (Naïades + ADES)
- `sources/naiades.py`, `sources/ades.py` : récupération données qualité eau pour le dép. 21
- `sig.py` : schéma normalisé et export GeoJSON (couche SIG)
- `ref_params.py` : chargement des listes de paramètres pesticides (Sandre / C3PO) et filtrage des analyses
- `data/out/` : sorties analyse (`analyse_cote_dor.json`, `substances_c3po_disponibles.json`, etc.)
- `data/sig/` : couche SIG (`impact_ppp_cote_dor.geojson`)
- `data/cache/` : cache des données brutes (C3PO, Naïades, ADES)

## Évolutions prévues

- Indicateurs de contrôle pour les agents de terrain (à définir après exploration des données).
- Choix de l’interface : CLI uniquement pour l’instant ; GUI possible ultérieurement.

## Licence

Conformément aux jeux de données : Licence Ouverte / Open Licence version 2.0.
