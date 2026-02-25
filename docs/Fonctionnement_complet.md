# Comment fonctionne phytoDB (explication simple et complète)

## À quoi sert le programme ?

**phytoDB** aide à analyser l’**impact des produits phytopharmaceutiques (PPP)** dans le **département de la Côte-d’Or (21)**.  
Objectif : croiser plusieurs sources de données pour repérer les zones les plus concernées et, à terme, alimenter une **carte (SIG)** utilisable pour orienter les contrôles (ex. lieux potentiels d’infractions liées aux PPP).

---

## D’où viennent les données ?

Le programme s’appuie sur **trois types de sources** (toutes ouvertes / API publiques) :

| Source | Ce qu’elle apporte | Rôle dans phytoDB |
|--------|--------------------|--------------------|
| **C3PO** (data.gouv.fr) | Liste des substances PPP (noms, codes CAS, liens entre bases) | Référentiel : savoir de quelles substances on parle. |
| **Naïades** (Hub’Eau) | Qualité des **eaux de surface** (rivières, lacs) : stations de mesure + analyses | Impact : où l’eau est mesurée et ce qu’on y trouve (dont pesticides possibles). |
| **ADES** (Hub’Eau) | Qualité des **eaux souterraines** (nappes) : points d’eau + analyses | Impact : idem pour les nappes. |

- **C3PO** est chargé via l’**API tabulaire data.gouv.fr**.
- **Naïades** et **ADES** sont chargés via l’**API Hub’Eau** (filtre par département 21).

Tout est configuré dans **`config.yaml`** (département 21, cache, RIDs des ressources C3PO, etc.).

---

## Comment ça se passe quand on lance le programme ?

Tout se fait en **ligne de commande** (CLI), dans le dossier du projet :

```bash
cd phytoDB
python main.py <commande> [options]
```

### Une seule commande pour tout faire : `run`

**`python main.py run`** enchaîne **cinq étapes** dans l’ordre :

1. **C3PO**  
   Le programme appelle l’API tabulaire data.gouv.fr pour récupérer la ressource C3PO configurée (ex. « substances_identification »).  
   → Environ 722 substances, mis en **cache** dans `data/cache/` si le cache est activé.

2. **Naïades**  
   Appel à l’API Hub’Eau « Qualité des cours d’eau » pour le département 21.  
   → Liste des **stations** de mesure (ex. 486 stations).  
   Optionnellement, avec `--naiades-analyses`, il récupère aussi les **analyses** (volume plus important).

3. **ADES**  
   Appel à l’API Hub’Eau « Qualité des nappes » pour le département 21.  
   → Liste des **stations** (points d’eau).  
   Optionnellement, avec `--ades-analyses`, il récupère aussi les **analyses**.

4. **Analyse**  
   Le programme analyse **C3PO** pour le département 21 (liste et propriétés des substances)  
   et écrit un résumé dans **`data/out/`** (JSON).

5. **Export SIG**  
   Les données Naïades et ADES (stations + analyses) sont converties en **points géographiques** (coordonnées) et regroupées dans un **fichier GeoJSON**.  
   → Fichier généré : **`data/sig/impact_ppp_cote_dor.geojson`**, utilisable dans QGIS ou ArcGIS pour afficher une couche « impact PPP » sur la Côte-d’Or.

Si une étape échoue (ex. ADES en erreur), les suivantes sont quand même exécutées avec les données déjà récupérées.

### Commandes séparées (si vous ne voulez pas tout faire d’un coup)

- **`python main.py fetch`**  
  Ne fait que l’étape 1 (C3PO). Option `--ventes` pour tenter aussi les ventes par l’API.

- **`python main.py fetch-naiades`**  
  Ne fait que l’étape 2 (Naïades). Option `--analyses` pour inclure les analyses.

- **`python main.py fetch-ades`**  
  Ne fait que l’étape 3 (ADES). Option `--analyses` pour inclure les analyses.

- **`python main.py analyze`**  
  Ne fait que l’étape 4 (analyse C3PO + ventes).  
  Vous pouvez passer un fichier de ventes : `--ventes-file chemin/vers/fichier.csv`.

- **`python main.py export-sig`**  
  Ne fait que l’étape 5 (création du GeoJSON).  
  Il utilise les données déjà en cache (Naïades/ADES) ou les récupère si le cache est vide.

- **`python main.py list-resources`**  
  Affiche la liste des ressources C3PO disponibles (pour remplir `config.yaml` si besoin).

---

## Où sont les résultats et à quoi ils servent ?

| Emplacement | Contenu | Usage |
|-------------|--------|--------|
| **`data/out/`** | Résultats de l’**analyse** : `analyse_cote_dor.json`, `top_substances_ventes_21.csv`, `substances_c3po_disponibles.json` (si pas de ventes). | Consulter les indicateurs, les listes de substances, les agrégats pour la Côte-d’Or. |
| **`data/sig/`** | **Couche géographique** : `impact_ppp_cote_dor.geojson` (points = stations et analyses Naïades/ADES). | Ouvrir dans QGIS/ArcGIS pour cartographier les zones de mesure de la qualité de l’eau (impact potentiel des PPP). |
| **`data/cache/`** | Données brutes **mises en cache** (C3PO, Naïades, ADES) pour ne pas tout re-télécharger à chaque run. | Accélérer les prochains lancements ; option `--no-cache` pour forcer un nouveau téléchargement. |

**Couche SIG** : uniquement des entités en Côte-d'Or. Table attributaire normalisée : `wkt_geom`, `source`, `type_donnee`, `bss_id`, `code_bss`, `code_station`, `libelle_station`, `code_departement`, `code_commune`, `libelle_commune`, `nom_cours_eau`, `nom_masse_deau`, `num_departement`, `nom_commune`, puis `libelle_parametre`, `code_parametre`, `resultat`, `symbole_unite`, `date_prelevement`, `annee` (données PPP et impact). Permet d'évaluer le degré d'impact (filtrage par paramètre, concentrations, quantités).

---

## Schéma du flux (résumé)

```
config.yaml (département 21, RIDs C3PO, cache)
        |
        v
+-------+--------+
|  python main.py run   |
+-------+--------+
        |
        +---> 1) C3PO (API tabulaire) ----------> data/cache/ + analyse
        |
        +---> 2) Naïades (API Hub'Eau) ----------> data/cache/ + export SIG
        |
        +---> 3) ADES (API Hub'Eau) ------------> data/cache/ + export SIG
        |
        +---> 4) Analyse (C3PO + ventes) -------> data/out/*.json, *.csv
        |
        +---> 5) Export SIG (Naïades + ADES) ---> data/sig/impact_ppp_cote_dor.geojson
```

---

## Cas particuliers utiles à savoir

- **Ventes vides pour la Côte-d’Or**  
  Les « ventes par département » BNV-D sont par **département du vendeur**. En 21, il y a peu de vendeurs, donc souvent 0 ligne. Ce n’est pas une erreur du programme.  
  Voir : `docs/BNVD_pourquoi_aucun_resultat_cote_dor.md`.

- **Achats masqués « nc »**  
  Depuis 2021, les achats peuvent être masqués (confidentialité). Le module **`bnvd_contournement_nc.py`** permet d’utiliser des **millésimes avant 2021** ou des **achats par code postal** (filtre 21xxx, en excluant les « nc ») pour avoir une estimation.  
  Détails : section 4 de `docs/BNVD_pourquoi_aucun_resultat_cote_dor.md`.

- **Ventes à partir d’un fichier**  
  Si vous téléchargez vous-même un ZIP BNV-D et extrayez un CSV de ventes (ou d’achats), vous pouvez le passer à l’analyse avec :  
  `python main.py run --ventes-file chemin/vers/fichier.csv`  
  ou  
  `python main.py analyze --ventes-file chemin/vers/fichier.csv`.

En résumé : le programme **récupère** des données ouvertes (C3PO, ventes, Naïades, ADES), les **filtre** pour la Côte-d’Or, les **analyse** (C3PO + ventes) et **exporte** une couche SIG pour visualiser les zones concernées.
