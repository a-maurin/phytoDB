# Analyse du projet data-pesticides (comparaison avec phytoDB)

Rapport d’analyse du dépôt cloné dans `/media/e357/Windows/Users/aguirre.maurin/Documents/GitHub/data-pesticides`.

---

## 1. Analyse du code

### Stack et structure

- **Langage / framework** : PHP (Silex pour le web, Symfony Console pour la CLI).
- **Points d’entrée** :
  - **Web** : `web/index.php` → `app/config/config.php`, `app/config/routes.php` → API JSON et page d’accueil.
  - **CLI** : `bin/console` avec les commandes :
    - `data-pesticides:load-dataset` — import CSV → triplestore ;
    - `data-pesticides:warmup-api` — préchauffage du cache API.

### Modules métier (namespace `Neveldo\DataPesticides`)

| Composant | Rôle |
|-----------|------|
| **Triplestore/** | Client SPARQL (requêtes, `CLEAR GRAPH`, `LOAD` fichier dans un graphe). |
| **Dataset/Reader/** | `CSVReader` : lecture CSV ligne par ligne, délimiteur `,`, première ligne ignorée (en-tête). |
| **Dataset/Writer/** | `TurtleWriter` : écriture RDF Turtle (fichiers `.ttl` dans `datasets/rdf/`). |
| **Dataset/TriplesConverter/** | Conversion CSV → triplets RDF selon le type de jeu : |
| → StationTriplesConverter | Stations : code, ville, libellé, département, altitude, coord. L93 (x,y) → WGS84. |
| → StationStatementTriplesConverter | Par année : station, pesticide, nb analyses, concentration moyenne (µg/L). |
| → StationStatementTotalTriplesConverter | Par année : station, nb analyses, concentration totale. |
| → PesticideTriplesConverter | Pesticides : code, libellés, famille, rôle, statut, date fin usage, valeur normative. |
| → RoleTriplesConverter, DepartmentTriplesConverter | Rôles (fonctions) et départements (depts2016). |
| **Dataset/Loader.php** | Chaîne : Reader → TriplesConverter → Writer → `clearGraph` + `loadFile` vers Blazegraph. |
| **Controller/ApiController** | Requêtes SPARQL sur le triplestore + appel HTTP à Sandre (fiches GPR) ; réponses JSON, cache fichier. |
| **Command/LoadDatasetCommand** | Charge un CSV selon `--type` et optionnellement `--year`, lance le Loader. |

### Formats CSV attendus (après en-tête)

- **stations** : `[0]=code, [1]=code_ville, [2]=libellé, [3]=code_département, [5]=altitude, [8]=x_L93, [9]=y_L93`.
- **station_statements** (ex. `ma_qp_fm_rcsrco_pesteso_YYYY.csv`) : `[0]=code_station, [1]=code_pesticide, [2]=nb_analyses, [3]=concentration_moyenne`.
- **station_statements_total** (ex. `moy_tot_quantif_YYYY.csv`) : `[1]=code_station, [2]=nb_analyses, [3]=concentration_totale`.
- **pesticides** : `[0]=code, [1]=libellé, [2]=libellé_secondaire, [3]=famille, [4]=rôle, [5]=statut, [10]=date_fin_usage, [12]=valeur_normale`.
- **departments** (depts2016) : `[0]=code_région, [1]=code_insee, [5]=libellé`.

Les données métier sont **uniquement eaux souterraines** (stations = points ADES, données agrégées par station × pesticide × année ou station × année).

---

## 2. Sources utilisées

### Données fournies dans le dépôt (pas de téléchargement automatique)

- **`datasets/source.tar.gz`** (à décompresser avec `install.sh`) contient :
  - **Référentiels** : `stations.csv`, `pesticides.csv`, `fonctions.csv` (rôles), `depts2016.csv`.
  - **Données annuelles** : `ma_qp_fm_rcsrco_pesteso_2007.csv` … `ma_qp_fm_rcsrco_pesteso_2014.csv`, `moy_tot_quantif_2007.csv` … `moy_tot_quantif_2014.csv`.

Origine indiquée dans la doc / les vues : jeu du **concours** « Data-visualisation sur les pesticides dans les eaux souterraines » (Ministère, 2016), probablement dérivé de données alors disponibles sur **donnees.statistiques.developpement-durable.gouv.fr** ou data.gouv.fr. Aucune URL d’API ou de mise à jour automatique n’est présente dans le code.

### API / services externes

- **Triplestore** : Blazegraph, endpoint configuré dans `app/config/config.php` (ex. `http://localhost:9999/blazegraph/namespace/datapesticides/sparql`). Toutes les données « carte / graphiques » viennent de requêtes SPARQL sur ces graphes.
- **Sandre / EauFrance** : dans `ApiController::apiGetSandreData()` :
  - URL : `http://id.eaufrance.fr/gpr/{id}.xml`
  - Usage : récupération des **fiches GPR** (paramètres) pour l’affichage côté front (fiche Sandre, liens vers `id.eaufrance.fr/par/...`). Pas d’utilisation pour charger les données d’analyses.

Liens uniquement documentaires / UI : ADES (`FichePtEau.aspx`), Sandre (urn, fiches), eaufrance.fr, statistiques développement-durable, concours.

---

## 3. Comment les sources sont utilisées par le code

### Pipeline d’import (données jusqu’à 2014)

1. **Récupération** : Fichiers CSV locaux (chemins passés en `--file=` à `bin/console data-pesticides:load-dataset`). Aucun fetch HTTP pour les données de fond.
2. **Traitement** :
   - `CSVReader` lit chaque ligne (après en-tête) → tableau de colonnes.
   - Le `TriplesConverter` correspondant au `--type` produit des triplets RDF (entités `dpo:` / `dpd:`).
   - `TurtleWriter` écrit dans `datasets/rdf/<nom_du_fichier>.ttl`.
3. **Sortie** :
   - Le client SPARQL fait `CLEAR GRAPH <graphe_cible>` puis `LOAD <file://...ttl> INTO GRAPH <graphe_cible>` (graphe dérivé de `graph_prefix` + nom du fichier).
   - Les années pour `station_statements` et `station_statements_total` sont fixées par `--year=YYYY` et stockées dans le RDF (`dpo:year`).

### API (lecture et exposition)

1. **Récupération** :
   - Données principales : requêtes SPARQL sur le triplestore (liste des années, stations, départements, familles de pesticides, concentrations par station/département/année, dépassements, métadonnées min/max, etc.).
   - Données Sandre : GET `http://id.eaufrance.fr/gpr/{id}.xml` pour une fiche paramètre (affichage uniquement).
2. **Traitement** : Réponses SPARQL (text/csv) converties en structures PHP puis en JSON ; XML Sandre converti et renvoyé en JSON.
3. **Sortie** : Réponses HTTP JSON ; cache fichier dans `var/cache` (clé = méthode API + paramètres), rafraîchi avec `?refresh` ou `bin/console data-pesticides:warmup-api`.

Les données « à la date la plus récente » dépendent donc **uniquement** des CSV déjà importés : il n’y a pas de mécanisme dans le code pour aller chercher des années au-delà de 2014.

---

## 3.1 Mise à jour des données pour obtenir des résultats jusqu’à la date la plus récente

### Constat

- Les jeux fournis dans `source.tar.gz` s’arrêtent à **2014**.
- Aucun script ni aucune config dans le dépôt ne télécharge ou ne génère des CSV pour 2015 et au-delà.

### Options pour avoir des données à jour

1. **Réutiliser un jeu officiel mis à jour (si disponible)**  
   Vérifier sur **data.gouv.fr** ou **donnees.statistiques.developpement-durable.gouv.fr** si le jeu « Pesticides dans les eaux souterraines » (ou équivalent) existe encore et est publié avec des années récentes, et si le format reste compatible avec les colonnes attendues par les converters (index 0,1,2,3 pour station_statements, etc.). Si oui, télécharger les CSV par année et lancer :
   ```bash
   bin/console data-pesticides:load-dataset --type=station_statements --file=.../ma_qp_fm_rcsrco_pesteso_2022.csv --year=2022
   bin/console data-pesticides:load-dataset --type=station_statements_total --file=.../moy_tot_quantif_2022.csv --year=2022
   ```
   Puis `bin/console data-pesticides:warmup-api`.

2. **Régénérer les CSV à partir de Hub'Eau (ADES)**  
   Les données data-pesticides sont des **eaux souterraines** (équivalent ADES). Pour des années récentes, il faut :
   - Récupérer les analyses ADES (ex. via API Hub'Eau `qualite_nappes` / `analyses`) par année et par département (ou France entière).
   - Agréger par **(code_station, code_parametre, année)** pour produire :
     - un CSV de type **station_statements** : `code_station, code_parametre, nb_analyses, concentration_moyenne_µg/L` ;
     - un CSV de type **station_statements_total** : `?, code_station, nb_analyses, concentration_totale` (le premier champ peut être vide ou identifiant selon le converter).
   - Respecter le format des converters (délimiteur `,`, en-tête en première ligne, colonnes aux bons index) et les noms de stations/pesticides/départements déjà présents dans les référentiels `stations.csv`, `pesticides.csv`, `depts2016.csv`, ou mettre à jour ces référentiels.
   - Importer chaque année avec `load-dataset` puis lancer le warmup API.

3. **Adapter le code**  
   Si les CSV officiels ont un format ou des colonnes différents, il faudrait adapter les `TriplesConverter` (et éventuellement le `CSVReader` si délimiteur/ordre change) pour coller au nouveau schéma.

En résumé : pour des résultats jusqu’à la date la plus récente, il faut soit des **fichiers CSV à jour** (fournis ailleurs ou **générés à partir d’ADES/Hub'Eau**), puis les importer avec la commande existante ; le code actuel ne fait pas cette mise à jour tout seul.

---

## 4. Apports possibles de data-pesticides pour phytoDB

### Comparaison rapide

| Critère | data-pesticides | phytoDB |
|--------|-----------------|---------|
| Périmètre géo | France entière (stations/départements) | Côte-d'Or (dép. 21) |
| Type d’eaux | Eaux souterraines uniquement | Eaux de surface (Naïades) + eaux souterraines (ADES) |
| Source des données | CSV statiques 2007–2014 (concours) | API Hub'Eau (Naïades + ADES) à la demande |
| Mise à jour | Manuelle (réimport CSV) | Fetch + cache + export SIG |
| Sorties | Dataviz web (cartes, graphiques par année/famille), API JSON | GeoJSON (QGIS/ArcGIS), couches dérivées (hotspots, top10), stats par année |
| Stack | PHP, Silex, Blazegraph, RDF/SPARQL | Python, CLI, pas de triplestore |

### Ce que phytoDB a déjà en plus

- **Données vivantes** : accès direct aux API Hub'Eau (Naïades + ADES), donc possibilité d’avoir des années récentes en configurant les dates et en augmentant les pages.
- **Eaux de surface** : data-pesticides ne traite pas Naïades.
- **SIG** : export GeoJSON, styles QML, vues hotspots et top10, intégration dans un SIG (QGIS/ArcGIS).
- **Côte-d'Or** : ciblage départemental et statistiques par année déjà en place (`stats-annees`).

### Idées utiles à réutiliser ou s’en inspirer (sans reprendre le stack PHP/RDF)

1. **Agrégations par station × paramètre × année**  
   data-pesticides travaille en **concentration moyenne** et **nombre d’analyses** par (station, pesticide, année). Dans phytoDB, on pourrait ajouter une vue ou un export « agrégé » similaire (ex. GeoJSON ou CSV) : pour chaque (station ou BSS, code_parametre, année), nombre de prélèvements et concentration moyenne (µg/L), pour faciliter tableaux de bord ou graphiques type « évolution par année » sans repasser par un triplestore.

2. **Familles de pesticides et comparaisons**  
   data-pesticides utilise des **familles** (herbicides, fongicides, etc.) et des graphiques par famille. phytoDB a déjà `ppp_usage` et des libellés (C3PO, ppp_dict). On pourrait ajouter des sorties ou des stats **par famille/usage** et par année (ex. `stats-annees` étendu par usage, ou couche « top 10 par famille »).

3. **Référentiel Sandre / GPR**  
   data-pesticides affiche des fiches GPR via `id.eaufrance.fr/gpr/{id}.xml`. phytoDB pourrait exposer ou lier les mêmes URLs (ou un champ `ppp_url_sandre`) pour les paramètres, en plus d’INRS/e-phy, pour homogénéiser avec les référentiels officiels.

4. **Pas de réutilisation directe du code PHP**  
   Le cœur de data-pesticides (Silex, Blazegraph, conversion CSV → RDF, SPARQL) est très spécifique et en PHP. Pour phytoDB (Python, pas de triplestore), il est plus pertinent d’**emprunter les idées** (agrégations, familles, liens Sandre) et de les implémenter en Python dans le flux existant (sig_views, analysis, ou nouveaux modules) plutôt que d’intégrer le dépôt data-pesticides tel quel.

### Conclusion

- **data-pesticides** est une dataviz historique (2007–2014, eaux souterraines, concours 2016), avec des données figées et aucune mise à jour automatique.
- **phytoDB** est déjà mieux adapté pour des **données récentes** et un usage **SIG** sur la Côte-d'Or (Naïades + ADES, export GeoJSON, stats par année).
- Les **améliorations utiles** pour phytoDB inspirées de data-pesticides sont : agrégations (station × paramètre × année), indicateurs par famille/usage, et liens vers les fiches Sandre/GPR. Elles peuvent être ajoutées en Python dans phytoDB sans dépendre du projet data-pesticides.
