# Données PPP Côte-d'Or : tables attributaires, sources et pistes (eaux brutes)

## 1. Tables attributaires et sources utilisées

### Périmètre
- **Département** : Côte-d'Or (code 21).
- **Données** : analyses de qualité des eaux (paramètres PPP / pesticides) localisées en 21.

### Sources programmées

| Source | Type | API / origine | Champs date utilisés |
|--------|------|----------------|----------------------|
| **Naïades** | Eaux de surface (cours d'eau, plans d'eau) | Hub'Eau — qualite_rivieres v2, `analyse_pc` | `date_prelevement` → `annee` |
| **ADES** | Eaux souterraines (nappes) | Hub'Eau — qualite_nappes v1, `analyses` | `date_debut_prelevement` → `annee` |

- **Naïades** : stations et analyses physico-chimiques par département ; filtrage optionnel par `code_parametre` (Sandre) et par période (`date_debut_prelevement`, `date_fin_prelevement`) via `config.yaml` (section `ppp.naiades`). Si `codes_parametre` est vide, le référentiel local `data/ref/parametres_pesticides.csv` (ou C3PO) est utilisé pour restreindre aux pesticides.
- **ADES** : stations (points d'eau) et analyses des nappes ; même logique de filtrage PPP et de fenêtre temporelle (`ppp.ades` dans `config.yaml`).

### Table attributaire normalisée (couche SIG)

Fichier principal : `data/sig/analyse_stations_ppp_cote_dor.geojson`. Une feature = une analyse (un prélèvement × un paramètre PPP), uniquement en Côte-d'Or.

Colonnes principales (voir `sig.COLONNES_ATTR`) :

- **PPP (en tête)** : `ppp_nom`, `ppp_usage`, `ppp_taux_ugl`, `ppp_depassement`, `ppp_seuil_sanitaire_ugl`, `ppp_ratio_seuil`, `ppp_usages_typiques`
- **Localisation** : `libelle_station`, `libelle_commune`, `nom_commune`, `nom_cours_eau`, `nom_masse_deau`, `code_station`, `code_commune`, `code_departement`, `bss_id`, `code_bss`, `num_departement`, `source`, `type_donnee`
- **Technique** : `libelle_parametre`, `code_parametre`, `resultat`, `symbole_unite`, **`date_prelevement`**, **`annee`**, `ppp_description`, `ppp_url_inrs`, `ppp_url_ephy`

Les champs **`date_prelevement`** et **`annee`** proviennent donc :
- Naïades : `date_prelevement` (API) → `annee` = 4 premiers caractères.
- ADES : `date_debut_prelevement` (API) → mappé en `date_prelevement` et `annee`.

Aucun filtre temporel n’est appliqué par défaut dans `config.yaml` (`date_debut` / `date_fin` à `null`), ce qui explique que les prélèvements remontent à des années anciennes tant que l’API et le cache les fournissent.

### Classement par année

Commande pour afficher le nombre de prélèvements par année (Côte-d'Or) :

```bash
python main.py stats-annees
```

Les effectifs sont lus en priorité depuis le cache (`data/cache/naiades_analyses_21.json`, `ades_analyses_21.json`), sinon depuis la couche SIG exportée. Exemple de sortie :

- 2008 : x analyses (Naïades: …, ADES: …)
- 2009 : x analyses
- …

---

## 2. Page DRAAF « Indicateurs d’impacts des PPP » (eaux brutes)

Page analysée :  
[https://draaf.bourgogne-franche-comte.agriculture.gouv.fr/indicateurs-d-impacts-des-ppp-sur-l-environnement-eau-biodiversite-air-a3482.html](https://draaf.bourgogne-franche-comte.agriculture.gouv.fr/indicateurs-d-impacts-des-ppp-sur-l-environnement-eau-biodiversite-air-a3482.html)

### Liens « Eaux brutes : souterraines et de surface »

- **DREAL BFC** — Suivi et enjeu de la qualité de l’eau :  
  [https://www.bourgogne-franche-comte.developpement-durable.gouv.fr/suivi-et-enjeu-de-la-qualite-de-l-eau-r2761.html](https://www.bourgogne-franche-comte.developpement-durable.gouv.fr/suivi-et-enjeu-de-la-qualite-de-l-eau-r2761.html)  
  → Portail thématique ; pas une API directe. Utile pour rapports et cartes régionales, pas pour alimenter automatiquement phytoDB.

- **Agence de l’eau Loire-Bretagne** — Qualité des eaux :  
  [https://agence.eau-loire-bretagne.fr/.../zoom-sur-la-qualite-des-eaux-en-loire-bretagne-2020.html](https://agence.eau-loire-bretagne.fr/home/bassin-loire-bretagne/zoom-sur-la-qualite-des-eaux-en-loire-bretagne-2020.html)  
  → Côte-d’Or concernée partiellement (bord ouest). Données souvent en synthèses annuelles / rapports.

- **Agence de l’eau Seine-Normandie** — Geo-Seine-Normandie :  
  [https://www.eau-seine-normandie.fr/Naviguez-sur-Geo-Seine-Normandie](https://www.eau-seine-normandie.fr/Naviguez-sur-Geo-Seine-Normandie)  
  → Côte-d’Or en grande partie dans le bassin Seine. Cartographie et données ; à vérifier si export/API ouverts.

- **Agence Rhône-Méditerranée-Corse** — Bilan qualité des eaux :  
  [https://www.rhone-mediterranee.eaufrance.fr/...](https://www.rhone-mediterranee.eaufrance.fr/bilan-annuel-de-la-qualite-des-eaux-des-bassins-rhone-mediterranee-et-corse)  
  → Hors bassin Côte-d’Or (sud-est France).

### Conclusion pour enrichir le programme (données récentes)

- **Hub'Eau (Naïades + ADES)** reste la source programmatique principale utilisée par phytoDB ; elle agrège déjà les données des bassins (dont Seine). Les anciennes données en base expliquent que des prélèvements remontent à plusieurs années.
- **Pour privilégier les données récentes** :
  1. **Configurer une fenêtre temporelle** dans `config.yaml` :  
     `ppp.naiades` et `ppp.ades` : renseigner `date_debut` et `date_fin` (ex. `date_fin: "2025-12-31"`) pour limiter les analyses aux années souhaitées.
  2. **Augmenter le volume récupéré** : lancer `python main.py run --naiades-analyses --ades-analyses` avec des `--max-pages-*` plus élevés pour avoir plus de pages récentes (les API Hub'Eau paginent par date/ordre).
  3. **DREAL BFC** : consulter le lien pour les bilans et indicateurs régionaux récents ; pas d’intégration API identifiée dans la page.
  4. **Geo-Seine-Normandie (AESN)** : à explorer pour exports ou API dédiés au bassin Seine (données potentiellement plus à jour ou complémentaires).
  5. **ARS** (eaux distribuées) et **Atmo BFC** (air) sont cités sur la page DRAAF mais concernent l’eau du robinet et l’air, pas les eaux brutes PPP.

Aucune nouvelle source « eaux brutes » directement exploitable en API n’a été trouvée sur cette page ; les pistes sont la configuration des dates dans phytoDB et l’exploration des portails DREAL / AESN pour rapports ou exports récents.
