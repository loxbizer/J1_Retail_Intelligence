# TP Jour 3 — Retail Intelligence Platform (UC-06)

Chaîne de transformation des données TPCx-BB : de la zone Bronze du Jour 2
aux zones Silver et Gold, avec contrôles qualité et stockage Parquet.

## 1. Prérequis

| Composant | Rôle |
| --- | --- |
| Python 3.11 ou plus | langage des traitements |
| Java 17, 21 ou 25 | machine virtuelle de Spark |
| PySpark 4.3.0 | moteur de traitement |
| Docker | héberge le cluster HDFS mis en place au Jour 2 |

Installation :

```bash
cd ~/retail-intelligence/tp3
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## 2. Versions réellement observées

Relevé du 18/11/2026, détail complet dans
`docs/environment/fiche-environnement-tp3.txt`.

| | |
| --- | --- |
| Machine | MacBook Pro Apple Silicon, macOS, 12 cœurs, 16 Go |
| Python | 3.15.2 |
| Java | OpenJDK 25.0.5 LTS (Temurin) |
| PySpark | 4.3.0 |
| Hadoop | 3.4.3 (Docker, conteneurs `retail-namenode` et `retail-datanode`) |
| Mode d'exécution | `local[*]` — calcul local, stockage HDFS distribué |

La matrice annoncée par Spark 4.3 est Java 17/21/25 et Python 3.11+. La
combinaison installée y est conforme.

## 3. Emplacement des données

| Zone | Chemin |
| --- | --- |
| Bronze (entrée, non modifiée) | `hdfs://localhost:9000/retail/bronze/data/<table>/<table>.dat` |
| Silver | `hdfs://localhost:9000/retail/silver/<domaine>/<table>` |
| Gold | `hdfs://localhost:9000/retail/gold/<jeu>` |
| Quarantaine | `hdfs://localhost:9000/retail/quarantine/<table>` |
| Repli local | `~/Downloads/retail_project/data` |

Sources traitées : `store_sales` (667 579 lignes), `item` (17 820),
`store` (12), `date_dim` (109 573).

## 4. Paramètres

Tout est centralisé en tête de `pipeline_tp3.py`, section 1 :
chemins HDFS, séparateur `|`, absence d'en-tête, schémas explicites des
quatre tables, colonnes obligatoires et clé de chaque source. Aucun chemin
n'est écrit en dur dans les traitements.

Deux réglages nécessaires et non évidents :

- `HADOOP_USER_NAME=root`, défini avant la session Spark : les zones `/retail`
  ont été créées par le conteneur, qui tourne en root.
- `spark.hadoop.dfs.client.use.datanode.hostname=true` : le NameNode
  communique au client l'adresse interne du DataNode dans le réseau Docker,
  que la machine hôte ne sait pas joindre.

## 5. Ordre d'exécution

```bash
source .venv/bin/activate
python fiche_environnement.py     # relevé de l'environnement
python 01_explorer.py             # inventaire et exploration des 4 sources
python pipeline_tp3.py            # les 5 étapes suivantes, dans l'ordre
```

`pipeline_tp3.py` accepte aussi une étape isolée :

```bash
python pipeline_tp3.py cles       # analyse de la clé de store_sales
python pipeline_tp3.py silver     # Bronze -> Silver
python pipeline_tp3.py gold       # Silver -> Gold
python pipeline_tp3.py qualite    # 20 contrôles qualité
python pipeline_tp3.py formats    # comparaison CSV / JSON / Parquet
```

L'ordre compte : `gold` lit la zone Silver **écrite sur disque**, pas un
DataFrame en mémoire. `qualite` contrôle Silver et Gold, donc vient après les
deux. Si les zones de sortie n'existent pas encore en écriture :

```bash
docker exec retail-namenode hdfs dfs -chmod -R 777 \
  /retail/silver /retail/gold /retail/quarantine
```

## 6. Sorties produites

| Chemin | Contenu | Grain |
| --- | --- | --- |
| `/retail/silver/sales/store_sales` | ventes nettoyées et tracées | un article vendu sur un ticket |
| `/retail/silver/products/item` | référentiel produit | un article |
| `/retail/silver/stores/store` | référentiel magasin | un magasin |
| `/retail/silver/time/date_dim` | référentiel temps | un jour |
| `/retail/gold/ca_mensuel_magasin` | CA, marge, quantités, transactions, panier moyen, évolution | un mois × un magasin |
| `/retail/gold/top_produits_quantite` | quantités et CA par article | un article |

Documentation et preuves :

| Chemin | Contenu |
| --- | --- |
| `docs/environment/` | fiche environnement |
| `docs/exploration/` | rapport d'exploration, rapport des clés |
| `docs/transformations/` | rapport Silver, rapport Gold, comparaison des formats |
| `docs/quality/` | rapport qualité, en texte et en CSV |
| `docs/DOCUMENTATION-J3.md` | inventaire, règles de nettoyage, fiches Gold, questions de recul |
| `outputs/comparaison_formats/` | les mêmes données en CSV, JSON et Parquet |
| `preuves/j3/` | captures et sorties de commandes |

## 7. Contrôles disponibles

```bash
python pipeline_tp3.py qualite
```

20 contrôles sur sept dimensions : complétude, unicité, validité des types,
cohérence des valeurs, volumes, clés candidates, intégrité référentielle.
Résultat du 18/11/2026 : **19 conformes, 1 anomalie**.

L'anomalie est l'unicité de la clé de `store_sales` — 4 136 lignes partagent
un couple (ticket, article). Elle est analysée dans
`docs/DOCUMENTATION-J3.md`, section 3 : ce ne sont pas des doublons mais des
lignes de vente distinctes, et aucune suppression n'a été faite.

Deux équations de bouclage sont vérifiées à chaque exécution :

- Bronze = Silver + quarantaine, pour les quatre tables ;
- somme des agrégats Gold = somme du détail Silver, soit 1 150 529 586,37 des
  deux côtés, écart 0,00.

## 8. Limites connues

- **Calcul local, pas distribué.** Spark tourne en `local[*]` sur une seule
  machine. Le stockage, lui, est bien distribué via HDFS. Aucun cluster Spark
  n'a été utilisé, et le TP n'évalue pas l'administration d'un cluster.
- **Périmètre réduit** à quatre sources sur les vingt-quatre du kit. Le canal
  web, les retours, les stocks, la navigation et les avis ne sont pas traités.
- **Jeu de rafraîchissement non traité** (`data_refresh`, 12 fichiers, 9,1 Mo).
- **Données synthétiques TPCx-BB** : aucune conclusion commerciale. Le profit
  net est négatif sur 74 % des lignes, l'unité monétaire n'est pas documentée,
  et les libellés produits sont du texte généré.
- **Parquet n'est pas une table transactionnelle.** Pas d'ACID, pas de
  versions, pas de retour arrière : ces garanties demanderaient une couche
  format de table (Delta Lake, Iceberg, Hudi), hors du socle obligatoire.
- `store_sales` **n'a pas de clé naturelle unique** dans les données fournies.
  Son grain est la ligne de vente.
