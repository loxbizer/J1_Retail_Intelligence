# J1 Retail Intelligence

TP Jour 1 — Retail Intelligence Platform

## UC-01 — Besoin et exploration

Livrables de cadrage, questions décisionnelles, KPI, inventaire, qualification et limites.

## UC-02 — Architecture

Comparaison des architectures, architecture cible, flux, positionnement des composants, gouvernance et trajectoire locale vers Azure.

Les choix non vérifiés sont identifiés comme propositions ou points à valider.

La cible Azure est une conception logique et non un déploiement réalisé.

## TP Jour 2 — Hadoop, HDFS et NiFi

### Environnement
- macOS
- Java : Eclipse Temurin 21.0.13
- Hadoop : Apache Hadoop 3.4.3 via Docker
- NiFi : Apache NiFi 2.12.0
- NameNode : `hdfs://localhost:9000`
- Configuration : `infrastructure/hadoop/docker-compose.yml`

### Dépôt HDFS — UC-03

- Sources initiales : `/retail-lake/raw/tpcxbb/initial`
- Sources de rafraîchissement : `/retail-lake/raw/tpcxbb/refresh`
- Contrôle : 24 fichiers initiaux et 12 fichiers de rafraîchissement.
- Intégrité : 36 fichiers sur 36 conformes au contrôle SHA-256.
- Rapport : `preuves/j2/hdfs/controle-integrite-sha256.txt`
- Arborescence et volumes : `preuves/j2/hdfs/arborescence-hdfs.txt`


### Démarrage et arrêt
Démarrer Hadoop : `docker compose -f infrastructure/hadoop/docker-compose.yml up -d`

Vérifier HDFS : `docker exec retail-namenode hdfs dfsadmin -report`

Démarrer NiFi : `~/Applications/nifi-2.12.0/bin/nifi.sh start`

Accès NiFi : `https://localhost:8443/nifi`

Arrêter NiFi : `~/Applications/nifi-2.12.0/bin/nifi.sh stop`

Arrêter Hadoop : `docker compose -f infrastructure/hadoop/docker-compose.yml down`

### Stockage HDFS
- `/retail/bronze/data` et `/retail/bronze/data_refresh` contiennent les données Bronze.
- Le flux NiFi a déposé `store_sales.dat` dans `/retail-lake/raw/nifi/`.
- `/retail` et `/retail-lake` sont deux chemins distincts.

### Flux NiFi réalisé
`GetFile → PutHDFS`. La relation `failure` de PutHDFS est reliée à LogAttribute ; `success` est auto-terminée. GetFile utilise `ingestion/nifi/input`, Keep Source File=false et Recurse=false. Le dépôt HDFS a été vérifié et le fichier était lisible.

### Tests et limites
Un répertoire source inexistant a provoqué une erreur de validation, puis le chemin valide a été restauré. Un conflit de nom de fichier a également été observé lors d’une nouvelle tentative. Un événement SEND a été consulté dans Provenance. Le SHA-256 du fichier local et celui du fichier HDFS ont été comparés : les deux empreintes sont identiques.

### Preuves et documentation
- `docs/architecture/` : organisation du Data Lake.
- `docs/ingestion/` : découverte, flux, inventaire et Provenance NiFi.
- `docs/operations/` : environnement et erreurs/corrections.
- `preuves/j2/hadoop/`, `preuves/j2/hdfs/`, `preuves/j2/nifi/` : captures et preuves du TP.

Les manipulations non réalisées ou non vérifiées ne sont pas présentées comme acquises.
