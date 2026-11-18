# Organisation du Data Lake Retail Intelligence

## 1. Architecture HDFS

Le projet utilise Apache Hadoop 3.4.3 avec HDFS dans Docker.

- NameNode : `retail-namenode`
- DataNode : `retail-datanode`
- URI HDFS : `hdfs://localhost:9000`
- Réplication : 1

## 2. Organisation des zones

### Zone historique `/retail`

- `/retail/bronze/data` : données initiales importées.
- `/retail/bronze/data_refresh` : données de mise à jour.
- `/retail/silver` : zone prévue pour les données nettoyées.
- `/retail/gold` : zone prévue pour les données préparées pour l’analyse.
- `/retail/quarantine` : zone prévue pour les données rejetées.
- `/retail/metadata` : zone prévue pour les métadonnées.

### Zone Raw `/retail-lake`

- `/retail-lake/raw/tpcxbb/initial` : dépôt manuel des données TPCx-BB initiales.
- `/retail-lake/raw/tpcxbb/refresh` : dépôt manuel des données TPCx-BB de mise à jour.
- `/retail-lake/raw/nifi` : données déposées par le flux Apache NiFi, dont `store_sales.dat`.

Les chemins `/retail` et `/retail-lake` sont deux arborescences HDFS distinctes. Les dépôts ont été conservés séparément.

## 3. Contrôle du dépôt UC-03

Les données locales `data` et `data_refresh` ont été copiées dans les destinations Raw correspondantes.

- Initial : 24 fichiers, environ 977 Mo.
- Refresh : 12 fichiers, environ 9,1 Mo.
- Total : 36 fichiers.
- Vérification SHA-256 : 36 fichiers conformes sur 36.
- Anomalies détectées : 0.

Le rapport détaillé est disponible dans `preuves/j2/hdfs/controle-integrite-sha256.txt`.

## 4. Ingestion NiFi UC-04

Le flux NiFi utilise `GetFile` puis `PutHDFS` pour déposer `store_sales.dat` dans `/retail-lake/raw/nifi`. La provenance et les captures sont conservées dans `docs/ingestion` et `preuves/j2/nifi`.

## 5. Évolutions prévues

Les zones Silver et Gold sont créées mais leur transformation n’est pas réalisée dans ce périmètre. Elles pourront accueillir les traitements ultérieurs.
