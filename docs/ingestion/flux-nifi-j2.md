# Flux d'ingestion NiFi — Jour 2

## Schéma logique

GetFile → PutHDFS

La relation failure de PutHDFS est dirigée vers LogAttribute pour faciliter le diagnostic.

## Fonctionnement
1. GetFile surveille le répertoire d'entrée local.
2. Le fichier est transformé en FlowFile.
3. La connexion transporte le FlowFile vers PutHDFS.
4. PutHDFS écrit le fichier dans le répertoire HDFS.
5. La relation failure permet d'observer les erreurs via LogAttribute.

## Destination
hdfs://localhost:9000/retail-lake/raw/nifi/store_sales.dat

## Paramètres de collecte
- Keep Source File : false
- Recurse Subdirectories : false

## Résultat constaté
Transfert réussi de store_sales.dat, environ 88,47 MB.
La présence et la lisibilité du fichier ont été contrôlées avec les commandes HDFS.
