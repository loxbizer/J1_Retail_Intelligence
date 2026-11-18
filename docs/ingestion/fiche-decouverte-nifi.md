# Fiche de découverte Apache NiFi — TP Jour 2

## Environnement
- Version : Apache NiFi 2.12.0
- Java : Eclipse Temurin 21.0.13 LTS
- Installation : ~/Applications/nifi-2.12.0
- État constaté : service UP, réponse HTTP 200

## Concepts utilisés
- FlowFile : unité de données traitée par NiFi.
- Processor : composant réalisant une opération.
- Connection : liaison transportant les FlowFiles.
- Queue : file d'attente entre processeurs.
- Process Group : regroupement logique des composants.
- Controller Service : service partagé configurant des ressources.
- Provenance Repository : historique des événements des FlowFiles.

## Flux réalisé
GetFile → PutHDFS

Un processeur LogAttribute a également été utilisé pour traiter la relation failure.

## Source
- Fichier : store_sales.dat
- Format : données délimitées par le caractère |
- Taille constatée : environ 88,47 MB
- Dossier d'entrée configuré : ingestion/nifi/input

## GetFile
- Keep Source File : false
- Recurse Subdirectories : false
- Rôle : récupérer un fichier du dossier local d'entrée.

## PutHDFS
- Rôle : déposer le contenu du FlowFile dans HDFS.
- Destination : /retail-lake/raw/nifi
- Ressource Hadoop : configuration renseignée dans le processeur.

## Résultat
Le fichier store_sales.dat a été déposé dans HDFS et sa lecture a été vérifiée.
Le fichier est présent à :
/retail-lake/raw/nifi/store_sales.dat

Taille HDFS constatée : 88,5 MB.

## Limites
La comparaison SHA-256 du fichier local `data/store_sales/store_sales.dat` avec `/retail-lake/raw/nifi/store_sales.dat` a été réalisée. Les deux empreintes sont identiques : `7e0a7f16b47483741754bb641c92601dd7384dacc8331d0bcfb8abac82ed3d2e`.
