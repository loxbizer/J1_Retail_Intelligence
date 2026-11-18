# Erreurs et corrections — TP Jour 2

| Difficulté | Constat | Action / correction |
|---|---|---|
| Permissions HDFS | L'utilisateur NiFi ne pouvait pas écrire dans le répertoire cible | Propriétaire du répertoire corrigé avec hdfs dfs -chown |
| Relation failure | PutHDFS était invalide sans traitement de la relation | Relation failure reliée à LogAttribute |
| Fichier déjà existant | PutHDFS a rejeté un doublon store_sales.dat | Conservation du premier dépôt, arrêt des nouvelles tentatives et nettoyage de la file |
| Répertoire d'entrée inexistant | NiFi a invalidé le chemin dossier_inexistant | Chemin d'entrée initial restauré et configuration appliquée |

## Test contrôlé
Un chemin d'entrée inexistant a été configuré temporairement.
NiFi a signalé que le répertoire n'existait pas.
Le chemin valide a ensuite été restauré.

## Conflit de nom
Une nouvelle tentative de dépôt a rencontré un fichier déjà existant.
Le premier fichier HDFS a été conservé.
