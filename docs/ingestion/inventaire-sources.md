# Inventaire des sources TPCx-BB — Jour 2

## Provenance

- Répertoire source : `/Users/lepaleonthologue/Downloads/retail_project`
- Jeu de données : TPCx-BB fourni pour le projet.
- Sources originales conservées sans modification.
- Fichiers `.DS_Store` exclus.
- Les fichiers `.dat` sont délimités par `|`.
- Les champs vides finaux sont conservés dans le comptage.

## Source `data`

- Domaines : 23
- Fichiers : 24
- Volume : 1,024,448,074 octets

| Domaine | Fichier | Format | Taille (octets) | Lignes | Colonnes |
|---|---|---|---:|---:|---:|
| customer | `customer.dat` | .dat | 13279063 | 99000 | 18 |
| customer_address | `customer_address.dat` | .dat | 5387229 | 49500 | 13 |
| customer_demographics | `customer_demographics.dat` | .dat | 77753083 | 1920800 | 9 |
| date_dim | `date_dim.dat` | .dat | 15395800 | 109573 | 28 |
| household_demographics | `household_demographics.dat` | .dat | 155100 | 7200 | 5 |
| income_band | `income_band.dat` | .dat | 327 | 20 | 3 |
| inventory | `inventory.dat` | .dat | 424943678 | 23255100 | 4 |
| item | `item.dat` | .dat | 6799286 | 17820 | 22 |
| item_marketprices | `item_marketprices.dat` | .dat | 4201170 | 89100 | 6 |
| item_marketprices | `item_marketprices_audit.csv` | .csv | 273 | 5 lignes physiques | 8 dans l'en-tête |
| product_reviews | `product_reviews.dat` | .dat | 56355389 | 89991 | 8 |
| promotion | `promotion.dat` | .dat | 46396 | 300 | 19 |
| reason | `reason.dat` | .dat | 3105 | 35 | 3 |
| ship_mode | `ship_mode.dat` | .dat | 1220 | 20 | 6 |
| store | `store.dat` | .dat | 3386 | 12 | 29 |
| store_returns | `store_returns.dat` | .dat | 4337400 | 37902 | 20 |
| store_sales | `store_sales.dat` | .dat | 92764700 | 667579 | 23 |
| time_dim | `time_dim.dat` | .dat | 5107780 | 86400 | 10 |
| warehouse | `warehouse.dat` | .dat | 584 | 5 | 14 |
| web_clickstreams | `web_clickstreams.dat` | .dat | 178722140 | 6770550 | 6 |
| web_page | `web_page.dat` | .dat | 8040 | 60 | 14 |
| web_returns | `web_returns.dat` | .dat | 5383513 | 38487 | 24 |
| web_sales | `web_sales.dat` | .dat | 133790574 | 668052 | 34 |
| web_site | `web_site.dat` | .dat | 8838 | 30 | 26 |

## Source `data_refresh`

- Domaines : 11
- Fichiers : 12
- Volume : 9,545,513 octets

| Domaine | Fichier | Format | Taille (octets) | Lignes | Colonnes |
|---|---|---|---:|---:|---:|
| customer | `customer.dat` | .dat | 134360 | 1000 | 18 |
| customer_address | `customer_address.dat` | .dat | 54587 | 500 | 13 |
| inventory | `inventory.dat` | .dat | 4292915 | 234900 | 4 |
| item | `item.dat` | .dat | 69173 | 180 | 22 |
| item_marketprices | `item_marketprices.dat` | .dat | 42547 | 900 | 6 |
| item_marketprices | `item_marketprices_audit.csv` | .csv | 151009 | 17986 lignes physiques | 8 dans l'en-tête |
| product_reviews | `product_reviews.dat` | .dat | 573685 | 909 | 8 |
| store_returns | `store_returns.dat` | .dat | 48825 | 426 | 20 |
| store_sales | `store_sales.dat` | .dat | 922171 | 6632 | 23 |
| web_clickstreams | `web_clickstreams.dat` | .dat | 1821864 | 68988 | 6 |
| web_returns | `web_returns.dat` | .dat | 55952 | 400 | 24 |
| web_sales | `web_sales.dat` | .dat | 1378425 | 6879 | 34 |

## Observations

- Les fichiers `.dat` ne présentent pas d'en-tête dans les échantillons inspectés.
- Les fichiers CSV d'audit sont identifiés séparément. Ils contiennent des sections de contrôle et des listes d'identifiants ; leur nombre de lignes physiques ne correspond pas à un nombre homogène d'enregistrements.
- Les deux sources `data` et `data_refresh` restent distinctes.
- Les champs vides en fin de ligne ne sont pas supprimés.
- Les relations entre tables restent à documenter.

## Limites

- Les noms et types métier des colonnes restent à rapprocher du schéma TPCx-BB.
- Le contrôle du nombre de colonnes ne remplace pas une validation métier.
