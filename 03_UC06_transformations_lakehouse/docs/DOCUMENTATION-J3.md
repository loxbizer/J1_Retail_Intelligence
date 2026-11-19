# Retail Intelligence Platform — Jour 3 (UC-06)

Transformation, qualité des données et Data Lakehouse.
Relevés du 18/11/2026. Toutes les mesures de ce document proviennent des
rapports produits dans `docs/`, pas d'estimations.

Environnement : Spark 4.3.0 en `local[*]`, Java 25.0.5, Python 3.15.2.
Stockage : HDFS Hadoop 3.4.3 (Docker), `hdfs://localhost:9000`.
Traitements exécutés sur les données HDFS, pas sur un échantillon local.

---

## 1. Inventaire technique des sources (livrable 2)

Quatre sources retenues : une table de faits et trois dimensions. Les gros
fichiers du kit (`inventory` 405 Mo, `web_clickstreams` 170 Mo) sont écartés :
volume inutile aux questions traitées, et `web_clickstreams` ne se rattache
aux ventes par aucun identifiant fiable.

### store_sales

| Information | Valeur |
| --- | --- |
| Nom réel du fichier | `store_sales.dat` |
| Emplacement | `hdfs://localhost:9000/retail/bronze/data/store_sales/` |
| Format | texte délimité `\|`, sans ligne d'en-tête |
| Taille | 88,5 Mo |
| Nombre de lignes | 667 579 |
| Nombre de colonnes | 23 |
| Colonnes principales | `ss_sold_date_sk`, `ss_store_sk`, `ss_item_sk`, `ss_ticket_number`, `ss_quantity`, `ss_net_paid`, `ss_net_profit` |
| Types observés | 11 entiers (clés, ticket, quantité), 12 décimaux (coûts, prix, montants) |
| Clé candidate | aucune clé naturelle unique — voir section 3 |
| Granularité | une ligne = un article vendu sur un ticket, dans un magasin, à une date |
| Anomalies identifiées | 4 136 lignes partagent un couple (ticket, article) ; nuls sur `ss_cdemo_sk` (238), `ss_hdemo_sk` (295), `ss_addr_sk` (448), `ss_promo_sk` (158) ; pas de séparateur en fin de ligne (23 champs exactement) |

### item

| Information | Valeur |
| --- | --- |
| Nom réel du fichier | `item.dat` |
| Emplacement | `hdfs://localhost:9000/retail/bronze/data/item/` |
| Format | texte délimité `\|`, sans en-tête |
| Taille | 6,5 Mo |
| Nombre de lignes | 17 820 |
| Nombre de colonnes | 22 |
| Colonnes principales | `i_item_sk`, `i_item_id`, `i_product_name`, `i_category`, `i_brand`, `i_current_price` |
| Types observés | entiers (clés, identifiants de marque et de classe), décimaux (prix, coût), texte (libellés), date (`i_rec_start_date`) |
| Clé candidate | `i_item_sk` — **unique**, 17 820 valeurs distinctes sur 17 820 lignes |
| Granularité | une ligne = un article du référentiel |
| Anomalies identifiées | `i_rec_end_date` nulle à 100 % ; `i_item_desc` (4), `i_brand_id` (8), `i_brand` (6) nuls |

### store

| Information | Valeur |
| --- | --- |
| Nom réel du fichier | `store.dat` |
| Emplacement | `hdfs://localhost:9000/retail/bronze/data/store/` |
| Format | texte délimité `\|`, sans en-tête |
| Taille | 3 Ko |
| Nombre de lignes | 12 |
| Nombre de colonnes | 29 |
| Colonnes principales | `s_store_sk`, `s_store_id`, `s_store_name`, `s_state`, `s_market_id`, `s_number_employees` |
| Types observés | entiers, texte, date (`s_rec_start_date`), décimal (`s_tax_precentage`) |
| Clé candidate | `s_store_sk` — **unique**, 12 sur 12 |
| Granularité | une ligne = un magasin |
| Anomalies identifiées | `s_rec_end_date` nulle à 100 % ; `s_tax_precentage` est une faute d'orthographe de la spécification, conservée telle quelle pour ne pas rompre la correspondance avec la source |

### date_dim

| Information | Valeur |
| --- | --- |
| Nom réel du fichier | `date_dim.dat` |
| Emplacement | `hdfs://localhost:9000/retail/bronze/data/date_dim/` |
| Format | texte délimité `\|`, sans en-tête |
| Taille | 14,7 Mo |
| Nombre de lignes | 109 573 |
| Nombre de colonnes | 28 |
| Colonnes principales | `d_date_sk`, `d_date`, `d_year`, `d_moy`, `d_qoy`, `d_day_name` |
| Types observés | entiers, texte, date (`d_date`) |
| Clé candidate | `d_date_sk` — **unique**, 109 573 sur 109 573 |
| Granularité | une ligne = un jour calendaire |
| Anomalies identifiées | aucune valeur nulle ; le référentiel couvre de 1900 à 2100, bien au-delà de la période réellement vendue (2001–2005) |

### Réponses aux questions techniques de l'étape 2

1. **Structurés, semi-structurés ou non structurés ?** Les quatre sont **structurés** : tabulaires, nombre de champs constant. Mais sans schéma embarqué — la structure est dans la spécification, pas dans le fichier. Le kit contient aussi du semi-structuré (`web_clickstreams`) et du non structuré (`product_reviews`), non traités ici.
2. **Colonnes correctement typées ?** Non à la source : un fichier texte n'a pas de types. L'inférence Spark a proposé des types corrects, puis ces types ont été **déclarés explicitement** avant la production de Silver.
3. **Valeurs nulles ?** Oui, et légitimes : clés de démographie, d'adresse et de promotion sur moins de 0,1 % des ventes ; `i_rec_end_date` et `s_rec_end_date` nulles à 100 %, ce qui signifie qu'aucun enregistrement n'est clôturé.
4. **Identifiants uniques quand attendu ?** Oui pour les trois dimensions, prouvé par comptage. Non pour la table de faits, qui n'a pas de clé naturelle — section 3.
5. **Granularité connue ?** Oui, et vérifiée : une ligne de `store_sales` est un article vendu sur un ticket, et non une transaction. 667 579 lignes pour 89 100 tickets distincts, soit 7,5 articles par ticket en moyenne.
6. **Données adaptées aux questions du Jour 1 ?** Q1, Q2, Q4 et Q5 sont couvertes par ces quatre sources. Q3 l'est par la jointure avec `item`. Q6 (retours) nécessiterait `store_returns`, hors périmètre du jour.

---

## 2. Règles de nettoyage (livrable 4)

Trois interdits tenus : aucune suppression arbitraire de ligne, aucun
remplacement de valeur sans règle explicite, aucune jointure sur une
ressemblance de noms.

| Anomalie | Règle | Action appliquée | Justification | Mesure |
| --- | --- | --- | --- | --- |
| Types non portés par le fichier | les types sont déclarés, pas devinés | `StructType` explicite à la lecture des 4 sources | l'inférence est une proposition, pas une validation ; un schéma déclaré rend la lecture déterministe et reproductible | 0 écart de type après relecture Parquet |
| Noms de colonnes absents | les noms officiels ne sont appliqués qu'après deux vérifications | contrôle du nombre de colonnes, puis preuve arithmétique `ss_ext_sales_price = ss_quantity × ss_sales_price` | une relation supposée à partir d'un nom n'est pas une relation démontrée | 23/23, 22/22, 29/29, 28/28 colonnes ; identité vérifiée sur 1000/1000 lignes |
| Espaces parasites dans les libellés | un libellé ne porte pas d'espaces de bord | `trim()` sur toutes les colonnes texte | harmonisation de forme, aucune information perdue | 12 colonnes texte sur `store_sales`, 13 sur `item`, 19 sur `store`, 15 sur `date_dim` |
| Clé obligatoire nulle | une mesure sans clé est inexploitable | mise à l'écart vers `/retail/quarantine`, pas suppression | la ligne ne peut être ni jointe ni attribuée ; écartée, elle reste auditable | 0 ligne concernée — la règle existe et n'a eu à s'appliquer sur aucune ligne |
| `ss_cdemo_sk`, `ss_hdemo_sk`, `ss_addr_sk`, `ss_promo_sk` nuls | une clé secondaire absente n'invalide pas la vente | **conservées**, valeur nulle laissée telle quelle | une vente sans profil démographique ni promotion reste une vente ; remplacer par 0 créerait un faux client et une fausse promotion | 238 / 295 / 448 / 158 lignes, soit moins de 0,1 % |
| `i_rec_end_date` et `s_rec_end_date` nulles à 100 % | une date de fin vide signifie « enregistrement courant » | **conservées**, typées en date | c'est la convention d'historisation de la source ; le référentiel ne contient qu'une version par article et par magasin | 17 820 et 12 lignes, soit la totalité |
| Profit net négatif | un montant négatif n'est pas une erreur | **conservé**, et marqué par la colonne `ind_montant_negatif` | un profit négatif est une vente à perte, cas métier réel ; le supprimer effacerait de l'information et biaiserait la marge | 495 971 lignes sur 667 579, soit 74,3 % |
| Couple (ticket, article) non unique | un doublon ne se supprime que si sa définition est claire | **aucune suppression** ; grain redéfini et écart documenté | aucune ligne n'est répétée à l'identique, et les lignes concernées diffèrent sur les quantités et les prix : ce sont des lignes de vente distinctes — section 3 | 4 038 groupes, 4 136 lignes en excès, 0 ligne strictement identique |
| Période du référentiel plus large que la période vendue | une date doit exister au référentiel | contrôle d'intégrité, aucune exclusion nécessaire | `date_dim` couvre 1900–2100 ; les ventes couvrent 2001–2005 | 0 vente hors plage |

### Ce qui n'a **pas** été fait, volontairement

- aucun `dropna()`, aucun `dropDuplicates()` ;
- aucun remplacement de nul par zéro ;
- aucune écriture dans la zone Bronze ;
- aucune suppression de ligne, dans aucune des quatre sources.

---

## 3. Analyse de la clé de `store_sales`

La spécification TPC-DS déclare `(ss_item_sk, ss_ticket_number)` comme clé
primaire de la table. Le contrôle montre un écart de 4 136 lignes. Avant
d'écrire une règle, trois tests ont été menés.

**Test 1 — clés candidates élargies.**

| Clé testée | Valeurs distinctes | Écart |
| --- | --- | --- |
| `ss_ticket_number` | 89 100 | 578 479 |
| `ss_ticket_number` + `ss_item_sk` | 663 443 | 4 136 |
| + `ss_sold_date_sk` | 663 443 | 4 136 |
| + `ss_store_sk` | 663 443 | 4 136 |
| + les deux | 663 443 | 4 136 |

Ajouter la date et le magasin ne change rien. Les lignes concernées partagent
donc le même ticket, le même article, la même date et le même magasin.

**Test 2 — lignes strictement identiques.** 667 579 lignes, 667 579 distinctes
sur les 23 colonnes : **aucune ligne n'est répétée à l'identique**.

**Test 3 — observation directe d'un groupe.** Sur un groupe de 4 lignes
partageant ticket et article, les colonnes qui diffèrent sont `ss_promo_sk`,
`ss_quantity`, `ss_wholesale_cost`, `ss_list_price`, `ss_sales_price`,
`ss_coupon_amt` et tous les montants étendus. Le client, la date, l'heure et
le magasin sont identiques.

**Conclusion.** Il s'agit du même article vendu plusieurs fois sur le même
ticket, en lignes distinctes, avec une promotion et un prix différents à
chaque ligne. C'est un cas réel en caisse. Le grain de la table est donc
**la ligne de vente**, plus fin que le couple (ticket, article), et la table
ne possède pas de clé naturelle unique dans les données fournies.

**Décision.** Aucune déduplication. L'écart de 4 136 est conservé comme
anomalie documentée dans le rapport qualité. Si une clé technique devenait
nécessaire pour un usage aval, elle serait ajoutée en Silver sous forme de
numéro d'occurrence au sein du groupe (ticket, article), et non obtenue en
supprimant des lignes.

---

## 4. Zone Silver (livrable 6)

| Table | Bronze | Silver | Quarantaine | Destination |
| --- | --- | --- | --- | --- |
| `store_sales` | 667 579 | 667 579 | 0 | `/retail/silver/sales/store_sales` |
| `item` | 17 820 | 17 820 | 0 | `/retail/silver/products/item` |
| `store` | 12 | 12 | 0 | `/retail/silver/stores/store` |
| `date_dim` | 109 573 | 109 573 | 0 | `/retail/silver/time/date_dim` |

**Équation de conservation : Bronze = Silver + quarantaine, vérifiée sur les
quatre tables.** Aucune ligne n'a disparu sans décision tracée.

Format d'écriture : Parquet. Convention de nommage :
`/retail/silver/<domaine>/<table>`, en minuscules, sans accent ni espace, sans
date dans le nom de répertoire.

**Traçabilité.** Trois colonnes sont ajoutées à chaque table :

- `src_fichier` — chemin HDFS du fichier d'origine, ligne par ligne, via `input_file_name()` ;
- `src_systeme` — système et table source (`TPCx-BB / store_sales`) ;
- `dt_traitement` — horodatage du traitement.

**Contrôles exécutés après écriture**, pour chaque table : relecture,
nombre de lignes, nombre de colonnes, conformité des types au schéma déclaré,
équation de conservation. Tous conformes.

---

## 5. Jeux Gold (livrable 7)

### 5.1 `gold/ca_mensuel_magasin`

| Élément | Contenu |
| --- | --- |
| Finalité métier | Q1 — évolution du chiffre d'affaires sur les périodes ; Q2 — contribution de chaque magasin ; Q4 — évolution entre deux périodes comparables ; Q5 — panier moyen |
| Sources | `silver.store_sales`, `silver.date_dim`, `silver.store` |
| Granularité | une ligne = un mois × un magasin |
| Dimensions | `d_year`, `d_moy`, `s_store_sk`, `s_store_name`, `s_state` |
| Mesures | `ca_net` (KPI 1), `marge_nette`, `quantite` (KPI 4), `nb_transactions` (KPI 2), `nb_lignes_vente`, `panier_moyen` (KPI 3), `ca_mois_precedent`, `evolution_pct` (KPI 5) |
| Règles de calcul | sommes sur les lignes Silver ; `nb_transactions` = nombre de `ss_ticket_number` **distincts** ; `panier_moyen` = `ca_net / nb_transactions` ; `evolution_pct` = variation par rapport au mois précédent du même magasin, calculée par fenêtre |
| Jointures | `date_dim` en `inner` (une vente sans date valide ne peut être placée dans un mois ; 0 cas constaté), `store` en `left` (une vente sans magasin au référentiel resterait du chiffre d'affaires ; 0 cas constaté) |
| Contrôle de bouclage | somme Gold = 1 150 529 586,37 = somme Silver du détail. **Écart : 0,00** — ni perte ni duplication |

Les trois KPI du dictionnaire du Jour 1 sont implantés ici. Le `countDistinct`
sur le numéro de ticket répond directement à la limite qui y était notée :
*une ligne de vente n'est pas nécessairement une transaction*.

### 5.2 `gold/top_produits_quantite`

| Élément | Contenu |
| --- | --- |
| Finalité métier | Q3 — quels produits représentent les plus grandes quantités vendues |
| Sources | `silver.store_sales`, `silver.item` |
| Granularité | une ligne = un article |
| Dimensions | `i_item_sk`, `i_product_name`, `i_category`, `i_brand` |
| Mesures | `quantite_vendue`, `ca_net`, `nb_transactions` |
| Jointure | `item` en `left` sur `ss_item_sk = i_item_sk` ; cardinalité n:1 vérifiée (`i_item_sk` unique) ; lignes avant jointure = lignes après |

### Limites d'interprétation communes aux deux jeux

- Données **synthétiques** TPCx-BB : aucune conclusion commerciale réelle.
- Périmètre **ventes magasin uniquement** ; le canal web (`web_sales`) n'est pas additionné, conformément à la limite posée au Jour 1 sur le KPI 1.
- Les **retours** (`store_returns`) ne sont pas déduits du chiffre d'affaires.
- L'**unité monétaire n'est pas documentée** dans le kit ; elle reste « à confirmer », comme au Jour 1.
- Le **profit net est négatif sur 74,3 %** des lignes : caractéristique du jeu généré, à ne pas lire comme une performance commerciale.
- Période couverte : **2001 à 2005**, 12 magasins, 89 100 tickets.

---

## 6. Comparaison des formats (livrable 8)

Mêmes données écrites dans les trois formats : `gold/ca_mensuel_magasin`.

| Format | Octets | Ko | Rapport au Parquet |
| --- | --- | --- | --- |
| CSV | 58 990 | 57,6 | × 1,89 |
| JSON | 189 424 | 185,0 | × 6,06 |
| **Parquet** | **31 240** | **30,5** | × 1,00 |

| Critère | CSV | JSON | Parquet |
| --- | --- | --- | --- |
| Organisation | une ligne de texte par enregistrement | un document par enregistrement, noms de champs répétés | colonnaire, par blocs, avec statistiques par bloc |
| Types | aucun, tout est texte à interpréter | types de base portés par la syntaxe | schéma typé enregistré dans le fichier |
| Lisibilité | directe | lisible mais verbeuse | binaire |
| Lecture sélective | non, tout le fichier est lu | dépend de la structure | oui, colonne par colonne |
| Usage adapté | échange, export, zone Bronze | semi-structuré, API | analytique, zones Silver et Gold |

Le JSON pèse six fois le Parquet parce qu'il répète le nom de chaque champ sur
chaque enregistrement. Le Parquet a été relu après écriture : schéma conservé
sans inférence, nombre de lignes identique.

**Limite essentielle.** Parquet décrit comment des données sont écrites *dans
un fichier*, pas comment un ensemble de fichiers forme une table. Pas de
commit atomique, pas d'isolation entre lecteurs et écrivains, pas de versions
consultables, pas de retour à un état antérieur. Ces garanties viennent d'une
couche format de table — Delta Lake, Iceberg, Hudi — posée au-dessus des mêmes
fichiers Parquet. Un répertoire Parquet n'est donc pas une table
transactionnelle ACID.

---

## 7. Rapport qualité (livrable 9)

20 contrôles exécutés, **19 conformes, 1 anomalie**. Détail complet dans
`docs/quality/rapport-qualite.txt` et `rapport_qualite_csv/`.

| Dimension | Contrôles | Résultat |
| --- | --- | --- |
| Complétude | 4 clés obligatoires non nulles | 0 manquante |
| Unicité | clé de la table de faits | **4 136 d'écart — anomalie assumée** |
| Validité | types conformes au schéma déclaré, panier moyen calculable | 0 écart |
| Cohérence | quantité strictement positive, date dans la plage du référentiel, bouclage Gold/Silver | 0 écart |
| Volume | Bronze = Silver + quarantaine sur 4 tables | 4 égalités vérifiées |
| Clé candidate | `i_item_sk`, `s_store_sk`, `d_date_sk` | 3 clés uniques |
| Intégrité | ventes → `item`, `store`, `date_dim` | 0 orphelin |

### Anomalie restante

`unicite_cle_vente` : 4 136 lignes partagent un couple (ticket, article).
Analysée en section 3, origine établie, décision prise de ne rien supprimer.
Conséquence pratique : aucune — les agrégats du Gold bouclent exactement avec
le détail Silver, ce qui serait impossible si ces lignes étaient des
duplications.

### Données mises à l'écart

Aucune. La règle de quarantaine est en place et opérationnelle ; aucune ligne
n'a eu à être écartée.

### Limites des contrôles

- Exécutés sur le jeu initial, pas sur le jeu de rafraîchissement (`data_refresh`, 12 fichiers, 9,1 Mo).
- L'intégrité n'est vérifiable que là où un référentiel existe.
- La fraîcheur n'est pas contrôlable : données synthétiques, sans date de chargement réelle.
- Aucun contrôle ne valide la **véracité métier** des montants, seulement leur plausibilité.

---

## 8. Questions de recul

**1. Pourquoi vérifier les schémas avant de transformer ?** Un schéma porte
les noms, les types et le caractère nullable. Sans lui, on transforme une
supposition. Ici c'est décisif : les `.dat` de TPCx-BB n'ont aucune ligne
d'en-tête, les noms de colonnes ne sont nulle part dans les données. Ils ont
été appliqués depuis la spécification puis **prouvés** : nombre de colonnes
conforme, et identité `ss_ext_sales_price = ss_quantity × ss_sales_price`
vérifiée sur 1000 lignes sur 1000. S'ajoute le fonctionnement différé de
Spark : une colonne mal nommée ne se signale qu'à l'action, parfois après
plusieurs minutes de calcul.

**2. Différence entre donnée brute, Silver et Gold ?** Trois
responsabilités. Bronze : les 977 Mo reçus, intacts, avec leur provenance,
jamais modifiés. Silver : même granularité que la source, mais typée,
nettoyée, contrôlée, tracée, réutilisable — 667 579 lignes de ventes avec
leurs trois colonnes de provenance. Gold : grain métier, agrégé pour répondre
à une question identifiée — un mois × un magasin.

**3. Pourquoi connaître la granularité avant d'agréger ?** Parce que le
résultat en dépend entièrement. Une ligne de `store_sales` est un article
vendu, pas une transaction : 667 579 lignes pour 89 100 tickets. Compter les
lignes donnerait un nombre de ventes faux d'un facteur 7,5. Le grain décide
aussi de ce qu'est un doublon, et interdit d'additionner deux mesures de
grains différents.

**4. Quels risques présente une jointure sur une clé supposée ?** La
multiplication des lignes, donc des sommes et des KPI faux, sans aucun message
d'erreur. D'où la vérification systématique avant chaque jointure : unicité de
la clé du côté référentiel (`i_item_sk`, `s_store_sk`, `d_date_sk` prouvées
uniques), absence d'orphelins (0 sur les trois relations), et comparaison du
nombre de lignes avant et après — identique dans les deux jeux Gold. Risque
symétrique avec un `inner` : la perte silencieuse des lignes non rapprochées.

**5. Pourquoi ne pas supprimer automatiquement les valeurs atypiques ?**
Parce qu'une valeur extrême est souvent un cas métier réel. Ici, 495 971
lignes portent un profit net négatif : ce sont des ventes à perte, pas des
erreurs. Les supprimer effacerait 74 % de la table. Même raisonnement pour les
4 136 lignes au couple (ticket, article) répété : elles diffèrent sur les prix
et les quantités, donc ce sont des lignes de vente distinctes.

**6. Quels contrôles vérifient la cohérence d'un jeu transformé ?** Les six
dimensions du cours, plus deux équations de bouclage. Concrètement : comptage
des nuls, `count` contre `distinct` sur les clés, `left_anti` pour
l'intégrité, bornes des mesures, relecture du Parquet pour le schéma et le
volume, Bronze = Silver + quarantaine, et somme des agrégats = somme du
détail — vérifiée au centime : 1 150 529 586,37 des deux côtés.

**7. Pourquoi Parquet est-il adapté aux usages analytiques ?** Parce qu'il est
colonnaire : une requête qui somme deux colonnes sur vingt-trois ne lit que
ces deux colonnes. S'y ajoutent le schéma typé enregistré dans le fichier
— plus d'inférence, plus d'ambiguïté — une compression efficace parce qu'une
colonne regroupe des valeurs de même nature, et des statistiques par bloc qui
permettent d'ignorer des portions de fichier. Mesuré ici : 1,89 fois plus
compact que le CSV et 6,06 fois plus que le JSON, sur les mêmes données.

**8. Pourquoi Parquet seul ne suffit-il pas pour un Lakehouse ?** Parce qu'il
décrit l'écriture dans un fichier, pas la gestion d'une table. Pas de commit
atomique : un `overwrite` interrompu laisse un répertoire à moitié écrit. Pas
d'isolation : un lecteur peut voir un état intermédiaire. Pas de versions, pas
de retour arrière, pas d'évolution de schéma encadrée. Ces garanties viennent
d'un format de table (Delta Lake, Iceberg, Hudi) qui maintient un journal de
métadonnées au-dessus des mêmes fichiers Parquet.

**9. Dans quels cas une base NoSQL compléterait-elle l'architecture ?** Quand
un besoin réclame un accès unitaire à faible latence par clé, que le lac
analytique ne sait pas servir : afficher un profil client, consulter
l'historique d'événements d'un identifiant, alimenter une application. HBase
se positionnerait en dérivation de la zone Gold, avec une Row Key conçue pour
l'accès attendu. Dans ce projet, aucune des six questions du Jour 1 n'est de
cette nature : elles sont toutes analytiques, et la zone Gold y répond.

**10. Quelles limites des données TPCx-BB faut-il prendre en compte ?** Jeu
**synthétique** : volumes, distributions et libellés sont générés, aucune
conclusion commerciale n'en découle. Le benchmark officiel n'est pas exécuté,
aucun score ni performance n'est revendiqué. Les libellés produits sont du
texte généré sans signification. Le profit net est négatif sur 74 % des
lignes, ce qui n'a pas de sens commercial. L'unité monétaire n'est pas
documentée. Une table présente ne garantit pas que ses colonnes et relations
soient renseignées — l'inventaire réel fait foi. `web_clickstreams` ne se
rattache aux ventes par aucun identifiant fiable : un clic n'est ni une
intention d'achat ni une vente. Et une corrélation observée ne démontre
aucune causalité.
