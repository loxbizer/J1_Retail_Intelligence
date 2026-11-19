# -*- coding: utf-8 -*-
"""
TP Jour 3 - etape 7 : Silver -> Gold.

Deux jeux Gold, chacun rattache a une question metier du Jour 1 :

  gold/ca_mensuel_magasin
      Q1 - comment evolue le chiffre d'affaires sur les periodes
      Q2 - quels magasins contribuent le plus ou le moins
      Q4 - evolution entre deux periodes comparables
      Q5 - le panier moyen est-il calculable de facon fiable
      Grain : une ligne = un mois x un magasin

  gold/top_produits_quantite
      Q3 - quels produits representent les plus grandes quantites vendues
      Grain : une ligne = un produit
      C'est ce jeu qui porte la jointure d'enrichissement avec item.

Toutes les jointures sont verifiees avant usage : lignes avant / apres,
et unicite de la cle du cote referentiel.

    python 04_gold.py
"""

from datetime import datetime
from pathlib import Path

from config import parametres as P
from pyspark.sql import functions as F
from pyspark.sql.window import Window

rapport = []


def dire(ligne=""):
    print(ligne)
    rapport.append(str(ligne))


spark = P.session("TP3-Silver-vers-Gold")

dire("=" * 60)
dire("RAPPORT GOLD - TP JOUR 3 (etape 7)")
dire(datetime.now().strftime("%d/%m/%Y %H:%M"))
dire("=" * 60)
dire("")

ventes = spark.read.parquet(P.chemin_silver("store_sales"))
item = spark.read.parquet(P.chemin_silver("item"))
magasin = spark.read.parquet(P.chemin_silver("store"))
calendrier = spark.read.parquet(P.chemin_silver("date_dim"))

nb_ventes = ventes.count()
dire(f"Lignes Silver (ventes) : {nb_ventes}")
dire("")

# ------------------------------------------------- jointures : verification

dire("=" * 60)
dire("VERIFICATION DES JOINTURES AVANT USAGE")
dire("=" * 60)

for nom, dim, cle_dim, cle_vente, cardinalite in (
    ("date_dim", calendrier, "d_date_sk", "ss_sold_date_sk", "n:1"),
    ("store", magasin, "s_store_sk", "ss_store_sk", "n:1"),
    ("item", item, "i_item_sk", "ss_item_sk", "n:1"),
):
    lignes = dim.count()
    distinctes = dim.select(cle_dim).distinct().count()
    orphelins = ventes.join(
        dim.select(cle_dim), ventes[cle_vente] == dim[cle_dim], "left_anti"
    ).count()
    dire(f"{cle_vente} -> {nom}.{cle_dim}   (cardinalite attendue {cardinalite})")
    dire(f"      referentiel : {lignes} lignes / {distinctes} cles distinctes"
         f" -> {'cle unique' if lignes == distinctes else 'CLE NON UNIQUE, risque de multiplication'}")
    dire(f"      ventes sans correspondance : {orphelins}")
dire("")

# ------------------------------------------ Gold 1 : CA mensuel par magasin

dire("=" * 60)
dire("GOLD 1 : ca_mensuel_magasin")
dire("=" * 60)

base = (
    ventes
    .join(calendrier.select("d_date_sk", "d_year", "d_moy"),
          ventes.ss_sold_date_sk == F.col("d_date_sk"), "inner")
    .join(magasin.select("s_store_sk", "s_store_name", "s_state"),
          ventes.ss_store_sk == F.col("s_store_sk"), "left")
)
nb_base = base.count()
dire(f"Lignes avant jointures : {nb_ventes}")
dire(f"Lignes apres jointures : {nb_base}")
dire("      -> aucune multiplication, aucune perte"
     if nb_base == nb_ventes else
     "      -> ECART : a expliquer avant d'agreger")

mensuel = (
    base
    .groupBy("d_year", "d_moy", "s_store_sk", "s_store_name", "s_state")
    .agg(
        F.round(F.sum("ss_net_paid"), 2).alias("ca_net"),
        F.round(F.sum("ss_net_profit"), 2).alias("marge_nette"),
        F.sum("ss_quantity").alias("quantite"),
        F.countDistinct("ss_ticket_number").alias("nb_transactions"),
        F.count("*").alias("nb_lignes_vente"),
    )
)

# KPI 3 du dictionnaire du Jour 1 : CA / nombre de transactions distinctes.
# try_divide protege le cas ou un groupe n'aurait aucune transaction.
mensuel = mensuel.withColumn(
    "panier_moyen",
    F.round(F.try_divide(F.col("ca_net"), F.col("nb_transactions")), 2),
)

# KPI 5 : evolution par rapport au mois precedent, magasin par magasin.
fenetre = Window.partitionBy("s_store_sk").orderBy("d_year", "d_moy")
mensuel = (
    mensuel
    .withColumn("ca_mois_precedent", F.lag("ca_net").over(fenetre))
    .withColumn(
        "evolution_pct",
        F.round(100 * F.try_divide(
            F.col("ca_net") - F.col("ca_mois_precedent"),
            F.col("ca_mois_precedent")), 1),
    )
    .orderBy("d_year", "d_moy", F.desc("ca_net"))
)

mensuel.write.mode("overwrite").parquet(f"{P.GOLD}/ca_mensuel_magasin")
dire(f"Ecrit : {P.GOLD}/ca_mensuel_magasin")

relu = spark.read.parquet(f"{P.GOLD}/ca_mensuel_magasin")
dire(f"Lignes Gold : {relu.count()}  (grain : un mois x un magasin)")
dire("")

# bouclage : le total agrege doit egaler le total du detail
total_detail = ventes.agg(F.round(F.sum("ss_net_paid"), 2)).collect()[0][0]
total_gold = relu.agg(F.round(F.sum("ca_net"), 2)).collect()[0][0]
dire("Bouclage des montants :")
dire(f"      somme Silver (detail) : {total_detail}")
dire(f"      somme Gold (agrege)   : {total_gold}")
ecart = round(abs((total_detail or 0) - (total_gold or 0)), 2)
dire(f"      ecart                 : {ecart}  "
     + ("-> OK, ni perte ni duplication" if ecart < 1 else "-> A EXPLIQUER"))
dire("")

dire("Apercu (10 premieres lignes) :")
for r in relu.orderBy("d_year", "d_moy", F.desc("ca_net")).limit(10).collect():
    dire(f"      {r['d_year']}-{r['d_moy']:02d}  {str(r['s_store_name']):<12}"
         f" ca {r['ca_net']:>12}  tickets {r['nb_transactions']:>6}"
         f"  panier {r['panier_moyen']}")
dire("")

# ------------------------------------------- Gold 2 : top produits vendus

dire("=" * 60)
dire("GOLD 2 : top_produits_quantite")
dire("=" * 60)

enrichi = ventes.join(
    item.select("i_item_sk", "i_product_name", "i_category", "i_brand"),
    ventes.ss_item_sk == F.col("i_item_sk"), "left",
)
nb_enrichi = enrichi.count()
dire(f"Lignes avant jointure item : {nb_ventes}")
dire(f"Lignes apres jointure item : {nb_enrichi}")
dire("      -> jointure sans effet de multiplication"
     if nb_enrichi == nb_ventes else "      -> ECART : jointure a revoir")

sans_categorie = enrichi.filter(F.col("i_category").isNull()).count()
dire(f"Lignes dont la categorie est nulle : {sans_categorie}")
dire("      (jointure en left : une vente sans produit au referentiel est")
dire("       conservee, sa categorie reste nulle, et c'est documente)")

produits = (
    enrichi
    .groupBy("i_item_sk", "i_product_name", "i_category", "i_brand")
    .agg(
        F.sum("ss_quantity").alias("quantite_vendue"),
        F.round(F.sum("ss_net_paid"), 2).alias("ca_net"),
        F.countDistinct("ss_ticket_number").alias("nb_transactions"),
    )
    .orderBy(F.desc("quantite_vendue"))
)

produits.write.mode("overwrite").parquet(f"{P.GOLD}/top_produits_quantite")
dire(f"Ecrit : {P.GOLD}/top_produits_quantite")

relu2 = spark.read.parquet(f"{P.GOLD}/top_produits_quantite")
dire(f"Lignes Gold : {relu2.count()}  (grain : un produit)")
dire("")
dire("Apercu (10 produits les plus vendus) :")
for r in relu2.orderBy(F.desc("quantite_vendue")).limit(10).collect():
    nom = (r["i_product_name"] or "(inconnu)")[:28]
    dire(f"      {nom:<30} qte {r['quantite_vendue']:>8}  ca {r['ca_net']:>12}")
dire("")

dire("FIN DU RAPPORT")

sortie = Path(P.DOC_TRANSFORMATIONS) / "rapport-gold.txt"
sortie.parent.mkdir(parents=True, exist_ok=True)
sortie.write_text("\n".join(rapport) + "\n", encoding="utf-8")
print(f"\nRapport ecrit dans : {sortie.resolve()}")

spark.stop()
