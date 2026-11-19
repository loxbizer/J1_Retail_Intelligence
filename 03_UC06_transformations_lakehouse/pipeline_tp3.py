# -*- coding: utf-8 -*-
"""
TP JOUR 3 - Retail Intelligence Platform - pipeline complet (UC-06).

Un seul fichier, cinq etapes. Lance tout :

    python pipeline_tp3.py

ou une etape precise :

    python pipeline_tp3.py cles
    python pipeline_tp3.py silver
    python pipeline_tp3.py gold
    python pipeline_tp3.py qualite
    python pipeline_tp3.py formats

Correspondance avec le TP :
    cles     complement d'etape 3 : elucider la cle de store_sales
    silver   etapes 4, 5, 6 : nettoyage, transformations, zone Silver
    gold     etape 7 : jeux analytiques Gold
    qualite  etape 9 : les sept dimensions de controle
    formats  etape 8 : comparaison CSV / JSON / Parquet

Regle tenue partout : aucune ligne n'est supprimee. Une ligne non
conforme est ECARTEE en quarantaine, donc conservee et auditable.
Aucune ecriture n'est faite dans la zone Bronze.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Les zones /retail ont ete creees par le conteneur, qui tourne en root.
# Un client qui se presente sous un autre nom ne peut pas y ecrire.
# A definir AVANT de demarrer la session Spark.
os.environ.setdefault("HADOOP_USER_NAME", "root")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType,
)

# =====================================================================
# 1. PARAMETRES  (etape 10 du TP : aucun chemin en dur dans les traitements)
# =====================================================================

HDFS = "hdfs://localhost:9000"
BRONZE = f"{HDFS}/retail/bronze/data"
SILVER = f"{HDFS}/retail/silver"
GOLD = f"{HDFS}/retail/gold"
QUARANTAINE = f"{HDFS}/retail/quarantine"

SEPARATEUR = "|"
ENTETE = False

DOC_EXPLORATION = Path("docs/exploration")
DOC_TRANSFORMATIONS = Path("docs/transformations")
DOC_QUALITE = Path("docs/quality")
OUTPUTS = Path("outputs")


def _schema(definition):
    """Transforme une liste (nom, type) en StructType.

    Spark n'applique pas le caractere nullable a la lecture d'un CSV :
    les colonnes obligatoires sont donc controlees par un filtre
    explicite, pas par le schema.
    """
    return StructType([StructField(n, t(), True) for n, t in definition])


SCHEMA_STORE_SALES = _schema([
    ("ss_sold_date_sk", IntegerType), ("ss_sold_time_sk", IntegerType),
    ("ss_item_sk", IntegerType), ("ss_customer_sk", IntegerType),
    ("ss_cdemo_sk", IntegerType), ("ss_hdemo_sk", IntegerType),
    ("ss_addr_sk", IntegerType), ("ss_store_sk", IntegerType),
    ("ss_promo_sk", IntegerType), ("ss_ticket_number", IntegerType),
    ("ss_quantity", IntegerType), ("ss_wholesale_cost", DoubleType),
    ("ss_list_price", DoubleType), ("ss_sales_price", DoubleType),
    ("ss_ext_discount_amt", DoubleType), ("ss_ext_sales_price", DoubleType),
    ("ss_ext_wholesale_cost", DoubleType), ("ss_ext_list_price", DoubleType),
    ("ss_ext_tax", DoubleType), ("ss_coupon_amt", DoubleType),
    ("ss_net_paid", DoubleType), ("ss_net_paid_inc_tax", DoubleType),
    ("ss_net_profit", DoubleType),
])

SCHEMA_ITEM = _schema([
    ("i_item_sk", IntegerType), ("i_item_id", StringType),
    ("i_rec_start_date", DateType), ("i_rec_end_date", DateType),
    ("i_item_desc", StringType), ("i_current_price", DoubleType),
    ("i_wholesale_cost", DoubleType), ("i_brand_id", IntegerType),
    ("i_brand", StringType), ("i_class_id", IntegerType),
    ("i_class", StringType), ("i_category_id", IntegerType),
    ("i_category", StringType), ("i_manufact_id", IntegerType),
    ("i_manufact", StringType), ("i_size", StringType),
    ("i_formulation", StringType), ("i_color", StringType),
    ("i_units", StringType), ("i_container", StringType),
    ("i_manager_id", IntegerType), ("i_product_name", StringType),
])

SCHEMA_STORE = _schema([
    ("s_store_sk", IntegerType), ("s_store_id", StringType),
    ("s_rec_start_date", DateType), ("s_rec_end_date", DateType),
    ("s_closed_date_sk", IntegerType), ("s_store_name", StringType),
    ("s_number_employees", IntegerType), ("s_floor_space", IntegerType),
    ("s_hours", StringType), ("s_manager", StringType),
    ("s_market_id", IntegerType), ("s_geography_class", StringType),
    ("s_market_desc", StringType), ("s_market_manager", StringType),
    ("s_division_id", IntegerType), ("s_division_name", StringType),
    ("s_company_id", IntegerType), ("s_company_name", StringType),
    ("s_street_number", IntegerType), ("s_street_name", StringType),
    ("s_street_type", StringType), ("s_suite_number", StringType),
    ("s_city", StringType), ("s_county", StringType), ("s_state", StringType),
    ("s_zip", IntegerType), ("s_country", StringType),
    ("s_gmt_offset", IntegerType), ("s_tax_precentage", DoubleType),
])

SCHEMA_DATE_DIM = _schema([
    ("d_date_sk", IntegerType), ("d_date_id", StringType),
    ("d_date", DateType), ("d_month_seq", IntegerType),
    ("d_week_seq", IntegerType), ("d_quarter_seq", IntegerType),
    ("d_year", IntegerType), ("d_dow", IntegerType), ("d_moy", IntegerType),
    ("d_dom", IntegerType), ("d_qoy", IntegerType), ("d_fy_year", IntegerType),
    ("d_fy_quarter_seq", IntegerType), ("d_fy_week_seq", IntegerType),
    ("d_day_name", StringType), ("d_quarter_name", StringType),
    ("d_holiday", StringType), ("d_weekend", StringType),
    ("d_following_holiday", StringType), ("d_first_dom", IntegerType),
    ("d_last_dom", IntegerType), ("d_same_day_ly", IntegerType),
    ("d_same_day_lq", IntegerType), ("d_current_day", StringType),
    ("d_current_week", StringType), ("d_current_month", StringType),
    ("d_current_quarter", StringType), ("d_current_year", StringType),
])

SOURCES = {
    "store_sales": {
        "schema": SCHEMA_STORE_SALES, "domaine": "sales",
        "obligatoires": ["ss_item_sk", "ss_ticket_number",
                         "ss_store_sk", "ss_sold_date_sk"],
        "cle": ["ss_sold_date_sk", "ss_ticket_number", "ss_item_sk"],
    },
    "item": {
        "schema": SCHEMA_ITEM, "domaine": "products",
        "obligatoires": ["i_item_sk"], "cle": ["i_item_sk"],
    },
    "store": {
        "schema": SCHEMA_STORE, "domaine": "stores",
        "obligatoires": ["s_store_sk"], "cle": ["s_store_sk"],
    },
    "date_dim": {
        "schema": SCHEMA_DATE_DIM, "domaine": "time",
        "obligatoires": ["d_date_sk"], "cle": ["d_date_sk"],
    },
}


def chemin_bronze(table):
    return f"{BRONZE}/{table}/{table}.dat"


def chemin_silver(table):
    return f"{SILVER}/{SOURCES[table]['domaine']}/{table}"


# =====================================================================
# 2. OUTILS
# =====================================================================

class Rapport:
    """Accumule les lignes affichees et les ecrit dans un fichier."""

    def __init__(self, titre, destination):
        self.lignes = []
        self.destination = Path(destination)
        self.dire("=" * 60)
        self.dire(titre)
        self.dire(datetime.now().strftime("%d/%m/%Y %H:%M"))
        self.dire("=" * 60)
        self.dire()

    def dire(self, ligne=""):
        print(ligne)
        self.lignes.append(str(ligne))

    def titre(self, texte):
        self.dire("=" * 60)
        self.dire(texte)
        self.dire("=" * 60)

    def ecrire(self):
        self.dire()
        self.dire("FIN DU RAPPORT")
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        self.destination.write_text("\n".join(self.lignes) + "\n", encoding="utf-8")
        print(f"\nRapport ecrit dans : {self.destination.resolve()}")


def session():
    spark = (
        SparkSession.builder
        .appName("TP3-Retail-Intelligence")
        .master("local[*]")
        # le NameNode communique l'adresse interne du DataNode dans le
        # reseau Docker : on demande au client d'utiliser le nom d'hote
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def lire_bronze(spark, table):
    """Lit une source avec son schema explicite : plus aucune inference."""
    return (
        spark.read
        .option("header", ENTETE)
        .option("sep", SEPARATEUR)
        .schema(SOURCES[table]["schema"])
        .csv(chemin_bronze(table))
    )


def lire_silver(spark, table):
    return spark.read.parquet(chemin_silver(table))


# =====================================================================
# 3. ETAPE "cles" - elucider la cle de store_sales
# =====================================================================

def etape_cles(spark):
    r = Rapport("CLES ET PERIMETRE - store_sales",
                DOC_EXPLORATION / "rapport-cles.txt")

    ventes = lire_bronze(spark, "store_sales").cache()
    item = lire_bronze(spark, "item")
    dates = lire_bronze(spark, "date_dim")
    nb = ventes.count()
    r.dire(f"Lignes : {nb}")
    r.dire()

    r.titre("1. CLES CANDIDATES TESTEES")
    for cle in (["ss_ticket_number"],
                ["ss_ticket_number", "ss_item_sk"],
                ["ss_sold_date_sk", "ss_ticket_number", "ss_item_sk"],
                ["ss_store_sk", "ss_ticket_number", "ss_item_sk"],
                ["ss_sold_date_sk", "ss_store_sk", "ss_ticket_number", "ss_item_sk"]):
        d = ventes.select(cle).distinct().count()
        r.dire(f"{' + '.join(cle):<60}{d:>9}  "
               + ("UNIQUE" if d == nb else f"ecart {nb - d}"))
    r.dire()
    r.dire("Lecture : la premiere cle UNIQUE est la cle reelle de la table.")
    r.dire("Si aucune ne l'est, il reste de vraies lignes repetees.")
    r.dire()

    r.titre("2. LIGNES STRICTEMENT IDENTIQUES (23 colonnes)")
    d = ventes.distinct().count()
    r.dire(f"Lignes distinctes : {d}")
    r.dire(f"Lignes repetees a l'identique : {nb - d}")
    r.dire("-> aucune ligne n'est repetee a l'identique" if nb == d
           else "-> il existe de vraies lignes repetees")
    r.dire()

    r.titre("3. DEUX GROUPES EN DOUBLE, EN ENTIER")
    groupes = (ventes.groupBy("ss_ticket_number", "ss_item_sk")
               .count().filter("count > 1").orderBy(F.desc("count")))
    r.dire(f"Groupes (ticket, item) a plus d'une ligne : {groupes.count()}")
    r.dire()
    for g in groupes.limit(2).collect():
        extrait = ventes.filter(
            (F.col("ss_ticket_number") == g[0]) & (F.col("ss_item_sk") == g[1])
        ).collect()
        r.dire(f"--- ticket {g[0]}, article {g[1]} : {g[2]} lignes ---")
        for colonne in [c.name for c in SCHEMA_STORE_SALES.fields]:
            valeurs = [str(x[colonne]) for x in extrait]
            marque = "   <== DIFFERENT" if len(set(valeurs)) > 1 else ""
            r.dire(f"  {colonne:<22}{' | '.join(valeurs)}{marque}")
        r.dire()
    r.dire("Lecture : les colonnes DIFFERENT expliquent le doublon. Si la")
    r.dire("date ou le magasin differe, ce sont deux ventes distinctes et")
    r.dire("la cle etait incomplete, pas violee.")
    r.dire()

    r.titre("4. DIMENSION item : historisation")
    r.dire(f"Lignes                 : {item.count()}")
    r.dire(f"i_item_sk distincts    : {item.select('i_item_sk').distinct().count()}")
    r.dire(f"i_item_id distincts    : {item.select('i_item_id').distinct().count()}")
    r.dire(f"i_rec_end_date non nul : {item.filter(F.col('i_rec_end_date').isNotNull()).count()}")
    r.dire()
    r.dire("Lecture : autant de i_item_id que de lignes et i_rec_end_date")
    r.dire("vide => une seule version par article, donc aucune jointure ne")
    r.dire("peut multiplier les lignes.")
    r.dire()

    r.titre("5. PERIMETRE REEL DES VENTES")
    avec = ventes.join(dates.select("d_date_sk", "d_date", "d_year", "d_moy"),
                       ventes.ss_sold_date_sk == F.col("d_date_sk"), "inner")
    r.dire(f"Lignes apres jointure date : {avec.count()}  (avant : {nb})")
    b = avec.agg(F.min("d_date"), F.max("d_date"), F.countDistinct("d_year")).collect()[0]
    r.dire(f"Premiere vente : {b[0]}")
    r.dire(f"Derniere vente : {b[1]}")
    r.dire(f"Annees couvertes : {b[2]}")
    r.dire()
    r.dire("Repartition par annee :")
    r.dire(f"  {'annee':<8}{'lignes':>10}{'tickets':>10}{'ca_net':>16}")
    for x in (avec.groupBy("d_year").agg(
            F.count("*").alias("l"),
            F.countDistinct("ss_ticket_number").alias("t"),
            F.round(F.sum("ss_net_paid"), 2).alias("ca")).orderBy("d_year").collect()):
        r.dire(f"  {x['d_year']:<8}{x['l']:>10}{x['t']:>10}{x['ca']:>16}")
    r.dire()
    r.dire(f"Tickets distincts : {ventes.select('ss_ticket_number').distinct().count()}")
    r.dire(f"Magasins presents : {ventes.select('ss_store_sk').distinct().count()} sur 12")
    r.dire()
    r.dire("Bornes des mesures :")
    for colonne in ("ss_quantity", "ss_net_paid", "ss_net_profit"):
        x = ventes.agg(F.min(colonne), F.max(colonne),
                       F.sum(F.when(F.col(colonne) < 0, 1).otherwise(0))).collect()[0]
        r.dire(f"  {colonne:<18}min {x[0]:>12}  max {x[1]:>12}  negatifs {x[2]:>8}")
    r.dire()
    r.dire("Lecture : un montant negatif n'est pas une erreur. Un profit net")
    r.dire("negatif est une vente a perte, cas metier reel.")

    ventes.unpersist()
    r.ecrire()


# =====================================================================
# 4. ETAPE "silver" - Bronze vers Silver
# =====================================================================

def etape_silver(spark):
    r = Rapport("SILVER - TP JOUR 3 (etapes 4, 5 et 6)",
                DOC_TRANSFORMATIONS / "rapport-silver.txt")
    bilan = []

    for table, conf in SOURCES.items():
        r.titre(f"TABLE : {table}  ->  silver/{conf['domaine']}/{table}")

        df = lire_bronze(spark, table)
        nb_bronze = df.count()
        r.dire(f"Lignes lues en Bronze : {nb_bronze}")
        r.dire(f"Colonnes : {len(df.columns)} (schema declare, types imposes)")

        # R1 - harmonisation des libelles : espaces de bord retires
        textes = [c.name for c in conf["schema"].fields
                  if isinstance(c.dataType, StringType)]
        for colonne in textes:
            df = df.withColumn(colonne, F.trim(F.col(colonne)))
        r.dire(f"R1 trim applique sur {len(textes)} colonne(s) texte")

        # R2 - mesure des valeurs nulles apres typage explicite
        nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                           for c in df.columns]).collect()[0].asDict()
        concernees = {c: v for c, v in nulls.items() if v}
        if concernees:
            r.dire("R2 valeurs nulles apres typage :")
            for colonne, compte in concernees.items():
                r.dire(f"      {colonne:<24}{compte:>9}  ({100 * compte / nb_bronze:.2f} %)")
        else:
            r.dire("R2 aucune valeur nulle apres typage")

        # R3 - mise a l'ecart des lignes sans cle obligatoire (pas de suppression)
        condition = None
        for colonne in conf["obligatoires"]:
            test = F.col(colonne).isNotNull()
            condition = test if condition is None else condition & test
        valides, rejetees = df.filter(condition), df.filter(~condition)
        nb_rejetees = rejetees.count()
        r.dire(f"R3 cles obligatoires {conf['obligatoires']}")
        r.dire(f"      lignes ecartees : {nb_rejetees}")
        if nb_rejetees:
            (rejetees
             .withColumn("motif_rejet", F.lit("cle obligatoire nulle"))
             .withColumn("dt_rejet", F.current_timestamp())
             .write.mode("overwrite").parquet(f"{QUARANTAINE}/{table}"))
            r.dire(f"      ecrites dans {QUARANTAINE}/{table}")

        # R4 - montants negatifs conserves et marques
        if table == "store_sales":
            valides = valides.withColumn(
                "ind_montant_negatif",
                (F.col("ss_net_paid") < 0) | (F.col("ss_net_profit") < 0))
            r.dire("R4 lignes a montant negatif : "
                   f"{valides.filter('ind_montant_negatif').count()}"
                   " -> conservees et marquees")

        nb_valides = valides.count()
        distinctes = valides.select(conf["cle"]).distinct().count()
        r.dire(f"Cle {' + '.join(conf['cle'])}")
        r.dire(f"      lignes {nb_valides} / distinctes {distinctes}"
               f" / ecart {nb_valides - distinctes}")
        r.dire("      -> cle unique, aucune deduplication necessaire"
               if nb_valides == distinctes else
               "      -> cle non unique : AUCUNE suppression, ecart documente")

        # provenance : d'ou vient chaque ligne, et quand elle a ete traitee
        silver = (valides
                  .withColumn("src_fichier", F.input_file_name())
                  .withColumn("src_systeme", F.lit(f"TPCx-BB / {table}"))
                  .withColumn("dt_traitement", F.current_timestamp()))

        destination = chemin_silver(table)
        silver.write.mode("overwrite").parquet(destination)
        r.dire(f"Ecrit : {destination}")

        relu = spark.read.parquet(destination)
        nb_relu = relu.count()
        attendus = {f.name: f.dataType.simpleString() for f in conf["schema"].fields}
        observes = {f.name: f.dataType.simpleString() for f in relu.schema.fields}
        ecarts = sum(1 for n, t in attendus.items() if observes.get(n) != t)
        r.dire("Controles apres ecriture :")
        r.dire("      relecture       : OK")
        r.dire(f"      lignes relues   : {nb_relu}")
        r.dire(f"      colonnes        : {len(relu.columns)} (dont 3 de provenance)")
        r.dire(f"      types conserves : {'OK' if ecarts == 0 else f'{ecarts} ecart(s)'}")
        r.dire(f"      bouclage        : {nb_bronze} = {nb_relu} + {nb_rejetees} -> "
               + ("OK" if nb_bronze == nb_relu + nb_rejetees else "ECART A EXPLIQUER"))
        r.dire()

        bilan.append((table, nb_bronze, nb_relu, nb_rejetees,
                      nb_valides == distinctes))

    r.titre("RECAPITULATIF")
    r.dire(f"{'table':<14}{'bronze':>10}{'silver':>10}{'quarant.':>10}{'cle unique':>13}")
    for t, b, s, q, u in bilan:
        r.dire(f"{t:<14}{b:>10}{s:>10}{q:>10}{str(u):>13}")
    r.dire()
    r.dire("Equation de conservation : bronze = silver + quarantaine.")
    r.dire("Aucune ligne n'a disparu sans decision tracee.")
    r.ecrire()


# =====================================================================
# 5. ETAPE "gold" - Silver vers Gold
# =====================================================================

def etape_gold(spark):
    r = Rapport("GOLD - TP JOUR 3 (etape 7)",
                DOC_TRANSFORMATIONS / "rapport-gold.txt")

    ventes = lire_silver(spark, "store_sales")
    item = lire_silver(spark, "item")
    magasin = lire_silver(spark, "store")
    calendrier = lire_silver(spark, "date_dim")
    nb_ventes = ventes.count()
    r.dire(f"Lignes Silver (ventes) : {nb_ventes}")
    r.dire()

    r.titre("VERIFICATION DES JOINTURES AVANT USAGE")
    for nom, dim, cle_dim, cle_vente in (
            ("date_dim", calendrier, "d_date_sk", "ss_sold_date_sk"),
            ("store", magasin, "s_store_sk", "ss_store_sk"),
            ("item", item, "i_item_sk", "ss_item_sk")):
        lignes = dim.count()
        distinctes = dim.select(cle_dim).distinct().count()
        orphelins = ventes.join(dim.select(cle_dim),
                                ventes[cle_vente] == dim[cle_dim],
                                "left_anti").count()
        r.dire(f"{cle_vente} -> {nom}.{cle_dim}   (cardinalite attendue n:1)")
        r.dire(f"      referentiel : {lignes} lignes / {distinctes} cles distinctes -> "
               + ("cle unique" if lignes == distinctes
                  else "CLE NON UNIQUE, risque de multiplication"))
        r.dire(f"      ventes sans correspondance : {orphelins}")
    r.dire()

    # ---- Gold 1 : CA mensuel par magasin (Q1, Q2, Q4, Q5) ----
    r.titre("GOLD 1 : ca_mensuel_magasin")
    base = (ventes
            .join(calendrier.select("d_date_sk", "d_year", "d_moy"),
                  ventes.ss_sold_date_sk == F.col("d_date_sk"), "inner")
            .join(magasin.select("s_store_sk", "s_store_name", "s_state"),
                  ventes.ss_store_sk == F.col("s_store_sk"), "left"))
    nb_base = base.count()
    r.dire(f"Lignes avant jointures : {nb_ventes}")
    r.dire(f"Lignes apres jointures : {nb_base}")
    r.dire("      -> ni multiplication ni perte" if nb_base == nb_ventes
           else "      -> ECART : a expliquer avant d'agreger")

    mensuel = (base
               .groupBy("d_year", "d_moy", "s_store_sk", "s_store_name", "s_state")
               .agg(F.round(F.sum("ss_net_paid"), 2).alias("ca_net"),
                    F.round(F.sum("ss_net_profit"), 2).alias("marge_nette"),
                    F.sum("ss_quantity").alias("quantite"),
                    F.countDistinct("ss_ticket_number").alias("nb_transactions"),
                    F.count("*").alias("nb_lignes_vente")))

    # KPI 3 du Jour 1 : CA / transactions distinctes. try_divide protege
    # le cas d'un groupe sans transaction (renvoie null, ne leve pas).
    mensuel = mensuel.withColumn(
        "panier_moyen",
        F.round(F.try_divide(F.col("ca_net"), F.col("nb_transactions")), 2))

    # KPI 5 : evolution par rapport au mois precedent, magasin par magasin
    fenetre = Window.partitionBy("s_store_sk").orderBy("d_year", "d_moy")
    mensuel = (mensuel
               .withColumn("ca_mois_precedent", F.lag("ca_net").over(fenetre))
               .withColumn("evolution_pct",
                           F.round(100 * F.try_divide(
                               F.col("ca_net") - F.col("ca_mois_precedent"),
                               F.col("ca_mois_precedent")), 1))
               .orderBy("d_year", "d_moy", F.desc("ca_net")))

    mensuel.write.mode("overwrite").parquet(f"{GOLD}/ca_mensuel_magasin")
    r.dire(f"Ecrit : {GOLD}/ca_mensuel_magasin")
    relu = spark.read.parquet(f"{GOLD}/ca_mensuel_magasin")
    r.dire(f"Lignes Gold : {relu.count()}  (grain : un mois x un magasin)")
    r.dire()

    total_detail = ventes.agg(F.round(F.sum("ss_net_paid"), 2)).collect()[0][0] or 0
    total_gold = relu.agg(F.round(F.sum("ca_net"), 2)).collect()[0][0] or 0
    ecart = round(abs(total_detail - total_gold), 2)
    r.dire("Bouclage des montants :")
    r.dire(f"      somme Silver (detail) : {total_detail}")
    r.dire(f"      somme Gold (agrege)   : {total_gold}")
    r.dire(f"      ecart                 : {ecart}  "
           + ("-> OK, ni perte ni duplication" if ecart < 1 else "-> A EXPLIQUER"))
    r.dire()
    r.dire("Apercu :")
    for x in relu.orderBy("d_year", "d_moy", F.desc("ca_net")).limit(10).collect():
        r.dire(f"      {x['d_year']}-{x['d_moy']:02d}  {str(x['s_store_name']):<12}"
               f" ca {x['ca_net']:>12}  tickets {x['nb_transactions']:>6}"
               f"  panier {x['panier_moyen']}")
    r.dire()

    # ---- Gold 2 : top produits par quantite (Q3) ----
    r.titre("GOLD 2 : top_produits_quantite")
    enrichi = ventes.join(
        item.select("i_item_sk", "i_product_name", "i_category", "i_brand"),
        ventes.ss_item_sk == F.col("i_item_sk"), "left")
    nb_enrichi = enrichi.count()
    r.dire(f"Lignes avant jointure item : {nb_ventes}")
    r.dire(f"Lignes apres jointure item : {nb_enrichi}")
    r.dire("      -> jointure sans multiplication" if nb_enrichi == nb_ventes
           else "      -> ECART : jointure a revoir")
    r.dire(f"Lignes a categorie nulle : {enrichi.filter(F.col('i_category').isNull()).count()}")
    r.dire("      (jointure left : une vente sans produit au referentiel est")
    r.dire("       conservee, categorie nulle, et c'est documente)")

    produits = (enrichi
                .groupBy("i_item_sk", "i_product_name", "i_category", "i_brand")
                .agg(F.sum("ss_quantity").alias("quantite_vendue"),
                     F.round(F.sum("ss_net_paid"), 2).alias("ca_net"),
                     F.countDistinct("ss_ticket_number").alias("nb_transactions"))
                .orderBy(F.desc("quantite_vendue")))
    produits.write.mode("overwrite").parquet(f"{GOLD}/top_produits_quantite")
    r.dire(f"Ecrit : {GOLD}/top_produits_quantite")
    relu2 = spark.read.parquet(f"{GOLD}/top_produits_quantite")
    r.dire(f"Lignes Gold : {relu2.count()}  (grain : un produit)")
    r.dire()
    r.dire("Apercu (10 produits les plus vendus) :")
    for x in relu2.orderBy(F.desc("quantite_vendue")).limit(10).collect():
        r.dire(f"      {(x['i_product_name'] or '(inconnu)')[:28]:<30}"
               f" qte {x['quantite_vendue']:>8}  ca {x['ca_net']:>12}")
    r.ecrire()


# =====================================================================
# 6. ETAPE "qualite" - les sept dimensions de controle
# =====================================================================

def etape_qualite(spark):
    resultats = []

    def controle(nom, dimension, regle, mesure, attendu, decision):
        statut = "OK" if mesure == attendu else "ANOMALIE"
        resultats.append({"controle": nom, "dimension": dimension,
                          "regle": regle, "mesure": int(mesure),
                          "attendu": int(attendu), "statut": statut,
                          "decision": decision})
        print(f"{statut:<9}{nom:<30}mesure {mesure} / attendu {attendu}")

    ventes = lire_silver(spark, "store_sales")
    item = lire_silver(spark, "item")
    magasin = lire_silver(spark, "store")
    calendrier = lire_silver(spark, "date_dim")
    nb_ventes = ventes.count()

    for colonne in ("ss_item_sk", "ss_ticket_number", "ss_store_sk", "ss_sold_date_sk"):
        controle(f"completude_{colonne}", "Completude", f"{colonne} non nul",
                 ventes.filter(F.col(colonne).isNull()).count(), 0,
                 "ligne ecartee en quarantaine")

    cle = SOURCES["store_sales"]["cle"]
    controle("unicite_cle_vente", "Unicite", " + ".join(cle),
             nb_ventes - ventes.select(cle).distinct().count(), 0,
             "aucune suppression : ecart documente et explique")

    attendus = {f.name: f.dataType.simpleString() for f in SCHEMA_STORE_SALES.fields}
    observes = {f.name: f.dataType.simpleString() for f in ventes.schema.fields}
    controle("validite_types", "Validite", "types conformes au schema declare",
             sum(1 for n, t in attendus.items() if observes.get(n) != t), 0,
             "schema explicite impose a la lecture")

    controle("coherence_quantite", "Coherence", "ss_quantity strictement positif",
             ventes.filter(F.col("ss_quantity") <= 0).count(), 0,
             "quantite nulle ou negative = retour, a traiter a part")

    bornes = calendrier.agg(F.min("d_date_sk"), F.max("d_date_sk")).collect()[0]
    controle("coherence_date", "Coherence", "date de vente dans la plage du referentiel",
             ventes.filter((F.col("ss_sold_date_sk") < bornes[0])
                           | (F.col("ss_sold_date_sk") > bornes[1])).count(), 0,
             "hors plage -> quarantaine")

    for table in SOURCES:
        bronze = lire_bronze(spark, table).count()
        silver = lire_silver(spark, table).count()
        try:
            quarantaine = spark.read.parquet(f"{QUARANTAINE}/{table}").count()
        except Exception:
            quarantaine = 0
        controle(f"volume_{table}", "Volume", "bronze = silver + quarantaine",
                 bronze, silver + quarantaine,
                 "un ecart = une ligne perdue sans decision")

    for nom, df, cle_dim in (("item", item, "i_item_sk"),
                             ("store", magasin, "s_store_sk"),
                             ("date_dim", calendrier, "d_date_sk")):
        controle(f"cle_{nom}", "Cle candidate", f"{cle_dim} unique",
                 df.count() - df.select(cle_dim).distinct().count(), 0,
                 "cle non unique = risque de multiplication en jointure")

    for nom, df, cle_dim, cle_vente in (
            ("item", item, "i_item_sk", "ss_item_sk"),
            ("store", magasin, "s_store_sk", "ss_store_sk"),
            ("date_dim", calendrier, "d_date_sk", "ss_sold_date_sk")):
        controle(f"integrite_{nom}", "Integrite", f"{cle_vente} present dans {nom}",
                 ventes.join(df.select(cle_dim),
                             ventes[cle_vente] == df[cle_dim], "left_anti").count(),
                 0, "jointure en left, valeur nulle assumee et documentee")

    gold = spark.read.parquet(f"{GOLD}/ca_mensuel_magasin")
    controle("bouclage_gold", "Coherence", "somme Gold = somme Silver",
             int(gold.agg(F.round(F.sum("ca_net"), 0)).collect()[0][0] or 0),
             int(ventes.agg(F.round(F.sum("ss_net_paid"), 0)).collect()[0][0] or 0),
             "un ecart = perte ou duplication dans la chaine")

    controle("panier_moyen_calculable", "Validite", "panier moyen renseigne partout",
             gold.filter(F.col("panier_moyen").isNull()).count(), 0,
             "null si aucune transaction : KPI 3 non calculable sur ce groupe")

    # restitution
    DOC_QUALITE.mkdir(parents=True, exist_ok=True)
    (spark.createDataFrame(resultats)
     .select("controle", "dimension", "regle", "mesure", "attendu",
             "statut", "decision")
     .coalesce(1).write.mode("overwrite").option("header", True)
     .csv(f"file://{DOC_QUALITE.resolve()}/rapport_qualite_csv"))

    ok = [x for x in resultats if x["statut"] == "OK"]
    anomalies = [x for x in resultats if x["statut"] != "OK"]

    r = Rapport("RAPPORT QUALITE - TP JOUR 3 (etape 9)",
                DOC_QUALITE / "rapport-qualite.txt")
    r.dire(f"Controles executes : {len(resultats)}")
    r.dire(f"Conformes          : {len(ok)}")
    r.dire(f"Anomalies          : {len(anomalies)}")
    r.dire()
    r.dire(f"{'statut':<10}{'dimension':<16}{'controle':<32}{'mesure':>12}{'attendu':>12}")
    for x in resultats:
        r.dire(f"{x['statut']:<10}{x['dimension']:<16}{x['controle']:<32}"
               f"{x['mesure']:>12}{x['attendu']:>12}")
    r.dire()
    r.titre("BILAN")
    r.dire("1. REGLES RESPECTEES")
    for x in ok:
        r.dire(f"   {x['controle']} : {x['regle']} -> mesure {x['mesure']}")
    r.dire()
    r.dire("2. ANOMALIES RESTANTES")
    if anomalies:
        for x in anomalies:
            r.dire(f"   {x['controle']} : {x['regle']}")
            r.dire(f"      mesure {x['mesure']} au lieu de {x['attendu']}")
            r.dire(f"      decision : {x['decision']}")
    else:
        r.dire("   aucune")
    r.dire()
    r.dire("3. DONNEES MISES A L'ECART")
    r.dire("   voir la zone de quarantaine et le rapport Silver")
    r.dire()
    r.dire("4. LIMITES DES CONTROLES")
    r.dire("   - executes sur le jeu initial, pas sur le jeu de rafraichissement")
    r.dire("   - l'integrite n'est verifiable que la ou un referentiel existe")
    r.dire("   - la fraicheur n'est pas controlable : donnees synthetiques,")
    r.dire("     sans date de chargement reelle")
    r.dire("   - aucun controle ne valide la veracite metier des montants,")
    r.dire("     seulement leur plausibilite")
    r.ecrire()


# =====================================================================
# 7. ETAPE "formats" - comparaison CSV / JSON / Parquet
# =====================================================================

def etape_formats(spark):
    base = OUTPUTS / "comparaison_formats"
    base.mkdir(parents=True, exist_ok=True)
    racine = f"file://{base.resolve()}"

    gold = spark.read.parquet(f"{GOLD}/ca_mensuel_magasin")
    nb = gold.count()

    gold.write.mode("overwrite").option("header", True).csv(f"{racine}/csv")
    gold.write.mode("overwrite").json(f"{racine}/json")
    gold.write.mode("overwrite").parquet(f"{racine}/parquet")

    def poids(dossier):
        total = 0
        for racine_dir, _, fichiers in os.walk(dossier):
            for f in fichiers:
                if not f.startswith("."):
                    total += os.path.getsize(os.path.join(racine_dir, f))
        return total

    mesures = {fmt: poids(base / fmt) for fmt in ("csv", "json", "parquet")}
    reference = mesures["parquet"] or 1

    r = Rapport("COMPARAISON DES FORMATS - TP JOUR 3 (etape 8)",
                DOC_TRANSFORMATIONS / "comparaison-formats.txt")
    r.dire(f"Jeu compare : gold/ca_mensuel_magasin ({nb} lignes,"
           f" {len(gold.columns)} colonnes)")
    r.dire("Memes donnees ecrites dans les trois formats.")
    r.dire()
    r.dire(f"{'format':<10}{'octets':>12}{'Ko':>10}{'x Parquet':>12}")
    for fmt in ("csv", "json", "parquet"):
        o = mesures[fmt]
        r.dire(f"{fmt:<10}{o:>12}{o / 1024:>10.1f}{o / reference:>12.2f}")
    r.dire()
    relu = spark.read.parquet(f"{racine}/parquet")
    r.dire("Relecture du Parquet produit :")
    r.dire(f"   lignes relues : {relu.count()}")
    r.dire(f"   colonnes      : {len(relu.columns)}")
    r.dire("   types conserves sans inference : "
           + ("OUI" if relu.schema == gold.schema else "A VERIFIER"))
    r.dire()
    r.titre("ANALYSE")
    for titre, csv, js, pq in (
            ("Organisation", "une ligne de texte par enregistrement",
             "un document par enregistrement, noms de champs repetes",
             "colonnaire, par blocs, avec statistiques par bloc"),
            ("Types", "aucun, tout est texte a interpreter",
             "types de base portes par la syntaxe",
             "schema type enregistre dans le fichier"),
            ("Lisibilite", "directe au cat", "lisible mais verbeuse",
             "binaire, illisible sans outil"),
            ("Lecture selective", "non, le fichier entier est lu",
             "depend de la structure", "oui, colonne par colonne"),
            ("Usage adapte", "echange, export, zone Bronze",
             "donnees semi-structurees, API",
             "analytique, zones Silver et Gold")):
        r.dire(titre)
        r.dire(f"   CSV     : {csv}")
        r.dire(f"   JSON    : {js}")
        r.dire(f"   Parquet : {pq}")
        r.dire()
    r.dire("Limite essentielle de Parquet")
    r.dire("   Parquet decrit comment les donnees sont ecrites DANS UN")
    r.dire("   FICHIER, pas comment un ensemble de fichiers forme une")
    r.dire("   table. Ni commit atomique, ni isolation, ni versions, ni")
    r.dire("   time travel : ces garanties viennent d'un format de table")
    r.dire("   (Delta Lake, Iceberg, Hudi) pose au-dessus des memes")
    r.dire("   fichiers Parquet. Un repertoire Parquet n'est donc pas une")
    r.dire("   table transactionnelle ACID.")
    r.ecrire()


# =====================================================================
# 8. ENCHAINEMENT
# =====================================================================

ETAPES = {
    "cles": etape_cles,
    "silver": etape_silver,
    "gold": etape_gold,
    "qualite": etape_qualite,
    "formats": etape_formats,
}

if __name__ == "__main__":
    demandees = sys.argv[1:] or list(ETAPES)
    inconnues = [e for e in demandees if e not in ETAPES]
    if inconnues:
        print(f"Etape(s) inconnue(s) : {inconnues}")
        print(f"Etapes disponibles : {', '.join(ETAPES)}")
        sys.exit(1)

    spark = session()
    print(f"Spark {spark.version} - etapes : {', '.join(demandees)}\n")
    for nom in demandees:
        print("\n" + "#" * 60)
        print(f"# ETAPE {nom.upper()}")
        print("#" * 60 + "\n")
        ETAPES[nom](spark)
    spark.stop()
    print("\nTermine.")
