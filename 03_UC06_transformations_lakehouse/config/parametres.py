# -*- coding: utf-8 -*-
"""
Parametres du TP Jour 3 : chemins, schemas explicites, constantes.

Un seul endroit a modifier pour changer d'environnement. C'est ce que
l'etape 10 du TP appelle externaliser les parametres : aucun chemin en
dur dans les traitements.
"""

import os

# ----------------------------------------------------------- identite HDFS
# Les zones /retail ont ete creees par le conteneur, qui tourne en root.
# Un client qui se presente sous un autre nom n'a pas le droit d'ecrire.
# On se declare donc root cote client. A faire AVANT la session Spark.
os.environ.setdefault("HADOOP_USER_NAME", "root")

# ---------------------------------------------------------------- chemins

HDFS = "hdfs://localhost:9000"

BRONZE = f"{HDFS}/retail/bronze/data"
SILVER = f"{HDFS}/retail/silver"
GOLD = f"{HDFS}/retail/gold"
QUARANTAINE = f"{HDFS}/retail/quarantine"

# Replis locaux, utilises uniquement si HDFS ne repond pas.
BRONZE_LOCAL = os.path.expanduser("~/Downloads/retail_project/data")
SORTIE_LOCALE = "data"

SEPARATEUR = "|"
ENTETE = False

# Sorties documentaires (systeme de fichiers local, pas HDFS)
DOC_EXPLORATION = "docs/exploration"
DOC_TRANSFORMATIONS = "docs/transformations"
DOC_QUALITE = "docs/quality"
OUTPUTS = "outputs"

# ------------------------------------------------------- schemas explicites
# Les types ne sont plus devines : ils sont declares. C'est ce que le
# cours appelle expliciter les types avant de produire la zone Silver.
# Remarque : Spark n'applique pas le caractere nullable a la lecture d'un
# CSV. Les colonnes obligatoires sont donc controlees par un filtre
# explicite dans 03_silver.py, pas par le schema.

from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType,
)


def _champs(definition):
    """Transforme une liste (nom, type) en StructType."""
    return StructType([StructField(nom, typ(), True) for nom, typ in definition])


SCHEMA_STORE_SALES = _champs([
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

SCHEMA_ITEM = _champs([
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

SCHEMA_STORE = _champs([
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

SCHEMA_DATE_DIM = _champs([
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

# table -> (schema, domaine Silver, colonnes obligatoires, cle)
SOURCES = {
    "store_sales": {
        "schema": SCHEMA_STORE_SALES,
        "domaine": "sales",
        "obligatoires": ["ss_item_sk", "ss_ticket_number",
                         "ss_store_sk", "ss_sold_date_sk"],
        "cle": ["ss_sold_date_sk", "ss_ticket_number", "ss_item_sk"],
    },
    "item": {
        "schema": SCHEMA_ITEM,
        "domaine": "products",
        "obligatoires": ["i_item_sk"],
        "cle": ["i_item_sk"],
    },
    "store": {
        "schema": SCHEMA_STORE,
        "domaine": "stores",
        "obligatoires": ["s_store_sk"],
        "cle": ["s_store_sk"],
    },
    "date_dim": {
        "schema": SCHEMA_DATE_DIM,
        "domaine": "time",
        "obligatoires": ["d_date_sk"],
        "cle": ["d_date_sk"],
    },
}


def chemin_bronze(table):
    return f"{BRONZE}/{table}/{table}.dat"


def chemin_silver(table):
    return f"{SILVER}/{SOURCES[table]['domaine']}/{table}"


def session(nom):
    """Session Spark locale, configuree pour lire le HDFS du conteneur."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName(nom)
        .master("local[*]")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
