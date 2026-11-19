# -*- coding: utf-8 -*-
"""
TP Jour 3 - Etape 2 et 3 : inventaire et exploration des sources.

Lance-moi depuis ~/retail-intelligence/tp3 :
    python 01_explorer.py

Ce que je fais, source par source :
  - lecture brute (delimiteur |, sans entete)
  - controle du nombre de colonnes contre le schema attendu
  - application des noms de colonnes SEULEMENT si le compte correspond
  - preuve arithmetique sur les ventes (montant etendu = quantite x prix)
  - comptage des lignes, des valeurs nulles, des doublons de cle
  - controle d'integrite referentielle entre les ventes et les dimensions

Je n'ecris jamais dans les zones Bronze. Je produis un rapport texte
dans docs/exploration/rapport-exploration.txt
"""

from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------------- chemins

HDFS = "hdfs://localhost:9000"
BRONZE_HDFS = f"{HDFS}/retail/bronze/data"
BRONZE_LOCAL = Path.home() / "Downloads" / "retail_project" / "data"

SORTIE = Path("docs/exploration/rapport-exploration.txt")

# --------------------------------------------- schemas attendus (TPCx-BB)
# Noms issus de la specification. Ils ne sont appliques qu'apres controle
# du nombre de colonnes : un ecart et on reste en _c0.._cn.

COLONNES = {
    "store_sales": [
        "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk",
        "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk",
        "ss_promo_sk", "ss_ticket_number", "ss_quantity", "ss_wholesale_cost",
        "ss_list_price", "ss_sales_price", "ss_ext_discount_amt",
        "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price",
        "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax",
        "ss_net_profit",
    ],
    "item": [
        "i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date",
        "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand_id",
        "i_brand", "i_class_id", "i_class", "i_category_id", "i_category",
        "i_manufact_id", "i_manufact", "i_size", "i_formulation", "i_color",
        "i_units", "i_container", "i_manager_id", "i_product_name",
    ],
    "store": [
        "s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date",
        "s_closed_date_sk", "s_store_name", "s_number_employees",
        "s_floor_space", "s_hours", "s_manager", "s_market_id",
        "s_geography_class", "s_market_desc", "s_market_manager",
        "s_division_id", "s_division_name", "s_company_id", "s_company_name",
        "s_street_number", "s_street_name", "s_street_type", "s_suite_number",
        "s_city", "s_county", "s_state", "s_zip", "s_country", "s_gmt_offset",
        "s_tax_precentage",
    ],
    "date_dim": [
        "d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq",
        "d_quarter_seq", "d_year", "d_dow", "d_moy", "d_dom", "d_qoy",
        "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name",
        "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday",
        "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq",
        "d_current_day", "d_current_week", "d_current_month",
        "d_current_quarter", "d_current_year",
    ],
}

CLES = {
    "store_sales": ["ss_ticket_number", "ss_item_sk"],
    "item": ["i_item_sk"],
    "store": ["s_store_sk"],
    "date_dim": ["d_date_sk"],
}

rapport = []


def dire(ligne=""):
    print(ligne)
    rapport.append(str(ligne))


# ------------------------------------------------------------------ spark

spark = (
    SparkSession.builder
    .appName("TP3-Exploration")
    .master("local[*]")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def lire(table):
    """Lit la table depuis HDFS, sinon depuis les fichiers locaux."""
    for origine, chemin in (
        ("HDFS", f"{BRONZE_HDFS}/{table}/{table}.dat"),
        ("local", str(BRONZE_LOCAL / table / f"{table}.dat")),
    ):
        try:
            df = (
                spark.read
                .option("header", False)
                .option("sep", "|")
                .option("inferSchema", True)
                .csv(chemin)
            )
            df.take(1)                      # force la lecture tout de suite
            return df, origine, chemin
        except Exception as erreur:
            dernier = str(erreur).strip().splitlines()[0][:160]
    raise RuntimeError(f"{table} illisible (HDFS et local). {dernier}")


def explorer(table):
    dire("=" * 60)
    dire(f"SOURCE : {table}")
    dire("=" * 60)

    df, origine, chemin = lire(table)
    attendu = COLONNES[table]

    dire(f"Origine      : {origine}")
    dire(f"Chemin       : {chemin}")
    dire(f"Colonnes lues: {len(df.columns)}  (schema attendu : {len(attendu)})")

    # separateur final : une colonne de plus, toujours vide
    if len(df.columns) == len(attendu) + 1:
        derniere = df.columns[-1]
        non_vides = df.filter(F.col(derniere).isNotNull()).count()
        dire(f"Colonne en trop : {derniere}, {non_vides} valeur(s) non nulle(s)")
        if non_vides == 0:
            df = df.drop(derniere)
            dire("  -> artefact du separateur final en fin de ligne, colonne retiree")

    if len(df.columns) == len(attendu):
        df = df.toDF(*attendu)
        dire("Noms de colonnes : APPLIQUES (compte conforme au schema)")
    else:
        dire("Noms de colonnes : NON APPLIQUES (compte non conforme)")
        dire("  -> les colonnes restent _c0.._cn, a signaler comme anomalie")
        df.printSchema()
        return df

    nb = df.count()
    dire(f"Lignes       : {nb}")
    dire("")
    dire("Schema :")
    for champ in df.schema.fields:
        dire(f"  {champ.name:<22} {champ.dataType.simpleString()}")

    dire("")
    dire("Valeurs nulles par colonne (seules les colonnes concernees) :")
    nulls = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    trouve = False
    for colonne, compte in nulls.items():
        if compte:
            part = 100 * compte / nb if nb else 0
            dire(f"  {colonne:<22} {compte:>10}  ({part:.2f} %)")
            trouve = True
    if not trouve:
        dire("  aucune")

    cle = CLES[table]
    distinctes = df.select(cle).distinct().count()
    dire("")
    dire(f"Cle candidate : {' + '.join(cle)}")
    dire(f"  lignes {nb} / cles distinctes {distinctes} / ecart {nb - distinctes}")
    dire("  -> cle unique" if nb == distinctes else "  -> cle NON unique, a examiner")

    dire("")
    dire("Apercu :")
    for ligne in df.limit(3).collect():
        valeurs = list(ligne.asDict().items())[:6]
        dire("  " + " | ".join(f"{k}={v}" for k, v in valeurs) + " | ...")
    dire("")
    return df


# -------------------------------------------------------------- execution

dire("=" * 60)
dire("RAPPORT D'EXPLORATION - TP JOUR 3 (etapes 2 et 3)")
dire(datetime.now().strftime("%d/%m/%Y %H:%M"))
dire("=" * 60)
dire("")

tables = {}
for nom in ("store_sales", "item", "store", "date_dim"):
    try:
        tables[nom] = explorer(nom)
    except Exception as erreur:
        dire(f"ECHEC sur {nom} : {str(erreur).strip().splitlines()[0][:200]}")
        dire("")

# preuve arithmetique : le nom des colonnes de montant est-il le bon ?
if "store_sales" in tables and "ss_quantity" in tables["store_sales"].columns:
    dire("=" * 60)
    dire("PREUVE ARITHMETIQUE SUR store_sales")
    dire("=" * 60)
    ventes = tables["store_sales"]
    echantillon = ventes.filter(
        F.col("ss_quantity").isNotNull() & F.col("ss_sales_price").isNotNull()
    ).limit(1000)
    total = echantillon.count()
    conformes = echantillon.filter(
        F.abs(F.col("ss_ext_sales_price")
              - F.col("ss_quantity") * F.col("ss_sales_price")) < 0.02
    ).count()
    dire(f"ss_ext_sales_price = ss_quantity x ss_sales_price")
    dire(f"  verifie sur {conformes} / {total} lignes testees")
    dire("  -> affectation des noms DEMONTREE" if total and conformes / total > 0.99
         else "  -> identite non verifiee, les noms restent une hypothese")
    dire("")

# integrite referentielle
if "store_sales" in tables:
    dire("=" * 60)
    dire("INTEGRITE REFERENTIELLE (ventes vers dimensions)")
    dire("=" * 60)
    ventes = tables["store_sales"]
    for dim, cle_vente, cle_dim in (
        ("item", "ss_item_sk", "i_item_sk"),
        ("store", "ss_store_sk", "s_store_sk"),
        ("date_dim", "ss_sold_date_sk", "d_date_sk"),
    ):
        if dim not in tables:
            continue
        orphelins = ventes.join(
            tables[dim].select(cle_dim),
            ventes[cle_vente] == tables[dim][cle_dim],
            "left_anti",
        ).count()
        nuls = ventes.filter(F.col(cle_vente).isNull()).count()
        dire(f"{cle_vente} -> {dim}.{cle_dim}")
        dire(f"  lignes sans correspondance : {orphelins}  (dont cle nulle : {nuls})")
    dire("")

dire("FIN DU RAPPORT")

SORTIE.parent.mkdir(parents=True, exist_ok=True)
SORTIE.write_text("\n".join(rapport) + "\n", encoding="utf-8")
print(f"\nRapport ecrit dans : {SORTIE.resolve()}")

spark.stop()
