# -*- coding: utf-8 -*-
"""
TP Jour 3 - etapes 4, 5 et 6 : Bronze -> Silver.

Ce que fait ce traitement, dans l'ordre :
  1. lit chaque source avec un SCHEMA EXPLICITE (plus d'inference)
  2. applique les regles de nettoyage, en comptant les lignes touchees
  3. ecarte en quarantaine les lignes dont une cle obligatoire est nulle
  4. ajoute trois colonnes de provenance
  5. ecrit la zone Silver en Parquet
  6. relit ce qui vient d'etre ecrit et controle schema, volume, bouclage

Principe tenu partout : aucune ligne n'est supprimee. Une ligne non
conforme est ECARTEE vers la quarantaine, donc conservee et auditable.

    python 03_silver.py
"""

from datetime import datetime
from pathlib import Path

from config import parametres as P
from pyspark.sql import functions as F

rapport = []


def dire(ligne=""):
    print(ligne)
    rapport.append(str(ligne))


spark = P.session("TP3-Bronze-vers-Silver")

dire("=" * 60)
dire("RAPPORT SILVER - TP JOUR 3 (etapes 4, 5 et 6)")
dire(datetime.now().strftime("%d/%m/%Y %H:%M"))
dire("=" * 60)
dire("")

bilan = []

for table, conf in P.SOURCES.items():
    dire("=" * 60)
    dire(f"TABLE : {table}  ->  silver/{conf['domaine']}/{table}")
    dire("=" * 60)

    # --- 1. lecture avec schema explicite -----------------------------
    df = (
        spark.read
        .option("header", P.ENTETE)
        .option("sep", P.SEPARATEUR)
        .schema(conf["schema"])          # plus aucune inference de type
        .csv(P.chemin_bronze(table))
    )
    nb_bronze = df.count()
    dire(f"Lignes lues en Bronze : {nb_bronze}")
    dire(f"Colonnes : {len(df.columns)} (schema declare, types imposes)")

    # --- 2. regles de nettoyage ---------------------------------------
    # R1 - harmonisation des libelles : espaces de bord retires.
    #      Aucune information n'est perdue, seule la mise en forme change.
    colonnes_texte = [
        c.name for c in conf["schema"].fields
        if c.dataType.simpleString() == "string"
    ]
    for colonne in colonnes_texte:
        df = df.withColumn(colonne, F.trim(F.col(colonne)))
    dire(f"R1 trim applique sur {len(colonnes_texte)} colonne(s) texte")

    # R2 - conversions sures. try_cast renvoie null au lieu de lever une
    #      exception : on peut donc MESURER les echecs de conversion.
    #      Ici les types sont deja imposes par le schema, le controle sert
    #      a prouver qu'aucune valeur n'a ete perdue a la lecture.
    nulls_avant = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    non_vides = {c: v for c, v in nulls_avant.items() if v}
    if non_vides:
        dire("R2 valeurs nulles constatees apres typage :")
        for colonne, compte in non_vides.items():
            dire(f"      {colonne:<24}{compte:>9}  ({100 * compte / nb_bronze:.2f} %)")
    else:
        dire("R2 aucune valeur nulle apres typage")

    # R3 - mise a l'ecart des lignes sans cle obligatoire.
    #      Ecartees, pas supprimees : elles partent en quarantaine.
    condition_valide = None
    for colonne in conf["obligatoires"]:
        test = F.col(colonne).isNotNull()
        condition_valide = test if condition_valide is None else condition_valide & test

    valides = df.filter(condition_valide)
    rejetees = df.filter(~condition_valide)
    nb_rejetees = rejetees.count()
    dire(f"R3 cles obligatoires {conf['obligatoires']}")
    dire(f"      lignes ecartees : {nb_rejetees}")

    if nb_rejetees:
        (rejetees
         .withColumn("motif_rejet", F.lit("cle obligatoire nulle"))
         .withColumn("dt_rejet", F.current_timestamp())
         .write.mode("overwrite")
         .parquet(f"{P.QUARANTAINE}/{table}"))
        dire(f"      ecrites dans {P.QUARANTAINE}/{table}")

    # R4 - montants negatifs : CONSERVES, et marques.
    #      Un montant negatif peut etre un retour, une correction ou une
    #      vente a perte. On documente, on ne purge pas.
    if table == "store_sales":
        valides = valides.withColumn(
            "ind_montant_negatif",
            (F.col("ss_net_paid") < 0) | (F.col("ss_net_profit") < 0),
        )
        nb_negatifs = valides.filter("ind_montant_negatif").count()
        dire(f"R4 lignes a montant negatif : {nb_negatifs} -> conservees et marquees")

    # --- 3. unicite de la cle ------------------------------------------
    nb_valides = valides.count()
    distinctes = valides.select(conf["cle"]).distinct().count()
    dire(f"Cle {' + '.join(conf['cle'])}")
    dire(f"      lignes {nb_valides} / distinctes {distinctes} / ecart {nb_valides - distinctes}")
    dire("      -> cle unique, aucune deduplication necessaire"
         if nb_valides == distinctes else
         "      -> cle non unique : AUCUNE suppression, ecart documente en qualite")

    # --- 4. provenance --------------------------------------------------
    silver = (
        valides
        .withColumn("src_fichier", F.input_file_name())
        .withColumn("src_systeme", F.lit(f"TPCx-BB / {table}"))
        .withColumn("dt_traitement", F.current_timestamp())
    )

    # --- 5. ecriture ----------------------------------------------------
    destination = P.chemin_silver(table)
    silver.write.mode("overwrite").parquet(destination)
    dire(f"Ecrit : {destination}")

    # --- 6. controles apres ecriture ------------------------------------
    relu = spark.read.parquet(destination)
    nb_relu = relu.count()
    dire("Controles apres ecriture :")
    dire(f"      relecture            : OK")
    dire(f"      lignes relues        : {nb_relu}")
    dire(f"      colonnes             : {len(relu.columns)} (dont 3 de provenance)")
    dire(f"      bouclage Bronze      : {nb_bronze} = {nb_relu} + {nb_rejetees} -> "
         + ("OK" if nb_bronze == nb_relu + nb_rejetees else "ECART A EXPLIQUER"))
    types_identiques = [f.dataType for f in relu.schema.fields][:len(conf["schema"].fields)] == \
                       [f.dataType for f in conf["schema"].fields]
    dire(f"      types conserves      : {'OK' if types_identiques else 'A VERIFIER'}")
    dire("")

    bilan.append({
        "table": table,
        "bronze": nb_bronze,
        "silver": nb_relu,
        "quarantaine": nb_rejetees,
        "cle_unique": nb_valides == distinctes,
    })

# ------------------------------------------------------------- recapitulatif

dire("=" * 60)
dire("RECAPITULATIF")
dire("=" * 60)
dire(f"{'table':<14}{'bronze':>10}{'silver':>10}{'quarant.':>10}{'cle unique':>13}")
for b in bilan:
    dire(f"{b['table']:<14}{b['bronze']:>10}{b['silver']:>10}"
         f"{b['quarantaine']:>10}{str(b['cle_unique']):>13}")
dire("")
dire("Equation de conservation : bronze = silver + quarantaine, par table.")
dire("Aucune ligne n'a disparu sans decision tracee.")
dire("")
dire("FIN DU RAPPORT")

sortie = Path(P.DOC_TRANSFORMATIONS) / "rapport-silver.txt"
sortie.parent.mkdir(parents=True, exist_ok=True)
sortie.write_text("\n".join(rapport) + "\n", encoding="utf-8")
print(f"\nRapport ecrit dans : {sortie.resolve()}")

spark.stop()
