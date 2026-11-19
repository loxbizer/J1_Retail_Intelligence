# -*- coding: utf-8 -*-
"""
TP Jour 3 - etape 9 : controles qualite.

Sept dimensions exigees par le TP : completude, unicite, validite des
types, coherence des valeurs, volumes, cles candidates, integrite
referentielle.

Chaque controle produit une ligne : regle, mesure, attendu, statut,
decision. Le resultat part dans un CSV exploitable et dans un rapport
texte lisible.

    python 05_qualite.py
"""

from datetime import datetime
from pathlib import Path

from config import parametres as P
from pyspark.sql import functions as F

spark = P.session("TP3-Qualite")

resultats = []


def controle(nom, dimension, regle, mesure, attendu, decision):
    statut = "OK" if mesure == attendu else "ANOMALIE"
    resultats.append({
        "controle": nom, "dimension": dimension, "regle": regle,
        "mesure": int(mesure), "attendu": int(attendu),
        "statut": statut, "decision": decision,
    })
    print(f"{statut:<9}{nom:<28}mesure {mesure} / attendu {attendu}")


ventes = spark.read.parquet(P.chemin_silver("store_sales"))
item = spark.read.parquet(P.chemin_silver("item"))
magasin = spark.read.parquet(P.chemin_silver("store"))
calendrier = spark.read.parquet(P.chemin_silver("date_dim"))

nb_ventes = ventes.count()

# 1 - completude
for colonne in ("ss_item_sk", "ss_ticket_number", "ss_store_sk", "ss_sold_date_sk"):
    controle(f"completude_{colonne}", "Completude", f"{colonne} non nul",
             ventes.filter(F.col(colonne).isNull()).count(), 0,
             "ligne ecartee en quarantaine si non nul impossible")

# 2 - unicite de la cle de la table de faits
cle = P.SOURCES["store_sales"]["cle"]
distinctes = ventes.select(cle).distinct().count()
controle("unicite_cle_vente", "Unicite", " + ".join(cle),
         nb_ventes - distinctes, 0,
         "aucune suppression : ecart documente et explique")

# 3 - validite des types
attendus = {f.name: f.dataType.simpleString()
            for f in P.SCHEMA_STORE_SALES.fields}
observes = {f.name: f.dataType.simpleString() for f in ventes.schema.fields}
ecarts = sum(1 for n, t in attendus.items() if observes.get(n) != t)
controle("validite_types", "Validite", "types conformes au schema declare",
         ecarts, 0, "schema explicite impose a la lecture")

# 4 - coherence des valeurs
controle("coherence_quantite", "Coherence", "ss_quantity strictement positif",
         ventes.filter(F.col("ss_quantity") <= 0).count(), 0,
         "une quantite nulle ou negative serait un retour, a traiter a part")

bornes = calendrier.agg(F.min("d_date_sk"), F.max("d_date_sk")).collect()[0]
controle("coherence_date", "Coherence", "date de vente dans la plage du referentiel",
         ventes.filter((F.col("ss_sold_date_sk") < bornes[0])
                       | (F.col("ss_sold_date_sk") > bornes[1])).count(), 0,
         "hors plage -> quarantaine")

# 5 - volumes : equation de conservation
for table in P.SOURCES:
    bronze = (spark.read.option("sep", P.SEPARATEUR).option("header", P.ENTETE)
              .schema(P.SOURCES[table]["schema"])
              .csv(P.chemin_bronze(table)).count())
    silver = spark.read.parquet(P.chemin_silver(table)).count()
    try:
        quarantaine = spark.read.parquet(f"{P.QUARANTAINE}/{table}").count()
    except Exception:
        quarantaine = 0
    controle(f"volume_{table}", "Volume", "bronze = silver + quarantaine",
             bronze, silver + quarantaine, "ecart = ligne perdue sans decision")

# 6 - cles candidates des dimensions
for nom, df, cle_dim in (("item", item, "i_item_sk"),
                         ("store", magasin, "s_store_sk"),
                         ("date_dim", calendrier, "d_date_sk")):
    controle(f"cle_{nom}", "Cle candidate", f"{cle_dim} unique",
             df.count() - df.select(cle_dim).distinct().count(), 0,
             "cle non unique = risque de multiplication en jointure")

# 7 - integrite referentielle
for nom, df, cle_dim, cle_vente in (("item", item, "i_item_sk", "ss_item_sk"),
                                    ("store", magasin, "s_store_sk", "ss_store_sk"),
                                    ("date_dim", calendrier, "d_date_sk", "ss_sold_date_sk")):
    controle(f"integrite_{nom}", "Integrite", f"{cle_vente} present dans {nom}",
             ventes.join(df.select(cle_dim),
                         ventes[cle_vente] == df[cle_dim], "left_anti").count(),
             0, "jointure en left, valeur nulle assumee et documentee")

# 8 - coherence du Gold avec le Silver
gold = spark.read.parquet(f"{P.GOLD}/ca_mensuel_magasin")
total_silver = ventes.agg(F.round(F.sum("ss_net_paid"), 0)).collect()[0][0] or 0
total_gold = gold.agg(F.round(F.sum("ca_net"), 0)).collect()[0][0] or 0
controle("bouclage_gold", "Coherence", "somme Gold = somme Silver",
         int(total_gold), int(total_silver),
         "ecart = perte ou duplication dans la chaine")

controle("panier_moyen_calculable", "Validite", "panier moyen non nul partout",
         gold.filter(F.col("panier_moyen").isNull()).count(), 0,
         "null si aucune transaction : KPI 3 non calculable sur ce groupe")

# ------------------------------------------------------------- restitution

rapport = spark.createDataFrame(resultats).select(
    "controle", "dimension", "regle", "mesure", "attendu", "statut", "decision")

Path(P.DOC_QUALITE).mkdir(parents=True, exist_ok=True)
(rapport.coalesce(1).write.mode("overwrite").option("header", True)
 .csv(f"file://{Path(P.DOC_QUALITE).resolve()}/rapport_qualite_csv"))

ok = sum(1 for r in resultats if r["statut"] == "OK")
anomalies = [r for r in resultats if r["statut"] != "OK"]

lignes = [
    "=" * 60,
    "RAPPORT QUALITE - TP JOUR 3 (etape 9)",
    datetime.now().strftime("%d/%m/%Y %H:%M"),
    "=" * 60,
    f"Controles executes : {len(resultats)}",
    f"Conformes          : {ok}",
    f"Anomalies          : {len(anomalies)}",
    "",
    f"{'statut':<10}{'dimension':<16}{'controle':<30}{'mesure':>10}{'attendu':>9}",
]
for r in resultats:
    lignes.append(f"{r['statut']:<10}{r['dimension']:<16}{r['controle']:<30}"
                  f"{r['mesure']:>10}{r['attendu']:>9}")

lignes += ["", "=" * 60, "BILAN", "=" * 60, "",
           "1. REGLES RESPECTEES"]
for r in resultats:
    if r["statut"] == "OK":
        lignes.append(f"   {r['controle']} : {r['regle']} -> mesure {r['mesure']}")

lignes += ["", "2. ANOMALIES RESTANTES"]
if anomalies:
    for r in anomalies:
        lignes.append(f"   {r['controle']} : {r['regle']}")
        lignes.append(f"      mesure {r['mesure']} au lieu de {r['attendu']}")
        lignes.append(f"      decision : {r['decision']}")
else:
    lignes.append("   aucune")

lignes += [
    "",
    "3. DONNEES MISES A L'ECART",
    "   voir la zone de quarantaine et le rapport Silver",
    "",
    "4. LIMITES DES CONTROLES",
    "   - controles executes sur l'integralite du jeu initial, pas sur le",
    "     jeu de rafraichissement",
    "   - l'integrite n'est verifiable que la ou un referentiel existe",
    "   - la fraicheur n'est pas controlable : donnees synthetiques, sans",
    "     date de chargement reelle",
    "   - aucun controle ne valide la veracite metier des montants,",
    "     seulement leur plausibilite",
    "",
    "FIN DU RAPPORT",
]

texte = "\n".join(lignes) + "\n"
sortie = Path(P.DOC_QUALITE) / "rapport-qualite.txt"
sortie.write_text(texte, encoding="utf-8")

print()
print(texte)
print(f"Rapport ecrit dans : {sortie.resolve()}")

spark.stop()
