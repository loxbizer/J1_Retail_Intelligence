# -*- coding: utf-8 -*-
"""
TP Jour 3 - etape 8 : comparaison des formats CSV / JSON / Parquet.

Pour que la comparaison soit honnete, on ecrit LES MEMES donnees dans
les trois formats, puis on mesure le volume reellement occupe.

Les sorties vont dans outputs/comparaison_formats sur le disque local :
ce sont des fichiers de demonstration, pas des donnees du Data Lake.

    python 06_formats.py
"""

import os
from datetime import datetime
from pathlib import Path

from config import parametres as P

spark = P.session("TP3-Comparaison-Formats")

BASE = Path(P.OUTPUTS) / "comparaison_formats"
BASE.mkdir(parents=True, exist_ok=True)
racine = f"file://{BASE.resolve()}"

gold = spark.read.parquet(f"{P.GOLD}/ca_mensuel_magasin")
nb = gold.count()
print(f"Jeu compare : gold/ca_mensuel_magasin, {nb} lignes, "
      f"{len(gold.columns)} colonnes")

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


mesures = {fmt: poids(BASE / fmt) for fmt in ("csv", "json", "parquet")}
reference = mesures["parquet"] or 1

lignes = [
    "=" * 60,
    "COMPARAISON DES FORMATS - TP JOUR 3 (etape 8)",
    datetime.now().strftime("%d/%m/%Y %H:%M"),
    "=" * 60,
    f"Jeu compare : gold/ca_mensuel_magasin ({nb} lignes, {len(gold.columns)} colonnes)",
    "Memes donnees ecrites dans les trois formats.",
    "",
    f"{'format':<10}{'octets':>12}{'Ko':>10}{'x Parquet':>12}",
]
for fmt in ("csv", "json", "parquet"):
    o = mesures[fmt]
    lignes.append(f"{fmt:<10}{o:>12}{o / 1024:>10.1f}{o / reference:>12.2f}")

lignes += [
    "",
    "Relecture du Parquet produit :",
]
relu = spark.read.parquet(f"{racine}/parquet")
lignes.append(f"   lignes relues : {relu.count()}")
lignes.append(f"   colonnes      : {len(relu.columns)}")
lignes.append("   types conserves sans inference : "
              + ("OUI" if relu.schema == gold.schema else "A VERIFIER"))

lignes += [
    "",
    "=" * 60,
    "ANALYSE",
    "=" * 60,
    "",
    "Organisation",
    "   CSV     : une ligne de texte par enregistrement",
    "   JSON    : un document par enregistrement, noms de champs repetes",
    "   Parquet : colonnaire, par blocs, avec statistiques par bloc",
    "",
    "Types",
    "   CSV     : aucun type, tout est texte a interpreter a la lecture",
    "   JSON    : types de base portes par la syntaxe",
    "   Parquet : schema type enregistre dans le fichier",
    "",
    "Lecture selective",
    "   CSV     : non, le fichier entier est lu",
    "   JSON    : depend de la structure",
    "   Parquet : oui, colonne par colonne",
    "",
    "Usages",
    "   CSV     : echange, export, conservation en zone Bronze",
    "   JSON    : donnees semi-structurees, API",
    "   Parquet : analytique, zones Silver et Gold",
    "",
    "Limites",
    "   CSV     : pas de types, compression faible",
    "   JSON    : volume, cout d'analyse",
    "   Parquet : binaire non editable, et surtout : AUCUNE garantie",
    "             transactionnelle. Parquet decrit comment les donnees",
    "             sont ecrites dans un fichier, pas comment un ensemble",
    "             de fichiers forme une table. Ni commit atomique, ni",
    "             versions, ni time travel : ces garanties viennent d'un",
    "             format de table (Delta Lake, Iceberg, Hudi) pose",
    "             au-dessus des memes fichiers Parquet.",
    "",
    "FIN DU RAPPORT",
]

texte = "\n".join(lignes) + "\n"
sortie = Path(P.DOC_TRANSFORMATIONS) / "comparaison-formats.txt"
sortie.parent.mkdir(parents=True, exist_ok=True)
sortie.write_text(texte, encoding="utf-8")

print()
print(texte)
print(f"Rapport ecrit dans : {sortie.resolve()}")

spark.stop()
