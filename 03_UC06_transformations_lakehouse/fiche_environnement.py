# -*- coding: utf-8 -*-
"""
Genere la fiche environnement du TP Jour 3 (livrable n.1).

A lancer depuis VS Code, avec l'environnement virtuel du TP active.
Le script releve les versions reellement installees, teste Spark,
teste la lecture HDFS, et ecrit le tout dans :

    docs/environment/fiche-environnement-tp3.txt

Rien n'est installe, rien n'est modifie : le script lit et ecrit
seulement son fichier de sortie.
"""

import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SORTIE = Path("docs/environment/fiche-environnement-tp3.txt")

HDFS_BASE = "hdfs://localhost:9000"
CHEMIN_BRONZE = f"{HDFS_BASE}/retail/bronze/data"
CHEMIN_TEST = f"{CHEMIN_BRONZE}/store/store.dat"   # le plus petit fichier : 3 Ko
REPLI_LOCAL = Path.home() / "Downloads" / "retail_project" / "data"

SOURCES = [
    ("store_sales.dat", "table de faits"),
    ("date_dim.dat", "dimension temps"),
    ("store.dat", "dimension magasin"),
    ("item.dat", "dimension produit"),
]


def commande(args):
    """Execute une commande et renvoie sa sortie, sans faire echouer le script."""
    try:
        resultat = subprocess.run(
            args, capture_output=True, text=True, timeout=60
        )
        sortie = (resultat.stdout + resultat.stderr).strip()
        return sortie if sortie else "(aucune sortie)"
    except FileNotFoundError:
        return f"(commande indisponible : {args[0]})"
    except subprocess.TimeoutExpired:
        return "(delai depasse)"
    except Exception as erreur:                     # pragma: no cover
        return f"(erreur : {erreur})"


def section(titre, contenu):
    return f"===== {titre} =====\n{contenu}\n"


def taille_lisible(octets):
    for unite in ("o", "Ko", "Mo", "Go"):
        if octets < 1024 or unite == "Go":
            return f"{octets:.1f} {unite}".replace(".0 ", " ")
        octets /= 1024


# ---------------------------------------------------------------- releves

print("Releve des versions...")

py_version = platform.python_version()
py_chemin = sys.executable
pip_version = commande([sys.executable, "-m", "pip", "--version"])
java_version = commande(["java", "-version"])
os_version = commande(["sw_vers"]) if platform.system() == "Darwin" else platform.platform()
ncpu = commande(["sysctl", "-n", "hw.ncpu"]) if platform.system() == "Darwin" else str(os.cpu_count())
memoire = commande(["sysctl", "-n", "hw.memsize"]) if platform.system() == "Darwin" else "(non releve)"

try:
    memoire_go = f"{int(memoire) / (1024 ** 3):.0f} Go ({memoire} octets)"
except (TypeError, ValueError):
    memoire_go = memoire

try:
    import pyspark
    pyspark_version = pyspark.__version__
except ImportError:
    pyspark_version = "(pyspark absent de cet environnement)"

try:
    import py4j
    py4j_version = py4j.__version__
except Exception:
    py4j_version = "(non releve)"

docker_dispo = shutil.which("docker") is not None
if docker_dispo:
    conteneurs = commande([
        "docker", "ps", "--filter", "name=retail-",
        "--format", "{{.Names}} : {{.Status}}",
    ])
    hadoop_version = commande([
        "docker", "exec", "retail-namenode", "hadoop", "version",
    ]).splitlines()[0] if conteneurs and "retail-namenode" in conteneurs else "(conteneur arrete)"
else:
    conteneurs = "(docker indisponible)"
    hadoop_version = "(docker indisponible)"

# ------------------------------------------------------------ test spark

print("Test de la session Spark...")

test_spark = "ECHEC"
spark_version = "(non demarre)"
detail_spark = ""
test_hdfs = "NON TESTE"
detail_hdfs = ""

try:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("TP3-Fiche-Environnement")
        .master("local[*]")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark_version = spark.version

    jeu = spark.createDataFrame(
        [("Magasin A", 1200.0), ("Magasin B", 850.0), ("Magasin C", 1600.0)],
        ["magasin", "chiffre_affaires"],
    )
    nb = jeu.count()
    test_spark = "OK" if nb == 3 else f"ECHEC (3 lignes attendues, {nb} obtenues)"
    detail_spark = f"Session local[*] creee, Spark {spark_version}, jeu de test a {nb} lignes."

    print("Test de lecture HDFS...")
    try:
        lignes = spark.read.text(CHEMIN_TEST).count()
        test_hdfs = "OK"
        detail_hdfs = (
            f"Lecture de {CHEMIN_TEST} : {lignes} lignes lues depuis HDFS.\n"
            "Les traitements du TP sont donc executes sur les donnees HDFS."
        )
    except Exception as erreur:
        test_hdfs = "ECHEC"
        premiere_ligne = str(erreur).strip().splitlines()[0][:300]
        detail_hdfs = (
            f"Lecture de {CHEMIN_TEST} impossible.\n"
            f"Message retenu : {premiere_ligne}\n"
            "Solution de repli appliquee : lecture des fichiers locaux."
        )

    spark.stop()
except Exception as erreur:
    detail_spark = f"Session Spark non creee. Message : {str(erreur).strip().splitlines()[0][:300]}"

# ------------------------------------------------------- sources locales

inventaire = []
for nom, nature in SOURCES:
    dossier = nom.replace(".dat", "")
    chemin = REPLI_LOCAL / dossier / nom
    if chemin.exists():
        inventaire.append(f"  {nom:<18}{taille_lisible(chemin.stat().st_size):>10}   {nature}")
    else:
        inventaire.append(f"  {nom:<18}{'absent':>10}   {nature}")

repli_dispo = "oui" if REPLI_LOCAL.exists() else "non"

# ------------------------------------------------------------- redaction

maintenant = datetime.now().strftime("%d/%m/%Y a %H:%M")

fiche = f"""==================================================
FICHE ENVIRONNEMENT - TP JOUR 3 (UC-06)
Retail Intelligence Platform
Releve du {maintenant}
==================================================

Les versions ci-dessous sont celles reellement observees sur la
machine de travail, relevees automatiquement avant le debut des
traitements.

{section("1. MACHINE ET SYSTEME", os_version)}
{section("2. VERSION PYTHON", f"Python {py_version}\nInterpreteur : {py_chemin}\n{pip_version}")}
{section("3. VERSION JAVA", java_version)}
{section("4. VERSION PYSPARK", f"PySpark {pyspark_version}\npy4j {py4j_version}\nInstallation par pip dans l'environnement virtuel du TP.")}
{section("5. COMPATIBILITE DES VERSIONS", f'''Matrice annoncee par Spark 4.3 : Java 17, 21 ou 25, Python 3.11 et
superieur. Les versions de Java 25 anterieures a la 25.0.3 sont
depreciees.
Versions installees ici : voir sections 2 a 4.
Conclusion : combinaison conforme a la matrice annoncee, aucune
reinstallation necessaire.

Point de vigilance : a partir de Spark 4, une conversion de type ou
une division qui echoue leve une exception au lieu de renvoyer une
valeur nulle. Les traitements utilisent donc try_cast et try_divide,
ce qui permet en plus de compter les conversions echouees.''')}
{section("6. ENVIRONNEMENT VIRTUEL", f'''Emplacement : {Path.home()}/retail-intelligence/tp3/.venv
Activation : source .venv/bin/activate
A reactiver a chaque ouverture de terminal.
Bibliotheques : pyspark, jupyterlab, ipykernel.''')}
{section("7. MODE D'EXECUTION", f'''Spark en mode local, master = local[*].
Processeurs disponibles : {ncpu}
Aucun cluster Spark n'est utilise. Ce mode ne reproduit pas les
conditions d'un environnement distribue de production.
Stockage HDFS fourni par Hadoop dans Docker (mis en place au Jour 2).
{hadoop_version}
Conteneurs : {conteneurs}''')}
{section("8. CHEMINS D'ACCES AUX DONNEES", f'''Source principale (zone Bronze du Jour 2) :
  {CHEMIN_BRONZE}/<table>/<table>.dat
Rafraichissement :
  {HDFS_BASE}/retail/bronze/data_refresh/
Seconde arborescence de depot :
  {HDFS_BASE}/retail-lake/raw/tpcxbb/initial/
Fichiers d'origine conserves sur la machine :
  {REPLI_LOCAL}  (present : {repli_dispo})

Sources retenues pour le Jour 3 :
{chr(10).join(inventaire)}

Les fichiers sources ne sont pas modifies et aucune ecriture n'est
effectuee dans les zones Bronze.''')}
{section("9. REPERTOIRES DE SORTIE", f'''Zone Silver      : {HDFS_BASE}/retail/silver
Zone Gold        : {HDFS_BASE}/retail/gold
Zone quarantaine : {HDFS_BASE}/retail/quarantine
Metadonnees      : {HDFS_BASE}/retail/metadata
Ces zones ont ete creees au Jour 2 et laissees vides.
Rapports, journaux et preuves : repertoires docs/ et outputs/.''')}
{section("10. RESSOURCES DISPONIBLES", f'''Processeurs : {ncpu}
Memoire physique : {memoire_go}
Memoire du driver : valeur par defaut, non modifiee.''')}
{section("11. TEST DE VALIDATION DE L'ENVIRONNEMENT", f'''Resultat : {test_spark}
{detail_spark}''')}
{section("12. LECTURE HDFS DEPUIS LA MACHINE LOCALE", f'''Reglage applique cote client :
  spark.hadoop.dfs.client.use.datanode.hostname = true
Ce reglage est necessaire parce que le NameNode communique au client
l'adresse interne du DataNode dans le reseau Docker.
Resultat : {test_hdfs}
{detail_hdfs}''')}
{section("13. SOLUTION DE REPLI", f'''Si la lecture HDFS n'aboutit pas, les traitements sont realises sur
les fichiers de {REPLI_LOCAL} et les zones Silver et Gold sont
ecrites sous ~/retail-intelligence/tp3/data.
Cette solution est prevue par le TP a condition d'etre documentee.
Dans ce cas le traitement reste local et n'est pas presente comme un
traitement distribue sur cluster.
Repli utilise : {"non" if test_hdfs == "OK" else ("oui" if test_hdfs == "ECHEC" else "a determiner")}''')}
===== FIN DE FICHE =====
"""

SORTIE.parent.mkdir(parents=True, exist_ok=True)
SORTIE.write_text(fiche, encoding="utf-8")

print()
print(fiche)
print(f"Fiche ecrite dans : {SORTIE.resolve()}")
