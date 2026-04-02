"""
import_kaggle.py
----------------
Lit le CSV LinkedIn Job Postings et insère les offres dans PostgreSQL.
Les paramètres de connexion viennent des variables d'environnement Docker.
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# =============================================================
# 1. CONFIGURATION — vient automatiquement du docker-compose
# =============================================================

CSV_PATH = "/app/data/job_postings.csv"   # Le CSV est dans le volume monté

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     5432,
    "dbname":   os.environ.get("POSTGRES_DB", "job_intelligent"),
    "user":     os.environ.get("POSTGRES_USER", "jobuser"),
    "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123")
}

# Commencez par 5000 pour tester, puis mettez None pour tout importer
LIMITE = 5000

# =============================================================
# 2. LECTURE DU CSV
# =============================================================

print(f"Lecture du fichier {CSV_PATH}...")

if not os.path.exists(CSV_PATH):
    print(f"ERREUR : fichier introuvable à {CSV_PATH}")
    print("Vérifiez que vous avez bien mis le CSV dans importer/data/")
    exit(1)

df = pd.read_csv(CSV_PATH, nrows=LIMITE)
print(f"  → {len(df)} lignes chargées")

# =============================================================
# 3. NETTOYAGE
# =============================================================

print("Nettoyage des données...")

def convertir_timestamp(ts):
    try:
        return datetime.fromtimestamp(float(ts) / 1000)
    except:
        return None

def to_float(val):
    try:
        return float(val)
    except:
        return None

df["date_propre"]  = df["listed_time"].apply(convertir_timestamp)
df["min_salary"]   = df["min_salary"].apply(to_float)
df["max_salary"]   = df["max_salary"].apply(to_float)
df = df.where(pd.notnull(df), None)

print("  → Nettoyage terminé")

# =============================================================
# 4. CONNEXION ET INSERTION
# =============================================================

print("Connexion à PostgreSQL...")
conn   = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
print("  → Connecté !")

INSERT_QUERY = """
    INSERT INTO job_offers (
        titre, entreprise, localisation, description,
        salaire_min, salaire_max, competences_texte,
        type_contrat, niveau_experience, url, source, date_publication
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

donnees = [
    (
        row.get("title"),
        row.get("company_name"),
        row.get("location"),
        row.get("description"),
        row.get("min_salary"),
        row.get("max_salary"),
        row.get("skills_desc"),
        row.get("formatted_work_type"),
        row.get("formatted_experience_level"),
        row.get("job_posting_url"),
        "linkedin",
        row.get("date_propre")
    )
    for _, row in df.iterrows()
]

print(f"Insertion de {len(donnees)} offres...")
execute_batch(cursor, INSERT_QUERY, donnees, page_size=500)
conn.commit()

cursor.execute("SELECT COUNT(*) FROM job_offers")
print(f"  → Total dans la base : {cursor.fetchone()[0]} offres")

cursor.close()
conn.close()
print("\nImport terminé ! Prochaine étape : le backend FastAPI.")