"""
import_kaggle.py
----------------
Lit le CSV LinkedIn Job Postings, fait le prétraitement,
et insère uniquement les offres data dans PostgreSQL.
"""

import os
import re
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# =============================================================
# CONFIGURATION
# =============================================================

CSV_PATH = "/app/data/job_postings.csv"

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     5432,
    "dbname":   os.environ.get("POSTGRES_DB", "job_intelligent"),
    "user":     os.environ.get("POSTGRES_USER", "jobuser"),
    "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123")
}

# Mots-clés pour garder uniquement les offres data
MOTS_CLES_DATA = [
    "data", "engineer", "analyst", "scientist", "machine learning",
    "python", "sql", "spark", "hadoop", "etl", "pipeline",
    "business intelligence", "bi ", "mlops", "deep learning",
    "artificial intelligence", "ai ", "nlp", "cloud", "aws", "azure",
    "databricks", "airflow", "kafka", "dbt", "tableau", "power bi"
]

# =============================================================
# ÉTAPE 1 — LECTURE DU CSV
# =============================================================

print("=" * 50)
print("IMPORT KAGGLE — LinkedIn Job Postings")
print("=" * 50)

if not os.path.exists(CSV_PATH):
    print(f"ERREUR : fichier introuvable à {CSV_PATH}")
    exit(1)

print("\n1. Lecture du CSV...")
df = pd.read_csv(CSV_PATH)
print(f"   → {len(df)} lignes chargées au total")

# =============================================================
# ÉTAPE 2 — PRÉTRAITEMENT
# =============================================================

print("\n2. Prétraitement des données...")

# --- 2a. Supprimer les lignes sans titre ni description ---
avant = len(df)
df = df.dropna(subset=["title"])
df = df[df["description"].notna() & (df["description"].str.len() > 50)]
print(f"   → Lignes sans titre/description supprimées : {avant - len(df)}")

# --- 2b. Dédoublonner sur job_id ---
avant = len(df)
df = df.drop_duplicates(subset=["job_id"])
print(f"   → Doublons supprimés : {avant - len(df)}")

# --- 2c. Garder uniquement les offres data ---
def est_offre_data(titre):
    if pd.isna(titre):
        return False
    titre_lower = str(titre).lower()
    return any(mot in titre_lower for mot in MOTS_CLES_DATA)

avant = len(df)
df = df[df["title"].apply(est_offre_data)]
print(f"   → Offres non-data supprimées : {avant - len(df)}")
print(f"   → Offres data conservées : {len(df)}")

# --- 2d. Nettoyer la description (retirer HTML) ---
def nettoyer_html(texte):
    if pd.isna(texte):
        return None
    texte = re.sub(r'<[^>]+>', ' ', str(texte))
    texte = re.sub(r'\s+', ' ', texte)
    return texte.strip()

df["description"] = df["description"].apply(nettoyer_html)

# --- 2e. Convertir le timestamp Unix en date lisible ---
def convertir_timestamp(ts):
    try:
        return datetime.fromtimestamp(float(ts) / 1000)
    except:
        return None

df["date_propre"] = df["listed_time"].apply(convertir_timestamp)

# --- 2f. Convertir les salaires en float ---
def to_float(val):
    try:
        v = float(val)
        if v <= 0 or v > 1_000_000:
            return None
        return v
    except:
        return None

df["min_salary"] = df["min_salary"].apply(to_float)
df["max_salary"] = df["max_salary"].apply(to_float)

# --- 2g. Remplacer tous les NaN par None ---
df = df.where(pd.notnull(df), None)

print("   → Nettoyage HTML, timestamps, salaires : OK")

# =============================================================
# ÉTAPE 3 — INSERTION DANS POSTGRESQL
# =============================================================

print("\n3. Connexion à PostgreSQL...")
conn   = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
print("   → Connecté !")

# Vider la table avant réimport pour éviter les doublons
cursor.execute("TRUNCATE TABLE job_offers RESTART IDENTITY;")
print("   → Table vidée (réimport propre)")

INSERT_QUERY = """
    INSERT INTO job_offers (
        titre, entreprise, localisation, description,
        salaire_min, salaire_max, competences_texte,
        type_contrat, niveau_experience, url,
        source, date_publication
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

donnees = [
    (
        str(row.get("title", ""))[:255] if row.get("title") else None,
        str(row.get("company_name", ""))[:255] if row.get("company_name") else None,
        str(row.get("location", ""))[:500] if row.get("location") else None,
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

print(f"\n4. Insertion de {len(donnees)} offres...")
execute_batch(cursor, INSERT_QUERY, donnees, page_size=500)
conn.commit()

cursor.execute("SELECT COUNT(*) FROM job_offers")
total = cursor.fetchone()[0]
print(f"   → Total dans la base : {total} offres ✅")

# Stats de vérification
cursor.execute("""
    SELECT type_contrat, COUNT(*)
    FROM job_offers
    WHERE type_contrat IS NOT NULL
    GROUP BY type_contrat
    ORDER BY COUNT(*) DESC LIMIT 5
""")
print("\n5. Répartition par type de contrat :")
for row in cursor.fetchall():
    print(f"   {row[0]:<20} : {row[1]} offres")

cursor.execute("SELECT COUNT(*) FROM job_offers WHERE salaire_min IS NOT NULL")
print(f"\n   Offres avec salaire renseigné : {cursor.fetchone()[0]}")

cursor.close()
conn.close()

print("\n" + "=" * 50)
print("Import Kaggle terminé avec succès !")
print("Prochaine étape : lancer le scraper France Travail")
print("=" * 50)