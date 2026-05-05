"""
save_to_db.py
-------------
Sauvegarde les offres dans job_market.job_offers (schéma Gold).
Remplit aussi dim_source et dim_contrat automatiquement
au moment de l'insertion — pas de seed manuel dans init.sql.
"""

import os
import io
import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from minio import Minio

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST",     "postgres_app"),
    "port":     5432,
    "dbname":   os.environ.get("POSTGRES_DB",       "job_intelligent"),
    "user":     os.environ.get("POSTGRES_USER",     "jobuser"),
    "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
}

MINIO_CONFIG = {
    "endpoint":   os.environ.get("MINIO_ENDPOINT",   "minio:9000"),
    "access_key": os.environ.get("MINIO_ACCESS_KEY", "admin"),
    "secret_key": os.environ.get("MINIO_SECRET_KEY", "motdepasse123"),
    "secure":     False,
}

BUCKET_RAW = "raw-scrapes"

# URL de base par source — alimentent dim_source automatiquement
SOURCE_URLS = {
    "france_travail": "https://api.francetravail.io",
    "remotive":       "https://remotive.com/api/remote-jobs",
    "adzuna":         "https://api.adzuna.com",
    "linkedin":       "https://www.linkedin.com/jobs",
}

# Catégorie par type de contrat — alimentent dim_contrat automatiquement
CONTRAT_CATEGORIES = {
    "Full-time":  "permanent",
    "Part-time":  "permanent",
    "Contract":   "temporaire",
    "Freelance":  "freelance",
    "Internship": "stage",
    "CDI":        "permanent",
    "CDD":        "temporaire",
    "MIS":        "temporaire",
}


# ─────────────────────────────────────────────
# HELPERS DIMENSIONS
# ─────────────────────────────────────────────

def upsert_source(cursor, source: str) -> int:
    """
    Insère la source dans dim_source si elle n'existe pas.
    Retourne l'id.
    """
    url_base = SOURCE_URLS.get(source, "")
    cursor.execute("""
        INSERT INTO job_market.dim_source (nom, url_base)
        VALUES (%s, %s)
        ON CONFLICT (nom) DO NOTHING
    """, (source, url_base))
    cursor.execute("SELECT id FROM job_market.dim_source WHERE nom = %s", (source,))
    return cursor.fetchone()[0]


def upsert_contrat(cursor, type_contrat: str) -> int | None:
    """
    Insère le type de contrat dans dim_contrat si inconnu.
    Retourne l'id, ou None si type_contrat est vide.
    """
    if not type_contrat:
        return None
    categorie = CONTRAT_CATEGORIES.get(type_contrat, "autre")
    cursor.execute("""
        INSERT INTO job_market.dim_contrat (type_contrat, categorie)
        VALUES (%s, %s)
        ON CONFLICT (type_contrat) DO NOTHING
    """, (type_contrat, categorie))
    cursor.execute("SELECT id FROM job_market.dim_contrat WHERE type_contrat = %s", (type_contrat,))
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_localisation(cursor, localisation: str) -> int | None:
    """
    Insère la localisation dans dim_localisation si inconnue.
    Retourne l'id, ou None si localisation est vide.
    """
    if not localisation:
        return None
    is_remote = "remote" in localisation.lower() or "télétravail" in localisation.lower()
    cursor.execute("""
        INSERT INTO job_market.dim_localisation (ville, is_remote)
        VALUES (%s, %s)
        ON CONFLICT (ville, pays) DO NOTHING
    """, (localisation, is_remote))
    cursor.execute(
        "SELECT id FROM job_market.dim_localisation WHERE ville = %s",
        (localisation,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


# ─────────────────────────────────────────────
# MINIO
# ─────────────────────────────────────────────

def sauvegarder_brut_dans_minio(offres_brutes, source="remotive"):
    try:
        client = Minio(**MINIO_CONFIG)
        if not client.bucket_exists(BUCKET_RAW):
            client.make_bucket(BUCKET_RAW)

        date_str    = datetime.now().strftime("%Y-%m-%d_%H-%M")
        nom_fichier = f"{source}_{date_str}.json"
        contenu     = json.dumps(offres_brutes, ensure_ascii=False, indent=2).encode("utf-8")

        client.put_object(
            BUCKET_RAW, nom_fichier,
            io.BytesIO(contenu), len(contenu),
            content_type="application/json"
        )
        print(f"   → Archivé dans MinIO : {BUCKET_RAW}/{nom_fichier} ✅")

    except Exception as e:
        print(f"   ⚠️ MinIO indisponible, archive ignorée : {e}")


# ─────────────────────────────────────────────
# INSERTION PRINCIPALE
# ─────────────────────────────────────────────

def inserer_dans_postgres(offres_propres, source="remotive"):
    """
    Insère les offres dans job_market.job_offers.
    Remplit automatiquement dim_source, dim_contrat, dim_localisation.
    """
    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 1. Upsert la source une seule fois pour tout le batch
    source_id = upsert_source(cursor, source)
    conn.commit()

    # 2. Enrichir chaque offre avec les IDs de dimensions
    offres_enrichies = []
    for offre in offres_propres:
        contrat_id     = upsert_contrat(cursor,     offre.get("type_contrat"))
        localisation_id = upsert_localisation(cursor, offre.get("localisation"))

        offres_enrichies.append({
            **offre,
            "source_id":        source_id,
            "contrat_id":       contrat_id,
            "localisation_id":  localisation_id,
        })

    conn.commit()

    # 3. Insertion en batch dans job_market.job_offers
    INSERT_QUERY = """
        INSERT INTO job_market.job_offers (
            titre, entreprise, localisation, description,
            salaire_min, salaire_max, devise,
            competences_texte, type_contrat, niveau_experience,
            url, source, source_id, contrat_id, localisation_id,
            date_publication
        ) VALUES (
            %(titre)s, %(entreprise)s, %(localisation)s, %(description)s,
            %(salaire_min)s, %(salaire_max)s, %(devise)s,
            %(competences_texte)s, %(type_contrat)s, %(niveau_experience)s,
            %(url)s, %(source)s, %(source_id)s, %(contrat_id)s, %(localisation_id)s,
            %(date_publication)s
        )
        ON CONFLICT (url) DO NOTHING
    """

    execute_batch(cursor, INSERT_QUERY, offres_enrichies, page_size=100)
    conn.commit()

    # 4. Stats
    cursor.execute(
        "SELECT COUNT(*) FROM job_market.job_offers WHERE source = %s", (source,)
    )
    total_source = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM job_market.job_offers")
    total = cursor.fetchone()[0]

    print(f"   → Offres {source} en base : {total_source}")
    print(f"   → Total toutes sources   : {total} ✅")

    cursor.close()
    conn.close()
