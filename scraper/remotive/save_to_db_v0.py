"""
save_to_db.py
-------------
Sauvegarde les offres Remotive dans PostgreSQL et MinIO.
"""

import os
import io
import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from minio import Minio

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     5432,
    "dbname":   os.environ.get("POSTGRES_DB", "job_intelligent"),
    "user":     os.environ.get("POSTGRES_USER", "jobuser"),
    "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
}

MINIO_CONFIG = {
    "endpoint":   os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
    "access_key": os.environ.get("MINIO_ACCESS_KEY", "admin"),
    "secret_key": os.environ.get("MINIO_SECRET_KEY", "motdepasse123"),
    "secure":     False,
}

BUCKET_RAW = "raw-scrapes"


def sauvegarder_brut_dans_minio(offres_brutes, source="remotive"):
    try:
        client = Minio(**MINIO_CONFIG)

        if not client.bucket_exists(BUCKET_RAW):
            client.make_bucket(BUCKET_RAW)

        date_str    = datetime.now().strftime("%Y-%m-%d_%H-%M")
        nom_fichier = f"{source}_{date_str}.json"
        contenu     = json.dumps(offres_brutes, ensure_ascii=False, indent=2).encode("utf-8")

        client.put_object(
            BUCKET_RAW,
            nom_fichier,
            io.BytesIO(contenu),
            len(contenu),
            content_type="application/json"
        )
        print(f"   → Archivé dans MinIO : {BUCKET_RAW}/{nom_fichier} ✅")

    except Exception as e:
        print(f"   ⚠️ MinIO indisponible, archive ignorée : {e}")


def inserer_dans_postgres(offres_propres):
    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    INSERT_QUERY = """
        INSERT INTO job_offers (
            titre, entreprise, localisation, description,
            salaire_min, salaire_max, devise,
            competences_texte, type_contrat, niveau_experience,
            url, source, date_publication
        ) VALUES (
            %(titre)s, %(entreprise)s, %(localisation)s, %(description)s,
            %(salaire_min)s, %(salaire_max)s, %(devise)s,
            %(competences_texte)s, %(type_contrat)s, %(niveau_experience)s,
            %(url)s, %(source)s, %(date_publication)s
        )
        ON CONFLICT (url) DO NOTHING
    """

    execute_batch(cursor, INSERT_QUERY, offres_propres, page_size=100)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM job_offers WHERE source = 'remotive'")
    total_remotive = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM job_offers")
    total = cursor.fetchone()[0]

    print(f"   → Offres Remotive en base : {total_remotive}")
    print(f"   → Total toutes sources : {total} ✅")

    cursor.close()
    conn.close()
