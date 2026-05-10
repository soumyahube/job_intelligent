"""
run_migration.py
----------------
Exécute migration.sql pour passer à l'architecture 2 schémas :
    job_market  (offres, dimensions, compétences, embeddings)
    candidate   (candidats, CV, recommandations)

Lancez ce script UNE SEULE FOIS depuis votre machine locale.

Usage :
    python run_migration.py

Variables d'environnement (optionnel) :
    POSTGRES_HOST     (défaut: localhost)
    POSTGRES_PORT     (défaut: 5544)
    POSTGRES_DB       (défaut: job_intelligent)
    POSTGRES_USER     (défaut: jobuser)
    POSTGRES_PASSWORD (défaut: motdepasse123)
"""

import os
import psycopg2

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST",     "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", "5544")),
    "dbname":   os.environ.get("POSTGRES_DB",       "job_intelligent"),
    "user":     os.environ.get("POSTGRES_USER",     "jobuser"),
    "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
}

MIGRATION_FILE = "migration.sql"


def run():
    print("=" * 60)
    print("MIGRATION — Architecture 2 schémas (job_market + candidate)")
    print("=" * 60)

    with open(MIGRATION_FILE, "r", encoding="utf-8") as f:
        sql = f.read()

    print(f"\nConnexion : {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['dbname']}")

    try:
        conn   = psycopg2.connect(**DB_CONFIG)
        # autocommit=False pour transaction unique
        conn.autocommit = False
        cursor = conn.cursor()

        print("Connexion établie ✅")
        print("\nExécution de la migration (peut prendre quelques minutes)...")

        cursor.execute(sql)
        conn.commit()

        print("\nMigration terminée ✅")

        # Vérification
        cursor.execute("""
            SELECT table_name, lignes FROM (
                SELECT 'job_market.job_offers'      AS table_name, COUNT(*) AS lignes FROM job_market.job_offers
                UNION ALL
                SELECT 'job_market.dim_source',       COUNT(*) FROM job_market.dim_source
                UNION ALL
                SELECT 'job_market.dim_contrat',      COUNT(*) FROM job_market.dim_contrat
                UNION ALL
                SELECT 'job_market.dim_localisation', COUNT(*) FROM job_market.dim_localisation
                UNION ALL
                SELECT 'job_market.dim_date',         COUNT(*) FROM job_market.dim_date
                UNION ALL
                SELECT 'job_market.dim_competence',   COUNT(*) FROM job_market.dim_competence
                UNION ALL
                SELECT 'job_market.job_competences',  COUNT(*) FROM job_market.job_competences
                UNION ALL
                SELECT 'candidate.candidates',        COUNT(*) FROM candidate.candidates
                UNION ALL
                SELECT 'candidate.cv_embeddings',     COUNT(*) FROM candidate.cv_embeddings
                UNION ALL
                SELECT 'candidate.candidate_skills',  COUNT(*) FROM candidate.candidate_skills
                UNION ALL
                SELECT 'candidate.recommendations',   COUNT(*) FROM candidate.recommendations
            ) t ORDER BY table_name
        """)

        rows = cursor.fetchall()
        print("\n─── État de la base après migration ───")
        for table, count in rows:
            print(f"  {table:<40} : {count:>6} lignes")
        print("───────────────────────────────────────")

        print("""
⚠️  Les anciennes tables (public.job_offers, public.user_profiles,
    public.recommendations, etc.) sont CONSERVÉES pour sécurité.

    Une fois que vous avez validé les données dans les nouveaux schémas,
    vous pouvez les supprimer avec :

        DROP TABLE IF EXISTS public.recommendations CASCADE;
        DROP TABLE IF EXISTS public.user_profiles    CASCADE;
        DROP TABLE IF EXISTS public.job_offers       CASCADE;
        DROP TABLE IF EXISTS public.dim_localisation CASCADE;
        DROP TABLE IF EXISTS public.dim_contrat      CASCADE;
        DROP TABLE IF EXISTS public.dim_source       CASCADE;
        """)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise


if __name__ == "__main__":
    run()
