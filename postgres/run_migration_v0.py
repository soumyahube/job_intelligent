"""
run_migration.py
----------------
Exécute migration.sql sur votre base PostgreSQL existante.
Lancez ce script UNE SEULE FOIS depuis votre machine locale.

Usage :
    python run_migration.py
"""

import os
import psycopg2

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST",     "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", "5544")),   # port exposé dans docker-compose
    "dbname":   os.environ.get("POSTGRES_DB",       "job_intelligent"),
    "user":     os.environ.get("POSTGRES_USER",     "jobuser"),
    "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
}

MIGRATION_FILE = "migration.sql"


def run():
    print("=" * 55)
    print("MIGRATION — job_intelligent")
    print("=" * 55)

    # Lire le fichier SQL
    with open(MIGRATION_FILE, "r", encoding="utf-8") as f:
        sql = f.read()

    print(f"\nConnexion à PostgreSQL : {DB_CONFIG['host']}:{DB_CONFIG['port']}")

    try:
        conn   = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("Connexion établie ✅")
        print("\nExécution de la migration...")

        # Exécuter tout le script d'un coup
        cursor.execute(sql)
        conn.commit()

        print("Migration terminée ✅")

        # Afficher le résultat de la vérification finale
        cursor.execute("""
            SELECT table_name, lignes FROM (
                SELECT 'job_offers'      AS table_name, COUNT(*) AS lignes FROM job_offers
                UNION ALL
                SELECT 'dim_source',       COUNT(*) FROM dim_source
                UNION ALL
                SELECT 'dim_contrat',      COUNT(*) FROM dim_contrat
                UNION ALL
                SELECT 'dim_localisation', COUNT(*) FROM dim_localisation
                UNION ALL
                SELECT 'user_profiles',    COUNT(*) FROM user_profiles
                UNION ALL
                SELECT 'recommendations',  COUNT(*) FROM recommendations
            ) t ORDER BY table_name
        """)

        rows = cursor.fetchall()
        print("\n─── État de la base après migration ───")
        for table, count in rows:
            print(f"  {table:<25} : {count:>6} lignes")
        print("───────────────────────────────────────")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        raise


if __name__ == "__main__":
    run()
