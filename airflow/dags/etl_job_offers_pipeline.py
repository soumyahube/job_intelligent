"""
etl_pipeline_dag.py
--------------------
DAG Airflow unifié pour le pipeline ETL complet.
Couvre les 3 sources : France Travail, Remotive, Adzuna.

Architecture du pipeline par source :
    extract → archive_minio → transform → load_postgres

Les 3 pipelines tournent en parallèle, puis une tâche finale
vérifie les comptages en base.

Schedule : tous les jours à 6h UTC.
"""

import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# ─────────────────────────────────────────────
# IMPORTS des modules scraper
# Les scrapers sont montés dans /app/scraper via docker-compose
# ─────────────────────────────────────────────
import sys
sys.path.insert(0, "/app/scraper/franceTravail")
sys.path.insert(0, "/app/scraper/remotive")
sys.path.insert(0, "/app/scraper/adzuna")

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# ARGUMENTS PAR DÉFAUT
# ─────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ─────────────────────────────────────────────
# FONCTIONS ETL — FRANCE TRAVAIL
# ─────────────────────────────────────────────

def ft_extract(**context):
    """E — Collecte les offres brutes depuis l'API France Travail."""
    from france_travail import collecter_toutes_les_offres
    offres = collecter_toutes_les_offres()
    if not offres:
        raise ValueError("France Travail : aucune offre collectée")
    log.info(f"France Travail : {len(offres)} offres extraites")
    # Passer les données via XCom
    context["ti"].xcom_push(key="offres_brutes", value=offres)


def ft_archive(**context):
    """Archive les données brutes dans MinIO (data lake)."""
    from save_to_db import sauvegarder_brut_dans_minio as save_minio
    offres = context["ti"].xcom_pull(task_ids="france_travail.ft_extract", key="offres_brutes")
    save_minio(offres)
    log.info(f"France Travail : {len(offres)} offres archivées dans MinIO")


def ft_transform(**context):
    """T — Prétraite et normalise les offres France Travail."""
    from preprocess import pretraiter_toutes
    offres_brutes = context["ti"].xcom_pull(task_ids="france_travail.ft_extract", key="offres_brutes")
    offres_propres = pretraiter_toutes(offres_brutes)
    if not offres_propres:
        raise ValueError("France Travail : aucune offre après prétraitement")
    log.info(f"France Travail : {len(offres_propres)} offres transformées")
    context["ti"].xcom_push(key="offres_propres", value=offres_propres)


def ft_load(**context):
    """L — Insère les offres France Travail dans PostgreSQL."""
    from save_to_db import inserer_dans_postgres
    offres = context["ti"].xcom_pull(task_ids="france_travail.ft_transform", key="offres_propres")
    inserer_dans_postgres(offres)
    log.info(f"France Travail : {len(offres)} offres chargées dans PostgreSQL")


# ─────────────────────────────────────────────
# FONCTIONS ETL — REMOTIVE
# ─────────────────────────────────────────────

def remotive_extract(**context):
    """E — Collecte les offres depuis l'API publique Remotive."""
    # Importer le module remotive (attention : conflit de nom avec le package)
    import importlib.util, os
    spec = importlib.util.spec_from_file_location(
        "remotive_scraper", "/app/scraper/remotive/remotive.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    offres = mod.collecter_toutes_les_offres()
    if not offres:
        raise ValueError("Remotive : aucune offre collectée")
    log.info(f"Remotive : {len(offres)} offres extraites")
    context["ti"].xcom_push(key="offres_brutes", value=offres)


def remotive_archive(**context):
    """Archive les données brutes Remotive dans MinIO."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "remotive_save", "/app/scraper/remotive/save_to_db.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    offres = context["ti"].xcom_pull(task_ids="remotive.remotive_extract", key="offres_brutes")
    mod.sauvegarder_brut_dans_minio(offres, source="remotive")
    log.info(f"Remotive : {len(offres)} offres archivées dans MinIO")


def remotive_transform(**context):
    """T — Prétraite et normalise les offres Remotive."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "remotive_preprocess", "/app/scraper/remotive/preprocess.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    offres_brutes  = context["ti"].xcom_pull(task_ids="remotive.remotive_extract", key="offres_brutes")
    offres_propres = mod.pretraiter_offres(offres_brutes)
    if not offres_propres:
        raise ValueError("Remotive : aucune offre après prétraitement")
    log.info(f"Remotive : {len(offres_propres)} offres transformées")
    context["ti"].xcom_push(key="offres_propres", value=offres_propres)


def remotive_load(**context):
    """L — Insère les offres Remotive dans PostgreSQL."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "remotive_save", "/app/scraper/remotive/save_to_db.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    offres = context["ti"].xcom_pull(task_ids="remotive.remotive_transform", key="offres_propres")
    mod.inserer_dans_postgres(offres)
    log.info(f"Remotive : {len(offres)} offres chargées dans PostgreSQL")


# ─────────────────────────────────────────────
# FONCTIONS ETL — ADZUNA
# ─────────────────────────────────────────────

def adzuna_extract(**context):
    """E — Collecte les offres depuis l'API Adzuna."""
    from adzuna import collecter_toutes_les_offres
    offres = collecter_toutes_les_offres()
    if not offres:
        raise ValueError("Adzuna : aucune offre collectée")
    log.info(f"Adzuna : {len(offres)} offres extraites")
    context["ti"].xcom_push(key="offres_brutes", value=offres)


def adzuna_archive(**context):
    """Archive les données brutes Adzuna dans MinIO."""
    from save_to_db import sauvegarder_brut_dans_minio as adzuna_save_minio
    offres = context["ti"].xcom_pull(task_ids="adzuna.adzuna_extract", key="offres_brutes")
    adzuna_save_minio(offres, source="adzuna")
    log.info(f"Adzuna : {len(offres)} offres archivées dans MinIO")


def adzuna_transform(**context):
    """T — Prétraite et normalise les offres Adzuna."""
    from preprocess import pretraiter_offres as adzuna_pretraiter
    offres_brutes  = context["ti"].xcom_pull(task_ids="adzuna.adzuna_extract", key="offres_brutes")
    offres_propres = adzuna_pretraiter(offres_brutes)
    if not offres_propres:
        raise ValueError("Adzuna : aucune offre après prétraitement")
    log.info(f"Adzuna : {len(offres_propres)} offres transformées")
    context["ti"].xcom_push(key="offres_propres", value=offres_propres)


def adzuna_load(**context):
    """L — Insère les offres Adzuna dans PostgreSQL."""
    from save_to_db import inserer_dans_postgres as adzuna_insert
    offres = context["ti"].xcom_pull(task_ids="adzuna.adzuna_transform", key="offres_propres")
    adzuna_insert(offres)
    log.info(f"Adzuna : {len(offres)} offres chargées dans PostgreSQL")


# ─────────────────────────────────────────────
# TÂCHE FINALE — VÉRIFICATION
# ─────────────────────────────────────────────

def verifier_totaux(**context):
    """
    Vérifie les totaux en base après le pipeline.
    Logue un résumé par source et le grand total.
    """
    import psycopg2

    DB_CONFIG = {
        "host":     os.environ.get("POSTGRES_HOST", "postgres_app"),
        "port":     5432,
        "dbname":   os.environ.get("POSTGRES_DB", "job_intelligent"),
        "user":     os.environ.get("POSTGRES_USER", "jobuser"),
        "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
    }

    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sources = ["france_travail", "remotive", "adzuna"]
    log.info("=" * 50)
    log.info("RÉSUMÉ DU PIPELINE ETL")
    log.info("=" * 50)

    for source in sources:
        cursor.execute("SELECT COUNT(*) FROM job_offers WHERE source = %s", (source,))
        count = cursor.fetchone()[0]
        log.info(f"  {source:<20} : {count:>6} offres en base")

    cursor.execute("SELECT COUNT(*) FROM job_offers")
    total = cursor.fetchone()[0]
    log.info(f"  {'TOTAL':<20} : {total:>6} offres")
    log.info("=" * 50)

    cursor.close()
    conn.close()


# ─────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────

with DAG(
    dag_id="etl_job_offers_pipeline",
    description="Pipeline ETL unifié : France Travail + Remotive + Adzuna → PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # Chaque jour à 6h UTC
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "scraper", "france-travail", "remotive", "adzuna"],
    doc_md="""
## Pipeline ETL — Offres d'emploi Data

Ce DAG orchestre la collecte quotidienne des offres d'emploi depuis 3 sources :

| Source        | Type    | Fréquence |
|---------------|---------|-----------|
| France Travail | API OAuth2 | Quotidienne |
| Remotive       | API publique | Quotidienne |
| Adzuna         | API REST | Quotidienne |

### Étapes par source
1. **Extract** — Appel API et collecte des données brutes
2. **Archive** — Sauvegarde du JSON brut dans MinIO (data lake)
3. **Transform** — Nettoyage et normalisation vers le schéma `job_offers`
4. **Load** — Insertion dans PostgreSQL (ON CONFLICT DO NOTHING)

### Parallélisme
Les 3 pipelines s'exécutent en parallèle.
La tâche `verifier_totaux` s'exécute en dernier pour valider les comptages.
    """,
) as dag:

    # ── Nœud de départ ──
    debut = EmptyOperator(task_id="debut_pipeline")

    # ── Nœud de fin ──
    verification = PythonOperator(
        task_id="verifier_totaux",
        python_callable=verifier_totaux,
    )

    # ═══════════════════════════════════════
    # TASK GROUP — FRANCE TRAVAIL
    # ═══════════════════════════════════════
    with TaskGroup("france_travail", tooltip="Pipeline ETL France Travail") as tg_ft:

        extract_ft = PythonOperator(
            task_id="ft_extract",
            python_callable=ft_extract,
            doc_md="**Extract** : Appel API France Travail (OAuth2) pour tous les mots-clés data.",
        )
        archive_ft = PythonOperator(
            task_id="ft_archive",
            python_callable=ft_archive,
            doc_md="**Archive** : Sauvegarde du JSON brut dans MinIO bucket `raw-scrapes`.",
        )
        transform_ft = PythonOperator(
            task_id="ft_transform",
            python_callable=ft_transform,
            doc_md="**Transform** : Nettoyage, normalisation salaires, compétences, types de contrat.",
        )
        load_ft = PythonOperator(
            task_id="ft_load",
            python_callable=ft_load,
            doc_md="**Load** : Insertion dans `job_offers` (ON CONFLICT DO NOTHING sur URL).",
        )

        extract_ft >> archive_ft >> transform_ft >> load_ft

    # ═══════════════════════════════════════
    # TASK GROUP — REMOTIVE
    # ═══════════════════════════════════════
    with TaskGroup("remotive", tooltip="Pipeline ETL Remotive") as tg_remotive:

        extract_remotive = PythonOperator(
            task_id="remotive_extract",
            python_callable=remotive_extract,
            doc_md="**Extract** : API publique Remotive (catégories data, software-dev, devops).",
        )
        archive_remotive = PythonOperator(
            task_id="remotive_archive",
            python_callable=remotive_archive,
            doc_md="**Archive** : Sauvegarde du JSON brut dans MinIO.",
        )
        transform_remotive = PythonOperator(
            task_id="remotive_transform",
            python_callable=remotive_transform,
            doc_md="**Transform** : Nettoyage HTML, extraction salaires, normalisation.",
        )
        load_remotive = PythonOperator(
            task_id="remotive_load",
            python_callable=remotive_load,
            doc_md="**Load** : Insertion dans PostgreSQL.",
        )

        extract_remotive >> archive_remotive >> transform_remotive >> load_remotive

    # ═══════════════════════════════════════
    # TASK GROUP — ADZUNA
    # ═══════════════════════════════════════
    with TaskGroup("adzuna", tooltip="Pipeline ETL Adzuna") as tg_adzuna:

        extract_adzuna = PythonOperator(
            task_id="adzuna_extract",
            python_callable=adzuna_extract,
            doc_md="**Extract** : API Adzuna (FR, GB, US) pour les profils data.",
        )
        archive_adzuna = PythonOperator(
            task_id="adzuna_archive",
            python_callable=adzuna_archive,
            doc_md="**Archive** : Sauvegarde du JSON brut dans MinIO.",
        )
        transform_adzuna = PythonOperator(
            task_id="adzuna_transform",
            python_callable=adzuna_transform,
            doc_md="**Transform** : Normalisation salaires, localisation, compétences.",
        )
        load_adzuna = PythonOperator(
            task_id="adzuna_load",
            python_callable=adzuna_load,
            doc_md="**Load** : Insertion dans PostgreSQL.",
        )

        extract_adzuna >> archive_adzuna >> transform_adzuna >> load_adzuna

    # ── Dépendances globales ──
    # debut → [3 pipelines en parallèle] → verification
    debut >> [tg_ft, tg_remotive, tg_adzuna] >> verification
