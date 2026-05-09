"""
etl_pipeline_dag.py
--------------------
DAG Airflow unifié pour le pipeline ETL complet.
Couvre les 3 sources : France Travail, Remotive, Adzuna.

Architecture correcte — SANS XCom pour les données :
    extract   → collecte + écrit JSON brut dans MinIO
    transform → lit depuis MinIO, normalise, réécrit dans MinIO
    load      → lit depuis MinIO, insère dans PostgreSQL

Pourquoi pas XCom pour les données ?
    XCom est stocké dans la DB Airflow (SQLite/Postgres) et est
    limité en taille. Des centaines d'offres avec descriptions
    dépassent cette limite et font crasher les tâches silencieusement
    avec executor_state=success mais state=failed/up_for_retry.
    MinIO est le bon endroit pour stocker les données inter-tâches.

Schedule : tous les jours à 6h UTC.
"""

import os
import io
import json
import logging
import importlib.util
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta

from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CHEMINS ABSOLUS — évite tout conflit d'import
# ─────────────────────────────────────────────
PATHS = {
    "ft": {
        "scraper":    "/app/scraper/franceTravail/france_travail.py",
        "preprocess": "/app/scraper/franceTravail/preprocess.py",
    },
    "remotive": {
        "scraper":    "/app/scraper/remotive/remotive.py",
        "preprocess": "/app/scraper/remotive/preprocess.py",
    },
    "adzuna": {
        "scraper":    "/app/scraper/adzuna/adzuna.py",
        "preprocess": "/app/scraper/adzuna/preprocess.py",
    },
}

BUCKET_RAW       = "raw-scrapes"
BUCKET_PROCESSED = "processed-scrapes"

SOURCE_URLS = {
    "france_travail": "https://api.francetravail.io",
    "remotive":       "https://remotive.com/api/remote-jobs",
    "adzuna":         "https://api.adzuna.com",
    "linkedin":       "https://www.linkedin.com/jobs",
}
 
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
 
INSERT_QUERY = """
    INSERT INTO job_market.job_offers (
        titre, entreprise, localisation, description,
        salaire_min, salaire_max, devise,
        competences_texte, type_contrat, niveau_experience,
        url, source, source_id, contrat_id, localisation_id,
        date_publication,
        is_remote
    ) VALUES (
        %(titre)s, %(entreprise)s, %(localisation)s, %(description)s,
        %(salaire_min)s, %(salaire_max)s, %(devise)s,
        %(competences_texte)s, %(type_contrat)s, %(niveau_experience)s,
        %(url)s, %(source)s, %(source_id)s, %(contrat_id)s, %(localisation_id)s,
        %(date_publication)s,
        %(is_remote)s
    )
    ON CONFLICT (url) DO NOTHING
"""

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_minio_client():
    return Minio(
        endpoint   = os.environ.get("MINIO_ENDPOINT",   "minio:9000"),
        access_key = os.environ.get("MINIO_ACCESS_KEY", "admin"),
        secret_key = os.environ.get("MINIO_SECRET_KEY", "motdepasse123"),
        secure     = False,
    )


def get_db_config():
    return {
        "host":     os.environ.get("POSTGRES_HOST",     "postgres_app"),
        "port":     5432,
        "dbname":   os.environ.get("POSTGRES_DB",       "job_intelligent"),
        "user":     os.environ.get("POSTGRES_USER",     "jobuser"),
        "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
    }


def load_module(name: str, path: str):
    """Charge un module Python depuis son chemin absolu. Évite les conflits de noms."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def minio_write(client, bucket: str, key: str, data: list):
    """Sérialise data en JSON et l'écrit dans MinIO."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    contenu = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    client.put_object(bucket, key, io.BytesIO(contenu), len(contenu),
                      content_type="application/json")
    log.info(f"MinIO écrit : {bucket}/{key} ({len(contenu) // 1024} Ko)")


def minio_read(client, bucket: str, key: str) -> list:
    """Lit et désérialise un JSON depuis MinIO."""
    response = client.get_object(bucket, key)
    data = json.loads(response.read().decode("utf-8"))
    log.info(f"MinIO lu : {bucket}/{key} ({len(data)} enregistrements)")
    return data


def make_key(source: str, run_id: str, stage: str) -> str:
    """
    Génère une clé MinIO unique par source + run_id + étape.
    Ex : france_travail/scheduled__2026-04-29T06-00-00/raw.json
    """
    safe_run = run_id.replace(":", "-").replace("+", "").replace(" ", "_")
    return f"{source}/{safe_run}/{stage}.json"


def pg_load(offres: list, source: str):
    conn   = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()

    # 1. source
    source_id = _upsert_source(cursor, source)
    conn.commit()

    offres_enrichies = []

    for offre in offres:

        contrat_id      = _upsert_contrat(cursor, offre.get("type_contrat"))
        localisation_id = _upsert_localisation(cursor, offre.get("localisation"))

        loc = (offre.get("localisation") or "").lower()

        # ✅ LOGIQUE CORRECTE REMOTE
        is_remote = (
            "remote" in loc
            or "télétravail" in loc
            or source == "remotive"
            or loc in ["worldwide", "anywhere", "global"]
        )

        offres_enrichies.append({
            **offre,
            "source_id": source_id,
            "contrat_id": contrat_id,
            "localisation_id": localisation_id,
            "is_remote": is_remote
        })

    conn.commit()

    execute_batch(cursor, INSERT_QUERY, offres_enrichies, page_size=100)
    conn.commit()

    cursor.close()
    conn.close()
 

 
def _upsert_source(cursor, source: str) -> int:
    url_base = SOURCE_URLS.get(source, "")
    cursor.execute("""
        INSERT INTO job_market.dim_source (nom, url_base)
        VALUES (%s, %s)
        ON CONFLICT (nom) DO NOTHING
    """, (source, url_base))
    cursor.execute("SELECT id FROM job_market.dim_source WHERE nom = %s", (source,))
    return cursor.fetchone()[0]
 
 
def _upsert_contrat(cursor, type_contrat: str):
    if not type_contrat:
        return None
    categorie = CONTRAT_CATEGORIES.get(type_contrat, "autre")
    cursor.execute("""
        INSERT INTO job_market.dim_contrat (type_contrat, categorie)
        VALUES (%s, %s)
        ON CONFLICT (type_contrat) DO NOTHING
    """, (type_contrat, categorie))
    cursor.execute(
        "SELECT id FROM job_market.dim_contrat WHERE type_contrat = %s", (type_contrat,)
    )
    row = cursor.fetchone()
    return row[0] if row else None
 
 
def _upsert_localisation(cursor, localisation: str):
    if not localisation:
        return None

    cursor.execute("""
        INSERT INTO job_market.dim_localisation (ville, pays)
        VALUES (%s, %s)
        ON CONFLICT (ville, pays) DO NOTHING
    """, (localisation, "France"))

    cursor.execute(
        "SELECT id FROM job_market.dim_localisation WHERE ville = %s",
        (localisation,)
    )
    row = cursor.fetchone()
    return row[0] if row else None

# ═══════════════════════════════════════════════════════════
# FRANCE TRAVAIL — Extract / Transform / Load
# ═══════════════════════════════════════════════════════════

def ft_extract(**context):
    """E — Collecte depuis France Travail et stocke le JSON brut dans MinIO."""
    run_id = context["run_id"]
    mod    = load_module("ft_scraper", PATHS["ft"]["scraper"])

    offres = mod.collecter_toutes_les_offres()
    if not offres:
        raise ValueError("France Travail : aucune offre collectée")
    log.info(f"France Travail : {len(offres)} offres extraites")

    key = make_key("france_travail", run_id, "raw")
    minio_write(get_minio_client(), BUCKET_RAW, key, offres)

    # XCom uniquement pour la clé MinIO (quelques octets — OK)
    context["ti"].xcom_push(key="raw_key", value=key)


def ft_transform(**context):
    """T — Lit le brut depuis MinIO, normalise, réécrit le résultat dans MinIO."""
    run_id  = context["run_id"]
    raw_key = context["ti"].xcom_pull(task_ids="france_travail.ft_extract", key="raw_key")
    client  = get_minio_client()

    offres_brutes  = minio_read(client, BUCKET_RAW, raw_key)
    mod            = load_module("ft_preprocess", PATHS["ft"]["preprocess"])
    offres_propres = mod.pretraiter_toutes(offres_brutes)

    if not offres_propres:
        raise ValueError("France Travail : aucune offre après prétraitement")
    log.info(f"France Travail : {len(offres_propres)} offres transformées")

    key = make_key("france_travail", run_id, "processed")
    minio_write(client, BUCKET_PROCESSED, key, offres_propres)
    context["ti"].xcom_push(key="processed_key", value=key)


def ft_load(**context):
    """L — Lit le JSON transformé depuis MinIO et insère dans PostgreSQL."""
    processed_key = context["ti"].xcom_pull(task_ids="france_travail.ft_transform",
                                             key="processed_key")
    offres = minio_read(get_minio_client(), BUCKET_PROCESSED, processed_key)
    pg_load(offres, "france_travail")


# ═══════════════════════════════════════════════════════════
# REMOTIVE — Extract / Transform / Load
# ═══════════════════════════════════════════════════════════

def remotive_extract(**context):
    """E — Collecte depuis Remotive et stocke le JSON brut dans MinIO."""
    run_id = context["run_id"]
    mod    = load_module("remotive_scraper", PATHS["remotive"]["scraper"])

    offres = mod.collecter_toutes_les_offres()
    if not offres:
        raise ValueError("Remotive : aucune offre collectée")
    log.info(f"Remotive : {len(offres)} offres extraites")

    key = make_key("remotive", run_id, "raw")
    minio_write(get_minio_client(), BUCKET_RAW, key, offres)
    context["ti"].xcom_push(key="raw_key", value=key)


def remotive_transform(**context):
    """T — Lit le brut depuis MinIO, normalise, réécrit dans MinIO."""
    run_id  = context["run_id"]
    raw_key = context["ti"].xcom_pull(task_ids="remotive.remotive_extract", key="raw_key")
    client  = get_minio_client()

    offres_brutes  = minio_read(client, BUCKET_RAW, raw_key)
    mod            = load_module("remotive_preprocess", PATHS["remotive"]["preprocess"])
    offres_propres = mod.pretraiter_offres(offres_brutes)

    if not offres_propres:
        raise ValueError("Remotive : aucune offre après prétraitement")
    log.info(f"Remotive : {len(offres_propres)} offres transformées")

    key = make_key("remotive", run_id, "processed")
    minio_write(client, BUCKET_PROCESSED, key, offres_propres)
    context["ti"].xcom_push(key="processed_key", value=key)


def remotive_load(**context):
    """L — Lit le JSON transformé depuis MinIO et insère dans PostgreSQL."""
    processed_key = context["ti"].xcom_pull(task_ids="remotive.remotive_transform",
                                             key="processed_key")
    offres = minio_read(get_minio_client(), BUCKET_PROCESSED, processed_key)
    pg_load(offres, "remotive")


# ═══════════════════════════════════════════════════════════
# ADZUNA — Extract / Transform / Load
# ═══════════════════════════════════════════════════════════

def adzuna_extract(**context):
    """E — Collecte depuis Adzuna et stocke le JSON brut dans MinIO."""
    run_id = context["run_id"]
    mod    = load_module("adzuna_scraper", PATHS["adzuna"]["scraper"])

    offres = mod.collecter_toutes_les_offres()
    if not offres:
        raise ValueError("Adzuna : aucune offre collectée")
    log.info(f"Adzuna : {len(offres)} offres extraites")

    key = make_key("adzuna", run_id, "raw")
    minio_write(get_minio_client(), BUCKET_RAW, key, offres)
    context["ti"].xcom_push(key="raw_key", value=key)


def adzuna_transform(**context):
    """T — Lit le brut depuis MinIO, normalise, réécrit dans MinIO."""
    run_id  = context["run_id"]
    raw_key = context["ti"].xcom_pull(task_ids="adzuna.adzuna_extract", key="raw_key")
    client  = get_minio_client()

    offres_brutes  = minio_read(client, BUCKET_RAW, raw_key)
    mod            = load_module("adzuna_preprocess", PATHS["adzuna"]["preprocess"])
    offres_propres = mod.pretraiter_offres(offres_brutes)

    if not offres_propres:
        raise ValueError("Adzuna : aucune offre après prétraitement")
    log.info(f"Adzuna : {len(offres_propres)} offres transformées")

    key = make_key("adzuna", run_id, "processed")
    minio_write(client, BUCKET_PROCESSED, key, offres_propres)
    context["ti"].xcom_push(key="processed_key", value=key)


def adzuna_load(**context):
    """L — Lit le JSON transformé depuis MinIO et insère dans PostgreSQL."""
    processed_key = context["ti"].xcom_pull(task_ids="adzuna.adzuna_transform",
                                             key="processed_key")
    offres = minio_read(get_minio_client(), BUCKET_PROCESSED, processed_key)
    pg_load(offres, "adzuna")


# ═══════════════════════════════════════════════════════════
# VÉRIFICATION FINALE
# ═══════════════════════════════════════════════════════════

def verifier_totaux(**context):
    """Résumé des comptages après le pipeline ETL complet."""
    conn   = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()
 
    log.info("=" * 50)
    log.info("RÉSUMÉ DU PIPELINE ETL")
    log.info("=" * 50)
 
    for source in ["france_travail", "remotive", "adzuna"]:
        cursor.execute(
            "SELECT COUNT(*) FROM job_market.job_offers WHERE source = %s", (source,)
        )
        log.info(f"  {source:<20} : {cursor.fetchone()[0]:>6} offres")
 
    cursor.execute("SELECT COUNT(*) FROM job_market.job_offers")
    log.info(f"  {'TOTAL':<20} : {cursor.fetchone()[0]:>6} offres")
 
    # Vérification rapide des dimensions remplies
    cursor.execute("SELECT COUNT(*) FROM job_market.dim_source")
    log.info(f"  dim_source       : {cursor.fetchone()[0]} entrées")
    cursor.execute("SELECT COUNT(*) FROM job_market.dim_contrat")
    log.info(f"  dim_contrat      : {cursor.fetchone()[0]} entrées")
    cursor.execute("SELECT COUNT(*) FROM job_market.dim_localisation")
    log.info(f"  dim_localisation : {cursor.fetchone()[0]} entrées")
 
    log.info("=" * 50)
 
    cursor.close()
    conn.close()

# ─────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="etl_job_offers_pipeline_v3",
    description="Pipeline ETL unifié : France Travail + Remotive + Adzuna → PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "scraper", "france-travail", "remotive", "adzuna"],
    doc_md="""
## Pipeline ETL — Offres d'emploi Data

Les données transitent via **MinIO** entre les étapes (pas de XCom pour les volumes).
XCom est utilisé uniquement pour les clés MinIO (quelques octets).

### Flux par source
`extract` → MinIO raw → `transform` → MinIO processed → `load` → PostgreSQL

### Parallélisme
Les 3 pipelines s'exécutent en parallèle, puis `verifier_totaux` valide les comptages.
    """,
) as dag:

    debut        = EmptyOperator(task_id="debut_pipeline")
    verification = PythonOperator(task_id="verifier_totaux", python_callable=verifier_totaux)

    # ── FRANCE TRAVAIL ──
    with TaskGroup("france_travail", tooltip="Pipeline ETL France Travail") as tg_ft:
        t_ft_e = PythonOperator(task_id="ft_extract",   python_callable=ft_extract)
        t_ft_t = PythonOperator(task_id="ft_transform", python_callable=ft_transform)
        t_ft_l = PythonOperator(task_id="ft_load",      python_callable=ft_load)
        t_ft_e >> t_ft_t >> t_ft_l

    # ── REMOTIVE ──
    with TaskGroup("remotive", tooltip="Pipeline ETL Remotive") as tg_remotive:
        t_rem_e = PythonOperator(task_id="remotive_extract",   python_callable=remotive_extract)
        t_rem_t = PythonOperator(task_id="remotive_transform", python_callable=remotive_transform)
        t_rem_l = PythonOperator(task_id="remotive_load",      python_callable=remotive_load)
        t_rem_e >> t_rem_t >> t_rem_l

    # ── ADZUNA ──
    with TaskGroup("adzuna", tooltip="Pipeline ETL Adzuna") as tg_adzuna:
        t_adz_e = PythonOperator(task_id="adzuna_extract",   python_callable=adzuna_extract)
        t_adz_t = PythonOperator(task_id="adzuna_transform", python_callable=adzuna_transform)
        t_adz_l = PythonOperator(task_id="adzuna_load",      python_callable=adzuna_load)
        t_adz_e >> t_adz_t >> t_adz_l

    trigger_silver_gold = TriggerDagRunOperator(
      task_id="trigger_silver_to_gold",
      trigger_dag_id="silver_to_gold_nlp",
      wait_for_completion=False,
    )
    debut >> [tg_ft, tg_remotive, tg_adzuna] >> verification >> trigger_silver_gold
