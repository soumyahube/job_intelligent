from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import os

with DAG(
    dag_id="scraper_remotive",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 7 * * *",   # Chaque jour à 7h UTC
    catchup=False,
    tags=["scraper", "remotive"],
) as dag:

    run_scraper = DockerOperator(
        task_id="run_scraper_remotive",
        image="scraper-remotive:latest",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="job-intelligent_app_network",
        force_pull=False,
        mount_tmp_dir=False,
        environment={
            "POSTGRES_HOST":     os.getenv("POSTGRES_HOST", "postgres_app"),
            "POSTGRES_USER":     os.getenv("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
            "POSTGRES_DB":       os.getenv("POSTGRES_DB"),
            "MINIO_ENDPOINT":    os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "MINIO_ACCESS_KEY":  os.getenv("MINIO_USER"),
            "MINIO_SECRET_KEY":  os.getenv("MINIO_PASSWORD"),
        },
    )
