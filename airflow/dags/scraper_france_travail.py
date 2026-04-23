from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
import os

with DAG(
    dag_id="scraper_france_travail",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["scraper", "france-travail"],
) as dag:

    run_scraper = DockerOperator(
        task_id="run_scraper_france_travail",
        image="scraper-francetravail:latest",   # nom de l'image buildée
        container_name="job_scraper_airflow_run",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",                    # accès à postgres_app et minio
        environment={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB"),
            "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            "MINIO_ACCESS_KEY": os.getenv("MINIO_USER"),
            "MINIO_SECRET_KEY": os.getenv("MINIO_PASSWORD"),
            "FT_CLIENT_ID": os.getenv("FT_CLIENT_ID"),
            "FT_CLIENT_SECRET": os.getenv("FT_CLIENT_SECRET"),
        },
    )