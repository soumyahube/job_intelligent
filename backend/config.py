import os

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "motdepasse123")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET",    "cvs")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres_app")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "jobuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "motdepasse123")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "job_intelligent")