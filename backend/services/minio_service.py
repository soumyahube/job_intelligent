from minio import Minio
from config import *

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def init_bucket():
    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    return client
def upload_to_minio(file):
    client = get_minio_client()

    client.put_object(
        MINIO_BUCKET,
        file.filename,
        file,
        length=file.content_length or -1,
        part_size=10 * 1024 * 1024
    )
client = get_minio_client()