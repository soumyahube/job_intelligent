from minio import Minio
from config import *
import io

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def init_bucket():
    client = get_minio_client()
    
    # Tester la connexion
    try:
        client.list_buckets()
        print("✅ MinIO connection successful")
    except Exception as e:
        print(f"❌ MinIO connection failed: {e}")
        raise

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"✅ Bucket '{MINIO_BUCKET}' created")
    else:
        print(f"✅ Bucket '{MINIO_BUCKET}' already exists")

    return client

def upload_to_minio(file):
    client = get_minio_client()
    file.seek(0)  # Important: retour au début du fichier
    data = file.read()
    file.seek(0)  # Reset pour une éventuelle réutilisation
    
    result = client.put_object(
        MINIO_BUCKET,
        file.filename,
        io.BytesIO(data),
        length=len(data),
    )
    print(f"✅ File uploaded to MinIO: {file.filename}, etag: {result.etag}")
    return result

def get_file_from_minio(filename):
    client = get_minio_client()
    response = client.get_object(MINIO_BUCKET, filename)
    data = response.read()
    response.close()
    response.release_conn()
    return data

def list_files():
    client = get_minio_client()
    objects = client.list_objects(MINIO_BUCKET, recursive=True)
    return [obj.object_name for obj in objects]