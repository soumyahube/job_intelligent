from flask import request, jsonify
from services.minio_service import upload_to_minio
from flask import Blueprint, request, jsonify
upload_bp = Blueprint("upload_bp", __name__)

@upload_bp.route("/upload", methods=["POST"])
def upload():
    file = request.files["file"]
    upload_to_minio(file)
    return jsonify({"message": "file uploaded successfully"})

