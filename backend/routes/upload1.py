from flask import Blueprint, request, jsonify
from services.minio_service import upload_to_minio

upload_bp = Blueprint("upload_bp", __name__)

@upload_bp.route("/upload", methods=["POST"])
def upload():
    print("UPLOAD HIT")

    file = request.files.get("file")

    if not file:
        print("NO FILE RECEIVED")
        return jsonify({"error": "no file"}), 400

    try:
        upload_to_minio(file)
        print("UPLOAD SUCCESS TO MINIO")
    except Exception as e:
        print("MINIO ERROR:", str(e))
        return jsonify({"error": str(e)}), 500

    return jsonify({"message": "file uploaded successfully"})