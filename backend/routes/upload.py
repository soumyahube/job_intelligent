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

@upload_bp.route("/analyze-text", methods=["POST"])
def analyze_text():
    data = request.get_json()
    profile_text = data.get("text", "")
    
    if not profile_text:
        return jsonify({"error": "no text provided"}), 400
    
    try:
        # 1. Extraire les compétences du texte
        skills = extract_skills_from_text(profile_text)
        
        # 2. Sauvegarder en PostgreSQL
        save_profile(profile_text, skills)
        
        # 3. Simuler/matcher avec des offres
        jobs = match_jobs(skills)
        
        return jsonify({
            "skills": skills,
            "jobs": jobs,
            "message": "Profil analysé avec succès"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500