from flask import Blueprint, request, jsonify
from services.minio_service import upload_to_minio
import requests
import os

upload_bp = Blueprint("upload_bp", __name__)

# ─────────────────────────────────────────────
# CONFIG AIRFLOW
# ─────────────────────────────────────────────
AIRFLOW_API_URL  = "http://airflow-webserver:8080/api/v1"
AIRFLOW_USER     = os.environ.get("AIRFLOW_ADMIN_USER",     "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin")
DAG_ID           = "cv_extraction"


def trigger_dag_cv(filename=None, type_entree="cv", texte_direct=None, email=None, nom=None):
    """Déclenche le DAG dag_cv_extraction via l'API REST Airflow."""
    url  = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns"
    conf = {
        "filename":     filename,
        "type_entree":  type_entree,
        "texte_direct": texte_direct,
        "email":        email,
        "nom":          nom,
    }
    try:
        response = requests.post(
            url,
            json={"conf": conf},
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=10,
        )
        if response.status_code in (200, 201):
            dag_run_id = response.json().get("dag_run_id")
            print(f"[Airflow] DAG déclenché : dag_run_id={dag_run_id}")
            return True
        else:
            print(f"[Airflow] Erreur {response.status_code} : {response.text}")
            return False
    except Exception as e:
        print(f"[Airflow] Exception lors du trigger : {e}")
        return False


# ─────────────────────────────────────────────
# ROUTE /upload
# ─────────────────────────────────────────────

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

    # Déclencher le DAG Airflow après l'upload
    ok = trigger_dag_cv(
        filename=file.filename,
        type_entree="cv",
        email=request.form.get("email"),
        nom=request.form.get("nom"),
    )

    if ok:
        return jsonify({"message": "CV uploadé et analyse lancée", "filename": file.filename})
    else:
        # On retourne quand même 200 : le fichier est bien dans MinIO
        return jsonify({"message": "CV uploadé mais DAG non déclenché", "filename": file.filename})


# ─────────────────────────────────────────────
# ROUTE /analyze-text
# ─────────────────────────────────────────────

@upload_bp.route("/analyze-text", methods=["POST"])
def analyze_text():
    data         = request.get_json()
    profile_text = data.get("text", "").strip()

    if not profile_text:
        return jsonify({"error": "no text provided"}), 400

    # Déclencher le DAG Airflow avec le texte direct
    ok = trigger_dag_cv(
        type_entree="text",
        texte_direct=profile_text,
        email=data.get("email"),
        nom=data.get("nom"),
    )

    if ok:
        return jsonify({"message": "Analyse lancée", "status": "processing"})
    else:
        return jsonify({"error": "Impossible de déclencher l'analyse"}), 500