from flask import Flask
from routes.upload import upload_bp
from routes.recommend import recommend_bp          # ← LIGNE 1 : importer
from services.minio_service import init_bucket
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Register routes
app.register_blueprint(upload_bp)
app.register_blueprint(recommend_bp)              # ← LIGNE 2 : enregistrer

# Health check
@app.route("/")
def health():
    return {"status": "ok"}

# Init MinIO bucket
init_bucket()

# Run inside Docker
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)