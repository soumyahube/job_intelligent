from flask import Flask
from routes.upload import upload_bp
from services.minio_service import init_bucket
from flask_cors import CORS
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
app.register_blueprint(upload_bp)

if __name__ == "__main__":
    app.run(debug=True)


init_bucket() 
@app.route("/")
def health():
    return {"status": "ok"}