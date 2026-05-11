from flask import Blueprint, jsonify, request
import psycopg2
import os

recommend_bp = Blueprint("recommend_bp", __name__)

# ─────────────────────────────────────────────
# CONNEXION POSTGRES
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres_app"),
        dbname=os.getenv("POSTGRES_DB", "job_intelligent"),
        user=os.getenv("POSTGRES_USER", "jobuser"),
        password=os.getenv("POSTGRES_PASSWORD", "motdepasse123"),
        port=5432,
    )


# ─────────────────────────────────────────────
# GET /recommend/<candidate_id>
# Retourne les top-N offres les plus proches
# du vecteur CV via cosine similarity (pgvector)
# ─────────────────────────────────────────────
@recommend_bp.route("/recommend/<int:candidate_id>", methods=["GET"])
def recommend(candidate_id):
    top_n = request.args.get("top", 10, type=int)

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # 1. Vérifier que l'embedding du candidat existe
        cur.execute(
            "SELECT embedding FROM candidate.cv_embeddings WHERE candidate_id = %s",
            (candidate_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({
                "error": "Embedding non trouvé. Le DAG Airflow est peut-être encore en cours, réessayer après 2-3 min."
            }), 404

        cv_embedding = row[0]  # type vector(384)

        # 2. Cosine similarity entre le CV et toutes les offres
        #    pgvector : <=> = distance cosinus (1 - similarity)
        #    On trie par distance croissante = similarité décroissante
        cur.execute(
            """
            SELECT
                jo.id,
                jo.titre,
                jo.entreprise,
                jo.localisation,
                jo.type_contrat,
                jo.salaire_min,
                jo.salaire_max,
                jo.devise,
                jo.url,
                jo.date_publication,
                ROUND(CAST((1 - (je.embedding <=> %s::vector)) AS numeric), 4) AS score
            FROM job_market.job_embeddings je
            JOIN job_market.job_offers     jo ON jo.id = je.job_id
            ORDER BY je.embedding <=> %s::vector
            LIMIT %s
            """,
            (cv_embedding, cv_embedding, top_n),
        )
        rows = cur.fetchall()
        cols = ["id", "titre", "entreprise", "localisation", "type_contrat",
                "salaire_min", "salaire_max", "devise", "url", "date_publication", "score"]
        results = [dict(zip(cols, r)) for r in rows]

        # Convertir date en string pour JSON
        for r in results:
            if r["date_publication"]:
                r["date_publication"] = r["date_publication"].strftime("%d/%m/%Y")

        # 3. Sauvegarder les recommandations dans la table candidate.recommendations
        #    (INSERT OR UPDATE via ON CONFLICT)
        for r in results:
            cur.execute(
                """
                INSERT INTO candidate.recommendations (candidate_id, job_id, score_similarite)
                VALUES (%s, %s, %s)
                ON CONFLICT (candidate_id, job_id)
                DO UPDATE SET score_similarite = EXCLUDED.score_similarite
                """,
                (candidate_id, r["id"], float(r["score"])),
            )
        conn.commit()

        return jsonify({
            "candidate_id": candidate_id,
            "total": len(results),
            "recommendations": results,
        })

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────────
# GET /candidate/latest
# Retourne l'id du dernier candidat inséré
# (utilisé par le frontend après upload)
# ─────────────────────────────────────────────
@recommend_bp.route("/candidate/latest", methods=["GET"])
def latest_candidate():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM candidate.candidates ORDER BY created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Aucun candidat trouvé"}), 404
        return jsonify({"candidate_id": row[0]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()