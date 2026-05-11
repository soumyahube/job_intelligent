"""
cv_extraction.py
-----------------
DAG Airflow déclenché à chaque upload de CV ou texte depuis le frontend.

Modèle NER : dslim/bert-base-NER (public, léger, fiable)
    → PER  : extraction du nom du candidat
    → MISC : extraction des compétences techniques
    → complété par liste TECH_SKILLS pour enrichir la détection

Corrections v4 :
    - Nettoyage du texte espacé type "E L - A S S R I O M A Y M A"
    - Extraction PDF avec gestion des layouts en colonnes
    - Nom extrait via NER (entité PER) au lieu de l'heuristique
    - Seuil de score augmenté à 0.92 pour réduire les faux positifs
    - BLACKLIST enrichie pour filtrer les faux positifs
    - Filtre longueur compétences (2-30 chars, max 3 mots)

schedule_interval=None : déclenché uniquement par le backend Flask.
"""

import os
import io
import re
import logging
import psycopg2
from datetime import datetime, timedelta

from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

BUCKET_CVS = "cvs"
NER_MODEL  = "dslim/bert-base-NER"

TECH_SKILLS = [
    "python", "sql", "java", "javascript", "typescript", "r", "scala", "c++", "c#",
    "airflow", "spark", "kafka", "hadoop", "dbt", "luigi", "mlflow",
    "postgresql", "mysql", "mongodb", "redis", "elasticsearch", "cassandra",
    "docker", "kubernetes", "terraform", "ansible", "jenkins", "gitlab", "github",
    "aws", "azure", "gcp", "minio", "s3", "ec2", "lambda",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch", "keras", "xgboost",
    "fastapi", "flask", "django", "react", "vue", "angular", "node",
    "git", "linux", "bash", "powershell",
    "machine learning", "deep learning", "nlp", "computer vision", "llm",
    "data engineering", "data science", "bi", "power bi", "tableau", "looker",
    "excel", "metabase", "superset", "matlab", "oracle",
    "api", "rest", "graphql", "microservices", "etl", "pipeline",
    "agile", "scrum", "jira", "confluence",
    "visual studio code", "visual studio", "pycharm", "jupyter",
    "spring boot", "laravel", "express", "tailwind", "bootstrap",
    "php", "html", "css", "pl/sql", "sqlite",
    "matplotlib", "seaborn", "plotly",
    "intellij", "eclipse", "codeblocks",
    "random forest", "decision tree", "neural network",
    "web scraping", "beautifulsoup", "selenium",
]

BLACKLIST = {
    "intel", "ens", "intermidiaire", "intermdiaire", "intermi",
    "ecole", "nationale", "sciences", "appliquees",
    "université", "institute", "master", "bachelor", "licence",
    "stage", "projet", "travail", "experience", "formation",
    "microsoft word", "windows", "ke", "ma",
    "intermdiaire comptition", "el hoceima", "studio code",
    "bien", "debutant", "maternele", "langue", "arabe", "francais",
    "anglais", "espagnol", "hackathon", "robotique", "centres",
    "interet", "certificat", "udemy", "freecodecamp",
    "soft", "skills", "gestion", "communication", "equipe",
    "rapport", "marketing", "controle",
}

TITLE_WORDS = {
    "cycle", "ingénieur", "master", "bachelor", "licence", "baccalauréat",
    "sciences", "physiques", "informatique", "données", "génie", "école",
    "nationale", "ensa", "université", "institut", "département",
}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_minio_client():
    return Minio(
        endpoint=os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.environ.get("MINIO_USER", "admin"),
        secret_key=os.environ.get("MINIO_PASSWORD", "motdepasse123"),
        secure=False,
    )


def get_db_config():
    return {
        "host":     os.environ.get("POSTGRES_HOST",     "postgres_app"),
        "port":     5432,
        "dbname":   os.environ.get("POSTGRES_DB",       "job_intelligent"),
        "user":     os.environ.get("POSTGRES_USER",     "jobuser"),
        "password": os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
    }


def get_conf(context: dict) -> dict:
    return context["dag_run"].conf or {}


def extract_email_from_text(text: str):
    match = re.search(r"[\w.\-+]+@[\w\-]+\.[a-zA-Z]{2,}", text)
    return match.group(0) if match else None


def clean_spaced_letters(text: str) -> str:
    """
    Nettoie le texte où les lettres sont séparées par des espaces.
    Ex: "E L - A S S R I O M A Y M A" → "EL-ASSRI OMAYMA"
    Conserve les espaces entre mots normaux.
    """
    lines = []
    for line in text.splitlines():
        # Détecter si la ligne est du texte espacé lettre par lettre
        # Pattern: majorité des "mots" font 1 caractère
        words = line.strip().split()
        if words and len(words) > 2:
            single_chars = sum(1 for w in words if len(w) == 1 or (len(w) == 2 and w[1] == '-'))
            ratio = single_chars / len(words)
            if ratio > 0.6:  # plus de 60% des tokens sont des lettres isolées
                # Reconstituer le texte sans espaces entre lettres
                cleaned = re.sub(
                    r'(?<=[A-ZÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝ])\s(?=[A-ZÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝ])',
                    '',
                    line.strip()
                )
                lines.append(cleaned)
                continue
        lines.append(line)
    return "\n".join(lines)


def _is_columnar_layout(text: str) -> bool:
    """
    Heuristique pour détecter un CV en 2 colonnes.
    Si beaucoup de lignes courtes → layout colonnes.
    """
    lines = [l for l in text.splitlines() if l.strip()]
    if len(lines) < 5:
        return False
    short_lines = sum(1 for l in lines if len(l.strip()) < 40)
    return short_lines / len(lines) > 0.5


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """
    Extrait le texte d'un PDF en gérant les layouts en colonnes.
    """
    import pdfplumber

    pages = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""

            if text and _is_columnar_layout(text):
                log.info("Layout en colonnes détecté — extraction par zones")
                width  = page.width
                height = page.height
                left   = page.crop((0, 0, width / 2, height)).extract_text(
                    x_tolerance=3, y_tolerance=3
                ) or ""
                right  = page.crop((width / 2, 0, width, height)).extract_text(
                    x_tolerance=3, y_tolerance=3
                ) or ""
                text = left + "\n" + right

            pages.append(text)

    return "\n".join(pages).strip()


def extract_name_with_ner(text: str, ner_pipeline) -> str:
    """
    Nettoie d'abord le texte espacé (ex: "E L - A S S R I"),
    puis utilise le NER pour détecter le nom (entité PER score > 0.90).
    Fallback sur heuristique améliorée.
    """
    # Nettoyer les espaces entre lettres avant NER
    text_clean = clean_spaced_letters(text[:800])

    try:
        entities = ner_pipeline(text_clean)
        for ent in entities:
            if ent["entity_group"] == "PER" and ent["score"] > 0.90:
                name  = re.sub(r"\s+", " ", ent["word"]).strip()
                words = name.lower().split()
                if len(name.split()) >= 2 and not any(w in TITLE_WORDS for w in words):
                    log.info(f"Nom NER : {name} (score={ent['score']:.2f})")
                    return name
    except Exception as e:
        log.warning(f"NER nom erreur : {e}")

    # Fallback heuristique sur texte nettoyé
    for line in text_clean.splitlines():
        line  = re.sub(r"\s+", " ", line).strip()
        words = line.split()
        if (
            2 <= len(words) <= 4
            and all(w[0].isupper() for w in words if w and w[0].isalpha())
            and not any(w.lower() in TITLE_WORDS for w in words)
            and not any(c.isdigit() for c in line)
        ):
            log.info(f"Nom heuristique : {line}")
            return line

    return None


def extract_skills_with_ner(text: str, ner_pipeline) -> list:
    """
    Extraction des compétences en 2 étapes :
    1. NER → entités MISC/ORG avec score > 0.92
    2. Liste TECH_SKILLS → correspondance directe
    Filtre BLACKLIST + longueur + nombre de mots.
    """
    competences = set()

    max_chars = 1500
    chunks    = [text[i:i + max_chars] for i in range(0, len(text), max_chars)]

    for chunk in chunks:
        try:
            entities = ner_pipeline(chunk)
            for ent in entities:
                if ent["entity_group"] in ("MISC", "ORG") and ent["score"] > 0.92:
                    skill = ent["word"].strip().lower()
                    skill = re.sub(r"[^a-zA-Z0-9\s\+\#\.\-]", "", skill).strip()
                    words = skill.split()
                    if (
                        2 <= len(skill) <= 30
                        and 1 <= len(words) <= 3
                        and skill not in BLACKLIST
                    ):
                        competences.add(skill)
        except Exception as e:
            log.warning(f"NER chunk error: {e}")

    text_lower = text.lower()
    for skill in TECH_SKILLS:
        if skill.lower() in text_lower and skill.lower() not in BLACKLIST:
            competences.add(skill.lower())

    log.info(f"NER+liste — {len(competences)} compétences : {competences}")
    return list(competences)


# ═══════════════════════════════════════════════════════════
# TASK 1 — Lire depuis MinIO et extraire le texte
# ═══════════════════════════════════════════════════════════

def read_from_minio(**context):
    """
    Lit le PDF depuis MinIO ou récupère le texte direct.
    Initialise le NER une seule fois pour nom + compétences.
    """
    from transformers import pipeline, AutoTokenizer, AutoModelForTokenClassification

    conf         = get_conf(context)
    type_entree  = conf.get("type_entree", "cv")
    filename     = conf.get("filename")
    texte_direct = conf.get("texte_direct")
    email        = conf.get("email")
    nom          = conf.get("nom")

    if type_entree == "text":
        if not texte_direct:
            raise ValueError("type_entree='text' mais texte_direct est vide")
        contenu_texte = texte_direct.strip()
        log.info(f"Texte direct : {len(contenu_texte)} caractères")

    else:
        if not filename:
            raise ValueError("type_entree='cv' mais filename est vide")

        client    = get_minio_client()
        response  = client.get_object(BUCKET_CVS, filename)
        pdf_bytes = response.read()
        log.info(f"PDF lu : {BUCKET_CVS}/{filename} ({len(pdf_bytes)} bytes)")

        # Extraction avec gestion colonnes
        contenu_texte = extract_text_from_pdf(pdf_bytes)

        if not contenu_texte:
            raise ValueError(f"Aucun texte extrait du PDF : {filename}")

        log.info(f"Texte extrait : {len(contenu_texte)} caractères")

    if not email:
        email = extract_email_from_text(contenu_texte)
        log.info(f"Email : {email}")

    # Initialiser NER une seule fois
    tokenizer    = AutoTokenizer.from_pretrained(NER_MODEL)
    model        = AutoModelForTokenClassification.from_pretrained(NER_MODEL)
    ner_pipeline = pipeline(
        "ner",
        model=model,
        tokenizer=tokenizer,
        aggregation_strategy="simple",
    )

    if not nom:
        nom = extract_name_with_ner(contenu_texte, ner_pipeline)
        log.info(f"Nom : {nom}")

    competences = extract_skills_with_ner(contenu_texte, ner_pipeline)

    context["ti"].xcom_push(key="contenu_texte", value=contenu_texte)
    context["ti"].xcom_push(key="type_entree",   value=type_entree)
    context["ti"].xcom_push(key="filename",       value=filename)
    context["ti"].xcom_push(key="email",          value=email)
    context["ti"].xcom_push(key="nom",            value=nom)
    context["ti"].xcom_push(key="competences",    value=competences)


# ═══════════════════════════════════════════════════════════
# TASK 2 — INSERT candidate.candidates
# ═══════════════════════════════════════════════════════════

def insert_candidate(**context):
    ti            = context["ti"]
    contenu_texte = ti.xcom_pull(task_ids="read_from_minio", key="contenu_texte")
    type_entree   = ti.xcom_pull(task_ids="read_from_minio", key="type_entree")
    filename      = ti.xcom_pull(task_ids="read_from_minio", key="filename")
    email         = ti.xcom_pull(task_ids="read_from_minio", key="email")
    nom           = ti.xcom_pull(task_ids="read_from_minio", key="nom")

    conn   = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO candidate.candidates
            (type_entree, contenu_texte, nom_fichier, email, nom)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (type_entree, contenu_texte, filename, email, nom))

    candidate_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()

    log.info(f"Candidat inséré : id={candidate_id}, nom={nom}, email={email}")
    ti.xcom_push(key="candidate_id", value=candidate_id)


# ═══════════════════════════════════════════════════════════
# TASK 3 — Générer embedding et INSERT cv_embeddings
# ═══════════════════════════════════════════════════════════

def generate_embedding(**context):
    from sentence_transformers import SentenceTransformer

    ti            = context["ti"]
    candidate_id  = ti.xcom_pull(task_ids="insert_candidate", key="candidate_id")
    contenu_texte = ti.xcom_pull(task_ids="read_from_minio",  key="contenu_texte")

    model     = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    embedding = model.encode(contenu_texte, normalize_embeddings=True).tolist()

    log.info(f"Embedding {len(embedding)} dims pour candidate_id={candidate_id}")

    conn   = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO candidate.cv_embeddings (candidate_id, embedding, modele)
        VALUES (%s, %s, %s)
        ON CONFLICT (candidate_id) DO UPDATE SET
            embedding  = EXCLUDED.embedding,
            created_at = NOW()
    """, (candidate_id, embedding, "sentence-transformers/all-MiniLM-L6-v2"))

    conn.commit()
    cursor.close()
    conn.close()

    log.info(f"cv_embeddings inséré pour candidate_id={candidate_id}")


# ═══════════════════════════════════════════════════════════
# TASK 4 — INSERT candidate_skills depuis XCom
# ═══════════════════════════════════════════════════════════

def extract_and_link_skills(**context):
    ti            = context["ti"]
    candidate_id  = ti.xcom_pull(task_ids="insert_candidate", key="candidate_id")
    competences   = ti.xcom_pull(task_ids="read_from_minio",  key="competences")

    if not competences:
        log.warning(f"Aucune compétence pour candidate_id={candidate_id}")
        return

    conn   = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()

    nb = 0
    for comp in competences:
        cursor.execute("""
            INSERT INTO job_market.dim_competence (nom)
            VALUES (%s) ON CONFLICT (nom) DO NOTHING
        """, (comp,))

        cursor.execute(
            "SELECT id FROM job_market.dim_competence WHERE nom = %s", (comp,)
        )
        row = cursor.fetchone()
        if not row:
            continue

        cursor.execute("""
            INSERT INTO candidate.candidate_skills (candidate_id, competence_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (candidate_id, row[0]))
        nb += 1

    conn.commit()
    cursor.close()
    conn.close()

    log.info(f"candidate_skills : {nb} compétences liées pour candidate_id={candidate_id}")


# ═══════════════════════════════════════════════════════════
# TASK 5 — Vérification finale
# ═══════════════════════════════════════════════════════════

def verifier_insertion(**context):
    ti           = context["ti"]
    candidate_id = ti.xcom_pull(task_ids="insert_candidate", key="candidate_id")

    conn   = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()

    log.info("=" * 50)
    log.info(f"RÉSUMÉ — candidate_id={candidate_id}")

    cursor.execute(
        "SELECT type_entree, nom, email, nom_fichier, created_at FROM candidate.candidates WHERE id = %s",
        (candidate_id,)
    )
    row = cursor.fetchone()
    log.info(f"  candidates    : type={row[0]}, nom={row[1]}, email={row[2]}, fichier={row[3]}")

    cursor.execute(
        "SELECT COUNT(*) FROM candidate.cv_embeddings WHERE candidate_id = %s", (candidate_id,)
    )
    log.info(f"  cv_embeddings : {cursor.fetchone()[0]} vecteur(s)")

    cursor.execute("""
        SELECT dc.nom FROM candidate.candidate_skills cs
        JOIN job_market.dim_competence dc ON dc.id = cs.competence_id
        WHERE cs.candidate_id = %s
        ORDER BY dc.nom
    """, (candidate_id,))
    skills = [r[0] for r in cursor.fetchall()]
    log.info(f"  skills        : {len(skills)} → {skills}")
    log.info("=" * 50)

    cursor.close()
    conn.close()


# ─────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="cv_extraction",
    description="Extraction CV → candidates, cv_embeddings, candidate_skills via NER v4",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cv", "candidate", "nlp", "ner", "embedding"],
) as dag:

    t_start      = EmptyOperator(task_id="debut")
    t1_read      = PythonOperator(task_id="read_from_minio",          python_callable=read_from_minio)
    t2_insert    = PythonOperator(task_id="insert_candidate",         python_callable=insert_candidate)
    t3_embedding = PythonOperator(task_id="generate_embedding",       python_callable=generate_embedding)
    t4_skills    = PythonOperator(task_id="extract_and_link_skills",  python_callable=extract_and_link_skills)
    t5_verify    = PythonOperator(task_id="verifier_insertion",       python_callable=verifier_insertion)
    t_end        = EmptyOperator(task_id="fin")

    t_start >> t1_read >> t2_insert >> [t3_embedding, t4_skills] >> t5_verify >> t_end