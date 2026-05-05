"""
silver_to_gold_dag.py
---------------------
DAG Airflow : étape SILVER → GOLD

Rôle : après que le DAG ETL a inséré les offres dans job_market.job_offers
       (avec competences_texte brut), ce DAG :
       1. Lit competences_texte depuis PostgreSQL
       2. Applique le NLP (tokenisation + matching fuzzy)
       3. Insère / met à jour dim_competence (dictionnaire officiel)
       4. Crée les liens dans job_competences (relation N-N)
       5. Génère les embeddings et les stocke dans job_embeddings

Architecture :
    job_market.job_offers.competences_texte  (SILVER)
        ↓  NLP Python
    job_market.dim_competence                (GOLD)
    job_market.job_competences               (GOLD)
    job_market.job_embeddings                (GOLD)

Schedule : après le DAG ETL principal (0 8 * * * — 2h après)
"""

import os
import re
import logging
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(
        host     = os.environ.get("POSTGRES_HOST",     "postgres_app"),
        port     = 5432,
        dbname   = os.environ.get("POSTGRES_DB",       "job_intelligent"),
        user     = os.environ.get("POSTGRES_USER",     "jobuser"),
        password = os.environ.get("POSTGRES_PASSWORD", "motdepasse123"),
    )

# Compétences canoniques connues (seed NLP)
# Clé = token à chercher dans le texte
# Valeur = nom normalisé dans dim_competence
COMPETENCES_SEED = {
    # Langages
    "python":           "python",
    "sql":              "sql",
    " r ":              "r",
    "scala":            "scala",
    "java":             "java",
    "javascript":       "javascript",
    "typescript":       "typescript",
    "bash":             "bash",
    "shell":            "bash",

    # Frameworks data
    "spark":            "spark",
    "pyspark":          "pyspark",
    "hadoop":           "hadoop",
    "flink":            "flink",
    "beam":             "apache beam",

    # Orchestration
    "airflow":          "airflow",
    "prefect":          "prefect",
    "dagster":          "dagster",
    "luigi":            "luigi",

    # Transformation
    "dbt":              "dbt",
    "etl":              "etl",
    "elt":              "etl",

    # Messaging / streaming
    "kafka":            "kafka",
    "rabbitmq":         "rabbitmq",
    "kinesis":          "kinesis",

    # Conteneurs / DevOps
    "docker":           "docker",
    "kubernetes":       "kubernetes",
    "k8s":              "kubernetes",
    "terraform":        "terraform",
    "helm":             "helm",
    "ci/cd":            "ci/cd",
    "github actions":   "ci/cd",
    "jenkins":          "ci/cd",
    "git":              "git",

    # Bases de données
    "postgresql":       "postgresql",
    "postgres":         "postgresql",
    "mysql":            "mysql",
    "mongodb":          "mongodb",
    "cassandra":        "cassandra",
    "redis":            "redis",
    "elasticsearch":    "elasticsearch",
    "opensearch":       "elasticsearch",
    "neo4j":            "neo4j",
    "clickhouse":       "clickhouse",

    # Cloud & data warehouses
    "aws":              "aws",
    "amazon web":       "aws",
    "azure":            "azure",
    "gcp":              "gcp",
    "google cloud":     "gcp",
    "s3":               "s3",
    "redshift":         "redshift",
    "bigquery":         "bigquery",
    "snowflake":        "snowflake",
    "databricks":       "databricks",
    "synapse":          "azure synapse",
    "glue":             "aws glue",

    # ML / IA
    "machine learning": "machine learning",
    "deep learning":    "deep learning",
    "nlp":              "nlp",
    "llm":              "llm",
    "mlops":            "mlops",
    "reinforcement":    "reinforcement learning",
    "computer vision":  "computer vision",

    # Librairies ML
    "scikit":           "scikit-learn",
    "sklearn":          "scikit-learn",
    "tensorflow":       "tensorflow",
    "pytorch":          "pytorch",
    "keras":            "keras",
    "xgboost":          "xgboost",
    "lightgbm":         "lightgbm",
    "hugging face":     "hugging face",
    "transformers":     "hugging face",
    "langchain":        "langchain",
    "openai":           "openai",

    # Librairies data
    "pandas":           "pandas",
    "numpy":            "numpy",
    "polars":           "polars",
    "dask":             "dask",
    "ray":              "ray",

    # BI / Visualisation
    "power bi":         "power bi",
    "powerbi":          "power bi",
    "tableau":          "tableau",
    "looker":           "looker",
    "metabase":         "metabase",
    "grafana":          "grafana",
    "superset":         "apache superset",

    # Domaines métier
    "data engineering": "data engineering",
    "data science":     "data science",
    "data analyst":     "data analysis",
    "data analysis":    "data analysis",
    "business intelligence": "business intelligence",
    "bi ":              "business intelligence",
    "data warehouse":   "data warehousing",
    "data lake":        "data lake",
    "lakehouse":        "data lake",
    "data mesh":        "data mesh",
    "data governance":  "data governance",
    "data quality":     "data quality",
    "data catalog":     "data catalog",
    "pipeline":         "data pipeline",
    "api":              "api",
    "rest":             "api",
    "graphql":          "api",
}

# Catégorie par nom normalisé
CATEGORIES_MAP = {
    "python": "langage", "sql": "langage", "r": "langage",
    "scala": "langage", "java": "langage", "javascript": "langage",
    "typescript": "langage", "bash": "langage",
    "spark": "framework", "pyspark": "framework", "hadoop": "framework",
    "flink": "framework", "apache beam": "framework",
    "airflow": "outil", "prefect": "outil", "dagster": "outil",
    "luigi": "outil", "dbt": "outil", "etl": "domaine",
    "kafka": "outil", "rabbitmq": "outil", "kinesis": "outil",
    "docker": "outil", "kubernetes": "outil", "terraform": "outil",
    "helm": "outil", "ci/cd": "outil", "git": "outil",
    "postgresql": "base_donnees", "mysql": "base_donnees",
    "mongodb": "base_donnees", "cassandra": "base_donnees",
    "redis": "base_donnees", "elasticsearch": "base_donnees",
    "neo4j": "base_donnees", "clickhouse": "base_donnees",
    "aws": "cloud", "azure": "cloud", "gcp": "cloud",
    "s3": "cloud", "redshift": "cloud", "bigquery": "cloud",
    "snowflake": "cloud", "databricks": "cloud",
    "azure synapse": "cloud", "aws glue": "cloud",
    "machine learning": "domaine", "deep learning": "domaine",
    "nlp": "domaine", "llm": "domaine", "mlops": "domaine",
    "reinforcement learning": "domaine", "computer vision": "domaine",
    "data engineering": "domaine", "data science": "domaine",
    "data analysis": "domaine", "business intelligence": "domaine",
    "data warehousing": "domaine", "data lake": "domaine",
    "data mesh": "domaine", "data governance": "domaine",
    "data quality": "domaine", "data catalog": "domaine",
    "data pipeline": "domaine", "api": "domaine",
    "scikit-learn": "librairie", "tensorflow": "librairie",
    "pytorch": "librairie", "keras": "librairie",
    "xgboost": "librairie", "lightgbm": "librairie",
    "hugging face": "librairie", "langchain": "librairie",
    "openai": "librairie", "pandas": "librairie",
    "numpy": "librairie", "polars": "librairie",
    "dask": "librairie", "ray": "librairie",
    "power bi": "visualisation", "tableau": "visualisation",
    "looker": "visualisation", "metabase": "visualisation",
    "grafana": "visualisation", "apache superset": "visualisation",
}


# ─────────────────────────────────────────────────────────────
# NLP — extraction de compétences depuis un texte brut
# ─────────────────────────────────────────────────────────────

def extraire_competences(texte: str) -> set:
    """
    Extrait les noms normalisés de compétences depuis un texte brut.
    Retourne un set de noms normalisés (ex: {'python', 'spark', 'airflow'}).

    Méthode : matching par sous-chaîne sur le texte mis en minuscules.
    On trie les tokens par longueur décroissante pour éviter qu'un token
    court ('r') matche à l'intérieur d'un token long ('airflow').
    """
    if not texte:
        return set()

    # Normalisation basique du texte
    texte = texte.lower()
    # Remplacer ponctuation par espaces (garde les / pour ci/cd)
    texte = re.sub(r"[,;:\n\t\r\|•\-–—]", " ", texte)
    texte = re.sub(r"\s+", " ", texte)

    trouvees = set()

    # Trier par longueur décroissante → priorité aux tokens longs
    tokens_tries = sorted(COMPETENCES_SEED.keys(), key=len, reverse=True)

    for token in tokens_tries:
        if token in texte:
            nom_normalise = COMPETENCES_SEED[token]
            trouvees.add(nom_normalise)

    return trouvees


# ─────────────────────────────────────────────────────────────
# TÂCHE 1 — Insérer les nouvelles compétences dans dim_competence
# ─────────────────────────────────────────────────────────────

def upsert_dim_competence(**context):
    """
    Parcourt toutes les offres récentes (non encore traitées)
    et insère dans dim_competence les compétences nouvellement détectées.
    On vise les offres dont competences_texte est renseigné
    mais qui n'ont pas encore de liens dans job_competences.
    """
    conn   = get_db()
    cursor = conn.cursor()

    # Récupérer les offres sans liens de compétences
    cursor.execute("""
        SELECT jo.id, jo.competences_texte, jo.description
        FROM job_market.job_offers jo
        LEFT JOIN job_market.job_competences jc ON jc.job_id = jo.id
        WHERE jc.job_id IS NULL
          AND (jo.competences_texte IS NOT NULL OR jo.description IS NOT NULL)
        LIMIT 5000
    """)
    offres = cursor.fetchall()
    log.info(f"Offres à traiter pour NLP : {len(offres)}")

    # Collecter toutes les compétences détectées
    toutes_competences = set()
    for job_id, comp_texte, description in offres:
        texte_combined = f"{comp_texte or ''} {description or ''}"
        trouvees = extraire_competences(texte_combined)
        toutes_competences.update(trouvees)

    # Insérer les nouvelles compétences dans dim_competence
    if toutes_competences:
        records = [
            (nom, CATEGORIES_MAP.get(nom, "autre"))
            for nom in toutes_competences
        ]
        execute_batch(
            cursor,
            """
            INSERT INTO job_market.dim_competence (nom, categorie)
            VALUES (%s, %s)
            ON CONFLICT (nom) DO NOTHING
            """,
            records,
            page_size=200
        )
        conn.commit()
        log.info(f"dim_competence : {len(toutes_competences)} compétences traitées")

    cursor.execute("SELECT COUNT(*) FROM job_market.dim_competence")
    total = cursor.fetchone()[0]
    log.info(f"dim_competence total : {total} compétences")

    cursor.close()
    conn.close()

    # Passer le nombre d'offres à la tâche suivante via XCom
    context["ti"].xcom_push(key="nb_offres", value=len(offres))


# ─────────────────────────────────────────────────────────────
# TÂCHE 2 — Remplir job_competences (relation N-N)
# ─────────────────────────────────────────────────────────────

def remplir_job_competences(**context):
    """
    Pour chaque offre sans liens de compétences :
    1. Extrait les compétences via NLP
    2. Retrouve les IDs dans dim_competence
    3. Insère les lignes dans job_competences
    """
    conn   = get_db()
    cursor = conn.cursor()

    # Récupérer les offres à traiter (même filtre que upsert_dim_competence)
    cursor.execute("""
        SELECT jo.id, jo.competences_texte, jo.description
        FROM job_market.job_offers jo
        LEFT JOIN job_market.job_competences jc ON jc.job_id = jo.id
        WHERE jc.job_id IS NULL
          AND (jo.competences_texte IS NOT NULL OR jo.description IS NOT NULL)
        LIMIT 5000
    """)
    offres = cursor.fetchall()

    # Charger le dictionnaire dim_competence en mémoire
    cursor.execute("SELECT id, nom FROM job_market.dim_competence")
    dict_competences = {nom: cid for cid, nom in cursor.fetchall()}

    liens = []
    stats = {"offres": 0, "liens": 0, "sans_match": 0}

    for job_id, comp_texte, description in offres:
        texte_combined = f"{comp_texte or ''} {description or ''}"
        trouvees = extraire_competences(texte_combined)

        if not trouvees:
            stats["sans_match"] += 1
            continue

        stats["offres"] += 1
        for nom in trouvees:
            cid = dict_competences.get(nom)
            if cid:
                liens.append((job_id, cid))
                stats["liens"] += 1

    # Insertion en batch
    if liens:
        execute_batch(
            cursor,
            """
            INSERT INTO job_market.job_competences (job_id, competence_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            liens,
            page_size=500
        )
        conn.commit()

    log.info(
        f"job_competences — offres traitées : {stats['offres']} | "
        f"liens créés : {stats['liens']} | "
        f"offres sans match : {stats['sans_match']}"
    )

    # Vérification
    cursor.execute("SELECT COUNT(*) FROM job_market.job_competences")
    total = cursor.fetchone()[0]
    log.info(f"job_competences total lignes : {total}")

    cursor.close()
    conn.close()


# ─────────────────────────────────────────────────────────────
# TÂCHE 3 — Remplir dim_date avec les nouvelles dates
# ─────────────────────────────────────────────────────────────

def remplir_dim_date(**context):
    """
    Insère dans dim_date les dates de publication
    des offres récemment ajoutées.
    """
    conn   = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO job_market.dim_date (date, annee, mois, semaine, trimestre, jour_semaine)
        SELECT DISTINCT
            date_publication::DATE,
            EXTRACT(YEAR    FROM date_publication)::INTEGER,
            EXTRACT(MONTH   FROM date_publication)::INTEGER,
            EXTRACT(WEEK    FROM date_publication)::INTEGER,
            EXTRACT(QUARTER FROM date_publication)::INTEGER,
            TO_CHAR(date_publication, 'Day')
        FROM job_market.job_offers
        WHERE date_publication IS NOT NULL
          AND date_id IS NULL
        ON CONFLICT (date) DO NOTHING
    """)

    # Lier job_offers → dim_date
    cursor.execute("""
        UPDATE job_market.job_offers jo
        SET date_id = dd.id
        FROM job_market.dim_date dd
        WHERE jo.date_publication::DATE = dd.date
          AND jo.date_id IS NULL
    """)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM job_market.dim_date")
    log.info(f"dim_date total : {cursor.fetchone()[0]} dates")

    cursor.close()
    conn.close()


# ─────────────────────────────────────────────────────────────
# TÂCHE 4 — Générer les embeddings des nouvelles offres
# ─────────────────────────────────────────────────────────────

def generer_embeddings(**context):
    """
    Génère les embeddings pour les offres qui n'en ont pas encore
    dans job_market.job_embeddings.

    Utilise sentence-transformers (all-MiniLM-L6-v2).
    Le texte encodé = titre + localisation + competences_texte (tronqué).
    """
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        log.warning("sentence-transformers non installé — embeddings ignorés")
        return

    conn   = get_db()
    cursor = conn.cursor()

    # Récupérer les offres sans embedding
    cursor.execute("""
        SELECT jo.id, jo.titre, jo.localisation, jo.competences_texte
        FROM job_market.job_offers jo
        LEFT JOIN job_market.job_embeddings je ON je.job_id = jo.id
        WHERE je.job_id IS NULL
        LIMIT 1000
    """)
    offres = cursor.fetchall()

    if not offres:
        log.info("Aucune offre sans embedding")
        cursor.close()
        conn.close()
        return

    log.info(f"Génération d'embeddings pour {len(offres)} offres...")

    modele = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    textes = [
        f"{titre or ''} {localisation or ''} {(comp or '')[:500]}"
        for _, titre, localisation, comp in offres
    ]

    embeddings = modele.encode(textes, batch_size=64, show_progress_bar=False)

    records = [
        (offres[i][0], embeddings[i].tolist())
        for i in range(len(offres))
    ]

    execute_batch(
        cursor,
        """
        INSERT INTO job_market.job_embeddings (job_id, embedding)
        VALUES (%s, %s::vector)
        ON CONFLICT (job_id) DO UPDATE SET embedding = EXCLUDED.embedding
        """,
        records,
        page_size=100
    )
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM job_market.job_embeddings")
    log.info(f"job_embeddings total : {cursor.fetchone()[0]}")

    cursor.close()
    conn.close()


# ─────────────────────────────────────────────────────────────
# TÂCHE 5 — Résumé final
# ─────────────────────────────────────────────────────────────

def resume_final(**context):
    conn   = get_db()
    cursor = conn.cursor()

    log.info("=" * 55)
    log.info("RÉSUMÉ SILVER → GOLD")
    log.info("=" * 55)

    queries = [
        ("job_market.job_offers",      "SELECT COUNT(*) FROM job_market.job_offers"),
        ("job_market.dim_competence",  "SELECT COUNT(*) FROM job_market.dim_competence"),
        ("job_market.job_competences", "SELECT COUNT(*) FROM job_market.job_competences"),
        ("job_market.dim_date",        "SELECT COUNT(*) FROM job_market.dim_date"),
        ("job_market.job_embeddings",  "SELECT COUNT(*) FROM job_market.job_embeddings"),
        ("candidate.candidates",       "SELECT COUNT(*) FROM candidate.candidates"),
        ("candidate.recommendations",  "SELECT COUNT(*) FROM candidate.recommendations"),
    ]

    for label, query in queries:
        cursor.execute(query)
        log.info(f"  {label:<35} : {cursor.fetchone()[0]:>6}")

    # Top 10 compétences les plus demandées
    cursor.execute("""
        SELECT dc.nom, COUNT(*) AS nb_offres
        FROM job_market.job_competences jc
        JOIN job_market.dim_competence dc ON dc.id = jc.competence_id
        GROUP BY dc.nom
        ORDER BY nb_offres DESC
        LIMIT 10
    """)
    top = cursor.fetchall()
    if top:
        log.info("\n  TOP 10 compétences :")
        for nom, nb in top:
            log.info(f"    {nom:<30} : {nb:>5} offres")

    log.info("=" * 55)

    cursor.close()
    conn.close()


# ─────────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="silver_to_gold_nlp",
    description="Silver→Gold : NLP compétences → dim_competence + job_competences + embeddings",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 8 * * *",   # 2h après le DAG ETL (qui tourne à 6h)
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gold", "nlp", "competences", "embeddings"],
    doc_md="""
## DAG Silver → Gold (NLP)

### Ce que fait ce DAG
Prend les offres brutes insérées par `etl_job_offers_pipeline_v3`
et les enrichit avec le NLP pour alimenter le schéma Gold.

### Flux
```
job_market.job_offers.competences_texte
    ↓ NLP (matching dictionnaire)
job_market.dim_competence    ← dictionnaire officiel des compétences
job_market.job_competences   ← relation N-N offres ↔ compétences
job_market.dim_date          ← dimension temporelle
job_market.job_embeddings    ← vecteurs pour le matching IA
```

### Ordre des tâches
`dim_date` → `dim_competence` → `job_competences` → `embeddings` → `resume`
    """,
) as dag:

    debut = EmptyOperator(task_id="debut")

    t_dim_date = PythonOperator(
        task_id="remplir_dim_date",
        python_callable=remplir_dim_date,
    )

    t_dim_comp = PythonOperator(
        task_id="upsert_dim_competence",
        python_callable=upsert_dim_competence,
    )

    t_job_comp = PythonOperator(
        task_id="remplir_job_competences",
        python_callable=remplir_job_competences,
    )

    t_embeddings = PythonOperator(
        task_id="generer_embeddings",
        python_callable=generer_embeddings,
    )

    t_resume = PythonOperator(
        task_id="resume_final",
        python_callable=resume_final,
    )

    # dim_date et dim_competence peuvent tourner en parallèle
    # job_competences dépend de dim_competence (besoin des IDs)
    # embeddings peut tourner en parallèle de job_competences
    debut >> [t_dim_date, t_dim_comp]
    t_dim_comp >> t_job_comp
    [t_job_comp, t_dim_date, t_embeddings] >> t_resume
    debut >> t_embeddings
