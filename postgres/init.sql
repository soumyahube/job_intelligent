-- =============================================================
-- init.sql — Architecture 2 schémas séparés
-- job_market  : tout ce qui concerne les offres d'emploi (BI + Data)
-- candidate   : tout ce qui concerne les candidats (IA + matching)
--
-- Exécuté UNE SEULE FOIS au premier démarrage de PostgreSQL
-- (quand le volume postgres_app_data est vide)
-- =============================================================

-- ─────────────────────────────────────────────
-- EXTENSIONS
-- ─────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- recherche fuzzy sur compétences


-- =============================================================
-- SCHÉMA 1 : JOB_MARKET
-- Tout ce qui vient des APIs / scrapers
-- Alimenté par le pipeline ETL Airflow (Bronze → Silver → Gold)
-- =============================================================
CREATE SCHEMA IF NOT EXISTS job_market;


-- ─────────────────────────────────────────────
-- DIMENSIONS (schéma en étoile)
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS job_market.dim_source (
    id         SERIAL PRIMARY KEY,
    nom        VARCHAR(50) UNIQUE NOT NULL,   -- 'france_travail', 'remotive', etc.
    url_base   TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_market.dim_contrat (
    id           SERIAL PRIMARY KEY,
    type_contrat VARCHAR(100) UNIQUE NOT NULL, -- 'Full-time', 'CDI', etc.
    categorie    VARCHAR(50)                   -- 'permanent', 'temporaire', etc.
);

CREATE TABLE IF NOT EXISTS job_market.dim_localisation (
    id        SERIAL PRIMARY KEY,
    ville     VARCHAR(255),
    pays      VARCHAR(100) DEFAULT 'France',
    region    VARCHAR(100),
    is_remote BOOLEAN DEFAULT FALSE,
    UNIQUE (ville, pays)
);

CREATE TABLE IF NOT EXISTS job_market.dim_date (
    id       SERIAL PRIMARY KEY,
    date     DATE UNIQUE NOT NULL,
    annee    INTEGER,
    mois     INTEGER,
    semaine  INTEGER,
    trimestre INTEGER,
    jour_semaine VARCHAR(20)
);

-- Dictionnaire officiel des compétences
-- Rempli automatiquement par le DAG Silver→Gold (NLP)
-- NE JAMAIS insérer manuellement (sauf données de référence initiales)
CREATE TABLE IF NOT EXISTS job_market.dim_competence (
    id         SERIAL PRIMARY KEY,
    nom        VARCHAR(100) UNIQUE NOT NULL,   -- 'python', 'sql', 'airflow', etc.
    categorie  VARCHAR(50),                    -- 'langage', 'outil', 'framework', 'cloud'
    created_at TIMESTAMP DEFAULT NOW()
);


-- ─────────────────────────────────────────────
-- TABLE CENTRALE : job_offers
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_market.job_offers (
    id               SERIAL PRIMARY KEY,

    -- Informations principales
    titre            VARCHAR(255),
    entreprise       VARCHAR(255),
    localisation     VARCHAR(500),  -- texte brut original (conservé pour debug)
    description      TEXT,

    -- Salaire
    salaire_min      FLOAT,
    salaire_max      FLOAT,
    devise           VARCHAR(10) DEFAULT 'USD',

    -- Compétences texte brut (input du NLP — NE PAS SUPPRIMER)
    -- Utilisé par le DAG Silver→Gold pour extraire les tokens
    competences_texte TEXT,

    -- Type de poste
    type_contrat        VARCHAR(100),
    niveau_experience   VARCHAR(100),
    type_teletravail    VARCHAR(100),

    -- Clés étrangères vers les dimensions
    source_id         INTEGER REFERENCES job_market.dim_source(id),
    contrat_id        INTEGER REFERENCES job_market.dim_contrat(id),
    localisation_id   INTEGER REFERENCES job_market.dim_localisation(id),
    date_id           INTEGER REFERENCES job_market.dim_date(id),

    -- Identification & traçabilité
    url              TEXT UNIQUE,
    source           VARCHAR(50) DEFAULT 'linkedin',
    date_publication TIMESTAMP,

    created_at       TIMESTAMP DEFAULT NOW()
);


-- ─────────────────────────────────────────────
-- TABLE DE LIAISON N-N : job_offers ↔ dim_competence
-- Remplie par le DAG Silver→Gold après NLP
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_market.job_competences (
    job_id        INTEGER NOT NULL REFERENCES job_market.job_offers(id) ON DELETE CASCADE,
    competence_id INTEGER NOT NULL REFERENCES job_market.dim_competence(id),
    PRIMARY KEY (job_id, competence_id)
);


-- ─────────────────────────────────────────────
-- TABLE EMBEDDINGS OFFRES (séparée de job_offers)
-- Séparation volontaire : embedding lourd, pas utile pour BI
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_market.job_embeddings (
    job_id    INTEGER PRIMARY KEY REFERENCES job_market.job_offers(id) ON DELETE CASCADE,
    embedding vector(384) NOT NULL,
    modele    VARCHAR(100) DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at TIMESTAMP DEFAULT NOW()
);


-- =============================================================
-- SCHÉMA 2 : CANDIDATE
-- Tout ce qui concerne les candidats et le matching IA
-- Alimenté par le backend Flask (upload CV, saisie texte)
-- =============================================================
CREATE SCHEMA IF NOT EXISTS candidate;


-- ─────────────────────────────────────────────
-- TABLE PRINCIPALE : candidates
-- Un enregistrement par soumission (CV ou texte)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candidate.candidates (
    id            SERIAL PRIMARY KEY,

    -- 'cv' si PDF uploadé, 'text' si texte saisi directement
    type_entree   VARCHAR(10) NOT NULL CHECK (type_entree IN ('cv', 'text')),

    -- Texte brut unifié (extrait du PDF ou saisi)
    contenu_texte TEXT NOT NULL,

    -- Nom du fichier PDF stocké dans MinIO (NULL si type = 'text')
    nom_fichier   VARCHAR(255),

    -- Métadonnées optionnelles
    email         VARCHAR(255),
    nom           VARCHAR(255),

    created_at    TIMESTAMP DEFAULT NOW()
);


-- ─────────────────────────────────────────────
-- TABLE EMBEDDINGS CANDIDATS
-- Même espace vectoriel que job_market.job_embeddings
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candidate.cv_embeddings (
    candidate_id INTEGER PRIMARY KEY REFERENCES candidate.candidates(id) ON DELETE CASCADE,
    embedding    vector(384) NOT NULL,
    modele       VARCHAR(100) DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at   TIMESTAMP DEFAULT NOW()
);


-- ─────────────────────────────────────────────
-- TABLE OPTIONNELLE : compétences extraites du CV
-- Remplie par le backend après NLP sur le CV
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candidate.candidate_skills (
    candidate_id  INTEGER NOT NULL REFERENCES candidate.candidates(id) ON DELETE CASCADE,
    competence_id INTEGER NOT NULL REFERENCES job_market.dim_competence(id),
    -- On référence le dictionnaire officiel du schéma job_market
    PRIMARY KEY (candidate_id, competence_id)
);


-- ─────────────────────────────────────────────
-- TABLE RECOMMANDATIONS
-- Résultats du matching candidat ↔ offres
-- Pont entre les 2 schémas
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candidate.recommendations (
    id               SERIAL PRIMARY KEY,
    candidate_id     INTEGER NOT NULL REFERENCES candidate.candidates(id)   ON DELETE CASCADE,
    job_id           INTEGER NOT NULL REFERENCES job_market.job_offers(id)  ON DELETE CASCADE,
    score_similarite FLOAT   NOT NULL,          -- cosine similarity [0, 1]
    created_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (candidate_id, job_id)
);


-- =============================================================
-- DONNÉES DE RÉFÉRENCE — AUCUNE
-- =============================================================
-- Toutes les dimensions sont remplies automatiquement par Airflow :
--
--   dim_source       → remplie par save_to_db.py à chaque scraping
--   dim_contrat      → remplie par save_to_db.py à chaque scraping
--   dim_localisation → remplie par le DAG silver_to_gold (NLP)
--   dim_date         → remplie par le DAG silver_to_gold (NLP)
--   dim_competence   → remplie par le DAG silver_to_gold (NLP)
--
-- init.sql ne contient que la STRUCTURE, jamais les données.
-- =============================================================


-- =============================================================
-- INDEX
-- =============================================================

-- job_market
CREATE INDEX IF NOT EXISTS idx_jo_source       ON job_market.job_offers(source);
CREATE INDEX IF NOT EXISTS idx_jo_date         ON job_market.job_offers(date_publication);
CREATE INDEX IF NOT EXISTS idx_jo_contrat      ON job_market.job_offers(type_contrat);
CREATE INDEX IF NOT EXISTS idx_jo_localisation ON job_market.job_offers(localisation);
CREATE INDEX IF NOT EXISTS idx_jo_source_id    ON job_market.job_offers(source_id);
CREATE INDEX IF NOT EXISTS idx_jo_date_id      ON job_market.job_offers(date_id);
CREATE INDEX IF NOT EXISTS idx_jc_job          ON job_market.job_competences(job_id);
CREATE INDEX IF NOT EXISTS idx_jc_comp         ON job_market.job_competences(competence_id);
CREATE INDEX IF NOT EXISTS idx_dc_nom          ON job_market.dim_competence(nom);

-- candidate
CREATE INDEX IF NOT EXISTS idx_rec_candidate   ON candidate.recommendations(candidate_id);
CREATE INDEX IF NOT EXISTS idx_rec_job         ON candidate.recommendations(job_id);
CREATE INDEX IF NOT EXISTS idx_csk_candidate   ON candidate.candidate_skills(candidate_id);

-- NOTE : Index ivfflat pgvector créé par embedder.py après insertion
-- (nécessite un minimum de lignes)
-- CREATE INDEX idx_job_emb ON job_market.job_embeddings
--   USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
-- CREATE INDEX idx_cv_emb ON candidate.cv_embeddings
--   USING ivfflat (embedding vector_cosine_ops) WITH (lists = 50);
