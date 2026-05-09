-- =============================================================
-- init.sql
-- Exécuté UNE SEULE FOIS au premier démarrage de PostgreSQL
-- (quand le volume postgres_app_data est vide)
-- =============================================================

-- ─────────────────────────────────────────────
-- EXTENSIONS
-- ─────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;


-- =============================================================
-- TABLES DIMENSIONS (schéma en étoile)
-- =============================================================

CREATE TABLE IF NOT EXISTS dim_source (
    id         SERIAL PRIMARY KEY,
    nom        VARCHAR(50) UNIQUE NOT NULL,
    url_base   TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_contrat (
    id           SERIAL PRIMARY KEY,
    type_contrat VARCHAR(100) UNIQUE NOT NULL,
    categorie    VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_localisation (
    id        SERIAL PRIMARY KEY,
    ville     VARCHAR(255),
    pays      VARCHAR(100) DEFAULT 'France',
    region    VARCHAR(100),
    is_remote BOOLEAN DEFAULT FALSE,
    UNIQUE (ville, pays)
);


-- =============================================================
-- TABLE CENTRALE : job_offers
-- =============================================================

CREATE TABLE IF NOT EXISTS job_offers (
    id               SERIAL PRIMARY KEY,

    -- Informations principales
    titre            VARCHAR(255),
    entreprise       VARCHAR(255),
    localisation     VARCHAR(500),
    description      TEXT,

    -- Salaire
    salaire_min      FLOAT,
    salaire_max      FLOAT,
    devise           VARCHAR(10) DEFAULT 'USD',

    -- Compétences (texte brut des APIs)
    competences_texte TEXT,

    -- Type de poste
    type_contrat        VARCHAR(100),
    niveau_experience   VARCHAR(100),
    type_teletravail    VARCHAR(100),

    -- Clés étrangères vers les dimensions
    -- (NULL accepté pour les données déjà insérées avant migration)
    source_id         INTEGER REFERENCES dim_source(id),
    contrat_id        INTEGER REFERENCES dim_contrat(id),
    localisation_id   INTEGER REFERENCES dim_localisation(id),

    -- Identification
    url              TEXT UNIQUE,
    source           VARCHAR(50) DEFAULT 'linkedin',
    date_publication TIMESTAMP,

    -- Embedding vectoriel (rempli par embedder.py après insertion)
    embedding        vector(384),

    created_at       TIMESTAMP DEFAULT NOW()
);


-- =============================================================
-- TABLE PROFILS UTILISATEURS
-- Gère les 2 cas : CV uploadé ET texte saisi
-- =============================================================

CREATE TABLE IF NOT EXISTS user_profiles (
    id            SERIAL PRIMARY KEY,

    -- 'cv' si PDF uploadé, 'text' si texte saisi directement
    type_entree   VARCHAR(10) NOT NULL CHECK (type_entree IN ('cv', 'text')),

    -- Texte unifié (extrait du PDF ou saisi par l'utilisateur)
    contenu_texte TEXT NOT NULL,

    -- Nom du fichier PDF (NULL si type_entree = 'text')
    nom_fichier   VARCHAR(255),

    -- Embedding calculé par le modèle (même espace que job_offers)
    embedding     vector(384),

    created_at    TIMESTAMP DEFAULT NOW()
);


-- =============================================================
-- TABLE RECOMMANDATIONS
-- Résultats du matching profil <-> offres
-- =============================================================

CREATE TABLE IF NOT EXISTS recommendations (
    id               SERIAL PRIMARY KEY,
    profile_id       INTEGER NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
    offre_id         INTEGER NOT NULL REFERENCES job_offers(id)    ON DELETE CASCADE,
    score_similarite FLOAT   NOT NULL,
    created_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (profile_id, offre_id)
);


-- =============================================================
-- DONNÉES DE RÉFÉRENCE
-- =============================================================

INSERT INTO dim_source (nom, url_base) VALUES
    ('france_travail', 'https://api.francetravail.io'),
    ('remotive',       'https://remotive.com/api/remote-jobs'),
    ('adzuna',         'https://api.adzuna.com'),
    ('linkedin',       'https://www.linkedin.com/jobs')
ON CONFLICT (nom) DO NOTHING;

INSERT INTO dim_contrat (type_contrat, categorie) VALUES
    ('Full-time',  'permanent'),
    ('Part-time',  'permanent'),
    ('Contract',   'temporaire'),
    ('Freelance',  'freelance'),
    ('Internship', 'stage'),
    ('CDI',        'permanent'),
    ('CDD',        'temporaire')
ON CONFLICT (type_contrat) DO NOTHING;


-- =============================================================
-- INDEX
-- =============================================================

-- Index classiques pour les requêtes Power BI et filtres API
CREATE INDEX IF NOT EXISTS idx_job_offers_source      ON job_offers(source);
CREATE INDEX IF NOT EXISTS idx_job_offers_date        ON job_offers(date_publication);
CREATE INDEX IF NOT EXISTS idx_job_offers_contrat     ON job_offers(type_contrat);
CREATE INDEX IF NOT EXISTS idx_job_offers_localisation ON job_offers(localisation);
CREATE INDEX IF NOT EXISTS idx_recommendations_profile ON recommendations(profile_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_offre   ON recommendations(offre_id);

-- NOTE : L'index pgvector ivfflat sur embedding est créé dans
-- embedder.py APRÈS insertion des données car il nécessite
-- un minimum de lignes pour être efficace.
-- CREATE INDEX idx_job_offers_embedding
--   ON job_offers USING ivfflat (embedding vector_cosine_ops)
--   WITH (lists = 100);
