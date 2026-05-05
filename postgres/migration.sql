-- =============================================================
-- migration.sql — Passage vers l'architecture 2 schémas
-- job_market + candidate
--
-- CE SCRIPT NE SUPPRIME AUCUNE DONNÉE EXISTANTE
-- Il crée les nouveaux schémas, migre les données,
-- et garde les anciennes tables publiques le temps
-- que vous validez la migration.
--
-- Ordre d'exécution :
--   python run_migration.py
-- =============================================================

-- ─────────────────────────────────────────────
-- ÉTAPE 0 — Extensions
-- ─────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- =============================================================
-- ÉTAPE 1 — Créer les 2 schémas
-- =============================================================
CREATE SCHEMA IF NOT EXISTS job_market;
CREATE SCHEMA IF NOT EXISTS candidate;


-- =============================================================
-- ÉTAPE 2 — Créer les tables du schéma JOB_MARKET
-- =============================================================

CREATE TABLE IF NOT EXISTS job_market.dim_source (
    id         SERIAL PRIMARY KEY,
    nom        VARCHAR(50) UNIQUE NOT NULL,
    url_base   TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_market.dim_contrat (
    id           SERIAL PRIMARY KEY,
    type_contrat VARCHAR(100) UNIQUE NOT NULL,
    categorie    VARCHAR(50)
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
    id        SERIAL PRIMARY KEY,
    date      DATE UNIQUE NOT NULL,
    annee     INTEGER,
    mois      INTEGER,
    semaine   INTEGER,
    trimestre INTEGER,
    jour_semaine VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS job_market.dim_competence (
    id         SERIAL PRIMARY KEY,
    nom        VARCHAR(100) UNIQUE NOT NULL,
    categorie  VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_market.job_offers (
    id               SERIAL PRIMARY KEY,
    titre            VARCHAR(255),
    entreprise       VARCHAR(255),
    localisation     VARCHAR(500),
    description      TEXT,
    salaire_min      FLOAT,
    salaire_max      FLOAT,
    devise           VARCHAR(10) DEFAULT 'USD',
    competences_texte TEXT,
    type_contrat        VARCHAR(100),
    niveau_experience   VARCHAR(100),
    type_teletravail    VARCHAR(100),
    source_id         INTEGER REFERENCES job_market.dim_source(id),
    contrat_id        INTEGER REFERENCES job_market.dim_contrat(id),
    localisation_id   INTEGER REFERENCES job_market.dim_localisation(id),
    date_id           INTEGER REFERENCES job_market.dim_date(id),
    url              TEXT UNIQUE,
    source           VARCHAR(50) DEFAULT 'linkedin',
    date_publication TIMESTAMP,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_market.job_competences (
    job_id        INTEGER NOT NULL REFERENCES job_market.job_offers(id) ON DELETE CASCADE,
    competence_id INTEGER NOT NULL REFERENCES job_market.dim_competence(id),
    PRIMARY KEY (job_id, competence_id)
);

CREATE TABLE IF NOT EXISTS job_market.job_embeddings (
    job_id    INTEGER PRIMARY KEY REFERENCES job_market.job_offers(id) ON DELETE CASCADE,
    embedding vector(384) NOT NULL,
    modele    VARCHAR(100) DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at TIMESTAMP DEFAULT NOW()
);


-- =============================================================
-- ÉTAPE 3 — Créer les tables du schéma CANDIDATE
-- =============================================================

CREATE TABLE IF NOT EXISTS candidate.candidates (
    id            SERIAL PRIMARY KEY,
    type_entree   VARCHAR(10) NOT NULL CHECK (type_entree IN ('cv', 'text')),
    contenu_texte TEXT NOT NULL,
    nom_fichier   VARCHAR(255),
    email         VARCHAR(255),
    nom           VARCHAR(255),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS candidate.cv_embeddings (
    candidate_id INTEGER PRIMARY KEY REFERENCES candidate.candidates(id) ON DELETE CASCADE,
    embedding    vector(384) NOT NULL,
    modele       VARCHAR(100) DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS candidate.candidate_skills (
    candidate_id  INTEGER NOT NULL REFERENCES candidate.candidates(id) ON DELETE CASCADE,
    competence_id INTEGER NOT NULL REFERENCES job_market.dim_competence(id),
    PRIMARY KEY (candidate_id, competence_id)
);

CREATE TABLE IF NOT EXISTS candidate.recommendations (
    id               SERIAL PRIMARY KEY,
    candidate_id     INTEGER NOT NULL REFERENCES candidate.candidates(id)  ON DELETE CASCADE,
    job_id           INTEGER NOT NULL REFERENCES job_market.job_offers(id) ON DELETE CASCADE,
    score_similarite FLOAT   NOT NULL,
    created_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (candidate_id, job_id)
);


-- =============================================================
-- ÉTAPE 4 — Données de référence
-- ⚠️  AUCUN INSERT ICI
-- Les dimensions sont remplies par Airflow automatiquement :
--   dim_source       → save_to_db.py (à chaque scraping)
--   dim_contrat      → save_to_db.py (à chaque scraping)
--   dim_localisation → DAG silver_to_gold
--   dim_date         → DAG silver_to_gold
--   dim_competence   → DAG silver_to_gold (NLP)
-- =============================================================


-- =============================================================
-- ÉTAPE 5 — Peupler dim_date avec les dates existantes
-- =============================================================
INSERT INTO job_market.dim_date (date, annee, mois, semaine, trimestre, jour_semaine)
SELECT DISTINCT
    date_publication::DATE,
    EXTRACT(YEAR  FROM date_publication)::INTEGER,
    EXTRACT(MONTH FROM date_publication)::INTEGER,
    EXTRACT(WEEK  FROM date_publication)::INTEGER,
    EXTRACT(QUARTER FROM date_publication)::INTEGER,
    TO_CHAR(date_publication, 'Day')
FROM public.job_offers
WHERE date_publication IS NOT NULL
ON CONFLICT (date) DO NOTHING;


-- =============================================================
-- ÉTAPE 6 — Migrer les localisations existantes
-- =============================================================
INSERT INTO job_market.dim_localisation (ville, is_remote)
SELECT DISTINCT
    localisation,
    CASE WHEN LOWER(localisation) LIKE '%remote%'
              OR LOWER(localisation) LIKE '%télétravail%'
         THEN TRUE ELSE FALSE END
FROM public.job_offers
WHERE localisation IS NOT NULL AND localisation != ''
ON CONFLICT (ville, pays) DO NOTHING;


-- =============================================================
-- ÉTAPE 7 — Migrer les offres existantes vers job_market.job_offers
-- On copie TOUTES les offres de la table publique existante
-- =============================================================
INSERT INTO job_market.job_offers (
    titre, entreprise, localisation, description,
    salaire_min, salaire_max, devise,
    competences_texte, type_contrat, niveau_experience,
    type_teletravail, url, source, date_publication,
    source_id, contrat_id, localisation_id, date_id,
    created_at
)
SELECT
    jo.titre,
    jo.entreprise,
    jo.localisation,
    jo.description,
    jo.salaire_min,
    jo.salaire_max,
    jo.devise,
    jo.competences_texte,
    jo.type_contrat,
    jo.niveau_experience,
    jo.type_teletravail,
    jo.url,
    jo.source,
    jo.date_publication,
    ds.id   AS source_id,
    dc.id   AS contrat_id,
    dl.id   AS localisation_id,
    dd.id   AS date_id,
    jo.created_at
FROM public.job_offers jo
LEFT JOIN job_market.dim_source      ds ON jo.source        = ds.nom
LEFT JOIN job_market.dim_contrat     dc ON jo.type_contrat  = dc.type_contrat
LEFT JOIN job_market.dim_localisation dl ON jo.localisation  = dl.ville
LEFT JOIN job_market.dim_date        dd ON jo.date_publication::DATE = dd.date
ON CONFLICT (url) DO NOTHING;


-- =============================================================
-- ÉTAPE 8 — Migrer les profils utilisateurs existants
--           (ancienne table public.user_profiles)
-- =============================================================
INSERT INTO candidate.candidates (
    type_entree, contenu_texte, nom_fichier, created_at
)
SELECT
    type_entree,
    contenu_texte,
    nom_fichier,
    created_at
FROM public.user_profiles
ON CONFLICT DO NOTHING;


-- =============================================================
-- ÉTAPE 9 — Migrer les recommandations existantes
-- On fait la jointure via url (car les IDs peuvent différer)
-- =============================================================
INSERT INTO candidate.recommendations (candidate_id, job_id, score_similarite, created_at)
SELECT
    nc.id   AS candidate_id,
    njo.id  AS job_id,
    r.score_similarite,
    r.created_at
FROM public.recommendations r
-- Retrouver le candidat migré via son contenu
JOIN public.user_profiles up    ON r.profile_id = up.id
JOIN candidate.candidates nc    ON nc.contenu_texte = up.contenu_texte
                                AND nc.created_at   = up.created_at
-- Retrouver l'offre migrée via son url
JOIN public.job_offers pjo      ON r.offre_id = pjo.id
JOIN job_market.job_offers njo  ON njo.url = pjo.url
ON CONFLICT (candidate_id, job_id) DO NOTHING;


-- =============================================================
-- ÉTAPE 10 — Index sur les nouvelles tables
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
-- Index trigram pour matching fuzzy des compétences (NLP)
CREATE INDEX IF NOT EXISTS idx_dc_nom_trgm     ON job_market.dim_competence USING gin(nom gin_trgm_ops);

-- candidate
CREATE INDEX IF NOT EXISTS idx_rec_candidate   ON candidate.recommendations(candidate_id);
CREATE INDEX IF NOT EXISTS idx_rec_job         ON candidate.recommendations(job_id);
CREATE INDEX IF NOT EXISTS idx_csk_candidate   ON candidate.candidate_skills(candidate_id);


-- =============================================================
-- VÉRIFICATION FINALE
-- =============================================================
SELECT 'job_market.job_offers'      AS table_name, COUNT(*) AS lignes FROM job_market.job_offers
UNION ALL
SELECT 'job_market.dim_source',       COUNT(*) FROM job_market.dim_source
UNION ALL
SELECT 'job_market.dim_contrat',      COUNT(*) FROM job_market.dim_contrat
UNION ALL
SELECT 'job_market.dim_localisation', COUNT(*) FROM job_market.dim_localisation
UNION ALL
SELECT 'job_market.dim_date',         COUNT(*) FROM job_market.dim_date
UNION ALL
SELECT 'job_market.dim_competence',   COUNT(*) FROM job_market.dim_competence
UNION ALL
SELECT 'job_market.job_competences',  COUNT(*) FROM job_market.job_competences
UNION ALL
SELECT 'candidate.candidates',        COUNT(*) FROM candidate.candidates
UNION ALL
SELECT 'candidate.cv_embeddings',     COUNT(*) FROM candidate.cv_embeddings
UNION ALL
SELECT 'candidate.candidate_skills',  COUNT(*) FROM candidate.candidate_skills
UNION ALL
SELECT 'candidate.recommendations',   COUNT(*) FROM candidate.recommendations
ORDER BY table_name;

-- =============================================================
-- NOTE IMPORTANTE — anciennes tables publiques
-- Elles sont CONSERVÉES pour sécurité.
-- Une fois que vous avez validé la migration, vous pouvez
-- les supprimer avec :
--
--   DROP TABLE IF EXISTS public.recommendations CASCADE;
--   DROP TABLE IF EXISTS public.user_profiles    CASCADE;
--   DROP TABLE IF EXISTS public.job_offers       CASCADE;
--   DROP TABLE IF EXISTS public.dim_localisation CASCADE;
--   DROP TABLE IF EXISTS public.dim_contrat      CASCADE;
--   DROP TABLE IF EXISTS public.dim_source       CASCADE;
-- =============================================================
