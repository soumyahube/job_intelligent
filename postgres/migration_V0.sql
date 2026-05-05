-- =============================================================
-- migration.sql
-- À exécuter UNE SEULE FOIS sur votre base existante
-- via DBeaver, psql, ou le script Python ci-dessous
--
-- Ce script NE SUPPRIME RIEN — il ajoute seulement
-- de nouvelles tables et colonnes par dessus l'existant
-- =============================================================



-- ─────────────────────────────────────────────
-- ÉTAPE 1 — Extension pgvector (si pas encore active)
-- ─────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS vector;


-- ─────────────────────────────────────────────
-- ÉTAPE 2 — Tables dimensions
-- ─────────────────────────────────────────────
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


-- ─────────────────────────────────────────────
-- ÉTAPE 3 — Nouvelles colonnes dans job_offers
-- (IF NOT EXISTS évite toute erreur si déjà présentes)
-- ─────────────────────────────────────────────
ALTER TABLE job_offers
    ADD COLUMN IF NOT EXISTS source_id        INTEGER REFERENCES dim_source(id),
    ADD COLUMN IF NOT EXISTS contrat_id       INTEGER REFERENCES dim_contrat(id),
    ADD COLUMN IF NOT EXISTS localisation_id  INTEGER REFERENCES dim_localisation(id),
    ADD COLUMN IF NOT EXISTS embedding        vector(384);


-- ─────────────────────────────────────────────
-- ÉTAPE 4 — Table profils utilisateurs
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_profiles (
    id            SERIAL PRIMARY KEY,
    type_entree   VARCHAR(10) NOT NULL CHECK (type_entree IN ('cv', 'text')),
    contenu_texte TEXT NOT NULL,
    nom_fichier   VARCHAR(255),
    embedding     vector(384),
    created_at    TIMESTAMP DEFAULT NOW()
);


-- ─────────────────────────────────────────────
-- ÉTAPE 5 — Table recommandations
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS recommendations (
    id               SERIAL PRIMARY KEY,
    profile_id       INTEGER NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
    offre_id         INTEGER NOT NULL REFERENCES job_offers(id)    ON DELETE CASCADE,
    score_similarite FLOAT   NOT NULL,
    created_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (profile_id, offre_id)
);


-- ─────────────────────────────────────────────
-- ÉTAPE 6 — Données de référence dans les dimensions
-- ─────────────────────────────────────────────
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


-- ─────────────────────────────────────────────
-- ÉTAPE 7 — Remplir source_id depuis la colonne
--           source existante dans job_offers
-- (Réconcilie vos données existantes avec les dimensions)
-- ─────────────────────────────────────────────
UPDATE job_offers jo
SET source_id = ds.id
FROM dim_source ds
WHERE jo.source = ds.nom
  AND jo.source_id IS NULL;


-- ─────────────────────────────────────────────
-- ÉTAPE 8 — Remplir contrat_id depuis type_contrat
-- ─────────────────────────────────────────────
UPDATE job_offers jo
SET contrat_id = dc.id
FROM dim_contrat dc
WHERE jo.type_contrat = dc.type_contrat
  AND jo.contrat_id IS NULL;


-- ─────────────────────────────────────────────
-- ÉTAPE 9 — Remplir dim_localisation depuis
--           les localisations existantes
-- ─────────────────────────────────────────────

-- Insérer les localisations uniques depuis job_offers
INSERT INTO dim_localisation (ville, is_remote)
SELECT DISTINCT
    localisation,
    CASE WHEN LOWER(localisation) LIKE '%remote%'
              OR LOWER(localisation) LIKE '%télétravail%'
         THEN TRUE ELSE FALSE END
FROM job_offers
WHERE localisation IS NOT NULL
  AND localisation != ''
ON CONFLICT (ville, pays) DO NOTHING;

-- Lier job_offers à dim_localisation
UPDATE job_offers jo
SET localisation_id = dl.id
FROM dim_localisation dl
WHERE jo.localisation = dl.ville
  AND jo.localisation_id IS NULL;


-- ─────────────────────────────────────────────
-- ÉTAPE 10 — Index classiques
-- ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_job_offers_source       ON job_offers(source);
CREATE INDEX IF NOT EXISTS idx_job_offers_date         ON job_offers(date_publication);
CREATE INDEX IF NOT EXISTS idx_job_offers_contrat      ON job_offers(type_contrat);
CREATE INDEX IF NOT EXISTS idx_job_offers_localisation ON job_offers(localisation);
CREATE INDEX IF NOT EXISTS idx_recommendations_profile ON recommendations(profile_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_offre   ON recommendations(offre_id);


-- ─────────────────────────────────────────────
-- VÉRIFICATION FINALE
-- ─────────────────────────────────────────────
SELECT 'job_offers'      AS table_name, COUNT(*) AS lignes FROM job_offers
UNION ALL
SELECT 'dim_source',       COUNT(*) FROM dim_source
UNION ALL
SELECT 'dim_contrat',      COUNT(*) FROM dim_contrat
UNION ALL
SELECT 'dim_localisation', COUNT(*) FROM dim_localisation
UNION ALL
SELECT 'user_profiles',    COUNT(*) FROM user_profiles
UNION ALL
SELECT 'recommendations',  COUNT(*) FROM recommendations
ORDER BY table_name;
