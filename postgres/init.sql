-- =============================================================
-- init.sql — Créé automatiquement au démarrage de PostgreSQL
-- Adapté au dataset : LinkedIn Job Postings 2023-2024
-- =============================================================

-- Active l'extension pgvector (recherche par similarité)
CREATE EXTENSION IF NOT EXISTS vector;

-- Table principale des offres d'emploi
CREATE TABLE IF NOT EXISTS job_offers (
    id              SERIAL PRIMARY KEY,

    -- Infos principales (viennent directement du CSV)
    titre           VARCHAR(255),
    entreprise      VARCHAR(255),
    localisation    VARCHAR(500),
    description      TEXT,

    -- Salaire (le CSV a min/max séparément)
    salaire_min     FLOAT,
    salaire_max     FLOAT,
    devise          VARCHAR(10)   DEFAULT 'USD',

    -- Compétences (le CSV a un champ texte "skills_desc")
    -- On le stocke en texte simple, pas en tableau
    competences_texte TEXT,

    -- Type de poste
    type_contrat        VARCHAR(100),  -- "Full-time", "Part-time", "Contract"
    niveau_experience   VARCHAR(100),  -- "Entry level", "Mid-Senior", "Director"
    type_teletravail    VARCHAR(100),  -- "Remote", "On-site", "Hybrid"

    -- Lien et source
    url             TEXT,
    source          VARCHAR(50)   DEFAULT 'linkedin',

    -- Date (listed_time dans le CSV est un timestamp Unix → on convertit)
    date_publication TIMESTAMP,

    -- Embedding : NULL au départ, rempli après par le script Python
    -- 384 = taille du modèle sentence-transformers qu'on va utiliser
    embedding       vector(384),

    -- Date d'ajout dans notre base
    created_at      TIMESTAMP     DEFAULT NOW()
);

-- Index pour accélérer la recherche par similarité cosinus
-- (créé seulement quand la table a des données, sinon erreur)
-- On le crée dans le script Python après l'import