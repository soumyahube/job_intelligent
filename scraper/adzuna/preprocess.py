"""
preprocess.py
-------------
Prétraitement des offres Adzuna.
Normalise les données vers le schéma de la table job_offers.
"""

import re
from datetime import datetime


def nettoyer_texte(texte):
    """Supprime les caractères superflus et limite la taille."""
    if not texte:
        return ""
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte[:5000]


def extraire_salaire(offre):
    """
    Adzuna retourne salary_min et salary_max directement en float.
    La devise dépend du pays de collecte.
    """
    sal_min = offre.get("salary_min")
    sal_max = offre.get("salary_max")
    pays    = offre.get("_pays", "us")

    devise_map = {"fr": "EUR", "gb": "GBP", "us": "USD", "de": "EUR", "ca": "CAD"}
    devise = devise_map.get(pays, "USD")

    # Convertir en float si possible
    try:
        sal_min = float(sal_min) if sal_min else None
        sal_max = float(sal_max) if sal_max else None
    except (TypeError, ValueError):
        sal_min, sal_max = None, None

    return sal_min, sal_max, devise


def extraire_localisation(offre):
    """
    Adzuna retourne location.display_name comme 'Paris, Île-de-France'.
    """
    location = offre.get("location", {})
    return location.get("display_name", "Remote") or "Remote"


def normaliser_type_contrat(offre):
    """
    Adzuna retourne contract_type : 'permanent', 'contract', 'part_time'.
    """
    ct = offre.get("contract_type", "")
    mapping = {
        "permanent":  "Full-time",
        "contract":   "Contract",
        "part_time":  "Part-time",
        "full_time":  "Full-time",
    }
    return mapping.get((ct or "").lower(), "Full-time")


def parser_date(date_str):
    """Parse la date Adzuna : '2024-01-15T12:00:00Z'."""
    if not date_str:
        return datetime.now()
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.now()


def extraire_competences(offre):
    """
    Adzuna ne fournit pas de tags structurés.
    On extrait les mots-clés tech depuis la description.
    """
    TECH_KEYWORDS = [
        "python", "sql", "spark", "hadoop", "kafka", "airflow",
        "dbt", "tableau", "power bi", "looker", "aws", "gcp", "azure",
        "docker", "kubernetes", "tensorflow", "pytorch", "scikit-learn",
        "pandas", "numpy", "r ", "scala", "java", "databricks",
        "snowflake", "redshift", "bigquery", "elasticsearch",
    ]
    desc = (offre.get("description", "") or "").lower()
    titre = (offre.get("title", "") or "").lower()
    texte = f"{titre} {desc}"

    trouvees = [kw for kw in TECH_KEYWORDS if kw in texte]
    return ", ".join(trouvees) if trouvees else ""


def pretraiter_offres(offres_brutes):
    """
    Transforme les offres brutes Adzuna vers le schéma job_offers.
    """
    offres_propres = []

    for offre in offres_brutes:
        try:
            titre = (offre.get("title", "") or "").strip()
            if not titre:
                continue

            description = nettoyer_texte(offre.get("description", ""))
            if not description:
                continue

            url = offre.get("redirect_url", "")
            if not url:
                continue

            entreprise   = (offre.get("company", {}) or {}).get("display_name", "Inconnu") or "Inconnu"
            localisation = extraire_localisation(offre)
            sal_min, sal_max, devise = extraire_salaire(offre)
            type_contrat = normaliser_type_contrat(offre)
            competences  = extraire_competences(offre)
            date_pub     = parser_date(offre.get("created"))

            offres_propres.append({
                "titre":             titre,
                "entreprise":        entreprise,
                "localisation":      localisation,
                "description":       description,
                "salaire_min":       sal_min,
                "salaire_max":       sal_max,
                "devise":            devise,
                "competences_texte": competences,
                "type_contrat":      type_contrat,
                "niveau_experience": None,
                "url":               url,
                "source":            "adzuna",
                "date_publication":  date_pub,
            })

        except Exception as e:
            print(f"   ⚠️ Offre Adzuna ignorée : {e}")
            continue

    return offres_propres
