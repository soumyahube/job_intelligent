"""
preprocess.py
-------------
Prétraitement des offres Remotive.
Normalise les données vers le schéma de la table job_offers.
"""

import re
from datetime import datetime


def nettoyer_html(texte):
    """Supprime les balises HTML d'un texte."""
    if not texte:
        return ""
    texte = re.sub(r"<[^>]+>", " ", texte)
    texte = re.sub(r"&nbsp;", " ", texte)
    texte = re.sub(r"&amp;", "&", texte)
    texte = re.sub(r"&lt;", "<", texte)
    texte = re.sub(r"&gt;", ">", texte)
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte[:5000]  # Limiter la taille


def extraire_salaire(salary_str):
    """
    Extrait salaire min/max depuis une chaîne comme '$80k - $120k'.
    Retourne (min, max, devise).
    """
    if not salary_str:
        return None, None, "USD"

    salary_str = salary_str.lower().replace(",", "")

    # Détecter la devise
    devise = "USD"
    if "€" in salary_str or "eur" in salary_str:
        devise = "EUR"
    elif "£" in salary_str or "gbp" in salary_str:
        devise = "GBP"

    # Extraire les nombres (avec support k = milliers)
    nombres = re.findall(r"(\d+(?:\.\d+)?)\s*k?", salary_str)
    valeurs = []
    for n in nombres:
        val = float(n)
        if "k" in salary_str[salary_str.find(n):salary_str.find(n)+5]:
            val *= 1000
        valeurs.append(val)

    if len(valeurs) >= 2:
        return min(valeurs), max(valeurs), devise
    elif len(valeurs) == 1:
        return valeurs[0], valeurs[0], devise

    return None, None, devise


def normaliser_type_contrat(job_type):
    """Normalise le type de contrat."""
    if not job_type:
        return "Full-time"
    mapping = {
        "full_time": "Full-time",
        "part_time": "Part-time",
        "contract": "Contract",
        "freelance": "Freelance",
        "internship": "Internship",
    }
    return mapping.get(job_type.lower(), job_type)


def parser_date(date_str):
    """Parse la date de publication Remotive."""
    if not date_str:
        return datetime.now()
    try:
        # Format Remotive : "2024-01-15T12:00:00"
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.now()


def pretraiter_offres(offres_brutes):
    """
    Transforme les offres brutes Remotive vers le schéma job_offers.
    """
    offres_propres = []

    for offre in offres_brutes:
        try:
            titre = offre.get("title", "").strip()
            if not titre:
                continue

            description_raw = offre.get("description", "")
            description = nettoyer_html(description_raw)
            if not description:
                continue

            # Entreprise
            entreprise = offre.get("company_name", "").strip() or "Inconnu"

            # Localisation — Remotive est remote donc on note "Remote"
            candidate_region = offre.get("candidate_required_location", "")
            localisation = candidate_region if candidate_region else "Remote"

            # Salaire
            salary_str = offre.get("salary", "")
            salaire_min, salaire_max, devise = extraire_salaire(salary_str)

            # Compétences — extraire depuis les tags
            tags = offre.get("tags", [])
            competences = ", ".join(tags) if tags else ""

            # Type de contrat
            job_type = offre.get("job_type", "full_time")
            type_contrat = normaliser_type_contrat(job_type)

            # URL
            url = offre.get("url", "")
            if not url:
                continue  # Sans URL on ne peut pas déduplicer

            # Date
            date_pub = parser_date(offre.get("publication_date", ""))

            offres_propres.append({
                "titre":             titre,
                "entreprise":        entreprise,
                "localisation":      localisation,
                "description":       description,
                "salaire_min":       salaire_min,
                "salaire_max":       salaire_max,
                "devise":            devise,
                "competences_texte": competences,
                "type_contrat":      type_contrat,
                "niveau_experience": None,
                "url":               url,
                "source":            "remotive",
                "date_publication":  date_pub,
            })

        except Exception as e:
            print(f"   ⚠️ Offre ignorée (erreur) : {e}")
            continue

    return offres_propres
