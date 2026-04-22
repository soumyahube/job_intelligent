"""
preprocess.py
-------------
Nettoie et normalise les données brutes de l'API France Travail
avant insertion dans PostgreSQL.
"""

import re
from datetime import datetime


def extraire_salaire(salaire_obj):
    """
    L'API retourne : {"libelle": "Annuel de 35000 à 45000 sur 12 mois"}
    On extrait les deux nombres : (35000.0, 45000.0)
    """
    if not salaire_obj:
        return None, None

    libelle = salaire_obj.get("libelle", "")

    # Chercher tous les nombres dans la chaîne
    nombres = re.findall(r'\d+(?:\s\d{3})*(?:[.,]\d+)?', libelle)
    nombres = [float(n.replace(" ", "").replace(",", ".")) for n in nombres]

    if len(nombres) >= 2:
        return min(nombres), max(nombres)
    elif len(nombres) == 1:
        return nombres[0], None
    return None, None


def extraire_ville(lieu_obj):
    """
    L'API retourne : {"libelle": "75 - Paris", "codePostal": "75001"}
    On extrait juste : "Paris"
    """
    if not lieu_obj:
        return None

    libelle = lieu_obj.get("libelle", "")

    # Format "75 - Paris" → on prend après le tiret
    if " - " in libelle:
        return libelle.split(" - ", 1)[1].strip()

    return libelle.strip()


def extraire_competences(competences_liste):
    """
    L'API retourne : [{"libelle": "Python"}, {"libelle": "SQL"}]
    On extrait : "Python, SQL"
    """
    if not competences_liste:
        return None

    noms = [c.get("libelle", "") for c in competences_liste if c.get("libelle")]
    return ", ".join(noms) if noms else None


def convertir_date(date_str):
    """
    L'API retourne : "2024-01-15T10:30:00+01:00"
    On convertit en datetime Python.
    """
    if not date_str:
        return None
    try:
        # Retirer le fuseau horaire pour simplifier
        date_str = date_str[:19]
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    except:
        return None


def normaliser_contrat(type_contrat):
    """
    Normalise les types de contrat France Travail en anglais
    pour cohérence avec les données Kaggle.
    """
    mapping = {
        "CDI": "Full-time",
        "CDD": "Contract",
        "MIS": "Contract",    # Mission intérim
        "SAI": "Contract",    # Saisonnier
        "LIB": "Contract",    # Libéral
        "FRA": "Contract",    # Franchise
        "REP": "Full-time",   # Reprise
        "TTI": "Full-time",   # Travail temporaire
        "DIN": "Full-time",   # Contrat d'insertion
    }
    return mapping.get(type_contrat, type_contrat)


def pretraiter_offre(offre_brute):
    """
    Transforme une offre brute de l'API en dictionnaire
    prêt pour insertion dans PostgreSQL.
    """
    sal_min, sal_max = extraire_salaire(offre_brute.get("salaire"))

    return {
        "titre":            offre_brute.get("intitule", "")[:255],
        "entreprise":       offre_brute.get("entreprise", {}).get("nom", "")[:255],
        "localisation":     extraire_ville(offre_brute.get("lieuTravail")),
        "description":      offre_brute.get("description"),
        "salaire_min":      sal_min,
        "salaire_max":      sal_max,
        "devise":           "EUR",
        "competences_texte": extraire_competences(offre_brute.get("competences")),
        "type_contrat":     normaliser_contrat(offre_brute.get("typeContrat")),
        "niveau_experience": offre_brute.get("experienceLibelle"),
        "url":              offre_brute.get("origineOffre", {}).get("urlOrigine"),
        "source":           "france_travail",
        "date_publication": convertir_date(offre_brute.get("dateCreation")),
    }


def pretraiter_toutes(offres_brutes):
    """
    Applique le prétraitement sur toutes les offres.
    Filtre les offres sans titre ou description.
    """
    offres_propres = []

    for offre in offres_brutes:
        propre = pretraiter_offre(offre)

        # Ignorer les offres sans titre ou sans description
        if not propre["titre"] or not propre["description"]:
            continue

        offres_propres.append(propre)

    print(f"   → {len(offres_propres)} offres valides après prétraitement")
    return offres_propres