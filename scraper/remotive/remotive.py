"""
remotive.py
-----------
Collecte les offres d'emploi data depuis l'API Remotive.
Aucune clé API requise — API publique gratuite.
"""

import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://remotive.com/api/remote-jobs"

# Catégories ciblées
CATEGORIES = ["data"]

# Mots-clés data pour filtrer les offres software-dev et devops
KEYWORDS_DATA = [
    "data engineer", "data engineering","data lake", "etl","ml", "machine learning", "deep learning",
    "mlops", "analytics", "bi ", "business intelligence","pyspark", "spark", "airflow", "dbt", "warehouse", "pipeline",
    "spark", "airflow", "dbt", "warehouse", "pipeline",
    "python", "sql", "engineer", "scientist", "analyst"
]


def get_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def est_offre_data(offre):
    """
    Filtre strict sur le titre uniquement — Data Engineering uniquement.
    """
    titre = offre.get("title", "").lower()
    return any(kw in titre for kw in KEYWORDS_DATA)


def collecter_par_categorie(categorie):
    """
    Collecte les offres pour une catégorie donnée.
    """
    session = get_session()
    params = {"category": categorie}

    for tentative in range(1, 4):
        try:
            response = session.get(API_BASE, params=params, timeout=30)

            if response.status_code == 200:
                jobs = response.json().get("jobs", [])
                print(f"   → '{categorie}' : {len(jobs)} offres récupérées")
                return jobs

            else:
                print(f"   ERREUR API '{categorie}' : {response.status_code}")
                return []

        except requests.exceptions.ConnectionError as e:
            print(f"   ⚠️ Erreur connexion '{categorie}' (tentative {tentative}/3)")
            if tentative < 3:
                time.sleep(tentative * 5)
            else:
                print(f"   ❌ Échec définitif pour '{categorie}'")
                return []

        except requests.exceptions.Timeout:
            print(f"   ⚠️ Timeout '{categorie}' (tentative {tentative}/3)")
            if tentative < 3:
                time.sleep(tentative * 5)
            else:
                return []

        except Exception as e:
            print(f"   ❌ Erreur inattendue '{categorie}' : {e}")
            return []

    return []


def collecter_toutes_les_offres():
    """
    Collecte et filtre toutes les offres data depuis Remotive.
    """
    toutes_les_offres = []
    ids_vus = set()

    for categorie in CATEGORIES:
        offres = collecter_par_categorie(categorie)

        for offre in offres:
            offre_id = offre.get("id")
            if offre_id and offre_id not in ids_vus:
                # Pour la catégorie data : tout garder
                # Pour les autres : filtrer sur les mots-clés
                if categorie == "data" or est_offre_data(offre):
                    ids_vus.add(offre_id)
                    toutes_les_offres.append(offre)

        time.sleep(1)  # Respecter l'API

    print(f"\n   Total offres data uniques collectées : {len(toutes_les_offres)}")
    return toutes_les_offres
