"""
adzuna.py
---------
Collecte les offres d'emploi data depuis l'API Adzuna.
Nécessite : ADZUNA_APP_ID et ADZUNA_API_KEY dans les variables d'environnement.
API doc : https://developer.adzuna.com/
"""

import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

APP_ID  = os.environ.get("ADZUNA_APP_ID")
API_KEY = os.environ.get("ADZUNA_API_KEY")

# Pays ciblés (Adzuna couvre plusieurs marchés)
PAYS = ["fr", "gb", "us"]

# Mots-clés data
KEYWORDS = [
    "data engineer",
    "data scientist",
    "data analyst",
    "machine learning engineer",
    "MLOps engineer",
    "business intelligence",
    "data architect",
]

API_BASE = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"


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


def collecter_par_keyword_et_pays(keyword, pays, nb_pages=2):
    """
    Collecte les offres pour un mot-clé et un pays donnés.
    Adzuna retourne max 50 résultats par page.
    """
    session = get_session()
    offres = []

    for page in range(1, nb_pages + 1):
        url = API_BASE.format(country=pays, page=page)
        params = {
            "app_id":       APP_ID,
            "app_key":      API_KEY,
            "what":         keyword,
            "results_per_page": 50,
            "content-type": "application/json",
        }

        for tentative in range(1, 4):
            try:
                response = session.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    results = data.get("results", [])
                    print(f"   → '{keyword}' [{pays}] page {page} : {len(results)} offres")
                    offres.extend(results)
                    break

                elif response.status_code == 401:
                    print(f"   ❌ Clé API Adzuna invalide ou manquante")
                    return []

                else:
                    print(f"   ERREUR API Adzuna '{keyword}' [{pays}] : {response.status_code}")
                    break

            except requests.exceptions.ConnectionError:
                print(f"   ⚠️ Erreur connexion (tentative {tentative}/3)")
                if tentative < 3:
                    time.sleep(tentative * 5)
            except requests.exceptions.Timeout:
                print(f"   ⚠️ Timeout (tentative {tentative}/3)")
                if tentative < 3:
                    time.sleep(tentative * 5)
            except Exception as e:
                print(f"   ❌ Erreur inattendue : {e}")
                break

        time.sleep(0.5)  # Respecter le rate limit Adzuna

    return offres


def collecter_toutes_les_offres():
    """
    Collecte et déduplique toutes les offres Adzuna.
    """
    if not APP_ID or not API_KEY:
        print("   ❌ ADZUNA_APP_ID et ADZUNA_API_KEY requis")
        return []

    toutes_les_offres = []
    ids_vus = set()

    for pays in PAYS:
        for keyword in KEYWORDS:
            offres = collecter_par_keyword_et_pays(keyword, pays)
            for offre in offres:
                offre_id = offre.get("id")
                if offre_id and offre_id not in ids_vus:
                    # Enrichir avec les métadonnées de collecte
                    offre["_pays"] = pays
                    ids_vus.add(offre_id)
                    toutes_les_offres.append(offre)
            time.sleep(1)

    print(f"\n   Total offres Adzuna uniques collectées : {len(toutes_les_offres)}")
    return toutes_les_offres
