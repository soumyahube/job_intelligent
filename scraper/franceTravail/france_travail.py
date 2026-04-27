"""
france_travail.py
-----------------
Récupère les offres d'emploi data depuis l'API France Travail.
Retourne les données brutes en JSON.
"""

import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Vos clés API — viennent du fichier .env
CLIENT_ID     = os.environ.get("FT_CLIENT_ID")
CLIENT_SECRET = os.environ.get("FT_CLIENT_SECRET")

# URLs de l'API France Travail
TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
API_URL   = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

# Mots-clés pour cibler les offres data
RECHERCHES_DATA = [
    "data engineer",
    "data scientist",
    "data analyst",
    "machine learning engineer",
    "MLOps",
    "ingenieur données",
    "analyste données",
    "business intelligence",
    "data architect"
]


def get_session():
    """
    Crée une session requests avec retry automatique.
    Gère les erreurs réseau et les codes HTTP 5xx.
    """
    session = requests.Session()
    retry = Retry(
        total=3,                          # 3 tentatives max
        backoff_factor=2,                 # attendre 2s, 4s, 8s entre chaque
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_token():
    """
    Récupère un token d'accès OAuth2.
    Ce token est valable 30 minutes — on le redemande à chaque lancement.
    """
    print("   Récupération du token OAuth2...")
    session = get_session()

    try:
        response = session.post(
            TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope":         "api_offresdemploiv2 o2dsoffre"
            },
            timeout=30
        )
    except Exception as e:
        print(f"   ERREUR token (réseau) : {e}")
        return None

    if response.status_code not in (200, 206):
        print(f"   ERREUR token : {response.status_code} — {response.text}")
        return None

    token = response.json().get("access_token")
    print("   → Token obtenu ✅")
    return token


def recuperer_offres(token, mot_cle, nb_max=100):
    """
    Récupère les offres pour un mot-clé donné.
    L'API retourne max 150 offres par requête.
    Retry automatique en cas d'erreur SSL ou réseau.
    """
    headers = {"Authorization": f"Bearer {token}"}
    params  = {
        "motsCles": mot_cle,
        "range":    f"0-{nb_max - 1}",
        "sort":     "1",
    }

    for tentative in range(1, 4):  # 3 tentatives max
        try:
            session  = get_session()
            response = session.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                offres = response.json().get("resultats", [])
                print(f"   → '{mot_cle}' : {len(offres)} offres récupérées")
                return offres

            elif response.status_code == 204:
                print(f"   → '{mot_cle}' : aucune offre trouvée")
                return []

            else:
                print(f"   ERREUR API '{mot_cle}' : {response.status_code}")
                return []

        except requests.exceptions.SSLError as e:
            print(f"   ⚠️ Erreur SSL '{mot_cle}' (tentative {tentative}/3) : {e}")
            if tentative < 3:
                attente = tentative * 5  # 5s, 10s, 15s
                print(f"   ⏳ Retry dans {attente}s...")
                time.sleep(attente)
            else:
                print(f"   ❌ Échec définitif pour '{mot_cle}' après 3 tentatives")
                return []

        except requests.exceptions.ConnectionError as e:
            print(f"   ⚠️ Erreur connexion '{mot_cle}' (tentative {tentative}/3) : {e}")
            if tentative < 3:
                attente = tentative * 5
                print(f"   ⏳ Retry dans {attente}s...")
                time.sleep(attente)
            else:
                print(f"   ❌ Échec définitif pour '{mot_cle}' après 3 tentatives")
                return []

        except requests.exceptions.Timeout:
            print(f"   ⚠️ Timeout '{mot_cle}' (tentative {tentative}/3)")
            if tentative < 3:
                print(f"   ⏳ Retry dans {tentative * 5}s...")
                time.sleep(tentative * 5)
            else:
                print(f"   ❌ Timeout définitif pour '{mot_cle}'")
                return []

    return []


def collecter_toutes_les_offres():
    """
    Lance la collecte pour tous les mots-clés data.
    Retourne une liste de toutes les offres brutes.
    """
    token = get_token()
    if not token:
        return []

    toutes_les_offres = []
    ids_vus = set()

    for mot_cle in RECHERCHES_DATA:
        offres = recuperer_offres(token, mot_cle)
        for offre in offres:
            offre_id = offre.get("id")
            if offre_id and offre_id not in ids_vus:
                ids_vus.add(offre_id)
                toutes_les_offres.append(offre)

    print(f"\n   Total offres uniques collectées : {len(toutes_les_offres)}")
    return toutes_les_offres