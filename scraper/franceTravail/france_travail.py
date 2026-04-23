"""
france_travail.py
-----------------
Récupère les offres d'emploi data depuis l'API France Travail.
Retourne les données brutes en JSON.
"""

import os
import requests

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


def get_token():
    """
    Récupère un token d'accès OAuth2.
    Ce token est valable 30 minutes — on le redemande à chaque lancement.
    """
    print("   Récupération du token OAuth2...")
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope":         "api_offresdemploiv2 o2dsoffre"
        }
    )

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
    """
    headers = {"Authorization": f"Bearer {token}"}

    params = {
        "motsCles":  mot_cle,
        "range":     f"0-{nb_max - 1}",  # 0 à 99 = 100 offres
        "sort":      "1",                 # Tri par date (plus récent en premier)
    }

    response = requests.get(API_URL, headers=headers, params=params)

    if response.status_code == 200:
        data     = response.json()
        offres   = data.get("resultats", [])
        print(f"   → '{mot_cle}' : {len(offres)} offres récupérées")
        return offres

    elif response.status_code == 204:
        # 204 = pas de résultats pour ce mot-clé
        print(f"   → '{mot_cle}' : aucune offre trouvée")
        return []

    else:
        print(f"   ERREUR API '{mot_cle}' : {response.status_code}")
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
    ids_vus = set()  # Pour éviter les doublons entre mots-clés

    for mot_cle in RECHERCHES_DATA:
        offres = recuperer_offres(token, mot_cle)
        for offre in offres:
            offre_id = offre.get("id")
            if offre_id and offre_id not in ids_vus:
                ids_vus.add(offre_id)
                toutes_les_offres.append(offre)

    print(f"\n   Total offres uniques collectées : {len(toutes_les_offres)}")
    return toutes_les_offres
