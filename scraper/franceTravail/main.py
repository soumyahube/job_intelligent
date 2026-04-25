"""
main.py
-------
Point d'entrée du scraper France Travail.
Orchestre : collecte → prétraitement → sauvegarde.
"""

from france_travail import collecter_toutes_les_offres
from preprocess    import pretraiter_toutes
from save_to_db    import sauvegarder_brut_dans_minio, inserer_dans_postgres

print("=" * 50)
print("SCRAPER FRANCE TRAVAIL")
print("=" * 50)

# Étape 1 — Collecter les offres brutes depuis l'API
print("\n1. Collecte des offres depuis l'API France Travail...")
offres_brutes = collecter_toutes_les_offres()

if not offres_brutes:
    print("Aucune offre collectée. Vérifiez vos clés API dans le .env")
    exit(1)

# Étape 2 — Archiver les données brutes dans MinIO
print("\n2. Archive des données brutes dans MinIO...")
sauvegarder_brut_dans_minio(offres_brutes)

# Étape 3 — Prétraitement
print("\n3. Prétraitement des données...")
offres_propres = pretraiter_toutes(offres_brutes)

# Étape 4 — Insertion dans PostgreSQL
print("\n4. Insertion dans PostgreSQL...")
inserer_dans_postgres(offres_propres)

print("\n" + "=" * 50)
print("Scraper terminé avec succès !")
print("=" * 50)