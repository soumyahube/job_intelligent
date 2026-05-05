"""
main.py
-------
Point d'entrée du scraper Remotive.
Orchestre la collecte, le prétraitement et la sauvegarde.
"""

from remotive import collecter_toutes_les_offres
from preprocess import pretraiter_offres
from scraper.remotive.save_to_db_v0 import sauvegarder_brut_dans_minio, inserer_dans_postgres

print("=" * 50)
print("SCRAPER REMOTIVE")
print("=" * 50)

# 1. Collecte
print("\n1. Collecte des offres depuis l'API Remotive...")
offres_brutes = collecter_toutes_les_offres()

if not offres_brutes:
    print("Aucune offre collectée. Vérifiez votre connexion.")
    exit(1)

# 2. Archive MinIO
print("\n2. Archive des données brutes dans MinIO...")
sauvegarder_brut_dans_minio(offres_brutes, source="remotive")

# 3. Prétraitement
print("\n3. Prétraitement des données...")
offres_propres = pretraiter_offres(offres_brutes)
print(f"   → {len(offres_propres)} offres valides après prétraitement")

if not offres_propres:
    print("Aucune offre valide après prétraitement.")
    exit(1)

# 4. Insertion PostgreSQL
print("\n4. Insertion dans PostgreSQL...")
inserer_dans_postgres(offres_propres)

print("\n" + "=" * 50)
print("Scraper Remotive terminé avec succès !")
print("=" * 50)
