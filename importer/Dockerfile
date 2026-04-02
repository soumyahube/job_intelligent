FROM python:3.11-slim

WORKDIR /app

# Copie et installe les dépendances en premier
# (Docker met ça en cache → plus rapide si vous relancez)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie le script d'import
COPY import_kaggle.py .

# Lance le script quand le container démarre
CMD ["python", "import_kaggle.py"]