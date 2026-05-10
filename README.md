# 🧠 Job Intelligent --- Système d'agrégation & recommandation d'offres

**Job Intelligent** est une plateforme complète pour : - collecter
automatiquement des offres d'emploi depuis plusieurs sources web -
stocker les données dans une base de données - exposer une API backend -
afficher une interface web frontend - automatiser des tâches via Apache
Airflow

Ce projet permet à un utilisateur de centraliser et visualiser des
opportunités d'emploi en un seul endroit.

## 📌 Fonctionnalités principales

### Scraping d'offres d'emploi

Le module `scraper` : - collecte automatiquement les offres depuis
plusieurs sites - nettoie et normalise les données (titre, entreprise,
localisation, description, date...)

### Ingestion & préparation des données

Le dossier `importer` contient des scripts pour : - importer des
datasets d'offres - transformer et filtrer les données avant insertion

### Backend API

Le backend (dossier `backend`) permet : - la recherche par mots-clés,
localisation, entreprise - la pagination - le filtrage dynamique -
l'exposition des données pour le frontend

### Frontend

Le dossier `frontend` contient une interface web permettant : -
d'afficher les offres - d'effectuer des recherches - de consulter les
détails d'une offre

### Airflow (Automatisation)

Le dossier `airflow` contient les DAGs pour : - planifier le scraping -
importer automatiquement les données - synchroniser la base

### Base de données PostgreSQL

Le projet utilise PostgreSQL pour : - stocker toutes les offres -
faciliter la recherche rapide - construire des statistiques

## 🧱 Architecture du projet

    Job Intelligent Architecture

      Data Simulators / Scrapers
                |
                v
            Importer ETL
                |
                v
       PostgreSQL Database
                |
                v
           Backend API
                |
                v
           Frontend Web

    Automation:
           Airflow Scheduler

## 🧰 Stack Technique

  Composant         Technologies
  ----------------- ----------------------------------
  Scraping          Python (Requests, BeautifulSoup)
  Backend           FastAPI / Flask
  Orchestration     Apache Airflow
  Base de données   PostgreSQL
  Frontend          HTML, CSS, JS
  Infrastructure    Docker / Docker Compose

## 🚀 Installation

### 1. Cloner le projet

``` bash
git clone https://github.com/soumyahube/job_intelligent
cd job_intelligent
```

### 2. Copier le fichier `.env`

``` bash
cp .env.example .env
```

### 3. Lancer les services

``` bash
docker compose up -d
```

### 4. Lancer le scraping

``` bash
python scraper/main.py
```

### 5. Accéder aux interfaces

-   Frontend : http://localhost:3000\
-   Backend API : http://localhost:8000/api\
-   Airflow : http://localhost:8080

## Exemples API

### Recherche par mot‑clé

``` bash
curl http://localhost:8000/api/jobs?query=python
```

### Filtrer par localisation

``` bash
curl http://localhost:8000/api/jobs?location=Casablanca
```

## TODO

-   Ajouter un système de recommandation intelligent
-   Ajouter un scoring des offres
-   Intégrer une authentification utilisateur
-   Ajouter tests unitaires & CI/CD
-   Intégrer Redis pour le caching


