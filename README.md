# Projet d'Analyse du Harcèlement en Ligne

## 🎯 Objectif

Ce projet vise à analyser les données de harcèlement en ligne en utilisant une stack complète de data science : Python, MongoDB, Elasticsearch et Kibana. L'objectif est de créer un pipeline complet de traitement de données textuelles, depuis l'ingestion jusqu'à la visualisation.

## 🏗️ Architecture

```
Dataset CSV → MongoDB → Prétraitement → NLP → Elasticsearch → Kibana
```

### Technologies utilisées

- **Python** : Traitement des données, NLP, ingestion
- **MongoDB** : Base de données NoSQL pour le stockage initial
- **Elasticsearch** : Moteur de recherche et d'indexation
- **Kibana** : Visualisation et tableaux de bord
- **Docker** : Containerisation de l'environnement

## 📋 Prérequis

- Docker et Docker Compose installés
- Python 3.8+ (si exécution en local)
- Au moins 4GB de RAM disponible pour Elasticsearch

## 🚀 Installation et Configuration

### 1. Cloner le projet et préparer l'environnement

```bash
git clone <votre-repo>
cd cyberbullying-analysis
```

### 2. Lancer l'infrastructure avec Docker Compose

```bash
# Lancer tous les services
docker-compose up -d
#Or
make up

# Vérifier que tous les services sont opérationnels
docker-compose ps
```

### 3. Accès aux services

- **MongoDB** : `localhost:27017`
- **Mongo Express** : `http://localhost:8081` (admin/admin)
- **Elasticsearch** : `http://localhost:9200`
- **Kibana** : `http://localhost:5601`

### 4. Installation des dépendances Python

```bash
# Créer un environnement virtuel
venv\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt
```

## 📊 Dataset

### Source
- **Dataset principal** : [Cyberbullying and Harassment Detection](https://www.kaggle.com/datasets/saifulislam7/cyberbullying-and-harassmentdetection-using-ml
)
- **Format** : CSV
- **Contenu** : Messages textuels avec labels de classification

### Préparation
1. Télécharger le dataset depuis Kaggle
2. Renommer le fichier en `bullying.csv`
3. Placer le fichier dans le répertoire racine du projet

## 🔄 Pipeline de Traitement

### Étape 1 : Ingestion des données (data_loader.py)

```bash
python data_loader.py
#Or
make ingest 
```

**Fonctionnalités :**
- Lecture du fichier CSV avec pandas
- Détection automatique des colonnes (text, label, type)
- Nettoyage des données vides
- Insertion dans MongoDB avec métadonnées :
  - `id_post` : Identifiant unique UUID
  - `text` : Contenu du message
  - `label` : Classification (harcèlement ou non)
  - `type` : Type de harcèlement
  - `created_at` : Timestamp d'insertion
  - `source` : Source des données

### Étape 2 : Prétraitement (preprocessing.py)

```bash
python preprocessing.py
#Or
make preprocess
```

**Transformations appliquées :**
- Conversion en minuscules
- Suppression des balises HTML
- Suppression des URLs
- Suppression des caractères spéciaux et ponctuation
- Suppression des chiffres
- Suppression des mots vides (stopwords)
- Lemmatisation avec NLTK
- Tokenisation

**Champs ajoutés :**
- `original_text` : Texte original
- `cleaned_text` : Texte nettoyé
- `tokens` : Liste des tokens
- `processed_text` : Texte final traité
- `word_count` : Nombre de mots

### Étape 3 : Traitement NLP (nlp_pipeline.py)

```bash
python nlp_pipeline.py
#or
make nlp
```

**Analyses effectuées :**
- **Détection de langue** : Utilisation de `langdetect`
- **Analyse de sentiment** : Double approche avec TextBlob et VADER
- **Consensus de sentiment** : Algorithme de fusion des résultats

**Champs ajoutés :**
- `language_detection` : Langue détectée et niveau de confiance
- `textblob_sentiment` : Résultats TextBlob (polarité, subjectivité)
- `vader_sentiment` : Résultats VADER (scores détaillés)
- `sentiment_consensus` : Sentiment final et score de confiance

### Étape 4 : Indexation Elasticsearch (es_ingest.py)

```bash
python es_ingest.py
#Or
make index
```
### Run the complete Pipeline 
```bash
make all
```

**Fonctionnalités :**
- Création de l'index `harcelement_posts`
- Mapping optimisé pour la recherche et l'analyse
- Transfert des données enrichies depuis MongoDB
- Gestion des erreurs et reprise en cas d'échec

### Étape 5 : Visualisation Kibana

Accéder à Kibana via `http://localhost:5601` et importer les dashboards pré-configurés.

## 📁 Structure du Projet

```
cyberbullying-analysis/
├── data_loader.py              # Étape 1: Ingestion des données
├── preprocessing.py            # Étape 2: Prétraitement des textes
├── nlp_pipeline.py            # Étape 3: Traitement NLP
├── es_ingest.py               # Étape 4: Indexation Elasticsearch
├── tests/                     # Tests unitaires
│   ├── test_data_loader.py
│   ├── test_preprocessing.py
│   └── test_nlp_pipeline.py
├── docker-compose.yml         # Configuration Docker
├── Dockerfile                 # Image Python personnalisée
├── requirements.txt           # Dépendances Python
├── README.md                  # Documentation
├── Makefile                   # Makefile pour automatiser les commandes Docker et scripts Python
└── bullying.csv              # Dataset (à télécharger)
```

## 🧪 Tests

### Exécution des tests unitaires

```bash
# Tous les tests
python -m pytest tests/ -v

# Tests spécifiques
python -m pytest tests/test_data_loader.py -v
python -m pytest tests/test_preprocessing.py -v
python -m pytest tests/test_nlp_pipeline.py -v
```

### Tests de couverture

```bash
python -m pytest tests/ --cov=. --cov-report=html
```

## 📊 Visualisations Kibana

### Dashboards disponibles

1. **🗣️ Répartition des Langues**
   - Type : Pie chart
   - But : Montrer les langues les plus fréquentes dans les publications
   - Exemple : en, fr, other

2. **🚨 Types de Harcèlement**
   - Type : Diagramme en barres
   - But : Identifier les types de harcèlement les plus courants

3. **😊 Analyse des Sentiments**
   - Type : Donut
   - But : Visualiser la distribution des sentiments (positive, neutral, negative)

4. **📉 Posts Négatifs** 
   - Type : Métrique simple
   - But : Afficher le nombre total de publications classées comme négatives

5. **🔥 Langue x Sentiment** 
   - Type : Heatmap
   - But : Analyser visuellement l’intensité du harcèlement selon la langue et le sentiment
#

## 🔧 Configuration avancée

### Variables d'environnement

```bash
# MongoDB
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=harcelement

# Elasticsearch
ES_HOST=localhost
ES_PORT=9200
ES_INDEX=harcelement_posts

# Logging
LOG_LEVEL=INFO
```

## 🚨 Dépannage

### Problèmes courants

1. **Elasticsearch ne démarre pas**
   ```bash
   # Vérifier les logs
   docker-compose logs elasticsearch
   
   
   ```

2. **Erreur de connexion MongoDB**
   ```bash
   # Vérifier le status
   docker-compose ps mongo
   
   # Redémarrer le service
   docker-compose restart mongo
   ```

3. **Problèmes de performance**
   - Augmenter la RAM allouée à Elasticsearch
   - Réduire la taille des batches de traitement
### Logs et monitoring

```bash
# Voir les logs en temps réel
docker-compose logs -f

# Logs spécifiques
docker-compose logs elasticsearch
docker-compose logs kibana
#OR
# View logs
make logs

# Restart services
make down
make up

# Clean everything and restart
make clean
make build
make up
```
## 🛠️ Available Make Commands

```bash
make build       # Build Docker images
make up          # Start all services
make down        # Stop all services
make logs        # View logs
make shell       # Access app container
make ingest      # Run data ingestion
make preprocess  # Run preprocessing
make nlp         # Run NLP pipeline
make index       # Index to Elasticsearch
make all         # Run complete pipeline
make clean       # Remove all containers and data
```
## 📈 Métriques de performance

### Benchmarks 

- **Ingestion** : >1000 documents/seconde
- **Prétraitement** : <500 documents/seconde
- **NLP** : <100 documents/seconde
- **Indexation ES** : ~2000 documents/seconde

### Optimisations possibles

1. **Parallélisation** : Utilisation de multiprocessing
2. **Batch processing** : Traitement par lots plus importants
3. **Caching** : Mise en cache des résultats NLP
4. **Index optimization** : Tuning des paramètres Elasticsearch


## 📚 Ressources

### Documentation officielle

- [MongoDB Python Driver](https://pymongo.readthedocs.io/)
- [Elasticsearch Python Client](https://elasticsearch-py.readthedocs.io/)
- [Kibana Guide](https://www.elastic.co/guide/en/kibana/current/index.html)
- [NLTK Documentation](https://www.nltk.org/)

### Ressources supplémentaires

- [Guide NLP avec Python](https://www.nltk.org/book/)
- [Elasticsearch Best Practices](https://www.elastic.co/guide/en/elasticsearch/reference/current/general-recommendations.html)
- [MongoDB Schema Design](https://docs.mongodb.com/manual/data-modeling/)


## 👥 Auteur

- **Ben Slama Latifa** - Développement initial
