# 1. Image Python légère
FROM python:3.8-slim

# 2. Répertoire de travail
WORKDIR /app

# 3. Variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# 4. Installation des dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 5. Copier requirements.txt pour le cache Docker
COPY requirements.txt .

# 6. Installer les dépendances Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 7. Télécharger les données NLTK nécessaires
RUN python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('vader_lexicon')"

# 8. Copier le code de l'application
COPY . .

# 9. Créer un dossier logs si nécessaire
RUN mkdir -p logs

# 10. Commande par défaut (modifiable via docker-compose)
CMD ["python", "es_ingest.py"]
