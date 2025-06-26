import pandas as pd
import pymongo
from pymongo import MongoClient
import re
import string
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from bs4 import BeautifulSoup
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TextPreprocessor:
    """Classe pour le prétraitement des textes"""
    
    def __init__(self):
        self.setup_nltk()
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words('english'))
        
    def setup_nltk(self):
        """Télécharge les ressources NLTK nécessaires"""
        required_resources = [
            ('tokenizers/punkt', 'punkt'),
            ('tokenizers/punkt_tab', 'punkt_tab'),
            ('corpora/stopwords', 'stopwords'),
            ('corpora/wordnet', 'wordnet'),
            ('taggers/averaged_perceptron_tagger', 'averaged_perceptron_tagger'),
            ('corpora/omw-1.4', 'omw-1.4')
        ]
        
        for resource_path, resource_name in required_resources:
            try:
                nltk.data.find(resource_path)
            except LookupError:
                logger.info(f"Téléchargement de {resource_name}...")
                try:
                    nltk.download(resource_name, quiet=True)
                except Exception as e:
                    logger.warning(f"Impossible de télécharger {resource_name}: {e}")
        
        logger.info("Configuration NLTK terminée")
    
    def remove_html_tags(self, text):
        """Supprime les balises HTML"""
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text()
    
    def remove_urls(self, text):
        """Supprime les URLs"""
        url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        return url_pattern.sub('', text)
    
    def remove_special_chars(self, text):
        """Supprime les caractères spéciaux mais garde les espaces"""
        # Garde seulement les lettres, chiffres et espaces
        return re.sub(r'[^a-zA-Z0-9\s]', '', text)
    
    def remove_punctuation_and_digits(self, text):
        """Supprime la ponctuation et les chiffres"""
        # Supprime la ponctuation
        text = text.translate(str.maketrans('', '', string.punctuation))
        # Supprime les chiffres
        text = re.sub(r'\d+', '', text)
        return text
    
    def remove_stopwords(self, tokens):
        """Supprime les mots vides"""
        return [token for token in tokens if token.lower() not in self.stop_words]
    
    def lemmatize_tokens(self, tokens):
        """Applique la lemmatisation"""
        return [self.lemmatizer.lemmatize(token.lower()) for token in tokens]
    
    def preprocess_text(self, text):
        """Pipeline complet de prétraitement"""
        if not text or pd.isna(text):
            return {
                'original_text': '',
                'cleaned_text': '',
                'tokens': [],
                'processed_text': '',
                'word_count': 0
            }
        
        # Étape 1: Conversion en string et minuscules
        text = str(text).lower()
        original_text = text
        
        # Étape 2: Suppression des balises HTML
        text = self.remove_html_tags(text)
        
        # Étape 3: Suppression des URLs
        text = self.remove_urls(text)
        
        # Étape 4: Suppression des caractères spéciaux
        text = self.remove_special_chars(text)
        
        # Étape 5: Suppression de la ponctuation et des chiffres
        text = self.remove_punctuation_and_digits(text)
        
        # Étape 6: Suppression des espaces multiples
        text = re.sub(r'\s+', ' ', text).strip()
        
        cleaned_text = text
        
        # Étape 7: Tokenisation
        tokens = word_tokenize(text)
        
        # Étape 8: Suppression des mots vides
        tokens = self.remove_stopwords(tokens)
        
        # Étape 9: Lemmatisation
        tokens = self.lemmatize_tokens(tokens)
        
        # Étape 10: Suppression des tokens vides
        tokens = [token for token in tokens if token.strip() and len(token) > 1]
        
        # Reconstitution du texte traité
        processed_text = ' '.join(tokens)
        
        return {
            'original_text': original_text,
            'cleaned_text': cleaned_text,
            'tokens': tokens,
            'processed_text': processed_text,
            'word_count': len(tokens)
        }

def connect_mongodb():
    """Connexion à MongoDB"""
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["harcelement"]
        collection = db["posts"]
        logger.info("Connexion MongoDB établie")
        return client, db, collection
    except Exception as e:
        logger.error(f"Erreur MongoDB: {e}")
        return None, None, None

def get_documents_to_process(collection, batch_size=100):
    """Récupère les documents à traiter par batch"""
    try:
        cursor = collection.find({"processed": False}).limit(batch_size)
        documents = list(cursor)
        logger.info(f"Documents récupérés pour traitement: {len(documents)}")
        return documents
    except Exception as e:
        logger.error(f"Erreur récupération documents: {e}")
        return []

def update_document_with_preprocessing(collection, doc_id, preprocessing_result):
    """Met à jour un document avec les résultats du prétraitement"""
    try:
        update_data = {
            "original_text": preprocessing_result['original_text'],
            "cleaned_text": preprocessing_result['cleaned_text'],
            "tokens": preprocessing_result['tokens'],
            "processed_text": preprocessing_result['processed_text'],
            "word_count": preprocessing_result['word_count'],
            "processed": True,
            "preprocessing_date": datetime.utcnow()
        }
        
        result = collection.update_one(
            {"_id": doc_id},
            {"$set": update_data}
        )
        
        return result.modified_count > 0
    except Exception as e:
        logger.error(f"Erreur mise à jour document {doc_id}: {e}")
        return False

def process_documents(collection, preprocessor):
    """Traite tous les documents non traités"""
    total_processed = 0
    batch_size = 100
    
    while True:
        # Récupère un batch de documents
        documents = get_documents_to_process(collection, batch_size)
        
        if not documents:
            break
        
        batch_processed = 0
        
        for doc in documents:
            try:
                # Prétraitement du texte
                text_field = doc.get('text', '')
                preprocessing_result = preprocessor.preprocess_text(text_field)
                
                # Mise à jour du document
                if update_document_with_preprocessing(collection, doc['_id'], preprocessing_result):
                    batch_processed += 1
                    
                    # Log de progression
                    if batch_processed % 50 == 0:
                        logger.info(f"Traités dans ce batch: {batch_processed}")
                        
            except Exception as e:
                logger.error(f"Erreur traitement document {doc.get('_id')}: {e}")
                continue
        
        total_processed += batch_processed
        logger.info(f"Batch terminé: {batch_processed} documents traités")
        
        # Si le batch n'est pas complet, on a terminé
        if len(documents) < batch_size:
            break
    
    logger.info(f"Prétraitement terminé: {total_processed} documents traités au total")
    return total_processed

def verify_preprocessing(collection):
    """Vérifie les résultats du prétraitement"""
    try:
        total = collection.count_documents({})
        processed = collection.count_documents({"processed": True})
        
        print(f"\n=== STATISTIQUES DU PRÉTRAITEMENT ===")
        print(f"Total documents: {total}")
        print(f"Documents traités: {processed}")
        print(f"Documents non traités: {total - processed}")
        
        # Statistiques sur les mots
        pipeline = [
            {"$match": {"processed": True}},
            {"$group": {
                "_id": None,
                "avg_word_count": {"$avg": "$word_count"},
                "min_word_count": {"$min": "$word_count"},
                "max_word_count": {"$max": "$word_count"}
            }}
        ]
        
        stats = list(collection.aggregate(pipeline))
        if stats:
            stat = stats[0]
            print(f"\n=== STATISTIQUES DES MOTS ===")
            print(f"Nombre moyen de mots: {stat['avg_word_count']:.2f}")
            print(f"Minimum de mots: {stat['min_word_count']}")
            print(f"Maximum de mots: {stat['max_word_count']}")
        
        # Échantillons de textes traités
        print(f"\n=== ÉCHANTILLONS ===")
        samples = collection.find({"processed": True}).limit(3)
        
        for i, doc in enumerate(samples, 1):
            print(f"\nÉchantillon {i}:")
            print(f"Label: {doc.get('label')}")
            print(f"Texte original: {doc.get('original_text', '')[:100]}...")
            print(f"Texte nettoyé: {doc.get('cleaned_text', '')[:100]}...")
            print(f"Texte traité: {doc.get('processed_text', '')[:100]}...")
            print(f"Nombre de mots: {doc.get('word_count', 0)}")
            
    except Exception as e:
        logger.error(f"Erreur vérification: {e}")

def reset_processing_status(collection):
    """Remet à zéro le statut de traitement (utile pour les tests)"""
    try:
        result = collection.update_many(
            {},
            {"$unset": {
                "original_text": "",
                "cleaned_text": "",
                "tokens": "",
                "processed_text": "",
                "word_count": "",
                "preprocessing_date": ""
            },
            "$set": {"processed": False}}
        )
        logger.info(f"Statut de traitement remis à zéro pour {result.modified_count} documents")
        return result.modified_count
    except Exception as e:
        logger.error(f"Erreur reset: {e}")
        return 0

def main():
    """Fonction principale"""
    print("=== PRÉTRAITEMENT DES DONNÉES TEXTUELLES ===\n")
    
    # Connexion MongoDB
    client, db, collection = connect_mongodb()
    if collection is None:
        print("❌ Impossible de se connecter à MongoDB")
        return
    
    try:
        # Initialisation du préprocesseur
        print("🔧 Initialisation du préprocesseur...")
        preprocessor = TextPreprocessor()
        
        # Option pour reset (utile en développement)
        reset_choice = input("Voulez-vous remettre à zéro le traitement? (y/N): ")
        if reset_choice.lower() == 'y':
            print("🔄 Remise à zéro du statut de traitement...")
            reset_processing_status(collection)
        
        # Traitement des documents
        print("\n📝 Début du prétraitement...")
        processed_count = process_documents(collection, preprocessor)
        
        if processed_count > 0:
            print(f"✅ Prétraitement terminé: {processed_count} documents traités")
        else:
            print("ℹ️ Aucun document à traiter (tous déjà traités)")
        
        # Vérification des résultats
        print("\n🔍 Vérification des résultats:")
        verify_preprocessing(collection)
        
        print("\n✅ PRÉTRAITEMENT TERMINÉ AVEC SUCCÈS!")
        
    except Exception as e:
        print(f"❌ Erreur générale: {e}")
        logger.error(f"Erreur générale: {e}")
    
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()