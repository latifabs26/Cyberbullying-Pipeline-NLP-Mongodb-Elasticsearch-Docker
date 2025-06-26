import pandas as pd
import pymongo
from pymongo import MongoClient
import logging
from datetime import datetime
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fixe la graine pour des résultats reproductibles avec langdetect
DetectorFactory.seed = 0

class NLPProcessor:
    """Classe pour le traitement NLP des textes"""
    
    def __init__(self):
        self.vader_analyzer = SentimentIntensityAnalyzer()
        logger.info("NLP Processor initialisé")
    
    def detect_language(self, text):
        """Détecte la langue du texte"""
        if not text or len(text.strip()) < 3:
            return {
                'language': 'unknown',
                'confidence': 0.0
            }
        
        try:
            # Nettoie le texte pour améliorer la détection
            clean_text = re.sub(r'[^a-zA-Z\s]', '', str(text))
            if len(clean_text.strip()) < 3:
                return {
                    'language': 'unknown',
                    'confidence': 0.0
                }
            

            # Détection de la langue
            detected_lang = detect(clean_text)
            
            # Estimation de la confiance basée sur la longueur du texte
            confidence = min(0.95, max(0.5, len(clean_text.split()) * 0.1))
            
            return {
                'language': detected_lang,
                'confidence': round(confidence, 2)
            }
            
        except LangDetectException as e:
            logger.warning(f"Erreur détection langue: {e}")
            return {
                'language': 'unknown',
                'confidence': 0.0
            }
        except Exception as e:
            logger.error(f"Erreur inattendue détection langue: {e}")
            return {
                'language': 'unknown',
                'confidence': 0.0
            }
    
    def analyze_sentiment_textblob(self, text):
        """Analyse le sentiment avec TextBlob"""
        if not text or len(text.strip()) == 0:
            return {
                'polarity': 0.0,
                'subjectivity': 0.0,
                'sentiment_label': 'neutral'
            }
        
        try:
            blob = TextBlob(str(text))
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # Classification du sentiment
            if polarity > 0.1:
                sentiment_label = 'positive'
            elif polarity < -0.1:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'
            
            return {
                'polarity': round(polarity, 3),
                'subjectivity': round(subjectivity, 3),
                'sentiment_label': sentiment_label
            }
            
        except Exception as e:
            logger.error(f"Erreur analyse sentiment TextBlob: {e}")
            return {
                'polarity': 0.0,
                'subjectivity': 0.0,
                'sentiment_label': 'neutral'
            }
    
    def analyze_sentiment_vader(self, text):
        """Analyse le sentiment avec VADER"""
        if not text or len(text.strip()) == 0:
            return {
                'compound': 0.0,
                'positive': 0.0,
                'neutral': 1.0,
                'negative': 0.0,
                'sentiment_label': 'neutral'
            }
        
        try:
            scores = self.vader_analyzer.polarity_scores(str(text))
            
            # Classification basée sur le score compound
            compound = scores['compound']
            if compound >= 0.05:
                sentiment_label = 'positive'
            elif compound <= -0.05:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'
            
            return {
                'compound': round(compound, 3),
                'positive': round(scores['pos'], 3),
                'neutral': round(scores['neu'], 3),
                'negative': round(scores['neg'], 3),
                'sentiment_label': sentiment_label
            }
            
        except Exception as e:
            logger.error(f"Erreur analyse sentiment VADER: {e}")
            return {
                'compound': 0.0,
                'positive': 0.0,
                'neutral': 1.0,
                'negative': 0.0,
                'sentiment_label': 'neutral'
            }
    
    def get_sentiment_consensus(self, textblob_result, vader_result):
        """Obtient un consensus entre TextBlob et VADER"""
        textblob_label = textblob_result['sentiment_label']
        vader_label = vader_result['sentiment_label']
        
        # Si les deux sont d'accord
        if textblob_label == vader_label:
            consensus = textblob_label
            confidence = 0.8
        else:
            # En cas de désaccord, on privilégie VADER pour les textes informels
            consensus = vader_label
            confidence = 0.6
        
        # Score final basé sur la moyenne pondérée
        final_score = (textblob_result['polarity'] + vader_result['compound']) / 2
        
        return {
            'final_sentiment': consensus,
            'confidence': confidence,
            'final_score': round(final_score, 3)
        }
    
    def process_text_nlp(self, text, processed_text=None):
        """Pipeline complet de traitement NLP"""
        if not text and not processed_text:
            return {
                'language_detection': {'language': 'unknown', 'confidence': 0.0},
                'textblob_sentiment': {'polarity': 0.0, 'subjectivity': 0.0, 'sentiment_label': 'neutral'},
                'vader_sentiment': {'compound': 0.0, 'positive': 0.0, 'neutral': 1.0, 'negative': 0.0, 'sentiment_label': 'neutral'},
                'sentiment_consensus': {'final_sentiment': 'neutral', 'confidence': 0.0, 'final_score': 0.0}
            }
        
        # Utilise le texte traité si disponible, sinon le texte original
        text_for_analysis = processed_text if processed_text else text
        text_for_language = text  # Utilise le texte original pour la détection de langue
        
        # Détection de langueanalyze_sentiment_
        language_result = self.detect_language(text_for_language)
        
        # Analyse de sentiment avec TextBlob
        textblob_result = self.analyze_sentiment_textblob(text_for_analysis)
        
        # Analyse de sentiment avec VADER
        vader_result = self.analyze_sentiment_vader(text_for_analysis)
        
        # Consensus des sentiments
        consensus_result = self.get_sentiment_consensus(textblob_result, vader_result)
        
        return {
            'language_detection': language_result,
            'textblob_sentiment': textblob_result,
            'vader_sentiment': vader_result,
            'sentiment_consensus': consensus_result
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

def get_documents_for_nlp(collection, batch_size=100):
    """Récupère les documents traités mais sans NLP"""
    try:
        cursor = collection.find({
            "processed": True,
            "nlp_processed": {"$ne": True}
        }).limit(batch_size)
        documents = list(cursor)
        logger.info(f"Documents récupérés pour NLP: {len(documents)}")
        return documents
    except Exception as e:
        logger.error(f"Erreur récupération documents NLP: {e}")
        return []

def update_document_with_nlp(collection, doc_id, nlp_result):
    """Met à jour un document avec les résultats NLP"""
    try:
        update_data = {
            "language_detection": nlp_result['language_detection'],
            "textblob_sentiment": nlp_result['textblob_sentiment'],
            "vader_sentiment": nlp_result['vader_sentiment'],
            "sentiment_consensus": nlp_result['sentiment_consensus'],
            "nlp_processed": True,
            "nlp_processing_date": datetime.utcnow()
        }
        
        result = collection.update_one(
            {"_id": doc_id},
            {"$set": update_data}
        )
        
        return result.modified_count > 0
    except Exception as e:
        logger.error(f"Erreur mise à jour NLP document {doc_id}: {e}")
        return False

def process_documents_nlp(collection, nlp_processor):
    """Traite tous les documents avec NLP"""
    total_processed = 0
    batch_size = 100
    
    while True:
        # Récupère un batch de documents
        documents = get_documents_for_nlp(collection, batch_size)
        
        if not documents:
            break
        
        batch_processed = 0
        
        for doc in documents:
            try:
                # Récupère les textes
                original_text = doc.get('original_text', '')
                processed_text = doc.get('processed_text', '')
                
                # Traitement NLP
                nlp_result = nlp_processor.process_text_nlp(original_text, processed_text)
                
                # Mise à jour du document
                if update_document_with_nlp(collection, doc['_id'], nlp_result):
                    batch_processed += 1
                    
                    # Log de progression
                    if batch_processed % 25 == 0:
                        logger.info(f"NLP traités dans ce batch: {batch_processed}")
                        
            except Exception as e:
                logger.error(f"Erreur traitement NLP document {doc.get('_id')}: {e}")
                continue
        
        total_processed += batch_processed
        logger.info(f"Batch NLP terminé: {batch_processed} documents traités")
        
        # Si le batch n'est pas complet, on a terminé
        if len(documents) < batch_size:
            break
    
    logger.info(f"Traitement NLP terminé: {total_processed} documents traités au total")
    return total_processed

def verify_nlp_processing(collection):
    """Vérifie les résultats du traitement NLP"""
    try:
        total = collection.count_documents({"processed": True})
        nlp_processed = collection.count_documents({"nlp_processed": True})
        
        print(f"\n=== STATISTIQUES DU TRAITEMENT NLP ===")
        print(f"Total documents prétraités: {total}")
        print(f"Documents avec NLP: {nlp_processed}")
        print(f"Documents sans NLP: {total - nlp_processed}")
        
        # Statistiques sur les langues
        pipeline_lang = [
            {"$match": {"nlp_processed": True}},
            {"$group": {
                "_id": "$language_detection.language",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        lang_stats = list(collection.aggregate(pipeline_lang))
        print(f"\n=== RÉPARTITION DES LANGUES ===")
        for stat in lang_stats[:10]:  # Top 10
            print(f"{stat['_id']}: {stat['count']} documents")
        
        # Statistiques sur les sentiments
        pipeline_sentiment = [
            {"$match": {"nlp_processed": True}},
            {"$group": {
                "_id": "$sentiment_consensus.final_sentiment",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        sentiment_stats = list(collection.aggregate(pipeline_sentiment))
        print(f"\n=== RÉPARTITION DES SENTIMENTS ===")
        for stat in sentiment_stats:
            print(f"{stat['_id']}: {stat['count']} documents")
        
        # Échantillons de résultats NLP
        print(f"\n=== ÉCHANTILLONS NLP ===")
        samples = collection.find({"nlp_processed": True}).limit(3)
        
        for i, doc in enumerate(samples, 1):
            print(f"\nÉchantillon {i}:")
            print(f"Label: {doc.get('label')}")
            print(f"Texte: {doc.get('original_text', '')[:100]}...")
            print(f"Langue: {doc.get('language_detection', {}).get('language', 'N/A')}")
            print(f"Confiance langue: {doc.get('language_detection', {}).get('confidence', 'N/A')}")
            print(f"Sentiment final: {doc.get('sentiment_consensus', {}).get('final_sentiment', 'N/A')}")
            print(f"Score sentiment: {doc.get('sentiment_consensus', {}).get('final_score', 'N/A')}")
            print(f"TextBlob: {doc.get('textblob_sentiment', {}).get('sentiment_label', 'N/A')}")
            print(f"VADER: {doc.get('vader_sentiment', {}).get('sentiment_label', 'N/A')}")
            
    except Exception as e:
        logger.error(f"Erreur vérification NLP: {e}")

def reset_nlp_processing(collection):
    """Remet à zéro le traitement NLP"""
    try:
        result = collection.update_many(
            {},
            {"$unset": {
                "language_detection": "",
                "textblob_sentiment": "",
                "vader_sentiment": "",
                "sentiment_consensus": "",
                "nlp_processing_date": ""
            },
            "$set": {"nlp_processed": False}}
        )
        logger.info(f"Traitement NLP remis à zéro pour {result.modified_count} documents")
        return result.modified_count
    except Exception as e:
        logger.error(f"Erreur reset NLP: {e}")
        return 0

def main():
    """Fonction principale"""
    print("=== TRAITEMENT NLP DES DONNÉES ===\n")
    
    # Connexion MongoDB
    client, db, collection = connect_mongodb()
    if collection is None:
        print("❌ Impossible de se connecter à MongoDB")
        return
    
    try:
        # Vérification des prérequis
        total_docs = collection.count_documents({})
        processed_docs = collection.count_documents({"processed": True})
        
        print(f"📊 Documents total: {total_docs}")
        print(f"📊 Documents prétraités: {processed_docs}")
        
        if processed_docs == 0:
            print("❌ Aucun document prétraité trouvé. Exécutez d'abord preprocessing.py")
            return
        
        # Initialisation du processeur NLP
        print("🔧 Initialisation du processeur NLP...")
        nlp_processor = NLPProcessor()
        
        # Option pour reset (utile en développement)
        reset_choice = input("Voulez-vous remettre à zéro le traitement NLP? (y/N): ")
        if reset_choice.lower() == 'y':
            print("🔄 Remise à zéro du traitement NLP...")
            reset_nlp_processing(collection)
        
        # Traitement NLP des documents
        print("\n🤖 Début du traitement NLP...")
        processed_count = process_documents_nlp(collection, nlp_processor)
        
        if processed_count > 0:
            print(f"✅ Traitement NLP terminé: {processed_count} documents traités")
        else:
            print("ℹ️ Aucun document à traiter (tous déjà traités)")
        
        # Vérification des résultats
        print("\n🔍 Vérification des résultats NLP:")
        verify_nlp_processing(collection)
        
        print("\n✅ TRAITEMENT NLP TERMINÉ AVEC SUCCÈS!")
        
    except Exception as e:
        print(f"❌ Erreur générale: {e}")
        logger.error(f"Erreur générale: {e}")
    
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()