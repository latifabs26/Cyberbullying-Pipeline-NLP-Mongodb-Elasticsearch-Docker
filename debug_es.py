import pymongo
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import json
from datetime import datetime
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_mongodb():
    """Connexion à MongoDB"""
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["harcelement"]
        collection = db["posts"]
        return client, db, collection
    except Exception as e:
        print(f"❌ Erreur connexion MongoDB: {e}")
        return None, None, None

def debug_mongodb_documents():
    """Debug des documents MongoDB"""
    client, db, collection = connect_mongodb()
    if collection is None:
        return
    
    try:
        # Statistiques de base
        total = collection.count_documents({})
        processed = collection.count_documents({"processed": True})
        nlp_processed = collection.count_documents({"nlp_processed": True})
        
        print(f"=== DEBUG MONGODB ===")
        print(f"Total documents: {total}")
        print(f"Documents processed: {processed}")
        print(f"Documents NLP processed: {nlp_processed}")
        
        if nlp_processed == 0:
            print("❌ Aucun document avec NLP trouvé!")
            return
        
        # Examine un document d'exemple
        sample_doc = collection.find_one({"nlp_processed": True})
        
        print(f"\n=== STRUCTURE D'UN DOCUMENT ===")
        print(f"ID: {sample_doc.get('_id')}")
        print(f"Keys: {list(sample_doc.keys())}")
        
        # Vérifie les champs critiques
        print(f"\n=== CHAMPS CRITIQUES ===")
        print(f"original_text: {type(sample_doc.get('original_text'))} - {len(str(sample_doc.get('original_text', ''))) if sample_doc.get('original_text') else 0} caractères")
        print(f"processed_text: {type(sample_doc.get('processed_text'))} - {len(str(sample_doc.get('processed_text', ''))) if sample_doc.get('processed_text') else 0} caractères")
        print(f"label: {sample_doc.get('label')} ({type(sample_doc.get('label'))})")
        print(f"type: {sample_doc.get('type')} ({type(sample_doc.get('type'))})")
        
        # Vérifie les données NLP
        print(f"\n=== DONNÉES NLP ===")
        lang_data = sample_doc.get('language_detection', {})
        print(f"language_detection: {lang_data}")
        
        sentiment_data = sample_doc.get('sentiment_consensus', {})
        print(f"sentiment_consensus: {sentiment_data}")
        
        textblob_data = sample_doc.get('textblob_sentiment', {})
        print(f"textblob_sentiment: {textblob_data}")
        
        vader_data = sample_doc.get('vader_sentiment', {})
        print(f"vader_sentiment: {vader_data}")
        
        # Test de transformation
        print(f"\n=== TEST DE TRANSFORMATION ===")
        try:
            from elasticSearch_ingest import ElasticsearchIngestor
            ingestor = ElasticsearchIngestor()
            transformed = ingestor.transform_document(sample_doc)
            
            if transformed:
                print("✅ Transformation réussie")
                print(f"Champs transformés: {list(transformed.keys())}")
                
                # Vérifie les types de données
                print(f"\n=== TYPES DE DONNÉES TRANSFORMÉES ===")
                for key, value in transformed.items():
                    print(f"{key}: {type(value)} = {value}")
                    
            else:
                print("❌ Échec de la transformation")
                
        except Exception as e:
            print(f"❌ Erreur lors de la transformation: {e}")
            
    except Exception as e:
        print(f"❌ Erreur debug MongoDB: {e}")
    
    finally:
        if client:
            client.close()

def debug_elasticsearch():
    """Debug Elasticsearch"""
    try:
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        
        print(f"\n=== DEBUG ELASTICSEARCH ===")
        
        # Test de connexion
        if es.ping():
            print("✅ Connexion Elasticsearch OK")
        else:
            print("❌ Connexion Elasticsearch échoue")
            return
        
        # Info cluster
        info = es.info()
        print(f"Version ES: {info['version']['number']}")
        
        # Vérifie l'index
        index_name = "harcelement_posts"
        if es.indices.exists(index=index_name):
            print(f"✅ Index {index_name} existe")
            
            # Stats de l'index
            stats = es.indices.stats(index=index_name)
            count = es.count(index=index_name)
            print(f"Documents dans l'index: {count['count']}")
            
            # Mapping de l'index
            mapping = es.indices.get_mapping(index=index_name)
            print(f"Mapping OK: {bool(mapping)}")
            
        else:
            print(f"❌ Index {index_name} n'existe pas")
            
    except Exception as e:
        print(f"❌ Erreur debug Elasticsearch: {e}")

def test_single_document_indexing():
    """Test l'indexation d'un seul document"""
    client, db, collection = connect_mongodb()
    if collection is None:
        return
    
    try:
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        
        # Récupère un document
        sample_doc = collection.find_one({"nlp_processed": True})
        if not sample_doc:
            print("❌ Aucun document NLP trouvé")
            return
        
        print(f"\n=== TEST INDEXATION DOCUMENT UNIQUE ===")
        
        # Transforme le document
        from elasticSearch_ingest import ElasticsearchIngestor
        ingestor = ElasticsearchIngestor()
        es_doc = ingestor.transform_document(sample_doc)
        
        if not es_doc:
            print("❌ Échec transformation")
            return
        
        print(f"✅ Document transformé: {es_doc['id_post']}")
        
        # Tentative d'indexation
        try:
            response = es.index(
                index="test_harcelement",
                id=es_doc['id_post'],
                document=es_doc
            )
            print(f"✅ Indexation réussie: {response['result']}")
            
            # Supprime l'index de test
            es.indices.delete(index="test_harcelement", ignore=[400, 404])
            
        except Exception as e:
            print(f"❌ Erreur indexation: {e}")
            print(f"Type d'erreur: {type(e)}")
            
            # Essaie de comprendre l'erreur
            if hasattr(e, 'body'):
                print(f"Détails erreur: {e.body}")
            
    except Exception as e:
        print(f"❌ Erreur test: {e}")
    
    finally:
        if client:
            client.close()

def main():
    """Fonction principale de debug"""
    print("=== SCRIPT DE DEBUG ELASTICSEARCH ===\n")
    
    # Debug MongoDB
    debug_mongodb_documents()
    
    # Debug Elasticsearch
    debug_elasticsearch()
    
    # Test indexation simple
    test_single_document_indexing()
    
    print("\n=== DEBUG TERMINÉ ===")

if __name__ == "__main__":
    main()