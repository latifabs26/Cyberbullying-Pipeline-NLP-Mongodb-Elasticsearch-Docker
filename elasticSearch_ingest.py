from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import ConnectionError as ESConnectionError
import pymongo
from pymongo import MongoClient
import logging
from datetime import datetime, timezone  # Added timezone import
import json
from typing import Dict, List, Any
import re

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticsearchIngestor:
    """Classe pour l'ingestion des données dans Elasticsearch"""
    
    def __init__(self, es_host="localhost", es_port=9200):
        try:
            # Method 1: Most compatible connection string approach
            self.es = Elasticsearch(
                hosts=[f"http://{es_host}:{es_port}"],
                timeout=30,
                max_retries=10,
                retry_on_timeout=True
            )
            
            # Test connection with info() instead of ping()
            version_info = self.es.info()
            logger.info(f"✅ Connexion Elasticsearch établie - Version: {version_info['version']['number']}")
            logger.info(f"✅ Cluster: {version_info['cluster_name']}")
        
        except ESConnectionError as ce:
            logger.error("❌ Connexion échouée à Elasticsearch")
            logger.error("💡 Vérifiez que Elasticsearch est démarré sur localhost:9200")
            raise ce
        
        except Exception as e:
            logger.error(f"❌ Erreur connexion Elasticsearch: {e}")
            logger.error("💡 Essayez de redémarrer Elasticsearch ou vérifiez la version du client")
            raise Exception("Impossible de se connecter à Elasticsearch")
    
    def create_index_mapping(self, index_name="harcelement_posts"):
        """Crée l'index avec le mapping approprié"""
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "text_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "stop"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    # Champs de base
                    "id_post": {
                        "type": "keyword"
                    },
                    "titre": {
                        "type": "text",
                        "analyzer": "text_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "contenu": {
                        "type": "text",
                        "analyzer": "text_analyzer"
                    },
                    "contenu_original": {
                        "type": "text"
                    },
                    "contenu_traite": {
                        "type": "text",
                        "analyzer": "text_analyzer"
                    },
                    "auteur": {
                        "type": "keyword"
                    },
                    
                    "url": {
                        "type": "keyword"
                    },
                    "label": {
                        "type": "keyword"
                    },
                    "type_harcelement": {
                        "type": "keyword"
                    },
                    
                    # Champs NLP - Langue
                    "langue": {
                        "type": "keyword"
                    },
                    "langue_confiance": {
                        "type": "float"
                    },
                    
                    # Champs NLP - Sentiment
                    "sentiment": {
                        "type": "keyword"
                    },
                    "score": {
                        "type": "float"
                    },
                    "sentiment_confiance": {
                        "type": "float"
                    },
                    
                    # Détails des sentiments
                    "textblob_polarite": {
                        "type": "float"
                    },
                    "textblob_subjectivite": {
                        "type": "float"
                    },
                    "textblob_sentiment": {
                        "type": "keyword"
                    },
                    
                    "vader_compound": {
                        "type": "float"
                    },
                    "vader_positif": {
                        "type": "float"
                    },
                    "vader_neutre": {
                        "type": "float"
                    },
                    "vader_negatif": {
                        "type": "float"
                    },
                    "vader_sentiment": {
                        "type": "keyword"
                    },
                    
                    
                    
                    
                    # Champs calculés
                    "longueur_texte": {
                        "type": "integer"
                    },
                    "nb_mots": {
                        "type": "integer"
                    }
                }
            }
        }
        
        try:
            # Supprime l'index s'il existe déjà
            if self.es.indices.exists(index=index_name):
                logger.info(f"🗑️ Suppression de l'index existant: {index_name}")
                self.es.indices.delete(index=index_name)
            
            # Crée le nouvel index - Fixed for newer ES versions
            try:
                response = self.es.indices.create(index=index_name, **mapping)
                logger.info(f"✅ Index créé: {index_name}")
                return True
            except Exception:
                # Fallback for older versions
                response = self.es.indices.create(index=index_name, body=mapping)
                logger.info(f"✅ Index créé (méthode alternative): {index_name}")
                return True
            
        except Exception as e:
            logger.error(f"❌ Erreur création index: {e}")
            return False
    
    def safe_datetime_format(self, date_value):
        """Safely format datetime values for Elasticsearch"""
        if not date_value:
            return datetime.now(timezone.utc).isoformat()
        
        if isinstance(date_value, str):
            # If it's already a string, validate it's in ISO format
            try:
                # Try to parse it to validate
                datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                return date_value
            except ValueError:
                # If parsing fails, return current time
                logger.warning(f"Invalid date format: {date_value}, using current time")
                return datetime.now(timezone.utc).isoformat()
        
        if isinstance(date_value, datetime):
            # If it's a datetime object, ensure it's timezone-aware
            if date_value.tzinfo is None:
                date_value = date_value.replace(tzinfo=timezone.utc)
            return date_value.isoformat()
        
        # For any other type, return current time
        logger.warning(f"Unexpected date type: {type(date_value)}, using current time")
        return datetime.now(timezone.utc).isoformat()
    
    def transform_document(self, mongo_doc: Dict) -> Dict:
        """Transforme un document MongoDB pour Elasticsearch"""
        try:
            # Document de base
            es_doc = {
                "id_post": str(mongo_doc.get('_id')),
                #"date_indexation": datetime.now(timezone.utc).replace(microsecond=0).isoformat()  # ✅ Fixé pour Elasticsearch
            }
            
            # Champs de base avec validation
            original_text = mongo_doc.get('original_text', '')
            processed_text = mongo_doc.get('processed_text', '')
            
            # Ensure text fields are strings
            es_doc["contenu_original"] = str(original_text) if original_text else ''
            es_doc["contenu_traite"] = str(processed_text) if processed_text else ''
            es_doc["contenu"] = es_doc["contenu_traite"] or es_doc["contenu_original"]
            
            # Create title from content
            title_text = es_doc["contenu_original"] or es_doc["contenu"]
            if len(title_text) > 100:
                es_doc["titre"] = title_text[:100] + "..."
            else:
                es_doc["titre"] = title_text
            
            # Ensure required fields are strings
            es_doc["label"] = str(mongo_doc.get('label', 'unknown'))
            es_doc["type_harcelement"] = str(mongo_doc.get('type', 'unknown'))
            es_doc["auteur"] = str(mongo_doc.get('author', 'anonymous'))
            es_doc["url"] = str(mongo_doc.get('url', ''))
            
            # Handle dates safely
            #es_doc["date"] = self.safe_datetime_format(mongo_doc.get('date'))
            
            # Champs de langue avec validation
            lang_data = mongo_doc.get('language_detection', {})
            es_doc["langue"] = str(lang_data.get('language', 'unknown'))
            es_doc["langue_confiance"] = float(lang_data.get('confidence', 0.0))
            
            # Champs de sentiment principal avec validation
            sentiment_consensus = mongo_doc.get('sentiment_consensus', {})
            es_doc["sentiment"] = str(sentiment_consensus.get('final_sentiment', 'neutral'))
            es_doc["score"] = float(sentiment_consensus.get('final_score', 0.0))
            es_doc["sentiment_confiance"] = float(sentiment_consensus.get('confidence', 0.0))
            
            # Détails TextBlob avec validation
            textblob_data = mongo_doc.get('textblob_sentiment', {})
            es_doc["textblob_polarite"] = float(textblob_data.get('polarity', 0.0))
            es_doc["textblob_subjectivite"] = float(textblob_data.get('subjectivity', 0.0))
            es_doc["textblob_sentiment"] = str(textblob_data.get('sentiment_label', 'neutral'))
            
            # Détails VADER avec validation
            vader_data = mongo_doc.get('vader_sentiment', {})
            es_doc["vader_compound"] = float(vader_data.get('compound', 0.0))
            es_doc["vader_positif"] = float(vader_data.get('positive', 0.0))
            es_doc["vader_neutre"] = float(vader_data.get('neutral', 1.0))
            es_doc["vader_negatif"] = float(vader_data.get('negative', 0.0))
            es_doc["vader_sentiment"] = str(vader_data.get('sentiment_label', 'neutral'))
            
             
            # Champs calculés avec validation
            contenu_text = es_doc["contenu"]
            es_doc["longueur_texte"] = len(contenu_text) if contenu_text else 0
            es_doc["nb_mots"] = len(contenu_text.split()) if contenu_text else 0
            
            return es_doc
            
        except Exception as e:
            logger.error(f"❌ Erreur transformation document {mongo_doc.get('_id')}: {e}")
            logger.error(f"   Document problématique: {str(mongo_doc)[:200]}...")
            return None
    
    def bulk_index_documents(self, documents: List[Dict], index_name="harcelement_posts"):
        """Indexe les documents en bulk avec meilleure gestion d'erreurs"""
        try:
            actions = []
            
            for doc in documents:
                es_doc = self.transform_document(doc)
                if es_doc:
                    action = {
                        "_index": index_name,
                        "_id": es_doc["id_post"],
                        "_source": es_doc
                    }
                    actions.append(action)
            
            if actions:
                try:
                    # Use bulk helper function with better error handling
                    results = bulk(
                        self.es, 
                        actions, 
                        stats_only=False,
                        raise_on_error=False,  # Don't raise exception on errors
                        raise_on_exception=False  # Don't raise on individual doc errors
                    )
                    
                    success_count = results[0]
                    failed_items = results[1] if len(results) > 1 else []
                    
                    # Log des erreurs détaillées
                    if failed_items:
                        logger.error(f"❌ Erreurs d'indexation détectées: {len(failed_items)}")
                        for i, item in enumerate(failed_items[:5]):  # Affiche les 5 premières erreurs
                            # Handle different error formats
                            if 'index' in item:
                                error_info = item['index'].get('error', {})
                                doc_id = item['index'].get('_id', 'N/A')
                            elif 'create' in item:
                                error_info = item['create'].get('error', {})
                                doc_id = item['create'].get('_id', 'N/A')
                            else:
                                error_info = item
                                doc_id = 'N/A'
                            
                            logger.error(f"   - Document ID: {doc_id}")
                            logger.error(f"   - Erreur: {error_info.get('reason', str(error_info))}")
                            logger.error(f"   - Type: {error_info.get('type', 'Type inconnu')}")
                            
                            # Log the problematic document for debugging
                            if i < 2:  # Only log first 2 docs to avoid spam
                                problematic_doc = next((a for a in actions if a["_id"] == doc_id), None)
                                if problematic_doc:
                                    logger.error(f"   - Document content preview: {str(problematic_doc['_source'])[:200]}...")
                    
                    successful = success_count
                    logger.info(f"✅ Indexés: {successful}, Échoués: {len(failed_items)}")
                    return successful, failed_items
                    
                except Exception as bulk_error:
                    logger.error(f"❌ Erreur bulk helper: {bulk_error}")
                    # Try manual bulk indexing as fallback
                    return self._manual_bulk_index(actions, index_name)
            else:
                logger.warning("Aucun document valide à indexer")
                return 0, []
                
        except Exception as e:
            logger.error(f"❌ Erreur bulk indexation: {e}")
            return 0, []
    
    def _manual_bulk_index(self, actions, index_name):
        """Fallback manual bulk indexing with better error handling"""
        successful = 0
        failed = []
        
        logger.info(f"🔄 Fallback: indexation manuelle de {len(actions)} documents")
        
        for i, action in enumerate(actions):
            try:
                # Use 'document' instead of deprecated 'body' for newer ES versions
                try:
                    response = self.es.index(
                        index=index_name,
                        id=action["_id"],
                        document=action["_source"]  # New API
                    )
                except TypeError:
                    # Fallback for older ES versions
                    response = self.es.index(
                        index=index_name,
                        id=action["_id"],
                        body=action["_source"]  # Old API
                    )
                
                successful += 1
                
                # Log progress every 100 docs
                if (i + 1) % 100 == 0:
                    logger.info(f"   Processed {i + 1}/{len(actions)} documents")
                    
            except Exception as e:
                logger.error(f"❌ Erreur indexation document {action['_id']}: {e}")
                failed.append({
                    "index": {
                        "_id": action["_id"],
                        "error": {"reason": str(e), "type": type(e).__name__}
                    }
                })
                
                # Log problematic document for first few errors
                if len(failed) <= 3:
                    logger.error(f"   Document problématique: {str(action['_source'])[:200]}...")
        
        logger.info(f"✅ Indexation manuelle terminée: {successful} succès, {len(failed)} échecs")
        return successful, failed
    
    def get_index_stats(self, index_name="harcelement_posts"):
        """Obtient les statistiques de l'index"""
        try:
            stats = self.es.indices.stats(index=index_name)
            count = self.es.count(index=index_name)
            
            return {
                "total_documents": count['count'],
                "index_size": stats['indices'][index_name]['total']['store']['size_in_bytes'],
                "index_size_mb": round(stats['indices'][index_name]['total']['store']['size_in_bytes'] / (1024*1024), 2)
            }
        except Exception as e:
            logger.error(f"❌ Erreur stats index: {e}")
            return {}
    
    def test_search(self, index_name="harcelement_posts"):
        """Test de recherche de base"""
        try:
            # Recherche de tous les documents - Fixed for newer ES versions
            try:
                all_docs = self.es.search(
                    index=index_name,
                    query={"match_all": {}},
                    size=0
                )
            except TypeError:
                # Fallback for older ES versions
                all_docs = self.es.search(
                    index=index_name,
                    body={"query": {"match_all": {}}},
                    size=0
                )
            
            # Agrégation par sentiment
            try:
                sentiment_agg = self.es.search(
                    index=index_name,
                    size=0,
                    aggs={
                        "sentiments": {
                            "terms": {"field": "sentiment"}
                        }
                    }
                )
            except TypeError:
                sentiment_agg = self.es.search(
                    index=index_name,
                    body={
                        "size": 0,
                        "aggs": {
                            "sentiments": {
                                "terms": {"field": "sentiment"}
                            }
                        }
                    }
                )
            
            # Agrégation par langue
            try:
                langue_agg = self.es.search(
                    index=index_name,
                    size=0,
                    aggs={
                        "langues": {
                            "terms": {"field": "langue"}
                        }
                    }
                )
            except TypeError:
                langue_agg = self.es.search(
                    index=index_name,
                    body={
                        "size": 0,
                        "aggs": {
                            "langues": {
                                "terms": {"field": "langue"}
                            }
                        }
                    }
                )
            
            print(f"\n=== TEST DE RECHERCHE ELASTICSEARCH ===")
            print(f"Total documents indexés: {all_docs['hits']['total']['value']}")
            
            print(f"\n=== RÉPARTITION DES SENTIMENTS ===")
            for bucket in sentiment_agg['aggregations']['sentiments']['buckets']:
                print(f"{bucket['key']}: {bucket['doc_count']} documents")
            
            print(f"\n=== RÉPARTITION DES LANGUES ===")
            for bucket in langue_agg['aggregations']['langues']['buckets']:
                print(f"{bucket['key']}: {bucket['doc_count']} documents")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur test recherche: {e}")
            return False

def connect_mongodb():
    """Connexion à MongoDB"""
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["harcelement"]
        collection = db["posts"]
        logger.info("✅ Connexion MongoDB établie")
        return client, db, collection
    except Exception as e:
        logger.error(f"❌ Erreur connexion MongoDB: {e}")
        return None, None, None

def debug_elasticsearch_version():
    """Debug pour vérifier la version d'Elasticsearch"""
    try:
        # Test simple connection
        es_simple = Elasticsearch(["http://localhost:9200"])
        info = es_simple.info()
        print(f"✅ Elasticsearch Version: {info['version']['number']}")
        print(f"✅ Cluster: {info['cluster_name']}")
        return True
    except Exception as e:
        print(f"❌ Erreur debug Elasticsearch: {e}")
        return False

def get_processed_documents(collection, batch_size=1000):
    """Récupère les documents traités avec NLP"""
    try:
        cursor = collection.find({
            "processed": True,
            "nlp_processed": True
        })
        documents = list(cursor)
        logger.info(f"📄 Documents récupérés: {len(documents)}")
        return documents
    except Exception as e:
        logger.error(f"❌ Erreur récupération documents: {e}")
        return []

def ingest_to_elasticsearch(mongo_collection, es_ingestor, index_name="harcelement_posts"):
    """Pipeline principal d'ingestion"""
    try:
        # Crée l'index
        print("🔧 Création de l'index Elasticsearch...")
        if not es_ingestor.create_index_mapping(index_name):
            print("❌ Échec création index")
            return False
        
        # Récupère les documents
        print("📄 Récupération des documents MongoDB...")
        documents = get_processed_documents(mongo_collection)
        
        if not documents:
            print("❌ Aucun document traité trouvé")
            return False
        
        print(f"📊 {len(documents)} documents à indexer")
        
        # Indexation par batch
        batch_size = 500
        total_indexed = 0
        total_failed = 0
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            batch_num = i//batch_size + 1
            total_batches = (len(documents)-1)//batch_size + 1
            print(f"🔄 Indexation batch {batch_num}/{total_batches}")
            
            success, failed = es_ingestor.bulk_index_documents(batch, index_name)
            total_indexed += success
            total_failed += len(failed) if failed else 0
        
        print(f"✅ Indexation terminée:")
        print(f"   - Documents indexés: {total_indexed}")
        print(f"   - Documents échoués: {total_failed}")
        
        # Statistiques finales
        stats = es_ingestor.get_index_stats(index_name)
        if stats:
            print(f"📊 Statistiques de l'index:")
            print(f"   - Total documents: {stats['total_documents']}")
            print(f"   - Taille index: {stats['index_size_mb']} MB")
        
        # Test de recherche
        print("\n🔍 Test de recherche...")
        es_ingestor.test_search(index_name)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur pipeline ingestion: {e}")
        return False
def debug_single_document(self, mongo_doc):
    """Debug a single document transformation"""
    es_doc = self.transform_document(mongo_doc)
    print(f"Original MongoDB doc keys: {list(mongo_doc.keys())}")
    print(f"Transformed ES doc: {json.dumps(es_doc, indent=2, default=str)}")
    return es_doc

def main():
    """Fonction principale"""
    print("=== INDEXATION ELASTICSEARCH ===\n")
    
    # Debug Elasticsearch first
    print("🔧 Test de connexion Elasticsearch...")
    if not debug_elasticsearch_version():
        print("❌ Impossible de se connecter à Elasticsearch")
        print("💡 Vérifiez que Elasticsearch est démarré")
        return
    
    # Connexion MongoDB
    print("🔗 Connexion à MongoDB...")
    client, db, collection = connect_mongodb()
    if collection is None:
        print("❌ Impossible de se connecter à MongoDB")
        return
    
    try:
        # Vérification des prérequis
        total_docs = collection.count_documents({})
        processed_docs = collection.count_documents({"processed": True})
        nlp_docs = collection.count_documents({"nlp_processed": True})
        
        print(f"📊 Documents total: {total_docs}")
        print(f"📊 Documents prétraités: {processed_docs}")
        print(f"📊 Documents avec NLP: {nlp_docs}")
        
        if nlp_docs == 0:
            print("❌ Aucun document avec NLP trouvé. Exécutez d'abord nlp_pipeline.py")
            return
        
        # Initialisation Elasticsearch
        print("\n🔌 Connexion à Elasticsearch...")
        es_ingestor = ElasticsearchIngestor()
        
        # Ingestion
        print("\n🚀 Début de l'ingestion...")
        success = ingest_to_elasticsearch(collection, es_ingestor)
        
        if success:
            print("\n✅ INDEXATION ELASTICSEARCH TERMINÉE AVEC SUCCÈS!")
            print("\n💡 Vous pouvez maintenant:")
            print("   - Accéder à Kibana: http://localhost:5601")
            print("   - Explorer l'index: harcelement_posts")
            print("   - Créer des visualisations et dashboards")
        else:
            print("\n❌ Échec de l'indexation")
        
    except Exception as e:
        print(f"❌ Erreur générale: {e}")
        logger.error(f"Erreur générale: {e}")
    
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()