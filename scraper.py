import pandas as pd
import pymongo
from pymongo import MongoClient
import uuid
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
        logger.info("Connexion MongoDB établie")
        return client, db, collection
    except Exception as e:
        logger.error(f"Erreur MongoDB: {e}")
        return None, None, None

def load_dataset(file_path):
    """Charge le dataset CSV"""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Dataset chargé: {len(df)} lignes")
        print(f"Colonnes disponibles: {list(df.columns)}")
        print(df.head())
        return df
    except Exception as e:
        logger.error(f"Erreur chargement CSV: {e}")
        return None

def clean_data(df):
    """Nettoie et prépare les données"""
    # Trouve les colonnes automatiquement
    text_col = None
    label_col = None
    type_col = None
    
    # Cherche les colonnes par nom
    for col in df.columns:
        col_lower = col.lower()
        if 'text' in col_lower or 'message' in col_lower or 'tweet' in col_lower:
            text_col = col
        elif 'label' in col_lower or 'class' in col_lower:
            label_col = col
        elif 'type' in col_lower or 'category' in col_lower:
            type_col = col
    
    # Valeurs par défaut si pas trouvées
    if not text_col:
        text_col = df.columns[0]  # Première colonne
    if not label_col:
        label_col = df.columns[-1]  # Dernière colonne
    if not type_col:
        type_col = label_col
    
    print(f"Colonnes utilisées - Texte: {text_col}, Label: {label_col}, Type: {type_col}")
    
    # Supprime les lignes vides
    df_clean = df.dropna(subset=[text_col])
    df_clean = df_clean[df_clean[text_col].str.strip() != '']
    
    logger.info(f"Données nettoyées: {len(df_clean)} lignes")
    return df_clean, text_col, label_col, type_col

def create_documents(df, text_col, label_col, type_col):
    """Crée les documents pour MongoDB"""
    documents = []
    
    for _, row in df.iterrows():
        doc = {
            "id_post": str(uuid.uuid4()),
            "text": str(row[text_col]).strip(),
            "label": str(row[label_col]),
            "type": str(row[type_col]) if type_col in row else str(row[label_col]),
            "created_at": datetime.utcnow(),
            "processed": False,
            "source": "kaggle_dataset"
        }
        documents.append(doc)
    
    logger.info(f"Documents créés: {len(documents)}")
    return documents

def insert_data(collection, documents):
    """Insère les données dans MongoDB"""
    try:
        # Supprime l'ancienne collection si elle existe
        collection.drop()
        
        # Insère les nouveaux documents
        result = collection.insert_many(documents)
        
        # Crée des index
        collection.create_index("id_post", unique=True)
        collection.create_index("label")
        collection.create_index("type")
        
        logger.info(f"Documents insérés: {len(result.inserted_ids)}")
        return True
    except Exception as e:
        logger.error(f"Erreur insertion: {e}")
        return False

def verify_data(collection):
    """Vérifie les données insérées"""
    try:
        total = collection.count_documents({})
        print(f"\nTotal documents: {total}")
        
        # Statistiques par label
        pipeline = [{"$group": {"_id": "$label", "count": {"$sum": 1}}}]
        stats = list(collection.aggregate(pipeline))
        print("\nRépartition par label:")
        for stat in stats:
            print(f"  {stat['_id']}: {stat['count']}")
        
        # Échantillon
        print("\nÉchantillon de données:")
        for doc in collection.find().limit(2):
            print(f"  Label: {doc['label']} | Type: {doc['type']}")
            print(f"  Texte: {doc['text'][:80]}...")
            print()
            
    except Exception as e:
        logger.error(f"Erreur vérification: {e}")

def main():
    """Fonction principale"""
    print("=== CHARGEMENT DU DATASET DE CYBERHARCÈLEMENT ===\n")
    
    # Paramètre à modifier selon votre fichier
    csv_file = "bullying.csv"  # Changez le nom ici
    
    # 1. Connexion MongoDB
    client, db, collection = connect_mongodb()
    if collection is None:
        print("❌ Impossible de se connecter à MongoDB")
        return
    
    try:
        # 2. Chargement du CSV
        print("📁 Chargement du dataset...")
        df = load_dataset(csv_file)
        if df is None:
            print("❌ Impossible de charger le dataset")
            return
        
        # 3. Nettoyage des données
        print("\n🧹 Nettoyage des données...")
        df_clean, text_col, label_col, type_col = clean_data(df)
        
        # 4. Création des documents
        print("\n📝 Création des documents...")
        documents = create_documents(df_clean, text_col, label_col, type_col)
        
        # 5. Insertion dans MongoDB
        print("\n💾 Insertion dans MongoDB...")
        if insert_data(collection, documents):
            print("✅ Données insérées avec succès")
        else:
            print("❌ Erreur lors de l'insertion")
            return
        
        # 6. Vérification
        print("\n🔍 Vérification des données:")
        verify_data(collection)
        
        print("\n✅ PROCESSUS TERMINÉ AVEC SUCCÈS!")
        
    except Exception as e:
        print(f"❌ Erreur générale: {e}")
    
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()