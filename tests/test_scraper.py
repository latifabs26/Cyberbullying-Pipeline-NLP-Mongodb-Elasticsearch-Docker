import unittest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

# Import du module à tester
try:
    import scraper
except ImportError:
    import sys
    sys.path.append('.')
    import scraper

class TestScraper(unittest.TestCase):
    """Tests pour le scraper simplifié"""
    
    def setUp(self):
        """Données de test"""
        self.sample_data = {
            'text': ['Message de harcèlement', 'Message normal', 'Contenu de bullying'],
            'label': ['harassment', 'not_harassment', 'harassment'],
            'type': ['cyberbullying', 'normal', 'bullying']
        }
        self.df = pd.DataFrame(self.sample_data)
    
    @patch('pymongo.MongoClient')
    def test_connect_mongodb_success(self, mock_client):
        """Test connexion MongoDB réussie"""
        mock_client.return_value = MagicMock()
        
        client, db, collection = scraper.connect_mongodb()
        
        self.assertIsNotNone(client)
        self.assertIsNotNone(db)
        self.assertIsNotNone(collection)
    
    def test_load_dataset_success(self):
        """Test chargement CSV réussi"""
        # Crée un fichier CSV temporaire
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            self.df.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            result = scraper.load_dataset(temp_file)
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)
        finally:
            os.unlink(temp_file)
    
    def test_load_dataset_failure(self):
        """Test chargement CSV échoué"""
        result = scraper.load_dataset("fichier_inexistant.csv")
        self.assertIsNone(result)
    
    def test_clean_data(self):
        """Test nettoyage des données"""
        df_clean, text_col, label_col, type_col = scraper.clean_data(self.df)
        
        self.assertIsInstance(df_clean, pd.DataFrame)
        self.assertEqual(len(df_clean), 3)  # Aucune ligne supprimée
        self.assertEqual(text_col, 'text')
        self.assertEqual(label_col, 'label')
        self.assertEqual(type_col, 'type')
    
    def test_clean_data_with_empty_rows(self):
        """Test nettoyage avec lignes vides"""
        df_with_empty = self.df.copy()
        df_with_empty.loc[3] = ['', 'label', 'type']  # Ligne vide
        df_with_empty.loc[4] = [None, 'label', 'type']  # Ligne None
        
        df_clean, _, _, _ = scraper.clean_data(df_with_empty)
        
        # Doit supprimer les lignes vides
        self.assertEqual(len(df_clean), 3)
    
    def test_create_documents(self):
        """Test création des documents"""
        documents = scraper.create_documents(self.df, 'text', 'label', 'type')
        
        self.assertEqual(len(documents), 3)
        
        # Vérifie la structure du premier document
        doc = documents[0]
        required_fields = ['id_post', 'text', 'label', 'type', 'created_at', 'processed', 'source']
        
        for field in required_fields:
            self.assertIn(field, doc)
        
        self.assertEqual(doc['text'], 'Message de harcèlement')
        self.assertEqual(doc['label'], 'harassment')
        self.assertFalse(doc['processed'])
    
    @patch('pymongo.collection.Collection.insert_many')
    @patch('pymongo.collection.Collection.create_index')
    @patch('pymongo.collection.Collection.drop')
    def test_insert_data_success(self, mock_drop, mock_create_index, mock_insert):
        """Test insertion réussie"""
        mock_insert.return_value = MagicMock(inserted_ids=['id1', 'id2', 'id3'])
        mock_collection = MagicMock()
        mock_collection.insert_many = mock_insert
        mock_collection.create_index = mock_create_index
        mock_collection.drop = mock_drop
        
        documents = [{'text': 'test', 'label': 'test'}]
        result = scraper.insert_data(mock_collection, documents)
        
        self.assertTrue(result)
        mock_insert.assert_called_once()
        mock_drop.assert_called_once()
        self.assertEqual(mock_create_index.call_count, 3)  # 3 index créés
    
    @patch('pymongo.collection.Collection.count_documents')
    @patch('pymongo.collection.Collection.aggregate')
    @patch('pymongo.collection.Collection.find')
    def test_verify_data(self, mock_find, mock_aggregate, mock_count):
        """Test vérification des données"""
        mock_count.return_value = 100
        mock_aggregate.return_value = [
            {'_id': 'harassment', 'count': 60},
            {'_id': 'normal', 'count': 40}
        ]
        mock_find.return_value.limit.return_value = [
            {'text': 'Test', 'label': 'harassment', 'type': 'cyberbullying'}
        ]
        
        mock_collection = MagicMock()
        mock_collection.count_documents = mock_count
        mock_collection.aggregate = mock_aggregate
        mock_collection.find = mock_find
        
        try:
            scraper.verify_data(mock_collection)
            success = True
        except Exception:
            success = False
        
        self.assertTrue(success)

class TestDataIntegrity(unittest.TestCase):
    """Tests d'intégrité des données"""
    
    def test_document_has_required_fields(self):
        """Test que les documents ont tous les champs requis"""
        df = pd.DataFrame({
            'text': ['Test message'],
            'label': ['harassment']
        })
        
        documents = scraper.create_documents(df, 'text', 'label', 'label')
        doc = documents[0]
        
        required_fields = ['id_post', 'text', 'label', 'type']
        for field in required_fields:
            self.assertIn(field, doc)
            self.assertIsNotNone(doc[field])
    
    def test_unique_ids(self):
        """Test que les IDs sont uniques"""
        df = pd.DataFrame({
            'text': ['Message 1', 'Message 2', 'Message 3'],
            'label': ['harassment', 'normal', 'harassment']
        })
        
        documents = scraper.create_documents(df, 'text', 'label', 'label')
        ids = [doc['id_post'] for doc in documents]
        
        self.assertEqual(len(ids), len(set(ids)))

def run_tests():
    """Lance tous les tests"""
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestScraper))
    suite.addTests(loader.loadTestsFromTestCase(TestDataIntegrity))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    print("🧪 Lancement des tests unitaires...\n")
    
    if run_tests():
        print("\n✅ Tous les tests sont passés!")
    else:
        print("\n❌ Certains tests ont échoué.")
        exit(1)
