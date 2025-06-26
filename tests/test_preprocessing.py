import unittest
import sys
import os
from unittest.mock import patch, MagicMock
import pandas as pd

# Ajouter le répertoire parent au path pour importer preprocessing
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from preprocessing import TextPreprocessor, connect_mongodb

class TestTextPreprocessor(unittest.TestCase):
    """Tests pour la classe TextPreprocessor"""
    
    def setUp(self):
        """Configuration avant chaque test"""
        self.preprocessor = TextPreprocessor()
    
    def test_remove_html_tags(self):
        """Test de suppression des balises HTML"""
        text_with_html = "<p>This is a <strong>test</strong> message.</p>"
        expected = "This is a test message."
        result = self.preprocessor.remove_html_tags(text_with_html)
        self.assertEqual(result, expected)
    
    def test_remove_urls(self):
        """Test de suppression des URLs"""
        text_with_url = "Check this out: https://www.example.com and http://test.org"
        result = self.preprocessor.remove_urls(text_with_url)
        self.assertNotIn("https://www.example.com", result)
        self.assertNotIn("http://test.org", result)
        self.assertIn("Check this out:", result)
    
    def test_remove_special_chars(self):
        """Test de suppression des caractères spéciaux"""
        text_with_special = "Hello @user! This is #awesome & cool..."
        result = self.preprocessor.remove_special_chars(text_with_special)
        expected = "Hello user This is awesome  cool"
        self.assertEqual(result, expected)
    
    def test_remove_punctuation_and_digits(self):
        """Test de suppression de la ponctuation et des chiffres"""
        text_with_punct = "Hello! This is test 123 with punctuation..."
        result = self.preprocessor.remove_punctuation_and_digits(text_with_punct)
        # Supprime les chiffres et la ponctuation
        self.assertNotIn("123", result)
        self.assertNotIn("!", result)
        self.assertNotIn("...", result)
    
    def test_remove_stopwords(self):
        """Test de suppression des mots vides"""
        tokens = ["this", "is", "a", "test", "message"]
        result = self.preprocessor.remove_stopwords(tokens)
        # "this", "is", "a" sont des stopwords en anglais
        self.assertNotIn("this", result)
        self.assertNotIn("is", result)
        self.assertNotIn("a", result)
        self.assertIn("test", result)
        self.assertIn("message", result)
    
    def test_lemmatize_tokens(self):
        """Test de lemmatisation"""
        tokens = ["running", "dogs", "better", "flying"]
        result = self.preprocessor.lemmatize_tokens(tokens)
        # Vérifie quelques lemmatisations connues
        self.assertIn("running", result)  # ou "run" selon le contexte
        self.assertIn("dog", result)
        self.assertTrue(len(result) == len(tokens))
    
    def test_preprocess_text_complete(self):
        """Test du pipeline complet de prétraitement"""
        text = "<p>Hello @user! This is a TEST 123 with https://example.com and punctuation...</p>"
        result = self.preprocessor.preprocess_text(text)
        
        # Vérifie la structure du résultat
        self.assertIn('original_text', result)
        self.assertIn('cleaned_text', result)
        self.assertIn('tokens', result)
        self.assertIn('processed_text', result)
        self.assertIn('word_count', result)
        
        # Vérifie que le texte original est préservé
        self.assertIn("hello @user!", result['original_text'])
        
        # Vérifie que le traitement a eu lieu
        self.assertIsInstance(result['tokens'], list)
        self.assertIsInstance(result['word_count'], int)
        
        # Vérifie que certains éléments ont été supprimés
        processed_lower = result['processed_text'].lower()
        self.assertNotIn("123", processed_lower)
        self.assertNotIn("https://", processed_lower)
    
    def test_preprocess_empty_text(self):
        """Test avec texte vide ou None"""
        # Test avec None
        result_none = self.preprocessor.preprocess_text(None)
        self.assertEqual(result_none['processed_text'], '')
        self.assertEqual(result_none['word_count'], 0)
        
        # Test avec chaîne vide
        result_empty = self.preprocessor.preprocess_text("")
        self.assertEqual(result_empty['processed_text'], '')
        self.assertEqual(result_empty['word_count'], 0)
        
        # Test avec espaces seulement
        result_spaces = self.preprocessor.preprocess_text("   ")
        self.assertEqual(result_spaces['processed_text'], '')
        self.assertEqual(result_spaces['word_count'], 0)
    
    def test_preprocess_text_with_only_stopwords(self):
        """Test avec texte contenant uniquement des stopwords"""
        text = "the and or but"
        result = self.preprocessor.preprocess_text(text)
        # Après suppression des stopwords, le texte devrait être vide ou presque
        self.assertTrue(len(result['processed_text']) < len(text))
    
    def test_preprocess_text_with_numbers_and_special_chars(self):
        """Test avec chiffres et caractères spéciaux variés"""
        text = "Contact me at +33123456789 or email@domain.com! Price: $99.99"
        result = self.preprocessor.preprocess_text(text)
        
        # Vérifie que les chiffres ont été supprimés
        self.assertNotIn("33123456789", result['processed_text'])
        self.assertNotIn("99", result['processed_text'])
        
        # Vérifie que les caractères spéciaux ont été supprimés
        self.assertNotIn("@", result['processed_text'])
        self.assertNotIn("$", result['processed_text'])
        self.assertNotIn("+", result['processed_text'])

class TestMongoDBConnection(unittest.TestCase):
    """Tests pour la connexion MongoDB"""
    
    @patch('preprocessing.MongoClient')
    def test_connect_mongodb_success(self, mock_mongo_client):
        """Test de connexion MongoDB réussie"""
        # Mock de la connexion réussie
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        
        mock_mongo_client.return_value = mock_client
        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        
        client, db, collection = connect_mongodb()
        
        self.assertIsNotNone(client)
        self.assertIsNotNone(db)
        self.assertIsNotNone(collection)
        mock_mongo_client.assert_called_once_with("mongodb://localhost:27017/")
    
    @patch('preprocessing.MongoClient')
    def test_connect_mongodb_failure(self, mock_mongo_client):
        """Test d'échec de connexion MongoDB"""
        # Mock d'une exception lors de la connexion
        mock_mongo_client.side_effect = Exception("Connection failed")
        
        client, db, collection = connect_mongodb()
        
        self.assertIsNone(client)
        self.assertIsNone(db)
        self.assertIsNone(collection)

class TestPreprocessingIntegration(unittest.TestCase):
    """Tests d'intégration pour le prétraitement"""
    
    def setUp(self):
        """Configuration avant chaque test"""
        self.preprocessor = TextPreprocessor()
    
    def test_preprocessing_pipeline_consistency(self):
        """Test de cohérence du pipeline de prétraitement"""
        test_texts = [
            "This is a simple test message.",
            "<p>HTML content with <strong>tags</strong></p>",
            "Message with URL: https://example.com and @mentions",
            "Text with numbers 123 and punctuation!!!",
            "",
            None
        ]
        
        for text in test_texts:
            result = self.preprocessor.preprocess_text(text)
            
            # Vérifie que tous les champs requis sont présents
            required_fields = ['original_text', 'cleaned_text', 'tokens', 'processed_text', 'word_count']
            for field in required_fields:
                self.assertIn(field, result, f"Champ {field} manquant pour le texte: {text}")
            
            # Vérifie les types
            self.assertIsInstance(result['tokens'], list)
            self.assertIsInstance(result['word_count'], int)
            self.assertIsInstance(result['processed_text'], str)
            
            # Vérifie la cohérence entre tokens et word_count
            self.assertEqual(len(result['tokens']), result['word_count'])
    
    def test_batch_processing_simulation(self):
        """Simule le traitement par batch"""
        test_documents = [
            {"text": "This is document 1 with some content."},
            {"text": "Document 2 has different content with URLs: https://test.com"},
            {"text": "<p>Document 3 with HTML <strong>tags</strong></p>"},
            {"text": "Document 4 with numbers 123 and special chars @#$"},
            {"text": ""}
        ]
        
        processed_results = []
        
        for doc in test_documents:
            result = self.preprocessor.preprocess_text(doc.get('text', ''))
            processed_results.append(result)
        
        # Vérifie que tous les documents ont été traités
        self.assertEqual(len(processed_results), len(test_documents))
        
        # Vérifie que les résultats sont cohérents
        for result in processed_results:
            self.assertIn('processed_text', result)
            self.assertIsInstance(result['word_count'], int)
            self.assertGreaterEqual(result['word_count'], 0)

def run_tests():
    """Execute tous les tests"""
    # Crée une suite de tests
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Ajoute toutes les classes de test
    suite.addTests(loader.loadTestsFromTestCase(TestTextPreprocessor))
    suite.addTests(loader.loadTestsFromTestCase(TestMongoDBConnection))
    suite.addTests(loader.loadTestsFromTestCase(TestPreprocessingIntegration))
    
    # Execute les tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == "__main__":
    print("=== TESTS DU MODULE DE PRÉTRAITEMENT ===\n")
    
    success = run_tests()
    
    if success:
        print("\n✅ Tous les tests sont passés avec succès!")
    else:
        print("\n❌ Certains tests ont échoué.")
        sys.exit(1)