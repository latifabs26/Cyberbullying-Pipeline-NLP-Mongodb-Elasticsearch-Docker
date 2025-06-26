import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Ajouter le répertoire parent au path pour importer nlp_pipeline
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nlp_pipeline import NLPProcessor, connect_mongodb, get_documents_for_nlp, update_document_with_nlp

class TestNLPProcessor(unittest.TestCase):
    
    def setUp(self):
        """Initialise le processeur NLP pour les tests"""
        self.nlp_processor = NLPProcessor()
    
    def test_detect_language_english(self):
        """Test détection langue anglaise"""
        text = "This is a test message in English language"
        result = self.nlp_processor.detect_language(text)
        
        self.assertIsInstance(result, dict)
        self.assertIn('language', result)
        self.assertIn('confidence', result)
        self.assertEqual(result['language'], 'en')
        self.assertGreater(result['confidence'], 0)
    
    def test_detect_language_french(self):
        """Test détection langue française"""
        text = "Ceci est un message de test en langue française"
        result = self.nlp_processor.detect_language(text)
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result['language'], 'fr')
        self.assertGreater(result['confidence'], 0)
    
    def test_detect_language_empty_text(self):
        """Test détection langue avec texte vide"""
        result = self.nlp_processor.detect_language("")
        
        self.assertEqual(result['language'], 'unknown')
        self.assertEqual(result['confidence'], 0.0)
    
    def test_detect_language_short_text(self):
        """Test détection langue avec texte très court"""
        result = self.nlp_processor.detect_language("Hi")
        
        self.assertIsInstance(result, dict)
        self.assertIn('language', result)
    
    def test_analyze_sentiment_textblob_positive(self):
        """Test analyse sentiment positive avec TextBlob"""
        text = "I love this amazing product! It's fantastic and wonderful!"
        result = self.nlp_processor.analyze_sentiment_textblob(text)
        
        self.assertIsInstance(result, dict)
        self.assertIn('polarity', result)
        self.assertIn('subjectivity', result)
        self.assertIn('sentiment_label', result)
        self.assertEqual(result['sentiment_label'], 'positive')
        self.assertGreater(result['polarity'], 0)
    
    def test_analyze_sentiment_textblob_negative(self):
        """Test analyse sentiment négative avec TextBlob"""
        text = "I hate this terrible product! It's awful and horrible!"
        result = self.nlp_processor.analyze_sentiment_textblob(text)
        
        self.assertEqual(result['sentiment_label'], 'negative')
        self.assertLess(result['polarity'], 0)
    
    def test_analyze_sentiment_textblob_neutral(self):
        """Test analyse sentiment neutre avec TextBlob"""
        text = "This is a regular product with standard features."
        result = self.nlp_processor.analyze_sentiment_textblob(text)
        
        self.assertIsInstance(result, dict)
        self.assertIn('sentiment_label', result)
        # Le sentiment peut être neutre ou légèrement positif/négatif
        self.assertIn(result['sentiment_label'], ['neutral', 'positive', 'negative'])
    
    def test_analyze_sentiment_textblob_empty(self):
        """Test analyse sentiment avec texte vide"""
        result = self.nlp_processor.analyze_sentiment_textblob("")
        
        self.assertEqual(result['polarity'], 0.0)
        self.assertEqual(result['sentiment_label'], 'neutral')
    
    def test_analyze_sentiment_vader_positive(self):
        """Test analyse sentiment positive avec VADER"""
        text = "I love this amazing product! It's fantastic and wonderful! 😊"
        result = self.nlp_processor.analyze_sentiment_vader(text)
        
        self.assertIsInstance(result, dict)
        self.assertIn('compound', result)
        self.assertIn('positive', result)
        self.assertIn('negative', result)
        self.assertIn('neutral', result)
        self.assertIn('sentiment_label', result)
        self.assertEqual(result['sentiment_label'], 'positive')
        self.assertGreater(result['compound'], 0)
    
    def test_analyze_sentiment_vader_negative(self):
        """Test analyse sentiment négative avec VADER"""
        text = "I hate this terrible product! It's awful and horrible! 😠"
        result = self.nlp_processor.analyze_sentiment_vader(text)
        
        self.assertEqual(result['sentiment_label'], 'negative')
        self.assertLess(result['compound'], 0)
    
    def test_analyze_sentiment_vader_empty(self):
        """Test analyse sentiment VADER avec texte vide"""
        result = self.nlp_processor.analyze_sentiment_vader("")
        
        self.assertEqual(result['compound'], 0.0)
        self.assertEqual(result['sentiment_label'], 'neutral')
    
    def test_get_sentiment_consensus_agreement(self):
        """Test consensus quand TextBlob et VADER sont d'accord"""
        textblob_result = {
            'sentiment_label': 'positive',
            'polarity': 0.5
        }
        vader_result = {
            'sentiment_label': 'positive',
            'compound': 0.6
        }
        
        result = self.nlp_processor.get_sentiment_consensus(textblob_result, vader_result)
        
        self.assertEqual(result['final_sentiment'], 'positive')
        self.assertEqual(result['confidence'], 0.8)
        self.assertAlmostEqual(result['final_score'], 0.55, places=2)
    
    def test_get_sentiment_consensus_disagreement(self):
        """Test consensus quand TextBlob et VADER ne sont pas d'accord"""
        textblob_result = {
            'sentiment_label': 'positive',
            'polarity': 0.2
        }
        vader_result = {
            'sentiment_label': 'negative',
            'compound': -0.3
        }
        
        result = self.nlp_processor.get_sentiment_consensus(textblob_result, vader_result)
        
        self.assertEqual(result['final_sentiment'], 'negative')  # VADER prioritaire
        self.assertEqual(result['confidence'], 0.6)
    
    def test_process_text_nlp_complete(self):
        """Test du pipeline NLP complet"""
        text = "This is a wonderful test message! I really love it!"
        result = self.nlp_processor.process_text_nlp(text)
        
        self.assertIsInstance(result, dict)
        self.assertIn('language_detection', result)
        self.assertIn('textblob_sentiment', result)
        self.assertIn('vader_sentiment', result)
        self.assertIn('sentiment_consensus', result)
        
        # Vérifications des sous-structures
        self.assertIn('language', result['language_detection'])
        self.assertIn('sentiment_label', result['textblob_sentiment'])
        self.assertIn('sentiment_label', result['vader_sentiment'])
        self.assertIn('final_sentiment', result['sentiment_consensus'])
    
    def test_process_text_nlp_empty(self):
        """Test du pipeline NLP avec texte vide"""
        result = self.nlp_processor.process_text_nlp("")
        
        self.assertEqual(result['language_detection']['language'], 'unknown')
        self.assertEqual(result['sentiment_consensus']['final_sentiment'], 'neutral')
    
    def test_process_text_nlp_with_processed_text(self):
        """Test du pipeline NLP avec texte original et traité"""
        original_text = "This is a wonderful test message! I really love it! 😊"
        processed_text = "wonderful test message love"
        
        result = self.nlp_processor.process_text_nlp(original_text, processed_text)
        
        self.assertIsInstance(result, dict)
        self.assertIn('language_detection', result)
        self.assertIn('sentiment_consensus', result)

class TestMongoDBFunctions(unittest.TestCase):
    
    @patch('nlp_pipeline.MongoClient')
    def test_connect_mongodb_success(self, mock_client):
        """Test connexion MongoDB réussie"""
        mock_client_instance = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        
        mock_client.return_value = mock_client_instance
        mock_client_instance.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        
        client, db, collection = connect_mongodb()
        
        self.assertIsNotNone(client)
        self.assertIsNotNone(db)
        self.assertIsNotNone(collection)
        mock_client.assert_called_once_with("mongodb://localhost:27017/")
        
    
    def test_get_documents_for_nlp(self):
        """Test récupération documents pour NLP"""
        mock_collection = Mock()
        mock_cursor = Mock()
        mock_documents = [
            {'_id': 1, 'text': 'test1', 'processed': True},
            {'_id': 2, 'text': 'test2', 'processed': True}
        ]
        
        mock_collection.find.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_cursor.__iter__ = Mock(return_value=iter(mock_documents))
        list(mock_cursor)  # Simule la conversion en liste
        
        # Mock la fonction list() pour retourner nos documents
        with patch('builtins.list', return_value=mock_documents):
            documents = get_documents_for_nlp(mock_collection, batch_size=100)
        
        self.assertEqual(len(documents), 2)
        mock_collection.find.assert_called_once_with({
            "processed": True,
            "nlp_processed": {"$ne": True}
        })
    
    def test_update_document_with_nlp(self):
        """Test mise à jour document avec résultats NLP"""
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.modified_count = 1
        mock_collection.update_one.return_value = mock_result
        
        nlp_result = {
            'language_detection': {'language': 'en', 'confidence': 0.95},
            'textblob_sentiment': {'sentiment_label': 'positive', 'polarity': 0.5},
            'vader_sentiment': {'sentiment_label': 'positive', 'compound': 0.6},
            'sentiment_consensus': {'final_sentiment': 'positive', 'final_score': 0.55}
        }
        
        result = update_document_with_nlp(mock_collection, 'test_id', nlp_result)
        
        self.assertTrue(result)
        mock_collection.update_one.assert_called_once()

class TestNLPIntegration(unittest.TestCase):
    """Tests d'intégration pour le pipeline NLP"""
    
    def test_nlp_pipeline_robustness(self):
        """Test la robustesse du pipeline avec différents types de textes"""
        nlp_processor = NLPProcessor()
        
        test_cases = [
            "Hello world!",  # Simple
            "😊 I love this! 💕",  # Avec emojis
            "RT @user: Check this out https://example.com",  # Tweet-like
            "URGENT!!! CALL NOW!!!",  # Caps et ponctuation
            "123 456 789",  # Nombres uniquement
            "a",  # Très court
            "",  # Vide
            None,  # None
        ]
        
        for text in test_cases:
            with self.subTest(text=text):
                try:
                    result = nlp_processor.process_text_nlp(text)
                    print(f"Input: {text}\nResult: {result}\n")  # 👈 ajoute ceci
                    self.assertIsInstance(result, dict)
                    self.assertIn('language_detection', result)
                    self.assertIn('sentiment_consensus', result)
                except Exception as e:
                    self.fail(f"Pipeline failed for text '{text}': {e}")

if __name__ == '__main__':
    # Configuration pour les tests
    import warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    
    # Exécution des tests
    unittest.main(verbosity=2)