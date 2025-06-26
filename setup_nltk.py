#!/usr/bin/env python3
"""
Script d'installation des ressources NLTK
Exécutez ce script avant d'utiliser preprocessing.py
"""

import nltk
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_nltk_resources():
    """Télécharge toutes les ressources NLTK nécessaires"""
    
    # Liste des ressources nécessaires
    resources = [
        'punkt',           # Tokenisation de phrases (ancienne version)
        'punkt_tab',       # Tokenisation de phrases (nouvelle version)
        'stopwords',       # Mots vides
        'wordnet',         # Base de données lexicale pour lemmatisation
        'omw-1.4',         # Open Multilingual Wordnet (pour lemmatisation)
        'vader_lexicon'    # Pour l'analyse de sentiment (étape suivante)
    ]
    
    print("=== INSTALLATION DES RESSOURCES NLTK ===\n")
    
    successful_downloads = 0
    failed_downloads = []
    
    for resource in resources:
        try:
            print(f"📥 Téléchargement de {resource}...")
            nltk.download(resource, quiet=False)
            successful_downloads += 1
            print(f"✅ {resource} téléchargé avec succès")
        except Exception as e:
            print(f"❌ Erreur lors du téléchargement de {resource}: {e}")
            failed_downloads.append(resource)
            logger.error(f"Erreur téléchargement {resource}: {e}")
        print()
    
    # Résumé
    print("=== RÉSUMÉ DE L'INSTALLATION ===")
    print(f"✅ Ressources téléchargées avec succès: {successful_downloads}")
    if failed_downloads:
        print(f"❌ Ressources en échec: {len(failed_downloads)}")
        for resource in failed_downloads:
            print(f"   - {resource}")
    else:
        print("🎉 Toutes les ressources ont été téléchargées avec succès!")
    
    return len(failed_downloads) == 0

def test_nltk_resources():
    """Teste que les ressources NLTK fonctionnent correctement"""
    print("\n=== TEST DES RESSOURCES NLTK ===\n")
    
    tests = []
    
    # Test de tokenisation
    try:
        from nltk.tokenize import word_tokenize, sent_tokenize
        test_text = "Hello world. This is a test sentence."
        tokens = word_tokenize(test_text)
        sentences = sent_tokenize(test_text)
        print("✅ Tokenisation : OK")
        print(f"   Tokens: {tokens}")
        print(f"   Phrases: {sentences}")
        tests.append(True)
    except Exception as e:
        print(f"❌ Tokenisation : ERREUR - {e}")
        tests.append(False)
    
    # Test des stopwords
    try:
        from nltk.corpus import stopwords
        english_stopwords = set(stopwords.words('english'))
        print(f"✅ Stopwords : OK ({len(english_stopwords)} mots)")
        print(f"   Exemple: {list(english_stopwords)[:5]}")
        tests.append(True)
    except Exception as e:
        print(f"❌ Stopwords : ERREUR - {e}")
        tests.append(False)
    
    # Test de lemmatisation
    try:
        from nltk.stem import WordNetLemmatizer
        lemmatizer = WordNetLemmatizer()
        test_words = ['running', 'dogs', 'better']
        lemmatized = [lemmatizer.lemmatize(word) for word in test_words]
        print("✅ Lemmatisation : OK")
        print(f"   {test_words} → {lemmatized}")
        tests.append(True)
    except Exception as e:
        print(f"❌ Lemmatisation : ERREUR - {e}")
        tests.append(False)
    
    # Test d'étiquetage POS
    try:
        from nltk import pos_tag
        from nltk.tokenize import word_tokenize
        test_text = "The quick brown fox jumps"
        tokens = word_tokenize(test_text)
        pos_tags = pos_tag(tokens)
        print("✅ Étiquetage POS : OK")
        print(f"   {pos_tags}")
        tests.append(True)
    except Exception as e:
        print(f"❌ Étiquetage POS : ERREUR - {e}")
        tests.append(False)
    
    success_rate = sum(tests) / len(tests) * 100
    print(f"\n📊 Taux de réussite des tests: {success_rate:.1f}%")
    
    return all(tests)

def main():
    """Fonction principale"""
    print("🚀 Configuration de l'environnement NLTK pour le projet de cyberharcèlement\n")
    
    # Téléchargement des ressources
    download_success = download_nltk_resources()
    
    if download_success:
        # Test des ressources
        test_success = test_nltk_resources()
        
        if test_success:
            print("\n🎉 CONFIGURATION NLTK TERMINÉE AVEC SUCCÈS!")
            print("Vous pouvez maintenant exécuter preprocessing.py")
        else:
            print("\n⚠️ Certains tests ont échoué. Vérifiez les erreurs ci-dessus.")
    else:
        print("\n❌ Certaines ressources n'ont pas pu être téléchargées.")
        print("Essayez de relancer le script ou téléchargez-les manuellement :")
        print(">>> import nltk")
        print(">>> nltk.download('all')")

if __name__ == "__main__":
    main()