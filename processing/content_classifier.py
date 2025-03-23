"""
Content classifier for the Mexican Municipal Candidates Scraper.

This module provides content type classification (news, articles, discourse)
and quote extraction, with both rule-based and ML-ready approaches.
"""
import re
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import traceback

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger
from processing.name_matcher import create_name_matcher

logger = get_logger(__name__)

# Try to import optional NLP libraries
try:
    import spacy
    try:
        NLP_MODEL = spacy.load("es_core_news_sm")
        SPACY_AVAILABLE = True
        logger.info("Loaded Spanish spaCy model for content classification")
    except:
        SPACY_AVAILABLE = False
        NLP_MODEL = None
        logger.warning("Spanish spaCy model not available. Install with 'python -m spacy download es_core_news_sm'")
except ImportError:
    SPACY_AVAILABLE = False
    NLP_MODEL = None
    logger.warning("spaCy not available. Install with 'pip install spacy' for better content classification")


class ContentClassifier:
    """
    Classifies content into news, articles, discourse/quotes from candidates
    using both rule-based and ML-ready approaches.
    """
    
    def __init__(self, nlp_model=None, name_matcher=None, max_workers=2):
        """
        Initialize the content classifier.
        
        Args:
            nlp_model: Optional NLP model for enhanced classification
            name_matcher: Name matcher for candidate name recognition
            max_workers (int): Maximum number of worker threads
        """
        self.nlp_model = nlp_model
        self.max_workers = max_workers
        
        # Initialize name matcher if not provided
        self.name_matcher = name_matcher or create_name_matcher()
        
        # Speech indicators in Spanish
        self.speech_indicators = [
            "dijo", "afirmó", "declaró", "expresó", "mencionó", "señaló",
            "comentó", "indicó", "manifestó", "sostuvo", "aseguró",
            '"', '"', '«', '»', ':', '"', 'agregó', 'añadió', 'subrayó',
            "explicó", "apuntó", "destacó", "enfatizó", "resaltó",
            "argumentó", "puntualizó", "detalló", "refirió"
        ]
        
        # News indicators
        self.news_indicators = [
            "noticia", "reportaje", "periodista", "diario", "periódico",
            "boletín", "comunicado", "informó", "reportó", "publicó",
            "redacción", "staff", "corresponsal", "agencia", "fuente",
            "fecha de publicación", "publicado el", "actualizado el",
            "ayer", "hoy", "anteayer", "comunicado de prensa", "nota",
            "reportero", "cobertura", "enviado", "entrevista", "crónica"
        ]
        
        # Article indicators
        self.article_indicators = [
            "artículo", "opinión", "editorial", "columna", "análisis",
            "perspectiva", "punto de vista", "enfoque", "estudio", "investigación",
            "escrito por", "autor", "colaboración", "tribuna", "ensayo",
            "reflexión", "comentarista", "colaborador", "columnista", "crítica",
            "especialista", "experto"
        ]
        
        # Quote patterns
        self.quote_patterns = [
            r'"([^"]{10,})"',                 # Standard quotes (min 10 chars)
            r'"([^"]{10,})"',                 # Curly quotes
            r'«([^»]{10,})»',                 # Spanish quotes
            r'"([^"]{10,})"',                 # Double straight quotes
            r'["\']([^"\']{10,})["\']',       # Mixed quotes
            
            # Speech followed by colon and quote
            r'(?:dijo|afirmó|declaró|expresó|mencionó|señaló|comentó|indicó|manifestó|sostuvo|aseguró)(?:\s+\w+\s+|\s+)[:]\s+"([^"]{10,})"',
            
            # Speech followed by comma and quote
            r'(?:dijo|afirmó|declaró|expresó|mencionó|señaló|comentó|indicó|manifestó|sostuvo|aseguró)(?:\s+\w+\s+|\s+)[,]\s+"([^"]{10,})"',
            
            # Name followed by speech verb and quote
            r'([A-Z][a-zñáéíóú]+ [A-Z][a-zñáéíóú]+)(?:\s+)(?:dijo|afirmó|declaró|expresó|mencionó|señaló|comentó|indicó)(?:\s+)[:]\s+"([^"]{10,})"',
        ]
        
        # Performance optimization: pre-compile regex patterns
        self.compiled_quote_patterns = [re.compile(pattern, re.DOTALL) for pattern in self.quote_patterns]
    
    def classify_content(self, text, candidate_name=None, parallel=False):
        """
        Classify content type and extract quotes if available.
        
        Args:
            text (str): Content text to classify
            candidate_name (str, optional): Candidate name for quote relevance
            parallel (bool, optional): Use parallel processing for large content
            
        Returns:
            dict: Classification results with content type, confidence, and quotes
        """
        if not text:
            return {
                'content_type': 'unknown',
                'confidence': 0.0,
                'quotes': [],
                'quote_count': 0
            }
        
        # Initialize scores
        news_score = 0
        article_score = 0
        discourse_score = 0
        
        text_lower = text.lower()
        
        # Check for news indicators
        for indicator in self.news_indicators:
            if indicator.lower() in text_lower:
                news_score += 1
        
        # Check for article indicators
        for indicator in self.article_indicators:
            if indicator.lower() in text_lower:
                article_score += 1
        
        # Extract quotes using regex
        quotes = self._extract_quotes(text, candidate_name, parallel=parallel)
        
        quote_count = len(quotes)
        if quote_count > 0:
            discourse_score += 2  # Base score for having quotes
        
        # Check for candidate in quotes
        candidate_in_quotes = False
        
        if candidate_name:
            # Use name matcher for more robust checking
            for quote in quotes:
                quote_text = quote.get('text', '')
                found, score, _ = self.name_matcher.fuzzy_match_name(quote_text, candidate_name, threshold=70)
                if found and score > 70:
                    candidate_in_quotes = True
                    discourse_score += 3
                    break
        
        # Check speech indicators near candidate name
        if candidate_name:
            # First check if candidate name appears in text
            found, score, _ = self.name_matcher.fuzzy_match_name(text, candidate_name, threshold=70)
            
            if found and score > 70:
                for indicator in self.speech_indicators:
                    indicator_lower = indicator.lower()
                    if indicator_lower in text_lower:
                        discourse_score += 1
                        
                        # Extract contexts around candidate name
                        contexts = self.name_matcher.extract_name_contexts(text, candidate_name)
                        
                        # Check if speech indicators appear near candidate name
                        for context in contexts:
                            context_text = context.get('text', '').lower()
                            if indicator_lower in context_text:
                                discourse_score += 3
                                break
        
        # Use NLP model for enhanced classification if available
        if SPACY_AVAILABLE and self.nlp_model:
            try:
                # Process first 10k chars to avoid memory issues
                doc = self.nlp_model(text[:10000])
                
                # Check for speech verbs with persons as subjects
                for token in doc:
                    if token.pos_ == "VERB" and token.lemma_ in [
                        "decir", "afirmar", "declarar", "expresar", "mencionar", 
                        "señalar", "comentar", "indicar", "manifestar"
                    ]:
                        for child in token.children:
                            if child.dep_ == "nsubj" and child.pos_ == "PROPN":
                                discourse_score += 2
                                break
                
                # Check for quote attribution patterns
                for ent in doc.ents:
                    if ent.label_ == "PER":
                        # Look for speech verbs after person entities
                        i = ent.end
                        if i < len(doc) and doc[i].lemma_ in [
                            "decir", "afirmar", "declarar", "expresar", "mencionar"
                        ]:
                            discourse_score += 2
            except Exception as e:
                logger.warning(f"Error in NLP-based classification: {str(e)}")
        
        # Determine content type based on scores
        content_type = 'unknown'
        confidence = 0.4  # Base confidence
        
        max_score = max(news_score, article_score, discourse_score)
        
        if max_score > 0:
            if max_score == news_score:
                content_type = 'news'
                confidence = min(0.5 + (news_score * 0.1), 1.0)
            elif max_score == article_score:
                content_type = 'article'
                confidence = min(0.5 + (article_score * 0.1), 1.0)
            elif max_score == discourse_score:
                content_type = 'discourse'
                confidence = min(0.5 + (discourse_score * 0.05), 1.0)
                
                # Extra boost if candidate is in quotes
                if candidate_in_quotes:
                    confidence = min(confidence + 0.2, 1.0)
        
        return {
            'content_type': content_type,
            'confidence': confidence,
            'quotes': quotes,
            'quote_count': quote_count
        }
    
    def _extract_quotes(self, text, candidate_name=None, parallel=False):
        """
        Extract quotes from text with context.
        
        Args:
            text (str): Text to extract quotes from
            candidate_name (str, optional): Candidate name for relevance scoring
            parallel (bool, optional): Use parallel processing for large content
            
        Returns:
            list: List of quote dictionaries
        """
        if not text:
            return []
        
        quotes = []
        
        # Decide whether to use parallel processing
        if parallel and len(text) > 10000 and self.max_workers > 1:
            return self._extract_quotes_parallel(text, candidate_name)
        
        # Apply all patterns
        for pattern_idx, pattern in enumerate(self.compiled_quote_patterns):
            # Get all matches with their positions
            for match in pattern.finditer(text):
                try:
                    if pattern_idx >= 7:  # Special case for name + verb + quote pattern
                        quote_text = match.group(2)
                        name_mentioned = match.group(1)
                        
                        # Only add if the name is related to our candidate
                        if candidate_name and not self._is_related_name(name_mentioned, candidate_name):
                            continue
                    else:
                        quote_text = match.group(1)
                    
                    # Get match position
                    start_pos = max(0, match.start() - 50)
                    end_pos = min(len(text), match.end() + 50)
                    
                    # Extract context (text around the quote)
                    context = text[start_pos:end_pos]
                    
                    # Calculate confidence based on various factors
                    confidence = self._calculate_quote_confidence(quote_text, context, candidate_name)
                    
                    # Only add quotes with reasonable confidence
                    if confidence > 0.3 and len(quote_text) > 15:
                        quotes.append({
                            'text': quote_text,
                            'context': context,
                            'context_start': start_pos,
                            'context_end': end_pos,
                            'confidence': confidence
                        })
                except Exception as e:
                    logger.debug(f"Error extracting quote: {str(e)}")
                    continue
        
        # Remove duplicate quotes (same text)
        unique_quotes = []
        seen_texts = set()
        
        for quote in quotes:
            # Normalize the text for comparison (remove extra spaces, lowercase)
            normalized_text = ' '.join(quote['text'].lower().split())
            
            if normalized_text not in seen_texts:
                seen_texts.add(normalized_text)
                unique_quotes.append(quote)
        
        return unique_quotes
    
    def _extract_quotes_parallel(self, text, candidate_name=None):
        """
        Extract quotes using parallel processing for large text.
        
        Args:
            text (str): Text to extract quotes from
            candidate_name (str, optional): Candidate name for relevance scoring
            
        Returns:
            list: List of quote dictionaries
        """
        # Split text into chunks
        chunk_size = len(text) // self.max_workers
        chunks = []
        
        for i in range(0, len(text), chunk_size):
            # Use overlapping chunks to avoid missing quotes at boundaries
            start = max(0, i - 100)
            end = min(len(text), i + chunk_size + 100)
            chunks.append(text[start:end])
        
        # Process chunks in parallel
        all_quotes = []
        
        def process_chunk(chunk):
            return self._extract_quotes(chunk, candidate_name, parallel=False)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(process_chunk, chunks))
            
            for chunk_quotes in results:
                all_quotes.extend(chunk_quotes)
        
        # Deduplicate quotes
        unique_quotes = []
        seen_texts = set()
        
        for quote in all_quotes:
            # Normalize the text for comparison
            normalized_text = ' '.join(quote['text'].lower().split())
            
            if normalized_text not in seen_texts:
                seen_texts.add(normalized_text)
                unique_quotes.append(quote)
        
        return unique_quotes
    
    def _calculate_quote_confidence(self, quote_text, context, candidate_name=None):
        """
        Calculate confidence that this is a quote from the candidate.
        
        Args:
            quote_text (str): The quote text
            context (str): Context around the quote
            candidate_name (str, optional): Candidate name
            
        Returns:
            float: Confidence score (0-1)
        """
        confidence = 0.5  # Base confidence
        
        # Check if candidate name is in the context
        if candidate_name:
            found, score, _ = self.name_matcher.fuzzy_match_name(context, candidate_name, threshold=70)
            if found and score > 70:
                confidence += 0.3 * (score / 100)
        
        # Check for speech indicators in context
        speech_count = 0
        context_lower = context.lower()
        
        for indicator in self.speech_indicators:
            if indicator.lower() in context_lower:
                speech_count += 1
        
        confidence += min(0.3, speech_count * 0.1)
        
        # Reduce confidence for very short quotes
        if len(quote_text) < 20:
            confidence -= 0.1
        
        # Reduce confidence for quotes that seem like data/numbers rather than speech
        if re.match(r'^[\d\s.,%]+$', quote_text.strip()):
            confidence -= 0.3
        
        # Cap confidence between 0 and 1
        return max(0.0, min(1.0, confidence))
    
    def _is_related_name(self, name_text, candidate_name):
        """
        Check if a name mentioned in text is related to our candidate.
        
        Args:
            name_text (str): Name text from match
            candidate_name (str): Candidate name
            
        Returns:
            bool: True if related, False otherwise
        """
        if not name_text or not candidate_name:
            return False
        
        found, score, _ = self.name_matcher.fuzzy_match_name(name_text, candidate_name, threshold=70)
        return found and score > 70


def create_content_classifier(nlp_model=None, name_matcher=None, max_workers=2):
    """
    Factory function to create a content classifier.
    
    Args:
        nlp_model: Optional NLP model
        name_matcher: Optional name matcher
        max_workers (int): Maximum worker threads
        
    Returns:
        ContentClassifier: Content classifier instance
    """
    return ContentClassifier(
        nlp_model=nlp_model or NLP_MODEL,
        name_matcher=name_matcher,
        max_workers=max_workers
    )