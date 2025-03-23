"""
Advanced name matching utilities for the Mexican Municipal Candidates Scraper.

This module provides powerful fuzzy name matching using RapidFuzz,
with specialized handling for Spanish name patterns and optimizations
for performance when dealing with large text.
"""
import re
import unicodedata
import sys
from pathlib import Path

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger

logger = get_logger(__name__)

# Try to import RapidFuzz
try:
    from rapidfuzz import fuzz, process, utils
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    logger.warning("RapidFuzz not available. Install with 'pip install rapidfuzz' for better name matching.")
    RAPIDFUZZ_AVAILABLE = False

# Fallback to FuzzyWuzzy if RapidFuzz is not available
if not RAPIDFUZZ_AVAILABLE:
    try:
        from fuzzywuzzy import fuzz, process
        FUZZYWUZZY_AVAILABLE = True
        logger.warning("Using FuzzyWuzzy instead of RapidFuzz. Consider upgrading for better performance.")
    except ImportError:
        logger.warning("FuzzyWuzzy not available. Install with 'pip install fuzzywuzzy python-Levenshtein' for name matching.")
        FUZZYWUZZY_AVAILABLE = False


class NameMatcher:
    """
    Advanced name matcher using RapidFuzz (or FuzzyWuzzy as fallback)
    with specialized handling for Spanish names.
    """
    
    def __init__(self):
        """Initialize the name matcher."""
        self.fuzzy_available = RAPIDFUZZ_AVAILABLE or FUZZYWUZZY_AVAILABLE
        
        # Spanish honorifics and titles
        self.honorifics = [
            "lic.", "licenciado", "licenciada", 
            "dr.", "doctor", "doctora", 
            "ing.", "ingeniero", "ingeniera", 
            "mtro.", "maestro", "maestra", 
            "prof.", "profesor", "profesora",
            "sr.", "señor", "sra.", "señora", 
            "don", "doña"
        ]
        
        # Political roles
        self.roles = [
            "candidato", "candidata", 
            "alcalde", "alcaldesa", 
            "presidente municipal", "presidenta municipal",
            "regidor", "regidora",
            "síndico", "síndica",
            "diputado", "diputada",
            "senador", "senadora"
        ]
        
        # Prepositions in Spanish names
        self.prepositions = ["de", "del", "de la", "de los", "y"]
        
        # Maximum text chunk size for processing
        self.max_chunk_size = 5000
    
    def _normalize_text(self, text):
        """
        Normalize text by removing accents and converting to lowercase.
        
        Args:
            text (str): Text to normalize
            
        Returns:
            str: Normalized text
        """
        if not text:
            return ""
        # Remove accents
        text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        # Convert to lowercase
        return text.lower()
    
    def _generate_name_variations(self, name):
        """
        Generate variations of a name.
        
        Args:
            name (str): Original name
            
        Returns:
            list: List of name variations
        """
        if not name or len(name) < 3:
            return [name.lower()] if name else []
        
        variations = [name.lower()]
        
        # Add normalized version
        normalized = self._normalize_text(name)
        if normalized != name.lower():
            variations.append(normalized)
        
        # Split full name
        name_parts = name.lower().split()
        
        if len(name_parts) >= 2:
            # First name + last name
            first_name = name_parts[0]
            last_name = name_parts[-1]
            variations.append(f"{first_name} {last_name}")
            
            # Handle common Hispanic naming patterns
            if len(name_parts) >= 3:
                # For names like "Juan Carlos Pérez López"
                # First two names + last surname
                first_two_names = " ".join(name_parts[:2])
                variations.append(f"{first_two_names} {last_name}")
                
                # First name + both surnames
                if len(name_parts) >= 4:
                    paternal_surname = name_parts[-2]
                    full_last_name = f"{paternal_surname} {last_name}"
                    variations.append(f"{first_name} {full_last_name}")
            
            # Handle prepositions
            for i, part in enumerate(name_parts):
                if part in self.prepositions and i > 0 and i < len(name_parts) - 1:
                    # Create variations without the preposition
                    parts_without_prep = name_parts.copy()
                    parts_without_prep.pop(i)
                    variations.append(" ".join(parts_without_prep))
        
        return list(set(variations))
    
    def fuzzy_match_name(self, text, candidate_name, threshold=80, context_size=None):
        """
        Use fuzzy matching to find candidate name in text.
        
        Args:
            text (str): Text to search in
            candidate_name (str): Candidate name to look for
            threshold (int, optional): Matching threshold (0-100). Defaults to 80.
            context_size (int, optional): Size of text chunks to process. Defaults to None.
            
        Returns:
            tuple: (bool found, float score, str matched_text)
        """
        if not self.fuzzy_available or not text or not candidate_name:
            return False, 0, None
            
        # Direct match first (fastest)
        candidate_lower = candidate_name.lower()
        normalized_candidate = self._normalize_text(candidate_name)
        
        if candidate_lower in text.lower():
            return True, 100, candidate_lower
        
        if normalized_candidate != candidate_lower and normalized_candidate in text.lower():
            return True, 95, normalized_candidate
        
        # Get name variations
        variations = self._generate_name_variations(candidate_name)
        
        for variation in variations:
            if variation in text.lower():
                return True, 95, variation
        
        # Handle large text by chunking
        if len(text) > self.max_chunk_size:
            return self._chunk_and_match(text, candidate_name, variations, threshold, context_size)
        
        # RapidFuzz implementation
        if RAPIDFUZZ_AVAILABLE:
            return self._match_with_rapidfuzz(text, candidate_name, variations, threshold)
        
        # FuzzyWuzzy implementation (fallback)
        if FUZZYWUZZY_AVAILABLE:
            return self._match_with_fuzzywuzzy(text, candidate_name, variations, threshold)
        
        return False, 0, None
    
    def _chunk_and_match(self, text, candidate_name, variations, threshold=80, context_size=None):
        """
        Chunk large text and match in smaller pieces.
        
        Args:
            text (str): Text to search in
            candidate_name (str): Candidate name to look for
            variations (list): List of name variations
            threshold (int, optional): Matching threshold. Defaults to 80.
            context_size (int, optional): Size of text chunks. Defaults to None.
            
        Returns:
            tuple: (bool found, float score, str matched_text)
        """
        chunk_size = context_size or self.max_chunk_size
        text_lower = text.lower()
        
        # Break text into overlapping chunks to avoid splitting names
        chunks = []
        for i in range(0, len(text_lower), chunk_size // 2):
            chunk = text_lower[i:i + chunk_size]
            chunks.append(chunk)
        
        best_score = 0
        best_match = None
        found = False
        
        for chunk in chunks:
            # First check for direct matches in chunk
            for variation in variations:
                if variation in chunk:
                    return True, 95, variation
            
            # Then try fuzzy matching
            if RAPIDFUZZ_AVAILABLE:
                chunk_found, chunk_score, chunk_match = self._match_with_rapidfuzz(chunk, candidate_name, variations, threshold)
            elif FUZZYWUZZY_AVAILABLE:
                chunk_found, chunk_score, chunk_match = self._match_with_fuzzywuzzy(chunk, candidate_name, variations, threshold)
            else:
                return False, 0, None
            
            if chunk_found and chunk_score > best_score:
                best_score = chunk_score
                best_match = chunk_match
                found = True
        
        return found, best_score, best_match
    
    def _match_with_rapidfuzz(self, text, candidate_name, variations, threshold=80):
        """
        Match using RapidFuzz for optimal performance.
        
        Args:
            text (str): Text to search in
            candidate_name (str): Candidate name to look for
            variations (list): List of name variations
            threshold (int, optional): Matching threshold. Defaults to 80.
            
        Returns:
            tuple: (bool found, float score, str matched_text)
        """
        text_lower = text.lower()
        
        # Try to extract best matches for each variation
        best_score = 0
        best_match = None
        
        for variation in variations:
            # Try partial token ratio for more flexible matching
            score = fuzz.partial_token_ratio(variation, text_lower)
            
            if score > best_score:
                best_score = score
                best_match = variation
        
        # Try partial token sort ratio with original name
        score = fuzz.partial_token_sort_ratio(candidate_name.lower(), text_lower)
        if score > best_score:
            best_score = score
            best_match = candidate_name.lower()
        
        # Also try partial ratio for catching mentions in flowing text
        for variation in variations:
            matches = process.extract(
                variation, 
                [text_lower], 
                scorer=fuzz.partial_ratio,
                score_cutoff=threshold
            )
            
            if matches:
                match_score = matches[0][1]
                if match_score > best_score:
                    best_score = match_score
                    best_match = variation
        
        if best_score >= threshold:
            return True, best_score, best_match
        
        return False, 0, None
    
    def _match_with_fuzzywuzzy(self, text, candidate_name, variations, threshold=80):
        """
        Match using FuzzyWuzzy (fallback).
        
        Args:
            text (str): Text to search in
            candidate_name (str): Candidate name to look for
            variations (list): List of name variations
            threshold (int, optional): Matching threshold. Defaults to 80.
            
        Returns:
            tuple: (bool found, float score, str matched_text)
        """
        text_lower = text.lower()
        
        # Try to find the best match score
        best_score = 0
        best_match = None
        
        # Try token sort ratio first (for handling word order)
        score = fuzz.token_sort_ratio(candidate_name.lower(), text_lower)
        if score > best_score:
            best_score = score
            best_match = candidate_name.lower()
        
        # Try partial ratio for each variation
        for variation in variations:
            score = fuzz.partial_ratio(variation, text_lower)
            if score > best_score:
                best_score = score
                best_match = variation
        
        # Try the token set ratio (for subset matching)
        score = fuzz.token_set_ratio(candidate_name.lower(), text_lower)
        if score > best_score:
            best_score = score
            best_match = candidate_name.lower()
        
        if best_score >= threshold:
            return True, best_score, best_match
        
        return False, 0, None
    
    def extract_name_contexts(self, text, candidate_name, context_size=100, max_contexts=3):
        """
        Extract contexts around name mentions.
        
        Args:
            text (str): Text to search in
            candidate_name (str): Candidate name to look for
            context_size (int, optional): Size of context window. Defaults to 100.
            max_contexts (int, optional): Maximum number of contexts to extract. Defaults to 3.
            
        Returns:
            list: List of context dictionaries
        """
        if not text or not candidate_name:
            return []
        
        # Get name variations
        variations = self._generate_name_variations(candidate_name)
        variations.append(candidate_name.lower())
        
        contexts = []
        text_lower = text.lower()
        
        for variation in variations:
            start_pos = 0
            while start_pos < len(text_lower) and len(contexts) < max_contexts:
                pos = text_lower.find(variation, start_pos)
                if pos == -1:
                    break
                
                # Get context around the name
                context_start = max(0, pos - context_size // 2)
                context_end = min(len(text), pos + len(variation) + context_size // 2)
                
                # Extract the context
                context = text[context_start:context_end]
                
                # Add to contexts
                contexts.append({
                    'text': context,
                    'start': context_start,
                    'end': context_end,
                    'match': variation,
                    'match_position': pos - context_start
                })
                
                # Move to next position
                start_pos = pos + len(variation)
        
        # Also try fuzzy matching
        if self.fuzzy_available and len(contexts) < max_contexts:
            found, score, match = self.fuzzy_match_name(text, candidate_name, threshold=85)
            
            if found and match and score > 85:
                # Try to locate the match in the text
                if RAPIDFUZZ_AVAILABLE:
                    # Use RapidFuzz to find the best match position
                    matches = process.extract(
                        match,
                        [text_lower], 
                        scorer=fuzz.partial_ratio, 
                        score_cutoff=85
                    )
                    
                    if matches and len(matches) > 0:
                        # Get a context around the fuzzy match
                        text_chunks = text_lower.split()
                        for i, chunk in enumerate(text_chunks):
                            if fuzz.ratio(chunk, match) > 80:
                                start_idx = max(0, i - 10)
                                end_idx = min(len(text_chunks), i + 10)
                                context_text = ' '.join(text_chunks[start_idx:end_idx])
                                
                                contexts.append({
                                    'text': context_text,
                                    'start': 0,  # Approximate
                                    'end': len(context_text),  # Approximate
                                    'match': match,
                                    'match_position': -1,  # Unknown exact position
                                    'fuzzy_score': score
                                })
                                break
        
        return contexts[:max_contexts]


def create_name_matcher():
    """
    Factory function to create a name matcher.
    
    Returns:
        NameMatcher: Name matcher instance
    """
    return NameMatcher()