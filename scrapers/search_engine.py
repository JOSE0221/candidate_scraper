"""
Search engine for the Mexican Municipal Candidates Scraper.

This module provides searching capabilities for candidate information
with content classification, relevance scoring, and duplicate filtering.
"""
import sys
import time
import random
import traceback
import pandas as pd
from datetime import datetime  # This was missing before
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger
from config.settings import MAX_RESULTS_PER_CANDIDATE, MIN_RELEVANCE_THRESHOLD, RETRY_DELAY
from processing.content_classifier import create_content_classifier
from processing.entity_recognizer import create_entity_recognizer
from scrapers.content_extractor import create_content_extractor

logger = get_logger(__name__)

class SearchEngine:
    """
    Advanced search engine for finding and processing candidate information
    with content classification and relevance scoring.
    """
    
    def __init__(self, db_manager, oxylabs_manager=None, content_classifier=None, 
                entity_recognizer=None, year_range=2, max_results=None, 
                min_relevance=None, max_workers=5):
        """
        Initialize the search engine.
        
        Args:
            db_manager: Database manager for storage and caching
            oxylabs_manager (optional): Oxylabs API manager for search
            content_classifier (optional): Content classifier
            entity_recognizer (optional): Entity recognizer
            year_range (int, optional): Year range for temporal filtering
            max_results (int, optional): Maximum results per candidate
            min_relevance (float, optional): Minimum relevance threshold
            max_workers (int, optional): Maximum worker threads
        """
        self.db = db_manager
        self.oxylabs = oxylabs_manager
        self.year_range = year_range
        self.max_results = max_results or MAX_RESULTS_PER_CANDIDATE
        self.min_relevance = min_relevance or MIN_RELEVANCE_THRESHOLD
        self.max_workers = max_workers
        
        # Create content extractor
        self.content_extractor = create_content_extractor(self.db, self.oxylabs)
        
        # Create content classifier if not provided
        self.content_classifier = content_classifier or create_content_classifier()
        
        # Create entity recognizer if not provided
        self.entity_recognizer = entity_recognizer or create_entity_recognizer()
    
    def build_search_query(self, candidate_name, municipality, target_year, include_party=False, party=None):
        """
        Build a search query optimized for Oxylabs API with improved variations.
        
        Args:
            candidate_name (str): Candidate name
            municipality (str): Municipality name
            target_year (int): Target election year
            include_party (bool, optional): Include party in query
            party (str, optional): Political party
            
        Returns:
            str: Formatted search query
        """
        # Clean and normalize inputs
        clean_name = candidate_name.replace('  ', ' ').strip()
        clean_municipality = municipality.replace('  ', ' ').strip()
        
        # More targeted query using quotes for precision on the name
        query = f'"{clean_name}" {clean_municipality} {target_year}'
        
        # Add minimal political context without overcomplicating
        # Randomize between different political terms for better coverage
        political_terms = [
            "presidente municipal", 
            "candidato ayuntamiento", 
            "elecciones municipales",
            "candidatura",
            "alcalde"
        ]
        
        # Add a random political term
        import random
        query += f" {random.choice(political_terms)}"
        
        # Add political party if available
        if include_party and party:
            clean_party = party.replace('  ', ' ').strip()
            query += f" {clean_party}"
        
        return query
    
    def perform_simplified_search(self, candidate):
        """
        Perform a simplified search as a last resort.
        
        Args:
            candidate (dict): Candidate information
            
        Returns:
            list: Search results
        """
        try:
            # Extract basic information
            candidate_name = self._validate_string(candidate.get('name', candidate.get('PRESIDENTE_MUNICIPAL')))
            municipality = self._validate_string(candidate.get('municipality', candidate.get('MUNICIPIO')))
            
            if not candidate_name or not municipality:
                return []
            
            # Just use name and municipality, no other terms
            simple_query = f"{candidate_name} {municipality}"
            
            logger.info(f"Performing simplified search: {simple_query}")
            
            if self.oxylabs:
                results = self.oxylabs.search(simple_query)
                logger.info(f"Simplified search found {len(results)} results")
                return results
            return []
        except Exception as e:
            logger.error(f"Error in simplified search: {str(e)}")
            return []
    
    def _build_search_with_name_variation(self, candidate_name, municipality, target_year, party=None):
        """
        Build a search query with name variations.
        
        Args:
            candidate_name (str): Candidate name
            municipality (str): Municipality name
            target_year (int): Target election year
            party (str, optional): Political party
            
        Returns:
            str: Formatted search query
        """
        name_parts = candidate_name.split()
        
        if len(name_parts) < 2:
            return self.build_search_query(candidate_name, municipality, target_year, include_party=True, party=party)
        
        # Use first and last name
        first_name = name_parts[0]
        last_name = name_parts[-1]
        
        if len(name_parts) >= 3:
            # For names with middle parts, try different combinations
            query = f'"{first_name} {last_name}" "{municipality}" {target_year}'
            
            if party:
                query += f' "{party}"'
            
            query += ' "candidato" OR "presidente municipal" OR "elección"'
            
            return query
        
        return self.build_search_query(candidate_name, municipality, target_year, include_party=True, party=party)
    
    def search_candidate(self, candidate, batch_id=None):
        """
        Search for information about a specific candidate with robust data validation.
        
        Args:
            candidate (dict): Candidate information
            batch_id (int, optional): Batch ID
            
        Returns:
            list: List of article IDs
        """
        try:
            # Extract and validate candidate information
            candidate_id = candidate.get('id')
            
            # Validate essential fields
            candidate_name = self._validate_string(candidate.get('name', candidate.get('PRESIDENTE_MUNICIPAL')))
            municipality = self._validate_string(candidate.get('municipality', candidate.get('MUNICIPIO')))
            
            # Try to convert target_year to int
            target_year = None
            year_value = candidate.get('target_year', candidate.get('Year'))
            if year_value is not None:
                try:
                    target_year = int(year_value)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid target year: {year_value}, skipping candidate")
                    return []
            
            # Verify essential fields are present
            if not candidate_name or not municipality or not target_year:
                logger.warning(f"Skipping search for candidate with missing essential data: {candidate}")
                return []
            
            # Extract optional fields with validation
            state = self._validate_string(candidate.get('entidad', candidate.get('ENTIDAD')))
            gender = self._validate_string(candidate.get('gender', candidate.get('SEXO')))
            party = self._validate_string(candidate.get('party', candidate.get('PARTIDO')))
            period = self._validate_string(candidate.get('period_format', candidate.get('PERIODO_FORMATO_ORIGINAL')))
            
            logger.info(f"Searching for: {candidate_name} in {municipality}, year {target_year}")
            
            # Get or create candidate in database
            if not candidate_id:
                candidate_obj, created = self.db.get_or_create_candidate(
                    candidate_name, municipality, target_year,
                    state=state, gender=gender, party=party, period=period
                )
                if candidate_obj:
                    candidate_id = candidate_obj.id
                else:
                    logger.warning(f"Failed to get/create candidate record, skipping search")
                    return []
            
            # Update progress
            self.db.update_candidate_progress(
                candidate_id=candidate_id,
                candidate_name=candidate_name, 
                municipality=municipality, 
                target_year=target_year,
                status='SEARCHING', 
                batch_id=batch_id
            )
            
            # First, try a focused search with candidate and municipality
            query = self.build_search_query(candidate_name, municipality, target_year, include_party=True, party=party)
            
            # Try to get cached results
            cached_results = self.db.get_cached_search(query)
            
            if cached_results:
                logger.info(f"Using cached search results for {candidate_name}")
                results = cached_results
            else:
                # Perform the search with Oxylabs
                if self.oxylabs:
                    results = self.oxylabs.search(query)
                    
                    # Cache results
                    self.db.cache_search(query, results)
                else:
                    # No search capability available
                    logger.warning("No search provider available")
                    results = []
            
            # Filter out blacklisted domains
            filtered_results = []
            for result in results:
                # Skip results without URL
                url = result.get('url', '')
                if not url:
                    continue
                    
                # Extract domain
                domain = urlparse(url).netloc
                if domain.startswith('www.'):
                    domain = domain[4:]
                    
                # Check if domain is blacklisted
                if not self.db.is_blacklisted(domain):
                    filtered_results.append(result)
            
            # Process search results
            return self._process_search_results(
                filtered_results[:self.max_results],
                candidate_id, candidate_name, municipality, target_year,
                state, gender, party, period, batch_id
            )
        
        except Exception as e:
            logger.error(f"Error in search_candidate: {str(e)}")
            traceback.print_exc()
            return []   

    def _validate_string(self, value):
        """
        Validate and clean a string value with comprehensive error handling.
        
        Args:
            value: Value to validate as string
            
        Returns:
            str or None: Validated string or None if invalid
        """
        # Return None for None or NaN values
        if value is None or pd.isna(value):
            return None
            
        # Handle string values
        if isinstance(value, str):
            # Strip whitespace
            cleaned = value.strip()
            # Return None for empty strings
            return cleaned if cleaned else None
        
        # Handle numeric values by converting to string
        elif isinstance(value, (int, float)):
            # Skip NaN values
            if pd.isna(value):
                return None
            # Convert to string
            return str(value)
        
        # Return None for other types
        return None 
    
    def _process_search_results(self, results, candidate_id, candidate_name, municipality, 
                              target_year, state=None, gender=None, party=None, 
                              period=None, batch_id=None):
        """
        Process search results.
        
        Args:
            results (list): Search results
            candidate_id (int): Candidate ID
            candidate_name (str): Candidate name
            municipality (str): Municipality name
            target_year (int): Target election year
            state (str, optional): State name
            gender (str, optional): Candidate gender
            party (str, optional): Political party
            period (str, optional): Period format
            batch_id (int, optional): Batch ID
            
        Returns:
            list: List of article IDs
        """
        # Update candidate progress to EXTRACTING
        self.db.update_candidate_progress(
            candidate_id=candidate_id,
            candidate_name=candidate_name, 
            municipality=municipality, 
            target_year=target_year,
            status='EXTRACTING', 
            batch_id=batch_id
        )
        
        # Process results concurrently for better performance
        processed_results = []
        saved_articles = []
        
        if not results:
            logger.warning(f"No search results found for {candidate_name}")
            # Update progress as completed with no results
            self.db.update_candidate_progress(
                candidate_id=candidate_id,
                candidate_name=candidate_name, 
                municipality=municipality, 
                target_year=target_year,
                status='COMPLETED', 
                articles_found=0,
                batch_id=batch_id
            )
            return saved_articles
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_result = {
                executor.submit(
                    self._process_single_result, 
                    result, candidate_name, target_year, self.year_range
                ): result 
                for result in results
            }
            
            # Process results as they complete
            for future in future_to_result:
                try:
                    processed_result = future.result()
                    if processed_result and processed_result.get('success'):
                        processed_results.append(processed_result)
                except Exception as e:
                    logger.error(f"Error processing search result: {str(e)}")
        
        # Sort by overall relevance and take top results
        processed_results.sort(key=lambda x: x.get('overall_relevance', 0), reverse=True)
        
        # Only keep results above the minimum relevance threshold
        relevant_results = [r for r in processed_results if r.get('overall_relevance', 0) >= self.min_relevance]
        
        # Save articles to database
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all save tasks
            future_to_article = {
                executor.submit(
                    self._save_article, 
                    article_data, candidate_id, candidate_name, municipality, 
                    target_year, state, gender, party, period, batch_id
                ): article_data 
                for article_data in relevant_results
            }
            
            # Process results as they complete
            for future in future_to_article:
                try:
                    article_id = future.result()
                    if article_id:
                        saved_articles.append(article_id)
                except Exception as e:
                    logger.error(f"Error saving article: {str(e)}")
        
        # Update progress as completed
        self.db.update_candidate_progress(
            candidate_id=candidate_id,
            candidate_name=candidate_name, 
            municipality=municipality, 
            target_year=target_year,
            status='COMPLETED', 
            articles_found=len(saved_articles),
            batch_id=batch_id
        )
        
        return saved_articles
    
    def _process_single_result(self, result, candidate_name, target_year, year_range):
        """
        Process a single search result with more lenient thresholds.
        
        Args:
            result (dict): Search result
            candidate_name (str): Candidate name
            target_year (int): Target election year
            year_range (int): Year range for filtering
            
        Returns:
            dict: Processed result
        """
        try:
            url = result.get('url', '')
            if not url:
                return None
            
            # Extract content with random delay to avoid overloading servers
            import random
            import time
            time.sleep(random.uniform(0.3, 1.0))
            
            extraction_result = self.content_extractor.extract_content(
                url, candidate_name=candidate_name, target_year=target_year, year_range=year_range
            )
            
            if not extraction_result.get('success'):
                logger.warning(f"Content extraction failed for {url}")
                return None
                
            if not extraction_result.get('content'):
                logger.warning(f"No content extracted from {url}")
                return None
            
            # Classify content type and extract quotes
            content_type_result = self.content_classifier.classify_content(
                extraction_result.get('content'), candidate_name
            )
            
            # Calculate temporal relevance with more lenient interpretation
            temporal_score, year_lower, year_upper = self.entity_recognizer.calculate_temporal_relevance(
                extraction_result.get('content'),
                extraction_result.get('extracted_date'),
                target_year,
                year_range
            )
            
            # Be more lenient with temporal relevance
            if temporal_score < 0.1:  # Was 0.2
                logger.info(f"Low temporal score ({temporal_score:.2f}) but keeping: {url}")
                # Boost the score slightly to keep it in consideration
                temporal_score = 0.15
            
            # Calculate content relevance
            content_score = self.entity_recognizer.calculate_content_relevance(
                extraction_result.get('content'),
                candidate_name
            )
            
            # Be more lenient with content relevance
            if content_score < self.min_relevance / 2:
                logger.info(f"Low content score ({content_score:.2f}) but keeping: {url}")
                # Boost the score slightly
                content_score = max(content_score, self.min_relevance / 2)
            
            # Calculate overall relevance (weighted average) with adjusted formula
            # Give more weight to content relevance and less to temporal
            overall_relevance = (temporal_score * 0.3) + (content_score * 0.7)
            
            # Add bonus for content type
            if content_type_result.get('content_type') == 'discourse':
                # Prioritize content with candidate quotes
                overall_relevance += 0.2 * content_type_result.get('confidence', 0.0)
            elif content_type_result.get('content_type') == 'news':
                # Also boost news articles slightly
                overall_relevance += 0.1 * content_type_result.get('confidence', 0.0)
            
            # Extract biographical scores
            bio_score = self.entity_recognizer.calculate_biographical_score(
                extraction_result.get('content'), candidate_name
            )
            
            # Extract political scores
            political_score = self.entity_recognizer.calculate_political_score(
                extraction_result.get('content'), candidate_name
            )
            
            # Extract academic, professional & public service scores
            academic_score = self.entity_recognizer.calculate_academic_score(
                extraction_result.get('content'), candidate_name
            )
            
            professional_score = self.entity_recognizer.calculate_professional_score(
                extraction_result.get('content'), candidate_name
            )
            
            public_service_score = self.entity_recognizer.calculate_public_service_score(
                extraction_result.get('content'), candidate_name
            )
            
            # Extract entities
            entities = self.entity_recognizer.extract_entities(
                extraction_result.get('content'), candidate_name
            )
            
            # Calculate name match score using fuzzy matching
            found, match_score, _ = self.name_matcher.fuzzy_match_name(
                extraction_result.get('content'), candidate_name
            )
            name_match_score = match_score / 100 if found else 0.0
            
            # Boost overall relevance if name is found
            if name_match_score > 0.7:
                overall_relevance = min(1.0, overall_relevance + 0.1)
            
            # Ensure overall_relevance is capped at 1.0
            overall_relevance = min(1.0, overall_relevance)
            
            # Prepare result data
            from datetime import datetime
            
            result_data = {
                'success': True,
                'url': url,
                'title': extraction_result.get('title', ''),
                'snippet': result.get('snippet', ''),
                'content': extraction_result.get('content', ''),
                'html_content': extraction_result.get('html_content', ''),
                'source': self._extract_domain(url),
                'extracted_date': extraction_result.get('extracted_date'),
                'language': extraction_result.get('language', 'es'),
                'search_query': result.get('search_query', ''),
                
                'content_type': content_type_result.get('content_type', 'unknown'),
                'content_type_confidence': content_type_result.get('confidence', 0.0),
                'quotes': content_type_result.get('quotes', []),
                'quote_count': content_type_result.get('quote_count', 0),
                
                'temporal_relevance': temporal_score,
                'content_relevance': content_score,
                'overall_relevance': overall_relevance,
                'year_lower_bound': year_lower,
                'year_upper_bound': year_upper,
                
                'name_match_score': name_match_score,
                'fuzzy_match_score': name_match_score,
                'biographical_content_score': bio_score,
                'political_content_score': political_score,
                'academic_score': academic_score,
                'professional_score': professional_score,
                'public_service_score': public_service_score,
                
                'entities': entities,
                'date_confidence': 1.0 if extraction_result.get('extracted_date') else 0.5,
                'extraction_date': extraction_result.get('extraction_date', 
                                                        datetime.now().isoformat()),
                'from_cache': extraction_result.get('from_cache', False)
            }
            
            return result_data
                
        except Exception as e:
            logger.error(f"Error processing result {url}: {str(e)}")
            traceback.print_exc()
            return None
    
    def _extract_domain(self, url):
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ''
    
    def _save_article(self, article_data, candidate_id, candidate_name, municipality, 
                    target_year, state=None, gender=None, party=None, period=None, 
                    batch_id=None):
        """
        Save article and relationship to candidate.
        
        Args:
            article_data (dict): Article data
            candidate_id (int): Candidate ID
            candidate_name (str): Candidate name
            municipality (str): Municipality name
            target_year (int): Target election year
            state (str, optional): State name
            gender (str, optional): Candidate gender
            party (str, optional): Political party
            period (str, optional): Period format
            batch_id (int, optional): Batch ID
            
        Returns:
            int: Article ID or None on error
        """
        try:
            # Add candidate information
            article_data_copy = article_data.copy()  # Create a copy to avoid modifying the original
            
            if 'candidato' not in article_data_copy:
                article_data_copy['candidato'] = candidate_name
            if 'municipio' not in article_data_copy:
                article_data_copy['municipio'] = municipality
            if 'target_year' not in article_data_copy:
                article_data_copy['target_year'] = target_year
            if state and 'entidad' not in article_data_copy:
                article_data_copy['entidad'] = state
            if gender and 'sexo' not in article_data_copy:
                article_data_copy['sexo'] = gender
            if party and 'partido' not in article_data_copy:
                article_data_copy['partido'] = party
            if period and 'periodo_formato_original' not in article_data_copy:
                article_data_copy['periodo_formato_original'] = period
            if batch_id and 'batch_id' not in article_data_copy:
                article_data_copy['batch_id'] = batch_id
            
            # Save article to database with retry logic
            for attempt in range(3):  # Try up to 3 times
                try:
                    article_id = self.db.save_article(article_data_copy, batch_id)
                    return article_id
                except Exception as e:
                    if "database is locked" in str(e) and attempt < 2:
                        # Database is locked, wait and retry
                        wait_time = (attempt + 1) * 1.5  # Exponential backoff
                        logger.warning(f"Database locked when saving article, retrying in {wait_time}s (attempt {attempt+1}/3)")
                        time.sleep(wait_time)
                    else:
                        # Other error or final attempt failed
                        raise
            
            return None
            
        except Exception as e:
            logger.error(f"Error saving article data: {str(e)}")
            traceback.print_exc()
            return None
    
    def search_candidate_enhanced(self, candidate, batch_id=None):
        """
        Enhanced search to get relevant links per candidate using multiple strategies
        with improved query construction and fallback mechanisms.
        
        Args:
            candidate (dict): Candidate information
            batch_id (int, optional): Batch ID
            
        Returns:
            list: List of article IDs
        """
        try:
            # Extract and validate candidate information with robust error handling
            candidate_id = candidate.get('id')
            
            # Validate essential fields
            candidate_name = self._validate_string(candidate.get('name', candidate.get('PRESIDENTE_MUNICIPAL')))
            municipality = self._validate_string(candidate.get('municipality', candidate.get('MUNICIPIO')))
            
            # Try to convert target_year to int with proper validation
            target_year = None
            year_value = candidate.get('target_year', candidate.get('Year'))
            if year_value is not None:
                try:
                    target_year = int(year_value)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid target year: {year_value}, using approximate value")
                    # Try to extract just the numeric part
                    if isinstance(year_value, str):
                        import re
                        year_match = re.search(r'\d{4}', year_value)
                        if year_match:
                            target_year = int(year_match.group(0))
            
            # Verify essential fields are present
            if not candidate_name or not municipality:
                logger.warning(f"Skipping search for candidate with missing essential data: {candidate}")
                return []
                    
            # Use current year as fallback if target_year is still missing
            if not target_year:
                import datetime
                target_year = datetime.datetime.now().year
                logger.warning(f"Using current year {target_year} as fallback for missing target year")
            
            # Extract optional fields with validation
            state = self._validate_string(candidate.get('entidad', candidate.get('ENTIDAD')))
            gender = self._validate_string(candidate.get('gender', candidate.get('SEXO')))
            party = self._validate_string(candidate.get('party', candidate.get('PARTIDO')))
            period = self._validate_string(candidate.get('period_format', candidate.get('PERIODO_FORMATO_ORIGINAL')))
            
            logger.info(f"Enhanced search for: {candidate_name} in {municipality}, year {target_year}")
            
            # Get or create candidate in database with proper error handling
            if not candidate_id:
                try:
                    candidate_obj, created = self.db.get_or_create_candidate(
                        candidate_name, municipality, target_year,
                        state=state, gender=gender, party=party, period=period
                    )
                    if candidate_obj:
                        candidate_id = candidate_obj.id
                    else:
                        logger.warning(f"Failed to get/create candidate record, using temporary ID")
                        import random
                        candidate_id = f"temp_{random.randint(10000, 99999)}"
                except Exception as e:
                    logger.error(f"Database error when creating candidate: {str(e)}")
                    import random
                    candidate_id = f"temp_{random.randint(10000, 99999)}"
            
            # Update progress
            try:
                self.db.update_candidate_progress(
                    candidate_id=candidate_id,
                    candidate_name=candidate_name, 
                    municipality=municipality, 
                    target_year=target_year,
                    status='SEARCHING', 
                    batch_id=batch_id
                )
            except Exception as e:
                logger.error(f"Error updating candidate progress: {str(e)}")
            
            # Build search queries using multiple strategies for maximum coverage
            queries = []
            
            # STRATEGY 1: Name variations with different formats
            
            # Clean and normalize candidate name for better matching
            clean_name = candidate_name.replace('  ', ' ').strip()
            
            # A) Basic name formats without restrictive quotes
            queries.append(f'{clean_name} {municipality} {target_year}')
            queries.append(f'{clean_name} {municipality} presidente municipal {target_year}')
            
            # B) Add gender-specific terms if known
            if gender == 'F':
                queries.append(f'candidata {clean_name} {municipality} {target_year}')
                queries.append(f'presidenta municipal {clean_name} {municipality} {target_year}')
            else:
                queries.append(f'candidato {clean_name} {municipality} {target_year}')
                queries.append(f'presidente municipal {clean_name} {municipality} {target_year}')
            
            # C) Add political party if available
            if party:
                # Clean party name and convert common abbreviations
                clean_party = party.replace('  ', ' ').strip()
                queries.append(f'{clean_name} {municipality} {clean_party} {target_year}')
                queries.append(f'{clean_name} candidato {clean_party} {municipality} {target_year}')
            
            # STRATEGY 2: Try different name components for candidates with multiple names
            name_parts = clean_name.split()
            if len(name_parts) >= 3:
                # For names with 3+ parts, try different combinations
                first_name = name_parts[0]
                last_names = ' '.join(name_parts[-2:])  # Use last two parts as surnames
                
                queries.append(f'{first_name} {last_names} {municipality} {target_year}')
                queries.append(f'{first_name} {name_parts[-1]} {municipality} {target_year}')
                
                # Try with partido
                if party:
                    queries.append(f'{first_name} {last_names} {party} {municipality} {target_year}')
            
            # STRATEGY 3: Use period-specific terms
            election_terms = [
                "elecciones", 
                "candidatura", 
                "campaña electoral",
                "voto", 
                "alcaldía",
                "ayuntamiento"
            ]
            
            # Choose 2 random election terms to add to queries
            import random
            selected_terms = random.sample(election_terms, min(2, len(election_terms)))
            for term in selected_terms:
                queries.append(f'{clean_name} {municipality} {term} {target_year}')
            
            # STRATEGY 4: Use year ranges to capture more content
            # Include searches for the years around the target
            year_range = self.year_range or 2
            for year_offset in range(-year_range, year_range + 1):
                if year_offset == 0:  # Skip the exact year as it's already covered
                    continue
                    
                nearby_year = target_year + year_offset
                queries.append(f'{clean_name} {municipality} {nearby_year}')
                
            # STRATEGY 5: Add very specific exact match query with quotes
            queries.append(f'"{clean_name}" "{municipality}" {target_year}')
            
            # STRATEGY 6: Add specific Mexican news sources for better targeting
            news_sources = ["El Universal", "Excelsior", "Milenio", "Reforma", "La Jornada"]
            selected_source = random.choice(news_sources)
            queries.append(f'{clean_name} {municipality} {target_year} {selected_source}')
            
            # Deduplicate queries
            unique_queries = list(dict.fromkeys(queries))
            
            # Log the search strategies
            logger.info(f"Generated {len(unique_queries)} search queries for {candidate_name}")
            
            # For each query, store a mapping to extracted results
            all_results = []
            seen_urls = set()
            domains_searched = set()
            
            # Process queries in sequence with proper error handling
            for i, query in enumerate(unique_queries):
                try:
                    # Add a short delay between queries to avoid rate limiting
                    if i > 0:
                        import time
                        time.sleep(random.uniform(0.5, 1.5))
                    
                    # Try to get cached results first
                    cached_results = self.db.get_cached_search(query)
                    
                    results = None
                    if cached_results:
                        logger.info(f"Using cached search results for query: {query[:50]}...")
                        results = cached_results
                    else:
                        # Perform the search with Oxylabs
                        if self.oxylabs:
                            results = self.oxylabs.search(query)
                            
                            # Check for API errors
                            if isinstance(results, dict) and 'error' in results:
                                logger.warning(f"Oxylabs search error: {results.get('error')}")
                                continue
                            
                            # Cache results if we got some
                            if results and len(results) > 0:
                                self.db.cache_search(query, results)
                        else:
                            # No search capability available
                            logger.warning("No search provider available")
                            results = []
                    
                    # Process and filter results
                    if results:
                        logger.info(f"Found {len(results)} results for query: {query[:50]}...")
                        
                        # Filter and merge results
                        for result in results:
                            url = result.get('url', '')
                            if not url or url in seen_urls:
                                continue
                                
                            # Extract domain for blacklist checking and duplicate prevention
                            domain = self._extract_domain(url)
                                
                            # Track domains to understand search coverage
                            domains_searched.add(domain)
                                
                            # Check if domain is blacklisted
                            if not self.db.is_blacklisted(domain):
                                # Add query used to the result for tracking
                                result['search_query'] = query
                                all_results.append(result)
                                seen_urls.add(url)
                    else:
                        logger.info(f"No results found for query: {query[:50]}...")
                except Exception as e:
                    logger.error(f"Error processing query '{query[:50]}...': {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            # Log search summary
            logger.info(f"Complete search for {candidate_name}: {len(all_results)} total results from {len(domains_searched)} domains")
            
            # If we found no results, try simplified search approaches
            if not all_results:
                logger.warning(f"No results found with primary searches for {candidate_name}, trying simplified approaches")
                
                # Try simplified search
                simplified_results = self.perform_simplified_search(candidate)
                
                if simplified_results:
                    logger.info(f"Simplified search found {len(simplified_results)} results")
                    for result in simplified_results:
                        url = result.get('url', '')
                        if not url or url in seen_urls:
                            continue
                            
                        domain = self._extract_domain(url)
                        if not self.db.is_blacklisted(domain):
                            result['search_query'] = f"simplified: {candidate_name} {municipality}"
                            all_results.append(result)
                            seen_urls.add(url)
                
                # If still no results and Oxylabs is available, try one more approach
                if not all_results and self.oxylabs:
                    try:
                        # Final attempt with Google-friendly search syntax
                        direct_query = f'"{clean_name}" {municipality} {target_year} site:.mx'
                        logger.info(f"Attempting final search with query: {direct_query}")
                        
                        final_results = self.oxylabs.search(direct_query)
                        
                        if final_results and len(final_results) > 0:
                            for result in final_results:
                                url = result.get('url', '')
                                if not url or url in seen_urls:
                                    continue
                                    
                                domain = self._extract_domain(url)
                                if not self.db.is_blacklisted(domain):
                                    result['search_query'] = direct_query
                                    all_results.append(result)
                                    seen_urls.add(url)
                    except Exception as e:
                        logger.error(f"Error in final search attempt: {str(e)}")
            
            # Process the combined search results with max_results limit
            final_results = all_results[:self.max_results] if all_results else []
            
            if final_results:
                logger.info(f"Proceeding with {len(final_results)} results for {candidate_name}")
            else:
                logger.warning(f"No search results found for {candidate_name}")
                    
                # Update progress as completed with no results
                self.db.update_candidate_progress(
                    candidate_id=candidate_id,
                    candidate_name=candidate_name, 
                    municipality=municipality, 
                    target_year=target_year,
                    status='COMPLETED', 
                    articles_found=0,
                    batch_id=batch_id
                )
                return []
            
            # Process the results with _process_search_results
            return self._process_search_results(
                final_results,
                candidate_id, candidate_name, municipality, target_year,
                state, gender, party, period, batch_id
            )
                
        except Exception as e:
            logger.error(f"Unhandled error in search_candidate_enhanced: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    @property
    def name_matcher(self):
        """Get the name matcher from the entity recognizer"""
        return self.entity_recognizer.name_matcher


def create_search_engine(db_manager, oxylabs_manager=None, content_classifier=None, 
                       entity_recognizer=None, year_range=2, max_results=None, 
                       min_relevance=None, max_workers=5):
    """
    Factory function to create a search engine.
    
    Args:
        db_manager: Database manager
        oxylabs_manager (optional): Oxylabs API manager
        content_classifier (optional): Content classifier
        entity_recognizer (optional): Entity recognizer
        year_range (int, optional): Year range for filtering
        max_results (int, optional): Maximum results per candidate
        min_relevance (float, optional): Minimum relevance threshold
        max_workers (int, optional): Maximum worker threads
        
    Returns:
        SearchEngine: Search engine instance
    """
    return SearchEngine(
        db_manager=db_manager,
        oxylabs_manager=oxylabs_manager,
        content_classifier=content_classifier,
        entity_recognizer=entity_recognizer,
        year_range=year_range,
        max_results=max_results,
        min_relevance=min_relevance,
        max_workers=max_workers
    )