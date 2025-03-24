"""
Database manager for the Mexican Municipal Candidates Scraper.
"""
import os
import sqlite3
import json
import time
from datetime import datetime
import pandas as pd
import traceback
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger
from database.models import (
    Schema, hash_string, Candidate, Article, CandidateArticle, 
    Quote, EntityMention, ScrapingBatch, CandidateProfile
)
from config.settings import DEFAULT_BLACKLIST, load_blacklist

logger = get_logger(__name__)

class DatabaseManager:
    """
    Enhanced database manager with improved concurrency support and robust error handling.
    """
    
    def __init__(self, db_path, initialize=True, max_workers=5):
        """
        Initialize the database manager.
        
        Args:
            db_path (str): Path to the SQLite database file
            initialize (bool, optional): Whether to initialize the database. Defaults to True.
            max_workers (int, optional): Maximum number of worker threads for parallel operations.
        """
        self.db_path = db_path
        self.max_workers = max_workers
        
        # Create directory for database if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        
        # Cache for frequently accessed data
        self._cache = {
            'blacklist': set(),
            'candidates': {},
            'municipalities': {}
        }
        
        # Initialize database schema
        if initialize:
            self._initialize_db()
    
    def get_connection(self):
        """
        Get a database connection with row factory enabled.
        
        Returns:
            sqlite3.Connection: Database connection
        """
        conn = sqlite3.connect(self.db_path, timeout=30)  # 30-second timeout
        conn.row_factory = sqlite3.Row  # Enable row factory for named columns
        return conn
    
    def _initialize_db(self):
        """
        Initialize the database schema and populate default data.
        """
        try:
            logger.info(f"Initializing database at {self.db_path}")
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Set pragmas for better performance
            for pragma in Schema.PRAGMAS:
                cursor.execute(pragma)
            
            # Create tables
            for table_name, create_sql in Schema.TABLES.items():
                try:
                    cursor.execute(create_sql)
                except sqlite3.OperationalError as e:
                    logger.warning(f"Error creating table {table_name}: {str(e)}")
            
            # Create indexes
            for index_sql in Schema.INDEXES:
                try:
                    cursor.execute(index_sql)
                except sqlite3.OperationalError as e:
                    logger.warning(f"Error creating index: {str(e)}")
            
            # Initialize blacklist with domains from JSON file and default domains
            blacklist = load_blacklist().get('domains', [])
            for domain_info in blacklist:
                domain = domain_info['domain']
                reason = domain_info.get('reason', 'Added from blacklist file')
                
                cursor.execute(
                    'INSERT OR IGNORE INTO domain_blacklist (domain, reason, added_date) VALUES (?, ?, ?)',
                    (domain, reason, datetime.now().isoformat())
                )
                self._cache['blacklist'].add(domain)
            
            # Add default blacklist domains if not already added
            for domain_info in DEFAULT_BLACKLIST:
                domain = domain_info['domain']
                if domain not in self._cache['blacklist']:
                    reason = domain_info.get('reason', 'Default blacklist')
                    cursor.execute(
                        'INSERT OR IGNORE INTO domain_blacklist (domain, reason, added_date) VALUES (?, ?, ?)',
                        (domain, reason, datetime.now().isoformat())
                    )
                    self._cache['blacklist'].add(domain)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            traceback.print_exc()
            raise
    
    #--------------------------------------------------------------------------
    # Cache Management
    #--------------------------------------------------------------------------
    
    def get_cached_search(self, query):
        """
        Retrieve cached search results.
        
        Args:
            query (str): Search query
            
        Returns:
            dict or None: Cached search results, or None if not found or expired
        """
        try:
            query_hash = hash_string(query)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT response, timestamp FROM search_cache WHERE query_hash = ?', 
                (query_hash,)
            )
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                response_json, timestamp = result
                
                # Check if cache is expired (older than 7 days)
                cache_date = datetime.fromisoformat(timestamp)
                if (datetime.now() - cache_date).days > 7:
                    return None
                
                return json.loads(response_json)
            
            return None
        
        except Exception as e:
            logger.warning(f"Error retrieving search cache: {str(e)}")
            return None
    
    def cache_search(self, query, response):
        """
        Cache search results.
        
        Args:
            query (str): Search query
            response (dict): Search results to cache
            
        Returns:
            bool: Success status
        """
        try:
            query_hash = hash_string(query)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'INSERT OR REPLACE INTO search_cache (query_hash, query, response, timestamp) VALUES (?, ?, ?, ?)',
                (query_hash, query, json.dumps(response), datetime.now().isoformat())
            )
            
            conn.commit()
            conn.close()
            return True
        
        except Exception as e:
            logger.warning(f"Error caching search results: {str(e)}")
            return False
    
    def get_cached_content(self, url):
        """
        Retrieve cached content for a URL.
        
        Args:
            url (str): URL to retrieve content for
            
        Returns:
            dict or None: Cached content, or None if not found
        """
        try:
            url_hash = hash_string(url)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                '''SELECT title, content, extracted_date, html_content, timestamp, 
                   language, content_length FROM content_cache WHERE url_hash = ?''',
                (url_hash,)
            )
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'title': result['title'], 
                    'content': result['content'], 
                    'extracted_date': result['extracted_date'],
                    'html_content': result['html_content'],
                    'language': result['language'],
                    'content_length': result['content_length'],
                    'from_cache': True
                }
            
            return None
        
        except Exception as e:
            logger.warning(f"Error retrieving content cache: {str(e)}")
            return None
    
    def cache_content(self, url, title, content, extracted_date=None, html_content=None, language=None):
        """
        Cache extracted content.
        
        Args:
            url (str): URL of the content
            title (str): Title of the content
            content (str): Text content
            extracted_date (str, optional): Date extracted from the content
            html_content (str, optional): HTML content
            language (str, optional): Language of the content
            
        Returns:
            bool: Success status
        """
        try:
            url_hash = hash_string(url)
            content_length = len(content) if content else 0
            
            # Truncate large HTML content to avoid bloating the database
            if html_content and len(html_content) > 1000000:
                html_content = html_content[:100000] + "... [TRUNCATED]"
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                '''INSERT OR REPLACE INTO content_cache 
                   (url_hash, url, title, content, extracted_date, html_content, timestamp, language, content_length) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (url_hash, url, title, content, extracted_date or "", html_content or "", 
                 datetime.now().isoformat(), language or "es", content_length)
            )
            
            conn.commit()
            conn.close()
            return True
        
        except Exception as e:
            logger.warning(f"Error caching content: {str(e)}")
            return False
    
    def is_blacklisted(self, domain):
        """
        Check if a domain is blacklisted.
        
        Args:
            domain (str): Domain to check
            
        Returns:
            bool: True if domain is blacklisted, False otherwise
        """
        # First check memory cache for performance
        if domain in self._cache['blacklist']:
            return True
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT 1 FROM domain_blacklist WHERE domain = ?', (domain,))
            result = cursor.fetchone()
            
            conn.close()
            
            is_blacklisted = result is not None
            
            # Update cache if found
            if is_blacklisted:
                self._cache['blacklist'].add(domain)
                
            return is_blacklisted
        
        except Exception as e:
            logger.warning(f"Error checking domain blacklist: {str(e)}")
            return False
    
    # In database/db_manager.py, modify the update_domain_stats method:

    def update_domain_stats(self, domain, success=True, content_length=0, is_spanish=True):
        """
        Update domain success/failure statistics with improved concurrency handling.
        
        Args:
            domain (str): Domain to update stats for
            success (bool): Whether content extraction was successful
            content_length (int): Length of extracted content
            is_spanish (bool): Whether content is in Spanish
            
        Returns:
            bool: Success status
        """
        if not domain:
            return False
            
        try:
            import sqlite3
            import time
            import random
            
            # Try with retry logic to handle database locks
            for attempt in range(3):
                try:
                    conn = self.get_connection()
                    cursor = conn.cursor()
                    
                    # Set a timeout for busy database
                    cursor.execute("PRAGMA busy_timeout = 5000")  # 5 seconds
                    
                    # Use a simplified upsert approach to minimize locking
                    if success:
                        cursor.execute(
                            """INSERT INTO domain_stats 
                            (domain, success_count, failure_count, avg_content_length, last_updated, is_spanish) 
                            VALUES (?, 1, 0, ?, ?, ?)
                            ON CONFLICT(domain) DO UPDATE SET 
                            success_count = success_count + 1,
                            avg_content_length = (avg_content_length * success_count + ?) / (success_count + 1),
                            last_updated = ?""",
                            (domain, content_length, datetime.now().isoformat(), is_spanish, content_length, datetime.now().isoformat())
                        )
                    else:
                        cursor.execute(
                            """INSERT INTO domain_stats 
                            (domain, success_count, failure_count, avg_content_length, last_updated, is_spanish) 
                            VALUES (?, 0, 1, 0, ?, ?)
                            ON CONFLICT(domain) DO UPDATE SET 
                            failure_count = failure_count + 1,
                            last_updated = ?""",
                            (domain, datetime.now().isoformat(), is_spanish, datetime.now().isoformat())
                        )
                    
                    conn.commit()
                    conn.close()
                    return True
                    
                except sqlite3.OperationalError as e:
                    # Handle database locked errors with exponential backoff
                    if "database is locked" in str(e) and attempt < 2:
                        try:
                            conn.close()
                        except:
                            pass
                        
                        wait_time = (2 ** attempt) * random.uniform(0.5, 1.0)
                        logger.warning(f"Database locked, retrying in {wait_time:.2f}s (attempt {attempt+1}/3)")
                        time.sleep(wait_time)
                    else:
                        logger.warning(f"Database error updating domain stats for {domain}: {str(e)}")
                        return False
                except Exception as e:
                    logger.warning(f"Error updating domain stats for {domain}: {str(e)}")
                    try:
                        conn.close()
                    except:
                        pass
                    return False
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating domain stats for {domain}: {str(e)}")
            return False
    
    #--------------------------------------------------------------------------
    # Candidate Methods
    #--------------------------------------------------------------------------
    
    def get_or_create_candidate(self, name, municipality, target_year, state=None, gender=None, party=None, period=None):
        """
        Get or create a candidate by name, municipality, and target year with robust data validation.
        
        Args:
            name (str): Candidate name
            municipality (str): Municipality name
            target_year (int): Target election year
            state (str, optional): State name
            gender (str, optional): Candidate gender
            party (str, optional): Political party
            period (str, optional): Period format
            
        Returns:
            tuple: (Candidate object, bool created)
        """
        try:
            # Validate essential inputs
            if not name or not municipality or not target_year:
                logger.warning(f"Missing essential candidate data: name={name}, municipality={municipality}, year={target_year}")
                return None, False
                
            # Ensure values are properly typed
            # For name and municipality, ensure they are strings and not empty after stripping
            if isinstance(name, str):
                name = name.strip()
                if not name:
                    logger.warning("Empty candidate name after stripping whitespace")
                    return None, False
            else:
                try:
                    name = str(name).strip()
                    if not name:
                        logger.warning("Empty candidate name after conversion and stripping")
                        return None, False
                except (ValueError, TypeError, AttributeError):
                    logger.warning(f"Invalid candidate name: {name}")
                    return None, False
            
            if isinstance(municipality, str):
                municipality = municipality.strip()
                if not municipality:
                    logger.warning("Empty municipality after stripping whitespace")
                    return None, False
            else:
                try:
                    municipality = str(municipality).strip()
                    if not municipality:
                        logger.warning("Empty municipality after conversion and stripping")
                        return None, False
                except (ValueError, TypeError, AttributeError):
                    logger.warning(f"Invalid municipality: {municipality}")
                    return None, False
            
            # Ensure target_year is an integer
            try:
                target_year = int(target_year)
                # Validate year is within reasonable range
                if not (1980 <= target_year <= 2030):
                    logger.warning(f"Target year {target_year} outside reasonable range (1980-2030)")
                    # Continue anyway but log the warning
            except (ValueError, TypeError):
                logger.warning(f"Invalid target year: {target_year}")
                return None, False
            
            # Clean optional values
            if state:
                if isinstance(state, str):
                    state = state.strip() or None
                else:
                    try:
                        state = str(state).strip() or None
                    except (ValueError, TypeError, AttributeError):
                        state = None
            
            if gender:
                if isinstance(gender, str):
                    gender = gender.strip() or None
                else:
                    try:
                        gender = str(gender).strip() or None
                    except (ValueError, TypeError, AttributeError):
                        gender = None
            
            if party:
                if isinstance(party, str):
                    party = party.strip() or None
                else:
                    try:
                        party = str(party).strip() or None
                    except (ValueError, TypeError, AttributeError):
                        party = None
            
            if period:
                if isinstance(period, str):
                    period = period.strip() or None
                else:
                    try:
                        period = str(period).strip() or None
                    except (ValueError, TypeError, AttributeError):
                        period = None
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if candidate exists
            cursor.execute(
                'SELECT * FROM candidates WHERE name = ? AND municipality = ? AND target_year = ?',
                (name, municipality, target_year)
            )
            
            result = cursor.fetchone()
            
            if result:
                # Candidate exists
                candidate = Candidate.from_row(result)
                created = False
                
                # Update additional fields if provided
                if any([state, gender, party, period]):
                    updates = []
                    params = []
                    
                    if state and not candidate.entidad:
                        updates.append('entidad = ?')
                        params.append(state)
                    
                    if gender and not candidate.gender:
                        updates.append('gender = ?')
                        params.append(gender)
                    
                    if party and not candidate.party:
                        updates.append('party = ?')
                        params.append(party)
                    
                    if period and not candidate.period_format:
                        updates.append('period_format = ?')
                        params.append(period)
                    
                    if updates:
                        updates.append('updated_at = ?')
                        params.append(datetime.now().isoformat())
                        params.append(candidate.id)
                        
                        cursor.execute(
                            f'UPDATE candidates SET {", ".join(updates)} WHERE id = ?',
                            params
                        )
                        conn.commit()
                        
                        # Reload candidate
                        cursor.execute('SELECT * FROM candidates WHERE id = ?', (candidate.id,))
                        candidate = Candidate.from_row(cursor.fetchone())
            else:
                # Create new candidate
                now = datetime.now().isoformat()
                cursor.execute(
                    '''INSERT INTO candidates 
                    (name, municipality, entidad, target_year, gender, party, period_format, created_at, updated_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (name, municipality, state, target_year, gender, party, period, now, now)
                )
                
                candidate_id = cursor.lastrowid
                
                # Retrieve the newly created candidate
                cursor.execute('SELECT * FROM candidates WHERE id = ?', (candidate_id,))
                candidate = Candidate.from_row(cursor.fetchone())
                created = True
                
                conn.commit()
            
            conn.close()
            return candidate, created
            
        except Exception as e:
            logger.error(f"Error getting/creating candidate {name}: {str(e)}")
            traceback.print_exc()
            return None, False
    
    def get_candidate(self, candidate_id=None, name=None, municipality=None, target_year=None):
        """
        Get a candidate by ID or by name, municipality, and target year.
        
        Args:
            candidate_id (int, optional): Candidate ID
            name (str, optional): Candidate name
            municipality (str, optional): Municipality name
            target_year (int, optional): Target election year
            
        Returns:
            Candidate: Candidate object or None if not found
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if candidate_id:
                cursor.execute('SELECT * FROM candidates WHERE id = ?', (candidate_id,))
            elif name and municipality and target_year:
                cursor.execute(
                    'SELECT * FROM candidates WHERE name = ? AND municipality = ? AND target_year = ?',
                    (name, municipality, target_year)
                )
            else:
                logger.warning("get_candidate requires either candidate_id or (name, municipality, target_year)")
                conn.close()
                return None
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return Candidate.from_row(result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting candidate: {str(e)}")
            return None
    
    def get_all_candidates(self):
        """
        Get all candidates from the database.
        
        Returns:
            list: List of Candidate objects
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM candidates ORDER BY name')
            
            results = cursor.fetchall()
            conn.close()
            
            return [Candidate.from_row(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting all candidates: {str(e)}")
            return []
    
    #--------------------------------------------------------------------------
    # Article Methods
    #--------------------------------------------------------------------------
    
    def save_article(self, article_data, batch_id=None):
        """
        Save or update an article and its relationship to candidates.
        
        Args:
            article_data (dict): Article data
            batch_id (int, optional): Batch ID
            
        Returns:
            int: Article ID or None on error
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            url = article_data.get('url')
            if not url:
                logger.warning("Cannot save article without URL")
                conn.close()
                return None
            
            # Check if article already exists by URL
            cursor.execute('SELECT id FROM articles WHERE url = ?', (url,))
            result = cursor.fetchone()
            article_id = result['id'] if result else None
            
            # Default extraction date if not provided
            if 'extraction_date' not in article_data:
                article_data['extraction_date'] = datetime.now().isoformat()
            
            # Set batch ID if provided
            if batch_id and 'batch_id' not in article_data:
                article_data['batch_id'] = batch_id
            
            # Get candidate information
            candidate_name = article_data.pop('candidato', None)
            municipality = article_data.pop('municipio', None)
            target_year = article_data.pop('target_year', None)
            
            # Extract candidate-specific scores for the relationship
            candidate_scores = {
                'name_match_score': article_data.pop('name_match_score', 0.0),
                'fuzzy_match_score': article_data.pop('fuzzy_match_score', 0.0),
                'biographical_content_score': article_data.pop('biographical_content_score', 0.0),
                'political_content_score': article_data.pop('political_content_score', 0.0),
                'academic_score': article_data.pop('academic_score', 0.0),
                'professional_score': article_data.pop('professional_score', 0.0),
                'public_service_score': article_data.pop('public_service_score', 0.0),
            }
            
            # Handle entities and quotes separately
            entities = article_data.pop('entities', None)
            quotes = article_data.pop('quotes', None)
            
            # Handle insert or update
            if article_id:
                # Update existing article
                fields = []
                values = []
                
                for key, value in article_data.items():
                    # Skip non-article fields
                    if key in ('candidato', 'municipio', 'sexo', 'partido', 'periodo_formato_original', 
                               'entidad', 'cve_entidad', 'cve_municipio'):
                        continue
                    
                    # Map field names to database column names
                    if key == 'content_type':
                        fields.append('content_type = ?')
                    elif key == 'content_type_confidence':
                        fields.append('content_type_confidence = ?')
                    elif key == 'quote_count':
                        fields.append('quote_count = ?')
                    else:
                        fields.append(f"{key} = ?")
                    
                    values.append(value)
                
                if fields:
                    values.append(article_id)
                    query = f"UPDATE articles SET {', '.join(fields)} WHERE id = ?"
                    cursor.execute(query, values)
            else:
                # Insert new article
                keys = []
                placeholders = []
                values = []
                
                for key, value in article_data.items():
                    # Skip non-article fields
                    if key in ('candidato', 'municipio', 'sexo', 'partido', 'periodo_formato_original', 
                               'entidad', 'cve_entidad', 'cve_municipio'):
                        continue
                    
                    keys.append(key)
                    placeholders.append('?')
                    values.append(value)
                
                if keys:
                    query = f"INSERT INTO articles ({', '.join(keys)}) VALUES ({', '.join(placeholders)})"
                    cursor.execute(query, values)
                    article_id = cursor.lastrowid
            
            # Link article to candidate if candidate info is provided
            if candidate_name and municipality and target_year and article_id:
                # Get or create candidate
                state = article_data.get('entidad')
                gender = article_data.get('sexo')
                party = article_data.get('partido')
                period = article_data.get('periodo_formato_original')
                
                candidate, _ = self.get_or_create_candidate(
                    candidate_name, municipality, target_year,
                    state=state, gender=gender, party=party, period=period
                )
                
                if candidate:
                    # Check if relationship already exists
                    cursor.execute(
                        'SELECT id FROM candidate_articles WHERE candidate_id = ? AND article_id = ?',
                        (candidate.id, article_id)
                    )
                    relation = cursor.fetchone()
                    
                    overall_relevance = article_data.get('overall_relevance', 0.0)
                    
                    if relation:
                        # Update relationship
                        cursor.execute(
                            '''UPDATE candidate_articles SET 
                               name_match_score = ?, fuzzy_match_score = ?,
                               biographical_score = ?, political_score = ?,
                               academic_score = ?, professional_score = ?,
                               public_service_score = ?, relevance = ?
                               WHERE id = ?''',
                            (
                                candidate_scores['name_match_score'],
                                candidate_scores['fuzzy_match_score'],
                                candidate_scores['biographical_content_score'],
                                candidate_scores['political_content_score'],
                                candidate_scores['academic_score'],
                                candidate_scores['professional_score'],
                                candidate_scores['public_service_score'],
                                overall_relevance,
                                relation['id']
                            )
                        )
                    else:
                        # Create relationship
                        cursor.execute(
                            '''INSERT INTO candidate_articles
                               (candidate_id, article_id, name_match_score, fuzzy_match_score,
                                biographical_score, political_score, academic_score,
                                professional_score, public_service_score, relevance)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (
                                candidate.id,
                                article_id,
                                candidate_scores['name_match_score'],
                                candidate_scores['fuzzy_match_score'],
                                candidate_scores['biographical_content_score'],
                                candidate_scores['political_content_score'],
                                candidate_scores['academic_score'],
                                candidate_scores['professional_score'],
                                candidate_scores['public_service_score'],
                                overall_relevance
                            )
                        )
                    
                    # Save quotes
                    if quotes and len(quotes) > 0:
                        for quote_data in quotes:
                            cursor.execute(
                                '''INSERT INTO quotes
                                   (article_id, candidate_id, quote_text, quote_context,
                                    context_start, context_end, extraction_confidence, extracted_date)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                (
                                    article_id,
                                    candidate.id,
                                    quote_data.get('text', ''),
                                    quote_data.get('context', ''),
                                    quote_data.get('context_start', 0),
                                    quote_data.get('context_end', 0),
                                    quote_data.get('confidence', 0.5),
                                    datetime.now().isoformat()
                                )
                            )
            
            # Save entities if provided
            if entities and article_id:
                for entity_type, entity_items in entities.items():
                    if isinstance(entity_items, dict):
                        for entity_text, score in entity_items.items():
                            cursor.execute(
                                '''INSERT OR REPLACE INTO entities
                                   (article_id, entity_type, entity_text, relevance_score, extraction_date, entity_context)
                                   VALUES (?, ?, ?, ?, ?, ?)''',
                                (
                                    article_id,
                                    entity_type,
                                    entity_text,
                                    score,
                                    datetime.now().isoformat(),
                                    ''  # Entity context could be enhanced in future
                                )
                            )
            
            conn.commit()
            conn.close()
            
            return article_id
            
        except Exception as e:
            logger.error(f"Error saving article: {str(e)}")
            traceback.print_exc()
            return None
    
    def get_article(self, article_id=None, url=None):
        """
        Get an article by ID or URL.
        
        Args:
            article_id (int, optional): Article ID
            url (str, optional): Article URL
            
        Returns:
            Article: Article object or None if not found
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if article_id:
                cursor.execute('SELECT * FROM articles WHERE id = ?', (article_id,))
            elif url:
                cursor.execute('SELECT * FROM articles WHERE url = ?', (url,))
            else:
                logger.warning("get_article requires either article_id or url")
                conn.close()
                return None
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return Article.from_row(result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article: {str(e)}")
            return None
    
    def get_candidate_articles(self, candidate_id=None, candidate_name=None, municipality=None, 
                              target_year=None, min_relevance=0.3):
        """
        Get articles for a specific candidate with optional filtering.
        
        Args:
            candidate_id (int, optional): Candidate ID
            candidate_name (str, optional): Candidate name
            municipality (str, optional): Municipality name
            target_year (int, optional): Target election year
            min_relevance (float, optional): Minimum relevance threshold
            
        Returns:
            pandas.DataFrame: DataFrame with article data
        """
        try:
            # Get candidate if not provided by ID
            if not candidate_id and candidate_name and municipality and target_year:
                candidate = self.get_candidate(
                    name=candidate_name, municipality=municipality, target_year=target_year
                )
                if candidate:
                    candidate_id = candidate.id
                else:
                    logger.warning(f"Candidate not found: {candidate_name}, {municipality}, {target_year}")
                    return pd.DataFrame()
            
            if not candidate_id:
                logger.warning("get_candidate_articles requires either candidate_id or (candidate_name, municipality, target_year)")
                return pd.DataFrame()
            
            conn = self.get_connection()
            
            query = """
            SELECT a.*, ca.name_match_score, ca.fuzzy_match_score, ca.biographical_score,
                   ca.political_score, ca.academic_score, ca.professional_score, 
                   ca.public_service_score, ca.relevance
            FROM articles a
            JOIN candidate_articles ca ON a.id = ca.article_id
            WHERE ca.candidate_id = ? AND ca.relevance >= ?
            ORDER BY ca.relevance DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(candidate_id, min_relevance))
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving candidate articles: {str(e)}")
            return pd.DataFrame()
    
    def get_all_articles(self, min_relevance=0.1, target_year=None, year_range=None,
                        batch_id=None, limit=None, temporal_relevance_threshold=0.3):
        """
        Retrieve all articles with optional filtering.
        
        Args:
            min_relevance (float, optional): Minimum overall relevance threshold
            target_year (int, optional): Target election year
            year_range (int, optional): Year range for filtering
            batch_id (int, optional): Batch ID
            limit (int, optional): Maximum number of results
            temporal_relevance_threshold (float, optional): Minimum temporal relevance threshold
            
        Returns:
            pandas.DataFrame: DataFrame with article data
        """
        try:
            conn = self.get_connection()
            query = """
            SELECT a.*, c.name as candidate_name, c.municipality, c.target_year, c.gender, c.party,
                   ca.name_match_score, ca.fuzzy_match_score, ca.biographical_score,
                   ca.political_score, ca.academic_score, ca.professional_score, 
                   ca.public_service_score, ca.relevance
            FROM articles a
            JOIN candidate_articles ca ON a.id = ca.article_id
            JOIN candidates c ON ca.candidate_id = c.id
            WHERE a.overall_relevance >= ? 
            AND a.temporal_relevance >= ?
            """
            params = [min_relevance, temporal_relevance_threshold]
                
            if target_year is not None:
                if year_range is not None:
                    query += """ 
                    AND (
                        (a.year_lower_bound IS NOT NULL AND a.year_upper_bound IS NOT NULL AND 
                         ? BETWEEN a.year_lower_bound AND a.year_upper_bound)
                        OR 
                        (c.target_year BETWEEN ? AND ?)
                    )"""
                    params.extend([target_year, target_year - year_range, target_year + year_range])
                else:
                    query += " AND c.target_year = ?"
                    params.append(target_year)
            
            if batch_id is not None:
                query += " AND a.batch_id = ?"
                params.append(batch_id)
                    
            query += " ORDER BY c.name, ca.relevance DESC"
            
            if limit is not None:
                query += " LIMIT ?"
                params.append(limit)
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving articles: {str(e)}")
            return pd.DataFrame()
    
    #--------------------------------------------------------------------------
    # Quote Methods
    #--------------------------------------------------------------------------
    
    def save_candidate_quotes(self, article_id, candidate_id, quotes):
        """
        Save candidate quotes extracted from an article.
        
        Args:
            article_id (int): Article ID
            candidate_id (int): Candidate ID
            quotes (list): List of quote dictionaries
            
        Returns:
            int: Number of quotes saved
        """
        if not quotes or not article_id or not candidate_id:
            return 0
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            quote_count = 0
            for quote_data in quotes:
                cursor.execute(
                    '''INSERT INTO quotes
                    (article_id, candidate_id, quote_text, quote_context, context_start, context_end, 
                     extraction_confidence, extracted_date) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (article_id, candidate_id, 
                     quote_data.get('text', ''), quote_data.get('context', ''),
                     quote_data.get('context_start', 0), quote_data.get('context_end', 0),
                     quote_data.get('confidence', 0.5), datetime.now().isoformat())
                )
                quote_count += 1
            
            # Update quote count in the article
            cursor.execute(
                'UPDATE articles SET quote_count = ? WHERE id = ?',
                (quote_count, article_id)
            )
            
            conn.commit()
            conn.close()
            
            return quote_count
            
        except Exception as e:
            logger.error(f"Error saving candidate quotes: {str(e)}")
            return 0
    
    def get_candidate_quotes(self, candidate_id=None, candidate_name=None, municipality=None, target_year=None):
        """
        Get quotes for a specific candidate.
        
        Args:
            candidate_id (int, optional): Candidate ID
            candidate_name (str, optional): Candidate name
            municipality (str, optional): Municipality name
            target_year (int, optional): Target election year
            
        Returns:
            pandas.DataFrame: DataFrame with quote data
        """
        try:
            # Get candidate if not provided by ID
            if not candidate_id and candidate_name and municipality and target_year:
                candidate = self.get_candidate(
                    name=candidate_name, municipality=municipality, target_year=target_year
                )
                if candidate:
                    candidate_id = candidate.id
                else:
                    logger.warning(f"Candidate not found: {candidate_name}, {municipality}, {target_year}")
                    return pd.DataFrame()
            
            if not candidate_id:
                logger.warning("get_candidate_quotes requires either candidate_id or (candidate_name, municipality, target_year)")
                return pd.DataFrame()
            
            conn = self.get_connection()
            
            query = """
            SELECT q.*, a.url, a.title, a.source, a.content_date, a.content_type, c.name as candidate_name
            FROM quotes q
            JOIN articles a ON q.article_id = a.id
            JOIN candidates c ON q.candidate_id = c.id
            WHERE q.candidate_id = ?
            ORDER BY q.extraction_confidence DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(candidate_id,))
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving candidate quotes: {str(e)}")
            return pd.DataFrame()
    
    #--------------------------------------------------------------------------
    # Batch Processing Methods
    #--------------------------------------------------------------------------
    
    def create_batch(self, total_candidates, config=None):
        """
        Create a new batch for processing.
        
        Args:
            total_candidates (int): Total number of candidates to process
            config (dict, optional): Batch configuration
            
        Returns:
            int: Batch ID or None on error
        """
        try:
            config_json = json.dumps(config) if config else "{}"
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                '''INSERT INTO scraping_batches (started_at, total_candidates, completed_candidates, status, config)
                   VALUES (?, ?, ?, ?, ?)''',
                (datetime.now().isoformat(), total_candidates, 0, 'STARTED', config_json)
            )
            
            batch_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            logger.info(f"Created new batch with ID {batch_id} for {total_candidates} candidates")
            return batch_id
            
        except Exception as e:
            logger.error(f"Error creating batch: {str(e)}")
            return None
    
    def update_batch_status(self, batch_id, status, completed_candidates=None):
        """
        Update batch status.
        
        Args:
            batch_id (int): Batch ID
            status (str): New status ('STARTED', 'IN_PROGRESS', 'COMPLETED', 'FAILED')
            completed_candidates (int, optional): Number of completed candidates
            
        Returns:
            bool: Success status
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if completed_candidates is not None:
                cursor.execute(
                    'UPDATE scraping_batches SET status = ?, completed_candidates = ? WHERE id = ?',
                    (status, completed_candidates, batch_id)
                )
            else:
                cursor.execute(
                    'UPDATE scraping_batches SET status = ? WHERE id = ?',
                    (status, batch_id)
                )
                
            # If status is COMPLETED, add completion time
            if status == 'COMPLETED':
                cursor.execute(
                    'UPDATE scraping_batches SET completed_at = ? WHERE id = ?',
                    (datetime.now().isoformat(), batch_id)
                )
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated batch {batch_id} status to {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating batch status: {str(e)}")
            return False
    
    def update_candidate_progress(self, candidate_id=None, candidate_name=None, municipality=None, 
                                target_year=None, status='IN_PROGRESS', articles_found=0, batch_id=None):
        """
        Update scraping progress for a candidate.
        
        Args:
            candidate_id (int, optional): Candidate ID
            candidate_name (str, optional): Candidate name
            municipality (str, optional): Municipality name
            target_year (int, optional): Target election year
            status (str): Progress status ('SEARCHING', 'EXTRACTING', 'COMPLETED', 'FAILED')
            articles_found (int, optional): Number of articles found
            batch_id (int, optional): Batch ID
            
        Returns:
            bool: Success status
        """
        try:
            # Get candidate if not provided by ID
            if not candidate_id and candidate_name and municipality and target_year:
                candidate, created = self.get_or_create_candidate(
                    candidate_name, municipality, target_year
                )
                if candidate:
                    candidate_id = candidate.id
                else:
                    logger.warning(f"Failed to get/create candidate: {candidate_name}, {municipality}, {target_year}")
                    return False
            
            if not candidate_id:
                logger.warning("update_candidate_progress requires either candidate_id or (candidate_name, municipality, target_year)")
                return False
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if progress entry exists
            cursor.execute(
                'SELECT id FROM scraping_progress WHERE candidate_id = ? AND batch_id = ?',
                (candidate_id, batch_id)
            )
            
            result = cursor.fetchone()
            current_time = datetime.now().isoformat()
            
            if result:
                # Update existing record
                if status == 'COMPLETED':
                    cursor.execute(
                        '''UPDATE scraping_progress 
                           SET status = ?, articles_found = ?, completed_at = ?
                           WHERE id = ?''',
                        (status, articles_found, current_time, result['id'])
                    )
                else:
                    cursor.execute(
                        'UPDATE scraping_progress SET status = ?, articles_found = ? WHERE id = ?',
                        (status, articles_found, result['id'])
                    )
            else:
                # Insert new record
                if status == 'COMPLETED':
                    cursor.execute(
                        '''INSERT INTO scraping_progress 
                           (candidate_id, status, started_at, completed_at, articles_found, batch_id)
                           VALUES (?, ?, ?, ?, ?, ?)''',
                        (candidate_id, status, current_time, current_time, articles_found, batch_id)
                    )
                else:
                    cursor.execute(
                        '''INSERT INTO scraping_progress 
                           (candidate_id, status, started_at, articles_found, batch_id)
                           VALUES (?, ?, ?, ?, ?)''',
                        (candidate_id, status, current_time, articles_found, batch_id)
                    )
            
            conn.commit()
            
            # If a candidate was completed, update the batch progress
            if status == 'COMPLETED' and batch_id:
                self.increment_batch_progress(batch_id)
            
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error updating candidate progress: {str(e)}")
            return False
    
    def increment_batch_progress(self, batch_id):
        """
        Increment completed candidates count for a batch.
        
        Args:
            batch_id (int): Batch ID
            
        Returns:
            bool: Success status
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Increment completed count
            cursor.execute(
                'UPDATE scraping_batches SET completed_candidates = completed_candidates + 1 WHERE id = ?',
                (batch_id,)
            )
            
            # Check if all candidates are completed
            cursor.execute(
                'SELECT completed_candidates, total_candidates FROM scraping_batches WHERE id = ?',
                (batch_id,)
            )
            
            result = cursor.fetchone()
            
            if result and result['completed_candidates'] >= result['total_candidates']:
                # All candidates are completed, update batch status
                cursor.execute(
                    'UPDATE scraping_batches SET status = ?, completed_at = ? WHERE id = ?',
                    ('COMPLETED', datetime.now().isoformat(), batch_id)
                )
                logger.info(f"Batch {batch_id} completed all {result['total_candidates']} candidates")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error incrementing batch progress: {str(e)}")
            return False
    
    def get_completed_candidates(self, batch_id=None):
        """
        Get list of completed candidate IDs.
        
        Args:
            batch_id (int, optional): Batch ID
            
        Returns:
            list: List of candidate IDs
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if batch_id:
                cursor.execute(
                    'SELECT candidate_id FROM scraping_progress WHERE status = ? AND batch_id = ?',
                    ('COMPLETED', batch_id)
                )
            else:
                cursor.execute(
                    'SELECT candidate_id FROM scraping_progress WHERE status = ?',
                    ('COMPLETED',)
                )
            
            results = cursor.fetchall()
            conn.close()
            
            return [r['candidate_id'] for r in results]
            
        except Exception as e:
            logger.error(f"Error getting completed candidates: {str(e)}")
            return []
    
    def get_candidates_to_process(self, all_candidates, batch_id=None):
        """
        Get candidates that need to be processed (not completed yet).
        
        Args:
            all_candidates (list): List of candidate dictionaries or Candidate objects
            batch_id (int, optional): Batch ID
            
        Returns:
            list: List of candidate objects not yet completed
        """
        completed_ids = set(self.get_completed_candidates(batch_id))
        
        # Filter out completed candidates
        to_process = []
        
        for candidate in all_candidates:
            if isinstance(candidate, dict):
                # Handle dictionary input
                name = candidate.get('PRESIDENTE_MUNICIPAL', candidate.get('name'))
                municipality = candidate.get('MUNICIPIO', candidate.get('municipality'))
                target_year = candidate.get('Year', candidate.get('target_year'))
                
                if name and municipality and target_year:
                    cand_obj = self.get_candidate(
                        name=name, municipality=municipality, target_year=target_year
                    )
                    
                    if cand_obj is None:
                        # Not in database yet, include for processing
                        to_process.append(candidate)
                    elif cand_obj.id not in completed_ids:
                        # In database but not completed, include for processing
                        to_process.append(candidate)
            
            elif isinstance(candidate, Candidate):
                # Handle Candidate object input
                if candidate.id not in completed_ids:
                    to_process.append(candidate)
        
        return to_process
    
    #--------------------------------------------------------------------------
    # Profile Methods
    #--------------------------------------------------------------------------
    
    def get_candidate_profile(self, candidate_id=None, candidate_name=None, municipality=None, 
                             target_year=None, year_range=2):
        """
        Get a complete profile for a candidate by aggregating all articles.
        
        Args:
            candidate_id (int, optional): Candidate ID
            candidate_name (str, optional): Candidate name
            municipality (str, optional): Municipality name
            target_year (int, optional): Target election year
            year_range (int, optional): Year range for filtering
            
        Returns:
            dict: Candidate profile data
        """
        try:
            # Get candidate if not provided by ID
            if not candidate_id and candidate_name and municipality and target_year:
                candidate = self.get_candidate(
                    name=candidate_name, municipality=municipality, target_year=target_year
                )
                if candidate:
                    candidate_id = candidate.id
                else:
                    logger.warning(f"Candidate not found: {candidate_name}, {municipality}, {target_year}")
                    return None
            
            if not candidate_id:
                logger.warning("get_candidate_profile requires either candidate_id or (candidate_name, municipality, target_year)")
                return None
            
            conn = self.get_connection()
            
            # Get candidate data
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM candidates WHERE id = ?', (candidate_id,))
            candidate = Candidate.from_row(cursor.fetchone())
            
            if not candidate:
                conn.close()
                return None
            
            # Improved query with year range filtering
            if target_year:
                query = """
                SELECT a.*, ca.name_match_score, ca.fuzzy_match_score, ca.biographical_score,
                       ca.political_score, ca.academic_score, ca.professional_score, 
                       ca.public_service_score, ca.relevance 
                FROM articles a
                JOIN candidate_articles ca ON a.id = ca.article_id
                WHERE ca.candidate_id = ?
                AND (
                    (a.year_lower_bound IS NOT NULL AND a.year_upper_bound IS NOT NULL AND 
                     ? BETWEEN a.year_lower_bound AND a.year_upper_bound)
                    OR 
                    (? BETWEEN ? AND ?)
                    OR
                    (? BETWEEN a.year_lower_bound AND a.year_upper_bound)
                )
                """
                params = [
                    candidate_id,
                    target_year,
                    target_year, target_year - year_range, target_year + year_range,
                    target_year
                ]
            else:
                query = """
                SELECT a.*, ca.name_match_score, ca.fuzzy_match_score, ca.biographical_score,
                       ca.political_score, ca.academic_score, ca.professional_score, 
                       ca.public_service_score, ca.relevance 
                FROM articles a
                JOIN candidate_articles ca ON a.id = ca.article_id
                WHERE ca.candidate_id = ?
                """
                params = [candidate_id]
                
            query += " ORDER BY ca.relevance DESC, a.temporal_relevance DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty:
                conn.close()
                return None
            
            # Get quotes for this candidate
            quotes_query = """
            SELECT q.* FROM quotes q
            WHERE q.candidate_id = ?
            ORDER BY q.extraction_confidence DESC
            """
            quotes_df = pd.read_sql_query(quotes_query, conn, params=[candidate_id])
            
            # Count content types
            content_types = df['content_type'].value_counts().to_dict()
            
            # Create profile summary
            profile = {
                'id': candidate.id,
                'name': candidate.name,
                'municipality': candidate.municipality,
                'year': candidate.target_year,
                'gender': candidate.gender,
                'political_party': candidate.party,
                'period': candidate.period_format,
                'article_count': len(df),
                'avg_relevance': float(df['relevance'].mean()),
                'avg_temporal_relevance': float(df['temporal_relevance'].mean()),
                'top_sources': df['source'].value_counts().head(5).to_dict(),
                'biography_score': float(df['biographical_score'].mean()),
                'political_score': float(df['political_score'].mean()),
                'academic_score': float(df['academic_score'].mean()),
                'professional_score': float(df['professional_score'].mean()),
                'public_service_score': float(df['public_service_score'].mean()),
                'content_types': content_types,
                'quote_count': len(quotes_df)
            }
            
            # Add excerpts from top articles
            top_articles = df.sort_values(['relevance', 'temporal_relevance'], ascending=[False, False]).head(3)
            excerpts = []
            
            for _, article in top_articles.iterrows():
                content = article['content']
                if content and len(content) > 20:
                    # Extract a meaningful excerpt (first 300 chars)
                    excerpt = content[:300] + "..." if len(content) > 300 else content
                    excerpts.append({
                        'title': article['title'],
                        'source': article['source'],
                        'url': article['url'],
                        'excerpt': excerpt,
                        'relevance': float(article['relevance']),
                        'temporal_relevance': float(article['temporal_relevance']),
                        'content_date': article['content_date'],
                        'content_type': article['content_type']
                    })
            
            profile['excerpts'] = excerpts
            
            # Add top quotes
            top_quotes = []
            if not quotes_df.empty:
                for _, quote in quotes_df.head(5).iterrows():
                    quote_text = quote['quote_text']
                    if quote_text and len(quote_text) > 10:
                        top_quotes.append({
                            'text': quote_text,
                            'context': quote['quote_context'][:100] + "..." if len(quote['quote_context']) > 100 else quote['quote_context'],
                            'confidence': float(quote['extraction_confidence'])
                        })
            
            profile['top_quotes'] = top_quotes
            
            # Save or update this profile in the database
            self._save_candidate_profile(candidate_id, profile)
            
            conn.close()
            return profile
            
        except Exception as e:
            logger.error(f"Error retrieving candidate profile: {str(e)}")
            traceback.print_exc()
            return None
    
    def _save_candidate_profile(self, candidate_id, profile):
        """
        Save or update a candidate profile in the database.
        
        Args:
            candidate_id (int): Candidate ID
            profile (dict): Profile data
            
        Returns:
            bool: Success status
        """
        try:
            if not candidate_id:
                return False
                
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if profile already exists
            cursor.execute(
                'SELECT id FROM candidate_profiles WHERE candidate_id = ?',
                (candidate_id,)
            )
            
            result = cursor.fetchone()
            
            current_time = datetime.now().isoformat()
            profile_json = json.dumps(profile, ensure_ascii=False)
            
            if result:
                # Update existing profile
                cursor.execute(
                    '''UPDATE candidate_profiles SET 
                    total_articles = ?, total_quotes = ?, 
                    avg_biographical_score = ?, avg_political_score = ?,
                    avg_academic_score = ?, avg_professional_score = ?,
                    avg_public_service_score = ?,
                    news_count = ?, article_count = ?, discourse_count = ?,
                    profile_json = ?, updated_date = ?
                    WHERE id = ?''',
                    (
                        profile.get('article_count', 0),
                        profile.get('quote_count', 0),
                        profile.get('biography_score', 0.0),
                        profile.get('political_score', 0.0),
                        profile.get('academic_score', 0.0),
                        profile.get('professional_score', 0.0),
                        profile.get('public_service_score', 0.0),
                        profile.get('content_types', {}).get('news', 0),
                        profile.get('content_types', {}).get('article', 0),
                        profile.get('content_types', {}).get('discourse', 0),
                        profile_json,
                        current_time,
                        result['id']
                    )
                )
            else:
                # Insert new profile
                cursor.execute(
                    '''INSERT INTO candidate_profiles
                    (candidate_id, total_articles, total_quotes, 
                    avg_biographical_score, avg_political_score,
                    avg_academic_score, avg_professional_score, avg_public_service_score,
                    news_count, article_count, discourse_count,
                    profile_json, created_date, updated_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        candidate_id,
                        profile.get('article_count', 0),
                        profile.get('quote_count', 0),
                        profile.get('biography_score', 0.0),
                        profile.get('political_score', 0.0),
                        profile.get('academic_score', 0.0),
                        profile.get('professional_score', 0.0),
                        profile.get('public_service_score', 0.0),
                        profile.get('content_types', {}).get('news', 0),
                        profile.get('content_types', {}).get('article', 0),
                        profile.get('content_types', {}).get('discourse', 0),
                        profile_json,
                        current_time,
                        current_time
                    )
                )
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error saving candidate profile: {str(e)}")
            return False
    
    def create_candidate_profiles(self, min_relevance=0.3):
        """
        Create or update profiles for all candidates with articles.
        
        Args:
            min_relevance (float, optional): Minimum relevance threshold
            
        Returns:
            int: Number of profiles created/updated
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all candidates with articles
            cursor.execute('''
            SELECT DISTINCT ca.candidate_id 
            FROM candidate_articles ca
            JOIN articles a ON ca.article_id = a.id
            WHERE ca.relevance >= ?
            ''', (min_relevance,))
            
            candidates = cursor.fetchall()
            conn.close()
            
            if not candidates:
                logger.warning("No candidates found for profile creation")
                return 0
            
            # Create profiles in parallel
            def create_profile(candidate_id):
                try:
                    profile = self.get_candidate_profile(candidate_id=candidate_id)
                    return profile is not None
                except Exception as e:
                    logger.warning(f"Error creating profile for candidate {candidate_id}: {str(e)}")
                    return False
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_candidate = {
                    executor.submit(create_profile, row['candidate_id']): row['candidate_id']
                    for row in candidates
                }
                
                updated_count = 0
                for future in future_to_candidate:
                    try:
                        if future.result():
                            updated_count += 1
                    except Exception as e:
                        logger.warning(f"Profile creation failed: {str(e)}")
            
            logger.info(f"Created/updated {updated_count} candidate profiles")
            return updated_count
            
        except Exception as e:
            logger.error(f"Error creating candidate profiles: {str(e)}")
            traceback.print_exc()
            return 0
    
    #--------------------------------------------------------------------------
    # Dataset Generation Methods
    #--------------------------------------------------------------------------
    
    def generate_ml_dataset(self, output_path='data', format='json', min_relevance=0.3):
        """
        Generate a structured dataset for ML/NLP applications.
        
        Args:
            output_path (str, optional): Output directory path
            format (str, optional): Output format ('json' or 'csv')
            min_relevance (float, optional): Minimum relevance threshold
            
        Returns:
            list or None: Dataset as a list of dictionaries, or None on error
        """
        try:
            os.makedirs(output_path, exist_ok=True)
            
            # Get all candidate profiles
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get profiles ordered by total articles (most data first)
            cursor.execute('''
            SELECT cp.*, c.name, c.municipality, c.target_year, c.gender, c.party, c.period_format
            FROM candidate_profiles cp
            JOIN candidates c ON cp.candidate_id = c.id
            ORDER BY cp.total_articles DESC, cp.total_quotes DESC
            ''')
            
            profiles = cursor.fetchall()
            
            if not profiles:
                logger.warning("No candidate profiles found for dataset generation")
                conn.close()
                return None
            
            # Create dataset
            dataset = []
            
            for profile_row in profiles:
                profile_id = profile_row['id']
                candidate_id = profile_row['candidate_id']
                candidate_name = profile_row['name']
                municipality = profile_row['municipality']
                target_year = profile_row['target_year']
                
                # Get articles for this candidate
                articles_df = self.get_candidate_articles(
                    candidate_id=candidate_id,
                    min_relevance=min_relevance
                )
                
                # Get quotes for this candidate
                quotes_df = self.get_candidate_quotes(candidate_id=candidate_id)
                
                # Skip candidates with no articles
                if articles_df.empty:
                    continue
                
                # Create candidate entry
                candidate_entry = {
                    'candidate_id': f"{candidate_name}_{municipality}_{target_year}".replace(" ", "_"),
                    'db_candidate_id': candidate_id,
                    'name': candidate_name,
                    'municipality': municipality,
                    'target_year': target_year,
                    'gender': profile_row['gender'],
                    'party': profile_row['party'],
                    'period': profile_row['period_format'],
                    
                    # High-level statistics
                    'num_articles': len(articles_df),
                    'num_quotes': len(quotes_df),
                    'content_type_distribution': {
                        'news': profile_row['news_count'],
                        'article': profile_row['article_count'],
                        'discourse': profile_row['discourse_count'],
                        'unknown': len(articles_df) - (profile_row['news_count'] + profile_row['article_count'] + profile_row['discourse_count'])
                    },
                    
                    # Scoring metrics
                    'metrics': {
                        'biographical_score': float(profile_row['avg_biographical_score']),
                        'political_score': float(profile_row['avg_political_score']),
                        'academic_score': float(profile_row['avg_academic_score']),
                        'professional_score': float(profile_row['avg_professional_score']),
                        'public_service_score': float(profile_row['avg_public_service_score'])
                    },
                    
                    # Article details
                    'articles': [],
                    
                    # Quote details
                    'quotes': [],
                    
                    # Aggregated text for NLP/LLM training
                    'aggregated_content': ''
                }
                
                # Add article details
                aggregated_texts = []
                
                for _, article in articles_df.iterrows():
                    article_entry = {
                        'id': int(article['id']),
                        'url': article['url'],
                        'title': article['title'],
                        'source': article['source'],
                        'content_date': article['content_date'],
                        'content_type': article['content_type'],
                        'relevance': {
                            'overall': float(article['overall_relevance']),
                            'temporal': float(article['temporal_relevance']),
                            'content': float(article['content_relevance'])
                        }
                    }
                    
                    candidate_entry['articles'].append(article_entry)
                    
                    # Add to aggregated content with metadata context
                    article_header = f"ARTICLE: {article['title']} | SOURCE: {article['source']} | DATE: {article['content_date']} | TYPE: {article['content_type']}\n\n"
                    article_text = f"{article_header}{article['content']}\n\n{'='*50}\n\n"
                    aggregated_texts.append(article_text)
                
                # Add quotes
                for _, quote in quotes_df.iterrows():
                    quote_entry = {
                        'id': int(quote['id']),
                        'text': quote['quote_text'],
                        'context': quote['quote_context'],
                        'confidence': float(quote['extraction_confidence']),
                        'article_id': int(quote['article_id']),
                        'source': quote['source'],
                        'date': quote['content_date']
                    }
                    candidate_entry['quotes'].append(quote_entry)
                
                # Set aggregated content
                candidate_entry['aggregated_content'] = "\n".join(aggregated_texts)
                
                dataset.append(candidate_entry)
            
            conn.close()
            
            # Save dataset
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if format.lower() == 'json':
                # Export to JSON
                json_path = os.path.join(output_path, f'candidate_ml_dataset_{timestamp}.json')
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(dataset, f, ensure_ascii=False, indent=2)
                logger.info(f"Exported ML dataset with {len(dataset)} candidates to {json_path}")
                
            elif format.lower() == 'csv':
                # Export to CSV (flattened)
                import csv
                csv_path = os.path.join(output_path, f'candidate_ml_dataset_{timestamp}.csv')
                
                # Flatten the dataset
                flattened_data = []
                for entry in dataset:
                    flat_entry = {
                        'candidate_id': entry['candidate_id'],
                        'db_candidate_id': entry['db_candidate_id'],
                        'name': entry['name'],
                        'municipality': entry['municipality'],
                        'target_year': entry['target_year'],
                        'gender': entry['gender'],
                        'party': entry['party'],
                        'period': entry['period'],
                        'num_articles': entry['num_articles'],
                        'num_quotes': entry['num_quotes'],
                        'news_count': entry['content_type_distribution']['news'],
                        'article_count': entry['content_type_distribution']['article'],
                        'discourse_count': entry['content_type_distribution']['discourse'],
                        'biographical_score': entry['metrics']['biographical_score'],
                        'political_score': entry['metrics']['political_score'],
                        'academic_score': entry['metrics']['academic_score'],
                        'professional_score': entry['metrics']['professional_score'],
                        'public_service_score': entry['metrics']['public_service_score']
                    }
                    flattened_data.append(flat_entry)
                
                # Write CSV
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    if flattened_data:
                        writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
                        writer.writeheader()
                        writer.writerows(flattened_data)
                        
                logger.info(f"Exported flattened ML dataset with {len(flattened_data)} candidates to {csv_path}")
                
                # Also export quotes separately
                quotes_path = os.path.join(output_path, f'candidate_quotes_{timestamp}.csv')
                all_quotes = []
                
                for entry in dataset:
                    candidate_id = entry['candidate_id']
                    name = entry['name']
                    
                    for quote in entry['quotes']:
                        flat_quote = {
                            'candidate_id': candidate_id,
                            'db_candidate_id': entry['db_candidate_id'],
                            'candidate_name': name,
                            'quote_id': quote['id'],
                            'quote_text': quote['text'],
                            'quote_context': quote['context'],
                            'confidence': quote['confidence'],
                            'article_id': quote['article_id'],
                            'source': quote['source'],
                            'date': quote['date']
                        }
                        all_quotes.append(flat_quote)
                
                # Write quotes CSV
                with open(quotes_path, 'w', encoding='utf-8', newline='') as f:
                    if all_quotes:
                        writer = csv.DictWriter(f, fieldnames=all_quotes[0].keys())
                        writer.writeheader()
                        writer.writerows(all_quotes)
                        
                logger.info(f"Exported {len(all_quotes)} quotes to {quotes_path}")
            
            return dataset
            
        except Exception as e:
            logger.error(f"Error generating ML dataset: {str(e)}")
            traceback.print_exc()
            return None
    
    def repair_common_issues(self):
        """
        Attempt to repair common database issues.
        This function handles orphaned records, inconsistent statuses, 
        and other common database integrity problems.
        
        Returns:
            dict: Summary of repairs made
        """
        repairs = {
            'orphaned_links_removed': 0,
            'orphaned_quotes_removed': 0,
            'batch_statuses_fixed': 0,
            'invalid_records_fixed': 0
        }
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 1. Remove candidate_articles with missing candidates or articles
            cursor.execute("""
                DELETE FROM candidate_articles 
                WHERE candidate_id NOT IN (SELECT id FROM candidates)
                OR article_id NOT IN (SELECT id FROM articles)
            """)
            repairs['orphaned_links_removed'] = cursor.rowcount
            
            # 2. Remove quotes with missing articles or candidates
            cursor.execute("""
                DELETE FROM quotes
                WHERE article_id NOT IN (SELECT id FROM articles)
                OR candidate_id NOT IN (SELECT id FROM candidates)
            """)
            repairs['orphaned_quotes_removed'] = cursor.rowcount
            
            # 3. Fix batch statuses with inconsistent completion counts
            cursor.execute("""
                UPDATE scraping_batches
                SET status = 'IN_PROGRESS'
                WHERE status = 'COMPLETED' 
                AND completed_candidates < total_candidates
            """)
            repairs['batch_statuses_fixed'] = cursor.rowcount
            
            # 4. Fix invalid dates in articles
            cursor.execute("""
                UPDATE articles
                SET extracted_date = NULL
                WHERE extracted_date NOT LIKE '____-__-__'
                AND extracted_date IS NOT NULL
            """)
            repairs['invalid_dates_fixed'] = cursor.rowcount
            
            # 5. Remove articles from blacklisted domains
            cursor.execute("""
                DELETE FROM articles
                WHERE source IN (SELECT domain FROM domain_blacklist)
            """)
            repairs['blacklisted_articles_removed'] = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Database repairs completed: {repairs}")
            return repairs
            
        except Exception as e:
            logger.error(f"Error repairing database: {str(e)}")
            traceback.print_exc()
            return repairs