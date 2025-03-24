"""
Database manager for the Mexican Municipal Candidates Scraper.
"""
import os
import sqlite3
import json
import time
import random
import queue
import functools
import threading
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

def db_retry(max_retries=3, retry_delay=0.5):
    """
    Decorator to retry database operations on failure.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        retry_delay (float): Base delay between retries in seconds
        
    Returns:
        function: Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(self, *args, **kwargs)
                except sqlite3.OperationalError as e:
                    # Check if it's a database lock error
                    if "database is locked" in str(e):
                        last_exception = e
                        if attempt < max_retries - 1:
                            # Add some randomness to retry delay to avoid contention
                            sleep_time = retry_delay * (2 ** attempt) * (0.5 + random.random())
                            logger.debug(f"Database locked, retrying {func.__name__} in {sleep_time:.2f}s (attempt {attempt+1}/{max_retries})")
                            time.sleep(sleep_time)
                        else:
                            logger.warning(f"Max retries reached for {func.__name__} after database lock, giving up")
                    else:
                        # For other operational errors, log and raise
                        logger.warning(f"Database error in {func.__name__}: {str(e)}")
                        raise
                except sqlite3.IntegrityError as e:
                    # Special handling for unique constraint violations
                    if "UNIQUE constraint failed" in str(e) and attempt < max_retries - 1:
                        sleep_time = retry_delay * (1.5 ** attempt) * (0.5 + random.random())
                        logger.debug(f"UNIQUE constraint failed, retrying {func.__name__} in {sleep_time:.2f}s (attempt {attempt+1}/{max_retries})")
                        time.sleep(sleep_time)
                        last_exception = e
                    else:
                        logger.warning(f"Integrity error in {func.__name__}: {str(e)}")
                        raise
                except Exception as e:
                    # For other exceptions, log and raise
                    logger.warning(f"Error in {func.__name__}: {str(e)}")
                    raise
            
            # If we've exhausted all retries
            if last_exception:
                raise last_exception
            return None
        
        return wrapper
    
    return decorator

class DatabaseManager:
    """
    Enhanced database manager with improved concurrency support and robust error handling.
    """
    
    def __init__(self, db_path, initialize=True, max_workers=5, pool_size=10):
        """
        Initialize the database manager with connection pooling.
        
        Args:
            db_path (str): Path to the SQLite database file
            initialize (bool, optional): Whether to initialize the database. Defaults to True.
            max_workers (int, optional): Maximum number of worker threads for parallel operations.
            pool_size (int, optional): Size of the connection pool. Defaults to 10.
        """
        self.db_path = db_path
        self.max_workers = max_workers
        
        # Add connection pool
        self.pool_size = pool_size
        self._connection_pool = queue.Queue(maxsize=pool_size)
        self._pool_lock = threading.RLock()
        
        # Create directory for database if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        
        # Cache for frequently accessed data
        self._cache = {
            'blacklist': set(),
            'candidates': {},
            'municipalities': {}
        }
        
        # Initialize the pool with connections
        for _ in range(pool_size):
            conn = self._create_connection()
            self._connection_pool.put(conn)
        
        # Initialize database schema
        if initialize:
            self._initialize_db()
    
    def _create_connection(self):
        """Create a new database connection with proper settings."""
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row  # Enable row factory for named columns
        
        # Set pragmas for better performance
        cursor = conn.cursor()
        for pragma in Schema.PRAGMAS:
            cursor.execute(pragma)
        
        return conn
    
    def get_connection(self, timeout=5):
        """
        Get a database connection from the pool with timeout.
        
        Returns:
            sqlite3.Connection: Database connection
        """
        try:
            # Try to get a connection from the pool
            conn = self._connection_pool.get(timeout=timeout)
            
            # Verify the connection is valid
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return conn
            except sqlite3.Error:
                # Connection is invalid, create a new one
                try:
                    conn.close()
                except:
                    pass
                logger.warning("Found invalid connection in pool, creating new one")
                return self._create_connection()
                
        except queue.Empty:
            # If the pool is empty, create a new connection
            logger.warning("Connection pool empty, creating new connection")
            return self._create_connection()
    
    def return_connection(self, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: Connection to return
        """
        try:
            # If the pool is full, just close the connection
            if self._connection_pool.full():
                conn.close()
            else:
                # Otherwise, return it to the pool
                self._connection_pool.put(conn)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {str(e)}")
            try:
                conn.close()
            except:
                pass
    
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
            self.return_connection(conn)
            
            logger.info(f"Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            traceback.print_exc()
            raise
    
    #--------------------------------------------------------------------------
    # Cache Management
    #--------------------------------------------------------------------------
    
    @db_retry(max_retries=3, retry_delay=0.5)
    def get_cached_search(self, query):
        """
        Retrieve cached search results.
        
        Args:
            query (str): Search query
            
        Returns:
            dict or None: Cached search results, or None if not found or expired
        """
        conn = None
        try:
            query_hash = hash_string(query)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT response, timestamp FROM search_cache WHERE query_hash = ?', 
                (query_hash,)
            )
            
            result = cursor.fetchone()
            
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
        
        finally:
            if conn:
                self.return_connection(conn)
    
    @db_retry(max_retries=3, retry_delay=0.5)
    def cache_search(self, query, response):
        """
        Cache search results.
        
        Args:
            query (str): Search query
            response (dict): Search results to cache
            
        Returns:
            bool: Success status
        """
        conn = None
        try:
            query_hash = hash_string(query)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'INSERT OR REPLACE INTO search_cache (query_hash, query, response, timestamp) VALUES (?, ?, ?, ?)',
                (query_hash, query, json.dumps(response), datetime.now().isoformat())
            )
            
            conn.commit()
            return True
        
        except Exception as e:
            logger.warning(f"Error caching search results: {str(e)}")
            return False
        
        finally:
            if conn:
                self.return_connection(conn)
    
    @db_retry(max_retries=3, retry_delay=0.5)
    def get_cached_content(self, url):
        """
        Retrieve cached content for a URL.
        
        Args:
            url (str): URL to retrieve content for
            
        Returns:
            dict or None: Cached content, or None if not found
        """
        conn = None
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
        
        finally:
            if conn:
                self.return_connection(conn)
    
    @db_retry(max_retries=3, retry_delay=0.5)
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
        conn = None
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
            return True
        
        except Exception as e:
            logger.warning(f"Error caching content: {str(e)}")
            return False
        
        finally:
            if conn:
                self.return_connection(conn)
    
    @db_retry(max_retries=3, retry_delay=0.5)
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
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT 1 FROM domain_blacklist WHERE domain = ?', (domain,))
            result = cursor.fetchone()
            
            is_blacklisted = result is not None
            
            # Update cache if found
            if is_blacklisted:
                self._cache['blacklist'].add(domain)
                
            return is_blacklisted
        
        except Exception as e:
            logger.warning(f"Error checking domain blacklist: {str(e)}")
            return False
        
        finally:
            if conn:
                self.return_connection(conn)
    
    # Fixed save_article method implementation
    @db_retry(max_retries=5, retry_delay=1.0)
    def save_article(self, article_data, batch_id=None):
        """
        Save or update an article and its relationship to candidates with improved error handling.
        
        Args:
            article_data (dict): Article data
            batch_id (int, optional): Batch ID
            
        Returns:
            int: Article ID or None on error
        """
        # Import traceback to avoid the UnboundLocalError
        import traceback
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Remove fields that don't exist in database schema to prevent errors
            article_data_copy = article_data.copy()
            article_data_copy.pop('success', None)
            article_data_copy.pop('from_cache', None) 
            article_data_copy.pop('oxylabs_used', None)
            
            url = article_data_copy.get('url', '')
            if not url:
                logger.warning("Cannot save article without URL")
                if conn:
                    self.return_connection(conn)
                return None
            
            # Debug output to help troubleshoot
            logger.info(f"Attempting to save article: {url[:50]}...")
            
            # Check if article already exists by URL
            cursor.execute('SELECT id FROM articles WHERE url = ?', (url,))
            result = cursor.fetchone()
            article_id = result['id'] if result else None
            
            # Log article data summary for debugging
            title = article_data_copy.get('title', '')[:50]
            content_len = len(article_data_copy.get('content', ''))
            relevance = article_data_copy.get('overall_relevance', 0)
            logger.info(f"Article: {title}... | Content length: {content_len} | Relevance: {relevance:.2f}")
            
            # Default extraction date if not provided
            if 'extraction_date' not in article_data_copy:
                article_data_copy['extraction_date'] = datetime.now().isoformat()
            
            # Set batch ID if provided
            if batch_id and 'batch_id' not in article_data_copy:
                article_data_copy['batch_id'] = batch_id
            
            # Get candidate information
            candidate_name = article_data_copy.pop('candidato', None)
            municipality = article_data_copy.pop('municipio', None)
            target_year = article_data_copy.pop('target_year', None)
            
            # Extract candidate-specific scores for the relationship
            candidate_scores = {
                'name_match_score': article_data_copy.pop('name_match_score', 0.0),
                'fuzzy_match_score': article_data_copy.pop('fuzzy_match_score', 0.0),
                'biographical_content_score': article_data_copy.pop('biographical_content_score', 0.0),
                'political_content_score': article_data_copy.pop('political_content_score', 0.0),
                'academic_score': article_data_copy.pop('academic_score', 0.0),
                'professional_score': article_data_copy.pop('professional_score', 0.0),
                'public_service_score': article_data_copy.pop('public_service_score', 0.0),
            }
            
            # Handle entities and quotes separately
            entities = article_data_copy.pop('entities', None)
            quotes = article_data_copy.pop('quotes', None)
            
            # Handle insert or update
            if article_id:
                # Update existing article
                fields = []
                values = []
                
                for key, value in article_data_copy.items():
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
                    logger.info(f"Updated existing article ID {article_id}")
            else:
                # Insert new article
                keys = []
                placeholders = []
                values = []
                
                for key, value in article_data_copy.items():
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
                    logger.info(f"Inserted new article with ID {article_id}")
            
            # Link article to candidate if candidate info is provided
            if candidate_name and municipality and target_year and article_id:
                # Get or create candidate
                state = article_data.get('entidad')
                gender = article_data.get('sexo')
                party = article_data.get('partido')
                period = article_data.get('periodo_formato_original')
                
                logger.info(f"Linking article to candidate: {candidate_name}, {municipality}, {target_year}")
                
                try:
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
                        
                        overall_relevance = article_data_copy.get('overall_relevance', 0.0)
                        
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
                            logger.info(f"Updated existing candidate-article relationship: {relation['id']}")
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
                            logger.info(f"Created new candidate-article relationship: {candidate.id}-{article_id}")
                        
                        # Save quotes
                        quotes_saved = 0
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
                                quotes_saved += 1
                        
                        if quotes_saved > 0:
                            logger.info(f"Saved {quotes_saved} quotes for candidate {candidate.id}")
                except Exception as candidate_error:
                    logger.error(f"Error linking to candidate: {str(candidate_error)}")
                    traceback.print_exc()
            
            # Save entities if provided
            entities_saved = 0
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
                            entities_saved += 1
            
            if entities_saved > 0:
                logger.info(f"Saved {entities_saved} entities for article {article_id}")
            
            conn.commit()
            
            if conn:
                self.return_connection(conn)
            
            return article_id
            
        except Exception as e:
            logger.error(f"Error saving article: {str(e)}")
            traceback.print_exc()
            if conn:
                try:
                    conn.rollback()
                    self.return_connection(conn)
                except:
                    pass
            return None

    @db_retry(max_retries=3, retry_delay=0.5)
    def get_article(self, article_id=None, url=None):
        """
        Get an article by ID or URL.
        
        Args:
            article_id (int, optional): Article ID
            url (str, optional): Article URL
            
        Returns:
            Article: Article object or None if not found
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if article_id:
                cursor.execute('SELECT * FROM articles WHERE id = ?', (article_id,))
            elif url:
                cursor.execute('SELECT * FROM articles WHERE url = ?', (url,))
            else:
                logger.warning("get_article requires either article_id or url")
                return None
            
            result = cursor.fetchone()
            
            if result:
                return Article.from_row(result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article: {str(e)}")
            return None
        
        finally:
            if conn:
                self.return_connection(conn)
    
    # ... rest of the class implementation stays the same
    
    def repair_database(self):
        """
        Perform comprehensive database repairs and integrity checks.
        
        Returns:
            dict: Summary of repairs made
        """
        repairs = {
            'orphaned_links_removed': 0,
            'orphaned_quotes_removed': 0,
            'batch_statuses_fixed': 0,
            'invalid_records_fixed': 0,
            'duplicate_entries_removed': 0,
            'locks_cleared': 0,
            'indexes_rebuilt': 0,
            'schema_fixed': False
        }
        
        try:
            logger.info("Starting database repair process...")
            
            # Get a direct connection to bypass the pool
            conn = sqlite3.connect(self.db_path, timeout=60)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Check if articles table has problematic column schema
            try:
                cursor.execute("PRAGMA table_info(articles)")
                articles_columns = cursor.fetchall()
                column_names = [col[1] for col in articles_columns]
                
                # Check for problematic columns
                problem_columns = ['success', 'from_cache', 'oxylabs_used']
                found_problem_columns = [col for col in problem_columns if col in column_names]
                
                if found_problem_columns:
                    logger.info(f"Found problematic columns in articles table: {found_problem_columns}")
                    
                    # Get all valid column names that aren't in problem_columns
                    valid_columns = [col for col in column_names if col not in problem_columns]
                    
                    # Create a new table without the problem columns
                    cursor.execute(f"CREATE TABLE articles_new AS SELECT {', '.join(valid_columns)} FROM articles")
                    
                    # Drop old table and rename new one
                    cursor.execute("DROP TABLE articles")
                    cursor.execute("ALTER TABLE articles_new RENAME TO articles")
                    
                    # Recreate indexes
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(overall_relevance)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_content_type ON articles(content_type)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_temporal ON articles(temporal_relevance)")
                    
                    repairs['schema_fixed'] = True
                    repairs['invalid_records_fixed'] += 1
            except Exception as e:
                logger.error(f"Error fixing articles table schema: {str(e)}")
                traceback.print_exc()
            
            # First, run integrity check
            cursor.execute("PRAGMA integrity_check")
            integrity_result = cursor.fetchone()[0]
            if integrity_result != "ok":
                logger.error(f"Database integrity check failed: {integrity_result}")
            
            # Clear any locks
            try:
                cursor.execute("PRAGMA busy_timeout = 60000")  # 60 second timeout
                cursor.execute("PRAGMA journal_mode = DELETE")  # Reset journal mode
                cursor.execute("PRAGMA locking_mode = NORMAL")  # Reset locking mode
                repairs['locks_cleared'] = 1
            except Exception as e:
                logger.warning(f"Error clearing database locks: {str(e)}")
            
            # 1. Remove candidate_articles with missing candidates or articles
            try:
                cursor.execute("""
                    DELETE FROM candidate_articles 
                    WHERE candidate_id NOT IN (SELECT id FROM candidates)
                    OR article_id NOT IN (SELECT id FROM articles)
                """)
                repairs['orphaned_links_removed'] = cursor.rowcount
            except Exception as e:
                logger.warning(f"Error removing orphaned candidate_articles: {str(e)}")
            
            # 2. Remove quotes with missing articles or candidates
            try:
                cursor.execute("""
                    DELETE FROM quotes
                    WHERE article_id NOT IN (SELECT id FROM articles)
                    OR candidate_id NOT IN (SELECT id FROM candidates)
                """)
                repairs['orphaned_quotes_removed'] = cursor.rowcount
            except Exception as e:
                logger.warning(f"Error removing orphaned quotes: {str(e)}")
            
            # 3. Fix batch statuses with inconsistent completion counts
            try:
                cursor.execute("""
                    UPDATE scraping_batches
                    SET status = 'IN_PROGRESS'
                    WHERE status = 'COMPLETED' 
                    AND completed_candidates < total_candidates
                """)
                repairs['batch_statuses_fixed'] += cursor.rowcount
                
                # Fix zero total_candidates
                cursor.execute("""
                    UPDATE scraping_batches
                    SET total_candidates = 1
                    WHERE total_candidates = 0
                """)
                repairs['batch_statuses_fixed'] += cursor.rowcount
            except Exception as e:
                logger.warning(f"Error fixing batch statuses: {str(e)}")
            
            # 8. Rebuild indexes
            try:
                for index_sql in Schema.INDEXES:
                    try:
                        cursor.execute(index_sql)
                        repairs['indexes_rebuilt'] += 1
                    except sqlite3.OperationalError:
                        pass  # Index already exists
            except Exception as e:
                logger.warning(f"Error rebuilding indexes: {str(e)}")
            
            # 9. Run VACUUM to reclaim space and defragment
            try:
                cursor.execute("VACUUM")
            except Exception as e:
                logger.warning(f"Error running VACUUM: {str(e)}")
            
            conn.commit()
            conn.close()
            
            # Update pool with new connections
            self._refresh_connection_pool()
            
            logger.info(f"Database repairs completed: {repairs}")
            return repairs
            
        except Exception as e:
            logger.error(f"Error repairing database: {str(e)}")
            traceback.print_exc()
            return repairs
    
    def _refresh_connection_pool(self):
        """Refresh all connections in the pool after database repair."""
        try:
            # Drain the pool
            drained_conns = []
            while not self._connection_pool.empty():
                try:
                    conn = self._connection_pool.get_nowait()
                    drained_conns.append(conn)
                except queue.Empty:
                    break
            
            # Close all old connections
            for conn in drained_conns:
                try:
                    conn.close()
                except:
                    pass
            
            # Create fresh connections
            for _ in range(self.pool_size):
                try:
                    conn = self._create_connection()
                    self._connection_pool.put(conn)
                except:
                    pass
            
            logger.info(f"Refreshed connection pool with {self._connection_pool.qsize()} new connections")
        except Exception as e:
            logger.warning(f"Error refreshing connection pool: {str(e)}")
    
    def repair_common_issues(self):
        """
        Attempt to repair common database issues.
        This function handles orphaned records, inconsistent statuses, 
        and other common database integrity problems.
        
        Returns:
            dict: Summary of repairs made
        """
        # Use the more comprehensive repair function
        return self.repair_database()

    # Note: The rest of the methods would remain unchanged