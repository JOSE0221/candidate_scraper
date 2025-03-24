#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Enhanced Mexican Municipal Candidates Web Scraper

A robust and high-performance system for gathering and organizing information
about Mexican municipal candidates from diverse sources, with enhanced
temporal relevance filtering to focus on election year ±2 years.

Features:
- Content type classification (news, articles, discourse/quotes)
- Enhanced fuzzy name matching
- Top 10 relevant links per candidate
- Structured dataset generation for ML/NLP
- Quote extraction and candidate statement analysis
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import sqlite3
import pandas as pd
import numpy as np
import requests
import re
import io
import warnings
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import dateparser
import dateutil.parser
from pprint import pprint
import traceback
from collections import Counter

# Configure logging
import logging.handlers
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Set up console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger.addHandler(console_handler)

# Set up file handler with rotation
log_directory = 'logs'
os.makedirs(log_directory, exist_ok=True)
file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(log_directory, "mexican_candidates_scraper.log"), 
    maxBytes=10*1024*1024, 
    backupCount=5,
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Oxylabs API configuration
OXYLABS_USERNAME = "jose0221_t0O13"
OXYLABS_PASSWORD = "Tr0p1c0s_159497"

# Try to import optional dependencies
try:
    from langdetect import detect
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False
    logger.warning("langdetect not available. Install with 'pip install langdetect' for better language detection.")

try:
    from fuzzywuzzy import fuzz, process
    FUZZYWUZZY_AVAILABLE = True
except ImportError:
    FUZZYWUZZY_AVAILABLE = False
    logger.warning("FuzzyWuzzy not available. Install with 'pip install fuzzywuzzy python-Levenshtein' for better name matching.")

try:
    import spacy
    try:
        NLP_MODEL = spacy.load("es_core_news_sm")
        SPACY_AVAILABLE = True
        logger.info("Loaded Spanish spaCy model for entity recognition")
    except:
        SPACY_AVAILABLE = False
        logger.warning("Spanish spaCy model not available. Install with 'python -m spacy download es_core_news_sm'")
except ImportError:
    SPACY_AVAILABLE = False
    NLP_MODEL = None
    logger.warning("spaCy not available. Install with 'pip install spacy' for better entity recognition.")


class DatabaseManager:
    """Handles all database operations for caching and storing results with enhanced features"""
    
    def __init__(self, db_path):
        """Initialize the database manager"""
        self.db_path = db_path
        
        # Initialize cache before database operations
        self._cache = {
            'blacklist': set(),  # Blacklisted domains
            'candidates': {},    # Quick lookup for candidate variations
            'municipalities': {} # Municipality variations
        }
        
        self._initialize_db()
            
    def _initialize_db(self):
        """Create database and tables if they don't exist with enhanced schema"""
        try:
            # Create directories if needed
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Set pragma for better performance
            cursor.execute('PRAGMA journal_mode = WAL')  # Use write-ahead logging
            cursor.execute('PRAGMA synchronous = NORMAL')  # Less durability but faster
            cursor.execute('PRAGMA cache_size = -32000')  # 32MB cache
            
            # Create search cache table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_cache (
                query_hash TEXT PRIMARY KEY,
                query TEXT,
                response TEXT,
                timestamp TEXT
            )
            ''')
            
            # Create content cache table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS content_cache (
                url_hash TEXT PRIMARY KEY,
                url TEXT,
                title TEXT,
                content TEXT,
                extracted_date TEXT,
                html_content TEXT,
                timestamp TEXT,
                language TEXT,
                content_length INTEGER
            )
            ''')
            
            # Create main articles table with improved structure and content classification fields
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entidad TEXT,
                cve_entidad INTEGER,
                municipio TEXT,
                cve_municipio INTEGER,
                target_year INTEGER,
                year_lower_bound INTEGER,
                year_upper_bound INTEGER,
                candidato TEXT,
                sexo TEXT,
                partido TEXT,
                periodo_formato_original TEXT,
                url TEXT UNIQUE,
                source TEXT,
                title TEXT,
                snippet TEXT,
                content TEXT,
                html_content TEXT,
                search_query TEXT,
                content_date TEXT,
                date_confidence REAL,
                year_relevance REAL,
                temporal_relevance REAL,
                content_relevance REAL,
                overall_relevance REAL,
                extraction_date TEXT,
                language TEXT,
                name_match_score REAL,
                biographical_content_score REAL,
                political_content_score REAL,
                academic_score REAL,
                professional_score REAL,
                public_service_score REAL,
                content_type TEXT,
                content_type_confidence REAL,
                quote_count INTEGER DEFAULT 0,
                fuzzy_match_score REAL DEFAULT 0,
                processed BOOLEAN DEFAULT 0,
                batch_id INTEGER
            )
            ''')
            
            # Create quotes table for storing candidate quotes
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS candidate_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER,
                candidato TEXT,
                quote_text TEXT,
                quote_context TEXT, 
                context_start INTEGER,
                context_end INTEGER,
                extraction_confidence REAL,
                extracted_date TEXT,
                FOREIGN KEY (article_id) REFERENCES articles(id)
            )
            ''')
            
            # Create domain stats table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS domain_stats (
                domain TEXT PRIMARY KEY,
                success_count INTEGER DEFAULT 0,
                failure_count INTEGER DEFAULT 0,
                avg_content_length REAL DEFAULT 0,
                last_updated TEXT,
                is_spanish BOOLEAN DEFAULT 1
            )
            ''')
            
            # Create blacklist table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS domain_blacklist (
                domain TEXT PRIMARY KEY,
                reason TEXT,
                added_date TEXT
            )
            ''')
            
            # Create entities table with enhanced fields
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS extracted_entities (
                article_id INTEGER,
                entity_type TEXT,
                entity_text TEXT,
                relevance_score REAL,
                extraction_date TEXT,
                entity_context TEXT,
                PRIMARY KEY (article_id, entity_type, entity_text),
                FOREIGN KEY (article_id) REFERENCES articles(id)
            )
            ''')
            
            # Create progress tracking table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_progress (
                candidato TEXT,
                municipio TEXT,
                target_year INTEGER,
                status TEXT,
                started_at TEXT,
                completed_at TEXT,
                articles_found INTEGER DEFAULT 0,
                batch_id INTEGER,
                PRIMARY KEY (candidato, municipio, target_year)
            )
            ''')
            
            # Create batch tracking table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                completed_at TEXT,
                total_candidates INTEGER,
                completed_candidates INTEGER DEFAULT 0,
                status TEXT,
                config TEXT
            )
            ''')
            
            # Create candidate profiles table for consolidated information
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS candidate_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidato TEXT,
                municipio TEXT,
                entidad TEXT,
                target_year INTEGER,
                sexo TEXT,
                partido TEXT,
                periodo_formato_original TEXT,
                total_articles INTEGER DEFAULT 0,
                total_quotes INTEGER DEFAULT 0,
                avg_biographical_score REAL DEFAULT 0,
                avg_political_score REAL DEFAULT 0,
                avg_academic_score REAL DEFAULT 0,
                avg_professional_score REAL DEFAULT 0,
                avg_public_service_score REAL DEFAULT 0,
                news_count INTEGER DEFAULT 0,
                article_count INTEGER DEFAULT 0,
                discourse_count INTEGER DEFAULT 0,
                profile_json TEXT,
                created_date TEXT,
                updated_date TEXT,
                UNIQUE(candidato, municipio, target_year)
            )
            ''')
            
            # Create necessary indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_candidate ON articles(candidato)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_municipality ON articles(municipio)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_year ON articles(target_year)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_year_bounds ON articles(year_lower_bound, year_upper_bound)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(overall_relevance)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_temporal_relevance ON articles(temporal_relevance)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_content_type ON articles(content_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_quotes_article ON candidate_quotes(article_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_quotes_candidate ON candidate_quotes(candidato)')
            
            # Initialize blacklist with problematic domains
            permanent_blacklist = [
                ('facebook.com', 'Social media platform'),
                ('twitter.com', 'Social media platform'),
                ('x.com', 'Social media platform (formerly Twitter)'),
                ('instagram.com', 'Social media platform'),
                ('youtube.com', 'Video platform'),
                ('tiktok.com', 'Social media platform'),
                ('linkedin.com', 'Professional network'),
                ('pinterest.com', 'Image sharing platform'),
                ('mercadolibre.com', 'E-commerce site'),
                ('amazon.com', 'E-commerce site'),
                ('walmart.com', 'E-commerce site'),
                ('flickr.com', 'Image hosting platform'),
                ('reddit.com', 'Social media platform'),
                ('slideshare.net', 'Presentation platform'),
                ('issuu.com', 'Publishing platform'),
                ('spotify.com', 'Music streaming platform'),
                ('idealista.com', 'Real estate platform'),
                ('inmuebles24.com', 'Real estate platform'),
                ('vivastreet.com', 'Classified ads platform'),
                ('olx.com', 'Classified ads platform'),
                ('segundamano.mx', 'Classified ads platform'),
                ('booking.com', 'Travel platform'),
                ('tripadvisor.com', 'Travel platform'),
                ('googleusercontent.com', 'Google storage domain'),
                ('googleapis.com', 'Google API domain'),
                ('google.com', 'Search engine'),
            ]
            
            for domain, reason in permanent_blacklist:
                cursor.execute(
                    'INSERT OR IGNORE INTO domain_blacklist (domain, reason, added_date) VALUES (?, ?, ?)',
                    (domain, reason, datetime.now().isoformat())
                )
                self._cache['blacklist'].add(domain)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Enhanced database initialized at {self.db_path}")
        
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            traceback.print_exc()
            raise
            
    def get_connection(self):
        """Get a database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Use row factory for named columns
        return conn
    
    def get_cached_search(self, query):
        """Retrieve cached search results"""
        try:
            query_hash = self._hash_string(query)
            
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
        """Cache search results"""
        try:
            query_hash = self._hash_string(query)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'INSERT OR REPLACE INTO search_cache (query_hash, query, response, timestamp) VALUES (?, ?, ?, ?)',
                (query_hash, query, json.dumps(response), datetime.now().isoformat())
            )
            
            conn.commit()
            conn.close()
        
        except Exception as e:
            logger.warning(f"Error caching search results: {str(e)}")
    
    def get_cached_content(self, url):
        """Retrieve cached content"""
        try:
            url_hash = self._hash_string(url)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT title, content, extracted_date, html_content, timestamp, language, content_length FROM content_cache WHERE url_hash = ?',
                (url_hash,)
            )
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                title, content, extracted_date, html_content, timestamp, language, content_length = result
                return {
                    'title': title, 
                    'content': content, 
                    'extracted_date': extracted_date,
                    'html_content': html_content,
                    'language': language,
                    'content_length': content_length,
                    'from_cache': True
                }
            
            return None
        
        except Exception as e:
            logger.warning(f"Error retrieving content cache: {str(e)}")
            return None
    
    def cache_content(self, url, title, content, extracted_date=None, html_content=None, language=None):
        """Cache extracted content"""
        try:
            url_hash = self._hash_string(url)
            content_length = len(content) if content else 0
            
            # Truncate large HTML content
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
        
        except Exception as e:
            logger.warning(f"Error caching content: {str(e)}")
    
    def save_article(self, article_data, batch_id=None):
        """Save article to database with enhanced fields"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if URL already exists to get id for updating
            existing_id = None
            if 'url' in article_data and article_data['url']:
                cursor.execute('SELECT id FROM articles WHERE url = ?', (article_data['url'],))
                result = cursor.fetchone()
                if result:
                    existing_id = result['id']
            
            # Prepare the field lists dynamically based on the article_data
            fields = []
            placeholders = []
            values = []
            
            # Basic article fields
            possible_fields = [
                'entidad', 'cve_entidad', 'municipio', 'cve_municipio', 
                'target_year', 'year_lower_bound', 'year_upper_bound', 'candidato', 
                'sexo', 'partido', 'periodo_formato_original', 'url', 'source', 'title', 
                'snippet', 'content', 'html_content', 'search_query', 'content_date',
                'date_confidence', 'year_relevance', 'temporal_relevance', 'content_relevance',
                'overall_relevance', 'extraction_date', 'language',
                'name_match_score', 'biographical_content_score', 'political_content_score',
                'academic_score', 'professional_score', 'public_service_score',
                'content_type', 'content_type_confidence', 'quote_count', 'fuzzy_match_score',
                'batch_id'
            ]
            
            # Only include fields that exist in article_data
            for field in possible_fields:
                if field in article_data:
                    fields.append(field)
                    placeholders.append('?')
                    values.append(article_data.get(field))
            
            # Add extraction_date if not explicitly provided
            if 'extraction_date' not in article_data:
                fields.append('extraction_date')
                placeholders.append('?')
                values.append(datetime.now().isoformat())
            
            # Handle insert or update
            if existing_id:
                # Update existing article
                set_clause = ', '.join([f"{field} = ?" for field in fields])
                query = f"UPDATE articles SET {set_clause} WHERE id = ?"
                values.append(existing_id)
                cursor.execute(query, values)
                article_id = existing_id
            else:
                # Insert new article
                fields_str = ', '.join(fields)
                placeholders_str = ', '.join(placeholders)
                query = f"INSERT INTO articles ({fields_str}) VALUES ({placeholders_str})"
                cursor.execute(query, values)
                article_id = cursor.lastrowid
            
            # Save extracted quotes if available
            if 'quotes' in article_data and article_id:
                for quote_data in article_data['quotes']:
                    cursor.execute(
                        '''INSERT INTO candidate_quotes 
                        (article_id, candidato, quote_text, quote_context, context_start, context_end, 
                         extraction_confidence, extracted_date) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (article_id, article_data.get('candidato', ''), 
                         quote_data.get('text', ''), quote_data.get('context', ''),
                         quote_data.get('context_start', 0), quote_data.get('context_end', 0),
                         quote_data.get('confidence', 0.5), datetime.now().isoformat())
                    )
            
            # Save extracted entities if available
            if 'entities' in article_data and article_id:
                for entity_type, entities in article_data['entities'].items():
                    if isinstance(entities, dict):
                        for entity_text, score in entities.items():
                            # For simplicity, we're setting entity_context to empty string
                            # A more advanced version would extract context around each entity
                            cursor.execute(
                                '''INSERT OR REPLACE INTO extracted_entities 
                                (article_id, entity_type, entity_text, relevance_score, extraction_date, entity_context) 
                                VALUES (?, ?, ?, ?, ?, ?)''',
                                (article_id, entity_type, entity_text, score, 
                                 datetime.now().isoformat(), '')
                            )
            
            conn.commit()
            conn.close()
            
            return article_id
        
        except Exception as e:
            logger.error(f"Error saving article: {str(e)}")
            traceback.print_exc()
            return None
    
    def is_blacklisted(self, domain):
        """Check if a domain is blacklisted"""
        # Check memory cache first for performance
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
    
    def update_domain_stats(self, domain, success=True, content_length=0, is_spanish=True):
        """Update domain success/failure statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get current stats
            cursor.execute('SELECT success_count, failure_count, avg_content_length FROM domain_stats WHERE domain = ?', (domain,))
            result = cursor.fetchone()
            
            if result:
                success_count, failure_count, avg_length = result
                
                # Update stats
                if success:
                    success_count += 1
                    total_content = (avg_length * (success_count - 1) + content_length)
                    avg_length = total_content / success_count if success_count > 0 else 0
                else:
                    failure_count += 1
                
                cursor.execute(
                    'UPDATE domain_stats SET success_count = ?, failure_count = ?, avg_content_length = ?, last_updated = ?, is_spanish = ? WHERE domain = ?',
                    (success_count, failure_count, avg_length, datetime.now().isoformat(), is_spanish, domain)
                )
            else:
                # Create new record
                cursor.execute(
                    'INSERT INTO domain_stats (domain, success_count, failure_count, avg_content_length, last_updated, is_spanish) VALUES (?, ?, ?, ?, ?, ?)',
                    (domain, 1 if success else 0, 0 if success else 1, content_length if success else 0, datetime.now().isoformat(), is_spanish)
                )
            
            conn.commit()
            conn.close()
        
        except Exception as e:
            logger.warning(f"Error updating domain stats: {str(e)}")
    
    def save_candidate_quotes(self, article_id, candidate_name, quotes):
        """Save candidate quotes extracted from an article"""
        if not quotes or not article_id:
            return 0
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            quote_count = 0
            for quote_data in quotes:
                cursor.execute(
                    '''INSERT INTO candidate_quotes 
                    (article_id, candidato, quote_text, quote_context, context_start, context_end, 
                     extraction_confidence, extracted_date) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (article_id, candidate_name, 
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
    
    def create_batch(self, total_candidates, config=None):
        """Create a new batch for processing"""
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
        """Update batch status"""
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
        
        except Exception as e:
            logger.error(f"Error updating batch status: {str(e)}")
    
    def update_candidate_progress(self, candidate_name, municipality, target_year, status, 
                                 articles_found=0, batch_id=None):
        """Update scraping progress for a candidate"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if entry exists
            cursor.execute(
                '''SELECT 1 FROM scraping_progress 
                   WHERE candidato = ? AND municipio = ? AND target_year = ?''',
                (candidate_name, municipality, target_year)
            )
            
            exists = cursor.fetchone() is not None
            
            current_time = datetime.now().isoformat()
            
            if exists:
                # Update existing record
                if status == 'COMPLETED':
                    cursor.execute(
                        '''UPDATE scraping_progress 
                           SET status = ?, articles_found = ?, completed_at = ?, batch_id = ?
                           WHERE candidato = ? AND municipio = ? AND target_year = ?''',
                        (status, articles_found, current_time, batch_id, 
                         candidate_name, municipality, target_year)
                    )
                else:
                    cursor.execute(
                        '''UPDATE scraping_progress 
                           SET status = ?, articles_found = ?, batch_id = ?
                           WHERE candidato = ? AND municipio = ? AND target_year = ?''',
                        (status, articles_found, batch_id, 
                         candidate_name, municipality, target_year)
                    )
            else:
                # Insert new record
                if status == 'COMPLETED':
                    cursor.execute(
                        '''INSERT INTO scraping_progress 
                           (candidato, municipio, target_year, status, 
                            started_at, completed_at, articles_found, batch_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (candidate_name, municipality, target_year, status, 
                         current_time, current_time, articles_found, batch_id)
                    )
                else:
                    cursor.execute(
                        '''INSERT INTO scraping_progress 
                           (candidato, municipio, target_year, status, 
                            started_at, articles_found, batch_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?)''',
                        (candidate_name, municipality, target_year, status, 
                         current_time, articles_found, batch_id)
                    )
            
            conn.commit()
            conn.close()
            
            # If a candidate was completed, update the batch progress
            if status == 'COMPLETED' and batch_id:
                self.increment_batch_progress(batch_id)
            
        except Exception as e:
            logger.error(f"Error updating candidate progress: {str(e)}")
    
    def increment_batch_progress(self, batch_id):
        """Increment completed candidates count for a batch"""
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
            
        except Exception as e:
            logger.error(f"Error incrementing batch progress: {str(e)}")
    
    def get_completed_candidates(self, batch_id=None):
        """Get list of completed candidate names"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if batch_id:
                cursor.execute(
                    '''SELECT candidato, municipio, target_year FROM scraping_progress 
                       WHERE status = 'COMPLETED' AND batch_id = ?''',
                    (batch_id,)
                )
            else:
                cursor.execute(
                    '''SELECT candidato, municipio, target_year FROM scraping_progress 
                       WHERE status = 'COMPLETED' '''
                )
            
            results = cursor.fetchall()
            conn.close()
            
            # Convert to list of tuples
            completed = [(r['candidato'], r['municipio'], r['target_year']) for r in results]
            
            return completed
            
        except Exception as e:
            logger.error(f"Error getting completed candidates: {str(e)}")
            return []
    
    def get_candidates_to_process(self, all_candidates, batch_id=None):
        """Get candidates that need to be processed (not completed yet)"""
        completed = set(self.get_completed_candidates(batch_id))
        
        # Filter out completed candidates
        to_process = []
        for candidate in all_candidates:
            name = candidate.get('PRESIDENTE_MUNICIPAL')
            municipality = candidate.get('MUNICIPIO')
            year = candidate.get('Year')
            
            if (name, municipality, year) not in completed:
                to_process.append(candidate)
        
        return to_process
    
    def get_candidate_articles(self, candidate_name, municipality=None, target_year=None, min_relevance=0.3):
        """Get all articles for a specific candidate with optional filtering"""
        try:
            conn = self.get_connection()
            query = "SELECT * FROM articles WHERE candidato = ? AND overall_relevance >= ?"
            params = [candidate_name, min_relevance]
            
            if municipality:
                query += " AND municipio = ?"
                params.append(municipality)
                
            if target_year:
                query += " AND target_year = ?"
                params.append(target_year)
                
            query += " ORDER BY overall_relevance DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
        except Exception as e:
            logger.error(f"Error retrieving candidate articles: {str(e)}")
            return pd.DataFrame()
    
    def get_all_articles(self, min_relevance=0.1, target_year=None, year_range=None, 
                        batch_id=None, limit=None, temporal_relevance_threshold=0.3):
        """Retrieve all articles with optional relevance filter and year range"""
        try:
            conn = self.get_connection()
            query = """
            SELECT * FROM articles WHERE overall_relevance >= ? 
            AND temporal_relevance >= ?
            """
            params = [min_relevance, temporal_relevance_threshold]
                
            if target_year is not None:
                if year_range is not None:
                    # Use the explicit year bounds if present, otherwise calculate from target year
                    query += """ 
                    AND (
                        (year_lower_bound IS NOT NULL AND year_upper_bound IS NOT NULL AND 
                         ? BETWEEN year_lower_bound AND year_upper_bound)
                        OR 
                        (target_year BETWEEN ? AND ?)
                    )"""
                    params.extend([target_year, target_year - year_range, target_year + year_range])
                else:
                    query += " AND target_year = ?"
                    params.append(target_year)
            
            if batch_id is not None:
                query += " AND batch_id = ?"
                params.append(batch_id)
                    
            query += " ORDER BY candidato, overall_relevance DESC"
            
            if limit is not None:
                query += " LIMIT ?"
                params.append(limit)
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
        except Exception as e:
            logger.error(f"Error retrieving articles: {str(e)}")
            return pd.DataFrame()
    
    def get_candidate_quotes(self, candidate_name, municipality=None, target_year=None):
        """Get all quotes for a specific candidate"""
        try:
            conn = self.get_connection()
            
            query = """
            SELECT q.*, a.url, a.title, a.source, a.content_date, a.content_type
            FROM candidate_quotes q
            JOIN articles a ON q.article_id = a.id
            WHERE q.candidato = ?
            """
            params = [candidate_name]
            
            if municipality:
                query += " AND a.municipio = ?"
                params.append(municipality)
                
            if target_year:
                query += " AND a.target_year = ?"
                params.append(target_year)
                
            query += " ORDER BY q.extraction_confidence DESC, a.overall_relevance DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
        
        except Exception as e:
            logger.error(f"Error retrieving candidate quotes: {str(e)}")
            return pd.DataFrame()
    
    def get_candidate_profile(self, candidate_name, municipality=None, year=None, year_range=2):
        """Get a complete profile for a candidate by aggregating all articles"""
        try:
            conn = self.get_connection()
            
            # Improved query with year range filtering
            if year:
                query = """
                SELECT * FROM articles 
                WHERE candidato = ?
                AND (
                    (year_lower_bound IS NOT NULL AND year_upper_bound IS NOT NULL AND 
                     ? BETWEEN year_lower_bound AND year_upper_bound)
                    OR 
                    (target_year BETWEEN ? AND ?)
                    OR
                    (? BETWEEN year_lower_bound AND year_upper_bound)
                )
                """
                params = [
                    candidate_name,
                    year,
                    year - year_range, year + year_range,
                    year
                ]
                
                if municipality:
                    query += " AND municipio = ?"
                    params.append(municipality)
            else:
                query = "SELECT * FROM articles WHERE candidato = ?"
                params = [candidate_name]
                
                if municipality:
                    query += " AND municipio = ?"
                    params.append(municipality)
                
            query += " ORDER BY overall_relevance DESC, temporal_relevance DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty:
                conn.close()
                return None
            
            # Get quotes for this candidate
            quotes_query = """
            SELECT q.* FROM candidate_quotes q
            JOIN articles a ON q.article_id = a.id
            WHERE q.candidato = ?
            """
            quotes_params = [candidate_name]
            
            if municipality:
                quotes_query += " AND a.municipio = ?"
                quotes_params.append(municipality)
                
            if year:
                quotes_query += " AND a.target_year = ?"
                quotes_params.append(year)
                
            quotes_df = pd.read_sql_query(quotes_query, conn, params=quotes_params)
            
            # Count content types
            content_types = df['content_type'].value_counts().to_dict()
            
            # Create profile summary
            profile = {
                'name': candidate_name,
                'municipality': municipality,
                'year': year,
                'gender': df['sexo'].iloc[0] if 'sexo' in df.columns else '',
                'political_party': df['partido'].iloc[0] if 'partido' in df.columns else '',
                'period': df['periodo_formato_original'].iloc[0] if 'periodo_formato_original' in df.columns else '',
                'article_count': len(df),
                'avg_relevance': float(df['overall_relevance'].mean()),
                'avg_temporal_relevance': float(df['temporal_relevance'].mean()) if 'temporal_relevance' in df.columns else 0.0,
                'top_sources': df['source'].value_counts().head(5).to_dict(),
                'biography_score': float(df['biographical_content_score'].mean()),
                'political_score': float(df['political_content_score'].mean()),
                'academic_score': float(df['academic_score'].mean() if 'academic_score' in df.columns else 0.0),
                'professional_score': float(df['professional_score'].mean() if 'professional_score' in df.columns else 0.0),
                'public_service_score': float(df['public_service_score'].mean() if 'public_service_score' in df.columns else 0.0),
                'content_types': content_types,
                'quote_count': len(quotes_df)
            }
            
            # Add excerpts from top articles
            top_articles = df.sort_values(['overall_relevance', 'temporal_relevance'], ascending=[False, False]).head(3)
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
                        'relevance': float(article['overall_relevance']),
                        'temporal_relevance': float(article.get('temporal_relevance', 0.0)),
                        'content_date': article.get('content_date', ''),
                        'content_type': article.get('content_type', 'unknown')
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
            self._save_candidate_profile(profile)
            
            conn.close()
            return profile
            
        except Exception as e:
            logger.error(f"Error retrieving candidate profile: {str(e)}")
            traceback.print_exc()
            return None
    
    def _save_candidate_profile(self, profile):
        """Save or update a candidate profile in the database"""
        try:
            name = profile.get('name')
            municipality = profile.get('municipality')
            year = profile.get('year')
            
            if not name:
                return False
                
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if profile already exists
            cursor.execute(
                'SELECT id FROM candidate_profiles WHERE candidato = ? AND municipio = ? AND target_year = ?',
                (name, municipality or '', year or 0)
            )
            
            result = cursor.fetchone()
            
            current_time = datetime.now().isoformat()
            profile_json = json.dumps(profile, ensure_ascii=False)
            
            if result:
                # Update existing profile
                cursor.execute(
                    '''UPDATE candidate_profiles SET 
                    entidad = ?, sexo = ?, partido = ?, periodo_formato_original = ?,
                    total_articles = ?, total_quotes = ?, 
                    avg_biographical_score = ?, avg_political_score = ?,
                    avg_academic_score = ?, avg_professional_score = ?,
                    avg_public_service_score = ?,
                    news_count = ?, article_count = ?, discourse_count = ?,
                    profile_json = ?, updated_date = ?
                    WHERE id = ?''',
                    (
                        profile.get('state', ''),
                        profile.get('gender', ''),
                        profile.get('political_party', ''),
                        profile.get('period', ''),
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
                    (candidato, municipio, entidad, target_year, sexo, partido, periodo_formato_original,
                    total_articles, total_quotes, 
                    avg_biographical_score, avg_political_score,
                    avg_academic_score, avg_professional_score, avg_public_service_score,
                    news_count, article_count, discourse_count,
                    profile_json, created_date, updated_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        name,
                        municipality or '',
                        profile.get('state', ''),
                        year or 0,
                        profile.get('gender', ''),
                        profile.get('political_party', ''),
                        profile.get('period', ''),
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
    
    def generate_ml_dataset(self, output_path='data', format='json', min_relevance=0.3):
        """Generate a structured dataset for ML/NLP applications"""
        try:
            os.makedirs(output_path, exist_ok=True)
            
            # Get all candidate profiles
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get profiles ordered by total articles (most data first)
            cursor.execute('''
            SELECT * FROM candidate_profiles
            ORDER BY total_articles DESC, total_quotes DESC
            ''')
            
            profiles = cursor.fetchall()
            
            if not profiles:
                logger.warning("No candidate profiles found for dataset generation")
                conn.close()
                return None
            
            # Create dataset
            dataset = []
            
            for profile in profiles:
                # Parse profile JSON
                profile_data = json.loads(profile['profile_json'])
                
                # Get articles and quotes for this candidate
                articles_df = self.get_candidate_articles(
                    profile['candidato'], 
                    profile['municipio'], 
                    profile['target_year'], 
                    min_relevance
                )
                
                quotes_df = self.get_candidate_quotes(
                    profile['candidato'], 
                    profile['municipio'], 
                    profile['target_year']
                )
                
                # Skip candidates with no articles
                if articles_df.empty:
                    continue
                
                # Create candidate entry
                candidate_entry = {
                    'candidate_id': f"{profile['candidato']}_{profile['municipio']}_{profile['target_year']}".replace(" ", "_"),
                    'name': profile['candidato'],
                    'municipality': profile['municipio'],
                    'state': profile['entidad'],
                    'election_year': profile['target_year'],
                    'gender': profile['sexo'],
                    'party': profile['partido'],
                    'period': profile['periodo_formato_original'],
                    
                    # High-level statistics
                    'num_articles': len(articles_df),
                    'num_quotes': len(quotes_df),
                    'content_type_distribution': {
                        'news': profile['news_count'],
                        'article': profile['article_count'],
                        'discourse': profile['discourse_count'],
                        'unknown': len(articles_df) - (profile['news_count'] + profile['article_count'] + profile['discourse_count'])
                    },
                    
                    # Scoring metrics
                    'metrics': {
                        'biographical_score': float(profile['avg_biographical_score']),
                        'political_score': float(profile['avg_political_score']),
                        'academic_score': float(profile['avg_academic_score']),
                        'professional_score': float(profile['avg_professional_score']),
                        'public_service_score': float(profile['avg_public_service_score'])
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
                        'name': entry['name'],
                        'municipality': entry['municipality'],
                        'state': entry['state'],
                        'election_year': entry['election_year'],
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
                            'candidate_name': name,
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
    
    def create_candidate_profiles(self, min_relevance=0.3):
        """Create or update profiles for all candidates with articles"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get unique candidates with articles
            cursor.execute('''
            SELECT DISTINCT candidato, municipio, target_year, entidad, sexo, partido, periodo_formato_original
            FROM articles
            WHERE overall_relevance >= ?
            ''', (min_relevance,))
            
            candidates = cursor.fetchall()
            conn.close()
            
            if not candidates:
                logger.warning("No candidates found for profile creation")
                return 0
            
            updated_count = 0
            for candidate in candidates:
                # Get profile for each candidate
                profile = self.get_candidate_profile(
                    candidate['candidato'],
                    candidate['municipio'],
                    candidate['target_year']
                )
                
                if profile:
                    updated_count += 1
            
            logger.info(f"Created/updated {updated_count} candidate profiles")
            return updated_count
            
        except Exception as e:
            logger.error(f"Error creating candidate profiles: {str(e)}")
            traceback.print_exc()
            return 0
    
    def _hash_string(self, text):
        """Create a hash from a string"""
        import hashlib
        if isinstance(text, str):
            return hashlib.md5(text.encode()).hexdigest()
        return hashlib.md5(str(text).encode()).hexdigest()


class OxylabsAPIManager:
    """Oxylabs API Manager for proxy and search capabilities"""
    
    def __init__(self, username, password, country='mx', api_mode="direct"):
        """Initialize the Oxylabs API manager"""
        self.username = username
        self.password = password
        self.country = country
        self.api_mode = api_mode
        self.session_id = self._generate_session_id()
        
        # Realtime API endpoint
        self.realtime_api_endpoint = 'https://realtime.oxylabs.io/v1/queries'
        
        # Map proxy types to Oxylabs endpoints
        self.proxy_endpoints = {
            'datacenter': 'dc.oxylabs.io:9000',
            'residential': f'customer-{username}.pr.oxylabs.io:7777',
            'serp': f'customer-{username}.os.oxylabs.io:9000',
        }
        
        # Specific agent headers for better results
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        ]
        
        logger.info(f"Initialized OxylabsAPIManager with country={country}")
    
    def _generate_session_id(self):
        """Generate a unique session ID for sticky sessions"""
        import random
        import time
        return f"mexscraper_{int(time.time())}_{random.randint(1000, 9999)}"
    
    def get_proxy_url(self, target_domain=None):
        """Get a formatted proxy URL for requests"""
        endpoint = self.proxy_endpoints.get('residential', self.proxy_endpoints['datacenter'])
        
        # Build the proxy URL with authentication
        proxy_url = f"http://{self.username}:{self.password}@{endpoint}"
        
        # Add country and session parameters
        params = []
        
        if self.country:
            params.append(f"country={self.country}")
        
        if self.session_id:
            params.append(f"session={self.session_id}")
        
        if target_domain:
            params.append(f"domain={target_domain}")
        
        # Add parameters to URL if any exist
        if params:
            proxy_url += "?" + "&".join(params)
        
        return proxy_url
    
    def get_proxies(self, target_domain=None):
        """Get proxy dictionary for requests"""
        proxy_url = self.get_proxy_url(target_domain)
        return {
            'http': proxy_url,
            'https': proxy_url
        }
    
    def realtime_api_request(self, search_query, source='google_search', parse=True, context=None):
        """Make a request using Oxylabs Realtime API with enhanced parameters"""
        try:
            # Basic payload structure - updated based on working example
            payload = {
                'source': source,
                'query': search_query,
                'parse': parse,
                'geo_location': self.country.upper() or "Mexico",
                'user_agent_type': 'desktop',
                'start_page': 1,
                'pages': 3,  # Retrieve multiple pages for more results
                'results_per_page': 20  # Try to get more results per page
            }
            
            # Add additional context if provided
            if context:
                payload.update(context)
            
            # Add locale for Mexican Spanish
            if 'locale' not in payload:
                payload['locale'] = 'es_MX'
            
            # Add specific options for Google
            if source == 'google_search':
                payload['parse_links'] = True
                payload['domain'] = 'google.com.mx'
                
                # Add a random delay for more natural request pattern
                payload['delay'] = random.randint(1000, 3000)
                
            # Make the request
            logger.info(f"Sending Oxylabs request for query: {search_query[:50]}...")
            response = requests.post(
                self.realtime_api_endpoint,
                auth=(self.username, self.password),
                json=payload,
                timeout=60
            )
            
            # Check for success
            if response.status_code == 200:
                logger.info(f"Oxylabs API request successful for: {search_query[:50]}...")
                return response.json()
            else:
                logger.warning(f"Oxylabs Realtime API error: {response.status_code} - {response.text}")
                return {'error': f"API error: {response.status_code}", 'status_code': response.status_code}
                
        except Exception as e:
            logger.error(f"Oxylabs Realtime API request error: {str(e)}")
            traceback.print_exc()
            return {'error': str(e)}
    
    def search(self, query, context=None):
        """Perform a search using Oxylabs with enhanced processing"""
        # Try to use the realtime API method using parameters from the working example
        response = self.realtime_api_request(
            query,
            source='google_search',
            parse=True,
            context=context
        )
        
        if 'error' in response:
            logger.warning(f"Oxylabs API error: {response['error']}")
            return []
        
        results = []
        
        # Extract search results from response
        try:
            if 'results' in response and len(response['results']) > 0:
                # Process all result pages
                for page_result in response['results']:
                    if 'content' in page_result and 'organic' in page_result['content']:
                        for item in page_result['content']['organic']:
                            result = {
                                'title': item.get('title', 'No title'),
                                'url': item.get('url', item.get('link', '')),
                                'snippet': item.get('description', item.get('snippet', '')),
                                'source': urlparse(item.get('url', item.get('link', ''))).netloc,
                                'position': item.get('position', 0),
                                'oxylabs_used': True
                            }
                            
                            # Skip duplicate URLs
                            if any(r['url'] == result['url'] for r in results):
                                continue
                                
                            results.append(result)
                    
                    # Try alternative response formats
                    elif 'organic_results' in page_result:
                        for item in page_result['organic_results']:
                            result = {
                                'title': item.get('title', 'No title'),
                                'url': item.get('url', item.get('link', '')),
                                'snippet': item.get('description', item.get('snippet', '')),
                                'source': urlparse(item.get('url', item.get('link', ''))).netloc,
                                'position': item.get('position', 0),
                                'oxylabs_used': True
                            }
                            
                            # Skip duplicate URLs
                            if any(r['url'] == result['url'] for r in results):
                                continue
                                
                            results.append(result)
        except Exception as parse_error:
            logger.warning(f"Error parsing Oxylabs results: {str(parse_error)}")
            traceback.print_exc()
        
        logger.info(f"Extracted {len(results)} search results from Oxylabs API")
        return results
    
    def fetch_content(self, url, headers=None, timeout=30):
        """Fetch content through Oxylabs"""
        try:
            if self.api_mode == 'realtime':
                return self._fetch_with_realtime_api(url, timeout)
            else:
                return self._fetch_with_direct_proxy(url, headers, timeout)
        except Exception as e:
            logger.error(f"Oxylabs fetch error: {str(e)}")
            return {'error': str(e)}
    
    def _fetch_with_direct_proxy(self, url, headers=None, timeout=30):
        """Fetch content using direct proxy connection"""
        try:
            # Get domain for proxy targeting
            domain = urlparse(url).netloc
            
            # Set up proxies
            proxies = self.get_proxies(domain)
            
            # Set up headers if not provided
            if not headers:
                headers = {
                    'User-Agent': random.choice(self.user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            
            # Make the request
            response = requests.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=timeout
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Oxylabs direct proxy fetch error: {str(e)}")
            return {'error': str(e)}
    
    def _fetch_with_realtime_api(self, url, timeout=30):
        """Fetch content using Oxylabs Realtime API"""
        try:
            # Basic payload for web scraping - updated based on working example
            payload = {
                'source': 'universal',
                'url': url,
                'render': 'html',
                'geo_location': self.country.upper() or "Mexico",
                'user_agent_type': 'desktop',
                'parse': True,
                'wait_for': {
                    'type': 'load',
                    'delay': 2000,  # Wait 2 seconds for JS heavy pages
                }
            }
            
            # Add specific options for better results
            payload['retry_404'] = True
            payload['locale'] = 'es_MX'
            payload['user_agent'] = random.choice(self.user_agents)
            
            # Make the request
            response = requests.post(
                self.realtime_api_endpoint,
                auth=(self.username, self.password),
                json=payload,
                timeout=timeout
            )
            
            # Check for success
            if response.status_code == 200:
                api_response = response.json()
                
                if 'results' in api_response and len(api_response['results']) > 0:
                    # Create a response-like object
                    class ResponseLike:
                        def __init__(self, content, status_code, url):
                            self.content = content
                            self.status_code = status_code
                            self.url = url
                            self.text = content.decode('utf-8', errors='replace')
                            self.encoding = 'utf-8'
                    
                    # Extract the HTML content
                    result = api_response['results'][0]
                    content = result.get('content', '').encode('utf-8')
                    status_code = result.get('status_code', 200)
                    
                    return ResponseLike(content, status_code, url)
                else:
                    logger.warning(f"Oxylabs Realtime API returned no results for {url}")
                    return {'error': "API returned no results", 'status_code': response.status_code}
            else:
                logger.warning(f"Oxylabs Realtime API error: {response.status_code} - {response.text}")
                return {'error': f"API error: {response.status_code}", 'status_code': response.status_code}
                
        except Exception as e:
            logger.error(f"Oxylabs Realtime API fetch error: {str(e)}")
            return {'error': str(e)}
    
    def rotate_session(self):
        """Rotate the session ID to get a new IP address"""
        self.session_id = self._generate_session_id()
        logger.info(f"Rotated proxy session to {self.session_id}")
        return self.session_id


class ContentClassifier:
    """Classifies content into news, articles, discourse/quotes from candidates"""
    
    def __init__(self, nlp_model=None):
        """Initialize with optional NLP model"""
        self.nlp_model = nlp_model
        
        # Speech indicators in Spanish
        self.speech_indicators = [
            "dijo", "afirmó", "declaró", "expresó", "mencionó", "señaló",
            "comentó", "indicó", "manifestó", "sostuvo", "aseguró",
            '"', '"', '«', '»', ':', '"', 'agregó', 'añadió', 'subrayó'
        ]
        
        # News indicators
        self.news_indicators = [
            "noticia", "reportaje", "periodista", "diario", "periódico",
            "boletín", "comunicado", "informó", "reportó", "publicó",
            "redacción", "staff", "corresponsal", "agencia", "fuente",
            "fecha de publicación", "publicado el", "actualizado el"
        ]
        
        # Article indicators
        self.article_indicators = [
            "artículo", "opinión", "editorial", "columna", "análisis",
            "perspectiva", "punto de vista", "enfoque", "estudio", "investigación",
            "escrito por", "autor", "colaboración"
        ]
    
    def classify_content(self, text, candidate_name):
        """Classify content type and extract quotes if available"""
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
        quotes = self._extract_quotes(text, candidate_name)
        
        quote_count = len(quotes)
        if quote_count > 0:
            discourse_score += 2  # Base score for having quotes
        
        # Check for candidate in quotes
        candidate_in_quotes = False
        candidate_lower = candidate_name.lower()
        name_parts = candidate_lower.split()
        
        for quote in quotes:
            quote_lower = quote.get('text', '').lower()
            # Check if candidate is mentioned in quotes
            if candidate_lower in quote_lower:
                candidate_in_quotes = True
                discourse_score += 3
                break
            # Check name parts (first name, last name)
            if len(name_parts) > 1:
                for part in name_parts:
                    if len(part) > 3 and part in quote_lower:
                        candidate_in_quotes = True
                        discourse_score += 2
                        break
        
        # Check speech indicators near candidate name
        for indicator in self.speech_indicators:
            indicator_lower = indicator.lower()
            if indicator_lower in text_lower:
                discourse_score += 1
                
                # Higher score if speech indicator is near candidate name
                # Check in windows of 100 characters around each mention of candidate name
                pos = 0
                while True:
                    pos = text_lower.find(candidate_lower, pos)
                    if pos == -1:
                        break
                    
                    # Define a window around the candidate name
                    start = max(0, pos - 100)
                    end = min(len(text_lower), pos + len(candidate_lower) + 100)
                    window = text_lower[start:end]
                    
                    if indicator_lower in window:
                        discourse_score += 3
                        break
                    
                    pos += len(candidate_lower)
        
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
    
    def _extract_quotes(self, text, candidate_name):
        """Extract quotes from text with context"""
        if not text:
            return []
            
        import re
        quotes = []
        
        # Different quote patterns in Spanish media
        patterns = [
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
        
        # Apply all patterns
        for pattern_idx, pattern in enumerate(patterns):
            # Get all matches with their positions
            for match in re.finditer(pattern, text):
                if pattern_idx >= 8:  # Special case for name + verb + quote pattern
                    quote_text = match.group(2)
                    name_mentioned = match.group(1)
                    
                    # Only add if the name is related to our candidate
                    if not self._is_related_name(name_mentioned, candidate_name):
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
    
    def _calculate_quote_confidence(self, quote_text, context, candidate_name):
        """Calculate confidence that this is a quote from the candidate"""
        confidence = 0.5  # Base confidence
        
        # Check if candidate name is in the context
        candidate_lower = candidate_name.lower()
        context_lower = context.lower()
        
        if candidate_lower in context_lower:
            confidence += 0.3
        
        # Check name parts
        name_parts = candidate_lower.split()
        if len(name_parts) > 1:
            for part in name_parts:
                if len(part) > 3 and part in context_lower:
                    confidence += 0.1
                    break
        
        # Check for speech indicators near the quote
        speech_count = 0
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
        """Check if a name mentioned in text is related to our candidate"""
        if not name_text or not candidate_name:
            return False
        
        name_text = name_text.lower()
        candidate_name = candidate_name.lower()
        
        # Direct match
        if name_text == candidate_name:
            return True
        
        # Check parts
        name_parts = candidate_name.split()
        found_parts = 0
        
        for part in name_parts:
            if len(part) > 3 and part in name_text:
                found_parts += 1
        
        # If we found multiple parts of the name, it's probably related
        return found_parts >= 2


class ContentExtractor:
    """Content extractor with improved text handling and content classification"""
    
    def __init__(self, db_manager, oxylabs_manager=None):
        """Initialize the content extractor"""
        self.db = db_manager
        self.oxylabs = oxylabs_manager
        self.max_retries = 3
        
        # Create content classifier
        self.content_classifier = ContentClassifier(
            nlp_model=NLP_MODEL if SPACY_AVAILABLE else None
        )
        
        # Import language detection
        self.detect_language = detect if LANGDETECT_AVAILABLE else lambda text: 'es'
        
        # User agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36 OPR/80.0.4170.63',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 (Telcel/Mexico)',
        ]
        
        # Encoding fallbacks
        self.encoding_fallbacks = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'windows-1252', 'iso-8859-15']
        
        # Enhanced Spanish date patterns
        self.date_patterns = [
            # Formal date formats
            r'(\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4})',
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{1,2}-\d{1,2}-\d{4})',
            r'(\d{4}/\d{1,2}/\d{1,2})',
            r'(\d{4}-\d{1,2}-\d{1,2})',
            
            # Date with time
            r'(\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4},?\s+\d{1,2}:\d{2})',
            r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2})',
            
            # Year references
            r'(?:en\sel\saño|durante|del\saño|año)\s+(\d{4})',
            r'(?:periodo|mandato|gestión)\s+(\d{4})[-\s]+(\d{4})',
            
            # Specific election/term references
            r'(?:(?:elecciones|votaciones|comicios).*?)(\d{4})',
            r'(?:elegido|electa|electo).*?(\d{4})',
            r'(?:ganó|triunfó|venció).*?(\d{4})',
            r'(?:candidato|candidata).*?(\d{4})',
            
            # Mexican-specific date formats
            r'(?:(?:Cd\.|Ciudad)\s+de\s+México,?\s+a\s+)?(\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4})',
            r'(?:trienio|mandato)\s+(\d{4})[-\s](\d{4})',
            r'(?:elecciones|votaciones|comicios)\s+(?:de|del)\s+(\d{4})',
            r'(?:campaña)\s+(\d{4})[-\s](\d{4})',
            
            # Additional period formats
            r'(?:del|de)\s+(\d{1,2})\s+(?:de|-)?\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de|del)?\s+(\d{4})\s+(?:al|a)\s+(\d{1,2})\s+(?:de|-)?\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de|del)?\s+(\d{4})',
            r'(?:del?)\s+(\d{4})\s+(?:al?)\s+(\d{4})',
            r'(\d{4})-(\d{4})',
            r'(\d{4})/(\d{4})'
        ]
        
        # HTTP session with retry capability
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def extract_content(self, url, candidate_name=None, target_year=None, year_range=2, use_oxylabs=True):
        """Extract content from a URL with multiple fallback methods and content classification"""
        # Extract domain for blacklist checking
        domain = self._extract_domain(url)
        
        # Check if domain is blacklisted
        if self.db.is_blacklisted(domain):
            return {
                'success': False,
                'title': '',
                'content': '',
                'html_content': '',
                'extracted_date': None,
                'language': None,
                'content_type': None,
                'content_type_confidence': 0.0,
                'quotes': [],
                'quote_count': 0,
                'error': f'Domain {domain} is blacklisted'
            }
        
        # Check cache first
        cached_content = self.db.get_cached_content(url)
        if cached_content:
            logger.info(f"Using cached content for {url}")
            
            # Classify content if candidate name is provided
            content_type_result = {'content_type': 'unknown', 'confidence': 0.0, 'quotes': [], 'quote_count': 0}
            if candidate_name and cached_content.get('content'):
                content_type_result = self.content_classifier.classify_content(
                    cached_content.get('content'), candidate_name
                )
            
            return {
                'success': True,
                'title': cached_content.get('title', ''),
                'content': cached_content.get('content', ''),
                'html_content': cached_content.get('html_content', ''),
                'extracted_date': cached_content.get('extracted_date'),
                'language': cached_content.get('language', 'es'),
                'content_type': content_type_result.get('content_type'),
                'content_type_confidence': content_type_result.get('confidence'),
                'quotes': content_type_result.get('quotes', []),
                'quote_count': content_type_result.get('quote_count', 0),
                'from_cache': True
            }
        
        # Select a random user agent
        user_agent = random.choice(self.user_agents)
        headers = {'User-Agent': user_agent}
        
        try:
            # Decide whether to use Oxylabs
            if use_oxylabs and self.oxylabs:
                logger.info(f"Using Oxylabs for content extraction: {url}")
                response = self.oxylabs.fetch_content(url, headers=headers, timeout=30)
                
                # Check if response is an error dictionary
                if isinstance(response, dict) and 'error' in response:
                    logger.warning(f"Oxylabs extraction failed: {response['error']}, falling back to direct request")
                    # Fall back to direct request
                    response = self.session.get(
                        url, 
                        headers=headers, 
                        timeout=30,
                        allow_redirects=True
                    )
            else:
                # Direct request
                response = self.session.get(
                    url, 
                    headers=headers, 
                    timeout=30,
                    allow_redirects=True
                )
            
            # Try to detect encoding correctly
            text_content = None
            
            # Auto-detect encoding with chardet if available
            try:
                import chardet
                detected = chardet.detect(response.content)
                if detected['confidence'] > 0.7:
                    response.encoding = detected['encoding']
                    text_content = response.text
            except ImportError:
                pass
                
            # If auto-detection failed, try all encoding fallbacks
            if text_content is None:
                for encoding in self.encoding_fallbacks:
                    try:
                        response.encoding = encoding
                        text_content = response.text
                        break
                    except UnicodeDecodeError:
                        continue
            
            # If all encodings fail, use apparent encoding
            if text_content is None:
                response.encoding = response.apparent_encoding
                text_content = response.text
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(text_content, 'html.parser')
            
            # Extract title
            title = soup.title.text.strip() if soup.title else ""
            
            # Look for publication date in meta tags
            extracted_date = self._extract_date_from_meta(soup)
            
            # Try multiple content extraction methods
            content = self._extract_content_with_methods(soup, url)
            
            # If no date found in meta, try to extract from content
            if not extracted_date:
                extracted_date = self._extract_date_from_text(content)
                
            # If still no date, try from title and URL
            if not extracted_date:
                extracted_date = self._extract_date_from_text(title)
                
            if not extracted_date:
                # Try to extract from the URL itself
                extracted_date = self._extract_date_from_text(url)
                
            # If no date found but target year is provided, check for implicit references
            # If no date found but target year is provided, check for implicit references
            if not extracted_date and target_year:
                year_str = str(target_year)
                
                # Look for target year and nearby years in content
                years_to_check = [str(y) for y in range(target_year - year_range, target_year + year_range + 1)]
                
                # Check if any year in our range is mentioned - prioritize exact target year
                if year_str in content or year_str in title:
                    extracted_date = f"{target_year}-01-01"
                elif any(year in content or year in title for year in years_to_check):
                    # Find which year is mentioned
                    for year in years_to_check:
                        if year in content or year in title:
                            extracted_date = f"{year}-01-01"
                            break
            
            # Detect language
            language = None
            try:
                # Try to detect language from content
                if len(content) > 50:
                    language = self.detect_language(content[:2000])
                elif title and len(title) > 10:
                    language = self.detect_language(title)
            except:
                # Default to Spanish
                language = 'es'
            
            # Classify content and extract quotes if candidate name is provided
            content_type_result = {'content_type': 'unknown', 'confidence': 0.0, 'quotes': [], 'quote_count': 0}
            if candidate_name and content:
                content_type_result = self.content_classifier.classify_content(content, candidate_name)
                
            # Clean the content
            clean_content = self.clean_text(content)
            
            # Cache the content
            self.db.cache_content(url, title, clean_content, extracted_date, text_content, language)
            
            # Update domain statistics
            self.db.update_domain_stats(domain, success=True, content_length=len(clean_content), is_spanish=(language == 'es'))
            
            return {
                'success': True,
                'title': title,
                'content': clean_content,
                'html_content': text_content,
                'extracted_date': extracted_date,
                'language': language,
                'content_type': content_type_result.get('content_type'),
                'content_type_confidence': content_type_result.get('confidence'),
                'quotes': content_type_result.get('quotes', []),
                'quote_count': content_type_result.get('quote_count', 0),
                'from_cache': False,
                'oxylabs_used': use_oxylabs and self.oxylabs is not None
            }
            
        except Exception as e:
            logger.warning(f"Content extraction error for {url}: {str(e)}")
            
            # Update domain statistics
            self.db.update_domain_stats(domain, success=False)
            
            # Try fallback extraction if not already using Oxylabs
            if not use_oxylabs and self.oxylabs:
                logger.info(f"Trying extraction with Oxylabs fallback for {url}")
                return self.extract_content(url, candidate_name, target_year, year_range, use_oxylabs=True)
            else:
                # Try ordinary fallback extraction
                try:
                    return self._extract_content_fallback(url, candidate_name, target_year)
                except Exception as fallback_error:
                    logger.warning(f"Fallback extraction failed for {url}: {str(fallback_error)}")
                    
                    return {
                        'success': False,
                        'title': '',
                        'content': '',
                        'html_content': '',
                        'extracted_date': None,
                        'language': None,
                        'content_type': None,
                        'content_type_confidence': 0.0,
                        'quotes': [],
                        'quote_count': 0,
                        'error': f'Extraction failed: {str(e)}'
                    }
    
    def _extract_content_fallback(self, url, candidate_name=None, target_year=None):
        """Fallback content extraction method with enhanced error handling"""
        domain = self._extract_domain(url)
        
        try:
            # Use a different User-Agent for the fallback
            user_agent = random.choice(self.user_agents)
            headers = {'User-Agent': user_agent}
            
            # Try to use Oxylabs if available
            if self.oxylabs:
                try:
                    response = self.oxylabs.fetch_content(url, headers=headers, timeout=45)
                    if isinstance(response, dict) and 'error' in response:
                        # Fallback to direct download
                        response = requests.get(
                            url, 
                            headers=headers, 
                            timeout=45,
                            allow_redirects=True
                        )
                except:
                    # Direct request
                    response = requests.get(
                        url, 
                        headers=headers, 
                        timeout=45,
                        allow_redirects=True
                    )
            else:
                # Direct request
                response = requests.get(
                    url, 
                    headers=headers, 
                    timeout=45,
                    allow_redirects=True
                )
            
            # Try different encodings
            html_content = ""
            for encoding in self.encoding_fallbacks:
                try:
                    response.encoding = encoding
                    html_content = response.text
                    break
                except:
                    continue
            
            # Simple parsing
            soup = BeautifulSoup(html_content, 'html.parser')
            
            title = soup.title.text.strip() if soup.title else ""
            
            # Get all text from the page
            content = soup.get_text(separator='\n\n', strip=True)
            
            # Clean the content
            content = self.clean_text(content)
            
            # Try to extract date
            extracted_date = self._extract_date_from_meta(soup)
            if not extracted_date:
                extracted_date = self._extract_date_from_text(content[:5000])
                
            # If no date and target year is provided, look for target year mentions
            if not extracted_date and target_year:
                if str(target_year) in content or str(target_year) in title:
                    extracted_date = f"{target_year}-01-01"
            
            # Detect language
            language = None
            try:
                # Try to detect language from content
                if len(content) > 50:
                    language = self.detect_language(content[:2000])
                elif title and len(title) > 10:
                    language = self.detect_language(title)
            except:
                # Default to Spanish
                language = 'es'
            
            # Classify content and extract quotes if candidate name is provided
            content_type_result = {'content_type': 'unknown', 'confidence': 0.0, 'quotes': [], 'quote_count': 0}
            if candidate_name and content:
                content_type_result = self.content_classifier.classify_content(content, candidate_name)
            
            # If we have some content, consider it a success
            if len(content) > 100:
                # Cache the content
                self.db.cache_content(url, title, content, extracted_date, html_content, language)
                
                # Update domain statistics
                self.db.update_domain_stats(domain, success=True, content_length=len(content), is_spanish=(language == 'es'))
                
                return {
                    'success': True,
                    'title': title,
                    'content': content,
                    'html_content': html_content,
                    'extracted_date': extracted_date,
                    'language': language,
                    'content_type': content_type_result.get('content_type'),
                    'content_type_confidence': content_type_result.get('confidence'),
                    'quotes': content_type_result.get('quotes', []),
                    'quote_count': content_type_result.get('quote_count', 0),
                    'from_cache': False
                }
            
            # Update domain statistics for failure
            self.db.update_domain_stats(domain, success=False)
            
            return {
                'success': False,
                'title': title,
                'content': '',
                'html_content': '',
                'extracted_date': None,
                'language': language,
                'content_type': None,
                'content_type_confidence': 0.0,
                'quotes': [],
                'quote_count': 0,
                'error': 'Insufficient content in fallback mode'
            }
            
        except Exception as e:
            # Update domain statistics for failure
            self.db.update_domain_stats(domain, success=False)
            
            return {
                'success': False,
                'title': '',
                'content': '',
                'html_content': '',
                'extracted_date': None,
                'language': None,
                'content_type': None,
                'content_type_confidence': 0.0,
                'quotes': [],
                'quote_count': 0,
                'error': f'Fallback extraction failed: {str(e)}'
            }
    
    def _extract_content_with_methods(self, soup, url):
        """Try multiple methods to extract content"""
        content = ""
        
        # Method 1: Extract article content
        article = soup.find('article')
        if article:
            content = article.get_text(separator='\n\n', strip=True)
            
        # Method 2: Look for main content container
        if not content or len(content) < 200:
            for container in ['main', 'div[class*="content"]', 'div[class*="article"]', 'div[id*="content"]', 'div[id*="article"]']:
                try:
                    main_content = soup.select_one(container)
                    if main_content:
                        extracted = main_content.get_text(separator='\n\n', strip=True)
                        if len(extracted) > len(content):
                            content = extracted
                except:
                    continue
        
        # Method 3: Extract all paragraphs
        if not content or len(content) < 200:
            paragraphs = []
            for p in soup.find_all('p'):
                try:
                    text = p.get_text(strip=True)
                    if len(text) > 20:  # Skip very short paragraphs
                        paragraphs.append(text)
                except:
                    continue
            if paragraphs:
                content = '\n\n'.join(paragraphs)
        
        # Method 4: Extract all div text as last resort
        if not content or len(content) < 200:
            try:
                for div in soup.find_all('div', class_=lambda c: c and any(x in str(c).lower() for x in ['content', 'article', 'text', 'body', 'entry', 'nota', 'noticia'])):
                    text = div.get_text(separator='\n\n', strip=True)
                    if len(text) > len(content):
                        content = text
            except:
                pass
        
        # Method 5: If still no good content, get the whole body
        if not content or len(content) < 200:
            try:
                body = soup.find('body')
                if body:
                    content = body.get_text(separator='\n\n', strip=True)
            except:
                pass
        
        return content
    
    def _extract_date_from_meta(self, soup):
        """Extract publication date from meta tags"""
        # Common meta tags for publication dates
        date_meta_names = [
            'article:published_time', 'datePublished', 'pubdate', 'date', 
            'DC.date.issued', 'article:modified_time', 'og:published_time',
            'publication-date', 'release_date', 'fecha', 'publication',
            'publish-date', 'lastmod', 'created', 'modified',
            'fecha-publicacion', 'fecha_publicacion', 'date-publication'
        ]
        
        for name in date_meta_names:
            # Try to find the meta tag
            try:
                meta_tag = soup.find('meta', {'property': name}) or soup.find('meta', {'name': name})
                if meta_tag and meta_tag.get('content'):
                    # Parse and format the date
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        parsed_date = dateutil.parser.parse(meta_tag['content'])
                        return parsed_date.strftime('%Y-%m-%d')
            except:
                continue
        
        # Try to find date in time tags
        try:
            time_tags = soup.find_all('time')
            for time_tag in time_tags:
                if time_tag.get('datetime'):
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            parsed_date = dateutil.parser.parse(time_tag['datetime'])
                            return parsed_date.strftime('%Y-%m-%d')
                    except:
                        continue
                elif time_tag.text:
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            parsed_date = dateparser.parse(time_tag.text, languages=['es'])
                            if parsed_date:
                                return parsed_date.strftime('%Y-%m-%d')
                    except:
                        continue
        except:
            pass
        
        return None
    
    def _extract_date_from_text(self, text):
        """Extract dates from text using regular expressions and date parsing"""
        if not text:
            return None
        
        # First try with regex patterns for Spanish dates
        for pattern in self.date_patterns:
            try:
                matches = re.findall(pattern, text)
                if matches:
                    for match in matches:
                        # If we found a period, use the first year
                        if isinstance(match, tuple) and len(match) > 1:
                            # Store all years found in the tuple
                            years = []
                            for item in match:
                                # Try to extract a year from the item
                                year_match = re.search(r'(\d{4})', item)
                                if year_match:
                                    years.append(int(year_match.group(1)))
                            
                            # If we found multiple years, use the earliest one
                            if years:
                                # First try to validate the years are reasonable
                                valid_years = [y for y in years if 1980 <= y <= datetime.now().year]
                                if valid_years:
                                    min_year = min(valid_years)
                                    return f"{min_year}-01-01"
                                continue  # No valid years, try next match
                            
                            match = match[0]  # Default to first item in tuple if no years found
                        
                        try:
                            # Try to parse the matched date string
                            with warnings.catch_warnings():
                                warnings.simplefilter("ignore")
                                parsed_date = dateparser.parse(match, languages=['es'])
                                if parsed_date and parsed_date.year > 1980 and parsed_date.year <= datetime.now().year:
                                    return parsed_date.strftime('%Y-%m-%d')
                        except:
                            continue
            except:
                continue
        
        # Look for standalone years in text
        try:
            year_pattern = r'\b(19[8-9]\d|20[0-2]\d)\b'  # Years 1980-2029
            year_matches = re.findall(year_pattern, text)
            if year_matches:
                # Get the most frequent year
                from collections import Counter
                year_counts = Counter(year_matches)
                most_common_year = year_counts.most_common(1)[0][0]
                return f"{most_common_year}-01-01"
        except:
            pass
                
        return None
    
    def _extract_domain(self, url):
        """Extract domain from URL"""
        try:
            domain = urlparse(url).netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ''
    
    def clean_text(self, text):
        """Clean extracted text"""
        if not text:
            return ""
            
        try:
            # Replace multiple newlines with a single one
            text = re.sub(r'\n\s*\n', '\n\n', text)
            
            # Remove excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            
            # Remove common boilerplate
            boilerplate = [
                'Cookies help us deliver our services',
                'Privacy Policy',
                'Terms of Service',
                'All Rights Reserved',
                'Copyright ©',
                'Derechos Reservados',
                'Política de Privacidad',
                'Términos y Condiciones',
                'Aviso Legal',
                'Aviso de Privacidad',
                'Todos los derechos reservados',
                'Queda prohibida la reproducción',
            ]
            
            for phrase in boilerplate:
                text = text.replace(phrase, '')
            
            return text.strip()
        except:
            # Return original text if cleaning fails
            return text


class MexicanEntityRecognizer:
    """Specialized entity recognizer for Mexican political entities with temporal context awareness"""
    
    def __init__(self, csv_data=None):
        """Initialize the Mexican entity recognizer"""
        self.csv_data = csv_data
        
        # Initialize NLP model if available
        self.nlp_model = NLP_MODEL if SPACY_AVAILABLE else None
        
        # Initialize fuzzy matching
        self.fuzzy_matching_available = FUZZYWUZZY_AVAILABLE
        
        # Initialize Mexican political entities knowledge base
        self.initialize_knowledge_base()
    
    def initialize_knowledge_base(self):
        """Initialize knowledge base with Mexican political entities"""
        # Mexican political parties
        self.political_parties = {
            "PRI": ["Partido Revolucionario Institucional", "PRI", "priista", "priístas"],
            "PAN": ["Partido Acción Nacional", "PAN", "panista", "panistas"],
            "PRD": ["Partido de la Revolución Democrática", "PRD", "perredista"],
            "MORENA": ["Movimiento Regeneración Nacional", "MORENA", "morenista"],
            "PT": ["Partido del Trabajo", "PT", "petista"],
            "PVEM": ["Partido Verde Ecologista de México", "PVEM", "Verde"],
            "MC": ["Movimiento Ciudadano", "MC"],
            "PANAL": ["Nueva Alianza", "PANAL"],
            "PES": ["Partido Encuentro Social", "PES"],
            "RSP": ["Redes Sociales Progresistas", "RSP"],
            "FXM": ["Fuerza por México", "FXM"]
        }
        
        # Mexican political positions - focus on candidate-related terms
        self.political_positions = [
            "candidato", "candidata", "candidato a la alcaldía", "candidata a la alcaldía",
            "aspirante", "precandidato", "precandidata", "contendiente", "postulante",
            "candidato a la presidencia municipal", "candidata a la presidencia municipal",
            "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal",
            "edil", "regidor", "regidora", "síndico", "síndica", "cabildo"
        ]
        
        # Mexican states
        self.mexican_states = {
            "AGU": "Aguascalientes",
            "BCN": "Baja California",
            "BCS": "Baja California Sur",
            "CAM": "Campeche",
            "CHP": "Chiapas",
            "CHH": "Chihuahua",
            "CDMX": "Ciudad de México",
            "DUR": "Durango",
            "GUA": "Guanajuato",
            "GRO": "Guerrero",
            "HID": "Hidalgo",
            "JAL": "Jalisco",
            "MEX": "Estado de México",
            "MIC": "Michoacán",
            "MOR": "Morelos",
            "NAY": "Nayarit",
            "NLE": "Nuevo León",
            "OAX": "Oaxaca",
            "PUE": "Puebla",
            "QUE": "Querétaro",
            "ROO": "Quintana Roo",
            "SLP": "San Luis Potosí",
            "SIN": "Sinaloa",
            "SON": "Sonora",
            "TAB": "Tabasco",
            "TAM": "Tamaulipas",
            "TLA": "Tlaxcala",
            "VER": "Veracruz",
            "YUC": "Yucatán",
            "ZAC": "Zacatecas"
        }
        
        # Election-related terms
        self.election_terms = [
            "elección", "elecciones", "electoral", "campaña", "voto", "votos", "votación",
            "urnas", "comicio", "comicios", "INE", "instituto electoral", "proceso electoral",
            "jornada electoral", "candidatura", "planilla", "triunfo electoral", "victoria electoral",
            "ganó la elección", "ganó las elecciones", "resultados electorales", "casilla", "casillas",
            "padrón electoral", "boleta", "boletas", "distrito electoral", "sección electoral"
        ]
        
        # Temporal/period-related terms
        self.temporal_terms = [
            "período", "periodo", "campaña electoral", "jornada electoral", "contienda electoral",
            "durante la campaña", "proceso electoral", "previo a la elección",
            "después de la elección", "antes de la elección", "tras la elección",
            "ciclo electoral", "proceso de selección de candidatos", "recta final de la campaña",
            "trienio", "gestión", "administración", "gobierno municipal", "ayuntamiento",
            "toma de posesión", "toma de protesta", "inicio de gobierno", "fin de gobierno"
        ]
        
        # Academic background terms
        self.academic_terms = [
            "licenciatura", "licenciado", "licenciada", "maestría", "doctorado", "posgrado",
            "ingeniero", "ingeniera", "abogado", "abogada", "médico", "médica", "doctor", "doctora",
            "título universitario", "estudios", "carrera", "profesión", "universidad", "UNAM", "IPN",
            "egresado", "egresada", "graduado", "graduada", "diplomado", "especialización",
            "bachillerato", "preparatoria", "técnico", "técnica", "formación académica",
            "educación superior", "facultad", "escuela", "titulación"
        ]
        
        # Professional experience terms
        self.professional_terms = [
            "experiencia profesional", "trayectoria profesional", "carrera profesional",
            "puesto", "cargo", "empleo", "empresario", "empresaria", "sector privado",
            "compañía", "empresa", "negocio", "corporativo", "gerente", "director", "directora",
            "coordinador", "coordinadora", "jefe", "jefa", "ejecutivo", "ejecutiva",
            "fundador", "fundadora", "consultor", "consultora", "asesor", "asesora",
            "industria", "comercio", "servicios", "despacho", "firma", "gestor", "gestora"
        ]
        
        # Public service terms
        self.public_service_terms = [
            "servidor público", "servidora pública", "funcionario público", "funcionaria pública",
            "servicio público", "administración pública", "gobierno federal", "gobierno estatal",
            "gobierno municipal", "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal",
            "diputado", "diputada", "senador", "senadora", "legislador", "legisladora",
            "regidor", "regidora", "síndico", "síndica", "concejal", "edil", "cabildo",
            "secretario", "secretaria", "director general", "directora general", "subsecretario", "subsecretaria",
            "delegado", "delegada", "procurador", "procuradora", "oficial mayor", "tesorero", "tesorera"
        ]
        
        # Build entity dictionaries from CSV data if available
        if self.csv_data is not None:
            self.candidate_names = set(self.csv_data['PRESIDENTE_MUNICIPAL'].str.strip().unique())
            self.municipalities = set(self.csv_data['MUNICIPIO'].str.strip().unique())
            
            # Create a mapping from candidates to municipalities for context
            self.candidate_to_municipality = {}
            for _, row in self.csv_data.iterrows():
                candidate = row['PRESIDENTE_MUNICIPAL'].strip()
                municipality = row['MUNICIPIO'].strip()
                
                if candidate not in self.candidate_to_municipality:
                    self.candidate_to_municipality[candidate] = set()
                
                self.candidate_to_municipality[candidate].add(municipality)
                
            # Create name variations for better matching
            self.name_variations = self._generate_name_variations()
        else:
            self.candidate_names = set()
            self.municipalities = set()
            self.candidate_to_municipality = {}
            self.name_variations = {}
    
    def _generate_name_variations(self):
        """Generate enhanced name variations with fuzzy matching support"""
        # Create enhanced variations dict
        variations = {}
        
        for name in self.candidate_names:
            # Add original name
            variations[name.lower()] = name
            
            # Skip names that are too short or None
            if not name or len(name) < 3:
                continue
            
            # Split full name
            name_parts = name.split()
            
            if len(name_parts) >= 2:
                # First name + last name
                first_name = name_parts[0]
                last_name = name_parts[-1]
                variations[f"{first_name} {last_name}".lower()] = name
                
                # Handle common Hispanic naming patterns
                if len(name_parts) >= 3:
                    # For names like "Juan Carlos Pérez López"
                    # First two names + last surname
                    first_two_names = " ".join(name_parts[:2])
                    variations[f"{first_two_names} {last_name}".lower()] = name
                    
                    # First name + both surnames
                    if len(name_parts) >= 4:
                        paternal_surname = name_parts[-2]
                        full_last_name = f"{paternal_surname} {last_name}"
                        variations[f"{first_name} {full_last_name}".lower()] = name
                
                # Handle compound surnames with prepositions
                prepositions = ["de", "del", "de la", "de los", "y"]
                for i in range(len(name_parts) - 1):
                    if name_parts[i].lower() in prepositions:
                        # Create variations without the preposition
                        parts_without_prep = name_parts.copy()
                        parts_without_prep.pop(i)
                        variations[" ".join(parts_without_prep).lower()] = name
                
                # Handle accented characters
                import unicodedata
                def remove_accents(text):
                    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
                
                normalized_name = remove_accents(name.lower())
                if normalized_name != name.lower():
                    variations[normalized_name] = name
                    
                # Add common honorifics and titles
                for title in ["lic.", "licenciado", "licenciada", "dr.", "doctor", "doctora", "ing.", "ingeniero", "ingeniera", "mtro.", "maestro", "maestra", "prof.", "profesor", "profesora"]:
                    variations[f"{title} {name}".lower()] = name
                    variations[f"{title} {first_name} {last_name}".lower()] = name
                
                # Add role-based variations
                for role in ["candidato", "candidata", "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal"]:
                    variations[f"{role} {name}".lower()] = name
        
        return variations
    
    def fuzzy_match_name(self, text, candidate_name, threshold=80):
        """Use fuzzy matching to find candidate name in text"""
        if not self.fuzzy_matching_available or not text or not candidate_name:
            return False, 0
        
        from fuzzywuzzy import fuzz, process
        
        text_lower = text.lower()
        candidate_lower = candidate_name.lower()
        
        # Direct match first (fastest)
        if candidate_lower in text_lower:
            return True, 100
        
        # Check name variations (also fast)
        for variation in self.name_variations:
            if variation in text_lower and self.name_variations[variation].lower() == candidate_lower:
                return True, 95
        
        # Try fuzzy matching for more complex cases
        # Break text into potential name chunks (3-5 word sequences)
        words = text_lower.split()
        chunks = []
        
        for i in range(len(words)):
            for j in range(3, 6):  # Check chunks of 3-5 words
                if i + j <= len(words):
                    chunk = " ".join(words[i:i+j])
                    chunks.append(chunk)
        
        # Get best match and score
        best_match = None
        best_score = 0
        
        # Check each chunk against candidate name
        for chunk in chunks:
            score = fuzz.token_sort_ratio(chunk, candidate_lower)
            if score > best_score:
                best_score = score
                best_match = chunk
        
        if best_score >= threshold:
            return True, best_score
        
        return False, 0
    
    def extract_entities(self, text, candidate_name=None, municipality=None):
        """Extract Mexican political entities from text"""
        if not text:
            return {}
        
        # Initialize results dictionary
        entities = {
            'PERSON': {},           # Detected persons
            'POLITICAL_PARTY': {},  # Political parties
            'POSITION': {},         # Political positions
            'LOCATION': {},         # Locations including municipalities
            'DATE': {},             # Dates mentioned
            'ELECTION_TERM': {},    # Election-related terms
            'TEMPORAL_TERM': {},    # Period/administration related terms
            'ACADEMIC': {},         # Academic background
            'PROFESSIONAL': {},     # Professional experience
            'PUBLIC_SERVICE': {}    # Public service experience
        }
        
        # Use spaCy if available
        if self.nlp_model:
            try:
                doc = self.nlp_model(text[:20000])  # Process first 20k chars to avoid memory issues
                
                # Extract entities detected by spaCy
                for ent in doc.ents:
                    if ent.label_ == 'PER':
                        entities['PERSON'][ent.text] = 1.0  # Base score
                    elif ent.label_ in ['LOC', 'GPE']:
                        entities['LOCATION'][ent.text] = 1.0  # Base score
                    elif ent.label_ == 'DATE':
                        entities['DATE'][ent.text] = 1.0  # Base score
            except Exception as e:
                logger.warning(f"Error in spaCy entity extraction: {str(e)}")
        
        # Find political parties
        for party_key, party_terms in self.political_parties.items():
            for term in party_terms:
                if term.lower() in text.lower():
                    # If party is found, add it with a score
                    entities['POLITICAL_PARTY'][party_key] = 1.0
        
        # Find political positions
        for position in self.political_positions:
            if position.lower() in text.lower():
                entities['POSITION'][position] = 1.0
        
        # Find election terms
        for term in self.election_terms:
            if term.lower() in text.lower():
                entities['ELECTION_TERM'][term] = 1.0
                
        # Find temporal/period terms
        for term in self.temporal_terms:
            if term.lower() in text.lower():
                entities['TEMPORAL_TERM'][term] = 1.0
                
        # Find academic terms
        for term in self.academic_terms:
            if term.lower() in text.lower():
                entities['ACADEMIC'][term] = 1.0
                
        # Find professional experience terms
        for term in self.professional_terms:
            if term.lower() in text.lower():
                entities['PROFESSIONAL'][term] = 1.0
                
        # Find public service terms
        for term in self.public_service_terms:
            if term.lower() in text.lower():
                entities['PUBLIC_SERVICE'][term] = 1.0
        
        # Check for specific candidate name
        if candidate_name:
            # Look for candidate name in text (with name variations)
            base_name = candidate_name.lower()
            
            # Check for exact name
            if base_name in text.lower():
                entities['PERSON'][candidate_name] = 2.0  # Higher score for target candidate
            
            # Check for name variations
            for variation, original_name in self.name_variations.items():
                if original_name.lower() == base_name and variation in text.lower():
                    entities['PERSON'][candidate_name] = 2.0  # Full match score
                    
            # Try fuzzy matching if available
            if self.fuzzy_matching_available:
                found, score = self.fuzzy_match_name(text, candidate_name)
                if found and score > 80:
                    # Convert score to 0-1 scale
                    entities['PERSON'][candidate_name] = max(entities['PERSON'].get(candidate_name, 0), score / 100 * 2)
        
        # Check for specific municipality
        if municipality:
            mun_lower = municipality.lower()
            if mun_lower in text.lower():
                entities['LOCATION'][municipality] = 2.0  # Higher score for target municipality
        
        return entities
    
    def calculate_temporal_relevance(self, content, extracted_date, target_year, year_range=2, period_original=None):
        """Calculate the temporal relevance with enhanced logic for election year ±2 bounds"""
        if not target_year:
            return 0.5, None, None  # Neutral score if no target year
        
        # Initialize year bounds
        year_lower_bound = target_year - year_range if year_range else target_year
        year_upper_bound = target_year + year_range if year_range else target_year
        
        # Parse original period if available
        if period_original:
            try:
                # Try to extract years from common Mexican period formats
                period_years = re.findall(r'(\d{4})', period_original)
                if len(period_years) >= 2:
                    # Update bounds from period if available
                    parsed_years = [int(y) for y in period_years]
                    period_start = min(parsed_years)
                    period_end = max(parsed_years)
                    
                    # Use the period bounds if they're more specific than our defaults
                    if period_start <= target_year <= period_end:
                        year_lower_bound = period_start
                        year_upper_bound = period_end
            except:
                pass
        
        # Convert target_year to string for comparisons
        target_year_str = str(target_year)
        years_in_range = [str(y) for y in range(year_lower_bound, year_upper_bound + 1)]
        
        # Start with a base score
        score = 0.5
        
        # If we have an extracted date, calculate based on proximity
        if extracted_date:
            try:
                content_year = int(extracted_date.split('-')[0])
                
                # Check if the content is within our target range
                if year_lower_bound <= content_year <= year_upper_bound:
                    # Calculate relevance based on proximity to target year
                    year_diff = abs(content_year - target_year)
                    if year_diff == 0:
                        score = 1.0  # Exact year match
                    elif year_diff == 1:
                        score = 0.9  # Off by 1 year
                    elif year_diff == 2:
                        score = 0.8  # Off by 2 years
                    else:
                        score = 0.7  # Within extended range
                elif content_year < year_lower_bound:
                    # Pre-election content
                    years_before = year_lower_bound - content_year
                    if years_before <= 1:
                        score = 0.6  # Just before our range
                    elif years_before <= 3:
                        score = 0.4  # Somewhat before our range
                    else:
                        score = 0.2  # Well before our range
                else:  # content_year > year_upper_bound
                    # Post-election content
                    years_after = content_year - year_upper_bound
                    if years_after <= 1:
                        score = 0.7  # Just after our range
                    elif years_after <= 3:
                        score = 0.5  # Somewhat after our range
                    else:
                        score = 0.3  # Well after our range
            except:
                # If we can't parse the year, look for year mentions in content
                if target_year_str in content:
                    score = 0.8  # Target year is mentioned in content
                elif any(year in content for year in years_in_range):
                    score = 0.7  # Any year in our range is mentioned
        else:
            # No extracted date, check for year mentions
            # Prioritize explicit mentions of the target year
            if target_year_str in content:
                score = 0.8
                
                # Check for election-specific phrases with the target year
                election_year_phrases = [
                    f"elecciones de {target_year_str}", 
                    f"elección de {target_year_str}",
                    f"comicios de {target_year_str}", 
                    f"votación de {target_year_str}",
                    f"campaña de {target_year_str}",
                    f"candidato en {target_year_str}",
                    f"candidata en {target_year_str}"
                ]
                
                for phrase in election_year_phrases:
                    if re.search(phrase, content, re.IGNORECASE):
                        score += 0.1  # Increase score for election context
                        if score >= 0.95:
                            score = 1.0
                            break
                            
            # Check for mentions of years in our target range
            elif any(year in content for year in years_in_range):
                score = 0.7
                
                # Find which years in our range are mentioned
                mentioned_years = [year for year in years_in_range if year in content]
                
                # If multiple years in range are mentioned, likely more relevant
                if len(mentioned_years) > 1:
                    score += 0.1
                    
                # Check for period references
                period_patterns = [
                    r'(\d{4})-(\d{4})',
                    r'(\d{4})/(\d{4})',
                    r'de (\d{4}) a (\d{4})',
                    r'del (\d{4}) al (\d{4})'
                ]
                
                for pattern in period_patterns:
                    periods = re.findall(pattern, content)
                    for period_match in periods:
                        try:
                            period_start = int(period_match[0])
                            period_end = int(period_match[1])
                            # If our target year falls within this period, increase relevance
                            if period_start <= target_year <= period_end:
                                score += 0.1
                                break
                        except:
                            continue
            
            # Check for explicit electoral terms
            electoral_terms = [
                "elección municipal", "elecciones municipales",
                "candidato", "candidata", "candidatos", "candidatas",
                "candidato a la presidencia municipal", "candidata a la presidencia municipal",
                "ganó la elección", "ganó las elecciones",
                "campaña electoral", "proceso electoral", "jornada electoral"
            ]
            
            if any(term.lower() in content.lower() for term in electoral_terms):
                score += 0.1  # Slight boost for electoral context
        
        return score, year_lower_bound, year_upper_bound
        
    def calculate_biographical_score(self, text, candidate_name):
        """Calculate how biographical the content is for a specific candidate"""
        if not text or not candidate_name:
            return 0.0
        
        score = 0.0
        text_lower = text.lower()
        
        # Check for candidate name
        if candidate_name.lower() in text_lower:
            score += 0.4
        else:
            # Check for name variations
            for variation, original_name in self.name_variations.items():
                if original_name.lower() == candidate_name.lower() and variation in text_lower:
                    score += 0.4
                    break
            
            # Try fuzzy matching if available and not found with exact matches
            if score == 0 and self.fuzzy_matching_available:
                found, match_score = self.fuzzy_match_name(text, candidate_name)
                if found:
                    score += 0.3 * (match_score / 100)
        
        # Look for biographical indicators
        biographical_terms = [
            "nació", "nacido en", "originario de", "oriundo de", "natal de",
            "edad", "familia", "casado", "casada", "hijo", "hija", "padre", "madre",
            "estudió", "educación", "formación", "se graduó", "egresado", "egresada",
            "profesión", "trayectoria", "carrera", "experiencia", "biografía", "perfil",
            "currículum", "cv", "licenciado", "licenciada", "doctor", "doctora", 
            "vida personal", "orígenes", "infancia", "antecedentes", "formación académica"
        ]
        
        for term in biographical_terms:
            if term in text_lower:
                score += 0.1
                if score >= 0.8:  # Cap at 0.8 for terms
                    break
        
        # Check for patterns of biographical information
        bio_patterns = [
            r'nació\s+en\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s,]+(?:en|el)\s+\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4}',
            r'(?:cursó|estudió)\s+(?:la|el)\s+(?:licenciatura|maestría|doctorado|carrera)\s+(?:en|de)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+\s+(?:en|por)\s+(?:la|el)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+',
            r'(?:es|tiene)\s+(?:licenciado|licenciada|ingeniero|ingeniera|doctor|doctora|maestro|maestra|profesor|profesora)\s+(?:en|de)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+\s+por\s+(?:la|el)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+'
        ]
        
        for pattern in bio_patterns:
            if re.search(pattern, text_lower):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_political_score(self, text, candidate_name=None, municipality=None):
        """Calculate how political/electoral the content is"""
        if not text:
            return 0.0
        
        score = 0.0
        text_lower = text.lower()
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name, municipality)
        
        # Check for political parties
        if entities['POLITICAL_PARTY']:
            score += 0.3
        
        # Check for political positions
        if entities['POSITION']:
            score += 0.3
        
        # Check for election terms
        if entities['ELECTION_TERM']:
            score += 0.2
        
        # Check for temporal/administrative terms
        if entities['TEMPORAL_TERM']:
            score += 0.2
        
        # Check for municipality mention
        if municipality and municipality.lower() in text_lower:
            score += 0.2
        
        # Check for election-specific patterns
        election_patterns = [
            r'gan(?:ó|a|ar|aron)\s+(?:la|las)\s+elecci(?:ón|ones)',
            r'candida(?:to|ta)\s+(?:a|por|del|de la|de)\s+(?:la\s+)?(?:alcaldía|presidencia\s+municipal|municipio)',
            r'compa(?:ñó|ña|ñar|ñaron)\s+(?:en|durante)\s+(?:la|su)\s+campaña',
            r'result(?:ó|a|ar|aron)\s+elect(?:o|a|os|as)',
            r'triun(?:fó|fa|far|faron)\s+en\s+(?:la|las)\s+(?:urnas|elecci(?:ón|ones)|votaci(?:ón|ones))'
        ]
        
        for pattern in election_patterns:
            if re.search(pattern, text_lower):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_academic_score(self, text, candidate_name=None):
        """Calculate how academic/educational the content is"""
        if not text:
            return 0.0
        
        score = 0.0
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name)
        
        # Check for academic terms
        if entities['ACADEMIC']:
            score += min(0.6, 0.1 * len(entities['ACADEMIC']))
        
        # Check for university mentions
        universities = [
            "unam", "universidad nacional autónoma de méxico",
            "ipn", "instituto politécnico nacional",
            "uam", "universidad autónoma metropolitana",
            "itesm", "tecnológico de monterrey",
            "universidad iberoamericana", "universidad anáhuac",
            "colmex", "colegio de méxico",
            "itam", "instituto tecnológico autónomo de méxico",
            "universidad veracruzana", "universidad de guadalajara",
            "buap", "benemérita universidad autónoma de puebla",
            "universidad autónoma de nuevo león", "uanl"
        ]
        
        for uni in universities:
            if uni in text.lower():
                score += 0.1
                if score >= 0.8:
                    break
        
        # Check for academic degree patterns
        academic_patterns = [
            r'(?:obtuvo|recibió|cuenta con)\s+(?:el|la|un|una)\s+(?:título|grado|licenciatura|maestría|doctorado)',
            r'(?:es|está)\s+(?:graduado|graduada|titulado|titulada)\s+(?:como|en|de)',
            r'(?:realizó|cursó|estudió)\s+(?:sus\s+estudios|la\s+carrera)',
            r'(?:licenciado|licenciada|ingeniero|ingeniera|abogado|abogada|doctor|doctora)\s+(?:en|por)\s+la'
        ]
        
        for pattern in academic_patterns:
            if re.search(pattern, text.lower()):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_professional_score(self, text, candidate_name=None):
        """Calculate how professional (work experience) the content is"""
        if not text:
            return 0.0
        
        score = 0.0
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name)
        
        # Check for professional terms
        if entities['PROFESSIONAL']:
            score += min(0.6, 0.1 * len(entities['PROFESSIONAL']))
        
        # Check for job titles
        job_titles = [
            "director", "directora", "gerente", "presidente", "presidenta", 
            "jefe", "jefa", "CEO", "coordinador", "coordinadora",
            "supervisor", "supervisora", "ejecutivo", "ejecutiva",
            "administrador", "administradora", "empresario", "empresaria",
            "consultor", "consultora", "asesor", "asesora", "titular",
            "fundador", "fundadora", "socio", "socia", "propietario", "propietaria"
        ]
        
        for title in job_titles:
            if title in text.lower():
                score += 0.1
                if score >= 0.8:
                    break
        
        # Check for professional experience patterns
        professional_patterns = [
            r'(?:ha|había)\s+(?:trabajado|laborado|colaborado|fungido)\s+como',
            r'(?:su|con)\s+(?:experiencia|trayectoria)\s+(?:en|como|dentro de)',
            r'(?:fundó|dirige|administra|preside|fundador de|director de)',
            r'(?:cuenta con|posee|tiene)\s+(?:una|amplia|extensa|sólida)\s+(?:experiencia|trayectoria)'
        ]
        
        for pattern in professional_patterns:
            if re.search(pattern, text.lower()):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    def calculate_public_service_score(self, text, candidate_name=None):
        """Calculate how much public service experience is mentioned"""
        if not text:
            return 0.0
        
        score = 0.0
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name)
        
        # Check for public service terms
        if entities['PUBLIC_SERVICE']:
            score += min(0.6, 0.1 * len(entities['PUBLIC_SERVICE']))
        
        # Check for government roles
        government_roles = [
            "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal",
            "regidor", "regidora", "síndico", "síndica", "diputado", "diputada",
            "senador", "senadora", "secretario de gobierno", "secretaria de gobierno",
            "funcionario", "funcionaria", "servidor público", "servidora pública",
            "delegado", "delegada", "gobernador", "gobernadora", "director general",
            "secretario de estado", "secretaria de estado", "subsecretario", "subsecretaria"
        ]
        
        for role in government_roles:
            if role in text.lower():
                score += 0.1
                if score >= 0.8:
                    break
                    
        # Check for government institutions
        institutions = [
            "ayuntamiento", "municipio", "gobierno municipal", "gobierno estatal", 
            "gobierno federal", "secretaría", "dependencia", "instituto",
            "cabildo", "palacio municipal", "congreso", "senado",
            "cámara de diputados", "delegación", "administración pública",
            "poder ejecutivo", "poder legislativo", "poder judicial"
        ]
        
        for institution in institutions:
            if institution in text.lower():
                score += 0.1
                if score >= 0.9:
                    break
        
        # Check for public service patterns
        public_service_patterns = [
            r'(?:ocupó|desempeñó|fungió como|ejerció)\s+(?:el cargo|la función|el puesto)\s+de',
            r'(?:se desempeñó|ha servido|ha trabajado|trabajó)\s+(?:en|dentro de|para)\s+(?:el|la|los|las)\s+(?:gobierno|administración|ayuntamiento|secretaría|instituto)',
            r'(?:formó parte|ha sido parte|perteneció|perteneció al|miembro de|integrante de)\s+(?:del|de la|el|la)\s+(?:administración|gobierno|cabildo|ayuntamiento|equipo de gobierno)'
        ]
        
        for pattern in public_service_patterns:
            if re.search(pattern, text.lower()):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_content_relevance(self, content, candidate_name, municipality=None):
        """Calculate overall content relevance combining various indicators"""
        if not content or not candidate_name:
            return 0.0
            
        # Calculate individual scores
        bio_score = self.calculate_biographical_score(content, candidate_name)
        political_score = self.calculate_political_score(content, candidate_name, municipality)
        academic_score = self.calculate_academic_score(content, candidate_name)
        professional_score = self.calculate_professional_score(content, candidate_name)
        public_service_score = self.calculate_public_service_score(content, candidate_name)
        
        # Candidate name match score
        name_match_score = 0.0
        if candidate_name.lower() in content.lower():
            name_match_score = 1.0
        else:
            # Check for name variations
            for variation, original_name in self.name_variations.items():
                if original_name.lower() == candidate_name.lower() and variation in content.lower():
                    name_match_score = 0.9
                    break
                    
            # Try fuzzy matching if still no match
            if name_match_score == 0.0 and self.fuzzy_matching_available:
                found, score = self.fuzzy_match_name(content, candidate_name)
                if found:
                    name_match_score = score / 100
        
        # Municipality mention boost
        municipality_boost = 0.1 if municipality and municipality.lower() in content.lower() else 0.0
        
        # Combine scores with appropriate weighting
        total_score = (
            (name_match_score * 0.25) +  # Name match is very important
            (bio_score * 0.2) +           # Biographical relevance
            (political_score * 0.25) +     # Political context
            (academic_score * 0.1) +       # Academic background
            (professional_score * 0.1) +   # Professional experience
            (public_service_score * 0.1) + # Public service
            municipality_boost             # Bonus for municipality mention
        )
        
        # Ensure score is capped at 1.0
        return min(1.0, total_score)


class SearchEngine:
    """Advanced search engine for finding candidate information with content classification"""
    
    def __init__(self, db_manager, oxylabs_manager=None, content_classifier=None, entity_recognizer=None,
                year_range=2, max_results=50, min_relevance=0.3):
        """Initialize the search engine with enhanced capabilities"""
        self.db = db_manager
        self.oxylabs = oxylabs_manager
        self.year_range = year_range
        self.max_results = max_results
        self.min_relevance = min_relevance
        
        # Create content extractor
        self.content_extractor = ContentExtractor(self.db, self.oxylabs)
        
        # Set content classifier
        self.content_classifier = content_classifier or self.content_extractor.content_classifier
        
        # Set entity recognizer
        self.entity_recognizer = entity_recognizer or MexicanEntityRecognizer()
    
    def build_search_query(self, candidate_name, municipality, target_year, include_party=False, party=None):
        """Build a search query with enhanced keywords for best results"""
        # Base query with candidate and municipality
        query = f'"{candidate_name}" "{municipality}"'
        
        # Add election year for temporal context
        year_str = str(target_year)
        query += f' {year_str}'
        
        # Add political context
        query += ' "candidato municipal" OR "elección municipal" OR "presidente municipal"'
        
        # Add political party if available
        if include_party and party:
            query += f' "{party}"'
        
        # Add specific Mexican electoral terms
        mexican_electoral_terms = [
            "INE",
            "candidatura",
            "alcaldía",
            "ayuntamiento",
            "cabildo",
            "planilla",
            "campaña",
            "votación",
            "elecciones",
            "presidencia municipal",
            "jornada electoral"
        ]
        
        # Add some but not all terms to avoid overly complex queries
        selected_terms = random.sample(mexican_electoral_terms, min(3, len(mexican_electoral_terms)))
        for term in selected_terms:
            query += f' OR "{term}"'
        
        return query
    
    def _build_search_with_name_variation(self, candidate_name, municipality, target_year, party=None):
        """Build a search query with name variations"""
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
    
    def search_candidate(self, candidate_name, municipality, target_year, 
                        state=None, gender=None, party=None, period=None, batch_id=None):
        """Search for information about a specific candidate"""
        logger.info(f"Searching for: {candidate_name} in {municipality}, year {target_year}")
        
        # Update progress
        self.db.update_candidate_progress(
            candidate_name, municipality, target_year, 
            'SEARCHING', batch_id=batch_id
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
            # Extract domain
            domain = urlparse(result.get('url', '')).netloc
            if domain.startswith('www.'):
                domain = domain[4:]
                
            # Check if domain is blacklisted
            if not self.db.is_blacklisted(domain):
                filtered_results.append(result)
        
        # Process search results
        processed_count = 0
        saved_articles = []
        
        # Update candidate progress to EXTRACTING
        self.db.update_candidate_progress(
            candidate_name, municipality, target_year, 
            'EXTRACTING', batch_id=batch_id
        )
        
        for result in filtered_results[:self.max_results]:
            try:
                url = result.get('url', '')
                if not url:
                    continue
                
                # Extract content
                extraction_result = self.content_extractor.extract_content(
                    url, candidate_name=candidate_name, target_year=target_year, year_range=self.year_range
                )
                
                if not extraction_result['success'] or not extraction_result.get('content'):
                    continue
                
                # Calculate temporal relevance
                temporal_score, year_lower, year_upper = self.entity_recognizer.calculate_temporal_relevance(
                    extraction_result['content'],
                    extraction_result.get('extracted_date'),
                    target_year,
                    self.year_range,
                    period
                )
                
                # Skip content with very low temporal relevance
                if temporal_score < 0.2:
                    continue
                
                # Calculate content relevance
                content_score = self.entity_recognizer.calculate_content_relevance(
                    extraction_result['content'],
                    candidate_name,
                    municipality
                )
                
                # Skip content with very low content relevance
                if content_score < self.min_relevance:
                    continue
                
                # Extract biographical scores
                bio_score = self.entity_recognizer.calculate_biographical_score(
                    extraction_result['content'], candidate_name
                )
                
                # Extract political scores
                political_score = self.entity_recognizer.calculate_political_score(
                    extraction_result['content'], candidate_name, municipality
                )
                
                # Extract academic, professional & public service scores
                academic_score = self.entity_recognizer.calculate_academic_score(
                    extraction_result['content'], candidate_name
                )
                
                professional_score = self.entity_recognizer.calculate_professional_score(
                    extraction_result['content'], candidate_name
                )
                
                public_service_score = self.entity_recognizer.calculate_public_service_score(
                    extraction_result['content'], candidate_name
                )
                
                # Extract entities
                entities = self.entity_recognizer.extract_entities(
                    extraction_result['content'], candidate_name, municipality
                )
                
                # Calculate name match score
                name_match_score = 0.0
                if candidate_name.lower() in extraction_result['content'].lower():
                    name_match_score = 1.0
                else:
                    # Check for name variations
                    for variation, original_name in self.entity_recognizer.name_variations.items():
                        if original_name.lower() == candidate_name.lower() and variation in extraction_result['content'].lower():
                            name_match_score = 0.9
                            break
                            
                    # Try fuzzy matching
                    if name_match_score == 0 and self.entity_recognizer.fuzzy_matching_available:
                        found, score = self.entity_recognizer.fuzzy_match_name(
                            extraction_result['content'], candidate_name
                        )
                        if found:
                            name_match_score = score / 100
                
                # Calculate overall relevance
                # Weighted average of temporal and content relevance
                overall_relevance = (temporal_score * 0.5) + (content_score * 0.5)
                
                # Prepare article data
                article_data = {
                    'entidad': state or '',
                    'cve_entidad': None,  # Would come from a catalog
                    'municipio': municipality,
                    'cve_municipio': None,  # Would come from a catalog
                    'target_year': target_year,
                    'year_lower_bound': year_lower,
                    'year_upper_bound': year_upper,
                    'candidato': candidate_name,
                    'sexo': gender or '',
                    'partido': party or '',
                    'periodo_formato_original': period or '',
                    'url': url,
                    'source': urlparse(url).netloc,
                    'title': extraction_result.get('title', ''),
                    'snippet': result.get('snippet', ''),
                    'content': extraction_result.get('content', ''),
                    'html_content': extraction_result.get('html_content', ''),
                    'search_query': query,
                    'content_date': extraction_result.get('extracted_date'),
                    'date_confidence': 1.0 if extraction_result.get('extracted_date') else 0.5,
                    'year_relevance': 0.0,  # For backward compatibility
                    'temporal_relevance': temporal_score,
                    'content_relevance': content_score,
                    'overall_relevance': overall_relevance,
                    'language': extraction_result.get('language', 'es'),
                    'name_match_score': name_match_score,
                    'biographical_content_score': bio_score,
                    'political_content_score': political_score,
                    'academic_score': academic_score,
                    'professional_score': professional_score,
                    'public_service_score': public_service_score,
                    'entities': entities,
                    'content_type': extraction_result.get('content_type', 'unknown'),
                    'content_type_confidence': extraction_result.get('content_type_confidence', 0.0),
                    'quote_count': extraction_result.get('quote_count', 0),
                    'quotes': extraction_result.get('quotes', []),
                    'fuzzy_match_score': 0.0
                }
                
                # Add fuzzy match score if applicable
                if self.entity_recognizer.fuzzy_matching_available:
                    found, score = self.entity_recognizer.fuzzy_match_name(
                        extraction_result.get('content', ''), candidate_name
                    )
                    if found:
                        article_data['fuzzy_match_score'] = score / 100
                
                # Save article to database
                article_id = self.db.save_article(article_data, batch_id)
                
                if article_id:
                    # Save quotes if any
                    if 'quotes' in article_data and article_data['quotes']:
                        self.db.save_candidate_quotes(
                            article_id, candidate_name, article_data['quotes']
                        )
                    
                    saved_articles.append(article_id)
                    processed_count += 1
                
                # Sleep to avoid overwhelming servers
                time.sleep(random.uniform(0.5, 1.5))
                
            except Exception as e:
                logger.error(f"Error processing search result: {str(e)}")
                traceback.print_exc()
                # Continue with next result
        
        # Update progress as completed
        self.db.update_candidate_progress(
            candidate_name, municipality, target_year, 
            'COMPLETED', articles_found=len(saved_articles),
            batch_id=batch_id
        )
        
        return saved_articles
    
    def search_candidate_enhanced(self, candidate_name, municipality, target_year, 
                                state=None, gender=None, party=None, period=None, batch_id=None):
        """Enhanced search to get 10 most relevant links per candidate"""
        logger.info(f"Enhanced search for: {candidate_name} in {municipality}, year {target_year}")
        
        # Update progress
        self.db.update_candidate_progress(
            candidate_name, municipality, target_year, 
            'SEARCHING', batch_id=batch_id
        )
        
        # Build multiple search queries with different strategies
        queries = [
            # Strategy 1: Exact name and municipality
            self.build_search_query(candidate_name, municipality, target_year, 
                                include_party=True, party=party),
            
            # Strategy 2: Include "candidato" or "candidata" based on gender
            self.build_search_query(
                f"{'candidato' if gender != 'F' else 'candidata'} {candidate_name}",
                municipality, target_year, include_party=True, party=party
            ),
            
            # Strategy 3: Include "presidente municipal" or "presidenta municipal"
            self.build_search_query(
                f"{'presidente' if gender != 'F' else 'presidenta'} municipal {candidate_name}",
                municipality, target_year, include_party=True, party=party
            ),
            
            # Strategy 4: Try with different name variations
            self._build_search_with_name_variation(candidate_name, municipality, target_year, party)
        ]
        
        # For each query, store a mapping to extracted results
        all_results = []
        seen_urls = set()
        
        for query in queries:
            # Try to get cached results
            cached_results = self.db.get_cached_search(query)
            
            results = None
            if cached_results:
                logger.info(f"Using cached search results for query: {query[:50]}...")
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
            
            # Filter and merge results
            for result in results:
                url = result.get('url', '')
                if not url or url in seen_urls:
                    continue
                    
                # Extract domain for blacklist checking
                domain = urlparse(url).netloc
                if domain.startswith('www.'):
                    domain = domain[4:]
                    
                # Check if domain is blacklisted
                if not self.db.is_blacklisted(domain):
                    # Add query used to the result for tracking
                    result['search_query'] = query
                    all_results.append(result)
                    seen_urls.add(url)
        
        # Process the combined search results
        processed_results = []
        
        # Update candidate progress to EXTRACTING
        self.db.update_candidate_progress(
            candidate_name, municipality, target_year, 
            'EXTRACTING', batch_id=batch_id
        )
        
        # Process all results to calculate relevance
        for result in all_results:
            try:
                url = result.get('url', '')
                if not url:
                    continue
                
                # Extract content
                extraction_result = self.content_extractor.extract_content(
                    url, candidate_name=candidate_name, target_year=target_year, year_range=self.year_range
                )
                
                if not extraction_result['success'] or not extraction_result.get('content'):
                    continue
                
                # Calculate temporal relevance
                temporal_score, year_lower, year_upper = self.entity_recognizer.calculate_temporal_relevance(
                    extraction_result['content'],
                    extraction_result.get('extracted_date'),
                    target_year,
                    self.year_range,
                    period
                )
                
                # Skip content with very low temporal relevance
                if temporal_score < 0.2:
                    continue
                
                # Calculate content relevance
                content_score = self.entity_recognizer.calculate_content_relevance(
                    extraction_result['content'],
                    candidate_name,
                    municipality
                )
                
                # Skip content with very low content relevance
                if content_score < self.min_relevance:
                    continue
                
                # Get content type and quotes
                content_type_result = extraction_result.get('content_type', 'unknown')
                content_type_confidence = extraction_result.get('content_type_confidence', 0.0)
                quotes = extraction_result.get('quotes', [])
                
                # Calculate overall relevance with enhanced scoring
                # Weighted average of temporal and content relevance with bonuses
                overall_relevance = (temporal_score * 0.4) + (content_score * 0.4)
                
                # Bonus for content type
                if content_type_result == 'discourse':
                    # Prioritize content with candidate quotes
                    overall_relevance += 0.2 * content_type_confidence
                elif content_type_result == 'news':
                    # News articles are generally more factual
                    overall_relevance += 0.1 * content_type_confidence
                
                # Cap at 1.0
                overall_relevance = min(1.0, overall_relevance)
                
                # Calculate additional scores
                bio_score = self.entity_recognizer.calculate_biographical_score(
                    extraction_result['content'], candidate_name
                )
                
                political_score = self.entity_recognizer.calculate_political_score(
                    extraction_result['content'], candidate_name, municipality
                )
                
                academic_score = self.entity_recognizer.calculate_academic_score(
                    extraction_result['content'], candidate_name
                )
                
                professional_score = self.entity_recognizer.calculate_professional_score(
                    extraction_result['content'], candidate_name
                )
                
                public_service_score = self.entity_recognizer.calculate_public_service_score(
                    extraction_result['content'], candidate_name
                )
                
                # Extract entities
                entities = self.entity_recognizer.extract_entities(
                    extraction_result['content'], candidate_name, municipality
                )
                
                # Fuzzy match score
                fuzzy_match_score = 0.0
                if self.entity_recognizer.fuzzy_matching_available:
                    found, score = self.entity_recognizer.fuzzy_match_name(
                        extraction_result['content'], candidate_name
                    )
                    if found:
                        fuzzy_match_score = score / 100
                
                # Prepare article data
                article_data = {
                    'entidad': state or '',
                    'cve_entidad': None,
                    'municipio': municipality,
                    'cve_municipio': None,
                    'target_year': target_year,
                    'year_lower_bound': year_lower,
                    'year_upper_bound': year_upper,
                    'candidato': candidate_name,
                    'sexo': gender or '',
                    'partido': party or '',
                    'periodo_formato_original': period or '',
                    'url': url,
                    'source': urlparse(url).netloc,
                    'title': extraction_result.get('title', ''),
                    'snippet': result.get('snippet', ''),
                    'content': extraction_result.get('content', ''),
                    'html_content': extraction_result.get('html_content', ''),
                    'search_query': result.get('search_query', ''),
                    'content_date': extraction_result.get('extracted_date'),
                    'date_confidence': 1.0 if extraction_result.get('extracted_date') else 0.5,
                    'temporal_relevance': temporal_score,
                    'content_relevance': content_score,
                    'overall_relevance': overall_relevance,
                    'language': extraction_result.get('language', 'es'),
                    'content_type': content_type_result,
                    'content_type_confidence': content_type_confidence,
                    'quote_count': len(quotes),
                    'biographical_content_score': bio_score,
                    'political_content_score': political_score,
                    'academic_score': academic_score,
                    'professional_score': professional_score,
                    'public_service_score': public_service_score,
                    'fuzzy_match_score': fuzzy_match_score,
                    'quotes': quotes,
                    'entities': entities
                }
                
                # Calculate name match score
                if candidate_name.lower() in extraction_result['content'].lower():
                    article_data['name_match_score'] = 1.0
                else:
                    # Check for name variations
                    for variation, original_name in self.entity_recognizer.name_variations.items():
                        if original_name.lower() == candidate_name.lower() and variation in extraction_result['content'].lower():
                            article_data['name_match_score'] = 0.9
                            break
                    if 'name_match_score' not in article_data:
                        article_data['name_match_score'] = fuzzy_match_score
                
                processed_results.append(article_data)
                
                # Sleep to avoid overwhelming servers
                time.sleep(random.uniform(0.3, 0.7))
                
            except Exception as e:
                logger.error(f"Error processing search result: {str(e)}")
                traceback.print_exc()
                # Continue with next result
        
        # Sort by overall relevance and take top 10
        processed_results.sort(key=lambda x: x['overall_relevance'], reverse=True)
        top_results = processed_results[:10]
        # Save articles to database
        saved_articles = []
        
        for article_data in top_results:
            try:
                # Save article to database
                article_id = self.db.save_article(article_data, batch_id)
                
                if article_id:
                    # Save quotes if any
                    if 'quotes' in article_data and article_data['quotes']:
                        self.db.save_candidate_quotes(
                            article_id, candidate_name, article_data['quotes']
                        )
                    
                    saved_articles.append(article_id)
            
            except Exception as e:
                logger.error(f"Error saving processed result: {str(e)}")
                traceback.print_exc()
        
        # Update progress as completed
        self.db.update_candidate_progress(
            candidate_name, municipality, target_year, 
            'COMPLETED', articles_found=len(saved_articles),
            batch_id=batch_id
        )
        
        return saved_articles


class MexicanCandidateScraper:
    """Main class for the Mexican municipal candidate scraper"""
    
    def __init__(self, db_path='data/mexican_candidates.db', candidates_csv=None, 
                max_threads=5, year_range=2, use_oxylabs=True, enhanced_search=True):
        """Initialize the scraper"""
        self.db_path = db_path
        self.candidates_csv = candidates_csv
        self.max_threads = max_threads
        self.year_range = year_range
        self.use_oxylabs = use_oxylabs
        self.enhanced_search = enhanced_search
        
        # Initialize database manager
        self.db = DatabaseManager(db_path)
        
        # Initialize Oxylabs manager if enabled
        if self.use_oxylabs:
            self.oxylabs = OxylabsAPIManager(OXYLABS_USERNAME, OXYLABS_PASSWORD, country='mx')
        else:
            self.oxylabs = None
        
        # Load candidates data
        self.candidates_data = self._load_candidates()
        
        # Set up entity recognizer with candidates data
        self.entity_recognizer = MexicanEntityRecognizer(self.candidates_data)
        
        # Set up content classifier
        self.content_classifier = ContentClassifier(
            nlp_model=NLP_MODEL if SPACY_AVAILABLE else None
        )
        
        # Initialize search engine
        self.search_engine = SearchEngine(
            self.db, 
            self.oxylabs,
            content_classifier=self.content_classifier,
            entity_recognizer=self.entity_recognizer,
            year_range=self.year_range
        )
    
    def _load_candidates(self):
        """Load candidates data from CSV with enhanced error handling"""
        if self.candidates_csv:
            try:
                # Try to read with pandas
                df = pd.read_csv(self.candidates_csv, encoding='utf-8')
                logger.info(f"Loaded {len(df)} candidates from CSV using UTF-8 encoding")
                return df
            except UnicodeDecodeError:
                # Try different encodings if UTF-8 fails
                for encoding in ['latin1', 'iso-8859-1', 'cp1252']:
                    try:
                        df = pd.read_csv(self.candidates_csv, encoding=encoding)
                        logger.info(f"Loaded {len(df)} candidates from CSV using {encoding} encoding")
                        return df
                    except:
                        continue
                        
                logger.error(f"Failed to load candidates CSV with any encoding")
            except Exception as e:
                logger.error(f"Error loading candidates CSV: {str(e)}")
                traceback.print_exc()
        
        return None
    
    def process_candidate(self, candidate):
        """Process a single candidate with enhanced search capabilities"""
        try:
            candidate_name = candidate.get('PRESIDENTE_MUNICIPAL')
            municipality = candidate.get('MUNICIPIO')
            target_year = candidate.get('Year')
            state = candidate.get('ENTIDAD')
            gender = candidate.get('SEXO')
            party = candidate.get('PARTIDO')
            period = candidate.get('PERIODO_FORMATO_ORIGINAL')
            batch_id = candidate.get('batch_id')
            
            logger.info(f"Processing candidate: {candidate_name}, {municipality}, {target_year}")
            
            # Use enhanced search if enabled, otherwise use standard search
            if self.enhanced_search:
                results = self.search_engine.search_candidate_enhanced(
                    candidate_name, municipality, target_year,
                    state=state, gender=gender, party=party, period=period,
                    batch_id=batch_id
                )
            else:
                results = self.search_engine.search_candidate(
                    candidate_name, municipality, target_year,
                    state=state, gender=gender, party=party, period=period,
                    batch_id=batch_id
                )
            
            return len(results)
        
        except Exception as e:
            logger.error(f"Error processing candidate: {str(e)}")
            traceback.print_exc()
            return 0
    
    def run_batch(self, candidates=None, max_candidates=None):
        """Run a batch of candidate processing with enhanced progress tracking"""
        try:
            # Use provided candidates or all candidates
            if candidates is None:
                if self.candidates_data is None:
                    logger.error("No candidates data available")
                    return
                
                all_candidates = self.candidates_data.to_dict('records')
            else:
                all_candidates = candidates
                
            # Limit to max_candidates if specified
            if max_candidates and max_candidates > 0:
                candidates_to_process = all_candidates[:max_candidates]
            else:
                candidates_to_process = all_candidates
            
            # Filter out already completed candidates
            candidates_to_process = self.db.get_candidates_to_process(candidates_to_process)
            
            if not candidates_to_process:
                logger.info("No new candidates to process")
                return
                
            total_candidates = len(candidates_to_process)
            logger.info(f"Starting batch with {total_candidates} candidates")
            
            # Create a new batch
            batch_id = self.db.create_batch(total_candidates)
            
            # Add batch_id to each candidate
            for candidate in candidates_to_process:
                candidate['batch_id'] = batch_id
            
            # Process candidates concurrently
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                future_to_candidate = {
                    executor.submit(self.process_candidate, candidate): candidate 
                    for candidate in candidates_to_process
                }
                
                completed_count = 0
                for future in as_completed(future_to_candidate):
                    candidate = future_to_candidate[future]
                    candidate_name = candidate.get('PRESIDENTE_MUNICIPAL')
                    
                    try:
                        result_count = future.result()
                        logger.info(f"Completed {candidate_name} with {result_count} results")
                        completed_count += 1
                        
                        # Update batch progress
                        progress_percent = (completed_count / total_candidates) * 100
                        logger.info(f"Batch progress: {progress_percent:.1f}% ({completed_count}/{total_candidates})")
                        
                        # Update batch status periodically
                        if completed_count % 10 == 0 or completed_count == total_candidates:
                            self.db.update_batch_status(
                                batch_id, 'IN_PROGRESS', completed_candidates=completed_count
                            )
                        
                    except Exception as e:
                        logger.error(f"Error with candidate {candidate_name}: {str(e)}")
                        traceback.print_exc()
            
            # Mark batch as completed
            self.db.update_batch_status(batch_id, 'COMPLETED', completed_candidates=completed_count)
            logger.info(f"Batch {batch_id} completed")
            
            # Create candidate profiles
            logger.info("Creating candidate profiles...")
            self.db.create_candidate_profiles()
            
        except Exception as e:
            logger.error(f"Error running batch: {str(e)}")
            traceback.print_exc()
    
    def export_results(self, output_path='results', format='json', min_relevance=0.3):
        """Export results to CSV or JSON with enhanced fields"""
        try:
            os.makedirs(output_path, exist_ok=True)
            
            # Get all articles
            df = self.db.get_all_articles(min_relevance=min_relevance)
            
            if df.empty:
                logger.warning("No results to export")
                return
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if format.lower() == 'csv':
                # Export to CSV
                csv_path = os.path.join(output_path, f'mexican_candidates_{timestamp}.csv')
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"Exported {len(df)} results to {csv_path}")
                
            elif format.lower() == 'json':
                # Export to JSON
                json_path = os.path.join(output_path, f'mexican_candidates_{timestamp}.json')
                df.to_json(json_path, orient='records', force_ascii=False, indent=4)
                logger.info(f"Exported {len(df)} results to {json_path}")
                
            # Export profiles
            self._export_profiles(output_path, timestamp)
            
            # Export dataset for ML/NLP
            self.export_ml_dataset(output_path, timestamp, format)
                
        except Exception as e:
            logger.error(f"Error exporting results: {str(e)}")
            traceback.print_exc()
    
    def export_ml_dataset(self, output_path, timestamp, format='json'):
        """Export ML/NLP dataset"""
        try:
            dataset_path = os.path.join(output_path, f'ml_dataset_{timestamp}')
            os.makedirs(dataset_path, exist_ok=True)
            
            # Generate the dataset
            dataset = self.db.generate_ml_dataset(dataset_path, format)
            
            if dataset:
                logger.info(f"Exported ML dataset with {len(dataset)} candidates")
                return True
            else:
                logger.warning("Failed to generate ML dataset")
                return False
                
        except Exception as e:
            logger.error(f"Error exporting ML dataset: {str(e)}")
            traceback.print_exc()
            return False
    
    def _export_profiles(self, output_path, timestamp):
        """Export candidate profiles with enhanced fields"""
        try:
            # Get unique candidates
            if self.candidates_data is None:
                logger.warning("No candidates data for profile export")
                return
                
            # Get candidate names
            candidate_names = self.candidates_data['PRESIDENTE_MUNICIPAL'].unique()
            
            profiles = []
            for candidate_name in candidate_names[:100]:  # Limit to 100 for performance
                # Get municipalities for this candidate
                municipalities = self.candidates_data[
                    self.candidates_data['PRESIDENTE_MUNICIPAL'] == candidate_name
                ]['MUNICIPIO'].unique()
                
                for municipality in municipalities:
                    # Get years for this candidate in this municipality
                    years = self.candidates_data[
                        (self.candidates_data['PRESIDENTE_MUNICIPAL'] == candidate_name) &
                        (self.candidates_data['MUNICIPIO'] == municipality)
                    ]['Year'].unique()
                    
                    for year in years:
                        profile = self.db.get_candidate_profile(
                            candidate_name, municipality, year
                        )
                        
                        if profile:
                            profiles.append(profile)
            
            if profiles:
                # Save profiles to JSON
                profiles_path = os.path.join(output_path, f'candidate_profiles_{timestamp}.json')
                with open(profiles_path, 'w', encoding='utf-8') as f:
                    json.dump(profiles, f, ensure_ascii=False, indent=4)
                    
                logger.info(f"Exported {len(profiles)} candidate profiles to {profiles_path}")
                
                # Export quotes separately
                self._export_quotes(output_path, timestamp)
                
        except Exception as e:
            logger.error(f"Error exporting profiles: {str(e)}")
            traceback.print_exc()
    
    def _export_quotes(self, output_path, timestamp):
        """Export candidate quotes to a separate file"""
        try:
            conn = self.db.get_connection()
            
            # Get all quotes
            quotes_df = pd.read_sql_query(
                '''SELECT q.*, a.title, a.url, a.source, a.content_date, a.content_type,
                   a.overall_relevance, a.temporal_relevance, a.content_relevance
                   FROM candidate_quotes q
                   JOIN articles a ON q.article_id = a.id
                   ORDER BY q.candidato, q.extraction_confidence DESC''',
                conn
            )
            
            conn.close()
            
            if not quotes_df.empty:
                # Save quotes to CSV
                quotes_path = os.path.join(output_path, f'candidate_quotes_{timestamp}.csv')
                quotes_df.to_csv(quotes_path, index=False, encoding='utf-8-sig')
                
                logger.info(f"Exported {len(quotes_df)} candidate quotes to {quotes_path}")
                
        except Exception as e:
            logger.error(f"Error exporting quotes: {str(e)}")
            traceback.print_exc()


def parse_arguments():
    """Parse command line arguments with enhanced options"""
    parser = argparse.ArgumentParser(description='Enhanced Mexican Municipal Candidates Web Scraper')
    
    parser.add_argument('--csv', '-c', type=str, help='Path to candidates CSV file')
    parser.add_argument('--db', '-d', type=str, default='data/mexican_candidates.db', 
                      help='Path to SQLite database')
    parser.add_argument('--threads', '-t', type=int, default=5, 
                      help='Maximum number of concurrent threads')
    parser.add_argument('--year-range', '-y', type=int, default=2, 
                      help='Year range for temporal filtering (±)')
    parser.add_argument('--max-candidates', '-m', type=int, default=0, 
                      help='Maximum number of candidates to process (0 for all)')
    parser.add_argument('--export', '-e', action='store_true', 
                      help='Export results after processing')
    parser.add_argument('--export-format', '-f', type=str, default='json', 
                      choices=['csv', 'json'], help='Export format (csv or json)')
    parser.add_argument('--min-relevance', '-r', type=float, default=0.3, 
                      help='Minimum relevance threshold for exports')
    parser.add_argument('--no-oxylabs', action='store_true', 
                      help='Disable Oxylabs API')
    parser.add_argument('--enhanced-search', action='store_true', 
                      help='Use enhanced search strategies (default: False)')
    parser.add_argument('--create-dataset', action='store_true', 
                      help='Create a consolidated dataset for ML/NLP')
    parser.add_argument('--dataset-format', type=str, default='json', 
                      choices=['json', 'csv'], help='Dataset format')
    parser.add_argument('--create-profiles', action='store_true',
                      help='Create candidate profiles without running scraper')
    parser.add_argument('--output-path', type=str, default='results',
                      help='Path for exported files')
    
    return parser.parse_args()


def main():
    """Main function with enhanced functionality"""
    # Parse arguments
    args = parse_arguments()
    
    # Initialize scraper
    scraper = MexicanCandidateScraper(
        db_path=args.db,
        candidates_csv=args.csv,
        max_threads=args.threads,
        year_range=args.year_range,
        use_oxylabs=not args.no_oxylabs,
        enhanced_search=args.enhanced_search
    )
    
    # Create profiles only if requested
    if args.create_profiles:
        logger.info("Creating candidate profiles...")
        scraper.db.create_candidate_profiles(min_relevance=args.min_relevance)
    else:
        # Run batch processing
        scraper.run_batch(max_candidates=args.max_candidates)
    
    # Export results if requested
    if args.export:
        scraper.export_results(
            output_path=args.output_path,
            format=args.export_format,
            min_relevance=args.min_relevance
        )
    
    # Create ML dataset if requested
    if args.create_dataset:
        scraper.export_ml_dataset(
            args.output_path,
            datetime.now().strftime('%Y%m%d_%H%M%S'),
            args.dataset_format
        )
    
    logger.info("Scraping completed!")


if __name__ == "__main__":
    main()