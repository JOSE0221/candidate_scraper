"""
Database models and schema definitions for the Mexican Municipal Candidates Scraper.
"""
import sqlite3
import json
import hashlib
from datetime import datetime
import os
import sys
from pathlib import Path

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger

logger = get_logger(__name__)

class Schema:
    """Database schema definitions with an improved structure"""
    
    # SQL statements to create tables with proper relationships
    PRAGMAS = [
        'PRAGMA journal_mode = WAL',  # Write-ahead logging for better concurrency
        'PRAGMA synchronous = NORMAL', # Balance between durability and performance
        'PRAGMA cache_size = -32000',  # 32MB cache
        'PRAGMA foreign_keys = ON',    # Enable foreign key constraints
    ]
    
    TABLES = {
        # Caching tables
        'search_cache': '''
            CREATE TABLE IF NOT EXISTS search_cache (
                query_hash TEXT PRIMARY KEY,
                query TEXT NOT NULL,
                response TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        ''',
        
        'content_cache': '''
            CREATE TABLE IF NOT EXISTS content_cache (
                url_hash TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT,
                content TEXT,
                extracted_date TEXT,
                html_content TEXT,
                timestamp TEXT NOT NULL,
                language TEXT,
                content_length INTEGER
            )
        ''',
        
        # Domain tables
        'domain_stats': '''
            CREATE TABLE IF NOT EXISTS domain_stats (
                domain TEXT PRIMARY KEY,
                success_count INTEGER DEFAULT 0,
                failure_count INTEGER DEFAULT 0,
                avg_content_length REAL DEFAULT 0,
                last_updated TEXT,
                is_spanish BOOLEAN DEFAULT 1
            )
        ''',
        
        'domain_blacklist': '''
            CREATE TABLE IF NOT EXISTS domain_blacklist (
                domain TEXT PRIMARY KEY,
                reason TEXT,
                added_date TEXT
            )
        ''',
        
        # Core tables
        'candidates': '''
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                municipality TEXT NOT NULL,
                entidad TEXT,
                cve_entidad INTEGER,
                cve_municipio INTEGER,
                target_year INTEGER NOT NULL,
                gender TEXT,
                party TEXT,
                period_format TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(name, municipality, target_year)
            )
        ''',
        
        'articles': '''
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                snippet TEXT,
                content TEXT,
                html_content TEXT,
                extracted_date TEXT,
                content_date TEXT,
                source TEXT,
                language TEXT,
                content_type TEXT,
                content_type_confidence REAL DEFAULT 0.0,
                date_confidence REAL DEFAULT 0.0,
                temporal_relevance REAL DEFAULT 0.0,
                content_relevance REAL DEFAULT 0.0,
                overall_relevance REAL DEFAULT 0.0,
                year_lower_bound INTEGER,
                year_upper_bound INTEGER,
                extraction_date TEXT NOT NULL,
                search_query TEXT,
                quote_count INTEGER DEFAULT 0,
                processed BOOLEAN DEFAULT 0,
                batch_id INTEGER,
                
                FOREIGN KEY (batch_id) REFERENCES scraping_batches(id)
            )
        ''',
        
        # Link table for many-to-many relationship between candidates and articles
        'candidate_articles': '''
            CREATE TABLE IF NOT EXISTS candidate_articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER NOT NULL,
                article_id INTEGER NOT NULL,
                name_match_score REAL DEFAULT 0.0,
                fuzzy_match_score REAL DEFAULT 0.0,
                biographical_score REAL DEFAULT 0.0,
                political_score REAL DEFAULT 0.0,
                academic_score REAL DEFAULT 0.0,
                professional_score REAL DEFAULT 0.0,
                public_service_score REAL DEFAULT 0.0,
                relevance REAL DEFAULT 0.0,
                
                FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                UNIQUE(candidate_id, article_id)
            )
        ''',
        
        'quotes': '''
            CREATE TABLE IF NOT EXISTS quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                candidate_id INTEGER NOT NULL,
                quote_text TEXT NOT NULL,
                quote_context TEXT,
                context_start INTEGER,
                context_end INTEGER,
                extraction_confidence REAL DEFAULT 0.0,
                extracted_date TEXT NOT NULL,
                
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
            )
        ''',
        
        'entities': '''
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                entity_type TEXT NOT NULL,
                entity_text TEXT NOT NULL,
                relevance_score REAL DEFAULT 0.0,
                extraction_date TEXT NOT NULL,
                entity_context TEXT,
                
                FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                UNIQUE(article_id, entity_type, entity_text)
            )
        ''',
        
        # Processing tracking tables
        'scraping_progress': '''
            CREATE TABLE IF NOT EXISTS scraping_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                articles_found INTEGER DEFAULT 0,
                batch_id INTEGER,
                
                FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
                FOREIGN KEY (batch_id) REFERENCES scraping_batches(id)
            )
        ''',
        
        'scraping_batches': '''
            CREATE TABLE IF NOT EXISTS scraping_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                total_candidates INTEGER NOT NULL,
                completed_candidates INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                config TEXT
            )
        ''',
        
        # Candidate profiles (derived data)
        'candidate_profiles': '''
            CREATE TABLE IF NOT EXISTS candidate_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER NOT NULL,
                total_articles INTEGER DEFAULT 0,
                total_quotes INTEGER DEFAULT 0,
                avg_biographical_score REAL DEFAULT 0.0,
                avg_political_score REAL DEFAULT 0.0,
                avg_academic_score REAL DEFAULT 0.0,
                avg_professional_score REAL DEFAULT 0.0,
                avg_public_service_score REAL DEFAULT 0.0,
                news_count INTEGER DEFAULT 0,
                article_count INTEGER DEFAULT 0,
                discourse_count INTEGER DEFAULT 0,
                profile_json TEXT,
                created_date TEXT NOT NULL,
                updated_date TEXT NOT NULL,
                
                FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
                UNIQUE(candidate_id)
            )
        '''
    }
    
    INDEXES = [
        'CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)',
        'CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)',
        'CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(overall_relevance)',
        'CREATE INDEX IF NOT EXISTS idx_articles_content_type ON articles(content_type)',
        'CREATE INDEX IF NOT EXISTS idx_articles_temporal ON articles(temporal_relevance)',
        'CREATE INDEX IF NOT EXISTS idx_articles_year_bounds ON articles(year_lower_bound, year_upper_bound)',
        'CREATE INDEX IF NOT EXISTS idx_articles_batch ON articles(batch_id)',
        
        'CREATE INDEX IF NOT EXISTS idx_candidates_name ON candidates(name)',
        'CREATE INDEX IF NOT EXISTS idx_candidates_municipality ON candidates(municipality)',
        'CREATE INDEX IF NOT EXISTS idx_candidates_year ON candidates(target_year)',
        
        'CREATE INDEX IF NOT EXISTS idx_candidate_articles_candidate ON candidate_articles(candidate_id)',
        'CREATE INDEX IF NOT EXISTS idx_candidate_articles_article ON candidate_articles(article_id)',
        'CREATE INDEX IF NOT EXISTS idx_candidate_articles_relevance ON candidate_articles(relevance)',
        
        'CREATE INDEX IF NOT EXISTS idx_quotes_article ON quotes(article_id)',
        'CREATE INDEX IF NOT EXISTS idx_quotes_candidate ON quotes(candidate_id)',
        
        'CREATE INDEX IF NOT EXISTS idx_entities_article ON entities(article_id)',
        'CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type)',
        'CREATE INDEX IF NOT EXISTS idx_entities_text ON entities(entity_text)',
        
        'CREATE INDEX IF NOT EXISTS idx_scraping_progress_candidate ON scraping_progress(candidate_id)',
        'CREATE INDEX IF NOT EXISTS idx_scraping_progress_batch ON scraping_progress(batch_id)',
        'CREATE INDEX IF NOT EXISTS idx_scraping_progress_status ON scraping_progress(status)',
    ]


def hash_string(text):
    """
    Create a hash from a string for caching purposes.
    
    Args:
        text (str): String to hash
        
    Returns:
        str: MD5 hash of the input string
    """
    if isinstance(text, str):
        return hashlib.md5(text.encode()).hexdigest()
    return hashlib.md5(str(text).encode()).hexdigest()


class Entity:
    """Base class for database entities with common methods"""
    
    @classmethod
    def from_row(cls, row):
        """Create an instance from a database row"""
        if row is None:
            return None
        
        instance = cls()
        for key in row.keys():
            setattr(instance, key, row[key])
        return instance
    
    def to_dict(self):
        """Convert instance to dictionary"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    
    def __repr__(self):
        """String representation"""
        attrs = ', '.join(f"{k}={v}" for k, v in self.to_dict().items())
        return f"{self.__class__.__name__}({attrs})"


class Candidate(Entity):
    """Candidate entity"""
    
    def __init__(self, name=None, municipality=None, target_year=None):
        self.id = None
        self.name = name
        self.municipality = municipality
        self.entidad = None
        self.cve_entidad = None
        self.cve_municipio = None
        self.target_year = target_year
        self.gender = None
        self.party = None
        self.period_format = None
        self.created_at = None
        self.updated_at = None


class Article(Entity):
    """Article entity"""
    
    def __init__(self, url=None, title=None, content=None):
        self.id = None
        self.url = url
        self.title = title
        self.snippet = None
        self.content = content
        self.html_content = None
        self.extracted_date = None
        self.content_date = None
        self.source = None
        self.language = None
        self.content_type = None
        self.content_type_confidence = 0.0
        self.date_confidence = 0.0
        self.temporal_relevance = 0.0
        self.content_relevance = 0.0
        self.overall_relevance = 0.0
        self.year_lower_bound = None
        self.year_upper_bound = None
        self.extraction_date = None
        self.search_query = None
        self.quote_count = 0
        self.processed = False
        self.batch_id = None


class CandidateArticle(Entity):
    """Link entity between Candidate and Article"""
    
    def __init__(self, candidate_id=None, article_id=None):
        self.id = None
        self.candidate_id = candidate_id
        self.article_id = article_id
        self.name_match_score = 0.0
        self.fuzzy_match_score = 0.0
        self.biographical_score = 0.0
        self.political_score = 0.0
        self.academic_score = 0.0
        self.professional_score = 0.0
        self.public_service_score = 0.0
        self.relevance = 0.0


class Quote(Entity):
    """Quote entity"""
    
    def __init__(self, article_id=None, candidate_id=None, quote_text=None):
        self.id = None
        self.article_id = article_id
        self.candidate_id = candidate_id
        self.quote_text = quote_text
        self.quote_context = None
        self.context_start = None
        self.context_end = None
        self.extraction_confidence = 0.0
        self.extracted_date = None


class EntityMention(Entity):
    """Entity mention in an article"""
    
    def __init__(self, article_id=None, entity_type=None, entity_text=None):
        self.id = None
        self.article_id = article_id
        self.entity_type = entity_type
        self.entity_text = entity_text
        self.relevance_score = 0.0
        self.extraction_date = None
        self.entity_context = None


class ScrapingBatch(Entity):
    """Scraping batch entity"""
    
    def __init__(self, total_candidates=0, config=None):
        self.id = None
        self.started_at = None
        self.completed_at = None
        self.total_candidates = total_candidates
        self.completed_candidates = 0
        self.status = None
        self.config = json.dumps(config) if config else None
    
    @property
    def config_dict(self):
        """Get config as a dictionary"""
        if self.config:
            try:
                return json.loads(self.config)
            except:
                return {}
        return {}


class CandidateProfile(Entity):
    """Candidate profile entity (derived data)"""
    
    def __init__(self, candidate_id=None):
        self.id = None
        self.candidate_id = candidate_id
        self.total_articles = 0
        self.total_quotes = 0
        self.avg_biographical_score = 0.0
        self.avg_political_score = 0.0
        self.avg_academic_score = 0.0
        self.avg_professional_score = 0.0
        self.avg_public_service_score = 0.0
        self.news_count = 0
        self.article_count = 0
        self.discourse_count = 0
        self.profile_json = None
        self.created_date = None
        self.updated_date = None
    
    @property
    def profile(self):
        """Get profile as a dictionary"""
        if self.profile_json:
            try:
                return json.loads(self.profile_json)
            except:
                return {}
        return {}
    
    @profile.setter
    def profile(self, value):
        """Set profile from a dictionary"""
        if isinstance(value, dict):
            self.profile_json = json.dumps(value, ensure_ascii=False)