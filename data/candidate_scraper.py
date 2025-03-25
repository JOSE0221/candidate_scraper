#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mexican Municipal Candidates Web Scraper
----------------------------------------
A comprehensive solution for scraping Spanish language information about Mexican political candidates,
building an enhanced dataset with links, article content, and candidate information.

This script:
1. Processes a CSV of candidate data
2. Searches the web for Spanish information about each candidate
3. Extracts and analyzes the content
4. Creates a structured dataset with all findings
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import datetime
import json
import os
import logging
import hashlib
import random
import sqlite3
from urllib.parse import quote, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Import NLP libraries for text analysis
import nltk
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords

# Simple progress indicator as replacement for tqdm
class SimpleProgress:
    def __init__(self, iterable=None, total=None, desc=None):
        self.iterable = iterable
        self.total = total or (len(iterable) if iterable is not None else None)
        self.desc = desc
        self.count = 0
        
    def __iter__(self):
        if self.desc:
            print(f"{self.desc}: Started")
        for item in self.iterable:
            self.count += 1
            if self.total and self.count % max(1, self.total//10) == 0:
                if self.desc:
                    print(f"{self.desc}: {self.count}/{self.total} ({int(100*self.count/self.total)}%)")
                else:
                    print(f"Progress: {self.count}/{self.total} ({int(100*self.count/self.total)}%)")
            yield item
        if self.desc:
            print(f"{self.desc}: Completed")

# =============================================================================
# Configuration Settings
# =============================================================================

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
RAW_DATA_DIR = os.path.join(RESULTS_DIR, "raw_data")
PROCESSED_DATA_DIR = os.path.join(RESULTS_DIR, "processed_data")

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Database settings
DEFAULT_DB_PATH = os.path.join(DATA_DIR, "mexican_candidates.db")

# Oxylabs API credentials
OXYLABS_USERNAME = "jose0221_t0O13"
OXYLABS_PASSWORD = "Tr0p1c0s_159497"
OXYLABS_COUNTRY = "mx"
OXYLABS_API_MODE = "direct"
OXYLABS_PROXY_URL = "https://pr.oxylabs.io:7777"

# Scraping settings
DEFAULT_THREADS = 5
ARTICLE_THREADS = 10
DEFAULT_YEAR_RANGE = 2
MAX_RETRIES = 3
RETRY_DELAY = 1.5
REQUEST_TIMEOUT = 30
MAX_RESULTS_PER_CANDIDATE = 10
MIN_RELEVANCE_THRESHOLD = 0.3
MIN_TEMPORAL_RELEVANCE = 0.2
USER_AGENT_ROTATION = True
SPANISH_CONFIDENCE_THRESHOLD = 0.7  # Minimum confidence score for Spanish language detection

# Default permanent blacklist of domains
BLACKLISTED_DOMAINS = [
    "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com",
    "tiktok.com", "linkedin.com", "pinterest.com", "mercadolibre.com",
    "amazon.com", "walmart.com", "flickr.com", "reddit.com", "slideshare.net",
    "issuu.com", "spotify.com", "idealista.com", "inmuebles24.com", 
    "vivastreet.com", "olx.com", "segundamano.mx", "booking.com", 
    "tripadvisor.com", "googleusercontent.com", "googleapis.com", "google.com"
]

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
]

# Mexican states mapping
MEXICAN_STATES = {
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

# Mexican political parties
POLITICAL_PARTIES = {
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

# Spanish language markers (common Spanish words to help identify Spanish content)
SPANISH_MARKERS = [
    "el", "la", "los", "las", "un", "una", "unos", "unas", "y", "o", "pero", "porque",
    "que", "quien", "como", "cuando", "donde", "cual", "cuales", "este", "esta", "estos",
    "estas", "ese", "esa", "esos", "esas", "aquel", "aquella", "aquellos", "aquellas",
    "para", "por", "con", "sin", "sobre", "entre", "durante", "hasta", "desde", "contra",
    "según", "hacia", "mediante", "excepto", "salvo", "más", "menos", "muy", "mucho",
    "poco", "tan", "tanto", "bastante", "demasiado", "nada", "algo", "alguien", "nadie",
    "alguno", "ninguno", "todo", "cada", "varios", "pocos", "algunos"
]

# Setup logging
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(LOGS_DIR, f"scraper_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# Database Setup
# =============================================================================

def setup_database(db_path=DEFAULT_DB_PATH):
    """
    Set up SQLite database for storing scraping results and metadata.
    
    Args:
        db_path (str): Path to SQLite database file
        
    Returns:
        sqlite3.Connection: Database connection object
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables if they don't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS candidates (
        id TEXT PRIMARY KEY,
        entidad TEXT,
        municipio TEXT,
        partido TEXT,
        nombre TEXT,
        periodo TEXT,
        year INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS urls (
        id TEXT PRIMARY KEY,
        candidate_id TEXT,
        url TEXT,
        title TEXT,
        source_type TEXT,
        language TEXT,
        spanish_confidence REAL,
        date TEXT,
        query TEXT,
        relevance_score REAL,
        has_candidate_statements INTEGER,
        processed INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (candidate_id) REFERENCES candidates(id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS articles (
        id TEXT PRIMARY KEY,
        url_id TEXT,
        text TEXT,
        content_type TEXT,
        contains_candidate_name INTEGER,
        contains_municipality INTEGER,
        mentioned_academic INTEGER,
        mentioned_professional INTEGER,
        mentioned_ideology INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (url_id) REFERENCES urls(id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS statements (
        id TEXT PRIMARY KEY,
        article_id TEXT,
        statement TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (article_id) REFERENCES articles(id)
    )
    ''')
    
    conn.commit()
    return conn


# =============================================================================
# OxylabScraper Class
# =============================================================================

class OxylabScraper:
    def __init__(self, username=OXYLABS_USERNAME, password=OXYLABS_PASSWORD, 
                 proxy_url=OXYLABS_PROXY_URL, country=OXYLABS_COUNTRY):
        """Initialize Oxylab scraper with credentials."""
        self.auth = (username, password)
        self.proxy_url = proxy_url
        self.country = country
        self.session = requests.Session()
        self.session.auth = self.auth
        
        # Download NLTK resources if needed
        try:
            nltk.data.find('tokenizers/punkt')
            nltk.data.find('corpora/stopwords')
        except LookupError:
            nltk.download('punkt')
            nltk.download('stopwords')
        
        self.stop_words_es = set(stopwords.words('spanish'))
        self.stop_words_en = set(stopwords.words('english'))
        
        # Load Spanish markers for improved language detection
        self.spanish_markers = set(SPANISH_MARKERS)
    
    def get_random_user_agent(self):
        """Get a random user agent from the list for rotation."""
        if USER_AGENT_ROTATION:
            return random.choice(USER_AGENTS)
        return USER_AGENTS[0]
    
    def search_google(self, query, year_range=None, max_pages=5, max_results=MAX_RESULTS_PER_CANDIDATE):
        """
        Perform a Google search via Oxylab, specifying Spanish language.
        
        Args:
            query (str): Search query
            year_range (tuple): (start_year, end_year) for date filtering
            max_pages (int): Maximum number of search result pages to process
            max_results (int): Maximum number of results to return
            
        Returns:
            list: List of search result URLs
        """
        all_results = []
        
        # Build date range filter if provided
        date_filter = ""
        if year_range:
            start_year, end_year = year_range
            date_filter = f" after:{start_year}-01-01 before:{end_year}-12-31"
        
        # Add language filter for Spanish content
        language_filter = " lang:es"
        
        # Complete query with filters
        filtered_query = query + date_filter + language_filter
        
        for page in range(max_pages):
            if len(all_results) >= max_results:
                break
                
            start = page * 10
            search_url = f"https://www.google.com/search?q={quote(filtered_query)}&start={start}&gl={self.country}&hl=es&lr=lang_es"
            
            logger.info(f"Searching Google: {search_url}")
            
            payload = {
                "source": "universal",
                "url": search_url,
                "render": "html",
                "parse": True,
                "geo_location": self.country,
                "user_agent": self.get_random_user_agent(),
                "accept_language": "es-MX,es;q=0.9",  # Specify Spanish language acceptance
                "locale": "es-MX"  # Set locale to Mexican Spanish
            }
            
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    response = self.session.post(
                        self.proxy_url, 
                        json=payload,
                        timeout=REQUEST_TIMEOUT
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'results' not in data or not data['results']:
                        logger.warning(f"No results found for {query} on page {page+1}")
                        break
                    
                    # Extract URLs from search results
                    soup = BeautifulSoup(data['results'][0]['content'], 'html.parser')
                    search_results = soup.select('div.yuRUbf > a')
                    
                    if not search_results:
                        logger.warning(f"No search result links found on page {page+1}")
                        break
                    
                    page_results = [link['href'] for link in search_results 
                                    if 'href' in link.attrs 
                                    and not any(domain in link['href'] for domain in BLACKLISTED_DOMAINS)]
                    
                    all_results.extend(page_results)
                    
                    # Check if there are more pages
                    next_page = soup.select_one('a#pnnext')
                    if not next_page:
                        break
                        
                    # Add a delay to avoid rate limiting
                    time.sleep(RETRY_DELAY)
                    break
                    
                except Exception as e:
                    retries += 1
                    logger.warning(f"Error searching {query} (attempt {retries}/{MAX_RETRIES}): {str(e)}")
                    time.sleep(RETRY_DELAY * retries)
            
            if retries == MAX_RETRIES:
                logger.error(f"Failed to search {query} after {MAX_RETRIES} attempts")
                break
                
        return list(set(all_results))[:max_results]  # Remove duplicates and limit results
    
    def search_bing(self, query, year_range=None, max_pages=5, max_results=MAX_RESULTS_PER_CANDIDATE):
        """
        Perform a Bing search via Oxylab, specifying Spanish language.
        
        Args:
            query (str): Search query
            year_range (tuple): (start_year, end_year) for date filtering
            max_pages (int): Maximum number of search result pages to process
            max_results (int): Maximum number of results to return
            
        Returns:
            list: List of search result URLs
        """
        all_results = []
        
        # Build date filter for Bing
        date_filter = ""
        if year_range:
            start_year, end_year = year_range
            # Bing uses a different date filter format than Google
            date_filter = f" daterange:{start_year}-{end_year}"
        
        # Add language filter for Spanish content
        language_filter = " language:es"
        
        # Complete query with filters
        filtered_query = query + date_filter + language_filter
        
        for page in range(max_pages):
            if len(all_results) >= max_results:
                break
                
            first = page * 10 + 1
            search_url = f"https://www.bing.com/search?q={quote(filtered_query)}&first={first}&cc={self.country}&setlang=es"
            
            logger.info(f"Searching Bing: {search_url}")
            
            payload = {
                "source": "universal",
                "url": search_url,
                "render": "html",
                "parse": True,
                "geo_location": self.country,
                "user_agent": self.get_random_user_agent(),
                "accept_language": "es-MX,es;q=0.9",  # Specify Spanish language acceptance
                "locale": "es-MX"  # Set locale to Mexican Spanish
            }
            
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    response = self.session.post(
                        self.proxy_url, 
                        json=payload,
                        timeout=REQUEST_TIMEOUT
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'results' not in data or not data['results']:
                        logger.warning(f"No Bing results found for {query} on page {page+1}")
                        break
                    
                    # Extract URLs from search results
                    soup = BeautifulSoup(data['results'][0]['content'], 'html.parser')
                    search_results = soup.select('h2 > a')
                    
                    if not search_results:
                        logger.warning(f"No Bing search result links found on page {page+1}")
                        break
                    
                    page_results = [link['href'] for link in search_results 
                                    if 'href' in link.attrs 
                                    and not link['href'].startswith('/')
                                    and not any(domain in link['href'] for domain in BLACKLISTED_DOMAINS)]
                    
                    all_results.extend(page_results)
                    
                    # Check if there are more pages
                    next_page = soup.select_one('a.sb_pagN')
                    if not next_page:
                        break
                        
                    # Add a delay to avoid rate limiting
                    time.sleep(RETRY_DELAY)
                    break
                    
                except Exception as e:
                    retries += 1
                    logger.warning(f"Error searching Bing for {query} (attempt {retries}/{MAX_RETRIES}): {str(e)}")
                    time.sleep(RETRY_DELAY * retries)
            
            if retries == MAX_RETRIES:
                logger.error(f"Failed to search Bing for {query} after {MAX_RETRIES} attempts")
                break
                
        return list(set(all_results))[:max_results]  # Remove duplicates and limit results
    
    def scrape_url(self, url):
        """
        Scrape content from a specific URL using Oxylab, with Spanish language preference.
        
        Args:
            url (str): URL to scrape
            
        Returns:
            dict: Scraped data including title, text, date, and metadata
        """
        logger.info(f"Scraping: {url}")
        
        result = {
            'url': url,
            'title': '',
            'text': '',
            'date': None,
            'source_type': self._classify_source(url),
            'language': 'unknown',
            'spanish_confidence': 0.0,
            'scrape_timestamp': datetime.datetime.now().isoformat()
        }
        
        retries = 0
        while retries < MAX_RETRIES:
            try:
                payload = {
                    "source": "universal",
                    "url": url,
                    "render": "html",
                    "parse": True,
                    "premium_proxy": "true",  # Use premium proxies for better success rate
                    "user_agent": self.get_random_user_agent(),
                    "accept_language": "es-MX,es;q=0.9",  # Request Spanish content
                    "locale": "es-MX",  # Set locale to Mexican Spanish
                    "country": OXYLABS_COUNTRY,
                    "device": "desktop"
                }
                
                response = self.session.post(
                    self.proxy_url, 
                    json=payload,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                data = response.json()
                
                if 'results' not in data or not data['results']:
                    logger.warning(f"No content found for {url}")
                    retries += 1
                    time.sleep(RETRY_DELAY * retries)
                    continue
                
                content = data['results'][0]['content']
                soup = BeautifulSoup(content, 'html.parser')
                
                # Extract title
                title_tag = soup.find('title')
                if title_tag:
                    result['title'] = title_tag.text.strip()
                
                # Extract main text content
                # Try different content selectors based on common patterns
                paragraphs = []
                
                for selector in [
                    'article', 'main', 
                    'div.content', 'div.article', 'div.post', 'div.entry-content',
                    '#content', '#main', '#article', '#post'
                ]:
                    content_div = soup.select_one(selector)
                    if content_div:
                        paragraphs = content_div.find_all('p')
                        break
                
                # If no matching selector, try to get all paragraphs and filter
                if not paragraphs:
                    paragraphs = soup.find_all('p')
                    # Filter out too short paragraphs or menu items
                    paragraphs = [p for p in paragraphs if len(p.text.strip()) > 60]
                
                result['text'] = '\n\n'.join([p.text.strip() for p in paragraphs])
                
                # Try to extract date
                result['date'] = self._extract_date(soup, url)
                
                # Detect language and get Spanish confidence
                language, spanish_confidence = self._detect_language_with_confidence(result['text'])
                result['language'] = language
                result['spanish_confidence'] = spanish_confidence
                
                # If we got good content and it's in Spanish, break the retry loop
                if result['text'] and len(result['text']) > 100 and result['language'] == 'es':
                    break
                # If content is not Spanish, retry with more aggressive Spanish language headers
                elif result['language'] != 'es' and spanish_confidence < SPANISH_CONFIDENCE_THRESHOLD:
                    logger.warning(f"Content not in Spanish (confidence: {spanish_confidence}), retrying with stronger language settings")
                    retries += 1
                    
                    # More aggressive Spanish language settings
                    payload["accept_language"] = "es-MX,es;q=1.0,en;q=0.1"
                    payload["headers"] = {
                        "Accept-Language": "es-MX,es;q=1.0,en;q=0.1",
                        "Content-Language": "es-MX",
                    }
                    
                    time.sleep(RETRY_DELAY * retries)
                else:
                    break
                
            except Exception as e:
                retries += 1
                logger.warning(f"Error scraping {url} (attempt {retries}/{MAX_RETRIES}): {str(e)}")
                time.sleep(RETRY_DELAY * retries)
        
        if retries == MAX_RETRIES:
            logger.error(f"Failed to scrape {url} after {MAX_RETRIES} attempts")
        
        return result
    
    def _extract_date(self, soup, url):
        """
        Extract publication date from a webpage.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            url (str): URL of the page
            
        Returns:
            str: ISO formatted date or None
        """
        # Try various meta tags for date
        for meta in soup.find_all('meta'):
            if meta.get('property') in ['article:published_time', 'og:published_time', 'datePublished']:
                date_str = meta.get('content')
                if date_str:
                    try:
                        # Parse and standardize date format
                        dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        return dt.date().isoformat()
                    except:
                        pass
                        
            if meta.get('name') in ['pubdate', 'publishdate', 'date']:
                date_str = meta.get('content')
                if date_str:
                    try:
                        # Try different date formats
                        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y']:
                            try:
                                dt = datetime.datetime.strptime(date_str, fmt)
                                return dt.date().isoformat()
                            except:
                                continue
                    except:
                        pass
        
        # Look for time elements
        time_tags = soup.find_all('time')
        for time_tag in time_tags:
            date_str = time_tag.get('datetime')
            if date_str:
                try:
                    dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    return dt.date().isoformat()
                except:
                    pass
        
        # Try to extract date from URL
        date_patterns = [
            r'(\d{4})/(\d{1,2})/(\d{1,2})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, url)
            if match:
                try:
                    year, month, day = map(int, match.groups())
                    if 1990 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                        return f"{year}-{month:02d}-{day:02d}"
                except:
                    pass
        
        return None
    
    def _detect_language_with_confidence(self, text):
        """
        Enhanced language detection with confidence score, focusing on Spanish.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            tuple: (detected_language, spanish_confidence_score)
        """
        if not text or len(text.strip()) < 50:
            return 'unknown', 0.0
            
        # Count Spanish and English stopwords and markers
        text_lower = text.lower()
        words = re.findall(r'\b\w+\b', text_lower)
        
        if not words:
            return 'unknown', 0.0
        
        total_words = len(words)
        
        # Count Spanish stopwords and markers
        es_stopword_count = sum(1 for word in words if word in self.stop_words_es)
        es_marker_count = sum(1 for word in words if word in self.spanish_markers)
        
        # Count English stopwords
        en_stopword_count = sum(1 for word in words if word in self.stop_words_en)
        
        # Calculate ratios
        es_stopword_ratio = es_stopword_count / total_words if total_words > 0 else 0
        es_marker_ratio = es_marker_count / total_words if total_words > 0 else 0
        en_stopword_ratio = en_stopword_count / total_words if total_words > 0 else 0
        
        # Combined Spanish confidence score (weighted average)
        es_confidence = (es_stopword_ratio * 0.7) + (es_marker_ratio * 0.3)
        
        # Spanish content should have high Spanish indicators and low English indicators
        if es_confidence > SPANISH_CONFIDENCE_THRESHOLD and es_stopword_ratio > en_stopword_ratio:
            return 'es', es_confidence
        elif en_stopword_ratio > 0.05 and en_stopword_ratio > es_stopword_ratio:
            return 'en', es_confidence
        elif es_confidence > 0.3:  # Lower threshold for "maybe Spanish"
            return 'es', es_confidence
        else:
            return 'unknown', es_confidence
    
    def _classify_source(self, url):
        """
        Classify the type of source based on the URL.
        
        Args:
            url (str): URL to classify
            
        Returns:
            str: Source type classification
        """
        domain = urlparse(url).netloc.lower()
        
        # News websites classification
        news_domains = [
            'cnn.com', 'bbc.', 'nytimes.com', 'washingtonpost.com', 
            'elpais.', 'elmundo.', 'eluniversal.', 'milenio.', 
            'excelsior.', 'jornada.', 'reforma.', 'proceso.',
            'news', 'noticia', 'diario', 'periodico'
        ]
        
        # Government websites
        gov_domains = [
            'gob.mx', 'gov', '.mil', 'gobierno', 'municipal'
        ]
        
        # Social media
        social_domains = [
            'facebook.com', 'twitter.com', 'instagram.com', 
            'youtube.com', 'tiktok.com', 'linkedin.com'
        ]
        
        # Academic
        academic_domains = [
            '.edu', 'academia.', 'researchgate.', 'scielo.', 
            'scholar.', 'university', 'universidad'
        ]
        
        # Political parties
        party_domains = [
            'pri.', 'pan.', 'morena.', 'prd.', 'movimientociudadano.',
            'partidoverde.', 'pt.'
        ]
        
        for d in news_domains:
            if d in domain:
                return 'news'
                
        for d in gov_domains:
            if d in domain:
                return 'government'
                
        for d in social_domains:
            if d in domain:
                return 'social_media'
                
        for d in academic_domains:
            if d in domain:
                return 'academic'
                
        for d in party_domains:
            if d in domain:
                return 'political_party'
                
        return 'other'

    def analyze_content(self, candidate_info, text, url):
        """
        Analyze scraped content to extract relevant information and classify it.
        Focuses on Spanish language content.
        
        Args:
            candidate_info (dict): Basic candidate information
            text (str): Scraped text content
            url (str): URL of the content
            
        Returns:
            dict: Analysis results
        """
        if not text:
            return {
                'contains_candidate_name': False,
                'contains_municipality': False,
                'relevance_score': 0,
                'has_candidate_statements': False,
                'candidate_statements': [],
                'mentioned_academic': False,
                'mentioned_professional': False,
                'mentioned_ideology': False,
                'content_type': 'unknown'
            }
        
        # Check language and Spanish confidence
        language, spanish_confidence = self._detect_language_with_confidence(text)
        
        # Only analyze Spanish content
        if language != 'es' or spanish_confidence < SPANISH_CONFIDENCE_THRESHOLD:
            logger.info(f"Skipping non-Spanish content (confidence: {spanish_confidence}) from {url}")
            return {
                'contains_candidate_name': False,
                'contains_municipality': False,
                'relevance_score': 0,
                'has_candidate_statements': False,
                'candidate_statements': [],
                'mentioned_academic': False,
                'mentioned_professional': False,
                'mentioned_ideology': False,
                'content_type': 'unknown',
                'spanish_confidence': spanish_confidence
            }
        
        # Convert to lowercase for case-insensitive matching
        text_lower = text.lower()
        candidate_name_lower = candidate_info['PRESIDENTE_MUNICIPAL'].lower()
        municipality_lower = candidate_info['MUNICIPIO'].lower()
        
        # Get state name from abbreviation if possible
        entity_code = candidate_info.get('ENTIDAD', '')
        state_name = MEXICAN_STATES.get(entity_code, entity_code).lower()
        
        # Get party variations for the candidate's party
        party_code = candidate_info.get('PARTIDO', '')
        party_variations = [v.lower() for v in POLITICAL_PARTIES.get(party_code, [party_code])]
        
        # Check for basic relevance
        contains_candidate_name = candidate_name_lower in text_lower
        contains_municipality = municipality_lower in text_lower
        contains_state = state_name in text_lower
        contains_party = any(party_var in text_lower for party_var in party_variations)
        
        # Calculate basic relevance score
        relevance_score = 0
        if contains_candidate_name:
            relevance_score += 3
        if contains_municipality:
            relevance_score += 2
        if contains_state:
            relevance_score += 1
        if contains_party:
            relevance_score += 1
        
        # Name variations (first name, last name)
        name_parts = candidate_name_lower.split()
        if len(name_parts) > 1:
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
            if first_name in text_lower and len(first_name) > 3:  # Avoid short names that may match many contexts
                relevance_score += 1
            if last_name in text_lower and len(last_name) > 3:
                relevance_score += 1
        
        # Temporal relevance - check if election year is mentioned
        election_year = str(candidate_info.get('Year', ''))
        if election_year in text:
            relevance_score += 2
        
        # Extract potential candidate statements (simplified approach)
        has_candidate_statements = False
        candidate_statements = []
        
        # Split into sentences
        sentences = sent_tokenize(text)
        
        # Spanish speech indicators
        speech_indicators = [
            "dijo", "declaró", "afirmó", "mencionó", "expresó", "señaló",
            "comentó", "aseguró", "prometió", "anunció", "manifestó",
            "explicó", "indicó", "subrayó", "destacó", "enfatizó",
            "sostuvo", "opinó", "reveló", "advirtió", "informó"
        ]
        
        for sentence in sentences:
            sentence_lower = sentence.lower()
            
            # Check if sentence is about the candidate
            is_about_candidate = candidate_name_lower in sentence_lower
            if not is_about_candidate:
                for part in name_parts:
                    if len(part) > 3 and part in sentence_lower:  # Avoid short name parts
                        is_about_candidate = True
                        break
            
            # Check for speech indicators
            is_speech = False
            for indicator in speech_indicators:
                if indicator in sentence_lower:
                    is_speech = True
                    break
            
            # Direct quotes
            has_quotes = '"' in sentence or '"' in sentence or '"' in sentence or "'" in sentence
            
            # If this might be a candidate statement
            if is_about_candidate and (is_speech or has_quotes):
                has_candidate_statements = True
                candidate_statements.append(sentence.strip())
        
        # Check for specific information categories using Spanish keywords
        academic_keywords = [
            "educación", "universidad", "título", "grado", "licenciatura", 
            "maestría", "doctorado", "estudios", "carrera", "académico",
            "escuela", "instituto", "formación", "egresado", "titulado",
            "colegio", "preparación", "profesor", "investigador", "académica"
        ]
        
        professional_keywords = [
            "experiencia", "profesional", "trabajo", "empleo", "cargo", 
            "puesto", "ocupación", "profesión", "empresario", "negocio",
            "trayectoria", "carrera", "laboral", "currículum", "curriculum",
            "desempeñó", "ejerció", "laboró", "administró", "dirigió"
        ]
        
        ideology_keywords = [
            "ideología", "político", "partido", "posición", "propuesta", 
            "programa", "agenda", "izquierda", "derecha", "centro",
            "liberal", "conservador", "progresista", "nacionalista",
            "valores", "principios", "creencias", "doctrina", "iniciativa",
            "reforma", "cambio", "política", "ideológico", "campaña"
        ]
        
        election_keywords = [
            "elección", "campaña", "voto", "electoral", "urna", 
            "candidato", "candidatura", "elecciones", "comicios", "votación",
            "sufragio", "elector", "votante", "distrito", "jornada",
            "proceso", "boleta", "triunfo", "victoria", "ganador"
        ]
        
        mentioned_academic = any(keyword in text_lower for keyword in academic_keywords)
        mentioned_professional = any(keyword in text_lower for keyword in professional_keywords)
        mentioned_ideology = any(keyword in text_lower for keyword in ideology_keywords)
        mentioned_election = any(keyword in text_lower for keyword in election_keywords)
        
        # Apply relevance threshold
        if relevance_score < MIN_RELEVANCE_THRESHOLD * 10:  # Scale the threshold
            relevance_score = 0
        
        # Determine content type based on keywords and source classification
        content_type = "general"
        if mentioned_academic and mentioned_professional:
            content_type = "biographical"
        elif mentioned_election:
            content_type = "electoral"
        elif mentioned_ideology:
            content_type = "ideological"
        
        return {
            'contains_candidate_name': contains_candidate_name,
            'contains_municipality': contains_municipality,
            'relevance_score': relevance_score,
            'has_candidate_statements': has_candidate_statements,
            'candidate_statements': candidate_statements,
            'mentioned_academic': mentioned_academic,
            'mentioned_professional': mentioned_professional,
            'mentioned_ideology': mentioned_ideology,
            'content_type': content_type,
            'spanish_confidence': spanish_confidence
        }


# =============================================================================
# Main Processing Functions
# =============================================================================

def process_candidates_file(csv_path, db_conn=None, scraper=None, year_range=DEFAULT_YEAR_RANGE, resume=True):
    """
    Process a CSV file of political candidates and scrape relevant Spanish information.
    
    Args:
        csv_path (str): Path to CSV file containing candidate information
        db_conn (sqlite3.Connection): Database connection
        scraper (OxylabScraper): Initialized scraper instance
        year_range (int): Number of years before and after election to include
        resume (bool): Whether to resume an incomplete scraping job
        
    Returns:
        str: Path to the final consolidated CSV output
    """
    # Load candidates data
    logger.info(f"Loading candidates from {csv_path}")
    candidates_df = pd.read_csv(csv_path)
    
    # Initialize database connection if not provided
    if db_conn is None:
        db_conn = setup_database()
    cursor = db_conn.cursor()
    
    # Initialize the scraper if not provided
    if scraper is None:
        scraper = OxylabScraper()
    
    # Track progress
    results = []
    candidate_count = len(candidates_df)
    
    # Check for candidates already processed if resuming
    processed_candidates = set()
    if resume:
        cursor.execute("SELECT id FROM candidates")
        processed_candidates = {row[0] for row in cursor.fetchall()}
        logger.info(f"Resuming job with {len(processed_candidates)} already processed candidates")
    
    # Simple progress tracker
    print(f"Processing {candidate_count} candidates")
    
    for idx, row in enumerate(candidates_df.iterrows()):
        idx = row[0]  # Get actual index from pandas row tuple
        row = row[1]  # Get actual data from pandas row tuple
        
        # Show progress
        if idx % 10 == 0 or idx == candidate_count - 1:
            print(f"Progress: {idx+1}/{candidate_count} candidates ({int(100*(idx+1)/candidate_count)}%)")
        
        try:
            # Create a unique identifier for this candidate
            candidate_id = hashlib.md5(f"{row['PRESIDENTE_MUNICIPAL']}_{row['MUNICIPIO']}_{row['Year']}".encode()).hexdigest()
            
            # Skip if already processed and resuming
            if candidate_id in processed_candidates and resume:
                logger.info(f"Skipping already processed candidate: {row['PRESIDENTE_MUNICIPAL']}")
                continue
            
            # Build search queries
            candidate_name = row['PRESIDENTE_MUNICIPAL']
            municipality = row['MUNICIPIO']
            entity = row['ENTIDAD']
            party = row['PARTIDO']
            election_year = int(row['Year'])
            
            # Define year range for search (election year ± specified years)
            search_year_range = (election_year - year_range, election_year + year_range)
            
            # Prepare different query variations for comprehensive search
            queries = [
                f"{candidate_name} {municipality} {election_year}",  # Basic query
                f"{candidate_name} candidato {municipality} {party}",  # With party
                f"{candidate_name} elección {municipality} {entity}",  # With state/entity
                f"{candidate_name} campaña {municipality}"  # Campaign focused
            ]
            
            # Add full name variations of the political party
            if party in POLITICAL_PARTIES:
                party_full_name = POLITICAL_PARTIES[party][0]
                queries.append(f"{candidate_name} {municipality} {party_full_name}")
            
            # Add full state name if abbreviated
            if entity in MEXICAN_STATES:
                state_full_name = MEXICAN_STATES[entity]
                queries.append(f"{candidate_name} {municipality} {state_full_name}")
            
            # Track URLs for this candidate to avoid duplicates
            candidate_urls = set()
            
            # Store candidate basic info for later reference
            candidate_info = {
                'ENTIDAD': entity,
                'MUNICIPIO': municipality,
                'PARTIDO': party,
                'PRESIDENTE_MUNICIPAL': candidate_name,
                'PERIODO_FORMATO_ORIGINAL': row.get('PERIODO_FORMATO_ORIGINAL', ''),
                'Year': election_year,
                'candidate_id': candidate_id
            }
            
            # Insert candidate into database
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO candidates (id, entidad, municipio, partido, nombre, periodo, year) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        candidate_id,
                        entity,
                        municipality,
                        party,
                        candidate_name,
                        row.get('PERIODO_FORMATO_ORIGINAL', ''),
                        election_year
                    )
                )
                db_conn.commit()
            except Exception as e:
                logger.error(f"Error inserting candidate {candidate_id}: {str(e)}")
                db_conn.rollback()
            
            # Save candidate info in JSON format
            candidate_file = os.path.join(RAW_DATA_DIR, f"candidate_{candidate_id}.json")
            with open(candidate_file, 'w', encoding='utf-8') as f:
                json.dump(candidate_info, f, ensure_ascii=False, indent=2)
            
            # Search and scrape for each query variation
            for query in queries:
                logger.info(f"Processing query: {query}")
                
                # Search Google (Spanish-only)
                google_urls = scraper.search_google(query, year_range=search_year_range)
                
                # Search Bing for additional results (Spanish-only)
                bing_urls = scraper.search_bing(query, year_range=search_year_range)
                
                # Combine results, removing duplicates
                all_urls = list(set(google_urls + bing_urls) - candidate_urls)
                
                if not all_urls:
                    logger.warning(f"No search results found for query: {query}")
                    continue
                    
                logger.info(f"Found {len(all_urls)} unique URLs for query: {query}")
                
                # Process each URL
                for url in all_urls:
                    # Add to tracked URLs
                    candidate_urls.add(url)
                    
                    # Generate a unique ID for this URL
                    url_id = hashlib.md5(url.encode()).hexdigest()
                    
                    # Check if we've already scraped this URL
                    cursor.execute("SELECT id, processed, language, spanish_confidence FROM urls WHERE id = ?", (url_id,))
                    url_record = cursor.fetchone()
                    
                    if url_record and resume:
                        # URL already exists in database
                        if url_record[1] == 1:  # processed flag is set
                            # Skip if it's not Spanish
                            if url_record[2] != 'es' or (url_record[3] is not None and float(url_record[3]) < SPANISH_CONFIDENCE_THRESHOLD):
                                logger.info(f"Skipping non-Spanish content from {url} (confidence: {url_record[3]})")
                                continue
                                
                            logger.info(f"Using cached Spanish content for {url}")
                            
                            # Retrieve url info
                            cursor.execute("""
                                SELECT url, title, source_type, language, date, query, relevance_score, has_candidate_statements, spanish_confidence
                                FROM urls WHERE id = ?
                            """, (url_id,))
                            url_data = cursor.fetchone()
                            
                            if url_data:
                                url_info = {
                                    'url': url_data[0],
                                    'title': url_data[1],
                                    'source_type': url_data[2],
                                    'language': url_data[3],
                                    'date': url_data[4],
                                    'query': url_data[5],
                                    'relevance_score': url_data[6],
                                    'has_candidate_statements': bool(url_data[7]),
                                    'spanish_confidence': url_data[8]
                                }
                                
                                # Retrieve article info
                                cursor.execute("""
                                    SELECT content_type, contains_candidate_name, contains_municipality, 
                                    mentioned_academic, mentioned_professional, mentioned_ideology
                                    FROM articles WHERE url_id = ?
                                """, (url_id,))
                                article_data = cursor.fetchone()
                                
                                if article_data:
                                    results.append({
                                        **candidate_info,
                                        **url_info,
                                        'content_type': article_data[0],
                                        'contains_candidate_name': bool(article_data[1]),
                                        'contains_municipality': bool(article_data[2]),
                                        'mentioned_academic': bool(article_data[3]),
                                        'mentioned_professional': bool(article_data[4]),
                                        'mentioned_ideology': bool(article_data[5])
                                    })
                            
                            continue
                    
                    # Scrape the URL
                    try:
                        scraped_data = scraper.scrape_url(url)
                        
                        # Skip if no meaningful content was extracted
                        if not scraped_data.get('text') or len(scraped_data.get('text', '')) < 100:
                            logger.warning(f"Insufficient content from {url}")
                            continue
                        
                        # Skip if content is not in Spanish with sufficient confidence
                        if scraped_data.get('language') != 'es' or scraped_data.get('spanish_confidence', 0) < SPANISH_CONFIDENCE_THRESHOLD:
                            logger.info(f"Skipping non-Spanish content from {url} (confidence: {scraped_data.get('spanish_confidence', 0)})")
                            
                            # Still store in database but mark as non-Spanish
                            try:
                                cursor.execute("""
                                    INSERT OR REPLACE INTO urls 
                                    (id, candidate_id, url, title, source_type, language, spanish_confidence, date, query, relevance_score, has_candidate_statements, processed)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    url_id,
                                    candidate_id,
                                    url,
                                    scraped_data.get('title', ''),
                                    scraped_data.get('source_type', 'unknown'),
                                    scraped_data.get('language', 'unknown'),
                                    scraped_data.get('spanish_confidence', 0),
                                    scraped_data.get('date'),
                                    query,
                                    0,  # Zero relevance for non-Spanish
                                    0,  # No statements for non-Spanish
                                    1   # Mark as processed
                                ))
                                db_conn.commit()
                            except Exception as e:
                                logger.error(f"Database error for {url}: {str(e)}")
                                db_conn.rollback()
                                
                            continue
                        
                        # Add query that found this result
                        scraped_data['query'] = query
                        
                        # Analyze content relevance and extract information
                        analysis = scraper.analyze_content(
                            candidate_info, 
                            scraped_data.get('text', ''),
                            url
                        )
                        
                        # Skip if analysis determined content isn't relevant or Spanish
                        if analysis['relevance_score'] == 0 or analysis.get('spanish_confidence', 0) < SPANISH_CONFIDENCE_THRESHOLD:
                            logger.info(f"Content not relevant or not sufficiently Spanish from {url}")
                            
                            # Store in database but mark as not relevant
                            try:
                                cursor.execute("""
                                    INSERT OR REPLACE INTO urls 
                                    (id, candidate_id, url, title, source_type, language, spanish_confidence, date, query, relevance_score, has_candidate_statements, processed)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    url_id,
                                    candidate_id,
                                    url,
                                    scraped_data.get('title', ''),
                                    scraped_data.get('source_type', 'unknown'),
                                    scraped_data.get('language', 'unknown'),
                                    analysis.get('spanish_confidence', 0),
                                    scraped_data.get('date'),
                                    query,
                                    0,  # Zero relevance
                                    0,  # No statements
                                    1   # Mark as processed
                                ))
                                db_conn.commit()
                            except Exception as e:
                                logger.error(f"Database error for {url}: {str(e)}")
                                db_conn.rollback()
                                
                            continue
                        
                        # Save raw result as JSON
                        result_file = os.path.join(RAW_DATA_DIR, f"result_{url_id}.json")
                        with open(result_file, 'w', encoding='utf-8') as f:
                            json.dump({**scraped_data, 'analysis': analysis}, f, ensure_ascii=False, indent=2)
                        
                        # Insert URL info into database
                        try:
                            cursor.execute("""
                                INSERT OR REPLACE INTO urls 
                                (id, candidate_id, url, title, source_type, language, spanish_confidence, date, query, relevance_score, has_candidate_statements, processed)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                url_id,
                                candidate_id,
                                url,
                                scraped_data.get('title', ''),
                                scraped_data.get('source_type', 'unknown'),
                                scraped_data.get('language', 'unknown'),
                                scraped_data.get('spanish_confidence', 0),
                                scraped_data.get('date'),
                                query,
                                analysis['relevance_score'],
                                int(analysis['has_candidate_statements']),
                                1  # Processed flag
                            ))
                            
                            # Insert article content
                            article_id = hashlib.md5((url_id + '_article').encode()).hexdigest()
                            cursor.execute("""
                                INSERT OR REPLACE INTO articles
                                (id, url_id, text, content_type, contains_candidate_name, contains_municipality,
                                mentioned_academic, mentioned_professional, mentioned_ideology)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                article_id,
                                url_id,
                                scraped_data.get('text', ''),
                                analysis['content_type'],
                                int(analysis['contains_candidate_name']),
                                int(analysis['contains_municipality']),
                                int(analysis['mentioned_academic']),
                                int(analysis['mentioned_professional']),
                                int(analysis['mentioned_ideology'])
                            ))
                            
                            # Insert candidate statements
                            for statement in analysis.get('candidate_statements', []):
                                statement_id = hashlib.md5((article_id + statement[:50]).encode()).hexdigest()
                                cursor.execute("""
                                    INSERT OR IGNORE INTO statements
                                    (id, article_id, statement)
                                    VALUES (?, ?, ?)
                                """, (
                                    statement_id,
                                    article_id,
                                    statement
                                ))
                            
                            db_conn.commit()
                        except Exception as e:
                            logger.error(f"Database error for {url}: {str(e)}")
                            db_conn.rollback()
                        
                        # Add to results
                        results.append({
                            **candidate_info,
                            'url': url,
                            'title': scraped_data.get('title', ''),
                            'date': scraped_data.get('date'),
                            'source_type': scraped_data.get('source_type', 'unknown'),
                            'language': scraped_data.get('language', 'unknown'),
                            'spanish_confidence': scraped_data.get('spanish_confidence', 0),
                            'content_type': analysis['content_type'],
                            'relevance_score': analysis['relevance_score'],
                            'has_candidate_statements': analysis['has_candidate_statements'],
                            'contains_candidate_name': analysis['contains_candidate_name'],
                            'contains_municipality': analysis['contains_municipality'],
                            'mentioned_academic': analysis['mentioned_academic'],
                            'mentioned_professional': analysis['mentioned_professional'],
                            'mentioned_ideology': analysis['mentioned_ideology']
                        })
                        
                        # Sleep to avoid rate limiting
                        time.sleep(RETRY_DELAY)
                        
                    except Exception as e:
                        logger.error(f"Error processing {url}: {str(e)}")
                        continue
            
            # Create intermediate CSV file periodically
            if idx % 10 == 0 or idx == len(candidates_df) - 1:
                intermediate_df = pd.DataFrame(results)
                if not intermediate_df.empty:
                    intermediate_csv = os.path.join(PROCESSED_DATA_DIR, f"intermediate_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    intermediate_df.to_csv(intermediate_csv, index=False)
                    logger.info(f"Saved intermediate results to {intermediate_csv}")
        
        except Exception as e:
            logger.error(f"Error processing candidate {idx}: {str(e)}")
            continue
    
    # Create final consolidated CSV
    if results:
        final_df = pd.DataFrame(results)
        final_csv = os.path.join(RESULTS_DIR, f"candidate_web_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        final_df.to_csv(final_csv, index=False)
        logger.info(f"Saved final consolidated results to {final_csv}")
        
        # Create a full dataset that includes text content
        create_full_dataset(db_conn, final_csv)
        
        return final_csv
    else:
        logger.warning("No results were found for any candidates.")
        return None


def create_full_dataset(db_conn, summary_csv_path):
    """
    Create a full dataset that includes all the Spanish text content from the database.
    
    Args:
        db_conn (sqlite3.Connection): Database connection
        summary_csv_path (str): Path to the summary CSV file
    """
    logger.info("Creating full dataset with Spanish text content...")
    
    cursor = db_conn.cursor()
    
    # Query for full dataset (only Spanish content)
    cursor.execute("""
        SELECT 
            c.id as candidate_id,
            c.entidad,
            c.municipio,
            c.partido,
            c.nombre as presidente_municipal,
            c.periodo as periodo_formato_original,
            c.year,
            u.id as url_id,
            u.url,
            u.title,
            u.source_type,
            u.language,
            u.spanish_confidence,
            u.date,
            u.query,
            u.relevance_score,
            u.has_candidate_statements,
            a.text,
            a.content_type,
            a.contains_candidate_name,
            a.contains_municipality,
            a.mentioned_academic,
            a.mentioned_professional,
            a.mentioned_ideology
        FROM 
            candidates c
        JOIN
            urls u ON c.id = u.candidate_id
        JOIN
            articles a ON u.id = a.url_id
        WHERE
            u.relevance_score > 0
            AND u.language = 'es'
            AND u.spanish_confidence >= ?
    """, (SPANISH_CONFIDENCE_THRESHOLD,))
    
    rows = cursor.fetchall()
    
    if not rows:
        logger.warning("No Spanish records found for full dataset.")
        return
    
    # Create a DataFrame from the query results
    columns = [
        'candidate_id', 'ENTIDAD', 'MUNICIPIO', 'PARTIDO', 'PRESIDENTE_MUNICIPAL',
        'PERIODO_FORMATO_ORIGINAL', 'Year', 'url_id', 'url', 'title', 'source_type',
        'language', 'spanish_confidence', 'date', 'query', 'relevance_score', 'has_candidate_statements',
        'text', 'content_type', 'contains_candidate_name', 'contains_municipality',
        'mentioned_academic', 'mentioned_professional', 'mentioned_ideology'
    ]
    
    full_df = pd.DataFrame(rows, columns=columns)
    
    # Convert boolean flags from integers
    for col in ['has_candidate_statements', 'contains_candidate_name', 'contains_municipality',
                'mentioned_academic', 'mentioned_professional', 'mentioned_ideology']:
        full_df[col] = full_df[col].astype(bool)
    
    # Save as CSV
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    full_csv_path = os.path.join(RESULTS_DIR, f"candidate_full_dataset_{timestamp}.csv")
    full_df.to_csv(full_csv_path, index=False)
    
    logger.info(f"Saved full Spanish dataset with {len(full_df)} records to {full_csv_path}")
    
    # Create dataset of candidate statements
    cursor.execute("""
        SELECT 
            c.id as candidate_id,
            c.entidad,
            c.municipio,
            c.partido,
            c.nombre as presidente_municipal,
            c.year,
            u.url,
            u.title,
            u.date,
            u.source_type,
            u.spanish_confidence,
            s.statement
        FROM 
            candidates c
        JOIN
            urls u ON c.id = u.candidate_id
        JOIN
            articles a ON u.id = a.url_id
        JOIN
            statements s ON a.id = s.article_id
        WHERE
            u.language = 'es'
            AND u.spanish_confidence >= ?
    """, (SPANISH_CONFIDENCE_THRESHOLD,))
    
    statement_rows = cursor.fetchall()
    
    if statement_rows:
        statement_columns = [
            'candidate_id', 'ENTIDAD', 'MUNICIPIO', 'PARTIDO', 'PRESIDENTE_MUNICIPAL',
            'Year', 'url', 'title', 'date', 'source_type', 'spanish_confidence', 'statement'
        ]
        
        statements_df = pd.DataFrame(statement_rows, columns=statement_columns)
        statements_csv_path = os.path.join(RESULTS_DIR, f"candidate_statements_{timestamp}.csv")
        statements_df.to_csv(statements_csv_path, index=False)
        
        logger.info(f"Saved {len(statement_rows)} Spanish candidate statements to {statements_csv_path}")
    else:
        logger.warning("No Spanish candidate statements found.")


def parallel_process_candidates(csv_path, max_workers=DEFAULT_THREADS, chunk_size=50, year_range=DEFAULT_YEAR_RANGE):
    """
    Process candidates in parallel to speed up scraping.
    Only collects Spanish language content.
    
    Args:
        csv_path (str): Path to CSV file containing candidate information
        max_workers (int): Maximum number of parallel workers
        chunk_size (int): Number of candidates to process in each chunk
        year_range (int): Number of years before and after election to include
        
    Returns:
        str: Path to the final consolidated CSV output
    """
    # Load candidates data
    candidates_df = pd.read_csv(csv_path)
    
    # Split the dataframe into chunks
    chunks = [candidates_df.iloc[i:i+chunk_size] for i in range(0, len(candidates_df), chunk_size)]
    
    logger.info(f"Processing {len(candidates_df)} candidates in {len(chunks)} chunks with {max_workers} workers")
    print(f"Processing {len(candidates_df)} candidates in {len(chunks)} chunks with {max_workers} workers")
    
    # Create a database connection
    db_conn = setup_database()
    
    # Create a shared scraper instance
    scraper = OxylabScraper()
    
    # Function to process a chunk of candidates
    def process_chunk(chunk_df, chunk_idx):
        logger.info(f"Starting chunk {chunk_idx+1}/{len(chunks)}")
        print(f"Starting chunk {chunk_idx+1}/{len(chunks)}")
        
        # Create temp directory for this chunk
        temp_dir = os.path.join(PROCESSED_DATA_DIR, f"temp_chunk_{chunk_idx}")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Save chunk to temporary CSV
        chunk_csv = os.path.join(temp_dir, f"chunk_{chunk_idx}.csv")
        chunk_df.to_csv(chunk_csv, index=False)
        
        # Process the chunk
        return process_candidates_file(
            chunk_csv, 
            db_conn=db_conn,
            scraper=scraper,
            year_range=year_range,
            resume=True
        )
    
    # Process chunks in parallel
    chunk_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk = {
            executor.submit(process_chunk, chunk, i): (i, chunk) 
            for i, chunk in enumerate(chunks)
        }
        
        # Instead of using tqdm, track progress manually
        completed_chunks = 0
        total_chunks = len(chunks)
        print(f"Processing chunks: Started (0/{total_chunks})")
        
        for future in as_completed(future_to_chunk):
            chunk_idx, _ = future_to_chunk[future]
            try:
                result_csv = future.result()
                completed_chunks += 1
                print(f"Processing chunks: {completed_chunks}/{total_chunks} ({int(100*completed_chunks/total_chunks)}%)")
                
                if result_csv:
                    chunk_results.append((chunk_idx, result_csv))
                    logger.info(f"Completed chunk {chunk_idx+1}/{len(chunks)}")
                else:
                    logger.warning(f"No results for chunk {chunk_idx+1}/{len(chunks)}")
            except Exception as e:
                completed_chunks += 1
                print(f"Processing chunks: {completed_chunks}/{total_chunks} ({int(100*completed_chunks/total_chunks)}%)")
                logger.error(f"Error processing chunk {chunk_idx+1}: {str(e)}")
    
    # Query the database for the final consolidated results (Spanish only)
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT 
            c.id as candidate_id,
            c.entidad,
            c.municipio,
            c.partido,
            c.nombre as presidente_municipal,
            c.periodo as periodo_formato_original,
            c.year,
            u.url,
            u.title,
            u.source_type,
            u.language,
            u.spanish_confidence,
            u.date,
            u.relevance_score,
            u.has_candidate_statements,
            a.content_type,
            a.contains_candidate_name,
            a.contains_municipality,
            a.mentioned_academic,
            a.mentioned_professional,
            a.mentioned_ideology
        FROM 
            candidates c
        JOIN
            urls u ON c.id = u.candidate_id
        JOIN
            articles a ON u.id = a.url_id
        WHERE
            u.relevance_score > 0
            AND u.language = 'es'
            AND u.spanish_confidence >= ?
    """, (SPANISH_CONFIDENCE_THRESHOLD,))
    
    rows = cursor.fetchall()
    
    if rows:
        # Create a DataFrame from the query results
        columns = [
            'candidate_id', 'ENTIDAD', 'MUNICIPIO', 'PARTIDO', 'PRESIDENTE_MUNICIPAL',
            'PERIODO_FORMATO_ORIGINAL', 'Year', 'url', 'title', 'source_type',
            'language', 'spanish_confidence', 'date', 'relevance_score', 'has_candidate_statements',
            'content_type', 'contains_candidate_name', 'contains_municipality',
            'mentioned_academic', 'mentioned_professional', 'mentioned_ideology'
        ]
        
        final_df = pd.DataFrame(rows, columns=columns)
        
        # Convert boolean flags from integers
        for col in ['has_candidate_statements', 'contains_candidate_name', 'contains_municipality',
                    'mentioned_academic', 'mentioned_professional', 'mentioned_ideology']:
            final_df[col] = final_df[col].astype(bool)
        
        # Save final consolidated CSV
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M')
        final_csv = os.path.join(RESULTS_DIR, f"candidate_web_data_complete_{timestamp}.csv")
        final_df.to_csv(final_csv, index=False)
        
        # Create full dataset
        create_full_dataset(db_conn, final_csv)
        
        logger.info(f"Saved final consolidated Spanish results ({len(final_df)} records) to {final_csv}")
        db_conn.close()
        
        return final_csv
    else:
        logger.warning("No Spanish results were found for any candidates.")
        db_conn.close()
        return None


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape Spanish web information about Mexican political candidates")
    
    parser.add_argument("--input", required=True, help="Path to CSV file containing candidate information")
    parser.add_argument("--output-dir", default=RESULTS_DIR, help="Directory to save scraped data")
    parser.add_argument("--parallel", action="store_true", help="Use parallel processing")
    parser.add_argument("--workers", type=int, default=DEFAULT_THREADS, help="Number of parallel workers")
    parser.add_argument("--chunk-size", type=int, default=50, help="Candidates per chunk for parallel processing")
    parser.add_argument("--no-resume", action="store_true", help="Don't resume from previous run")
    parser.add_argument("--year-range", type=int, default=DEFAULT_YEAR_RANGE, help="Years before/after election to include")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite database")
    parser.add_argument("--spanish-threshold", type=float, default=SPANISH_CONFIDENCE_THRESHOLD, 
                        help="Minimum confidence threshold for Spanish content (0.0-1.0)")
    
    args = parser.parse_args()
    
    # Update Spanish threshold if specified
    if args.spanish_threshold != SPANISH_CONFIDENCE_THRESHOLD:
        SPANISH_CONFIDENCE_THRESHOLD = args.spanish_threshold
        logger.info(f"Spanish confidence threshold set to {SPANISH_CONFIDENCE_THRESHOLD}")
    
    # Update output directory if specified
    if args.output_dir != RESULTS_DIR:
        RESULTS_DIR = args.output_dir
        RAW_DATA_DIR = os.path.join(RESULTS_DIR, "raw_data")
        PROCESSED_DATA_DIR = os.path.join(RESULTS_DIR, "processed_data")
        os.makedirs(RESULTS_DIR, exist_ok=True)
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    logger.info("Starting Mexican municipal candidate web scraping (Spanish content only)")
    logger.info(f"Using Oxylabs proxy with username: {OXYLABS_USERNAME}")
    
    if args.parallel:
        logger.info(f"Using parallel processing with {args.workers} workers")
        result = parallel_process_candidates(
            args.input,
            max_workers=args.workers,
            chunk_size=args.chunk_size,
            year_range=args.year_range
        )
    else:
        logger.info("Using sequential processing")
        db_conn = setup_database(args.db)
        result = process_candidates_file(
            args.input,
            db_conn=db_conn,
            year_range=args.year_range,
            resume=not args.no_resume
        )
        db_conn.close()
    
    if result:
        logger.info(f"Scraping completed successfully. Final output: {result}")
    else:
        logger.error("Scraping failed to produce Spanish results")