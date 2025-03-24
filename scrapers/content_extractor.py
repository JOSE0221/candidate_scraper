"""
Content extractor for the Mexican Municipal Candidates Scraper.

This module handles extracting content from web pages with intelligent
fallbacks, encoding detection, and robust error handling.
"""
import re
import random
import requests
import sys
from pathlib import Path
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import traceback
import time
from concurrent.futures import ThreadPoolExecutor

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger
from config.settings import USER_AGENTS, MAX_RETRIES, RETRY_DELAY
from processing.entity_recognizer import create_entity_recognizer

logger = get_logger(__name__)

# Try to import optional dependencies
try:
    import chardet
    CHARDET_AVAILABLE = True
except ImportError:
    CHARDET_AVAILABLE = False
    logger.warning("chardet not available. Install with 'pip install chardet' for better encoding detection.")

try:
    from langdetect import detect
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False
    logger.warning("langdetect not available. Install with 'pip install langdetect' for better language detection.")


class ContentExtractor:
    """
    Enhanced content extractor with improved text handling, fallback mechanisms,
    and content analysis capabilities.
    """
    
    def __init__(self, db_manager, oxylabs_manager=None, entity_recognizer=None):
        """
        Initialize the content extractor.
        
        Args:
            db_manager: Database manager for caching and stats
            oxylabs_manager (optional): Oxylabs API manager for proxy access
            entity_recognizer (optional): Entity recognizer for content analysis
        """
        self.db = db_manager
        self.oxylabs = oxylabs_manager
        self.max_retries = MAX_RETRIES
        
        # Create entity recognizer if not provided
        self.entity_recognizer = entity_recognizer or create_entity_recognizer()
        
        # Language detection function
        self.detect_language = detect if LANGDETECT_AVAILABLE else lambda text: 'es'
        
        # User agent rotation from config
        self.user_agents = USER_AGENTS
        
        # Encoding fallbacks
        self.encoding_fallbacks = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'windows-1252', 'iso-8859-15']
        
        # HTTP session with retry capability
        self.session = self._create_session()
    
    def _create_session(self):
        """
        Create a requests session with custom adapters and retry capability.
        
        Returns:
            requests.Session: Configured session
        """
        session = requests.Session()
        
        # Set up adapter with retry capability
        adapter = requests.adapters.HTTPAdapter(
            max_retries=self.max_retries,
            pool_connections=10,
            pool_maxsize=20
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def extract_content(self, url, candidate_name=None, target_year=None, 
                      year_range=2, use_oxylabs=True):
        """
        Extract content from a URL with multiple fallback methods and content analysis.
        
        Args:
            url (str): URL to extract content from
            candidate_name (str, optional): Candidate name for relevance analysis
            target_year (int, optional): Target election year
            year_range (int, optional): Year range for temporal filtering
            use_oxylabs (bool, optional): Whether to use Oxylabs proxy
            
        Returns:
            dict: Extracted content with metadata
        """
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
                'error': f'Domain {domain} is blacklisted'
            }
        
        # Check cache first
        cached_content = self.db.get_cached_content(url)
        if cached_content:
            logger.info(f"Using cached content for {url}")
            
            # Extract date if needed
            extracted_date = cached_content.get('extracted_date')
            if not extracted_date and cached_content.get('content'):
                # Try to extract date from content
                extracted_date = self.entity_recognizer.extract_date_from_text(cached_content.get('content'))
                
                # Update cache with extracted date if found
                if extracted_date:
                    self.db.cache_content(
                        url, 
                        cached_content.get('title', ''), 
                        cached_content.get('content', ''),
                        extracted_date,
                        cached_content.get('html_content', ''),
                        cached_content.get('language', 'es')
                    )
            
            return {
                'success': True,
                'title': cached_content.get('title', ''),
                'content': cached_content.get('content', ''),
                'html_content': cached_content.get('html_content', ''),
                'extracted_date': extracted_date,
                'language': cached_content.get('language', 'es'),
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
                    response = self._fetch_with_session(url, headers)
            else:
                # Direct request
                response = self._fetch_with_session(url, headers)
            
            # Check if response is valid
            if isinstance(response, dict) and 'error' in response:
                logger.warning(f"Request failed: {response['error']}")
                # Update domain statistics
                self.db.update_domain_stats(domain, success=False)
                
                # Try fallback extraction
                return self._extract_content_fallback(url, candidate_name, target_year)
            
            # Try to detect encoding correctly
            text_content = self._detect_encoding(response)
            
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
                extracted_date = self.entity_recognizer.extract_date_from_text(content)
                
            # If still no date, try from title and URL
            if not extracted_date:
                extracted_date = self.entity_recognizer.extract_date_from_text(title)
                
            if not extracted_date:
                # Try to extract from the URL itself
                extracted_date = self.entity_recognizer.extract_date_from_text(url)
                
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
            language = self._detect_language(content, title)
            
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
                return self._extract_content_fallback(url, candidate_name, target_year)
    
    def _fetch_with_session(self, url, headers=None):
        """
        Fetch content with session and retry mechanism.
        
        Args:
            url (str): URL to fetch
            headers (dict, optional): Request headers
            
        Returns:
            requests.Response or dict: Response object or error information
        """
        retry_delay = RETRY_DELAY
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url, 
                    headers=headers, 
                    timeout=30,
                    allow_redirects=True
                )
                return response
            except Exception as e:
                error_msg = f"Request failed (attempt {attempt}/{self.max_retries}): {str(e)}"
                logger.warning(error_msg)
                
                if attempt < self.max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 1.5  # Exponential backoff
                else:
                    return {'error': str(e)}
    
    def _detect_encoding(self, response):
        """
        Detect and handle encoding with improved fallbacks.
        
        Args:
            response: Response object
            
        Returns:
            str: Decoded text content
        """
        # Try content-type header first
        if 'content-type' in response.headers:
            content_type = response.headers['content-type']
            if 'charset=' in content_type:
                charset = content_type.split('charset=')[-1].strip()
                try:
                    response.encoding = charset
                    return response.text
                except:
                    pass
        
        # Auto-detect encoding with chardet if available
        if CHARDET_AVAILABLE:
            try:
                detected = chardet.detect(response.content)
                if detected['confidence'] > 0.6:  # Lower confidence threshold
                    response.encoding = detected['encoding']
                    return response.text
            except Exception:
                pass
        
        # Try common encodings for Spanish/Latin American content
        for encoding in ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'windows-1252', 'iso-8859-15']:
            try:
                response.encoding = encoding
                return response.text
            except UnicodeDecodeError:
                continue
        
        # Try to extract encoding from HTML meta tags
        try:
            # Use a minimal parser for just getting the meta tag
            from html.parser import HTMLParser
            
            class MetaParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.encoding = None
                    
                def handle_starttag(self, tag, attrs):
                    if tag.lower() == 'meta':
                        attr_dict = dict(attrs)
                        if 'charset' in attr_dict:
                            self.encoding = attr_dict['charset']
                        elif 'http-equiv' in attr_dict and attr_dict['http-equiv'].lower() == 'content-type':
                            if 'content' in attr_dict and 'charset=' in attr_dict['content'].lower():
                                self.encoding = attr_dict['content'].lower().split('charset=')[-1].strip()
            
            parser = MetaParser()
            parser.feed(response.content.decode('ascii', errors='ignore')[:1000])
            
            if parser.encoding:
                try:
                    response.encoding = parser.encoding
                    return response.text
                except:
                    pass
        except:
            pass
        
        # If all encodings fail, use apparent encoding or UTF-8 as last resort
        response.encoding = response.apparent_encoding or 'utf-8'
        return response.text
    
    def _detect_language(self, content, title=None):
        """
        Detect the language of the content.
        
        Args:
            content (str): Main content text
            title (str, optional): Title text
            
        Returns:
            str: Detected language code
        """
        language = None
        try:
            # Try to detect language from content
            if LANGDETECT_AVAILABLE:
                if len(content) > 50:
                    language = self.detect_language(content[:2000])
                elif title and len(title) > 10:
                    language = self.detect_language(title)
        except:
            # Default to Spanish
            pass
            
        return language or 'es'
    
    def _extract_content_fallback(self, url, candidate_name=None, target_year=None):
        """
        Fallback content extraction method with enhanced error handling.
        
        Args:
            url (str): URL to extract content from
            candidate_name (str, optional): Candidate name
            target_year (int, optional): Target election year
            
        Returns:
            dict: Extracted content with metadata
        """
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
                        response = self._fetch_with_session(url, headers)
                except:
                    # Direct request
                    response = self._fetch_with_session(url, headers)
            else:
                # Direct request
                response = self._fetch_with_session(url, headers)
            
            # Check if response is valid
            if isinstance(response, dict) and 'error' in response:
                logger.warning(f"Fallback request failed: {response['error']}")
                # Return error info
                return {
                    'success': False,
                    'title': '',
                    'content': '',
                    'html_content': '',
                    'extracted_date': None,
                    'language': None,
                    'error': response['error']
                }
            
            # Try different encodings
            html_content = self._detect_encoding(response)
            
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
                extracted_date = self.entity_recognizer.extract_date_from_text(content[:5000])
                
            # If no date and target year is provided, look for target year mentions
            if not extracted_date and target_year:
                if str(target_year) in content or str(target_year) in title:
                    extracted_date = f"{target_year}-01-01"
            
            # Detect language
            language = self._detect_language(content, title)
            
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
                'error': f'Fallback extraction failed: {str(e)}'
            }
    
    def _extract_content_with_methods(self, soup, url):
        """
        Try multiple methods to extract content with improved selectors.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            url (str): Source URL
            
        Returns:
            str: Extracted content
        """
        content = ""
        domain = self._extract_domain(url)
        
        # Method 1: Extract article content with improved targeting
        article_selectors = ['article', 'div.article', 'div.post', 'div.entry', 'div.nota', 'div.noticia']
        for selector in article_selectors:
            try:
                articles = soup.select(selector)
                if articles:
                    # Take the longest article content if multiple are found
                    best_article = max(articles, key=lambda x: len(x.get_text()))
                    article_content = best_article.get_text(separator='\n\n', strip=True)
                    if len(article_content) > len(content):
                        content = article_content
            except:
                continue
        
        # Method 2: Look for main content container with enhanced selectors
        if not content or len(content) < 200:
            for container in [
                'main', 'div[class*="content"]', 'div[class*="article"]', 
                'div[id*="content"]', 'div[id*="article"]',
                # Additional selectors for Mexican news sites
                'div[class*="nota"]', 'div[class*="noticia"]', 'div[class*="entry"]',
                'div.article-body', 'div.post-content', 'div.entry-content',
                # Common Mexican news site selectors
                'div.nota-texto', 'div.contenido-nota', 'div.texto-nota',
                'div.cuerpo-nota', 'div.entry-content', 'div.node-body'
            ]:
                try:
                    main_content = soup.select_one(container)
                    if main_content:
                        extracted = main_content.get_text(separator='\n\n', strip=True)
                        if len(extracted) > len(content):
                            content = extracted
                except:
                    continue
        
        # Method 3: Extract all paragraphs with improved filtering
        if not content or len(content) < 200:
            paragraphs = []
            for p in soup.find_all('p'):
                try:
                    text = p.get_text(strip=True)
                    if len(text) > 10:  # Accept shorter paragraphs
                        # Skip navigation/footer paragraphs
                        if any(term in p.get('class', []) for term in ['nav', 'foot', 'copy', 'menu']):
                            continue
                        paragraphs.append(text)
                except:
                    continue
            if paragraphs:
                content = '\n\n'.join(paragraphs)
        
        # Method 4: Extract content from divs with relevant classes/IDs
        if not content or len(content) < 200:
            try:
                relevant_classes = ['content', 'article', 'text', 'body', 'entry', 'nota', 'noticia',
                                'post', 'main', 'story', 'contenido', 'texto']
                for div in soup.find_all('div'):
                    div_classes = ' '.join(div.get('class', [])).lower()
                    div_id = (div.get('id', '') or '').lower()
                    
                    # Check if any relevant term is in class or ID
                    if any(cls in div_classes for cls in relevant_classes) or any(cls in div_id for cls in relevant_classes):
                        text = div.get_text(separator='\n\n', strip=True)
                        if len(text) > len(content):
                            content = text
            except:
                pass
        
        # Method 5: Domain-specific extraction for common Mexican news sites
        if not content or len(content) < 200:
            # El Universal
            if 'eluniversal' in domain:
                universal_content = soup.select_one('div.field-name-body')
                if universal_content:
                    content = universal_content.get_text(separator='\n\n', strip=True)
            # Milenio
            elif 'milenio' in domain:
                milenio_content = soup.select_one('div.article-body')
                if milenio_content:
                    content = milenio_content.get_text(separator='\n\n', strip=True)
            # Proceso
            elif 'proceso' in domain:
                proceso_content = soup.select_one('div.cp-content')
                if proceso_content:
                    content = proceso_content.get_text(separator='\n\n', strip=True)
            # El Financiero
            elif 'elfinanciero' in domain:
                financiero_content = soup.select_one('div.field-name-body')
                if financiero_content:
                    content = financiero_content.get_text(separator='\n\n', strip=True)
            # Animal Político
            elif 'animalpolitico' in domain:
                animal_content = soup.select_one('div.ap-content')
                if animal_content:
                    content = animal_content.get_text(separator='\n\n', strip=True)
        
        # Method 6: If still no good content, get the whole body but remove obvious navigation/footer
        if not content or len(content) < 200:
            try:
                body = soup.find('body')
                if body:
                    # Remove navigation, header, footer and other non-content elements
                    for non_content in body.select('nav, footer, header, aside, .menu, .nav, .footer, .header, .sidebar, script, style'):
                        non_content.decompose()
                    
                    content = body.get_text(separator='\n\n', strip=True)
            except:
                pass
        
        # If content is too short, try a very basic approach
        if not content or len(content) < 100:
            try:
                content = soup.get_text(separator='\n\n', strip=True)
            except:
                pass
        
        return content
    
    def _extract_date_from_meta(self, soup):
        """
        Extract publication date from meta tags.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            str or None: Extracted date in ISO format, or None if not found
        """
        # Common meta tags for publication dates
        date_meta_names = [
            'article:published_time', 'datePublished', 'pubdate', 'date', 
            'DC.date.issued', 'article:modified_time', 'og:published_time',
            'publication-date', 'release_date', 'fecha', 'publication',
            'publish-date', 'lastmod', 'created', 'modified',
            'fecha-publicacion', 'fecha_publicacion', 'date-publication'
        ]
        
        import warnings
        
        for name in date_meta_names:
            # Try to find the meta tag
            try:
                meta_tag = soup.find('meta', {'property': name}) or soup.find('meta', {'name': name})
                if meta_tag and meta_tag.get('content'):
                    # Parse and format the date
                    if DATEUTIL_AVAILABLE:
                        import dateutil.parser
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
                        if DATEUTIL_AVAILABLE:
                            import dateutil.parser
                            with warnings.catch_warnings():
                                warnings.simplefilter("ignore")
                                parsed_date = dateutil.parser.parse(time_tag['datetime'])
                                return parsed_date.strftime('%Y-%m-%d')
                    except:
                        continue
                elif time_tag.text:
                    try:
                        if DATEPARSER_AVAILABLE:
                            import dateparser
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
    
    def _extract_domain(self, url):
        """
        Extract domain from URL.
        
        Args:
            url (str): URL
            
        Returns:
            str: Domain name
        """
        try:
            domain = urlparse(url).netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ''
    
    def clean_text(self, text):
        """
        Clean extracted text.
        
        Args:
            text (str): Text to clean
            
        Returns:
            str: Cleaned text
        """
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


def create_content_extractor(db_manager, oxylabs_manager=None, entity_recognizer=None):
    """
    Factory function to create a content extractor.
    
    Args:
        db_manager: Database manager
        oxylabs_manager (optional): Oxylabs API manager
        entity_recognizer (optional): Entity recognizer
        
    Returns:
        ContentExtractor: Content extractor instance
    """
    return ContentExtractor(
        db_manager=db_manager,
        oxylabs_manager=oxylabs_manager,
        entity_recognizer=entity_recognizer
    )