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
        
        # Define site-specific extraction rules for Mexican news sites
        self.site_extraction_rules = {
            'eluniversal.com.mx': {
                'selectors': ['div.field-name-body', 'article.entry', 'div.entry-content']
            },
            'milenio.com': {
                'selectors': ['div.article-body', 'div.content-body', 'div.story-content']
            },
            'proceso.com.mx': {
                'selectors': ['div.entry-content', 'article.post-content', 'div#article-body']
            },
            'jornada.com.mx': {
                'selectors': ['div.article-body', 'div#content-nota']
            },
            'elfinanciero.com.mx': {
                'selectors': ['div.article-body', 'div.main-story']
            },
            'eleconomista.com.mx': {
                'selectors': ['div.entry-content', 'div.SinglePage-content']
            },
            'excelsior.com.mx': {
                'selectors': ['div.entry-content', 'div.notaTexto']
            },
            'reforma.com': {
                'selectors': ['div.article-body', 'div.TextosCont']
            },
            'elsoldemexico.com.mx': {
                'selectors': ['div.newsfull__body', 'div.newsfull-text']
            },
            'informador.mx': {
                'selectors': ['article.entry', 'div.texto']
            },
            'elsiglodetorreon.com.mx': {
                'selectors': ['div.item-content', 'div.nota-content']
            },
            'aristeguinoticias.com': {
                'selectors': ['div.entry-content', 'article.nota-articulo']
            },
            'sinembargo.mx': {
                'selectors': ['div.entry-content', 'div.content-texto']
            },
            'sdpnoticias.com': {
                'selectors': ['article.col-md-12', 'div.newsfull__body']
            },
            'cronica.com.mx': {
                'selectors': ['div.article-body', 'div.txtcontent']
            },
            'oem.com.mx': {
                'selectors': ['div.entry-content', 'div.news-text-content']
            },
            'mediotiempo.com': {
                'selectors': ['div.entry-content', 'div.news__body']
            },
            'mx.reuters.com': {
                'selectors': ['div.article-body', 'div.ArticleBody']
            }
        }
    
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
        Detect and handle the encoding of the response content.
        
        Args:
            response: Response object
            
        Returns:
            str: Decoded text content
        """
        # Auto-detect encoding with chardet if available
        if CHARDET_AVAILABLE:
            try:
                detected = chardet.detect(response.content)
                if detected['confidence'] > 0.7:
                    response.encoding = detected['encoding']
                    return response.text
            except Exception:
                pass
                
        # Try all encoding fallbacks
        for encoding in self.encoding_fallbacks:
            try:
                response.encoding = encoding
                return response.text
            except UnicodeDecodeError:
                continue
        
        # If all encodings fail, use apparent encoding
        response.encoding = response.apparent_encoding
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
            
            # Try site-specific extraction for the domain
            base_domain = self._get_base_domain(domain)
            content = ""
            
            # Check if we have site-specific rules
            if base_domain in self.site_extraction_rules:
                selectors = self.site_extraction_rules[base_domain]['selectors']
                
                for selector in selectors:
                    try:
                        elements = soup.select(selector)
                        if elements:
                            for element in elements:
                                element_text = element.get_text(separator='\n\n', strip=True)
                                if len(element_text) > len(content):
                                    content = element_text
                    except:
                        continue
                        
            # If site-specific extraction failed, use generic extraction
            if not content or len(content) < 200:
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
        Try multiple methods to extract content with improved fallbacks for Mexican news sites.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            url (str): Source URL
            
        Returns:
            str: Extracted content
        """
        content = ""
        
        # Get base domain
        domain = self._extract_domain(url)
        base_domain = self._get_base_domain(domain)
        
        # Method 0: Use site-specific rules if available
        if base_domain in self.site_extraction_rules:
            selectors = self.site_extraction_rules[base_domain]['selectors']
            
            for selector in selectors:
                try:
                    elements = soup.select(selector)
                    if elements:
                        for element in elements:
                            element_text = element.get_text(separator='\n\n', strip=True)
                            if len(element_text) > len(content):
                                content = element_text
                except Exception as e:
                    logger.debug(f"Error with site-specific selector {selector}: {str(e)}")
                    continue
        
        # Method 1: Extract article content
        if not content or len(content) < 200:
            article = soup.find('article')
            if article:
                content = article.get_text(separator='\n\n', strip=True)
        
        # Method 2: Look for main content container
        if not content or len(content) < 200:
            for container in [
                'main', 
                'div[class*="content"]', 
                'div[class*="article"]', 
                'div[id*="content"]', 
                'div[id*="article"]',
                # Add more Mexican news site specific selectors
                'div[class*="nota"]',
                'div[class*="noticia"]',
                'div[class*="entry"]',
                'div[class*="post"]',
                'div.contenido',
                'div.cuerpo',
                'div.texto',
                'div.container-text',
                'div.article-body',
                'div.news-body',
                'div.news-text',
                'div.single-content',
                'div.post-content',
                'div.block-content',
                'div.newsfull__body',
                'div.news-content',
                'div.entry-body',
                'div.texto-nota',
                'div.noticia-contenido',
                'div.content-nota'
            ]:
                try:
                    elements = soup.select(container)
                    for main_content in elements:
                        # Skip very small elements
                        if len(main_content.get_text(strip=True)) < 50:
                            continue
                            
                        # Skip navigation, sidebar, and footer elements
                        if any(cls in str(main_content.get('class', [])).lower() for cls in ['nav', 'menu', 'sidebar', 'footer', 'header', 'comentario']):
                            continue
                            
                        extracted = main_content.get_text(separator='\n\n', strip=True)
                        if len(extracted) > len(content):
                            content = extracted
                except Exception as e:
                    logger.debug(f"Error extracting from {container}: {str(e)}")
                    continue
        
        # Method 3: Extract all paragraphs
        if not content or len(content) < 200:
            paragraphs = []
            
            # Try to find the most relevant container first
            content_containers = []
            
            # Look for semantic content containers
            for container_selector in ['article', 'main', 'div[class*="content"]', 'div[class*="article"]', 'div[class*="body"]']:
                containers = soup.select(container_selector)
                content_containers.extend(containers)
            
            # If we found potential content containers, prioritize paragraphs within them
            p_elements = []
            if content_containers:
                for container in content_containers:
                    p_elements.extend(container.find_all('p'))
            
            # If no paragraphs found in content containers, use all paragraphs
            if not p_elements:
                p_elements = soup.find_all('p')
                
            for p in p_elements:
                try:
                    # Skip elements that are likely not main content
                    parent_classes = str(p.parent.get('class', [])).lower()
                    if any(cls in parent_classes for cls in ['comment', 'sidebar', 'footer', 'menu', 'nav']):
                        continue
                        
                    text = p.get_text(strip=True)
                    # More aggressive filtering for paragraphs
                    if len(text) > 30:  # Skip very short paragraphs
                        paragraphs.append(text)
                except Exception as e:
                    logger.debug(f"Error processing paragraph: {str(e)}")
                    continue
                    
            if paragraphs:
                content = '\n\n'.join(paragraphs)
        
        # Method 4: Advanced text extraction for Mexican news sites
        if (not content or len(content) < 200) and url:
            # Specific extraction for common Mexican news sources
            if 'eluniversal' in domain:
                try:
                    content_div = soup.select_one('div.field-name-body')
                    if content_div:
                        content = content_div.get_text(separator='\n\n', strip=True)
                except:
                    pass
            elif 'proceso' in domain:
                try:
                    content_div = soup.select_one('div.entry-content')
                    if content_div:
                        content = content_div.get_text(separator='\n\n', strip=True)
                except:
                    pass
            elif 'milenio' in domain:
                try:
                    content_div = soup.select_one('div.story-content')
                    if content_div:
                        content = content_div.get_text(separator='\n\n', strip=True)
                except:
                    pass
            elif 'jornada' in domain:
                try:
                    content_div = soup.select_one('div#content-nota')
                    if content_div:
                        content = content_div.get_text(separator='\n\n', strip=True)
                except:
                    pass
            elif 'elfinanciero' in domain:
                try:
                    content_div = soup.select_one('div.article-body')
                    if content_div:
                        content = content_div.get_text(separator='\n\n', strip=True)
                except:
                    pass
        
        # Method 5: If still no good content, get the whole body with better filtering
        if not content or len(content) < 200:
            try:
                body = soup.find('body')
                if body:
                    # Remove non-content elements before extraction
                    for element in body.select('nav, header, footer, aside, .menu, .sidebar, .comments, script, style, meta'):
                        element.decompose()
                    
                    content = body.get_text(separator='\n\n', strip=True)
                    
                    # Basic content cleaning for whole body extraction
                    # Remove common navigation text
                    content = re.sub(r'(Inicio|Home|Principal|Menu|Navegación|Búsqueda|Search|Suscríbete|Subscribe)(\s+[\|»>])?\s*', '', content)
                    
                    # Remove common footer text
                    content = re.sub(r'(Todos los derechos reservados|Copyright|Aviso de Privacidad|Términos y Condiciones).*', '', content)
            except Exception as e:
                logger.debug(f"Error extracting from body: {str(e)}")
            
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
                    if 'dateutil' in sys.modules:
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
                        if 'dateutil' in sys.modules:
                            import dateutil.parser
                            with warnings.catch_warnings():
                                warnings.simplefilter("ignore")
                                parsed_date = dateutil.parser.parse(time_tag['datetime'])
                                return parsed_date.strftime('%Y-%m-%d')
                    except:
                        continue
                elif time_tag.text:
                    try:
                        if 'dateparser' in sys.modules:
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
    
    def _get_base_domain(self, domain):
        """
        Get base domain without subdomains.
        
        Args:
            domain (str): Full domain name
            
        Returns:
            str: Base domain
        """
        parts = domain.split('.')
        if len(parts) > 2:
            # Handle cases like blog.example.com.mx
            if parts[-1] == 'mx':
                return '.'.join(parts[-3:])  # Return example.com.mx
            else:
                return '.'.join(parts[-2:])  # Return example.com
        return domain
    
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
                'Comparte esta noticia',
                'Compartir en Facebook',
                'Compartir en Twitter',
                'Compartir en WhatsApp',
                'Seguir leyendo',
                'Lee también',
                'Te recomendamos',
                'Noticias relacionadas',
                'MÁS INFORMACIÓN',
                'NOTAS RELACIONADAS',
                'TAMBIÉN TE PUEDE INTERESAR',
                'COMENTARIOS',
                'Publicidad',
                'Publicado por',
                'COMPARTE ESTA NOTICIA',
                'ETIQUETAS',
                'Tags:',
                'NEWSLETTER'
            ]
            
            for phrase in boilerplate:
                text = text.replace(phrase, '')
            
            # Remove very short paragraphs (often navigation or ads)
            paragraphs = text.split('\n\n')
            filtered_paragraphs = [p for p in paragraphs if len(p.strip()) > 20]
            text = '\n\n'.join(filtered_paragraphs)
            
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