"""
Oxylabs API Manager for the Mexican Municipal Candidates Scraper.

This module handles all interactions with the Oxylabs API for proxy access
and search capabilities, with built-in error handling and session management.
"""
import random
import time
import json
import requests
from urllib.parse import urlparse
import sys
from pathlib import Path

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger
from config.settings import (
    OXYLABS_USERNAME, OXYLABS_PASSWORD, OXYLABS_COUNTRY,
    OXYLABS_API_MODE, USER_AGENTS, RETRY_DELAY, MAX_RETRIES
)

logger = get_logger(__name__)


class OxylabsAPIManager:
    """
    Oxylabs API Manager for proxy and search capabilities with error handling
    and automatic retries.
    """
    
    def __init__(self, username=None, password=None, country=None, api_mode=None):
        """
        Initialize the Oxylabs API manager.
        
        Args:
            username (str, optional): Oxylabs username. Defaults to config value.
            password (str, optional): Oxylabs password. Defaults to config value.
            country (str, optional): Target country code. Defaults to config value.
            api_mode (str, optional): API mode ('direct' or 'realtime'). Defaults to config value.
        """
        self.username = username or OXYLABS_USERNAME
        self.password = password or OXYLABS_PASSWORD
        self.country = country or OXYLABS_COUNTRY
        self.api_mode = api_mode or OXYLABS_API_MODE
        
        # Generate session ID for sticky sessions
        self.session_id = self._generate_session_id()
        
        # Realtime API endpoint
        self.realtime_api_endpoint = 'https://realtime.oxylabs.io/v1/queries'
        
        # Map proxy types to Oxylabs endpoints
        self.proxy_endpoints = {
            'datacenter': 'dc.oxylabs.io:9000',
            'residential': f'customer-{self.username}.pr.oxylabs.io:7777',
            'serp': f'customer-{self.username}.os.oxylabs.io:9000',
        }
        
        # User agents from config
        self.user_agents = USER_AGENTS
        
        logger.info(f"Initialized OxylabsAPIManager with country={self.country}, mode={self.api_mode}")
    
    def _generate_session_id(self):
        """
        Generate a unique session ID for sticky sessions.
        
        Returns:
            str: Unique session ID
        """
        return f"mexscraper_{int(time.time())}_{random.randint(1000, 9999)}"
    
    def get_proxy_url(self, target_domain=None):
        """
        Get a formatted proxy URL for requests.
        
        Args:
            target_domain (str, optional): Target domain for domain-specific routing.
            
        Returns:
            str: Formatted proxy URL
        """
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
        """
        Get proxy dictionary for requests.
        
        Args:
            target_domain (str, optional): Target domain for domain-specific routing.
            
        Returns:
            dict: Proxy dictionary for requests
        """
        proxy_url = self.get_proxy_url(target_domain)
        return {
            'http': proxy_url,
            'https': proxy_url
        }
    
    def realtime_api_request(self, search_query, source='google_search', parse=True, context=None):
        """
        Make a request using Oxylabs Realtime API with enhanced parameters.
        
        Args:
            search_query (str): Search query
            source (str, optional): Source type. Defaults to 'google_search'.
            parse (bool, optional): Whether to parse results. Defaults to True.
            context (dict, optional): Additional context parameters.
            
        Returns:
            dict: API response or error information
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Build a payload following Oxylabs documentation exactly
                payload = {
                    "source": source,
                    "query": search_query,
                    "parse": True,
                    "locale": "es-mx",
                    "geo_location": "Mexico",
                    "user_agent_type": "desktop"
                }
                
                # Add essential parameters for Google search
                if source == 'google_search':
                    payload["pages"] = 1
                    payload["start_page"] = 1
                    payload["domain"] = "com.mx"  # Use Mexican Google domain
                
                logger.info(f"Sending Oxylabs request (attempt {attempt}/{MAX_RETRIES}): {search_query[:50]}...")
                
                # Make the request
                response = requests.request(
                    'POST',
                    self.realtime_api_endpoint,
                    auth=(self.username, self.password),
                    json=payload,
                    timeout=60
                )
                
                # Check for success
                if response.status_code == 200:
                    logger.info(f"Oxylabs API request successful for: {search_query[:50]}...")
                    
                    # Parse response
                    try:
                        json_response = response.json()
                        
                        # Debug the status and structure
                        if 'results' in json_response and json_response['results']:
                            result_count = len(json_response['results'])
                            logger.info(f"Received {result_count} result pages from Oxylabs")
                            
                            # Check if we can see any organic results
                            for page in json_response['results']:
                                if 'content' in page and isinstance(page['content'], dict):
                                    if 'organic' in page['content'] and isinstance(page['content']['organic'], list):
                                        organic_count = len(page['content']['organic'])
                                        logger.info(f"Found {organic_count} total organic results across all pages")
                        
                        return json_response
                    except Exception as e:
                        logger.error(f"Error parsing JSON response: {str(e)}")
                        # Return raw text for debugging
                        return {"error": "JSON parse error", "raw_response": response.text[:500]}
                else:
                    error_msg = f"Oxylabs API error: {response.status_code} - {response.text[:200]}"
                    logger.warning(error_msg)
                    
                    # If we should retry, wait and continue
                    if attempt < MAX_RETRIES:
                        retry_delay = RETRY_DELAY * attempt  # Exponential backoff
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        return {'error': error_msg, 'status_code': response.status_code}
                    
            except Exception as e:
                error_msg = f"Oxylabs API request error: {str(e)}"
                logger.error(error_msg)
                
                if attempt < MAX_RETRIES:
                    retry_delay = RETRY_DELAY * attempt
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    return {'error': error_msg}
        
        # If we've exhausted all retries
        return {'error': 'Max retries exceeded'}
    
    def search(self, query, context=None):
        """
        Perform a search using Oxylabs with enhanced processing and error handling.
        
        Args:
            query (str): Search query
            context (dict, optional): Additional context parameters
            
        Returns:
            list: Search results
        """
        # Try to use the realtime API method
        response = self.realtime_api_request(
            query,
            source='google_search',
            parse=True,
            context=context
        )
        
        if isinstance(response, dict) and 'error' in response:
            logger.warning(f"Oxylabs API error: {response.get('error')}")
            return []
        
        results = []
        
        # Debug full response structure
        try:
            logger.debug(f"Full Oxylabs response structure: {json.dumps(response, indent=2)[:1000]}...")
        except:
            pass
        
        # Extract search results from response with multiple fallback strategies
        try:
            # First check if we have results at all
            if 'results' not in response or not response['results']:
                logger.warning(f"No results array in Oxylabs response for query: {query[:50]}...")
                return []
                
            # Try different possible response structures
            for page_result in response['results']:
                # Log the page result structure to understand what we're working with
                logger.debug(f"Processing result page with keys: {list(page_result.keys())}")
                
                # Strategy 1: Look for content.organic
                if 'content' in page_result and isinstance(page_result['content'], dict):
                    content = page_result['content']
                    logger.debug(f"Content keys: {list(content.keys())}")
                    
                    # Check for organic results
                    if 'organic' in content and isinstance(content['organic'], list):
                        organic_results = content['organic']
                        logger.info(f"Found {len(organic_results)} results in content.organic")
                        
                        for item in organic_results:
                            if not isinstance(item, dict):
                                continue
                                
                            # Extract URL with fallbacks
                            url = item.get('url') or item.get('link')
                            if not url:
                                continue
                                
                            result = {
                                'title': item.get('title', 'No title'),
                                'url': url,
                                'snippet': item.get('description', item.get('snippet', '')),
                                'source': self._extract_domain(url),
                                'position': item.get('position', 0),
                                'oxylabs_used': True
                            }
                            
                            # Skip duplicate URLs
                            if any(r['url'] == result['url'] for r in results):
                                continue
                                
                            results.append(result)
                
                # Strategy 2: Look for organic_results directly
                if 'organic_results' in page_result and isinstance(page_result['organic_results'], list):
                    organic_results = page_result['organic_results']
                    logger.info(f"Found {len(organic_results)} results in organic_results")
                    
                    for item in organic_results:
                        if not isinstance(item, dict):
                            continue
                            
                        # Extract URL with fallbacks
                        url = item.get('url') or item.get('link')
                        if not url:
                            continue
                            
                        result = {
                            'title': item.get('title', 'No title'),
                            'url': url,
                            'snippet': item.get('description', item.get('snippet', '')),
                            'source': self._extract_domain(url),
                            'position': item.get('position', 0),
                            'oxylabs_used': True
                        }
                        
                        # Skip duplicate URLs
                        if any(r['url'] == result['url'] for r in results):
                            continue
                            
                        results.append(result)
                
                # Strategy 3: Direct 'results' key
                if 'results' in page_result and isinstance(page_result['results'], list):
                    organic_results = page_result['results']
                    logger.info(f"Found {len(organic_results)} results in results")
                    
                    for item in organic_results:
                        if not isinstance(item, dict):
                            continue
                            
                        # Extract URL with fallbacks
                        url = item.get('url') or item.get('link')
                        if not url:
                            continue
                            
                        result = {
                            'title': item.get('title', 'No title'),
                            'url': url,
                            'snippet': item.get('description', item.get('snippet', '')),
                            'source': self._extract_domain(url),
                            'position': item.get('position', 0),
                            'oxylabs_used': True
                        }
                        
                        # Skip duplicate URLs
                        if any(r['url'] == result['url'] for r in results):
                            continue
                            
                        results.append(result)
                
                # Strategy 4: Check for any data structure with URLs
                if not results:
                    self._extract_urls_recursively(page_result, results, query)
            
            logger.info(f"Extracted {len(results)} search results from Oxylabs API")
            
        except Exception as parse_error:
            logger.warning(f"Error parsing Oxylabs results: {str(parse_error)}")
            import traceback
            traceback.print_exc()
        
        # If no results found with standard search, try direct search as a fallback
        if not results:
            logger.info(f"No results found with standard search, trying direct approach for: {query[:50]}")
            return self.search_direct_approach(query)
            
        return results
        
    def search_direct_approach(self, query):
        """
        Perform a direct web search with minimal parameters.
        Last resort when normal search fails.
        
        Args:
            query (str): Search query
            
        Returns:
            list: Search results
        """
        try:
            # Simplify the query by removing quotes and extra operators
            simplified_query = query.replace('"', '').replace('OR', '')
            
            # Build absolute minimal payload for Google search
            payload = {
                "source": "google",
                "domain": "com.mx",
                "query": simplified_query,
                "parse": True,
                "geo_location": "Mexico",
                "user_agent_type": "desktop"
            }
            
            logger.info(f"Trying direct search: {simplified_query}")
            
            response = requests.request(
                'POST',
                self.realtime_api_endpoint,
                auth=(self.username, self.password),
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    results = []
                    
                    # Very simplistic parsing - just look for URLs anywhere
                    def extract_urls(obj):
                        if isinstance(obj, dict):
                            for key, value in obj.items():
                                if key in ['url', 'link'] and isinstance(value, str) and value.startswith('http'):
                                    title = obj.get('title', 'No title')
                                    snippet = obj.get('description', obj.get('snippet', ''))
                                    results.append({
                                        'title': title,
                                        'url': value,
                                        'snippet': snippet,
                                        'source': self._extract_domain(value),
                                        'oxylabs_used': True
                                    })
                                elif isinstance(value, (dict, list)):
                                    extract_urls(value)
                        elif isinstance(obj, list):
                            for item in obj:
                                extract_urls(item)
                    
                    extract_urls(data)
                    logger.info(f"Direct search found {len(results)} results")
                    return results
                    
                except Exception as e:
                    logger.error(f"Error processing direct search: {str(e)}")
                    return []
            else:
                logger.warning(f"Direct search failed: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Direct search exception: {str(e)}")
            return []
    
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
    
    def _extract_urls_recursively(self, data, results, query, depth=0, max_depth=3):
        """
        Recursively extract URLs from any data structure.
        
        Args:
            data: Data structure to search
            results: Results list to append to
            query: Original search query
            depth: Current recursion depth
            max_depth: Maximum recursion depth
        """
        if depth > max_depth:
            return
            
        if isinstance(data, dict):
            # Check if this looks like a search result
            if 'url' in data or 'link' in data:
                url = data.get('url') or data.get('link')
                if isinstance(url, str) and url.startswith('http'):
                    # This looks like a result object
                    result = {
                        'title': data.get('title', 'No title'),
                        'url': url,
                        'snippet': data.get('description', data.get('snippet', '')),
                        'source': self._extract_domain(url),
                        'position': data.get('position', 0),
                        'oxylabs_used': True
                    }
                    
                    # Skip duplicate URLs
                    if any(r['url'] == result['url'] for r in results):
                        return
                        
                    results.append(result)
            
            # Recursively check all values
            for key, value in data.items():
                self._extract_urls_recursively(value, results, query, depth + 1, max_depth)
        
        elif isinstance(data, list):
            for item in data:
                self._extract_urls_recursively(item, results, query, depth + 1, max_depth)
    
    def fetch_content(self, url, headers=None, timeout=30):
        """
        Fetch content through Oxylabs with automatic retries and fallbacks.
        Always uses Realtime API instead of direct proxy to avoid connection issues.
        
        Args:
            url (str): URL to fetch
            headers (dict, optional): Custom headers
            timeout (int, optional): Request timeout in seconds
            
        Returns:
            requests.Response or dict: Response object or error information
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Using Oxylabs Realtime API for content extraction: {url}")
                return self._fetch_with_realtime_api(url, timeout)
            except Exception as e:
                error_msg = f"Oxylabs fetch error (attempt {attempt}/{MAX_RETRIES}): {str(e)}"
                logger.warning(error_msg)
                
                # If we should retry, wait and continue
                if attempt < MAX_RETRIES:
                    retry_delay = RETRY_DELAY * attempt  # Exponential backoff
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.warning(f"Maximum retries exceeded for {url}, attempting direct request")
                    # Try making a direct request without the proxy as last resort
                    try:
                        direct_response = requests.get(url, timeout=timeout, headers=headers or {
                            'User-Agent': random.choice(self.user_agents),
                            'Accept': 'text/html,application/xhtml+xml,application/xml',
                            'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3'
                        })
                        return direct_response
                    except Exception as direct_error:
                        return {'error': f"All methods failed: {error_msg}. Direct request error: {str(direct_error)}"}
        
        # If we've exhausted all retries
        return {'error': 'Max retries exceeded'}
    
    def _fetch_with_direct_proxy(self, url, headers=None, timeout=30):
            """
            Fetch content using direct proxy connection.
            
            Args:
                url (str): URL to fetch
                headers (dict, optional): Custom headers
                timeout (int, optional): Request timeout in seconds
                
            Returns:
                requests.Response or dict: Response object or error information
            """
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
                raise
        
        # In scrapers/oxylabs_manager.py, modify the _fetch_with_realtime_api method:

    def _fetch_with_realtime_api(self, url, timeout=30):
        """
        Fetch content using Oxylabs Realtime API with enhanced URL handling.
        
        Args:
            url (str): URL to fetch
            timeout (int, optional): Request timeout in seconds
            
        Returns:
            ResponseLike or dict: Response-like object or error information
        """
        try:
            # Clean and validate URL before sending
            cleaned_url = self._prepare_url_for_api(url)
            
            # Simplified payload for universal endpoint
            payload = {
                'source': 'universal',
                'url': cleaned_url,
                'render': 'html',
                'geo_location': 'Mexico',
                'parse': False  # Get raw HTML instead of parsed content
            }
            
            # Log the exact payload being sent for debugging
            logger.debug(f"Sending Oxylabs Realtime API request with payload: {payload}")
            
            # Make the request
            response = requests.request(
                'POST',
                self.realtime_api_endpoint,
                auth=(self.username, self.password),
                json=payload,
                timeout=timeout
            )
            
            # Check for success
            if response.status_code == 200:
                # Process response as before...
                # ...
            else:
                # Enhanced error logging
                logger.warning(f"Oxylabs Realtime API error: {response.status_code} - {response.text[:500]}")
                logger.warning(f"Failed URL: {cleaned_url}")
                return {'error': f"API error: {response.status_code}", 'url': cleaned_url}
                
        except Exception as e:
            logger.error(f"Oxylabs Realtime API fetch error: {str(e)} for URL: {url}")
            raise

    # Add this new helper method to OxylabsAPIManager
    def _prepare_url_for_api(self, url):
        """
        Prepare URL for API submission with proper encoding and validation.
        
        Args:
            url (str): Original URL
            
        Returns:
            str: Cleaned and properly formatted URL
        """
        # Ensure URL is properly formatted
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Handle URL encoding issues
        from urllib.parse import urlparse, urlunparse, quote
        
        # Parse URL into components
        parsed = urlparse(url)
        
        # Encode path and query parts properly
        path = quote(parsed.path)
        
        # Reassemble the URL
        cleaned_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        
        return cleaned_url
    
    def rotate_session(self):
        """
        Rotate the session ID to get a new IP address.
        
        Returns:
            str: New session ID
        """
        self.session_id = self._generate_session_id()
        logger.info(f"Rotated proxy session to {self.session_id}")
        return self.session_id


def create_oxylabs_manager(username=None, password=None, country=None, api_mode=None):
    """
    Factory function to create an Oxylabs API manager.
    
    Args:
        username (str, optional): Oxylabs username
        password (str, optional): Oxylabs password
        country (str, optional): Target country code
        api_mode (str, optional): API mode
        
    Returns:
        OxylabsAPIManager: Oxylabs API manager instance
    """
    return OxylabsAPIManager(
        username=username,
        password=password,
        country=country,
        api_mode=api_mode
    )