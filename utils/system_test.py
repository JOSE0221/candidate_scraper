# utils/system_test.py

"""
System testing utilities for the Mexican Municipal Candidates Scraper.

This module provides functions to test the integration of all system components
and verify that they're working together correctly.
"""
import sys
import os
import time
import json
from pathlib import Path
import traceback

# Add the project directory to the path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from utils.logger import get_logger, setup_logger
from config.settings import DEFAULT_DB_PATH
from database.db_manager import DatabaseManager
from scrapers.oxylabs_manager import OxylabsAPIManager
from processing.name_matcher import create_name_matcher
from processing.entity_recognizer import create_entity_recognizer
from processing.content_classifier import create_content_classifier
from scrapers.search_engine import create_search_engine
from scrapers.content_extractor import create_content_extractor

# Setup logger
logger = setup_logger()

class SystemTester:
    """
    System testing class for verifying component integration.
    """
    
    def __init__(self, db_path=DEFAULT_DB_PATH):
        """
        Initialize the system tester.
        
        Args:
            db_path (str): Database path
        """
        self.db_path = db_path
        self.results = {
            "database": {"status": "untested"},
            "oxylabs": {"status": "untested"},
            "name_matcher": {"status": "untested"},
            "entity_recognizer": {"status": "untested"},
            "content_classifier": {"status": "untested"},
            "search_engine": {"status": "untested"},
            "content_extractor": {"status": "untested"}
        }
    
    def test_all_components(self):
        """
        Test all system components in sequence.
        
        Returns:
            dict: Test results
        """
        try:
            logger.info("Starting system component tests")
            
            # Test database
            self.test_database()
            
            # Test Oxylabs
            self.test_oxylabs()
            
            # Test name matcher
            self.test_name_matcher()
            
            # Test entity recognizer
            self.test_entity_recognizer()
            
            # Test content classifier
            self.test_content_classifier()
            
            # Test content extractor
            self.test_content_extractor()
            
            # Test search engine
            self.test_search_engine()
            
            # Generate summary
            self._generate_summary()
            
            return self.results
            
        except Exception as e:
            logger.error(f"System test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            return {"status": "failed", "error": str(e)}
    
    def test_database(self):
        """Test database connectivity and basic operations."""
        try:
            logger.info("Testing database connection")
            self.db = DatabaseManager(self.db_path)
            
            # Test connection
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Check database schema
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Expected tables
            expected_tables = [
                'candidates', 'articles', 'candidate_articles', 'quotes', 
                'entities', 'domain_blacklist'
            ]
            
            # Validate tables
            missing_tables = [table for table in expected_tables if table not in tables]
            
            if missing_tables:
                self.results["database"] = {
                    "status": "warning",
                    "message": f"Missing tables: {', '.join(missing_tables)}",
                    "tables_found": tables
                }
            else:
                # Test writing and reading
                test_successful = True
                try:
                    # Try to add and retrieve from domain blacklist
                    test_domain = "test-system-domain.com"
                    cursor.execute(
                        "INSERT OR IGNORE INTO domain_blacklist (domain, reason, added_date) VALUES (?, ?, ?)",
                        (test_domain, "System test", time.strftime("%Y-%m-%d %H:%M:%S"))
                    )
                    
                    # Check if it worked
                    cursor.execute("SELECT domain FROM domain_blacklist WHERE domain = ?", (test_domain,))
                    result = cursor.fetchone()
                    
                    if not result or result[0] != test_domain:
                        test_successful = False
                        
                    # Clean up
                    cursor.execute("DELETE FROM domain_blacklist WHERE domain = ?", (test_domain,))
                    conn.commit()
                    
                except Exception as db_e:
                    test_successful = False
                    logger.error(f"Database operation test failed: {str(db_e)}")
                
                if test_successful:
                    self.results["database"] = {
                        "status": "success",
                        "message": "Database connection and operations working correctly",
                        "tables_found": tables
                    }
                else:
                    self.results["database"] = {
                        "status": "warning",
                        "message": "Database connection works but operations test failed",
                        "tables_found": tables
                    }
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Database test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results["database"] = {
                "status": "error",
                "message": f"Database test failed: {str(e)}"
            }
    
    def test_oxylabs(self):
        """Test Oxylabs connectivity and search functionality."""
        try:
            from config.settings import OXYLABS_USERNAME, OXYLABS_PASSWORD, OXYLABS_COUNTRY
            
            if not OXYLABS_USERNAME or not OXYLABS_PASSWORD:
                self.results["oxylabs"] = {
                    "status": "skipped",
                    "message": "Oxylabs credentials not configured"
                }
                return
                
            logger.info("Testing Oxylabs connection")
            
            # Initialize Oxylabs
            self.oxylabs = OxylabsAPIManager(OXYLABS_USERNAME, OXYLABS_PASSWORD, country=OXYLABS_COUNTRY)
            
            # Test simple search
            test_query = "test oxylabs connection"
            response = self.oxylabs.realtime_api_request(test_query, source='google_search')
            
            if isinstance(response, dict) and 'error' in response:
                self.results["oxylabs"] = {
                    "status": "error",
                    "message": f"Oxylabs API error: {response.get('error')}"
                }
                return
                
            if 'results' in response and response['results']:
                self.results["oxylabs"] = {
                    "status": "success",
                    "message": "Oxylabs API connection successful"
                }
            else:
                self.results["oxylabs"] = {
                    "status": "warning",
                    "message": "Oxylabs API connected but returned no results"
                }
                
        except Exception as e:
            logger.error(f"Oxylabs test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results["oxylabs"] = {
                "status": "error",
                "message": f"Oxylabs test failed: {str(e)}"
            }
    
    def test_name_matcher(self):
        """Test name matcher functionality."""
        try:
            logger.info("Testing name matcher")
            
            # Initialize name matcher
            self.name_matcher = create_name_matcher()
            
            # Test with sample names
            test_cases = [
                {
                    "text": "El presidente municipal Juan Pérez González comentó sobre el proyecto.",
                    "name": "Juan Pérez González",
                    "should_match": True
                },
                {
                    "text": "La alcaldesa María Rodríguez inauguró la nueva plaza.",
                    "name": "Juan Pérez González",
                    "should_match": False
                }
            ]
            
            results = []
            for case in test_cases:
                found, score, matched_text = self.name_matcher.fuzzy_match_name(
                    case["text"], case["name"]
                )
                results.append({
                    "case": case,
                    "found": found,
                    "score": score,
                    "matched_text": matched_text,
                    "success": found == case["should_match"]
                })
            
            success_count = sum(1 for r in results if r["success"])
            
            if success_count == len(test_cases):
                self.results["name_matcher"] = {
                    "status": "success",
                    "message": "Name matcher working correctly",
                    "test_results": results
                }
            else:
                self.results["name_matcher"] = {
                    "status": "warning",
                    "message": f"Name matcher passed {success_count}/{len(test_cases)} tests",
                    "test_results": results
                }
                
        except Exception as e:
            logger.error(f"Name matcher test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results["name_matcher"] = {
                "status": "error",
                "message": f"Name matcher test failed: {str(e)}"
            }
    
    def test_entity_recognizer(self):
        """Test entity recognizer functionality."""
        try:
            logger.info("Testing entity recognizer")
            
            # Create entity recognizer
            self.entity_recognizer = create_entity_recognizer(name_matcher=self.name_matcher)
            
            # Test sample text
            sample_text = "Juan Pérez fue elegido como alcalde de Monterrey en 2018. El PRI anunció su candidatura en enero."
            
            # Extract entities
            entities = self.entity_recognizer.extract_entities(
                sample_text, 
                candidate_name="Juan Pérez",
                municipality="Monterrey"
            )
            
            # Test date extraction
            extracted_date = self.entity_recognizer.extract_date_from_text(sample_text)
            
            # Check if key entities were found
            found_person = "Juan Pérez" in str(entities.get('PERSON', {}))
            found_party = "PRI" in str(entities.get('POLITICAL_PARTY', {}))
            found_location = "Monterrey" in str(entities.get('LOCATION', {}))
            
            if found_person and found_party and found_location and extracted_date:
                self.results["entity_recognizer"] = {
                    "status": "success",
                    "message": "Entity recognizer working correctly",
                    "entities": entities,
                    "extracted_date": extracted_date
                }
            else:
                missing = []
                if not found_person: missing.append("PERSON")
                if not found_party: missing.append("POLITICAL_PARTY")  
                if not found_location: missing.append("LOCATION")
                if not extracted_date: missing.append("DATE")
                
                self.results["entity_recognizer"] = {
                    "status": "warning",
                    "message": f"Entity recognizer partially working. Missing: {', '.join(missing)}",
                    "entities": entities,
                    "extracted_date": extracted_date
                }
                
        except Exception as e:
            logger.error(f"Entity recognizer test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results["entity_recognizer"] = {
                "status": "error",
                "message": f"Entity recognizer test failed: {str(e)}"
            }
    
    def test_content_classifier(self):
        """Test content classifier functionality."""
        try:
            logger.info("Testing content classifier")
            
            # Create content classifier
            self.content_classifier = create_content_classifier(name_matcher=self.name_matcher)
            
            # Test sample texts
            test_cases = [
                {
                    "text": "MONTERREY, Nuevo León.- Juan Pérez declaró ayer en rueda de prensa que buscará la reelección. \"Estamos trabajando para mejorar la ciudad\", dijo el alcalde.",
                    "expected_type": "news",
                    "should_find_quotes": True
                },
                {
                    "text": "El análisis de la gestión de Juan Pérez en Monterrey muestra resultados mixtos. Por un lado, ha mejorado la infraestructura, pero los índices de seguridad siguen siendo preocupantes.",
                    "expected_type": "article",
                    "should_find_quotes": False
                }
            ]
            
            results = []
            for case in test_cases:
                classification = self.content_classifier.classify_content(
                    case["text"], 
                    candidate_name="Juan Pérez"
                )
                
                content_type_correct = classification["content_type"] == case["expected_type"]
                quotes_correct = (len(classification["quotes"]) > 0) == case["should_find_quotes"]
                
                results.append({
                    "case": case,
                    "classification": classification,
                    "content_type_correct": content_type_correct,
                    "quotes_correct": quotes_correct,
                    "success": content_type_correct and quotes_correct
                })
            
            success_count = sum(1 for r in results if r["success"])
            
            if success_count == len(test_cases):
                self.results["content_classifier"] = {
                    "status": "success",
                    "message": "Content classifier working correctly",
                    "test_results": results
                }
            else:
                self.results["content_classifier"] = {
                    "status": "warning",
                    "message": f"Content classifier passed {success_count}/{len(test_cases)} tests",
                    "test_results": results
                }
                
        except Exception as e:
            logger.error(f"Content classifier test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results["content_classifier"] = {
                "status": "error",
                "message": f"Content classifier test failed: {str(e)}"
            }
    
    def test_content_extractor(self):
        """Test content extractor functionality."""
        try:
            logger.info("Testing content extractor")
            
            # Create content extractor
            self.content_extractor = create_content_extractor(
                self.db,
                self.oxylabs if hasattr(self, 'oxylabs') else None,
                self.entity_recognizer
            )
            
            # Test URL - use a reliable Mexican news site
            test_url = "https://www.eluniversal.com.mx/"
            
            # Extract content
            extraction = self.content_extractor.extract_content(
                test_url,
                use_oxylabs=hasattr(self, 'oxylabs')
            )
            
            if extraction.get('success') and extraction.get('content'):
                self.results["content_extractor"] = {
                    "status": "success",
                    "message": "Content extractor working correctly",
                    "title_length": len(extraction.get('title', '')),
                    "content_length": len(extraction.get('content', '')),
                    "language": extraction.get('language')
                }
            else:
                self.results["content_extractor"] = {
                    "status": "warning",
                    "message": f"Content extractor connected but failed to extract content: {extraction.get('error')}",
                    "extraction": {k: v for k, v in extraction.items() if k != 'html_content'} 
                }
                
        except Exception as e:
            logger.error(f"Content extractor test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results["content_extractor"] = {
                "status": "error",
                "message": f"Content extractor test failed: {str(e)}"
            }
    
    def test_search_engine(self):
        """Test search engine functionality."""
        try:
            logger.info("Testing search engine")
            
            # Create search engine
            self.search_engine = create_search_engine(
                self.db,
                self.oxylabs if hasattr(self, 'oxylabs') else None,
                self.content_classifier,
                self.entity_recognizer
            )
            
            # Skip full search test if no Oxylabs
            if not hasattr(self, 'oxylabs') or self.results.get("oxylabs", {}).get("status") != "success":
                self.results["search_engine"] = {
                    "status": "skipped",
                    "message": "Search engine test skipped due to missing or failed Oxylabs"
                }
                return
            
            # Simple search test
            test_candidate = {
                "name": "test candidate",
                "municipality": "test city",
                "target_year": 2020
            }
            
            # Use the low-level search method directly rather than the high-level candidate search
            query = f"{test_candidate['name']} {test_candidate['municipality']} {test_candidate['target_year']}"
            search_results = self.oxylabs.search(query)
            
            if search_results and len(search_results) > 0:
                self.results["search_engine"] = {
                    "status": "success",
                    "message": "Search engine working correctly",
                    "result_count": len(search_results)
                }
            else:
                self.results["search_engine"] = {
                    "status": "warning",
                    "message": "Search engine connected but returned no results",
                    "result_count": 0
                }
                
        except Exception as e:
            logger.error(f"Search engine test failed: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results["search_engine"] = {
                "status": "error",
                "message": f"Search engine test failed: {str(e)}"
            }
    
    def _generate_summary(self):
        """Generate a summary of all test results."""
        # Count results by status
        status_counts = {
            "success": 0,
            "warning": 0,
            "error": 0,
            "skipped": 0,
            "untested": 0
        }
        
        for component, result in self.results.items():
            if component != "summary":
                status = result.get("status", "untested")
                status_counts[status] += 1
        
        # Determine overall status
        if status_counts["error"] > 0:
            overall_status = "error"
        elif status_counts["warning"] > 0:
            overall_status = "warning"
        elif status_counts["success"] + status_counts["skipped"] == len(self.results) - 1:  # Exclude summary
            overall_status = "success"
        else:
            overall_status = "incomplete"
        
        self.results["summary"] = {
            "status": overall_status,
            "message": f"System test completed with {status_counts['success']} successful, {status_counts['warning']} warnings, {status_counts['error']} errors, {status_counts['skipped']} skipped",
            "component_counts": status_counts
        }

def run_system_test(db_path=DEFAULT_DB_PATH, output_file=None):
    """
    Run a comprehensive system test and optionally save results to file.
    
    Args:
        db_path (str, optional): Database path
        output_file (str, optional): Output file path for results
        
    Returns:
        dict: Test results
    """
    tester = SystemTester(db_path)
    results = tester.test_all_components()
    
    # Print summary to console
    summary = results.get("summary", {})
    status = summary.get("status", "unknown")
    message = summary.get("message", "No summary available")
    
    status_colors = {
        "success": "\033[92m",  # Green
        "warning": "\033[93m",  # Yellow
        "error": "\033[91m",    # Red
        "incomplete": "\033[94m",  # Blue
        "unknown": "\033[0m"    # Default
    }
    
    print(f"{status_colors.get(status, '')}{status.upper()}: {message}\033[0m")
    
    # Save to file if requested
    if output_file:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"Test results saved to {output_file}")
    
    return results

# Command line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run system tests for Mexican Municipal Candidates Scraper')
    parser.add_argument('--db', type=str, default=DEFAULT_DB_PATH, help='Database path')
    parser.add_argument('--output', type=str, default=None, help='Output file path')
    
    args = parser.parse_args()
    
    run_system_test(args.db, args.output)