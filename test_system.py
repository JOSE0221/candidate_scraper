#!/usr/bin/env python
"""
System test script for Mexican Municipal Candidates Scraper.

This script performs a targeted test of a single candidate to verify
all components are working correctly.
"""
import os
import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from utils.logger import setup_logger, get_logger
from config.settings import DEFAULT_DB_PATH
from database.db_manager import DatabaseManager
from scrapers.oxylabs_manager import OxylabsAPIManager, create_oxylabs_manager
from processing.name_matcher import create_name_matcher
from processing.entity_recognizer import create_entity_recognizer
from processing.content_classifier import create_content_classifier
from scrapers.search_engine import create_search_engine

# Setup logger
logger = setup_logger()

def test_single_candidate(candidate_name, municipality, target_year, 
                          state=None, gender=None, party=None, db_path=DEFAULT_DB_PATH):
    """
    Test the system by processing a single candidate.
    
    Args:
        candidate_name (str): Candidate name
        municipality (str): Municipality
        target_year (int): Election year
        state (str, optional): State
        gender (str, optional): Gender
        party (str, optional): Political party
        db_path (str, optional): Database path
        
    Returns:
        dict: Test results
    """
    results = {
        "success": False,
        "candidate": {
            "name": candidate_name,
            "municipality": municipality,
            "target_year": target_year,
            "state": state,
            "gender": gender,
            "party": party
        },
        "components": {},
        "article_count": 0
    }
    
    try:
        # Initialize database
        logger.info("Initializing database")
        db = DatabaseManager(db_path)
        results["components"]["database"] = {"status": "success"}
        
        # Initialize name matcher
        logger.info("Initializing name matcher")
        name_matcher = create_name_matcher()
        results["components"]["name_matcher"] = {"status": "success"}
        
        # Initialize entity recognizer
        logger.info("Initializing entity recognizer")
        entity_recognizer = create_entity_recognizer(name_matcher=name_matcher)
        results["components"]["entity_recognizer"] = {"status": "success"}
        
        # Initialize content classifier
        logger.info("Initializing content classifier")
        content_classifier = create_content_classifier(name_matcher=name_matcher)
        results["components"]["content_classifier"] = {"status": "success"}
        
        # Initialize Oxylabs
        logger.info("Initializing Oxylabs")
        try:
            oxylabs = create_oxylabs_manager()
            results["components"]["oxylabs"] = {"status": "success"}
        except Exception as e:
            logger.error(f"Oxylabs initialization failed: {str(e)}")
            oxylabs = None
            results["components"]["oxylabs"] = {"status": "error", "message": str(e)}
        
        # Initialize search engine
        logger.info("Initializing search engine")
        search_engine = create_search_engine(
            db, oxylabs,
            content_classifier=content_classifier,
            entity_recognizer=entity_recognizer
        )
        results["components"]["search_engine"] = {"status": "success"}
        
        # Get or create candidate
        logger.info(f"Creating candidate: {candidate_name}, {municipality}, {target_year}")
        candidate, created = db.get_or_create_candidate(
            candidate_name, municipality, target_year,
            state=state, gender=gender, party=party
        )
        
        if not candidate:
            logger.error("Failed to create candidate")
            results["error"] = "Failed to create candidate"
            return results
        
        # Create a test batch
        batch_id = db.create_batch(1, {"test": True})
        
        # Build candidate dictionary
        candidate_dict = {
            "name": candidate_name,
            "municipality": municipality,
            "target_year": target_year,
            "entidad": state,
            "gender": gender,
            "party": party,
            "batch_id": batch_id,
            "id": candidate.id
        }
        
        # Use search engine
        logger.info("Running search")
        article_ids = search_engine.search_candidate_enhanced(candidate_dict, batch_id=batch_id)
        
        results["article_count"] = len(article_ids)
        results["success"] = True
        results["batch_id"] = batch_id
        
        logger.info(f"Test completed successfully. Found {len(article_ids)} articles.")
        
        return results
    except Exception as e:
        import traceback
        logger.error(f"Test failed: {str(e)}")
        logger.debug(traceback.format_exc())
        
        results["error"] = str(e)
        results["traceback"] = traceback.format_exc()
        
        return results

def main():
    """Command line interface for the system test."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Mexican Municipal Candidates Scraper')
    parser.add_argument('--name', required=True, help='Candidate name')
    parser.add_argument('--municipality', required=True, help='Municipality')
    parser.add_argument('--year', type=int, required=True, help='Election year')
    parser.add_argument('--state', help='State')
    parser.add_argument('--gender', choices=['M', 'F'], help='Gender')
    parser.add_argument('--party', help='Political party')
    parser.add_argument('--db', default=DEFAULT_DB_PATH, help='Database path')
    parser.add_argument('--output', help='Output JSON file path')
    
    args = parser.parse_args()
    
    results = test_single_candidate(
        args.name, args.municipality, args.year,
        state=args.state, gender=args.gender, party=args.party,
        db_path=args.db
    )
    
    # Print summary
    status = "SUCCESS" if results["success"] else "FAILURE"
    print(f"{status}: Found {results['article_count']} articles for {args.name}")
    
    # Save results if requested
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")
        
def inspect_search_results(results, output_file=None):
    """
    Inspect search results and optionally save them to a file.
    
    Args:
        results (list): Search results to inspect
        output_file (str, optional): Output file path
    """
    import json
    
    # Print summary
    print(f"Found {len(results)} search results")
    
    # Extract domains
    domains = {}
    for result in results:
        url = result.get('url', '')
        if url:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            domains[domain] = domains.get(domain, 0) + 1
    
    print(f"Domains: {domains}")
    
    # Save to file if requested
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"Search results saved to {output_file}")

if __name__ == "__main__":
    main()