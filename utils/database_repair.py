# Create a new file utils/database_repair.py

"""
Database repair utilities for the Mexican Municipal Candidates Scraper.
"""
import os
import sys
import argparse
from pathlib import Path

# Add the project directory to the path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from utils.logger import get_logger, setup_logger
from database.db_manager import DatabaseManager
from config.settings import DEFAULT_DB_PATH

logger = setup_logger()

def repair_database(db_path=DEFAULT_DB_PATH, repair_type='all'):
    """
    Repair common database issues.
    
    Args:
        db_path (str): Path to the database file
        repair_type (str): Type of repair to perform ('all', 'constraints', 'orphans', 'stats')
        
    Returns:
        dict: Repair results
    """
    logger.info(f"Starting database repair for {db_path}")
    
    # Create database manager
    db = DatabaseManager(db_path)
    
    results = {}
    
    if repair_type in ['all', 'constraints']:
        logger.info("Repairing database constraints")
        # First, check and repair foreign key violations
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Check foreign key constraints
        cursor.execute("PRAGMA foreign_key_check")
        fk_violations = cursor.fetchall()
        
        if fk_violations:
            logger.warning(f"Found {len(fk_violations)} foreign key violations")
            results['fk_violations'] = len(fk_violations)
            
            # Handle violations by removing orphaned records
            for violation in fk_violations:
                table, rowid, parent, fk = violation
                logger.info(f"Fixing violation in {table} (rowid={rowid}) referencing {parent}")
                cursor.execute(f"DELETE FROM {table} WHERE rowid = ?", (rowid,))
            
            conn.commit()
            logger.info("Fixed foreign key violations")
        else:
            logger.info("No foreign key violations found")
            results['fk_violations'] = 0
        
        conn.close()
    
    if repair_type in ['all', 'orphans']:
        logger.info("Repairing orphaned links")
        # Repair orphaned links
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Remove candidate_articles with missing candidates or articles
        cursor.execute("""
            DELETE FROM candidate_articles 
            WHERE candidate_id NOT IN (SELECT id FROM candidates)
            OR article_id NOT IN (SELECT id FROM articles)
        """)
        orphaned_links = cursor.rowcount
        logger.info(f"Removed {orphaned_links} orphaned candidate-article links")
        results['orphaned_links'] = orphaned_links
        
        # Remove quotes with missing articles or candidates
        cursor.execute("""
            DELETE FROM quotes
            WHERE article_id NOT IN (SELECT id FROM articles)
            OR candidate_id NOT IN (SELECT id FROM candidates)
        """)
        orphaned_quotes = cursor.rowcount
        logger.info(f"Removed {orphaned_quotes} orphaned quotes")
        results['orphaned_quotes'] = orphaned_quotes
        
        conn.commit()
        conn.close()
    
    if repair_type in ['all', 'stats']:
        logger.info("Repairing domain statistics")
        # Repair domain statistics table
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get all domains with duplicate entries
        cursor.execute("""
            SELECT domain, COUNT(*) as count
            FROM domain_stats
            GROUP BY domain
            HAVING count > 1
        """)
        duplicates = cursor.fetchall()
        
        stats_fixed = 0
        if duplicates:
            logger.warning(f"Found {len(duplicates)} domains with duplicate stats")
            
            for domain_data in duplicates:
                domain = domain_data[0]
                logger.info(f"Fixing duplicate stats for domain: {domain}")
                
                # Get all entries for this domain
                cursor.execute("SELECT * FROM domain_stats WHERE domain = ?", (domain,))
                entries = cursor.fetchall()
                
                # Calculate combined stats
                success_count = sum(entry['success_count'] for entry in entries)
                failure_count = sum(entry['failure_count'] for entry in entries)
                
                # Calculate average content length
                total_success = sum(entry['success_count'] for entry in entries)
                weighted_length_sum = sum(entry['avg_content_length'] * entry['success_count'] for entry in entries if entry['success_count'] > 0)
                avg_content_length = weighted_length_sum / total_success if total_success > 0 else 0
                
                # Get latest timestamp
                timestamps = [entry['last_updated'] for entry in entries if entry['last_updated']]
                latest_timestamp = max(timestamps) if timestamps else None
                
                # Delete all entries for this domain
                cursor.execute("DELETE FROM domain_stats WHERE domain = ?", (domain,))
                
                # Insert the consolidated entry
                cursor.execute("""
                    INSERT INTO domain_stats 
                    (domain, success_count, failure_count, avg_content_length, last_updated, is_spanish)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (domain, success_count, failure_count, avg_content_length, latest_timestamp, True))
                
                stats_fixed += 1
        
            conn.commit()
            logger.info(f"Fixed {stats_fixed} domain stat entries")
        else:
            logger.info("No duplicate domain stats found")
        
        results['stats_fixed'] = stats_fixed
        conn.close()
    
    # Invoke built-in repair function
    other_repairs = db.repair_common_issues()
    results.update(other_repairs)
    
    logger.info("Database repair completed")
    logger.info(f"Repair results: {results}")
    
    return results

def main():
    """Main function for database repair script."""
    parser = argparse.ArgumentParser(description='Database repair utility for Mexican Candidates Scraper')
    parser.add_argument('--db', '-d', type=str, default=DEFAULT_DB_PATH, help='Path to SQLite database')
    parser.add_argument('--repair-type', '-t', type=str, default='all', 
                        choices=['all', 'constraints', 'orphans', 'stats'], 
                        help='Type of repair to perform')
    
    args = parser.parse_args()
    
    repair_database(args.db, args.repair_type)

if __name__ == '__main__':
    main()