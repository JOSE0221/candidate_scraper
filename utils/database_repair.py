# Create a new file utils/database_repair.py

"""
Database repair utilities for the Mexican Municipal Candidates Scraper.
"""
import os
import sys
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime

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
        
        # Remove entities with missing articles
        cursor.execute("""
            DELETE FROM entities
            WHERE article_id NOT IN (SELECT id FROM articles)
        """)
        orphaned_entities = cursor.rowcount
        logger.info(f"Removed {orphaned_entities} orphaned entities")
        results['orphaned_entities'] = orphaned_entities
        
        # Fix inconsistent scraping_progress entries
        cursor.execute("""
            DELETE FROM scraping_progress
            WHERE candidate_id NOT IN (SELECT id FROM candidates)
        """)
        orphaned_progress = cursor.rowcount
        if orphaned_progress > 0:
            logger.info(f"Removed {orphaned_progress} orphaned progress entries")
            results['orphaned_progress'] = orphaned_progress
        
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
        
        # Fix domain_blacklist issues by deduplicating
        cursor.execute("""
            SELECT domain, COUNT(*) as count
            FROM domain_blacklist
            GROUP BY domain
            HAVING count > 1
        """)
        
        blacklist_duplicates = cursor.fetchall()
        blacklist_fixed = 0
        
        if blacklist_duplicates:
            logger.warning(f"Found {len(blacklist_duplicates)} domains with duplicate blacklist entries")
            
            for domain_data in blacklist_duplicates:
                domain = domain_data[0]
                # Get entries for this domain
                cursor.execute("SELECT * FROM domain_blacklist WHERE domain = ? ORDER BY added_date DESC", (domain,))
                entries = cursor.fetchall()
                
                # Keep only the most recent entry
                most_recent = entries[0]
                
                # Delete all entries for this domain
                cursor.execute("DELETE FROM domain_blacklist WHERE domain = ?", (domain,))
                
                # Insert only the most recent entry
                cursor.execute("""
                    INSERT INTO domain_blacklist
                    (domain, reason, added_date)
                    VALUES (?, ?, ?)
                """, (most_recent['domain'], most_recent['reason'], most_recent['added_date']))
                
                blacklist_fixed += 1
            
            conn.commit()
            logger.info(f"Fixed {blacklist_fixed} duplicate blacklist entries")
            results['blacklist_fixed'] = blacklist_fixed
        else:
            logger.info("No duplicate blacklist entries found")
            results['blacklist_fixed'] = 0
        
        conn.close()
    
    # Fix inconsistent batch statuses
    if repair_type in ['all', 'batches']:
        logger.info("Repairing batch statuses")
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Find batches with inconsistent status
        cursor.execute("""
            SELECT id, total_candidates, completed_candidates, status
            FROM scraping_batches
            WHERE (status = 'COMPLETED' AND completed_candidates < total_candidates)
            OR (status = 'STARTED' AND completed_candidates > 0)
        """)
        
        inconsistent_batches = cursor.fetchall()
        batch_fixes = 0
        
        if inconsistent_batches:
            logger.warning(f"Found {len(inconsistent_batches)} batches with inconsistent status")
            
            for batch in inconsistent_batches:
                batch_id = batch['id']
                total = batch['total_candidates']
                completed = batch['completed_candidates']
                status = batch['status']
                
                # Determine correct status
                if completed == 0:
                    new_status = 'STARTED'
                elif completed < total:
                    new_status = 'IN_PROGRESS'
                else:
                    new_status = 'COMPLETED'
                    
                # Update if different
                if new_status != status:
                    logger.info(f"Updating batch {batch_id} from '{status}' to '{new_status}' ({completed}/{total} completed)")
                    
                    # If changing to COMPLETED, set completion date
                    if new_status == 'COMPLETED':
                        cursor.execute(
                            'UPDATE scraping_batches SET status = ?, completed_at = ? WHERE id = ?',
                            (new_status, datetime.now().isoformat(), batch_id)
                        )
                    else:
                        cursor.execute(
                            'UPDATE scraping_batches SET status = ? WHERE id = ?',
                            (new_status, batch_id)
                        )
                    batch_fixes += 1
            
            conn.commit()
            logger.info(f"Fixed {batch_fixes} batch status issues")
            results['batch_fixes'] = batch_fixes
        else:
            logger.info("No batch status issues found")
            results['batch_fixes'] = 0
        
        conn.close()
    
    # Invoke built-in repair function from DatabaseManager if it exists
    try:
        other_repairs = db.repair_common_issues()
        if isinstance(other_repairs, dict):
            results.update(other_repairs)
    except AttributeError:
        # repair_common_issues method might not exist
        logger.warning("Database manager does not have a repair_common_issues method")
    except Exception as e:
        logger.error(f"Error calling built-in repair function: {str(e)}")
    
    # Vacuum the database to reclaim space and optimize
    try:
        conn = db.get_connection()
        conn.execute("VACUUM")
        conn.close()
        logger.info("Vacuumed database to optimize storage")
    except Exception as e:
        logger.error(f"Error vacuuming database: {str(e)}")
    
    # Run integrity check
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA integrity_check")
        integrity_result = cursor.fetchone()[0]
        conn.close()
        
        if integrity_result == 'ok':
            logger.info("Database integrity check passed")
            results['integrity_check'] = 'passed'
        else:
            logger.warning(f"Database integrity check returned: {integrity_result}")
            results['integrity_check'] = 'failed'
    except Exception as e:
        logger.error(f"Error checking database integrity: {str(e)}")
        results['integrity_check'] = 'error'
    
    logger.info("Database repair completed")
    logger.info(f"Repair results: {results}")
    
    return results

def main():
    """Main function for database repair script."""
    parser = argparse.ArgumentParser(description='Database repair utility for Mexican Candidates Scraper')
    parser.add_argument('--db', '-d', type=str, default=DEFAULT_DB_PATH, help='Path to SQLite database')
    parser.add_argument('--repair-type', '-t', type=str, default='all', 
                        choices=['all', 'constraints', 'orphans', 'stats', 'batches'], 
                        help='Type of repair to perform')
    
    args = parser.parse_args()
    
    repair_database(args.db, args.repair_type)

if __name__ == '__main__':
    main()