#!/usr/bin/env python
"""
Direct database schema fix for mexican_candidates.db
"""
import sqlite3
import os
import sys
import time

def fix_schema():
    db_path = os.path.join('data', 'mexican_candidates.db')
    print(f"Fixing schema in {db_path}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if the problematic fields exist
        cursor.execute("PRAGMA table_info(articles)")
        columns = [col[1] for col in cursor.fetchall()]
        
        needs_fixing = False
        for col in ['success', 'from_cache', 'oxylabs_used']:
            if col in columns:
                needs_fixing = True
                print(f"Found problematic column: {col}")
        
        if not needs_fixing:
            print("No schema issues found")
            return True
        
        # Create backup
        backup_path = f"{db_path}_backup_{int(time.time())}"
        print(f"Creating backup at {backup_path}")
        conn.close()
        
        import shutil
        shutil.copy2(db_path, backup_path)
        
        # Reconnect after backup
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
            
        # Get schema definition without problematic columns
        safe_columns = [c for c in columns if c not in ['success', 'from_cache', 'oxylabs_used']]
        
        # Create temp table with same data but without problem columns
        print("Creating fixed table structure")
        cursor.execute("CREATE TABLE articles_new AS SELECT " + ", ".join(safe_columns) + " FROM articles")
        
        # Replace old table
        print("Replacing old table")
        cursor.execute("DROP TABLE articles")
        cursor.execute("ALTER TABLE articles_new RENAME TO articles")
        
        # Recreate indexes
        print("Recreating indexes")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(overall_relevance)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_content_type ON articles(content_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_temporal ON articles(temporal_relevance)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_year_bounds ON articles(year_lower_bound, year_upper_bound)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_batch ON articles(batch_id)")
        
        conn.commit()
        conn.close()
        
        print("Schema fixed successfully")
        return True
    except Exception as e:
        print(f"Error fixing schema: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    fix_schema()