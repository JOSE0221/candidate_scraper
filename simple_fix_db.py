import sqlite3
import os
import sys
from pathlib import Path

# Add the project directory to the path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from config.settings import DEFAULT_DB_PATH

def fix_database_schema():
    """Fix database schema issues by checking and updating if necessary."""
    print(f"Checking database at {DEFAULT_DB_PATH}")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(DEFAULT_DB_PATH)
        cursor = conn.cursor()
        
        # Get the current schema for the articles table
        cursor.execute("PRAGMA table_info(articles)")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"Current articles table columns: {columns}")
        
        # Check if 'success' column exists and handle it
        if 'success' not in columns:
            print("The 'success' column doesn't exist. No need to modify schema.")
        
        # Close the connection
        conn.close()
        print("Database schema check completed.")
        
    except Exception as e:
        print(f"Error checking database schema: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    fix_database_schema()