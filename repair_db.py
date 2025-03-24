# repair_db.py
import sqlite3
import os

db_path = os.path.join('data', 'mexican_candidates.db')
print(f"Repairing database at {db_path}")

# Connect directly to the database
conn = sqlite3.connect(db_path)
conn.isolation_level = None  # Autocommit mode
cursor = conn.cursor()

# Check articles table schema
cursor.execute("PRAGMA table_info(articles)")
columns = {row[1]: row for row in cursor.fetchall()}

# Check for problematic columns
problem_columns = ['success', 'from_cache', 'oxylabs_used']
found_problems = [col for col in problem_columns if col in columns]

if found_problems:
    print(f"Found problematic columns: {', '.join(found_problems)}")
    
    # Create backup
    print("Creating backup of articles table")
    cursor.execute("CREATE TABLE IF NOT EXISTS articles_backup AS SELECT * FROM articles")
    
    # Get valid columns
    valid_columns = [col for col in columns.keys() if col not in problem_columns]
    
    # Create fixed table
    print("Creating fixed articles table")
    cursor.execute("DROP TABLE IF EXISTS articles_fixed")
    
    # Get original table creation statement
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='articles'")
    create_stmt = cursor.fetchone()[0]
    
    # Remove problem columns from create statement
    for col in problem_columns:
        create_stmt = create_stmt.replace(f", {col} TEXT", "")
        create_stmt = create_stmt.replace(f", {col} BOOLEAN", "")
        create_stmt = create_stmt.replace(f"{col} TEXT", "")
        create_stmt = create_stmt.replace(f"{col} BOOLEAN", "")
    
    # Rename table in create statement
    create_stmt = create_stmt.replace("CREATE TABLE articles", "CREATE TABLE articles_fixed")
    cursor.execute(create_stmt)
    
    # Copy data
    cursor.execute(f"INSERT INTO articles_fixed SELECT {', '.join(valid_columns)} FROM articles")
    
    # Replace tables
    cursor.execute("DROP TABLE articles")
    cursor.execute("ALTER TABLE articles_fixed RENAME TO articles")
    
    # Rebuild indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(overall_relevance)")
    
    print("Schema repair completed successfully!")
else:
    print("No schema issues found with the articles table")

conn.close()