"""
Mexican Municipal Candidates Scraper main module.

This module provides the main scraper class and entry point for the application.
It orchestrates all components of the scraping system, including:
- Database management
- Web content extraction
- Name matching and entity recognition
- Content classification
- Search processing
- Result export

Module dependencies:
- utils.logger: Logging configuration
- config.settings: Application settings
- database.db_manager: Database operations
- scrapers.oxylabs_manager: Oxylabs API integration
- scrapers.search_engine: Search functionality
- processing.content_classifier: Content type classification
- processing.entity_recognizer: Entity recognition
- processing.name_matcher: Name matching

Usage:
    python main.py [options]
"""
import os
import sys
import json
import time
import pandas as pd
import argparse
import importlib
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import traceback

# Add the project directory to the path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

# Import project modules
from utils.logger import get_logger, setup_logger
from config.settings import (
    DEFAULT_DB_PATH, OXYLABS_USERNAME, OXYLABS_PASSWORD, OXYLABS_COUNTRY,
    DEFAULT_THREADS, ARTICLE_THREADS, DEFAULT_YEAR_RANGE, RESULTS_DIR,
    MIN_RELEVANCE_THRESHOLD
)

# Set up logger
logger = setup_logger()

# Dependency validation function
def validate_dependencies():
    """
    Validate all required dependencies before running.
    
    Returns:
        bool: True if all dependencies are available, False otherwise
    """
    required_modules = [
        ('database.db_manager', ['DatabaseManager']),
        ('scrapers.oxylabs_manager', ['OxylabsAPIManager']),
        ('scrapers.search_engine', ['create_search_engine']),
        ('processing.content_classifier', ['create_content_classifier']),
        ('processing.entity_recognizer', ['create_entity_recognizer']),
        ('processing.name_matcher', ['create_name_matcher']),
        ('utils.database_repair', ['repair_database'])
    ]
    
    all_available = True
    
    for module_name, classes in required_modules:
        try:
            module = importlib.import_module(module_name)
            for class_name in classes:
                if not hasattr(module, class_name):
                    logger.error(f"Required class {class_name} not found in module {module_name}")
                    all_available = False
        except ImportError as e:
            logger.error(f"Required module {module_name} not available: {str(e)}")
            all_available = False
    
    return all_available

# Validate dependencies before importing
if not validate_dependencies():
    logger.error("Missing required dependencies. Please check the installation and module structure.")
    sys.exit(1)

# Import modules after validation
from database.db_manager import DatabaseManager
from scrapers.oxylabs_manager import OxylabsAPIManager
from scrapers.search_engine import create_search_engine
from processing.content_classifier import create_content_classifier
from processing.entity_recognizer import create_entity_recognizer
from processing.name_matcher import create_name_matcher
from utils.database_repair import repair_database


class MexicanCandidateScraper:
    """
    Main class for the Mexican municipal candidate scraper.
    
    This class orchestrates all components of the scraping system, managing the
    initialization and interaction of database, search, and processing modules.
    """
    
    def __init__(self, db_path=DEFAULT_DB_PATH, candidates_csv=None, 
                max_threads=DEFAULT_THREADS, article_threads=ARTICLE_THREADS, 
                year_range=DEFAULT_YEAR_RANGE, use_oxylabs=True, enhanced_search=True):
        """
        Initialize the scraper.
        
        Args:
            db_path (str, optional): Path to SQLite database.
            candidates_csv (str, optional): Path to candidates CSV file.
            max_threads (int, optional): Maximum number of concurrent threads for candidates.
            article_threads (int, optional): Maximum number of concurrent threads for articles.
            year_range (int, optional): Year range for temporal filtering.
            use_oxylabs (bool, optional): Whether to use Oxylabs for searches.
            enhanced_search (bool, optional): Whether to use enhanced search strategies.
        """
        self.db_path = db_path
        self.candidates_csv = candidates_csv
        self.max_threads = max_threads
        self.article_threads = article_threads
        self.year_range = year_range
        self.use_oxylabs = use_oxylabs
        self.enhanced_search = enhanced_search
        
        # Initialize database manager
        logger.info(f"Initializing database at {db_path}")
        try:
            self.db = DatabaseManager(db_path)
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise RuntimeError("Database initialization failed") from e
        
        # Initialize Oxylabs manager if enabled
        if self.use_oxylabs:
            try:
                logger.info("Initializing Oxylabs API manager")
                self.oxylabs = OxylabsAPIManager(OXYLABS_USERNAME, OXYLABS_PASSWORD, country=OXYLABS_COUNTRY)
            except Exception as e:
                logger.error(f"Failed to initialize Oxylabs manager: {str(e)}")
                logger.warning("Continuing without Oxylabs API support")
                self.oxylabs = None
        else:
            self.oxylabs = None
        
        # Load candidates data
        self.candidates_data = self._load_candidates()
        
        # Set up name matcher
        try:
            self.name_matcher = create_name_matcher()
        except Exception as e:
            logger.error(f"Failed to initialize name matcher: {str(e)}")
            raise RuntimeError("Name matcher initialization failed") from e
        
        # Set up entity recognizer with candidates data
        try:
            self.entity_recognizer = create_entity_recognizer(
                candidates_data=self.candidates_data,
                name_matcher=self.name_matcher
            )
        except Exception as e:
            logger.error(f"Failed to initialize entity recognizer: {str(e)}")
            raise RuntimeError("Entity recognizer initialization failed") from e
        
        # Set up content classifier
        try:
            self.content_classifier = create_content_classifier(
                name_matcher=self.name_matcher
            )
        except Exception as e:
            logger.error(f"Failed to initialize content classifier: {str(e)}")
            raise RuntimeError("Content classifier initialization failed") from e
        
        # Initialize search engine
        try:
            self.search_engine = create_search_engine(
                self.db, 
                self.oxylabs,
                content_classifier=self.content_classifier,
                entity_recognizer=self.entity_recognizer,
                year_range=self.year_range,
                max_workers=self.article_threads
            )
        except Exception as e:
            logger.error(f"Failed to initialize search engine: {str(e)}")
            raise RuntimeError("Search engine initialization failed") from e
            
        logger.info("Mexican Candidate Scraper successfully initialized")
    
    def _load_candidates(self):
        """
        Load candidates data from CSV with comprehensive data cleaning and validation.
        
        Returns:
            pandas.DataFrame or None: Cleaned candidates data or None if loading fails
        """
        if self.candidates_csv:
            try:
                # Try to read with pandas
                df = pd.read_csv(self.candidates_csv, encoding='utf-8')
                
                # Data cleaning and validation
                logger.info(f"Raw data loaded: {len(df)} rows")
                
                # Essential columns that must have values
                essential_columns = ['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year']
                
                # Verify all essential columns exist
                missing_columns = [col for col in essential_columns if col not in df.columns]
                if missing_columns:
                    logger.error(f"Missing essential columns in CSV: {missing_columns}")
                    return None
                
                # Replace empty strings with NaN for consistent handling
                df = df.replace('', pd.NA)
                
                # Count rows with missing data in essential columns
                missing_data_count = df[df[essential_columns].isna().any(axis=1)].shape[0]
                logger.info(f"Rows with missing essential data: {missing_data_count}")
                
                # Drop rows with missing essential data
                df = df.dropna(subset=essential_columns)
                logger.info(f"Rows after dropping incomplete entries: {len(df)}")
                
                # Convert string columns to strings (handles numeric values)
                string_columns = [
                    'PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'ENTIDAD', 'PARTIDO', 
                    'PERIODO_FORMATO_ORIGINAL', 'SEXO'
                ]
                
                for col in string_columns:
                    if col in df.columns:
                        # Convert to string, but handle NaN values first
                        df[col] = df[col].fillna('').astype(str)
                        # Convert empty strings back to NaN
                        df[col] = df[col].replace('', pd.NA)
                        # Strip whitespace
                        df[col] = df[col].str.strip()
                
                # Ensure Year is numeric
                if 'Year' in df.columns:
                    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
                    # Drop rows with invalid years
                    invalid_years = df['Year'].isna().sum()
                    if invalid_years > 0:
                        logger.warning(f"Dropping {invalid_years} rows with invalid Year values")
                        df = df.dropna(subset=['Year'])
                    
                    # Verify years are within a reasonable range
                    out_of_range = ((df['Year'] < 1980) | (df['Year'] > 2030)).sum()
                    if out_of_range > 0:
                        logger.warning(f"Found {out_of_range} rows with years outside reasonable range (1980-2030)")
                
                # Check for duplicates
                duplicates = df.duplicated(subset=['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year'], keep='first').sum()
                if duplicates > 0:
                    logger.warning(f"Found {duplicates} duplicate candidate entries (keeping first occurrence)")
                    df = df.drop_duplicates(subset=['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year'], keep='first')
                
                logger.info(f"Successfully cleaned data: {len(df)} valid candidates")
                return df
                
            except UnicodeDecodeError:
                # Try different encodings if UTF-8 fails
                for encoding in ['latin1', 'iso-8859-1', 'cp1252']:
                    try:
                        df = pd.read_csv(self.candidates_csv, encoding=encoding)
                        logger.info(f"Loaded {len(df)} candidates from CSV using {encoding} encoding")
                        
                        # Apply the same cleaning as above
                        # Essential columns that must have values
                        essential_columns = ['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year']
                        
                        # Verify all essential columns exist
                        missing_columns = [col for col in essential_columns if col not in df.columns]
                        if missing_columns:
                            logger.error(f"Missing essential columns in CSV: {missing_columns}")
                            continue
                        
                        # Replace empty strings with NaN for consistent handling
                        df = df.replace('', pd.NA)
                        
                        # Drop rows with missing essential data
                        df = df.dropna(subset=essential_columns)
                        
                        # Convert string columns to strings (handles numeric values)
                        string_columns = [
                            'PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'ENTIDAD', 'PARTIDO', 
                            'PERIODO_FORMATO_ORIGINAL', 'SEXO'
                        ]
                        
                        for col in string_columns:
                            if col in df.columns:
                                # Convert to string, but handle NaN values first
                                df[col] = df[col].fillna('').astype(str)
                                # Convert empty strings back to NaN
                                df[col] = df[col].replace('', pd.NA)
                                # Strip whitespace
                                df[col] = df[col].str.strip()
                        
                        # Ensure Year is numeric
                        if 'Year' in df.columns:
                            df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
                            # Drop rows with invalid years
                            df = df.dropna(subset=['Year'])
                        
                        # Check for duplicates
                        df = df.drop_duplicates(subset=['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year'], keep='first')
                        
                        logger.info(f"Successfully cleaned data: {len(df)} valid candidates")
                        return df
                    except Exception as e:
                        logger.warning(f"Failed to load with {encoding} encoding: {str(e)}")
                        continue
                        
                logger.error(f"Failed to load candidates CSV with any encoding")
            except Exception as e:
                logger.error(f"Error loading candidates CSV: {str(e)}")
                traceback.print_exc()
        
        return None
    
    def process_candidate(self, candidate):
        """
        Process a single candidate with comprehensive error handling and data validation.
        
        Args:
            candidate (dict): Candidate information dictionary
            
        Returns:
            int: Number of articles found or 0 if processing failed
        """
        try:
            # Extract candidate information with data validation
            candidate_name = self._safe_get_value(candidate, 'PRESIDENTE_MUNICIPAL', 'name')
            municipality = self._safe_get_value(candidate, 'MUNICIPIO', 'municipality')
            target_year = self._safe_get_value(candidate, 'Year', 'target_year', numeric=True)
            
            # Verify essential fields are present
            if not candidate_name or not municipality or not target_year:
                logger.warning(f"Skipping candidate with missing essential data: {candidate}")
                return 0
            
            # Extract optional fields with validation
            state = self._safe_get_value(candidate, 'ENTIDAD', 'entidad')
            gender = self._safe_get_value(candidate, 'SEXO', 'gender')
            party = self._safe_get_value(candidate, 'PARTIDO', 'party')
            period = self._safe_get_value(candidate, 'PERIODO_FORMATO_ORIGINAL', 'period_format')
            batch_id = candidate.get('batch_id')
            
            # Create a clean candidate dictionary with validated data
            clean_candidate = {
                'name': candidate_name,
                'municipality': municipality,
                'target_year': target_year,
                'entidad': state,
                'gender': gender,
                'party': party,
                'period_format': period,
                'batch_id': batch_id
            }
            
            logger.info(f"Processing candidate: {candidate_name}, {municipality}, {target_year}")
            
            # Use enhanced search if enabled, otherwise use standard search
            if self.enhanced_search:
                results = self.search_engine.search_candidate_enhanced(
                    clean_candidate,
                    batch_id=batch_id
                )
            else:
                results = self.search_engine.search_candidate(
                    clean_candidate,
                    batch_id=batch_id
                )
            
            logger.info(f"Found {len(results)} articles for {candidate_name}")
            return len(results)
        
        except Exception as e:
            logger.error(f"Error processing candidate: {str(e)}")
            traceback.print_exc()
            return 0

    def _safe_get_value(self, data, primary_key, backup_key=None, numeric=False):
        """
        Safely extract a value from a dictionary with comprehensive data validation.
        
        Args:
            data (dict): Dictionary to extract from
            primary_key (str): Primary key to look for
            backup_key (str, optional): Backup key if primary isn't found
            numeric (bool, optional): Whether to treat as numeric value
            
        Returns:
            Value or None if not found/invalid
        """
        value = None
        
        # Try primary key first
        if primary_key in data and data[primary_key] is not None:
            value = data[primary_key]
        # Try backup key if primary key failed
        elif backup_key and backup_key in data and data[backup_key] is not None:
            value = data[backup_key]
        else:
            return None
        
        # Return None for pandas NA values
        if pd.isna(value):
            return None
            
        # Validate based on type
        if numeric:
            # For numeric values
            try:
                # Try converting to int first
                return int(value)
            except (ValueError, TypeError):
                try:
                    # If that fails, try float
                    return float(value)
                except (ValueError, TypeError):
                    # If all conversion fails, return None
                    return None
        else:
            # For string values
            if isinstance(value, str):
                # Return None for empty strings after stripping
                stripped = value.strip()
                return stripped if stripped else None
            elif isinstance(value, (int, float)):
                # Convert numeric types to string
                return str(value)
            else:
                # Return None for other types
                return None
    
    def run_batch(self, candidates=None, max_candidates=None):
        """
        Run a batch of candidate processing with enhanced progress tracking.
        
        Args:
            candidates (list, optional): List of candidate dictionaries or DataFrame.
            max_candidates (int, optional): Maximum number of candidates to process.
            
        Returns:
            int: Batch ID
        """
        try:
            # Use provided candidates or all candidates
            if candidates is None:
                if self.candidates_data is None:
                    logger.error("No candidates data available")
                    return None
                
                all_candidates = self.candidates_data.to_dict('records')
            elif isinstance(candidates, pd.DataFrame):
                all_candidates = candidates.to_dict('records')
            else:
                all_candidates = candidates
                
            # Limit to max_candidates if specified
            if max_candidates and max_candidates > 0:
                candidates_to_process = all_candidates[:max_candidates]
            else:
                candidates_to_process = all_candidates
            
            # Filter out already completed candidates
            candidates_to_process = self.db.get_candidates_to_process(candidates_to_process)
            
            if not candidates_to_process:
                logger.info("No new candidates to process")
                return None
                
            total_candidates = len(candidates_to_process)
            logger.info(f"Starting batch with {total_candidates} candidates")
            
            # Create a new batch with configuration
            config = {
                'max_threads': self.max_threads,
                'article_threads': self.article_threads,
                'year_range': self.year_range,
                'use_oxylabs': self.use_oxylabs,
                'enhanced_search': self.enhanced_search,
            }
            
            batch_id = self.db.create_batch(total_candidates, config)
            
            # Add batch_id to each candidate
            for candidate in candidates_to_process:
                candidate['batch_id'] = batch_id
            
            # Process candidates concurrently
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                future_to_candidate = {
                    executor.submit(self.process_candidate, candidate): candidate 
                    for candidate in candidates_to_process
                }
                
                completed_count = 0
                for future in future_to_candidate:
                    try:
                        candidate = future_to_candidate[future]
                        candidate_name = candidate.get('PRESIDENTE_MUNICIPAL', candidate.get('name'))
                        
                        result_count = future.result()
                        logger.info(f"Completed {candidate_name} with {result_count} results")
                        completed_count += 1
                        
                        # Update batch progress
                        progress_percent = (completed_count / total_candidates) * 100
                        logger.info(f"Batch progress: {progress_percent:.1f}% ({completed_count}/{total_candidates})")
                        
                        # Update batch status periodically
                        if completed_count % 10 == 0 or completed_count == total_candidates:
                            self.db.update_batch_status(
                                batch_id, 'IN_PROGRESS', completed_candidates=completed_count
                            )
                            
                    except Exception as e:
                        logger.error(f"Error with candidate {candidate_name}: {str(e)}")
                        traceback.print_exc()
            
            # Mark batch as completed
            self.db.update_batch_status(batch_id, 'COMPLETED', completed_candidates=completed_count)
            logger.info(f"Batch {batch_id} completed")
            
            # Create candidate profiles
            logger.info("Creating candidate profiles...")
            self.db.create_candidate_profiles()
            
            return batch_id
            
        except Exception as e:
            logger.error(f"Error running batch: {str(e)}")
            traceback.print_exc()
            return None
    
    def export_results(self, output_path=None, format='json', min_relevance=MIN_RELEVANCE_THRESHOLD, batch_id=None):
        """
        Export results to CSV or JSON with enhanced fields.
        
        Args:
            output_path (str, optional): Output directory path.
            format (str, optional): Export format ('csv' or 'json').
            min_relevance (float, optional): Minimum relevance threshold.
            batch_id (int, optional): Batch ID to export.
            
        Returns:
            dict: Paths to exported files
        """
        try:
            # Use default results dir if none provided
            if output_path is None:
                output_path = RESULTS_DIR
                
            os.makedirs(output_path, exist_ok=True)
            
            # Get all articles
            logger.info(f"Retrieving articles with minimum relevance of {min_relevance}")
            df = self.db.get_all_articles(
                min_relevance=min_relevance,
                batch_id=batch_id
            )
            
            if df.empty:
                logger.warning("No results to export")
                return {}
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            batch_suffix = f"_batch{batch_id}" if batch_id else ""
            
            export_paths = {}
            
            if format.lower() == 'csv':
                # Export to CSV
                csv_path = os.path.join(output_path, f'mexican_candidates{batch_suffix}_{timestamp}.csv')
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"Exported {len(df)} results to {csv_path}")
                export_paths['results'] = csv_path
                
            elif format.lower() == 'json':
                # Export to JSON
                json_path = os.path.join(output_path, f'mexican_candidates{batch_suffix}_{timestamp}.json')
                df.to_json(json_path, orient='records', force_ascii=False, indent=4)
                logger.info(f"Exported {len(df)} results to {json_path}")
                export_paths['results'] = json_path
                
            # Export profiles
            profiles_path = self._export_profiles(output_path, timestamp, batch_id)
            if profiles_path:
                export_paths['profiles'] = profiles_path
            
            # Export quotes
            quotes_path = self._export_quotes(output_path, timestamp, batch_id)
            if quotes_path:
                export_paths['quotes'] = quotes_path
                
            return export_paths
                
        except Exception as e:
            logger.error(f"Error exporting results: {str(e)}")
            traceback.print_exc()
            return {}
    
    def export_ml_dataset(self, output_path=None, format='json', min_relevance=MIN_RELEVANCE_THRESHOLD):
        """
        Export ML/NLP dataset.
        
        Args:
            output_path (str, optional): Output directory path.
            format (str, optional): Export format ('json' or 'csv').
            min_relevance (float, optional): Minimum relevance threshold.
            
        Returns:
            str or None: Path to dataset file
        """
        try:
            # Use default results dir if none provided
            if output_path is None:
                output_path = os.path.join(RESULTS_DIR, 'ml_datasets')
                
            os.makedirs(output_path, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Generate the dataset
            logger.info(f"Generating ML dataset with minimum relevance of {min_relevance}")
            dataset = self.db.generate_ml_dataset(output_path, format, min_relevance)
            
            if dataset:
                # Dataset is saved by the DB manager
                if format.lower() == 'json':
                    path = os.path.join(output_path, f'candidate_ml_dataset_{timestamp}.json')
                else:
                    path = os.path.join(output_path, f'candidate_ml_dataset_{timestamp}.csv')
                    
                logger.info(f"Exported ML dataset with {len(dataset)} candidates to {path}")
                return path
            else:
                logger.warning("Failed to generate ML dataset")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting ML dataset: {str(e)}")
            traceback.print_exc()
            return None
    
    def _export_profiles(self, output_path, timestamp, batch_id=None):
        """
        Export candidate profiles with enhanced fields.
        
        Args:
            output_path (str): Output directory path.
            timestamp (str): Timestamp for filenames.
            batch_id (int, optional): Batch ID to filter by.
            
        Returns:
            str or None: Path to profiles file
        """
        try:
            # Get unique candidates
            if self.candidates_data is None and batch_id is None:
                logger.warning("No candidates data for profile export")
                return None
            
            # Get candidates from database
            if batch_id:
                conn = self.db.get_connection()
                query = """
                SELECT DISTINCT c.id, c.name, c.municipality, c.target_year, c.gender, c.party, c.period_format
                FROM candidates c
                JOIN candidate_articles ca ON c.id = ca.candidate_id
                JOIN articles a ON ca.article_id = a.id
                WHERE a.batch_id = ?
                """
                df = pd.read_sql_query(query, conn, params=[batch_id])
                conn.close()
                candidate_ids = df['id'].tolist()
            else:
                # Get all candidates
                candidates = self.db.get_all_candidates()
                candidate_ids = [c.id for c in candidates]
            
            profiles = []
            for candidate_id in candidate_ids:
                profile = self.db.get_candidate_profile(candidate_id=candidate_id)
                if profile:
                    profiles.append(profile)
            
            if profiles:
                # Save profiles to JSON
                batch_suffix = f"_batch{batch_id}" if batch_id else ""
                profiles_path = os.path.join(output_path, f'candidate_profiles{batch_suffix}_{timestamp}.json')
                with open(profiles_path, 'w', encoding='utf-8') as f:
                    json.dump(profiles, f, ensure_ascii=False, indent=4)
                    
                logger.info(f"Exported {len(profiles)} candidate profiles to {profiles_path}")
                return profiles_path
                
            return None
                
        except Exception as e:
            logger.error(f"Error exporting profiles: {str(e)}")
            traceback.print_exc()
            return None
    
    def _export_quotes(self, output_path, timestamp, batch_id=None):
        """
        Export candidate quotes to a separate file.
        
        Args:
            output_path (str): Output directory path.
            timestamp (str): Timestamp for filenames.
            batch_id (int, optional): Batch ID to filter by.
            
        Returns:
            str or None: Path to quotes file
        """
        try:
            conn = self.db.get_connection()
            
            # Get all quotes
            if batch_id:
                query = """
                SELECT q.*, a.title, a.url, a.source, a.content_date, a.content_type,
                       a.overall_relevance, c.name as candidate_name, c.municipality, c.target_year
                FROM quotes q
                JOIN articles a ON q.article_id = a.id
                JOIN candidates c ON q.candidate_id = c.id
                WHERE a.batch_id = ?
                ORDER BY c.name, q.extraction_confidence DESC
                """
                quotes_df = pd.read_sql_query(query, conn, params=[batch_id])
            else:
                query = """
                SELECT q.*, a.title, a.url, a.source, a.content_date, a.content_type,
                       a.overall_relevance, c.name as candidate_name, c.municipality, c.target_year
                FROM quotes q
                JOIN articles a ON q.article_id = a.id
                JOIN candidates c ON q.candidate_id = c.id
                ORDER BY c.name, q.extraction_confidence DESC
                """
                quotes_df = pd.read_sql_query(query, conn)
            
            conn.close()
            
            if not quotes_df.empty:
                # Save quotes to CSV
                batch_suffix = f"_batch{batch_id}" if batch_id else ""
                quotes_path = os.path.join(output_path, f'candidate_quotes{batch_suffix}_{timestamp}.csv')
                quotes_df.to_csv(quotes_path, index=False, encoding='utf-8-sig')
                
                logger.info(f"Exported {len(quotes_df)} candidate quotes to {quotes_path}")
                return quotes_path
                
            return None
                
        except Exception as e:
            logger.error(f"Error exporting quotes: {str(e)}")
            traceback.print_exc()
            return None
    
    def create_profiles(self, min_relevance=MIN_RELEVANCE_THRESHOLD):
        """
        Create or update profiles for all candidates.
        
        Args:
            min_relevance (float, optional): Minimum relevance threshold.
            
        Returns:
            int: Number of profiles created/updated
        """
        try:
            logger.info(f"Creating candidate profiles with minimum relevance of {min_relevance}")
            return self.db.create_candidate_profiles(min_relevance)
        except Exception as e:
            logger.error(f"Error creating profiles: {str(e)}")
            traceback.print_exc()
            return 0
    
    def analyze_candidates_data(self):
        """
        Analyze the candidates data to identify potential issues.
        This function provides detailed diagnostics about the data quality.
        """
        if self.candidates_data is None:
            logger.warning("No candidates data available for analysis")
            return
            
        # Basic information
        total_rows = len(self.candidates_data)
        logger.info(f"Total rows in candidates data: {total_rows}")
        
        # Column presence check
        essential_columns = ['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year']
        optional_columns = ['ENTIDAD', 'SEXO', 'PARTIDO', 'PERIODO_FORMATO_ORIGINAL']
        
        all_columns = list(self.candidates_data.columns)
        logger.info(f"Available columns: {', '.join(all_columns)}")
        
        missing_essential = [col for col in essential_columns if col not in all_columns]
        if missing_essential:
            logger.error(f"Missing essential columns: {', '.join(missing_essential)}")
        
        missing_optional = [col for col in optional_columns if col not in all_columns]
        if missing_optional:
            logger.warning(f"Missing optional columns: {', '.join(missing_optional)}")
        
        # Missing values per column
        present_columns = [col for col in essential_columns + optional_columns if col in all_columns]
        missing_values = self.candidates_data[present_columns].isna().sum()
        
        logger.info("Missing values analysis:")
        for col, count in missing_values.items():
            if count > 0:
                percentage = (count / total_rows) * 100
                logger.info(f"  {col}: {count} missing values ({percentage:.2f}%)")
        
        # Data type analysis
        logger.info("Data type analysis:")
        for col in present_columns:
            unique_types = self.candidates_data[col].apply(type).value_counts()
            type_info = ", ".join([f"{t.__name__}: {c} values" for t, c in unique_types.items()])
            logger.info(f"  {col}: {type_info}")
            
            # For string columns, check if any values are not strings
            if col in ['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'ENTIDAD', 'PARTIDO', 'SEXO', 'PERIODO_FORMATO_ORIGINAL']:
                non_string = self.candidates_data[~self.candidates_data[col].apply(lambda x: isinstance(x, str))].shape[0]
                if non_string > 0:
                    logger.warning(f"  Column {col} has {non_string} non-string values")
                    
                    # Show sample of non-string values
                    non_string_samples = self.candidates_data[~self.candidates_data[col].apply(
                        lambda x: isinstance(x, str)
                    )][col].head(5).tolist()
                    logger.warning(f"  Sample non-string values: {non_string_samples}")
        
        # Year validation
        if 'Year' in self.candidates_data.columns:
            # Convert to numeric for analysis
            years = pd.to_numeric(self.candidates_data['Year'], errors='coerce')
            
            # Check for non-numeric years
            non_numeric = self.candidates_data[years.isna()].shape[0]
            if non_numeric > 0:
                logger.warning(f"Found {non_numeric} rows with non-numeric years")
                
                # Show sample of non-numeric years
                non_numeric_samples = self.candidates_data[years.isna()]['Year'].head(5).tolist()
                logger.warning(f"Sample non-numeric years: {non_numeric_samples}")
            
            # Check for years outside reasonable range
            invalid_years = self.candidates_data[~years.between(1980, 2030)].shape[0]
            if invalid_years > 0:
                logger.warning(f"Found {invalid_years} rows with years outside reasonable range (1980-2030)")
                
                # Show distribution of years
                year_distribution = years.value_counts().sort_index()
                logger.info(f"Year distribution: {dict(year_distribution)}")
        
        # Duplicate analysis
        if all(col in self.candidates_data.columns for col in ['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year']):
            # Look for exact duplicates
            exact_duplicates = self.candidates_data.duplicated().sum()
            if exact_duplicates > 0:
                logger.warning(f"Found {exact_duplicates} exact duplicate rows")
            
            # Look for duplicate candidate entries
            candidate_duplicates = self.candidates_data.duplicated(
                subset=['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year']
            ).sum()
            
            if candidate_duplicates > 0:
                logger.warning(f"Found {candidate_duplicates} duplicate candidate entries (same name, municipality, year)")
                
                # Show most frequent duplicates
                duplicate_counts = self.candidates_data.groupby(
                    ['PRESIDENTE_MUNICIPAL', 'MUNICIPIO', 'Year']
                ).size()
                
                top_duplicates = duplicate_counts[duplicate_counts > 1].sort_values(ascending=False).head(5)
                logger.warning(f"Top duplicated candidates: {dict(top_duplicates)}")
        
        # Value distribution for categorical columns
        for col in ['ENTIDAD', 'SEXO', 'PARTIDO']:
            if col in self.candidates_data.columns:
                value_counts = self.candidates_data[col].value_counts(dropna=False).head(10)
                logger.info(f"Top values for {col}: {dict(value_counts)}")
        
        # String length analysis for name fields
        if 'PRESIDENTE_MUNICIPAL' in self.candidates_data.columns:
            # Get string values only
            name_strings = self.candidates_data['PRESIDENTE_MUNICIPAL'][
                self.candidates_data['PRESIDENTE_MUNICIPAL'].apply(lambda x: isinstance(x, str))
            ]
            
            if not name_strings.empty:
                name_lengths = name_strings.str.len()
                logger.info(f"Candidate name length: min={name_lengths.min()}, max={name_lengths.max()}, avg={name_lengths.mean():.1f}")
                
                # Check for suspiciously short names
                short_names = name_strings[name_lengths < 5]
                if not short_names.empty:
                    logger.warning(f"Found {len(short_names)} suspiciously short candidate names")
                    logger.warning(f"Sample short names: {short_names.head(5).tolist()}")

    def validate_database(self):
        """
        Validate the database structure and contents.
        This function performs integrity checks on the database and reports any issues.
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Check tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Database tables: {', '.join(tables)}")
            
            # Expected tables
            expected_tables = [
                'candidates', 'articles', 'candidate_articles', 'quotes', 
                'entities', 'scraping_progress', 'scraping_batches', 
                'candidate_profiles', 'search_cache', 'content_cache', 
                'domain_stats', 'domain_blacklist'
            ]
            
            missing_tables = [table for table in expected_tables if table not in tables]
            if missing_tables:
                logger.warning(f"Missing expected tables: {', '.join(missing_tables)}")
            
            # Count records in main tables
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    logger.info(f"Table {table}: {count} records")
                except sqlite3.Error as e:
                    logger.error(f"Error querying table {table}: {str(e)}")
            
            # Check foreign key constraints
            cursor.execute("PRAGMA foreign_key_check")
            fk_violations = cursor.fetchall()
            if fk_violations:
                logger.error(f"Foreign key violations found: {len(fk_violations)}")
                for violation in fk_violations:
                    logger.error(f"  {violation}")
            
            # Check for candidate articles with missing candidates or articles
            if all(table in tables for table in ['candidates', 'articles', 'candidate_articles']):
                cursor.execute("""
                    SELECT COUNT(*) FROM candidate_articles ca 
                    LEFT JOIN candidates c ON ca.candidate_id = c.id
                    WHERE c.id IS NULL
                """)
                orphaned_links = cursor.fetchone()[0]
                if orphaned_links > 0:
                    logger.error(f"Found {orphaned_links} candidate_articles with missing candidates")
                
                cursor.execute("""
                    SELECT COUNT(*) FROM candidate_articles ca 
                    LEFT JOIN articles a ON ca.article_id = a.id
                    WHERE a.id IS NULL
                """)
                orphaned_article_links = cursor.fetchone()[0]
                if orphaned_article_links > 0:
                    logger.error(f"Found {orphaned_article_links} candidate_articles with missing articles")
            
            # Check for quotes with missing articles or candidates
            if 'quotes' in tables:
                cursor.execute("""
                    SELECT COUNT(*) FROM quotes q
                    LEFT JOIN articles a ON q.article_id = a.id
                    WHERE a.id IS NULL
                """)
                orphaned_quotes = cursor.fetchone()[0]
                if orphaned_quotes > 0:
                    logger.error(f"Found {orphaned_quotes} quotes with missing articles")
                
                cursor.execute("""
                    SELECT COUNT(*) FROM quotes q
                    LEFT JOIN candidates c ON q.candidate_id = c.id
                    WHERE c.id IS NULL
                """)
                orphaned_candidate_quotes = cursor.fetchone()[0]
                if orphaned_candidate_quotes > 0:
                    logger.error(f"Found {orphaned_candidate_quotes} quotes with missing candidates")
            
            # Validate scraping batches status
            if 'scraping_batches' in tables:
                cursor.execute("""
                    SELECT id, status, total_candidates, completed_candidates
                    FROM scraping_batches
                """)
                
                batches = cursor.fetchall()
                for batch in batches:
                    batch_id, status, total, completed = batch
                    
                    if status == 'COMPLETED' and total != completed:
                        logger.warning(f"Batch {batch_id} marked COMPLETED but only has {completed}/{total} candidates completed")
            
            # Check for blacklisted domains in articles
            if all(table in tables for table in ['articles', 'domain_blacklist']):
                cursor.execute("""
                    SELECT a.id, a.url, a.title, b.domain, b.reason
                    FROM articles a
                    JOIN domain_blacklist b ON a.source = b.domain
                    LIMIT 10
                """)
                
                blacklisted_articles = cursor.fetchall()
                if blacklisted_articles:
                    logger.warning(f"Found {len(blacklisted_articles)} articles from blacklisted domains")
                    for article in blacklisted_articles[:5]:
                        logger.warning(f"  Article {article['id']} from {article['domain']}: {article['title']}")
            
            # Check for duplicate articles
            cursor.execute("""
                SELECT url, COUNT(*) as count
                FROM articles
                GROUP BY url
                HAVING count > 1
                LIMIT 10
            """)
            
            duplicates = cursor.fetchall()
            if duplicates:
                logger.warning(f"Found {len(duplicates)} URLs with duplicate article entries")
                for dup in duplicates[:5]:
                    logger.warning(f"  URL {dup['url']} has {dup['count']} duplicate entries")
            
            conn.close()
            logger.info("Database validation complete")
            
        except Exception as e:
            logger.error(f"Database validation error: {str(e)}")
            traceback.print_exc()

    def process_single_candidate(self, candidate_name, municipality, target_year, 
                                state=None, gender=None, party=None, period=None):
        """
        Process a single candidate by direct specification.
        
        This method is intended for testing or processing a specific candidate without
        needing a candidates CSV file. It manually creates a candidate record and processes it.
        
        Args:
            candidate_name (str): Candidate name
            municipality (str): Municipality name
            target_year (int): Target election year
            state (str, optional): State name
            gender (str, optional): Candidate gender
            party (str, optional): Political party
            period (str, optional): Period format
            
        Returns:
            dict: Results of the processing with all found articles
        """
        # Validate parameters
        if not candidate_name or not municipality or not target_year:
            logger.error("Missing required parameters: candidate_name, municipality, target_year")
            return {"success": False, "error": "Missing required parameters"}
        
        try:
            logger.info(f"Processing single candidate: {candidate_name}, {municipality}, {target_year}")
            
            # Create a batch for this single candidate
            batch_id = self.db.create_batch(1, {
                'max_threads': self.max_threads,
                'article_threads': self.article_threads,
                'year_range': self.year_range,
                'use_oxylabs': self.use_oxylabs,
                'enhanced_search': self.enhanced_search,
                'single_candidate': True
            })
            
            # Create candidate dictionary for processing
            candidate = {
                'name': candidate_name,
                'municipality': municipality,
                'target_year': target_year,
                'entidad': state,
                'gender': gender,
                'party': party,
                'period_format': period,
                'batch_id': batch_id
            }
            
            # Get or create candidate in database
            candidate_obj, created = self.db.get_or_create_candidate(
                candidate_name, municipality, target_year,
                state=state, gender=gender, party=party, period=period
            )
            
            if not candidate_obj:
                logger.error("Failed to create candidate record")
                return {"success": False, "error": "Failed to create candidate record"}
            
            candidate_id = candidate_obj.id
            
            # Use enhanced search for best results
            if self.enhanced_search:
                articles = self.search_engine.search_candidate_enhanced(candidate, batch_id=batch_id)
            else:
                articles = self.search_engine.search_candidate(candidate, batch_id=batch_id)
            
            article_count = len(articles)
            logger.info(f"Found {article_count} articles for {candidate_name}")
            
            # Create candidate profile
            self.db.get_candidate_profile(candidate_id=candidate_id)
            
            # Get results and return detailed information
            results = {
                "success": True,
                "candidate_id": candidate_id,
                "candidate_name": candidate_name,
                "municipality": municipality,
                "target_year": target_year,
                "articles_found": article_count,
                "batch_id": batch_id
            }
            
            # Export results for this batch
            if article_count > 0:
                export_paths = self.export_results(
                    format='json',
                    batch_id=batch_id
                )
                
                results["export_paths"] = export_paths
            
            # Mark batch as completed
            self.db.update_batch_status(batch_id, 'COMPLETED', completed_candidates=1)
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing single candidate: {str(e)}")
            traceback.print_exc()
            return {"success": False, "error": str(e)}

def parse_arguments():
    """
    Parse command line arguments with enhanced data validation options.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Enhanced Mexican Municipal Candidates Web Scraper')
    
    # Input and database options
    parser.add_argument('--csv', '-c', type=str, help='Path to candidates CSV file')
    parser.add_argument('--db', '-d', type=str, default=DEFAULT_DB_PATH, 
                      help='Path to SQLite database')
    
    # Processing options
    parser.add_argument('--threads', '-t', type=int, default=DEFAULT_THREADS, 
                      help='Maximum number of concurrent threads for candidates')
    parser.add_argument('--article-threads', '-a', type=int, default=ARTICLE_THREADS,
                      help='Maximum number of concurrent threads for articles')
    parser.add_argument('--year-range', '-y', type=int, default=DEFAULT_YEAR_RANGE, 
                      help='Year range for temporal filtering (±)')
    parser.add_argument('--max-candidates', '-m', type=int, default=0, 
                      help='Maximum number of candidates to process (0 for all)')
    
    # Scraping behavior
    parser.add_argument('--no-oxylabs', action='store_true', 
                      help='Disable Oxylabs API')
    parser.add_argument('--enhanced-search', action='store_true', 
                      help='Use enhanced search strategies')
    
    # Output options
    parser.add_argument('--export', '-e', action='store_true', 
                      help='Export results after processing')
    parser.add_argument('--export-format', '-f', type=str, default='json', 
                      choices=['csv', 'json'], help='Export format (csv or json)')
    parser.add_argument('--min-relevance', '-r', type=float, default=MIN_RELEVANCE_THRESHOLD, 
                      help='Minimum relevance threshold for exports')
    parser.add_argument('--output-path', type=str, default=RESULTS_DIR,
                      help='Path for exported files')
    
    # Utility operations
    parser.add_argument('--create-dataset', action='store_true', 
                      help='Create a consolidated dataset for ML/NLP')
    parser.add_argument('--dataset-format', type=str, default='json', 
                      choices=['json', 'csv'], help='Dataset format')
    parser.add_argument('--create-profiles', action='store_true',
                      help='Create candidate profiles without running scraper')
    parser.add_argument('--extract-batch', type=int, default=None,
                      help='Export results for a specific batch')
    
    # Data validation and repair options
    parser.add_argument('--analyze-data', action='store_true',
                      help='Analyze candidates data without running scraper')
    parser.add_argument('--validate-db', action='store_true',
                      help='Validate the database structure and contents')
    parser.add_argument('--clean-only', action='store_true',
                      help='Clean and validate data without running scraper')
    parser.add_argument('--repair-db', action='store_true',
                      help='Attempt to repair common database issues')
    parser.add_argument('--repair-type', type=str, default='all',
                        choices=['all', 'constraints', 'orphans', 'stats'],
                        help='Type of database repair to perform')
                      
    # Single candidate processing
    parser.add_argument('--single-candidate', action='store_true',
                      help='Process a single candidate (requires name, municipality, and year)')
    parser.add_argument('--name', type=str, 
                      help='Candidate name for single candidate processing')
    parser.add_argument('--municipality', type=str, 
                      help='Municipality for single candidate processing')
    parser.add_argument('--year', type=int, 
                      help='Target year for single candidate processing')
    parser.add_argument('--state', type=str, 
                      help='State for single candidate processing')
    parser.add_argument('--gender', type=str, choices=['M', 'F'], 
                      help='Gender for single candidate processing')
    parser.add_argument('--party', type=str, 
                      help='Political party for single candidate processing')
    parser.add_argument('--period', type=str, 
                      help='Period format for single candidate processing')
                      
    return parser.parse_args()

def main():
    """
    Main function with enhanced data validation and error handling.
    """
    # Parse arguments
    args = parse_arguments()
    
    try:
        # Validate dependencies before proceeding
        if not validate_dependencies():
            logger.error("Missing required dependencies. Exiting.")
            sys.exit(1)
            
        # Add database repair option
        if args.repair_db:
            logger.info("Repairing database...")
            from utils.database_repair import repair_database
            repair_database(args.db, args.repair_type)
            return
        
        # Single candidate processing mode
        if args.single_candidate:
            # Validate required parameters
            if not args.name or not args.municipality or not args.year:
                logger.error("Single candidate processing requires --name, --municipality, and --year")
                return
                
            # Initialize scraper with minimal configuration
            scraper = MexicanCandidateScraper(
                db_path=args.db,
                max_threads=args.threads,
                article_threads=args.article_threads,
                year_range=args.year_range,
                use_oxylabs=not args.no_oxylabs,
                enhanced_search=True  # Always use enhanced search for single candidate
            )
            
            # Process the single candidate
            results = scraper.process_single_candidate(
                candidate_name=args.name,
                municipality=args.municipality,
                target_year=args.year,
                state=args.state,
                gender=args.gender,
                party=args.party,
                period=args.period
            )
            
            if results["success"]:
                logger.info(f"Successfully processed candidate {args.name}")
                logger.info(f"Results: {json.dumps(results, indent=2)}")
            else:
                logger.error(f"Failed to process candidate: {results.get('error', 'Unknown error')}")
                
            return
            
        # Initialize scraper
        scraper = MexicanCandidateScraper(
            db_path=args.db,
            candidates_csv=args.csv,
            max_threads=args.threads,
            article_threads=args.article_threads,
            year_range=args.year_range,
            use_oxylabs=not args.no_oxylabs,
            enhanced_search=args.enhanced_search
        )
        
        # Run data validation if requested
        if args.analyze_data:
            logger.info("Analyzing candidates data...")
            scraper.analyze_candidates_data()
            return
            
        if args.validate_db:
            logger.info("Validating database...")
            scraper.validate_database()
            return
            
        if args.clean_only:
            logger.info("Cleaning and validating data...")
            scraper.analyze_candidates_data()
            return
        
        batch_id = None
        
        # Create profiles only if requested
        if args.create_profiles:
            logger.info("Creating candidate profiles...")
            profiles_created = scraper.create_profiles(min_relevance=args.min_relevance)
            logger.info(f"Created/updated {profiles_created} candidate profiles")
        # Export specific batch if requested
        elif args.extract_batch is not None:
            logger.info(f"Exporting results for batch {args.extract_batch}")
            export_paths = scraper.export_results(
                output_path=args.output_path,
                format=args.export_format,
                min_relevance=args.min_relevance,
                batch_id=args.extract_batch
            )
            if export_paths:
                logger.info(f"Exported files: {export_paths}")
            else:
                logger.warning("No results were exported")
        else:
            # Run batch processing
            logger.info("Starting batch processing...")
            batch_id = scraper.run_batch(max_candidates=args.max_candidates)
            
            if batch_id is None:
                logger.warning("Batch processing did not complete successfully")
        
        # Export results if requested
        if args.export and batch_id:
            logger.info(f"Exporting results for batch {batch_id}...")
            export_paths = scraper.export_results(
                output_path=args.output_path,
                format=args.export_format,
                min_relevance=args.min_relevance,
                batch_id=batch_id
            )
            if export_paths:
                logger.info(f"Exported files: {export_paths}")
            else:
                logger.warning("No results were exported")
        
        # Create ML dataset if requested
        if args.create_dataset:
            logger.info("Creating ML dataset...")
            dataset_path = scraper.export_ml_dataset(
                args.output_path,
                args.dataset_format,
                args.min_relevance
            )
            if dataset_path:
                logger.info(f"Created ML dataset: {dataset_path}")
            else:
                logger.warning("Failed to create ML dataset")
        
        logger.info("Scraping completed!")
        
    except Exception as e:
        logger.error(f"Critical error in main function: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    

if __name__ == "__main__":
    main()