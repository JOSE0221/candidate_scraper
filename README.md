# Mexican Municipal Candidates Scraper

A robust, high-performance system for gathering and organizing information about Mexican municipal candidates from diverse web sources, with enhanced temporal relevance filtering focused on election years.

## Features

- **Modular Architecture**: Each component exists in its own file for maintainability and flexibility
- **Multi-level Concurrency**: Parallel processing at both the candidate and article level for maximum performance
- **Robust Database Schema**: Many-to-many relationships prevent data duplication when articles mention multiple candidates
- **Advanced Name Matching**: Uses RapidFuzz for high-performance, approximate name matching with Spanish name patterns
- **Smart Content Classification**: Identifies news articles, opinion pieces, and direct candidate discourse
- **Quote Extraction**: Identifies and extracts direct quotes from candidates
- **Temporal Relevance**: Focuses on content from election year ±2 years with date extraction and validation
- **Entity Recognition**: Identifies Mexican political entities, locations, and context-specific information
- **Comprehensive Error Handling**: Continues processing despite individual failures with detailed logging
- **Flexible Domain Blacklist**: External JSON configuration of domains to skip
- **ML-ready Datasets**: Exports structured data for machine learning projects

## Installation

### Requirements

- Python 3.8+
- SQLite 3
- Internet access for web scraping

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/mexican-candidates-scraper.git
   cd mexican-candidates-scraper
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install required packages:
   ```
   pip install -r requirements.txt
   ```

4. Install optional packages for enhanced functionality:
   ```
   # For improved name matching
   pip install rapidfuzz
   
   # For NLP capabilities
   pip install spacy
   python -m spacy download es_core_news_sm
   
   # For improved date parsing
   pip install dateparser python-dateutil
   
   # For language detection
   pip install langdetect
   
   # For encoding detection
   pip install chardet
   ```

5. Configure settings:
   - Update the Oxylabs credentials in `config/settings.py` if you have an account
   - Customize blacklist domains in `config/blacklists.json`

## Usage

### Basic Usage

Run the scraper with default settings:

```
python main.py --csv path/to/candidates.csv
```

### Command Line Options

```
usage: main.py [-h] [--csv CSV] [--db DB] [--threads THREADS] [--article-threads ARTICLE_THREADS] [--year-range YEAR_RANGE]
               [--max-candidates MAX_CANDIDATES] [--export] [--export-format {csv,json}] [--min-relevance MIN_RELEVANCE] [--no-oxylabs]
               [--enhanced-search] [--create-dataset] [--dataset-format {json,csv}] [--create-profiles] [--output-path OUTPUT_PATH]
               [--extract-batch EXTRACT_BATCH]

Enhanced Mexican Municipal Candidates Web Scraper

optional arguments:
  -h, --help            show this help message and exit
  --csv CSV, -c CSV     Path to candidates CSV file
  --db DB, -d DB        Path to SQLite database
  --threads THREADS, -t THREADS
                        Maximum number of concurrent threads for candidates
  --article-threads ARTICLE_THREADS, -a ARTICLE_THREADS
                        Maximum number of concurrent threads for articles
  --year-range YEAR_RANGE, -y YEAR_RANGE
                        Year range for temporal filtering (±)
  --max-candidates MAX_CANDIDATES, -m MAX_CANDIDATES
                        Maximum number of candidates to process (0 for all)
  --export, -e          Export results after processing
  --export-format {csv,json}, -f {csv,json}
                        Export format (csv or json)
  --min-relevance MIN_RELEVANCE, -r MIN_RELEVANCE
                        Minimum relevance threshold for exports
  --no-oxylabs          Disable Oxylabs API
  --enhanced-search     Use enhanced search strategies
  --create-dataset      Create a consolidated dataset for ML/NLP
  --dataset-format {json,csv}
                        Dataset format
  --create-profiles     Create candidate profiles without running scraper
  --output-path OUTPUT_PATH
                        Path for exported files
  --extract-batch EXTRACT_BATCH
                        Export results for a specific batch
```

### CSV Format

The input CSV should contain at least these columns:
- `PRESIDENTE_MUNICIPAL`: Candidate name
- `MUNICIPIO`: Municipality name
- `Year`: Election year
- `ENTIDAD`: State (optional)
- `SEXO`: Gender (optional)
- `PARTIDO`: Political party (optional)
- `PERIODO_FORMATO_ORIGINAL`: Original period format (optional)

### Examples

1. Run with enhanced search and export results:
   ```
   python main.py --csv data/candidates.csv --enhanced-search --export
   ```

2. Process a limited number of candidates:
   ```
   python main.py --csv data/candidates.csv --max-candidates 10
   ```

3. Create ML dataset without running scraper:
   ```
   python main.py --create-dataset --dataset-format json --output-path data/ml
   ```

4. Export specific batch results:
   ```
   python main.py --extract-batch 5 --export-format csv
   ```

## Project Structure

```
mexican_candidates_scraper/
│
├── config/
│   ├── __init__.py
│   ├── settings.py                 # Configuration settings
│   └── blacklists.json             # Domain blacklist
│
├── database/
│   ├── __init__.py
│   ├── db_manager.py               # Database operations
│   ├── models.py                   # Database schema models
│   └── migrations.py               # Database migration utilities
│
├── scrapers/
│   ├── __init__.py
│   ├── oxylabs_manager.py          # Oxylabs API integration
│   ├── content_extractor.py        # Content extraction logic
│   └── search_engine.py            # Search engine functionality
│
├── processing/
│   ├── __init__.py
│   ├── content_classifier.py       # Content classification
│   ├── entity_recognizer.py        # Entity recognition
│   ├── quote_extractor.py          # Quote extraction
│   └── name_matcher.py             # Fuzzy name matching
│
├── utils/
│   ├── __init__.py
│   ├── logger.py                   # Logging configuration
│   ├── metrics.py                  # Performance metrics
│   └── helpers.py                  # Helper functions
│
├── __init__.py
├── main.py                         # Entry point
├── requirements.txt                # Dependencies
└── README.md                       # Documentation
```

## Output

### Database

The main database contains these primary tables:
- `candidates`: Information about municipal candidates
- `articles`: Articles extracted from the web
- `candidate_articles`: Many-to-many link between candidates and articles
- `quotes`: Extracted quotes from candidates
- `entities`: Named entities extracted from articles
- `scraping_batches`: Batch processing information
- `candidate_profiles`: Pre-aggregated candidate profiles

### Exports

The scraper can export:
1. Full article data (CSV/JSON)
2. Candidate profiles (JSON)
3. Quotes (CSV)
4. ML-ready dataset (CSV/JSON)

## Performance Considerations

- **CPU-bound operations**: The scraper is optimized for multi-core CPUs with parallel processing
- **Network-bound operations**: Adjust threads based on your internet connection and API rate limits
- **Memory usage**: Large content extraction may use significant memory
- **Disk space**: The database can grow large with many candidates and articles

## Extending the Project

The modular design makes it easy to extend functionality:
- Add new content sources by creating a new search engine in `scrapers/`
- Enhance entity recognition by expanding patterns in `processing/entity_recognizer.py`
- Improve classification by adding new rules to `processing/content_classifier.py`
- Add new export formats by extending methods in `main.py`

## Troubleshooting

Common issues and solutions:

1. **Rate limiting**: If experiencing API blocks:
   - Reduce concurrency with `--threads` and `--article-threads`
   - Add a proxy service

2. **Memory errors**:
   - Process fewer candidates at once with `--max-candidates`
   - Reduce article concurrency with `--article-threads`

3. **Incorrect content extraction**:
   - Check domain blacklists in `config/blacklists.json`
   - Inspect logs for encoding errors

4. **Low-quality results**:
   - Increase minimum relevance with `--min-relevance`
   - Enable enhanced search with `--enhanced-search`

## License

[MIT License](LICENSE)