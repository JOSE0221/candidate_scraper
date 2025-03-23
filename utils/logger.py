"""
Centralized logging configuration for the Mexican Municipal Candidates Scraper.
"""
import os
import logging
import logging.handlers
from datetime import datetime
import sys
from pathlib import Path

# Import settings
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import LOGS_DIR

class CustomFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""
    
    COLORS = {
        'DEBUG': '\033[94m',  # Blue
        'INFO': '\033[92m',   # Green
        'WARNING': '\033[93m', # Yellow
        'ERROR': '\033[91m',  # Red
        'CRITICAL': '\033[91m\033[1m', # Bold Red
        'RESET': '\033[0m'    # Reset
    }
    
    def format(self, record):
        log_message = super().format(record)
        if hasattr(sys, 'ps1'):  # Check if running in interactive mode
            return f"{self.COLORS.get(record.levelname, '')}{log_message}{self.COLORS['RESET']}"
        return log_message

def setup_logger(name=None, level=logging.INFO, log_file=None):
    """
    Set up and configure a logger.
    
    Args:
        name (str, optional): Logger name. If None, returns the root logger.
        level (int, optional): Logging level. Defaults to INFO.
        log_file (str, optional): Path to log file. If None, auto-generates based on date.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get or create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicate logs
    if logger.handlers:
        logger.handlers.clear()
    
    # Create console handler with custom formatter
    console_handler = logging.StreamHandler()
    console_formatter = CustomFormatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if specified
    if log_file is None:
        # Auto-generate log filename with date
        date_str = datetime.now().strftime('%Y%m%d')
        log_file = os.path.join(LOGS_DIR, f"mexican_candidates_{date_str}.log")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Set up file handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger

# Create root logger
root_logger = setup_logger()

def get_logger(name=None):
    """
    Get a logger instance. Uses the hierarchical logging system.
    
    Args:
        name (str, optional): Logger name. If None, returns the root logger.
    
    Returns:
        logging.Logger: Logger instance
    """
    if name is None:
        return root_logger
    return logging.getLogger(name)