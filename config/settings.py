"""
Configuration settings for the Mexican Municipal Candidates Scraper.
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# Database settings
DEFAULT_DB_PATH = os.path.join(DATA_DIR, "mexican_candidates.db")

# Oxylabs API credentials - using environment variables
OXYLABS_USERNAME = os.getenv("OXYLABS_USERNAME", "jose0221_t0O13")  # Use env var or fallback
OXYLABS_PASSWORD = os.getenv("OXYLABS_PASSWORD", "Tr0p1c0s_159497")  # Use env var or fallback
OXYLABS_COUNTRY = "mx"
OXYLABS_API_MODE = "direct"  # 'direct' or 'realtime'

# Scraping settings
DEFAULT_THREADS = 5
ARTICLE_THREADS = 10  # Threads for concurrent article extraction
DEFAULT_YEAR_RANGE = 3  # Increased from 2
MAX_RETRIES = 5  # Increased from 3
RETRY_DELAY = 1.5  # seconds
REQUEST_TIMEOUT = 30  # seconds
MAX_RESULTS_PER_CANDIDATE = 15  # Increased from 10
MIN_RELEVANCE_THRESHOLD = 0.2  # Reduced from 0.3
MIN_TEMPORAL_RELEVANCE = 0.1  # Reduced from 0.2
USER_AGENT_ROTATION = True

# Load blacklisted domains from JSON
BLACKLIST_PATH = os.path.join(PROJECT_ROOT, "config", "blacklists.json")

def load_blacklist():
    """Load domain blacklist from JSON file with reduced strictness"""
    if os.path.exists(BLACKLIST_PATH):
        try:
            with open(BLACKLIST_PATH, 'r', encoding='utf-8') as f:
                blacklist_data = json.load(f)
                
                # Filter out non-essential blacklist entries to reduce strictness
                essential_domains = [
                    "facebook.com", "twitter.com", "x.com", "instagram.com", 
                    "youtube.com", "tiktok.com", "linkedin.com", "pinterest.com",
                    "google.com", "googleusercontent.com", "googleapis.com"
                ]
                
                filtered_domains = [
                    domain_entry for domain_entry in blacklist_data.get("domains", [])
                    if domain_entry.get("domain") in essential_domains
                ]
                
                return {"domains": filtered_domains}
        except json.JSONDecodeError:
            return {"domains": []}
    return {"domains": []}

# Reduced permanent blacklist of domains - only keep essential ones
DEFAULT_BLACKLIST = [
    {"domain": "facebook.com", "reason": "Social media platform"},
    {"domain": "twitter.com", "reason": "Social media platform"},
    {"domain": "x.com", "reason": "Social media platform (formerly Twitter)"},
    {"domain": "instagram.com", "reason": "Social media platform"},
    {"domain": "youtube.com", "reason": "Video platform"},
    {"domain": "tiktok.com", "reason": "Social media platform"},
    {"domain": "linkedin.com", "reason": "Professional network"},
    {"domain": "pinterest.com", "reason": "Image sharing platform"},
    {"domain": "googleusercontent.com", "reason": "Google storage domain"},
    {"domain": "googleapis.com", "reason": "Google API domain"},
    {"domain": "google.com", "reason": "Search engine"},
]

# Mexican states mapping
MEXICAN_STATES = {
    "AGU": "Aguascalientes",
    "BCN": "Baja California",
    "BCS": "Baja California Sur",
    "CAM": "Campeche",
    "CHP": "Chiapas",
    "CHH": "Chihuahua",
    "CDMX": "Ciudad de México",
    "DUR": "Durango",
    "GUA": "Guanajuato",
    "GRO": "Guerrero",
    "HID": "Hidalgo",
    "JAL": "Jalisco",
    "MEX": "Estado de México",
    "MIC": "Michoacán",
    "MOR": "Morelos",
    "NAY": "Nayarit",
    "NLE": "Nuevo León",
    "OAX": "Oaxaca",
    "PUE": "Puebla",
    "QUE": "Querétaro",
    "ROO": "Quintana Roo",
    "SLP": "San Luis Potosí",
    "SIN": "Sinaloa",
    "SON": "Sonora",
    "TAB": "Tabasco",
    "TAM": "Tamaulipas",
    "TLA": "Tlaxcala",
    "VER": "Veracruz",
    "YUC": "Yucatán",
    "ZAC": "Zacatecas"
}

# Mexican political parties
POLITICAL_PARTIES = {
    "PRI": ["Partido Revolucionario Institucional", "PRI", "priista", "priístas"],
    "PAN": ["Partido Acción Nacional", "PAN", "panista", "panistas"],
    "PRD": ["Partido de la Revolución Democrática", "PRD", "perredista"],
    "MORENA": ["Movimiento Regeneración Nacional", "MORENA", "morenista"],
    "PT": ["Partido del Trabajo", "PT", "petista"],
    "PVEM": ["Partido Verde Ecologista de México", "PVEM", "Verde"],
    "MC": ["Movimiento Ciudadano", "MC"],
    "PANAL": ["Nueva Alianza", "PANAL"],
    "PES": ["Partido Encuentro Social", "PES"],
    "RSP": ["Redes Sociales Progresistas", "RSP"],
    "FXM": ["Fuerza por México", "FXM"]
}

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.79 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36 OPR/85.0.4341.75'
]