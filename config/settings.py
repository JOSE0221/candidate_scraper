"""
Configuration settings for the Mexican Municipal Candidates Scraper.
"""
import os
import json
from pathlib import Path

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

# Oxylabs API credentials
OXYLABS_USERNAME = "jose0221_t0O13"  # Replace with environment variable in production
OXYLABS_PASSWORD = "Tr0p1c0s_159497"  # Replace with environment variable in production
OXYLABS_COUNTRY = "mx"
OXYLABS_API_MODE = "direct"  # 'direct' or 'realtime'

# Scraping settings
DEFAULT_THREADS = 5
ARTICLE_THREADS = 10  # Threads for concurrent article extraction
DEFAULT_YEAR_RANGE = 2
MAX_RETRIES = 3
RETRY_DELAY = 1.5  # seconds
REQUEST_TIMEOUT = 30  # seconds
MAX_RESULTS_PER_CANDIDATE = 10
MIN_RELEVANCE_THRESHOLD = 0.3
MIN_TEMPORAL_RELEVANCE = 0.2
USER_AGENT_ROTATION = True

# Load blacklisted domains from JSON
BLACKLIST_PATH = os.path.join(PROJECT_ROOT, "config", "blacklists.json")

def load_blacklist():
    """Load domain blacklist from JSON file"""
    if os.path.exists(BLACKLIST_PATH):
        try:
            with open(BLACKLIST_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {"domains": []}
    return {"domains": []}

# Default permanent blacklist of domains
DEFAULT_BLACKLIST = [
    {"domain": "facebook.com", "reason": "Social media platform"},
    {"domain": "twitter.com", "reason": "Social media platform"},
    {"domain": "x.com", "reason": "Social media platform (formerly Twitter)"},
    {"domain": "instagram.com", "reason": "Social media platform"},
    {"domain": "youtube.com", "reason": "Video platform"},
    {"domain": "tiktok.com", "reason": "Social media platform"},
    {"domain": "linkedin.com", "reason": "Professional network"},
    {"domain": "pinterest.com", "reason": "Image sharing platform"},
    {"domain": "mercadolibre.com", "reason": "E-commerce site"},
    {"domain": "amazon.com", "reason": "E-commerce site"},
    {"domain": "walmart.com", "reason": "E-commerce site"},
    {"domain": "flickr.com", "reason": "Image hosting platform"},
    {"domain": "reddit.com", "reason": "Social media platform"},
    {"domain": "slideshare.net", "reason": "Presentation platform"},
    {"domain": "issuu.com", "reason": "Publishing platform"},
    {"domain": "spotify.com", "reason": "Music streaming platform"},
    {"domain": "idealista.com", "reason": "Real estate platform"},
    {"domain": "inmuebles24.com", "reason": "Real estate platform"},
    {"domain": "vivastreet.com", "reason": "Classified ads platform"},
    {"domain": "olx.com", "reason": "Classified ads platform"},
    {"domain": "segundamano.mx", "reason": "Classified ads platform"},
    {"domain": "booking.com", "reason": "Travel platform"},
    {"domain": "tripadvisor.com", "reason": "Travel platform"},
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
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
]