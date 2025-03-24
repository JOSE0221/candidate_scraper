"""
Mexican entity recognizer for the Municipal Candidates Scraper.

This module provides specialized recognition of Mexican political entities,
locations, and temporal context with awareness of election cycles.
"""
import re
import datetime
import sys
from pathlib import Path
import pandas as pd
import warnings
import traceback

# Import project modules
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger
from config.settings import MEXICAN_STATES, POLITICAL_PARTIES
from processing.name_matcher import create_name_matcher

logger = get_logger(__name__)

# Try to import optional dependencies
try:
    import dateparser
    DATEPARSER_AVAILABLE = True
except ImportError:
    DATEPARSER_AVAILABLE = False
    logger.warning("dateparser not available. Install with 'pip install dateparser' for better date extraction.")

try:
    import dateutil.parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    logger.warning("dateutil not available. Install with 'pip install python-dateutil' for better date extraction.")

# Try to import spaCy
try:
    import spacy
    try:
        NLP_MODEL = spacy.load("es_core_news_sm")
        SPACY_AVAILABLE = True
        logger.info("Loaded Spanish spaCy model for entity recognition")
    except:
        SPACY_AVAILABLE = False
        NLP_MODEL = None
        logger.warning("Spanish spaCy model not available. Install with 'python -m spacy download es_core_news_sm'")
except ImportError:
    SPACY_AVAILABLE = False
    NLP_MODEL = None
    logger.warning("spaCy not available. Install with 'pip install spacy' for better entity recognition")


class MexicanEntityRecognizer:
    """
    Specialized entity recognizer for Mexican political entities,
    with temporal context awareness for election cycles.
    """
    
    def __init__(self, candidates_data=None, nlp_model=None, name_matcher=None):
        """
        Initialize the Mexican entity recognizer.
        
        Args:
            candidates_data (pandas.DataFrame, optional): Candidates data for context
            nlp_model: Optional NLP model for enhanced recognition
            name_matcher: Name matcher instance
        """
        self.candidates_data = candidates_data
        self.nlp_model = nlp_model or NLP_MODEL
        self.name_matcher = name_matcher or create_name_matcher()
        
        # Initialize Mexican political entities knowledge base
        self.initialize_knowledge_base()
        
        # Initialize date patterns
        self.initialize_date_patterns()
    
    def initialize_knowledge_base(self):
        """
        Initialize knowledge base with Mexican political entities.
        
        This method sets up all reference data needed for entity recognition,
        handling data quality issues and providing detailed logging.
        """
        # Mexican political parties from config
        self.political_parties = POLITICAL_PARTIES
        
        # Mexican states from config
        self.mexican_states = MEXICAN_STATES
        
        # Mexican political positions - focus on candidate-related terms
        self.political_positions = [
            "candidato", "candidata", "candidato a la alcaldía", "candidata a la alcaldía",
            "aspirante", "precandidato", "precandidata", "contendiente", "postulante",
            "candidato a la presidencia municipal", "candidata a la presidencia municipal",
            "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal",
            "edil", "regidor", "regidora", "síndico", "síndica", "cabildo"
        ]
        
        # Election-related terms
        self.election_terms = [
            "elección", "elecciones", "electoral", "campaña", "voto", "votos", "votación",
            "urnas", "comicio", "comicios", "INE", "instituto electoral", "proceso electoral",
            "jornada electoral", "candidatura", "planilla", "triunfo electoral", "victoria electoral",
            "ganó la elección", "ganó las elecciones", "resultados electorales", "casilla", "casillas",
            "padrón electoral", "boleta", "boletas", "distrito electoral", "sección electoral"
        ]
        
        # Temporal/period-related terms
        self.temporal_terms = [
            "período", "periodo", "campaña electoral", "jornada electoral", "contienda electoral",
            "durante la campaña", "proceso electoral", "previo a la elección",
            "después de la elección", "antes de la elección", "tras la elección",
            "ciclo electoral", "proceso de selección de candidatos", "recta final de la campaña",
            "trienio", "gestión", "administración", "gobierno municipal", "ayuntamiento",
            "toma de posesión", "toma de protesta", "inicio de gobierno", "fin de gobierno"
        ]
        
        # Academic background terms
        self.academic_terms = [
            "licenciatura", "licenciado", "licenciada", "maestría", "doctorado", "posgrado",
            "ingeniero", "ingeniera", "abogado", "abogada", "médico", "médica", "doctor", "doctora",
            "título universitario", "estudios", "carrera", "profesión", "universidad", "UNAM", "IPN",
            "egresado", "egresada", "graduado", "graduada", "diplomado", "especialización",
            "bachillerato", "preparatoria", "técnico", "técnica", "formación académica",
            "educación superior", "facultad", "escuela", "titulación"
        ]
        
        # Professional experience terms
        self.professional_terms = [
            "experiencia profesional", "trayectoria profesional", "carrera profesional",
            "puesto", "cargo", "empleo", "empresario", "empresaria", "sector privado",
            "compañía", "empresa", "negocio", "corporativo", "gerente", "director", "directora",
            "coordinador", "coordinadora", "jefe", "jefa", "ejecutivo", "ejecutiva",
            "fundador", "fundadora", "consultor", "consultora", "asesor", "asesora",
            "industria", "comercio", "servicios", "despacho", "firma", "gestor", "gestora"
        ]
        
        # Public service terms
        self.public_service_terms = [
            "servidor público", "servidora pública", "funcionario público", "funcionaria pública",
            "servicio público", "administración pública", "gobierno federal", "gobierno estatal",
            "gobierno municipal", "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal",
            "diputado", "diputada", "senador", "senadora", "legislador", "legisladora",
            "regidor", "regidora", "síndico", "síndica", "concejal", "edil", "cabildo",
            "secretario", "secretaria", "director general", "directora general", "subsecretario", "subsecretaria",
            "delegado", "delegada", "procurador", "procuradora", "oficial mayor", "tesorero", "tesorera"
        ]
        
        # Initialize candidate data structures with robust error handling
        self.candidate_names = set()
        self.municipalities = set()
        self.candidate_to_municipality = {}
        
        # Build entity dictionaries from CSV data if available
        if self.candidates_data is not None:
            try:
                # Get candidate names with validation
                valid_candidates_mask = self.candidates_data['PRESIDENTE_MUNICIPAL'].apply(
                    lambda x: isinstance(x, str) and bool(x.strip())
                )
                
                if valid_candidates_mask.any():
                    valid_candidates = self.candidates_data.loc[valid_candidates_mask, 'PRESIDENTE_MUNICIPAL']
                    self.candidate_names = set(valid_candidates.str.strip())
                    logger.info(f"Extracted {len(self.candidate_names)} valid candidate names")
                else:
                    logger.warning("No valid candidate names found in data")
                
                # Get municipalities with validation
                valid_municipalities_mask = self.candidates_data['MUNICIPIO'].apply(
                    lambda x: isinstance(x, str) and bool(x.strip())
                )
                
                if valid_municipalities_mask.any():
                    valid_municipalities = self.candidates_data.loc[valid_municipalities_mask, 'MUNICIPIO']
                    self.municipalities = set(valid_municipalities.str.strip())
                    logger.info(f"Extracted {len(self.municipalities)} valid municipalities")
                else:
                    logger.warning("No valid municipalities found in data")
                
                # Create mapping from candidates to municipalities with validation
                for _, row in self.candidates_data.iterrows():
                    candidate_name = row.get('PRESIDENTE_MUNICIPAL')
                    municipality = row.get('MUNICIPIO')
                    
                    # Validate both fields are present and are strings
                    if not (isinstance(candidate_name, str) and isinstance(municipality, str)):
                        continue
                    
                    candidate_name = candidate_name.strip()
                    municipality = municipality.strip()
                    
                    # Skip empty strings
                    if not candidate_name or not municipality:
                        continue
                    
                    if candidate_name not in self.candidate_to_municipality:
                        self.candidate_to_municipality[candidate_name] = set()
                    
                    self.candidate_to_municipality[candidate_name].add(municipality)
                
                logger.info(f"Created mapping for {len(self.candidate_to_municipality)} candidates to municipalities")
                
            except Exception as e:
                logger.error(f"Error building entity dictionaries: {str(e)}")
                traceback.print_exc()
                # Initialize empty sets if error occurs
                self.candidate_names = set()
                self.municipalities = set()
                self.candidate_to_municipality = {}
    
    def initialize_date_patterns(self):
        """
        Initialize date extraction patterns.
        """
        # Enhanced Spanish date patterns
        self.date_patterns = [
            # Formal date formats
            r'(\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4})',
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{1,2}-\d{1,2}-\d{4})',
            r'(\d{4}/\d{1,2}/\d{1,2})',
            r'(\d{4}-\d{1,2}-\d{1,2})',
            
            # Date with time
            r'(\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4},?\s+\d{1,2}:\d{2})',
            r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2})',
            
            # Year references
            r'(?:en\sel\saño|durante|del\saño|año)\s+(\d{4})',
            r'(?:periodo|mandato|gestión)\s+(\d{4})[-\s]+(\d{4})',
            
            # Specific election/term references
            r'(?:(?:elecciones|votaciones|comicios).*?)(\d{4})',
            r'(?:elegido|electa|electo).*?(\d{4})',
            r'(?:ganó|triunfó|venció).*?(\d{4})',
            r'(?:candidato|candidata).*?(\d{4})',
            
            # Mexican-specific date formats
            r'(?:(?:Cd\.|Ciudad)\s+de\s+México,?\s+a\s+)?(\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4})',
            r'(?:trienio|mandato)\s+(\d{4})[-\s](\d{4})',
            r'(?:elecciones|votaciones|comicios)\s+(?:de|del)\s+(\d{4})',
            r'(?:campaña)\s+(\d{4})[-\s](\d{4})',
            
            # Additional period formats
            r'(?:del|de)\s+(\d{1,2})\s+(?:de|-)?\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de|del)?\s+(\d{4})\s+(?:al|a)\s+(\d{1,2})\s+(?:de|-)?\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de|del)?\s+(\d{4})',
            r'(?:del?)\s+(\d{4})\s+(?:al?)\s+(\d{4})',
            r'(\d{4})-(\d{4})',
            r'(\d{4})/(\d{4})'
        ]
        
        # Compile patterns for performance
        self.compiled_date_patterns = [re.compile(pattern) for pattern in self.date_patterns]
    
    def extract_entities(self, text, candidate_name=None, municipality=None):
        """
        Extract Mexican political entities from text.
        
        Args:
            text (str): Text to analyze
            candidate_name (str, optional): Candidate name for targeted extraction
            municipality (str, optional): Municipality name for context
            
        Returns:
            dict: Dictionary of extracted entities by type
        """
        if not text:
            return {}
        
        # Initialize results dictionary
        entities = {
            'PERSON': {},           # Detected persons
            'POLITICAL_PARTY': {},  # Political parties
            'POSITION': {},         # Political positions
            'LOCATION': {},         # Locations including municipalities
            'DATE': {},             # Dates mentioned
            'ELECTION_TERM': {},    # Election-related terms
            'TEMPORAL_TERM': {},    # Period/administration related terms
            'ACADEMIC': {},         # Academic background
            'PROFESSIONAL': {},     # Professional experience
            'PUBLIC_SERVICE': {}    # Public service experience
        }
        
        # Use spaCy if available
        if SPACY_AVAILABLE and self.nlp_model:
            try:
                doc = self.nlp_model(text[:20000])  # Process first 20k chars to avoid memory issues
                
                # Extract entities detected by spaCy
                for ent in doc.ents:
                    if ent.label_ == 'PER':
                        entities['PERSON'][ent.text] = 1.0  # Base score
                    elif ent.label_ in ['LOC', 'GPE']:
                        entities['LOCATION'][ent.text] = 1.0  # Base score
                    elif ent.label_ == 'DATE':
                        entities['DATE'][ent.text] = 1.0  # Base score
            except Exception as e:
                logger.warning(f"Error in spaCy entity extraction: {str(e)}")
        
        # Find political parties
        for party_key, party_terms in self.political_parties.items():
            for term in party_terms:
                if term.lower() in text.lower():
                    # If party is found, add it with a score
                    entities['POLITICAL_PARTY'][party_key] = 1.0
        
        # Find political positions
        for position in self.political_positions:
            if position.lower() in text.lower():
                entities['POSITION'][position] = 1.0
        
        # Find election terms
        for term in self.election_terms:
            if term.lower() in text.lower():
                entities['ELECTION_TERM'][term] = 1.0
                
        # Find temporal/period terms
        for term in self.temporal_terms:
            if term.lower() in text.lower():
                entities['TEMPORAL_TERM'][term] = 1.0
                
        # Find academic terms
        for term in self.academic_terms:
            if term.lower() in text.lower():
                entities['ACADEMIC'][term] = 1.0
                
        # Find professional experience terms
        for term in self.professional_terms:
            if term.lower() in text.lower():
                entities['PROFESSIONAL'][term] = 1.0
                
        # Find public service terms
        for term in self.public_service_terms:
            if term.lower() in text.lower():
                entities['PUBLIC_SERVICE'][term] = 1.0
        
        # Check for specific candidate name
        if candidate_name:
            # Look for candidate name in text (with fuzzy matching)
            found, score, match = self.name_matcher.fuzzy_match_name(text, candidate_name)
            if found:
                # Convert score to 0-1 scale
                entities['PERSON'][candidate_name] = score / 100 * 2  # Higher score for target candidate
        
        # Check for specific municipality
        if municipality:
            mun_lower = municipality.lower()
            if mun_lower in text.lower():
                entities['LOCATION'][municipality] = 2.0  # Higher score for target municipality
            
            # Check state corresponding to municipality
            # Could be enhanced with a proper municipality-to-state mapping
            for state_code, state_name in self.mexican_states.items():
                if state_name.lower() in text.lower():
                    entities['LOCATION'][state_name] = 1.5
        
        return entities
    
    def extract_date_from_text(self, text):
        """
        Extract dates from text using regular expressions and date parsing.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            str or None: Extracted date in ISO format, or None if not found
        """
        if not text:
            return None
        
        # First try with regex patterns for Spanish dates
        for pattern in self.compiled_date_patterns:
            try:
                matches = pattern.findall(text)
                if matches:
                    for match in matches:
                        # If we found a period, use the first year
                        if isinstance(match, tuple) and len(match) > 1:
                            # Store all years found in the tuple
                            years = []
                            for item in match:
                                # Try to extract a year from the item
                                year_match = re.search(r'(\d{4})', item)
                                if year_match:
                                    years.append(int(year_match.group(1)))
                            
                            # If we found multiple years, use the earliest one
                            if years:
                                # First try to validate the years are reasonable
                                valid_years = [y for y in years if 1980 <= y <= datetime.datetime.now().year]
                                if valid_years:
                                    min_year = min(valid_years)
                                    return f"{min_year}-01-01"
                                continue  # No valid years, try next match
                            
                            match = match[0]  # Default to first item in tuple if no years found
                        
                        try:
                            # Try to parse the matched date string
                            if DATEPARSER_AVAILABLE:
                                with warnings.catch_warnings():
                                    warnings.simplefilter("ignore")
                                    parsed_date = dateparser.parse(match, languages=['es'])
                                    if parsed_date and parsed_date.year > 1980 and parsed_date.year <= datetime.datetime.now().year:
                                        return parsed_date.strftime('%Y-%m-%d')
                            elif DATEUTIL_AVAILABLE:
                                with warnings.catch_warnings():
                                    warnings.simplefilter("ignore")
                                    parsed_date = dateutil.parser.parse(match)
                                    if parsed_date.year > 1980 and parsed_date.year <= datetime.datetime.now().year:
                                        return parsed_date.strftime('%Y-%m-%d')
                        except:
                            continue
            except:
                continue
        
        # Look for standalone years in text
        try:
            year_pattern = r'\b(19[8-9]\d|20[0-2]\d)\b'  # Years 1980-2029
            year_matches = re.findall(year_pattern, text)
            if year_matches:
                # Get the most frequent year
                from collections import Counter
                year_counts = Counter(year_matches)
                most_common_year = year_counts.most_common(1)[0][0]
                return f"{most_common_year}-01-01"
        except:
            pass
                
        return None
    
    def calculate_temporal_relevance(self, content, extracted_date, target_year, year_range=2, period_original=None):
        """
        Calculate the temporal relevance with enhanced logic for election year ±2 bounds.
        
        Args:
            content (str): Content text
            extracted_date (str): Extracted date in ISO format
            target_year (int): Target election year
            year_range (int, optional): Year range for filtering
            period_original (str, optional): Original period format
            
        Returns:
            tuple: (score, year_lower_bound, year_upper_bound)
        """
        if not content or not target_year:
            return 0.5, None, None  # Neutral score if no target year
        
        # Initialize year bounds
        year_lower_bound = target_year - year_range if year_range else target_year
        year_upper_bound = target_year + year_range if year_range else target_year
        
        # Parse original period if available
        if period_original:
            try:
                # Try to extract years from common Mexican period formats
                period_years = re.findall(r'(\d{4})', period_original)
                if len(period_years) >= 2:
                    # Update bounds from period if available
                    parsed_years = [int(y) for y in period_years]
                    period_start = min(parsed_years)
                    period_end = max(parsed_years)
                    
                    # Use the period bounds if they're more specific than our defaults
                    if period_start <= target_year <= period_end:
                        year_lower_bound = period_start
                        year_upper_bound = period_end
            except:
                pass
        
        # Convert target_year to string for comparisons
        target_year_str = str(target_year)
        years_in_range = [str(y) for y in range(year_lower_bound, year_upper_bound + 1)]
        
        # Start with a base score
        score = 0.5
        
        # If we have an extracted date, calculate based on proximity
        if extracted_date:
            try:
                content_year = int(extracted_date.split('-')[0])
                
                # Check if the content is within our target range
                if year_lower_bound <= content_year <= year_upper_bound:
                    # Calculate relevance based on proximity to target year
                    year_diff = abs(content_year - target_year)
                    if year_diff == 0:
                        score = 1.0  # Exact year match
                    elif year_diff == 1:
                        score = 0.9  # Off by 1 year
                    elif year_diff == 2:
                        score = 0.8  # Off by 2 years
                    else:
                        score = 0.7  # Within extended range
                elif content_year < year_lower_bound:
                    # Pre-election content
                    years_before = year_lower_bound - content_year
                    if years_before <= 1:
                        score = 0.6  # Just before our range
                    elif years_before <= 3:
                        score = 0.4  # Somewhat before our range
                    else:
                        score = 0.2  # Well before our range
                else:  # content_year > year_upper_bound
                    # Post-election content
                    years_after = content_year - year_upper_bound
                    if years_after <= 1:
                        score = 0.7  # Just after our range
                    elif years_after <= 3:
                        score = 0.5  # Somewhat after our range
                    else:
                        score = 0.3  # Well after our range
            except:
                # If we can't parse the year, look for year mentions in content
                if target_year_str in content:
                    score = 0.8  # Target year is mentioned in content
                elif any(year in content for year in years_in_range):
                    score = 0.7  # Any year in our range is mentioned
        else:
            # No extracted date, check for year mentions
            # Prioritize explicit mentions of the target year
            if target_year_str in content:
                score = 0.8
                
                # Check for election-specific phrases with the target year
                election_year_phrases = [
                    f"elecciones de {target_year_str}", 
                    f"elección de {target_year_str}",
                    f"comicios de {target_year_str}", 
                    f"votación de {target_year_str}",
                    f"campaña de {target_year_str}",
                    f"candidato en {target_year_str}",
                    f"candidata en {target_year_str}"
                ]
                
                for phrase in election_year_phrases:
                    if re.search(phrase, content, re.IGNORECASE):
                        score += 0.1  # Increase score for election context
                        if score >= 0.95:
                            score = 1.0
                            break
                            
            # Check for mentions of years in our target range
            elif any(year in content for year in years_in_range):
                score = 0.7
                
                # Find which years in our range are mentioned
                mentioned_years = [year for year in years_in_range if year in content]
                
                # If multiple years in range are mentioned, likely more relevant
                if len(mentioned_years) > 1:
                    score += 0.1
                    
                # Check for period references
                period_patterns = [
                    r'(\d{4})-(\d{4})',
                    r'(\d{4})/(\d{4})',
                    r'de (\d{4}) a (\d{4})',
                    r'del (\d{4}) al (\d{4})'
                ]
                
                for pattern in period_patterns:
                    periods = re.findall(pattern, content)
                    for period_match in periods:
                        try:
                            period_start = int(period_match[0])
                            period_end = int(period_match[1])
                            # If our target year falls within this period, increase relevance
                            if period_start <= target_year <= period_end:
                                score += 0.1
                                break
                        except:
                            continue
            
            # Check for explicit electoral terms
            electoral_terms = [
                "elección municipal", "elecciones municipales",
                "candidato", "candidata", "candidatos", "candidatas",
                "candidato a la presidencia municipal", "candidata a la presidencia municipal",
                "ganó la elección", "ganó las elecciones",
                "campaña electoral", "proceso electoral", "jornada electoral"
            ]
            
            if any(term.lower() in content.lower() for term in electoral_terms):
                score += 0.1  # Slight boost for electoral context
        
        return score, year_lower_bound, year_upper_bound
    
    def calculate_biographical_score(self, text, candidate_name):
        """
        Calculate how biographical the content is for a specific candidate.
        
        Args:
            text (str): Content text
            candidate_name (str): Candidate name
            
        Returns:
            float: Biographical score (0-1)
        """
        if not text or not candidate_name:
            return 0.0
        
        score = 0.0
        
        # Check for candidate name
        found, match_score, _ = self.name_matcher.fuzzy_match_name(text, candidate_name)
        if found:
            score += 0.4 * (match_score / 100)
        
        # Look for biographical indicators
        biographical_terms = [
            "nació", "nacido en", "originario de", "oriundo de", "natal de",
            "edad", "familia", "casado", "casada", "hijo", "hija", "padre", "madre",
            "estudió", "educación", "formación", "se graduó", "egresado", "egresada",
            "profesión", "trayectoria", "carrera", "experiencia", "biografía", "perfil",
            "currículum", "cv", "licenciado", "licenciada", "doctor", "doctora", 
            "vida personal", "orígenes", "infancia", "antecedentes", "formación académica"
        ]
        
        text_lower = text.lower()
        for term in biographical_terms:
            if term in text_lower:
                score += 0.1
                if score >= 0.8:  # Cap at 0.8 for terms
                    break
        
        # Check for patterns of biographical information
        bio_patterns = [
            r'nació\s+en\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s,]+(?:en|el)\s+\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+\d{4}',
            r'(?:cursó|estudió)\s+(?:la|el)\s+(?:licenciatura|maestría|doctorado|carrera)\s+(?:en|de)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+\s+(?:en|por)\s+(?:la|el)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+',
            r'(?:es|tiene)\s+(?:licenciado|licenciada|ingeniero|ingeniera|doctor|doctora|maestro|maestra|profesor|profesora)\s+(?:en|de)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+\s+por\s+(?:la|el)\s+[A-Za-zñáéíóúÁÉÍÓÚüÜ\s]+'
        ]
        
        for pattern in bio_patterns:
            if re.search(pattern, text_lower):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_political_score(self, text, candidate_name=None, municipality=None):
        """
        Calculate how political/electoral the content is.
        
        Args:
            text (str): Content text
            candidate_name (str, optional): Candidate name
            municipality (str, optional): Municipality name
            
        Returns:
            float: Political score (0-1)
        """
        if not text:
            return 0.0
        
        score = 0.0
        text_lower = text.lower()
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name, municipality)
        
        # Check for political parties
        if entities['POLITICAL_PARTY']:
            score += 0.3
        
        # Check for political positions
        if entities['POSITION']:
            score += 0.3
        
        # Check for election terms
        if entities['ELECTION_TERM']:
            score += 0.2
        
        # Check for temporal/administrative terms
        if entities['TEMPORAL_TERM']:
            score += 0.2
        
        # Check for municipality mention
        if municipality and municipality.lower() in text_lower:
            score += 0.2
        
        # Check for election-specific patterns
        election_patterns = [
            r'gan(?:ó|a|ar|aron)\s+(?:la|las)\s+elecci(?:ón|ones)',
            r'candida(?:to|ta)\s+(?:a|por|del|de la|de)\s+(?:la\s+)?(?:alcaldía|presidencia\s+municipal|municipio)',
            r'compa(?:ñó|ña|ñar|ñaron)\s+(?:en|durante)\s+(?:la|su)\s+campaña',
            r'result(?:ó|a|ar|aron)\s+elect(?:o|a|os|as)',
            r'triun(?:fó|fa|far|faron)\s+en\s+(?:la|las)\s+(?:urnas|elecci(?:ón|ones)|votaci(?:ón|ones))'
        ]
        
        for pattern in election_patterns:
            if re.search(pattern, text_lower):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_academic_score(self, text, candidate_name=None):
        """
        Calculate how academic/educational the content is.
        
        Args:
            text (str): Content text
            candidate_name (str, optional): Candidate name
            
        Returns:
            float: Academic score (0-1)
        """
        if not text:
            return 0.0
        
        score = 0.0
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name)
        
        # Check for academic terms
        if entities['ACADEMIC']:
            score += min(0.6, 0.1 * len(entities['ACADEMIC']))
        
        # Check for university mentions
        universities = [
            "unam", "universidad nacional autónoma de méxico",
            "ipn", "instituto politécnico nacional",
            "uam", "universidad autónoma metropolitana",
            "itesm", "tecnológico de monterrey",
            "universidad iberoamericana", "universidad anáhuac",
            "colmex", "colegio de méxico",
            "itam", "instituto tecnológico autónomo de méxico",
            "universidad veracruzana", "universidad de guadalajara",
            "buap", "benemérita universidad autónoma de puebla",
            "universidad autónoma de nuevo león", "uanl"
        ]
        
        text_lower = text.lower()
        for uni in universities:
            if uni in text_lower:
                score += 0.1
                if score >= 0.8:
                    break
        
        # Check for academic degree patterns
        academic_patterns = [
            r'(?:obtuvo|recibió|cuenta con)\s+(?:el|la|un|una)\s+(?:título|grado|licenciatura|maestría|doctorado)',
            r'(?:es|está)\s+(?:graduado|graduada|titulado|titulada)\s+(?:como|en|de)',
            r'(?:realizó|cursó|estudió)\s+(?:sus\s+estudios|la\s+carrera)',
            r'(?:licenciado|licenciada|ingeniero|ingeniera|abogado|abogada|doctor|doctora)\s+(?:en|por)\s+la'
        ]
        
        for pattern in academic_patterns:
            if re.search(pattern, text_lower):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_professional_score(self, text, candidate_name=None):
        """
        Calculate how professional (work experience) the content is.
        
        Args:
            text (str): Content text
            candidate_name (str, optional): Candidate name
            
        Returns:
            float: Professional score (0-1)
        """
        if not text:
            return 0.0
        
        score = 0.0
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name)
        
        # Check for professional terms
        if entities['PROFESSIONAL']:
            score += min(0.6, 0.1 * len(entities['PROFESSIONAL']))
        
        # Check for job titles
        job_titles = [
            "director", "directora", "gerente", "presidente", "presidenta", 
            "jefe", "jefa", "CEO", "coordinador", "coordinadora",
            "supervisor", "supervisora", "ejecutivo", "ejecutiva",
            "administrador", "administradora", "empresario", "empresaria",
            "consultor", "consultora", "asesor", "asesora", "titular",
            "fundador", "fundadora", "socio", "socia", "propietario", "propietaria"
        ]
        
        text_lower = text.lower()
        for title in job_titles:
            if title in text_lower:
                score += 0.1
                if score >= 0.8:
                    break
        
        # Check for professional experience patterns
        professional_patterns = [
            r'(?:ha|había)\s+(?:trabajado|laborado|colaborado|fungido)\s+como',
            r'(?:su|con)\s+(?:experiencia|trayectoria)\s+(?:en|como|dentro de)',
            r'(?:fundó|dirige|administra|preside|fundador de|director de)',
            r'(?:cuenta con|posee|tiene)\s+(?:una|amplia|extensa|sólida)\s+(?:experiencia|trayectoria)'
        ]
        
        for pattern in professional_patterns:
            if re.search(pattern, text_lower):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_public_service_score(self, text, candidate_name=None):
        """
        Calculate how much public service experience is mentioned.
        
        Args:
            text (str): Content text
            candidate_name (str, optional): Candidate name
            
        Returns:
            float: Public service score (0-1)
        """
        if not text:
            return 0.0
        
        score = 0.0
        
        # Extract entities
        entities = self.extract_entities(text, candidate_name)
        
        # Check for public service terms
        if entities['PUBLIC_SERVICE']:
            score += min(0.6, 0.1 * len(entities['PUBLIC_SERVICE']))
        
        # Check for government roles
        government_roles = [
            "alcalde", "alcaldesa", "presidente municipal", "presidenta municipal",
            "regidor", "regidora", "síndico", "síndica", "diputado", "diputada",
            "senador", "senadora", "secretario de gobierno", "secretaria de gobierno",
            "funcionario", "funcionaria", "servidor público", "servidora pública",
            "delegado", "delegada", "gobernador", "gobernadora", "director general",
            "secretario de estado", "secretaria de estado", "subsecretario", "subsecretaria"
        ]
        
        text_lower = text.lower()
        for role in government_roles:
            if role in text_lower:
                score += 0.1
                if score >= 0.8:
                    break
                    
        # Check for government institutions
        institutions = [
            "ayuntamiento", "municipio", "gobierno municipal", "gobierno estatal", 
            "gobierno federal", "secretaría", "dependencia", "instituto",
            "cabildo", "palacio municipal", "congreso", "senado",
            "cámara de diputados", "delegación", "administración pública",
            "poder ejecutivo", "poder legislativo", "poder judicial"
        ]
        
        for institution in institutions:
            if institution in text_lower:
                score += 0.1
                if score >= 0.9:
                    break
        
        # Check for public service patterns
        public_service_patterns = [
            r'(?:ocupó|desempeñó|fungió como|ejerció)\s+(?:el cargo|la función|el puesto)\s+de',
            r'(?:se desempeñó|ha servido|ha trabajado|trabajó)\s+(?:en|dentro de|para)\s+(?:el|la|los|las)\s+(?:gobierno|administración|ayuntamiento|secretaría|instituto)',
            r'(?:formó parte|ha sido parte|perteneció|perteneció al|miembro de|integrante de)\s+(?:del|de la|el|la)\s+(?:administración|gobierno|cabildo|ayuntamiento|equipo de gobierno)'
        ]
        
        for pattern in public_service_patterns:
            if re.search(pattern, text_lower):
                score += 0.15
                break
        
        return min(1.0, score)  # Cap at 1.0
    
    def calculate_content_relevance(self, content, candidate_name, municipality=None):
        """
        Calculate overall content relevance combining various indicators.
        
        Args:
            content (str): Content text
            candidate_name (str): Candidate name
            municipality (str, optional): Municipality name
            
        Returns:
            float: Overall relevance score (0-1)
        """
        if not content or not candidate_name:
            return 0.0
            
        # Calculate individual scores
        bio_score = self.calculate_biographical_score(content, candidate_name)
        political_score = self.calculate_political_score(content, candidate_name, municipality)
        academic_score = self.calculate_academic_score(content, candidate_name)
        professional_score = self.calculate_professional_score(content, candidate_name)
        public_service_score = self.calculate_public_service_score(content, candidate_name)
        
        # Candidate name match score
        found, match_score, _ = self.name_matcher.fuzzy_match_name(content, candidate_name)
        name_match_score = match_score / 100 if found else 0.0
        
        # Municipality mention boost
        municipality_boost = 0.1 if municipality and municipality.lower() in content.lower() else 0.0
        
        # Combine scores with appropriate weighting
        total_score = (
            (name_match_score * 0.25) +  # Name match is very important
            (bio_score * 0.2) +           # Biographical relevance
            (political_score * 0.25) +     # Political context
            (academic_score * 0.1) +       # Academic background
            (professional_score * 0.1) +   # Professional experience
            (public_service_score * 0.1) + # Public service
            municipality_boost             # Bonus for municipality mention
        )
        
        # Ensure score is capped at 1.0
        return min(1.0, total_score)


def create_entity_recognizer(candidates_data=None, nlp_model=None, name_matcher=None):
    """
    Factory function to create a Mexican entity recognizer.
    
    Args:
        candidates_data (pandas.DataFrame, optional): Candidates data
        nlp_model: Optional NLP model
        name_matcher: Optional name matcher
        
    Returns:
        MexicanEntityRecognizer: Entity recognizer instance
    """
    return MexicanEntityRecognizer(
        candidates_data=candidates_data,
        nlp_model=nlp_model or NLP_MODEL,
        name_matcher=name_matcher
    )