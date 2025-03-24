import csv
import requests
import os
import logging
from pprint import pprint
import re

# ------------------------------------------------------------------------------
# CONFIGURE LOGGING
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------------------------------------------------------------------------
# OXYLABS CREDENTIALS
# (In production, set these as environment variables for security, e.g.:
#  export OXYLABS_USERNAME="my_username"
#  export OXYLABS_PASSWORD="my_password"
#  ...
# )
# ------------------------------------------------------------------------------
OXYLABS_USERNAME = "jose0221_t0O13"    # Or: os.getenv("OXYLABS_USERNAME")
OXYLABS_PASSWORD = "Tr0p1c0s_159497"  # Or: os.getenv("OXYLABS_PASSWORD")
OXYLABS_COUNTRY = "mx"                # For localizing results (if needed)
OXYLABS_API_MODE = "realtime"         # 'direct' or 'realtime'

# ------------------------------------------------------------------------------
# IF YOU USE REALTIME SCRAPING (VIA OXYLABS' REALTIME API):
# The endpoint is typically:
#     https://realtime.oxylabs.io/v1/queries
# If you use direct scraping (Proxy Endpoint), you’d integrate differently.
# ------------------------------------------------------------------------------
BASE_URL = "https://realtime.oxylabs.io/v1/queries"

# ------------------------------------------------------------------------------
# SPANISH MONTHS MAPPING FOR PARSING
# (for "del 01-Ene-2011 al 31-Dic-2013" etc.)
# ------------------------------------------------------------------------------
SPANISH_MONTHS = {
    'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04', 'MAY': '05', 'JUN': '06',
    'JUL': '07', 'AGO': '08', 'SEP': '09', 'SET': '09',  # "SET" sometimes used for "SEPTIEMBRE"
    'OCT': '10', 'NOV': '11', 'DIC': '12'
}

# ------------------------------------------------------------------------------
# PARSE A PERIOD STRING (E.G. "del 01-Ene-2011 al 31-Dic-2013")
# Returns (start_mmddyyyy, end_mmddyyyy) in "MM/DD/YYYY" format, or None
# ------------------------------------------------------------------------------
def parse_period_range(period_str: str):
    """
    Attempt to parse a Spanish date range like:
      "del 01-Ene-2011 al 31-Dic-2013"
      "de 1991 a 1992"
      "del 01-Ene-1999 al 31-Dic-2001"
    Return (start_date_str, end_date_str) in "MM/DD/YYYY",
    or None if parsing is unsuccessful.
    """
    period_str = period_str.strip()
    if not period_str:
        return None

    # Regex for full day-month-year, e.g. "del 01-Ene-2011 al 31-Dic-2013"
    match = re.search(r"(\d{1,2})-([A-Za-z]{3})-(\d{4}).+(\d{1,2})-([A-Za-z]{3})-(\d{4})", period_str)
    if match:
        # e.g., group(1)=01, group(2)=Ene, group(3)=2011, group(4)=31, group(5)=Dic, group(6)=2013
        day1, mon1, year1 = match.group(1), match.group(2), match.group(3)
        day2, mon2, year2 = match.group(4), match.group(5), match.group(6)
        mon1_up = mon1.upper()
        mon2_up = mon2.upper()

        if mon1_up in SPANISH_MONTHS and mon2_up in SPANISH_MONTHS:
            mm1 = SPANISH_MONTHS[mon1_up]
            mm2 = SPANISH_MONTHS[mon2_up]
            start_str = f"{mm1}/{day1.zfill(2)}/{year1}"
            end_str   = f"{mm2}/{day2.zfill(2)}/{year2}"
            return (start_str, end_str)

    # Simpler pattern: "de 1991 a 1992" => entire years
    simple_match = re.search(r"[dD]e\s+(\d{4})\s+a\s+(\d{4})", period_str)
    if simple_match:
        start_year = simple_match.group(1)
        end_year   = simple_match.group(2)
        start_str  = f"01/01/{start_year}"
        end_str    = f"12/31/{end_year}"
        return (start_str, end_str)

    return None  # Could not parse

# ------------------------------------------------------------------------------
# FALLBACK: BUILD ±2-YEAR RANGE AROUND ELECTION YEAR
# ------------------------------------------------------------------------------
def build_fallback_range(election_year: int):
    """
    For example, if year=2011 => 01/01/2009 to 12/31/2013
    """
    start_y = election_year - 2
    end_y   = election_year + 2
    start_str = f"01/01/{start_y}"
    end_str   = f"12/31/{end_y}"
    return (start_str, end_str)

# ------------------------------------------------------------------------------
# MAKE TBS PARAM FOR GOOGLE (custom date range)
# ------------------------------------------------------------------------------
def make_tbs_param(start_date: str, end_date: str):
    """
    Convert "MM/DD/YYYY" start/end into cdr:1,cd_min:...,cd_max:...
    e.g., cdr:1,cd_min:01/01/2009,cd_max:12/31/2013
    """
    return f"cdr:1,cd_min:{start_date},cd_max:{end_date}"

# ------------------------------------------------------------------------------
# FETCH RESULTS FROM OXYLABS (GOOGLE NEWS, TBM=NWS)
# ------------------------------------------------------------------------------
def fetch_oxylabs_results(query: str, tbs_value: str, pages: int) -> dict:
    """
    Builds the JSON payload for Google News (tbm=nws),
    sends it to Oxylabs Realtime API, returns parsed JSON results.
    """
    payload = {
        "source": "google_search",
        # "domain": "com.mx" can be used for Mexico. Alternatively, "com" with geo_location="Mexico".
        "domain": "com.mx",
        "query": query,
        "parse": True,
        "pages": pages,
        "context": [
            # Spanish results
            {"key": "results_language", "value": "es"},
            # Google News
            {"key": "tbm", "value": "nws"},
            # More exhaustive: no near-duplicate filtering
            {"key": "filter", "value": "0"},
            # Restrict by date range
            {"key": "tbs", "value": tbs_value},
            # If you want safe search:
            # {"key": "safe_search", "value": "true"},
            # If you want to further localize to Mexico:
            # {"key": "geo_location", "value": "Mexico"},
        ]
    }

    # For 'direct' mode, you would typically use a different endpoint or approach.
    # This script is set for 'realtime' usage of the Oxylabs Realtime API.
    # You can check OXYLABS_API_MODE if you want to differentiate or decide:
    # if OXYLABS_API_MODE == "realtime": ...
    # elif OXYLABS_API_MODE == "direct": ...
    # but for demonstration, we'll assume 'realtime'.

    try:
        resp = requests.post(
            BASE_URL,
            auth=(OXYLABS_USERNAME, OXYLABS_PASSWORD),
            json=payload,
            timeout=60  # seconds
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Oxylabs request error (query='{query}'): {e}")
        return {"results": []}

# ------------------------------------------------------------------------------
# COUNT ORGANIC (NEWS) RESULTS
# ------------------------------------------------------------------------------
def count_organic_results(response_json: dict) -> int:
    """
    For Google News results (tbm=nws), items typically appear under "organic".
    We sum them across all pages returned.
    """
    results_list = response_json.get("results", [])
    total = 0
    for page_data in results_list:
        content = page_data.get("content", {})
        organic_list = content.get("organic", [])
        total += len(organic_list)
    return total

# ------------------------------------------------------------------------------
# EXTRACT UP TO 15 NEWS ARTICLES
# (candidate, municipality, year, link, title, date, main_text)
# ------------------------------------------------------------------------------
def extract_top_organic(response_json: dict,
                        candidate: str,
                        municipality: str,
                        year_str: str,
                        max_count: int = 15):
    """
    Traverse all 'organic' results, returning up to max_count items
    with the relevant fields for building a textual corpus / curricula measure.
    """
    extracted = []
    results_list = response_json.get("results", [])

    for page_data in results_list:
        content = page_data.get("content", {})
        organic_list = content.get("organic", [])
        for item in organic_list:
            link = item.get("link", "")
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            published_date = item.get("date", None)  # often present in GNews parse

            extracted.append({
                "candidate": candidate,
                "municipality": municipality,
                "year": year_str,
                "link": link,
                "title": title,
                "date": published_date,
                "main_text": snippet
            })
            if len(extracted) >= max_count:
                return extracted

    return extracted

# ------------------------------------------------------------------------------
# MAIN SCRAPER
# ------------------------------------------------------------------------------
def scrape_candidates(csv_file_path: str, pages_to_fetch: int = 2):
    """
    Reads 'candidates.csv', attempts to parse the period range or fallback ±2 yrs,
    queries Spanish Google News for each candidate, extracts structured data,
    logs results. Good basis for advanced text analysis in subsequent steps.

    CSV expected columns:
      ENTIDAD,
      MUNICIPIO,
      PARTIDO,
      PRESIDENTE_MUNICIPAL,
      PERIODO_FORMATO_ORIGINAL,
      Year
    """
    all_news = []  # consolidated results from all candidates

    with open(csv_file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            entidad     = row.get("ENTIDAD", "").strip()
            municipio   = row.get("MUNICIPIO", "").strip()
            partido     = row.get("PARTIDO", "").strip()
            candidato   = row.get("PRESIDENTE_MUNICIPAL", "").strip()
            periodo_str = row.get("PERIODO_FORMATO_ORIGINAL", "").strip()
            year_str    = row.get("Year", "").strip()

            # Validate or parse 'Year'
            try:
                election_year = int(year_str)
            except ValueError:
                logging.warning(f"Invalid 'Year' in row, skipping: {row}")
                continue

            # Attempt to parse the PERIODO_FORMATO_ORIGINAL
            parsed_range = parse_period_range(periodo_str)
            if parsed_range is not None:
                start_mmddyyyy, end_mmddyyyy = parsed_range
            else:
                # fallback to ±2 years
                start_mmddyyyy, end_mmddyyyy = build_fallback_range(election_year)

            # Build tbs param
            tbs_value = make_tbs_param(start_mmddyyyy, end_mmddyyyy)

            # ---------------------------
            # PRIMARY QUERY (full name + municipality)
            # ---------------------------
            main_query = f'"{candidato}" "{municipio}"'
            logging.info(f"[PRIMARY] Searching for: {main_query}, TBS={tbs_value}")

            primary_data = fetch_oxylabs_results(
                query=main_query,
                tbs_value=tbs_value,
                pages=pages_to_fetch
            )
            primary_org_count = count_organic_results(primary_data)
            logging.info(f"[PRIMARY] Found {primary_org_count} results.")

            if primary_org_count == 0:
                # ---------------------------
                # FALLBACK (last two surnames + municipality)
                # ---------------------------
                name_parts = candidato.split()
                if len(name_parts) >= 2:
                    fallback_name = " ".join(name_parts[-2:])
                else:
                    fallback_name = candidato

                fallback_query = f'"{fallback_name}" "{municipio}"'
                logging.info(f"[FALLBACK] Searching for: {fallback_query}, TBS={tbs_value}")

                fallback_data = fetch_oxylabs_results(
                    query=fallback_query,
                    tbs_value=tbs_value,
                    pages=pages_to_fetch
                )
                fallback_org_count = count_organic_results(fallback_data)
                logging.info(f"[FALLBACK] Found {fallback_org_count} results.")

                if fallback_org_count > 0:
                    final_data = fallback_data
                else:
                    final_data = primary_data
            else:
                final_data = primary_data

            # Extract up to 15 articles
            candidate_news = extract_top_organic(
                final_data,
                candidate=candidato,
                municipality=municipio,
                year_str=year_str,
                max_count=15
            )

            # Append to global results
            all_news.extend(candidate_news)

    # Done with all rows
    logging.info(f"Scraping finished. Collected {len(all_news)} total news items.")
    pprint(all_news)

# ------------------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # 1) CSV path (adjust if not in the same directory):
    csv_path = "candidates.csv"
    # Example: csv_path = "/Users/username/Documents/candidate_scraper/candidates.csv"

    # 2) Pages to fetch (each ~10 organic results). Increase if needed.
    scrape_candidates(csv_file_path=csv_path, pages_to_fetch=2)