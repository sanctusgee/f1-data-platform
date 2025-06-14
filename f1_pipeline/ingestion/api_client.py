# f1_pipeline/api_client.py
"""
Handles all HTTP requests to the Jolpica Ergast API with retry logic and rate limiting.
"""
import logging
import requests
from urllib.parse import urljoin
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import requests.exceptions

logger = logging.getLogger(__name__)


@retry(
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5)
)
def safe_request(url: str) -> dict:
    """Make HTTP request with retry logic for rate limit handling."""
    response = requests.get(url)
    if response.status_code == 429:
        raise requests.exceptions.HTTPError("429: Too Many Requests")
    response.raise_for_status()
    return response.json()


def get_rounds_for_season(api_base_url: str, season: int) -> list[int]:
    """Auto-detect round count for a season."""
    url = urljoin(api_base_url, f"f1/{season}.json")
    logger.info(f"Detecting rounds for season {season} via {url}")
    data = safe_request(url)
    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    return [int(r["round"]) for r in races]


def fetch_lap_data_raw(api_base_url: str, season: int, round_: int) -> dict:
    """Fetch raw lap data from API."""
    url = urljoin(api_base_url, f"f1/{season}/{round_}/laps")
    logger.info(f"Requesting lap data from {url}")
    return safe_request(url)


def fetch_pitstop_data_raw(api_base_url: str, season: int, round_: int) -> dict:
    """Fetch raw pit stop data from API."""
    url = urljoin(api_base_url, f"f1/{season}/{round_}/pitstops")
    logger.info(f"Requesting pit stop data from {url}")
    return safe_request(url)
