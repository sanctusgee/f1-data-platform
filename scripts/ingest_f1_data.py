import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from f1_pipeline.config.settings import get_config  # <- Fixed import
from f1_pipeline.ingestion.orchestrator import F1DataIngester

# Setup logging - bcos I like to see what is going on
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    """Main execution function."""
    config = get_config()

    # This here creates the main orchestrator that coordinates API calls, transformation, and loading
    ingester = F1DataIngester(
        api_base_url=config["api_base_url"],
        db_config=config["database"]
    )

    seasons = [2024] # Is a list -> can be just one year, or more values, eg [2023, 2024, 2025]
    max_laps = 25   # Limit for testing. Can increase up to None for full.  None means no limit. Full means all laps

    ingester.ingest_and_load(seasons, max_laps)


if __name__ == "__main__":
    main()