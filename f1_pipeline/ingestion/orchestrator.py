# f1_pipeline/orchestrator.py
"""
Orchestrates the complete F1 data ingestion workflow.
Coordinates API fetching, data transformation, and database loading.
"""
import logging
import pandas as pd
from f1_pipeline.ingestion.api_client import get_rounds_for_season, fetch_lap_data_raw, fetch_pitstop_data_raw
from f1_pipeline.ingestion.data_transformer import transform_lap_data, transform_pitstop_data
from f1_pipeline.utils.database import load_to_postgres

logger = logging.getLogger(__name__)


class F1DataIngester:
    """Main ingestion orchestrator with configuration management."""

    def __init__(self, api_base_url: str, db_config: dict):
        self.api_base_url = api_base_url
        self.db_config = db_config

    def ingest_race_data(self, season: int, round_: int, max_laps: int | None = None) -> tuple[
        pd.DataFrame, pd.DataFrame]:
        """Ingest data for a single race and return DataFrames."""
        logger.info(f"Processing season {season}, round {round_}")

        # Fetch raw data
        lap_raw = fetch_lap_data_raw(self.api_base_url, season, round_)
        pit_raw = fetch_pitstop_data_raw(self.api_base_url, season, round_)

        # Transform to DataFrames
        lap_df = transform_lap_data(lap_raw, season, round_, max_laps)
        pit_df = transform_pitstop_data(pit_raw, season, round_)

        return lap_df, pit_df

    def ingest_season(self, season: int, max_laps: int | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Ingest all rounds for a season with auto-detection."""
        logger.info(f"Starting ingestion for season {season}")

        rounds = get_rounds_for_season(self.api_base_url, season)
        logger.info(f"Season {season}: Found {len(rounds)} rounds")

        all_laps = []
        all_pits = []

        for round_ in rounds:
            try:
                lap_df, pit_df = self.ingest_race_data(season, round_, max_laps)
                all_laps.append(lap_df)
                all_pits.append(pit_df)
            except Exception as e:
                logger.warning(f"Failed to fetch data for {season} round {round_}: {e}")

        # Combine all DataFrames
        final_laps = pd.concat(all_laps, ignore_index=True) if all_laps else pd.DataFrame()
        final_pits = pd.concat(all_pits, ignore_index=True) if all_pits else pd.DataFrame()

        return final_laps, final_pits

    def ingest_and_load(self, seasons: list[int], max_laps: int | None = None):
        """Complete workflow: ingest multiple seasons and load to database."""
        logger.info("Starting complete ingestion process...")

        all_laps = []
        all_pits = []

        for season in seasons:
            try:
                lap_df, pit_df = self.ingest_season(season, max_laps)
                if not lap_df.empty:
                    all_laps.append(lap_df)
                if not pit_df.empty:
                    all_pits.append(pit_df)
            except Exception as e:
                logger.error(f"Failed to process season {season}: {e}")

        if all_laps:
            final_laps = pd.concat(all_laps, ignore_index=True)
            load_to_postgres(final_laps, "raw_lap_times", self.db_config)

        if all_pits:
            final_pits = pd.concat(all_pits, ignore_index=True)
            load_to_postgres(final_pits, "raw_pit_stops", self.db_config)

        logger.info("Ingestion complete.")