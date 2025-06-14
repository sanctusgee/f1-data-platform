# f1_pipeline/data_transformer.py
"""
Transforms raw JSON API responses into structured pandas DataFrames.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def transform_lap_data(json_data: dict, season: int, round_: int, max_laps: int | None = None) -> pd.DataFrame:
    """Transform nested lap JSON into flat DataFrame structure."""
    races = json_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    all_rows = []

    for race in races:
        race_name = race.get("raceName")
        circuit = race.get("Circuit", {}).get("circuitName")
        date = race.get("date")

        laps = race.get("Laps", [])
        if max_laps is not None:
            laps = laps[:max_laps]
        if len(laps) < (max_laps or 0):
            logger.info(f"Only {len(laps)} laps found for season {season}, round {round_}.")

        for lap in laps:
            lap_number = lap.get("number")
            for timing in lap.get("Timings", []):
                all_rows.append({
                    "race_id": f"{season}_{round_}",
                    "season": season,
                    "round": round_,
                    "race_name": race_name,
                    "circuit": circuit,
                    "date": date,
                    "lap_number": lap_number,
                    "driver_id": timing.get("driverId"),
                    "position": timing.get("position"),
                    "time": timing.get("time")
                })

    return pd.DataFrame(all_rows)


def transform_pitstop_data(json_data: dict, season: int, round_: int) -> pd.DataFrame:
    """Transform nested pit stop JSON into flat DataFrame structure."""
    races = json_data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    all_rows = []

    for race in races:
        race_name = race.get("raceName")
        circuit = race.get("Circuit", {}).get("circuitName")
        date = race.get("date")

        for stop in race.get("PitStops", []):
            all_rows.append({
                "race_id": f"{season}_{round_}",
                "season": season,
                "round": round_,
                "race_name": race_name,
                "circuit": circuit,
                "date": date,
                "driver_id": stop.get("driverId"),
                "stop": stop.get("stop"),
                "lap": stop.get("lap"),
                "time": stop.get("time"),
                "duration": stop.get("duration")
            })

    return pd.DataFrame(all_rows)