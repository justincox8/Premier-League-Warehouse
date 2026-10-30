import os
from dotenv import load_dotenv
import sys
from datetime import datetime, timezone
from pathlib import Path
import dlt
from dlt.sources.helpers import requests
import time

load_dotenv()

FOOTBALL_DATA_KEY = os.environ['FOOTBALL_DATA']
API = "https://api.football-data.org/v4/competitions/PL/matches"
leagues = ["PL", "BL1", "SA", "PD", "FL1", "DED", "PPL", "BSA", "CL"]

@dlt.resource(name= "matches", write_disposition="append")
def matches(seasons):
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    for league in leagues:
        API = f"https://api.football-data.org/v4/competitions/{league}/matches"
        for season in seasons:
            response = requests.get(
                API,
                headers = {
                    "X-Auth-Token": FOOTBALL_DATA_KEY
                },
            params = {"season": season}
            )
            response.raise_for_status()
            for matches in response.json()["matches"]:
                matches["ingestion_timestamp"] = ingestion_timestamp
                matches["season"] = season
                matches["league"] = league
                yield matches
        time.sleep(7)  


RAW_DIR = Path(__file__).resolve().parent / "raw"


def run_ingest(seasons: list[int]):
    pipeline = dlt.pipeline(
        pipeline_name="football_raw",
        destination=dlt.destinations.filesystem(bucket_url=RAW_DIR.as_uri()),
        dataset_name="match_data",
    )
    return pipeline.run(matches(seasons), loader_file_format="parquet")

seasons = [2023, 2024, 2025, 2026]

if __name__ == "__main__":
    for season in seasons:
        print(f"{season}: {run_ingest([season])}")
