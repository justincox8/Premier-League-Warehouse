import os
from dotenv import load_dotenv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import dlt
from dlt.sources.helpers import requests

load_dotenv()

FOOTBALL_DATA_KEY = os.environ['FOOTBALL_DATA']
API = "https://api.football-data.org/v4/competitions/PL/matches"


@dlt.resource(name= "pl-matches", write_disposition="append")
def pl_matches(seasons):
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    for season in seasons:
        response = requests.get(
            API,
            headers = {
                "X-Auth-Token": FOOTBALL_DATA_KEY
            },
            params = {"season": season}
        )
    response.raise_for_status()
    for match in response.json()["matches"]:
        match["ingestion_timestamp"] = ingestion_timestamp
        match["season"] = season
        yield match


RAW_DIR = Path(__file__).resolve().parent / "raw"


def run_ingest(seasons: list[int]):
    pipeline = dlt.pipeline(
        pipeline_name="pl_raw",
        destination=dlt.destinations.filesystem(bucket_url=RAW_DIR.as_uri()),
        dataset_name="football_data",
    )
    return pipeline.run(pl_matches(seasons), loader_file_format="parquet")


if __name__ == "__main__":
    seasons = [int(s) for s in sys.argv[1:]] or [2026]
    print(run_ingest(seasons))
