from pathlib import Path
from datetime import datetime
import pandas as pd

RAW_INPUT_PATH = Path("data/raw/claims_raw_sample.csv")


def ingest_claims_data(run_id: str) -> pd.DataFrame:
    df = pd.read_csv(RAW_INPUT_PATH)
    df["pipeline_run_id"] = run_id
    df["ingestion_timestamp"] = datetime.utcnow()
    df["source_file_name"] = RAW_INPUT_PATH.name
    return df
