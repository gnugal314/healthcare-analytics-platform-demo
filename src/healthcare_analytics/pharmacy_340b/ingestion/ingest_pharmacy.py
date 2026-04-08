from pathlib import Path
from datetime import datetime
import pandas as pd

RAW_INPUT_PATH = Path("data/raw/pharmacy_340b_raw_sample.csv")


def ingest_pharmacy_data(run_id: str) -> pd.DataFrame:
    df = pd.read_csv(RAW_INPUT_PATH)

    df["pipeline_run_id"] = run_id
    df["ingestion_timestamp"] = datetime.utcnow()
    df["source_file_name"] = RAW_INPUT_PATH.name

    return df
