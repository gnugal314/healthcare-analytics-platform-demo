from pathlib import Path
import pandas as pd

STAGING_OUTPUT_PATH = Path("data/staging/pharmacy_340b_staged.parquet")


def stage_pharmacy_data(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    staged_df = df.copy()
    staged_df.columns = [col.strip().lower() for col in staged_df.columns]

    STAGING_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    staged_df.to_parquet(STAGING_OUTPUT_PATH, index=False)

    return staged_df
