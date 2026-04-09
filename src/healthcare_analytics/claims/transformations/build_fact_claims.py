from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/curated/claims/fact_claims.parquet")


def build_fact_claims(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "claim_id", "member_id", "provider_id", "service_date",
        "claim_amount", "paid_amount", "units", "claim_status",
        "line_of_business", "procedure_code", "diagnosis_code",
        "pipeline_run_id"
    ]
    fact_df = df[[c for c in cols if c in df.columns]].copy()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fact_df.to_parquet(OUTPUT_PATH, index=False)
    return fact_df
