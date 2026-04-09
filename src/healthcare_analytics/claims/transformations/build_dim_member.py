from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/curated/claims/dim_member.parquet")


def build_dim_member(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["member_id", "member_dob", "member_gender", "plan_name"]
    dim_df = df[[c for c in cols if c in df.columns]].drop_duplicates().copy()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    dim_df.to_parquet(OUTPUT_PATH, index=False)
    return dim_df
