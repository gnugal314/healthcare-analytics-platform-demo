from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/curated/pharmacy/dim_drug.parquet")


def build_dim_drug(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["ndc_code", "drug_name", "generic_name", "drug_class"]
    dim_df = df[[col for col in cols if col in df.columns]].drop_duplicates().copy()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    dim_df.to_parquet(OUTPUT_PATH, index=False)

    return dim_df
