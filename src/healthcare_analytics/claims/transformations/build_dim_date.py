from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/curated/claims/dim_date.parquet")


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    date_df = pd.DataFrame()
    if "service_date" in df.columns:
        unique_dates = pd.to_datetime(df["service_date"]).dropna().drop_duplicates().sort_values()
        date_df["service_date"] = unique_dates
        date_df["year"] = unique_dates.dt.year
        date_df["month"] = unique_dates.dt.month
        date_df["month_name"] = unique_dates.dt.month_name()
        date_df["year_month"] = unique_dates.dt.to_period("M").astype(str)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    date_df.to_parquet(OUTPUT_PATH, index=False)
    return date_df
