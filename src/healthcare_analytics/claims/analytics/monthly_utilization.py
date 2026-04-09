from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/analytics/claims/monthly_utilization.parquet")


def build_monthly_utilization(fact_claims: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    df = fact_claims.copy()
    df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")
    df["year_month"] = df["service_date"].dt.to_period("M").astype(str)

    summary = (
        df.groupby(["year_month", "line_of_business"], dropna=False)
        .agg(
            total_claims=("claim_id", "nunique"),
            total_units=("units", "sum"),
            total_paid_amount=("paid_amount", "sum"),
        )
        .reset_index()
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(OUTPUT_PATH, index=False)
    return summary
