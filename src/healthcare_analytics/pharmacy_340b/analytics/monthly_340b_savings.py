from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/analytics/pharmacy/monthly_340b_savings.parquet")


def build_monthly_340b_savings(fact_rx: pd.DataFrame) -> pd.DataFrame:
    df = fact_rx.copy()
    df["fill_date"] = pd.to_datetime(df["fill_date"], errors="coerce")
    df["year_month"] = df["fill_date"].dt.to_period("M").astype(str)

    summary = (
        df.groupby(["year_month", "is_340b_eligible"], dropna=False)
        .agg(
            rx_claim_count=("rx_claim_id", "nunique"),
            total_paid_amount=("total_paid_amount", "sum"),
            total_340b_savings=("estimated_340b_savings", "sum"),
        )
        .reset_index()
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(OUTPUT_PATH, index=False)

    return summary
