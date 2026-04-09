from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/analytics/claims/provider_performance.parquet")


def build_provider_performance(fact_claims: pd.DataFrame, dim_provider: pd.DataFrame) -> pd.DataFrame:
    summary = (
        fact_claims.groupby("provider_id", dropna=False)
        .agg(
            claim_count=("claim_id", "nunique"),
            total_paid_amount=("paid_amount", "sum"),
            avg_paid_amount=("paid_amount", "mean"),
        )
        .reset_index()
    )

    summary = summary.merge(dim_provider, on="provider_id", how="left")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(OUTPUT_PATH, index=False)
    return summary
