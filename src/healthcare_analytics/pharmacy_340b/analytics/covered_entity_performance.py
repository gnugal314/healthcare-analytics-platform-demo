from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/analytics/pharmacy/covered_entity_performance.parquet")


def build_covered_entity_performance(fact_rx: pd.DataFrame) -> pd.DataFrame:
    summary = (
        fact_rx.groupby("covered_entity_id", dropna=False)
        .agg(
            rx_claim_count=("rx_claim_id", "nunique"),
            eligible_rx_count=("is_340b_eligible", "sum"),
            total_paid_amount=("total_paid_amount", "sum"),
            total_340b_savings=("estimated_340b_savings", "sum"),
        )
        .reset_index()
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(OUTPUT_PATH, index=False)

    return summary
