from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/analytics/pharmacy/payer_mix_summary.parquet")


def build_payer_mix_summary(fact_rx: pd.DataFrame) -> pd.DataFrame:
    summary = (
        fact_rx.groupby(["payer_type", "line_of_business"], dropna=False)
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
