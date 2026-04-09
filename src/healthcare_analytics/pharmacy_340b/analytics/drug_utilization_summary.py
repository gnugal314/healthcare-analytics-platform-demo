from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/analytics/pharmacy/drug_utilization_summary.parquet")


def build_drug_utilization_summary(fact_rx: pd.DataFrame, dim_drug: pd.DataFrame) -> pd.DataFrame:
    summary = (
        fact_rx.groupby("ndc_code", dropna=False)
        .agg(
            rx_claim_count=("rx_claim_id", "nunique"),
            total_quantity_dispensed=("quantity_dispensed", "sum"),
            total_paid_amount=("total_paid_amount", "sum"),
            total_340b_savings=("estimated_340b_savings", "sum"),
        )
        .reset_index()
    )

    if "ndc_code" in dim_drug.columns:
        summary = summary.merge(dim_drug, on="ndc_code", how="left")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(OUTPUT_PATH, index=False)

    return summary
