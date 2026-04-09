from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/curated/pharmacy/fact_rx_claims.parquet")


def build_fact_rx_claims(df: pd.DataFrame) -> pd.DataFrame:
    fact_cols = [
        "rx_claim_id",
        "member_id",
        "prescriber_id",
        "pharmacy_id",
        "covered_entity_id",
        "ndc_code",
        "fill_date",
        "days_supply",
        "quantity_dispensed",
        "ingredient_cost",
        "dispensing_fee",
        "total_paid_amount",
        "payer_type",
        "line_of_business",
        "claim_status",
        "is_340b_eligible",
        "estimated_340b_savings",
        "medicaid_indicator",
        "orphan_drug_indicator",
        "pipeline_run_id",
    ]

    fact_df = df[[col for col in fact_cols if col in df.columns]].copy()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fact_df.to_parquet(OUTPUT_PATH, index=False)

    return fact_df
