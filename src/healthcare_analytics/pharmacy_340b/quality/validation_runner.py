from pathlib import Path
import pandas as pd

from healthcare_analytics.pharmacy_340b.quality.business_rule_checks import (
    check_non_negative_fields,
    check_340b_savings_logic,
)

OUTPUT_PATH = Path("data/quality/pharmacy_validation_results.csv")


def run_validation_suite(df: pd.DataFrame, dataset_name: str, run_id: str) -> pd.DataFrame:
    results = []

    results.extend(
        check_non_negative_fields(
            df,
            ["quantity_dispensed", "ingredient_cost", "total_paid_amount", "estimated_340b_savings"]
        )
    )
    results.append(check_340b_savings_logic(df))

    results_df = pd.DataFrame(results)
    results_df["dataset_name"] = dataset_name
    results_df["run_id"] = run_id

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        existing = pd.read_csv(OUTPUT_PATH)
        results_df = pd.concat([existing, results_df], ignore_index=True)

    results_df.to_csv(OUTPUT_PATH, index=False)
    return results_df
