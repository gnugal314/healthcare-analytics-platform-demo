from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/quality/claims_validation_results.csv")


def run_validation_suite(df: pd.DataFrame, dataset_name: str, run_id: str) -> pd.DataFrame:
    results = []

    results.append({
        "check_name": "row_count_check",
        "passed": len(df) > 0,
        "metric_value": len(df)
    })

    for col in ["claim_id", "member_id", "provider_id"]:
        results.append({
            "check_name": f"{col}_not_null",
            "passed": col in df.columns and int(df[col].isna().sum()) == 0,
            "metric_value": int(df[col].isna().sum()) if col in df.columns else -1
        })

    duplicate_count = int(df.duplicated(subset=["claim_id"]).sum()) if "claim_id" in df.columns else -1
    results.append({
        "check_name": "duplicate_claim_id_check",
        "passed": duplicate_count == 0,
        "metric_value": duplicate_count
    })

    results_df = pd.DataFrame(results)
    results_df["dataset_name"] = dataset_name
    results_df["run_id"] = run_id

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        existing = pd.read_csv(OUTPUT_PATH)
        results_df = pd.concat([existing, results_df], ignore_index=True)

    results_df.to_csv(OUTPUT_PATH, index=False)
    return results_df
