from datetime import datetime
from pathlib import Path
import uuid
import pandas as pd

OUTPUT_PATH = Path("data/analytics/pipeline_run_summary.parquet")


def start_run(domain: str) -> str:
    run_id = str(uuid.uuid4())
    print(f"[START] domain={domain} run_id={run_id} timestamp={datetime.utcnow()}")
    return run_id


def end_run(run_id: str, domain: str, status: str) -> None:
    print(f"[END] domain={domain} run_id={run_id} status={status} timestamp={datetime.utcnow()}")


def capture_pipeline_metrics(
    run_id: str,
    domain: str,
    raw_count: int,
    staged_count: int,
    curated_count: int
) -> pd.DataFrame:
    metrics_df = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "domain": domain,
                "run_timestamp": datetime.utcnow(),
                "raw_count": raw_count,
                "staged_count": staged_count,
                "curated_count": curated_count,
                "drop_from_raw_to_curated": raw_count - curated_count,
            }
        ]
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUTPUT_PATH.exists():
        existing = pd.read_parquet(OUTPUT_PATH)
        metrics_df = pd.concat([existing, metrics_df], ignore_index=True)

    metrics_df.to_parquet(OUTPUT_PATH, index=False)
    return metrics_df
