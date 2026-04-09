from pathlib import Path
import pandas as pd

OUTPUT_PATH = Path("data/analytics/claims/member_cost_summary.parquet")


def build_member_cost_summary(fact_claims: pd.DataFrame, dim_member: pd.DataFrame) -> pd.DataFrame:
    summary = (
        fact_claims.groupby("member_id", dropna=False)
        .agg(
            total_claims=("claim_id", "nunique"),
            total_paid_amount=("paid_amount", "sum"),
            avg_paid_amount=("paid_amount", "mean"),
        )
        .reset_index()
    )

    summary = summary.merge(dim_member, on="member_id", how="left")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(OUTPUT_PATH, index=False)
    return summary
