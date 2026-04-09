from healthcare_analytics.shared.observability import start_run, end_run, capture_pipeline_metrics
from healthcare_analytics.claims.ingestion.ingest_claims import ingest_claims_data
from healthcare_analytics.claims.staging.stage_claims import stage_claims_data
from healthcare_analytics.claims.quality.validation_runner import run_validation_suite
from healthcare_analytics.claims.transformations.clean_claims import clean_claims_data
from healthcare_analytics.claims.transformations.build_fact_claims import build_fact_claims
from healthcare_analytics.claims.transformations.build_dim_member import build_dim_member
from healthcare_analytics.claims.transformations.build_dim_provider import build_dim_provider
from healthcare_analytics.claims.transformations.build_dim_date import build_dim_date
from healthcare_analytics.claims.analytics.monthly_utilization import build_monthly_utilization
from healthcare_analytics.claims.analytics.provider_performance import build_provider_performance
from healthcare_analytics.claims.analytics.member_cost_summary import build_member_cost_summary


def run_claims_pipeline() -> None:
    run_id = start_run(domain="claims")

    raw_df = ingest_claims_data(run_id=run_id)
    staged_df = stage_claims_data(raw_df, run_id=run_id)

    run_validation_suite(df=staged_df, dataset_name="claims_staged", run_id=run_id)

    clean_df = clean_claims_data(staged_df)

    fact_claims = build_fact_claims(clean_df)
    dim_member = build_dim_member(clean_df)
    dim_provider = build_dim_provider(clean_df)
    dim_date = build_dim_date(clean_df)

    build_monthly_utilization(fact_claims, dim_date)
    build_provider_performance(fact_claims, dim_provider)
    build_member_cost_summary(fact_claims, dim_member)

    capture_pipeline_metrics(
        run_id=run_id,
        domain="claims",
        raw_count=len(raw_df),
        staged_count=len(staged_df),
        curated_count=len(fact_claims),
    )

    end_run(run_id=run_id, domain="claims", status="SUCCESS")


if __name__ == "__main__":
    run_claims_pipeline()
