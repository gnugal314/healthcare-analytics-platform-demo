from healthcare_analytics.shared.observability import start_run, end_run, capture_pipeline_metrics
from healthcare_analytics.pharmacy_340b.ingestion.ingest_pharmacy import ingest_pharmacy_data
from healthcare_analytics.pharmacy_340b.staging.stage_pharmacy import stage_pharmacy_data
from healthcare_analytics.pharmacy_340b.quality.validation_runner import run_validation_suite
from healthcare_analytics.pharmacy_340b.transformations.clean_pharmacy import clean_pharmacy_data
from healthcare_analytics.pharmacy_340b.transformations.build_fact_rx_claims import build_fact_rx_claims
from healthcare_analytics.pharmacy_340b.transformations.build_dim_drug import build_dim_drug
from healthcare_analytics.pharmacy_340b.transformations.build_dim_pharmacy import build_dim_pharmacy
from healthcare_analytics.pharmacy_340b.transformations.build_dim_prescriber import build_dim_prescriber
from healthcare_analytics.pharmacy_340b.transformations.build_dim_rx_date import build_dim_rx_date
from healthcare_analytics.pharmacy_340b.analytics.monthly_340b_savings import build_monthly_340b_savings
from healthcare_analytics.pharmacy_340b.analytics.drug_utilization_summary import build_drug_utilization_summary
from healthcare_analytics.pharmacy_340b.analytics.covered_entity_performance import build_covered_entity_performance
from healthcare_analytics.pharmacy_340b.analytics.payer_mix_summary import build_payer_mix_summary


def run_pharmacy_pipeline() -> None:
    run_id = start_run(domain="pharmacy_340b")

    raw_df = ingest_pharmacy_data(run_id=run_id)
    staged_df = stage_pharmacy_data(raw_df, run_id=run_id)

    run_validation_suite(
        df=staged_df,
        dataset_name="pharmacy_340b_staged",
        run_id=run_id
    )

    clean_df = clean_pharmacy_data(staged_df)

    fact_rx = build_fact_rx_claims(clean_df)
    dim_drug = build_dim_drug(clean_df)
    dim_pharmacy = build_dim_pharmacy(clean_df)
    dim_prescriber = build_dim_prescriber(clean_df)
    dim_date = build_dim_rx_date(clean_df)

    build_monthly_340b_savings(fact_rx)
    build_drug_utilization_summary(fact_rx, dim_drug)
    build_covered_entity_performance(fact_rx)
    build_payer_mix_summary(fact_rx)

    capture_pipeline_metrics(
        run_id=run_id,
        domain="pharmacy_340b",
        raw_count=len(raw_df),
        staged_count=len(staged_df),
        curated_count=len(fact_rx),
    )

    end_run(run_id=run_id, domain="pharmacy_340b", status="SUCCESS")
