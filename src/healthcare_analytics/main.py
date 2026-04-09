from healthcare_analytics.claims.orchestrator import run_claims_pipeline
from healthcare_analytics.pharmacy_340b.orchestrator import run_pharmacy_pipeline


if __name__ == "__main__":
    run_claims_pipeline()
    run_pharmacy_pipeline()
