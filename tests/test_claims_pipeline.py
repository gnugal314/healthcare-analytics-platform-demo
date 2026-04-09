import pandas as pd
from healthcare_analytics.claims.transformations.clean_claims import clean_claims_data


def test_clean_claims_removes_invalid_and_duplicate_records():
    df = pd.DataFrame({
        "claim_id": [1, 1, 2, 3],
        "member_id": [100, 100, None, 102],
        "provider_id": [200, 200, 201, 202],
        "claim_amount": [100.0, 100.0, 150.0, -5.0],
        "paid_amount": [90.0, 90.0, 120.0, 0.0],
    })

    cleaned = clean_claims_data(df)

    assert cleaned["claim_id"].nunique() == 1
    assert cleaned["claim_id"].iloc[0] == 1
