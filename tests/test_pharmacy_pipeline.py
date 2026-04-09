import pandas as pd
from healthcare_analytics.pharmacy_340b.transformations.clean_pharmacy import clean_pharmacy_data


def test_clean_pharmacy_removes_invalid_rows():
    df = pd.DataFrame({
        "rx_claim_id": [1, 2, 2],
        "member_id": [100, 101, 101],
        "pharmacy_id": [10, 11, 11],
        "ndc_code": ["123", None, None],
        "quantity_dispensed": [30, -1, -1],
        "ingredient_cost": [100.0, -5.0, -5.0],
    })

    cleaned = clean_pharmacy_data(df)

    assert len(cleaned) == 1
    assert cleaned.iloc[0]["rx_claim_id"] == 1
