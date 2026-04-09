import pandas as pd
from healthcare_analytics.pharmacy_340b.quality.business_rule_checks import check_340b_savings_logic


def test_340b_savings_logic_check_flags_invalid_records():
    df = pd.DataFrame({
        "is_340b_eligible": [0, 1],
        "estimated_340b_savings": [25.0, 10.0]
    })

    result = check_340b_savings_logic(df)
    assert result["passed"] is False
    assert result["invalid_count"] == 1
