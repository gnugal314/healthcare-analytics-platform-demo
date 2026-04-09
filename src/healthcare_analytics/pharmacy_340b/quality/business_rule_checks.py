import pandas as pd


def check_non_negative_fields(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    results = []

    for col in columns:
        if col not in df.columns:
            results.append({
                "check_name": f"{col}_non_negative",
                "passed": False,
                "invalid_count": -1
            })
            continue

        invalid_count = int((df[col] < 0).sum())
        results.append({
            "check_name": f"{col}_non_negative",
            "passed": invalid_count == 0,
            "invalid_count": invalid_count
        })

    return results


def check_340b_savings_logic(df: pd.DataFrame) -> dict:
    invalid = df[
        (df["is_340b_eligible"] == 0) &
        (df["estimated_340b_savings"] > 0)
    ]

    return {
        "check_name": "340b_savings_logic_check",
        "passed": len(invalid) == 0,
        "invalid_count": int(len(invalid))
    }
