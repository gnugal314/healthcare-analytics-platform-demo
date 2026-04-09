import pandas as pd


def clean_claims_data(df: pd.DataFrame) -> pd.DataFrame:
    clean_df = df.copy()

    string_cols = [
        "member_name", "provider_name", "plan_name",
        "line_of_business", "provider_specialty", "provider_region"
    ]
    for col in string_cols:
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].astype(str).str.strip()

    numeric_cols = ["claim_amount", "paid_amount", "units"]
    for col in numeric_cols:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

    if "service_date" in clean_df.columns:
        clean_df["service_date"] = pd.to_datetime(clean_df["service_date"], errors="coerce")

    clean_df = clean_df.dropna(subset=["claim_id", "member_id", "provider_id"])
    clean_df = clean_df.drop_duplicates(subset=["claim_id"])
    clean_df = clean_df[clean_df["claim_amount"] >= 0]
    clean_df = clean_df[clean_df["paid_amount"] >= 0]

    return clean_df
