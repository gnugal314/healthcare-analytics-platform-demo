import pandas as pd


def clean_pharmacy_data(df: pd.DataFrame) -> pd.DataFrame:
    clean_df = df.copy()

    string_cols = [
        "drug_name", "generic_name", "drug_class", "payer_type",
        "line_of_business", "prescriber_specialty", "pharmacy_type",
        "encounter_type", "claim_status", "ndc_code"
    ]
    for col in string_cols:
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].astype(str).str.strip()

    numeric_cols = [
        "days_supply", "quantity_dispensed", "ingredient_cost",
        "dispensing_fee", "total_paid_amount", "wac_unit_cost",
        "estimated_340b_unit_cost", "estimated_340b_savings"
    ]
    for col in numeric_cols:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

    if "fill_date" in clean_df.columns:
        clean_df["fill_date"] = pd.to_datetime(clean_df["fill_date"], errors="coerce")

    clean_df = clean_df.dropna(subset=["rx_claim_id", "member_id", "pharmacy_id", "ndc_code"])
    clean_df = clean_df.drop_duplicates(subset=["rx_claim_id"])

    clean_df = clean_df[clean_df["quantity_dispensed"] >= 0]
    clean_df = clean_df[clean_df["ingredient_cost"] >= 0]

    return clean_df
