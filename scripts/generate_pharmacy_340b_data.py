import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
from pathlib import Path

fake = Faker()
np.random.seed(42)
random.seed(42)

N_ROWS = 5000


DRUGS = [
    {"ndc_code": "00093-7424-56", "drug_name": "Lisinopril 10mg", "generic_name": "Lisinopril", "drug_class": "ACE Inhibitor"},
    {"ndc_code": "00172-3928-60", "drug_name": "Metformin 500mg", "generic_name": "Metformin", "drug_class": "Antidiabetic"},
    {"ndc_code": "00078-0615-15", "drug_name": "Atorvastatin 20mg", "generic_name": "Atorvastatin", "drug_class": "Statin"},
    {"ndc_code": "54868-6174-00", "drug_name": "Albuterol HFA", "generic_name": "Albuterol", "drug_class": "Bronchodilator"},
    {"ndc_code": "60505-2515-03", "drug_name": "Amoxicillin 500mg", "generic_name": "Amoxicillin", "drug_class": "Antibiotic"},
    {"ndc_code": "55513-8410-01", "drug_name": "Insulin Glargine", "generic_name": "Insulin Glargine", "drug_class": "Antidiabetic"},
    {"ndc_code": "00378-0208-93", "drug_name": "Sertraline 50mg", "generic_name": "Sertraline", "drug_class": "Antidepressant"},
]

PAYER_TYPES = ["Medicaid", "Medicare", "Commercial", "Self Pay"]
LINES_OF_BUSINESS = ["Medicaid", "Medicare Advantage", "Commercial", "Exchange"]
PRESCRIBER_SPECIALTIES = ["Primary Care", "Cardiology", "Endocrinology", "Pulmonology", "Psychiatry"]
PHARMACY_TYPES = ["Owned Pharmacy", "Contract Pharmacy", "Retail Network"]
ENCOUNTER_TYPES = ["Outpatient Clinic", "Hospital Discharge", "Specialty Visit", "Primary Care Visit"]
CLAIM_STATUSES = ["PAID", "REVERSED", "REJECTED"]


def random_date(start_year=2023, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def create_costs():
    wac_unit_cost = round(np.random.uniform(1.0, 250.0), 2)
    estimated_340b_unit_cost = round(wac_unit_cost * np.random.uniform(0.45, 0.8), 2)
    return wac_unit_cost, estimated_340b_unit_cost


def determine_340b_eligibility(
    payer_type: str,
    encounter_type: str,
    orphan_drug_indicator: int,
    claim_status: str
) -> int:
    if claim_status != "PAID":
        return 0
    if orphan_drug_indicator == 1:
        return 0
    if encounter_type not in ["Outpatient Clinic", "Primary Care Visit", "Specialty Visit"]:
        return 0
    if payer_type == "Self Pay":
        return 0
    return 1


def generate_pharmacy_340b_data(n_rows: int) -> pd.DataFrame:
    rows = []

    for i in range(n_rows):
        drug = random.choice(DRUGS)
        payer_type = random.choice(PAYER_TYPES)
        line_of_business = random.choice(LINES_OF_BUSINESS)
        encounter_type = random.choice(ENCOUNTER_TYPES)
        claim_status = random.choices(
            CLAIM_STATUSES,
            weights=[0.9, 0.05, 0.05],
            k=1
        )[0]

        quantity_dispensed = random.choice([30, 60, 90])
        days_supply = random.choice([30, 60, 90])
        dispensing_fee = round(np.random.uniform(1.5, 12.0), 2)

        wac_unit_cost, estimated_340b_unit_cost = create_costs()
        ingredient_cost = round(quantity_dispensed * wac_unit_cost, 2)

        total_paid_amount = round(
            ingredient_cost * np.random.uniform(0.75, 1.2) + dispensing_fee,
            2
        )

        orphan_drug_indicator = np.random.choice([0, 1], p=[0.95, 0.05])
        medicaid_indicator = 1 if payer_type == "Medicaid" else 0

        is_340b_eligible = determine_340b_eligibility(
            payer_type=payer_type,
            encounter_type=encounter_type,
            orphan_drug_indicator=orphan_drug_indicator,
            claim_status=claim_status
        )

        if is_340b_eligible == 1:
            estimated_340b_savings = round(
                quantity_dispensed * (wac_unit_cost - estimated_340b_unit_cost),
                2
            )
        else:
            estimated_340b_savings = 0.0

        row = {
            "rx_claim_id": i + 1,
            "member_id": random.randint(1000, 3000),
            "prescriber_id": random.randint(400, 850),
            "pharmacy_id": random.randint(100, 180),
            "ndc_code": drug["ndc_code"],
            "drug_name": drug["drug_name"],
            "generic_name": drug["generic_name"],
            "drug_class": drug["drug_class"],
            "fill_date": random_date(),
            "days_supply": days_supply,
            "quantity_dispensed": quantity_dispensed,
            "ingredient_cost": ingredient_cost,
            "dispensing_fee": dispensing_fee,
            "total_paid_amount": total_paid_amount,
            "payer_type": payer_type,
            "line_of_business": line_of_business,
            "patient_zip": fake.postcode(),
            "prescriber_specialty": random.choice(PRESCRIBER_SPECIALTIES),
            "pharmacy_type": random.choice(PHARMACY_TYPES),
            "covered_entity_id": random.randint(10, 25),
            "encounter_type": encounter_type,
            "medicaid_indicator": medicaid_indicator,
            "orphan_drug_indicator": orphan_drug_indicator,
            "is_340b_eligible": is_340b_eligible,
            "wac_unit_cost": wac_unit_cost,
            "estimated_340b_unit_cost": estimated_340b_unit_cost,
            "estimated_340b_savings": estimated_340b_savings,
            "claim_status": claim_status,
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    # Inject realistic data quality issues
    df.loc[np.random.choice(df.index, size=40, replace=False), "quantity_dispensed"] = -1
    df.loc[np.random.choice(df.index, size=35, replace=False), "ndc_code"] = None
    df.loc[np.random.choice(df.index, size=25, replace=False), "ingredient_cost"] = -5
    df = pd.concat([df, df.sample(15, random_state=42)], ignore_index=True)

    return df


if __name__ == "__main__":
    df = generate_pharmacy_340b_data(N_ROWS)

    output_path = Path("data/raw/pharmacy_340b_raw_sample.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"Saved synthetic pharmacy 340B-style dataset to {output_path}")
    print(df.head())
