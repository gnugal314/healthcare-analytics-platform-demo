import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
from pathlib import Path

fake = Faker()
np.random.seed(42)
random.seed(42)

N_ROWS = 5000


def random_date():
    start = datetime(2023, 1, 1)
    end = datetime(2025, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_claims_data(n_rows: int) -> pd.DataFrame:
    rows = []

    for i in range(n_rows):
        claim_amount = round(np.random.uniform(75, 5000), 2)
        paid_amount = round(claim_amount * np.random.uniform(0.5, 1.0), 2)

        rows.append({
            "claim_id": i + 1,
            "member_id": random.randint(1000, 3000),
            "provider_id": random.randint(400, 900),
            "member_name": fake.name(),
            "provider_name": fake.name(),
            "service_date": random_date(),
            "claim_amount": claim_amount,
            "paid_amount": paid_amount,
            "units": random.randint(1, 8),
            "claim_status": random.choice(["PAID", "DENIED", "PENDING"]),
            "line_of_business": random.choice(["Commercial", "Medicare", "Medicaid"]),
            "procedure_code": random.choice(["99213", "99214", "80050", "93000"]),
            "diagnosis_code": random.choice(["E11.9", "I10", "M54.5", "J20.9"]),
            "provider_specialty": random.choice(["Primary Care", "Cardiology", "Orthopedics"]),
            "provider_region": random.choice(["Midwest", "South", "West"]),
            "member_dob": fake.date_of_birth(minimum_age=18, maximum_age=90),
            "member_gender": random.choice(["M", "F"]),
            "plan_name": random.choice(["Plan A", "Plan B", "Plan C"]),
        })

    df = pd.DataFrame(rows)

    df.loc[np.random.choice(df.index, size=50, replace=False), "claim_amount"] = -1
    df.loc[np.random.choice(df.index, size=50, replace=False), "member_id"] = None
    df = pd.concat([df, df.sample(20, random_state=42)], ignore_index=True)

    output_path = Path("data/raw/claims_raw_sample.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return df


if __name__ == "__main__":
    df = generate_claims_data(N_ROWS)
    print(f"Generated {len(df)} claims rows")
