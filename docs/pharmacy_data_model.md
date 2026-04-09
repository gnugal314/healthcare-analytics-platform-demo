# Pharmacy 340B Data Model

## Fact table
fact_rx_claims

Key measures:
- quantity_dispensed
- ingredient_cost
- total_paid_amount
- estimated_340b_savings

Key flags:
- is_340b_eligible
- medicaid_indicator
- orphan_drug_indicator

## Dimensions
- dim_drug
- dim_pharmacy
- dim_prescriber
- dim_rx_date
