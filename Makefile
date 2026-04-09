.PHONY: generate-data run test

generate-data:
	python3 scripts/generate_claims_data.py
	python3 scripts/generate_pharmacy_340b_data.py

run:
	PYTHONPATH=src python3 -m healthcare_analytics.main

test:
	pytest tests/
