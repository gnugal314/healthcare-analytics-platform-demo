# Healthcare Analytics Pipeline Demo
Production-style Python data pipelines for healthcare claims, pharmacy 340B analytics, data quality validation, and analytics-ready outputs.

## Overview

This project demonstrates how I design modular, analytics-ready healthcare data pipelines using Python. It is structured as a multi-domain analytics platform with two core use cases:

- **Claims Analytics Pipeline**
- **Pharmacy 340B Analytics Pipeline**

The goal is to simulate the kind of engineering work required in real healthcare environments where data must be trustworthy, scalable, auditable, and usable by downstream reporting and analytics teams.

This repository focuses on the parts of healthcare analytics that matter most in production:

- ingestion and staging
- transformation and dimensional modeling
- data quality validation
- observability and pipeline health
- analytics-ready output datasets

## Key Differentiators

- Multi-domain healthcare analytics design
- Synthetic claims and pharmacy 340B datasets
- Built-in data quality validation
- Modular raw, staging, curated, and analytics layers
- Test coverage for core transformation and business-rule logic

## Why I Built This
This project reflects my approach to building data platforms that are not only technically sound, but trusted by the business and usable in real operational contexts.

Analytics pipelines do more than move data. They support operational reporting, regulatory readiness, clinical decision-making, and financial visibility. I built this project to demonstrate how I approach Python-based data engineering in a way that emphasizes:

- **scale**
- **reliability**
- **business impact**
- **trust in data**

Rather than creating a notebook-only demo, I structured this project like a maintainable analytics platform with clear separation between raw ingestion, curated models, quality checks, and analytics outputs.

## Project Architecture

The platform includes two domain pipelines:

### 1. Claims Analytics Pipeline
Processes synthetic claims transactions into curated fact and dimension tables and produces analytics outputs such as:

- monthly utilization
- provider performance
- member cost summaries

### 2. Pharmacy 340B Analytics Pipeline
Processes synthetic outpatient pharmacy transactions with 340B-style attributes and produces analytics outputs such as:

- monthly 340B savings
- drug utilization summaries
- covered entity performance
- payer mix reporting

## Core Features

- Modular Python pipeline design
- Raw, staging, curated, and analytics layers
- Data quality validation framework
- Pipeline observability and run metrics
- Synthetic healthcare datasets with injected quality issues
- Reusable dimensional models for BI and analytics consumption
- Testable code structure for maintainability

## Tech Stack

- Python
- Pandas
- Parquet
- PyArrow
- Pytest
- YAML-based configuration

Optional future enhancements:
- DuckDB
- Great Expectations
- Airflow
- Streamlit or Power BI for presentation layer

## Repository Structure

```text
healthcare-analytics-platform-demo/
├── README.md
├── requirements.txt
├── .gitignore
├── pyproject.toml
├── Makefile
├── data/
├── docs/
├── scripts/
├── src/
│   └── healthcare_analytics/
│       ├── shared/
│       ├── claims/
│       └── pharmacy_340b/
└── tests/
```
## Setup

python3 -m venv .venv  
source .venv/bin/activate  
pip install -r requirements.txt  
