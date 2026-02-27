# suhailsat-demo-challengeC

## Overview

This project implements a reproducible ETL pipeline using:

- Python
- YAML configuration
- Git
- Local execution only (no cloud services)

The pipeline reads raw data (CSV/JSON), applies cleaning and transformation logic, and produces a final processed dataset.

---

## Project Structure
├── config/
│   └── pipeline.yaml
├── data/
│   ├── raw/
│   ├── processed/
│   └── logs/
├── src/
│   ├── pipeline.py
│   ├── io_utils.py
│   ├── transform.py
│   ├── quality.py
│   └── utils.py
├── requirements.txt
└── README.md

## Setup

### 1. Create virtual environment

python -m venv .venv
source .venv/bin/activate

### 2. Install dependencies

pip install -r requirements.txt

### 3. Install dependencies

python src/pipeline.py --config config/pipeline.yaml

## Output
	•	Processed dataset: data/processed/final_dataset.csv
	•	Execution logs: data/logs/run-<timestamp>.log

## Reproducibility 

	•	All pipeline parameters are externalized in YAML.
	•	Outputs are deterministic given identical inputs.
	•	Logs are timestamped and stored locally.
	•	No external services are required.

## Design Principles 

	•	Modularity (separated IO, transformation, quality)
	•	Config-driven execution
	•	Structured logging
	•	Clean folder separation
    •	Git Standard structure

## Processing
	•	Duplicates
	•	Negative values

Pipeline performs:
	•	Column renaming
	•	Type casting
	•	Null filtering
	•	Duplicate removal
	•	Value filtering (>= 0)

