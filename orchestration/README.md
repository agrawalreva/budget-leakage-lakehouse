# Airflow Orchestration

This directory contains Airflow DAGs for orchestrating the budget leakage detection pipeline.

## Setup

1. Install Airflow:
```bash
pip install apache-airflow==2.7.3
```

2. Initialize Airflow database:
```bash
airflow db init
```

3. Create admin user:
```bash
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

4. Start Airflow webserver:
```bash
airflow webserver --port 8080
```

5. Start Airflow scheduler:
```bash
airflow scheduler
```

## DAGs

- `budget_leakage_pipeline`: Main ETL pipeline running daily
  - bronze_to_silver: Transform raw CSV to cleaned Parquet
  - silver_to_gold: Build star schema
  - kpis_and_flags: Calculate KPIs and detect leakage

## Configuration

Set the `AIRFLOW_HOME` environment variable to point to this directory:
```bash
export AIRFLOW_HOME=/path/to/budget-leakage-lakehouse/orchestration
```

