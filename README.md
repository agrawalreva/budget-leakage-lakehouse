# Budget Leakage Lakehouse

A focused data lakehouse that surfaces **Budget vs Actual variance** and **Spend Leakage flags** for FP&A and marketing spend analysis.


## What It Does

The pipeline processes expense data through three layers (Bronze → Silver → Gold) and flags potential issues like:
- Duplicate invoices from the same vendor
- Expenses submitted on weekends or holidays
- Suspicious round-number transactions
- Campaign spend spikes without corresponding results
- Budget variances that need attention

The end result is a Streamlit dashboard that shows where money might be leaking and which departments are over/under budget.

## Architecture

I went with a medallion architecture because it's clean and scalable:

- **Bronze**: Raw CSV files
- **Silver**: Cleaned and validated Parquet files
- **Gold**: Star schema with dimensions and facts

Everything flows through PySpark transformations, and the final output is a star schema that makes querying easy.

## Setup

Check `SETUP_AND_RUN.md` for complete walkthrough. 
Quick version:

```bash
# Install everything
pip install -r requirements.txt

# Generate some sample data
python data/samples/generate_samples.py

# Run the whole pipeline
python demo_pipeline.py

# Launch the dashboard
streamlit run dashboard/app.py
```

## Components

**The Pipeline:**
- Three PySpark scripts that transform data from CSV to Parquet to star schema
- Five detection algorithms that flag suspicious patterns
- Data quality checks that validate everything

**The Dashboard:**
- Eight interactive charts showing expenses, leakage signals, budget variance, and vendor risk
- Built using Streamlit and Plotly

**The Infrastructure:**
- Airflow DAG for scheduling (if you want to run this daily)
- Tests to make sure everything works
- CI/CD pipeline that runs tests automatically

## The Numbers

On the sample dataset:
- 1,500 expense transactions processed
- 2,263 leakage signals detected
- 50 vendors analyzed for concentration risk
- 84 budget records tracked across departments

The dashboard breaks all of this down with charts and insights.

## Project Layout

```
├── pipelines/pyspark/     # The transformation scripts
├── dashboard/             # Streamlit app and charts
├── data_quality/         # Validation
├── orchestration/         # Airflow DAGs
├── tests/                 # Tests
├── config/                # YAML config files
├── metrics/               # Metrics calculator
└── warehouse/             # Snowflake SQL
```

## Tech Stack

- PySpark
- Parquet
- Streamlit
- Airflow
- pytest

## Documentation

- `SETUP_AND_RUN.md`
- `dashboard/DASHBOARD_ANALYSIS.md`

## Why I Built This

Started as a way to learn PySpark and data lakehouse patterns, but ended up being a solid example of how to build an enterprise level data pipeline. The leakage detection algorithms are based on common fraud patterns, and the whole thing is structured so you can easily add new detection rules or modify existing ones.
