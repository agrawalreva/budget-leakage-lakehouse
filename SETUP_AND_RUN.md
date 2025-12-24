# Setup and Run Guide

## Installation

### 1. Install Dependencies

```bash
# Install all requirements
pip install -r requirements.txt

# Or install PySpark specifically
pip install pyspark==3.5.0

# Verify installation
python -c "from pyspark.sql import SparkSession; print('PySpark installed!')"
```

**Note:** PySpark requires Java. If you get Java errors:
- macOS: `brew install openjdk@11`
- Linux: `sudo apt-get install openjdk-11-jdk`
- Windows: Download from Oracle or use Chocolatey

### 2. Verify Java Installation

```bash
java -version
# Should show Java 8, 11, or 17
```

## Running the Real Pipeline

### Step 1: Generate Sample Data

```bash
cd data/samples
python generate_samples.py
cd ../..
```

### Step 2: Run Bronze to Silver Transformation

```bash
python pipelines/pyspark/bronze_to_silver.py
```

**What it does:**
- Reads CSV files from `data/samples/`
- Validates schemas
- Cleans data (removes nulls, filters invalid amounts)
- Deduplicates records
- Writes Parquet files to `data/silver/`

**Expected output:**
```
INFO:__main__:Processing expenses data...
INFO:__main__:Processed 1500 expense records
INFO:__main__:Processing budgets data...
INFO:__main__:Processed 84 budget records
...
```

### Step 3: Run Silver to Gold Transformation

```bash
python pipelines/pyspark/silver_to_gold.py
```

**What it does:**
- Reads Parquet from `data/silver/`
- Builds 5 dimension tables (vendor, employee, department, campaign, calendar)
- Builds 2 fact tables (expense, campaign_spend)
- Creates star schema with dimension keys
- Writes to `data/gold/` with partitioning

**Expected output:**
```
INFO:__main__:Building dimension tables...
INFO:__main__:Built dimensions: vendor(50), employee(100), department(7)...
INFO:__main__:Built facts: expense(1500), campaign_spend(800)
```

### Step 4: Run KPI and Leakage Detection

```bash
python pipelines/pyspark/kpis_and_flags.py
```

**What it does:**
- Detects 5 types of leakage patterns
- Calculates budget variance
- Analyzes vendor concentration
- Writes leakage signals and KPIs to `data/gold/`

**Expected output:**
```
INFO:__main__:Detecting duplicate invoices...
INFO:__main__:Detecting sub-threshold repeats...
INFO:__main__:Persisted 105 leakage signals
```

### Step 5: Calculate Metrics

```bash
python metrics/calculate_metrics.py
```

**What it does:**
- Reads all gold layer data
- Calculates comprehensive metrics
- Estimates potential leakage
- Prints formatted report
- Saves JSON report to `metrics/metrics_report.json`

## Running the Dashboard

### Launch Streamlit Dashboard

```bash
streamlit run dashboard/app.py
```

**What happens:**
- Dashboard starts on `http://localhost:8501`
- Automatically opens in your browser
- Shows 4 interactive tabs:
  1. **Overview** - Executive summary, expense trends, leakage by type
  2. **Leakage Signals** - Filterable table of all detected anomalies
  3. **Budget Analysis** - Budget vs actual variance by department
  4. **Vendor Risk** - Vendor concentration and risk analysis

**Features:**
- Interactive Plotly charts
- Real-time filtering
- Risk score visualization
- Time-series analysis

### Dashboard Requirements

The dashboard reads from `data/gold/`, so you must run the pipeline first:
1. bronze_to_silver.py
2. silver_to_gold.py
3. kpis_and_flags.py

Then the dashboard will show real data!

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=pipelines --cov=data_quality --cov-report=html

# Run specific test file
pytest tests/test_bronze_to_silver.py -v
```

## Running Data Quality Checks

```bash
python data_quality/run_quality_checks.py
```

**What it does:**
- Validates silver and gold layer data
- Checks for nulls, duplicates, invalid ranges
- Reports pass/fail status for each check

## Troubleshooting

### PySpark Installation Issues

If PySpark fails to install:
```bash
# Try with pip3
pip3 install pyspark==3.5.0

# Or use conda
conda install -c conda-forge pyspark
```

### Java Issues

If you get "JAVA_HOME is not set":
```bash
# macOS
export JAVA_HOME=$(/usr/libexec/java_home)

# Linux
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Add to ~/.bashrc or ~/.zshrc to make permanent
```

### Memory Issues

If Spark runs out of memory, edit `config/config.yaml`:
```yaml
spark:
  executor_memory: "1g"  # Reduce if needed
  driver_memory: "512m"  # Reduce if needed
```

### Dashboard Not Loading Data

Make sure you've run all pipeline steps:
1. Check that `data/silver/` directory exists with Parquet files
2. Check that `data/gold/` directory exists with Parquet files
3. Run `python pipelines/pyspark/kpis_and_flags.py` to generate leakage signals

## Quick Start (All Steps)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate data
cd data/samples && python generate_samples.py && cd ../..

# 3. Run full pipeline
python pipelines/pyspark/bronze_to_silver.py
python pipelines/pyspark/silver_to_gold.py
python pipelines/pyspark/kpis_and_flags.py

# 4. Calculate metrics
python metrics/calculate_metrics.py

# 5. Launch dashboard
streamlit run dashboard/app.py
```

## Verification

After running the pipeline, verify output:

```bash
# Check silver layer
ls -lh data/silver/*/

# Check gold layer
ls -lh data/gold/*/

# Check metrics
cat metrics/metrics_report.json
```

All the pipeline code is **100% real** - it's actual PySpark code that processes real data files. The demo script (`demo_pipeline.py`) just simulates the output for demonstration purposes when PySpark isn't installed.

