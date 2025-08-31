# Budget Leakage Lakehouse

A focused data lakehouse that surfaces **Budget vs Actual variance** and **Spend Leakage flags** for FP&A and marketing spend analysis.

## Purpose

This project demonstrates a production-lean data architecture for:
- **Budget Intelligence**: Track monthly budget vs actual spending by department
- **Leakage Detection**: Identify suspicious spending patterns and anomalies
- **Marketing ROI**: Monitor campaign performance and cost efficiency

## Architecture

```
S3 Bronze (CSV) → PySpark Silver (Parquet) → PySpark Gold (Star Schema) → Snowflake → BI Tools
```

## Quick Start

1. **Drop sample CSVs** to S3 `/bronze/` directory
2. **Run PySpark jobs** in sequence:
   - `bronze_to_silver.py` - Data validation and cleaning
   - `silver_to_gold.py` - Star schema construction
   - `kpis_and_flags.py` - Leakage detection and KPIs
3. **Create Snowflake tables** using `warehouse/ddl_snowflake.sql`
4. **Connect BI tool** to Snowflake views for analysis

## Key Outputs

- **Budget Variance**: Monthly department spending vs budget
- **Leakage Flags**: Suspicious patterns (duplicates, weekend claims, round numbers)
- **Vendor Concentration**: Top spenders and risk analysis
- **Campaign ROI**: Marketing efficiency metrics

## Next Steps

- Add orchestration (Airflow/Prefect)
- Implement cost controls and monitoring
- Scale to production data volumes
