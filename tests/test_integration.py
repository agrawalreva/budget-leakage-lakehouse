import pytest
import os
import shutil
from pathlib import Path

def test_full_pipeline_integration(spark_session, test_config, tmp_path):
    from pipelines.pyspark.bronze_to_silver import transform_expenses, transform_budgets
    from pipelines.pyspark.silver_to_gold import build_dimensions, build_facts
    from pipelines.pyspark.kpis_and_flags import detect_duplicate_invoices, calculate_vendor_concentration
    
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    gold_path = str(tmp_path / "gold")
    
    for path in [bronze_path, silver_path, gold_path]:
        Path(path).mkdir(parents=True, exist_ok=True)
    
    sample_data_path = Path(__file__).parent.parent / "data" / "samples"
    
    if not (sample_data_path / "expenses_small.csv").exists():
        pytest.skip("Sample data not found")
    
    expenses_df = transform_expenses(
        spark_session,
        str(sample_data_path),
        silver_path,
        "expenses_small.csv"
    )
    
    assert expenses_df is not None
    assert expenses_df.count() > 0
    
    budgets_df = transform_budgets(
        spark_session,
        str(sample_data_path),
        silver_path,
        "budgets_small.csv"
    )
    
    assert budgets_df is not None
    
    dim_vendor, dim_employee, dim_department, dim_campaign, dim_calendar = build_dimensions(
        spark_session,
        silver_path
    )
    
    assert dim_vendor is not None
    assert dim_employee is not None
    assert dim_department is not None
    
    fact_expense, fact_campaign_spend = build_facts(
        spark_session,
        silver_path,
        dim_vendor,
        dim_employee,
        dim_department,
        dim_calendar
    )
    
    assert fact_expense is not None
    assert fact_expense.count() > 0
    
    fact_expense.write.mode("overwrite").parquet(f"{gold_path}/fact_expense")
    dim_vendor.write.mode("overwrite").parquet(f"{gold_path}/dim_vendor")
    dim_employee.write.mode("overwrite").parquet(f"{gold_path}/dim_employee")
    dim_department.write.mode("overwrite").parquet(f"{gold_path}/dim_department")
    
    duplicate_flags = detect_duplicate_invoices(spark_session, gold_path, test_config)
    assert duplicate_flags is not None
    
    vendor_concentration = calculate_vendor_concentration(spark_session, gold_path, test_config)
    assert vendor_concentration is not None
    assert vendor_concentration.count() > 0

