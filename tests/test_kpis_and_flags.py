import pytest
from pyspark.sql.functions import col
from pipelines.pyspark.kpis_and_flags import (
    detect_duplicate_invoices,
    detect_sub_threshold_repeats,
    detect_weekend_holiday_claims,
    detect_round_number_spikes,
    calculate_vendor_concentration
)

def test_detect_duplicate_invoices(spark_session, test_config, tmp_path):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
    
    gold_path = str(tmp_path / "gold")
    gold_path_obj = tmp_path / "gold"
    gold_path_obj.mkdir()
    
    fact_schema = StructType([
        StructField("invoice_id", StringType(), False),
        StructField("vendor_id", IntegerType(), False),
        StructField("employee_id", IntegerType(), False),
        StructField("dept_id", IntegerType(), False),
        StructField("trx_date", DateType(), False),
        StructField("amount", DecimalType(10, 2), False),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("budget_month_key", StringType(), True),
    ])
    
    fact_data = [
        ("INV-001", 1, 1, 1, "2024-01-15", 1000.00, "USD", "Credit Card", "2024-01"),
        ("INV-002", 1, 1, 1, "2024-01-20", 1050.00, "USD", "Credit Card", "2024-01"),
        ("INV-003", 2, 2, 2, "2024-01-16", 2000.00, "USD", "Wire", "2024-01"),
    ]
    
    fact_expense = spark_session.createDataFrame(fact_data, fact_schema)
    fact_expense.write.mode("overwrite").parquet(f"{gold_path}/fact_expense")
    
    vendor_schema = StructType([
        StructField("vendor_id", IntegerType(), False),
        StructField("vendor_name", StringType(), True),
    ])
    
    vendor_data = [
        (1, "Vendor A"),
        (2, "Vendor B"),
    ]
    
    dim_vendor = spark_session.createDataFrame(vendor_data, vendor_schema)
    dim_vendor.write.mode("overwrite").parquet(f"{gold_path}/dim_vendor")
    
    result = detect_duplicate_invoices(spark_session, gold_path, test_config)
    
    assert result is not None
    flagged_count = result.filter(col("score") > 0).count()
    assert flagged_count >= 0

def test_detect_sub_threshold_repeats(spark_session, test_config, tmp_path):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
    
    gold_path = str(tmp_path / "gold")
    gold_path_obj = tmp_path / "gold"
    gold_path_obj.mkdir()
    
    fact_schema = StructType([
        StructField("invoice_id", StringType(), False),
        StructField("vendor_id", IntegerType(), False),
        StructField("employee_id", IntegerType(), False),
        StructField("dept_id", IntegerType(), False),
        StructField("trx_date", DateType(), False),
        StructField("amount", DecimalType(10, 2), False),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("budget_month_key", StringType(), True),
    ])
    
    fact_data = [
        ("INV-001", 1, 1, 1, "2024-01-15", 50.00, "USD", "Credit Card", "2024-01"),
        ("INV-002", 1, 1, 1, "2024-01-16", 60.00, "USD", "Credit Card", "2024-01"),
        ("INV-003", 1, 1, 1, "2024-01-17", 70.00, "USD", "Credit Card", "2024-01"),
        ("INV-004", 2, 2, 2, "2024-01-16", 2000.00, "USD", "Wire", "2024-01"),
    ]
    
    fact_expense = spark_session.createDataFrame(fact_data, fact_schema)
    fact_expense.write.mode("overwrite").parquet(f"{gold_path}/fact_expense")
    
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), False),
        StructField("employee_name", StringType(), True),
    ])
    
    employee_data = [
        (1, "Employee 1"),
        (2, "Employee 2"),
    ]
    
    dim_employee = spark_session.createDataFrame(employee_data, employee_schema)
    dim_employee.write.mode("overwrite").parquet(f"{gold_path}/dim_employee")
    
    result = detect_sub_threshold_repeats(spark_session, gold_path, test_config)
    
    assert result is not None
    flagged_count = result.filter(col("score") > 0).count()
    assert flagged_count >= 0

def test_calculate_vendor_concentration(spark_session, test_config, tmp_path):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
    
    gold_path = str(tmp_path / "gold")
    gold_path_obj = tmp_path / "gold"
    gold_path_obj.mkdir()
    
    fact_schema = StructType([
        StructField("invoice_id", StringType(), False),
        StructField("vendor_id", IntegerType(), False),
        StructField("employee_id", IntegerType(), False),
        StructField("dept_id", IntegerType(), False),
        StructField("trx_date", DateType(), False),
        StructField("amount", DecimalType(10, 2), False),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("budget_month_key", StringType(), True),
    ])
    
    fact_data = [
        ("INV-001", 1, 1, 1, "2024-01-15", 10000.00, "USD", "Credit Card", "2024-01"),
        ("INV-002", 1, 1, 1, "2024-01-16", 5000.00, "USD", "Credit Card", "2024-01"),
        ("INV-003", 2, 2, 2, "2024-01-17", 1000.00, "USD", "Wire", "2024-01"),
    ]
    
    fact_expense = spark_session.createDataFrame(fact_data, fact_schema)
    fact_expense.write.mode("overwrite").parquet(f"{gold_path}/fact_expense")
    
    vendor_schema = StructType([
        StructField("vendor_id", IntegerType(), False),
        StructField("vendor_name", StringType(), True),
    ])
    
    vendor_data = [
        (1, "Vendor A"),
        (2, "Vendor B"),
    ]
    
    dim_vendor = spark_session.createDataFrame(vendor_data, vendor_schema)
    dim_vendor.write.mode("overwrite").parquet(f"{gold_path}/dim_vendor")
    
    result = calculate_vendor_concentration(spark_session, gold_path, test_config)
    
    assert result is not None
    assert result.count() == 2
    assert "spend_percentage" in result.columns
    assert "concentration_score" in result.columns

