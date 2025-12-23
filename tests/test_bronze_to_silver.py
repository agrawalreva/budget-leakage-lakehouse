import pytest
from pyspark.sql.functions import col
from pipelines.pyspark.bronze_to_silver import (
    transform_expenses,
    transform_budgets,
    transform_campaign_spend,
    transform_holidays,
    get_expenses_schema,
    get_budgets_schema
)

def test_expenses_schema():
    schema = get_expenses_schema()
    assert schema is not None
    assert len(schema.fields) == 12

def test_budgets_schema():
    schema = get_budgets_schema()
    assert schema is not None
    assert len(schema.fields) == 5

def test_transform_expenses(spark_session, test_config, sample_expense_data, tmp_path):
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    
    bronze_path_obj = tmp_path / "bronze"
    bronze_path_obj.mkdir()
    
    sample_expense_data.write.mode("overwrite").csv(
        f"{bronze_path}/expenses_small.csv",
        header=True
    )
    
    result = transform_expenses(
        spark_session,
        bronze_path,
        silver_path,
        "expenses_small.csv"
    )
    
    assert result is not None
    assert result.count() == 3
    assert "ingestion_timestamp" in result.columns
    assert "source_file" in result.columns

def test_transform_expenses_filters_invalid(spark_session, test_config, tmp_path):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
    
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    
    bronze_path_obj = tmp_path / "bronze"
    bronze_path_obj.mkdir()
    
    schema = StructType([
        StructField("invoice_id", StringType(), False),
        StructField("vendor_id", IntegerType(), False),
        StructField("vendor_name", StringType(), True),
        StructField("employee_id", IntegerType(), False),
        StructField("employee_name", StringType(), True),
        StructField("dept_id", IntegerType(), False),
        StructField("dept_name", StringType(), True),
        StructField("trx_date", DateType(), False),
        StructField("amount", DecimalType(10, 2), False),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("description", StringType(), True)
    ])
    
    data = [
        ("INV-001", 1, "Vendor A", 1, "Employee 1", 1, "Engineering", "2024-01-15", 1000.00, "USD", "Credit Card", "Test"),
        (None, 2, "Vendor B", 2, "Employee 2", 2, "Marketing", "2024-01-16", 2000.00, "USD", "Wire", "Test"),
        ("INV-003", 3, "Vendor C", 3, "Employee 3", 3, "Sales", "2024-01-17", -100.00, "USD", "Check", "Test"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df.write.mode("overwrite").csv(
        f"{bronze_path}/expenses_small.csv",
        header=True
    )
    
    result = transform_expenses(
        spark_session,
        bronze_path,
        silver_path,
        "expenses_small.csv"
    )
    
    assert result.count() == 1

def test_transform_expenses_deduplication(spark_session, test_config, tmp_path):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
    
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    
    bronze_path_obj = tmp_path / "bronze"
    bronze_path_obj.mkdir()
    
    schema = StructType([
        StructField("invoice_id", StringType(), False),
        StructField("vendor_id", IntegerType(), False),
        StructField("vendor_name", StringType(), True),
        StructField("employee_id", IntegerType(), False),
        StructField("employee_name", StringType(), True),
        StructField("dept_id", IntegerType(), False),
        StructField("dept_name", StringType(), True),
        StructField("trx_date", DateType(), False),
        StructField("amount", DecimalType(10, 2), False),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("description", StringType(), True)
    ])
    
    data = [
        ("INV-001", 1, "Vendor A", 1, "Employee 1", 1, "Engineering", "2024-01-15", 1000.00, "USD", "Credit Card", "Test"),
        ("INV-001", 1, "Vendor A", 1, "Employee 1", 1, "Engineering", "2024-01-15", 1000.00, "USD", "Credit Card", "Duplicate"),
        ("INV-002", 2, "Vendor B", 2, "Employee 2", 2, "Marketing", "2024-01-16", 2000.00, "USD", "Wire", "Test"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    df.write.mode("overwrite").csv(
        f"{bronze_path}/expenses_small.csv",
        header=True
    )
    
    result = transform_expenses(
        spark_session,
        bronze_path,
        silver_path,
        "expenses_small.csv"
    )
    
    assert result.count() == 2

def test_transform_budgets(spark_session, test_config, sample_budget_data, tmp_path):
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    
    bronze_path_obj = tmp_path / "bronze"
    bronze_path_obj.mkdir()
    
    sample_budget_data.write.mode("overwrite").csv(
        f"{bronze_path}/budgets_small.csv",
        header=True
    )
    
    result = transform_budgets(
        spark_session,
        bronze_path,
        silver_path,
        "budgets_small.csv"
    )
    
    assert result is not None
    assert result.count() == 2
    assert "ingestion_timestamp" in result.columns

