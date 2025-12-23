import pytest
import sys
from pathlib import Path
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config_loader import load_config

@pytest.fixture(scope="session")
def spark_session():
    config = load_config()
    spark_config = config['spark']
    
    spark = (SparkSession.builder
             .appName("test")
             .master("local[2]")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.driver.memory", "1g")
             .getOrCreate())
    
    yield spark
    spark.stop()

@pytest.fixture
def test_config():
    return load_config()

@pytest.fixture
def sample_expense_data(spark_session):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
    
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
        ("INV-001", 1, "Vendor A", 1, "Employee 1", 1, "Engineering", "2024-01-15", 1000.00, "USD", "Credit Card", "Test expense"),
        ("INV-002", 2, "Vendor B", 2, "Employee 2", 2, "Marketing", "2024-01-16", 2000.00, "USD", "Wire", "Test expense 2"),
        ("INV-003", 1, "Vendor A", 1, "Employee 1", 1, "Engineering", "2024-01-17", 1500.00, "USD", "Check", "Test expense 3"),
    ]
    
    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def sample_budget_data(spark_session):
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
    
    schema = StructType([
        StructField("dept_id", IntegerType(), False),
        StructField("dept_name", StringType(), True),
        StructField("budget_month", StringType(), False),
        StructField("budget_amount", DecimalType(12, 2), False),
        StructField("currency", StringType(), True)
    ])
    
    data = [
        (1, "Engineering", "2024-01", 50000.00, "USD"),
        (2, "Marketing", "2024-01", 40000.00, "USD"),
    ]
    
    return spark_session.createDataFrame(data, schema)

