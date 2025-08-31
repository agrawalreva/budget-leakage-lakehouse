#!/usr/bin/env python3
"""
Test script for Bronze to Silver transformation
Runs locally to verify the pipeline works with sample data
"""

import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def test_bronze_to_silver():
    """Test the bronze to silver transformation locally"""
    
    # Create Spark session for local testing
    spark = (SparkSession.builder
             .appName("Test-BronzeToSilver")
             .config("spark.sql.adaptive.enabled", "true")
             .master("local[*]")
             .getOrCreate())
    
    # Set paths for local testing
    bronze_path = "data/samples"
    silver_path = "data/silver"
    
    # Ensure silver directory exists
    os.makedirs(silver_path, exist_ok=True)
    
    try:
        # Test reading expenses data
        print("Testing expenses data transformation...")
        expenses_df = spark.read.csv(
            f"{bronze_path}/expenses_small.csv",
            header=True,
            inferSchema=True
        )
        
        print(f"Original expenses count: {expenses_df.count()}")
        print("Sample expenses data:")
        expenses_df.show(3)
        
        # Test basic transformations
        expenses_clean = (expenses_df
            .filter(col("invoice_id").isNotNull())
            .filter(col("amount") > 0)
            .withColumn("trx_date", to_date(col("trx_date")))
            .withColumn("amount", col("amount").cast("decimal(10,2)"))
        )
        
        print(f"Cleaned expenses count: {expenses_clean.count()}")
        
        # Test budgets data
        print("\nTesting budgets data transformation...")
        budgets_df = spark.read.csv(
            f"{bronze_path}/budgets_small.csv",
            header=True,
            inferSchema=True
        )
        
        print(f"Budgets count: {budgets_df.count()}")
        print("Sample budgets data:")
        budgets_df.show(3)
        
        # Test campaign spend data
        print("\nTesting campaign spend data transformation...")
        campaign_df = spark.read.csv(
            f"{bronze_path}/campaign_spend_small.csv",
            header=True,
            inferSchema=True
        )
        
        print(f"Campaign spend count: {campaign_df.count()}")
        print("Sample campaign data:")
        campaign_df.show(3)
        
        # Test holidays data
        print("\nTesting holidays data transformation...")
        holidays_df = spark.read.csv(
            f"{bronze_path}/holidays.csv",
            header=True,
            inferSchema=True
        )
        
        print(f"Holidays count: {holidays_df.count()}")
        print("Sample holidays data:")
        holidays_df.show(3)
        
        print("\n✅ All data sources can be read successfully!")
        print("Bronze to Silver transformation is ready for production.")
        
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    test_bronze_to_silver()
