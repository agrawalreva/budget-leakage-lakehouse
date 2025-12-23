#!/usr/bin/env python3
"""
Bronze to Silver Transformation Pipeline
Reads CSVs from S3 bronze layer, enforces schema, casts types, de-dupes, writes Parquet to silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from config.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(config):
    spark_config = config['spark']
    return (SparkSession.builder
            .appName(f"{spark_config['app_name']}-BronzeToSilver")
            .config("spark.sql.adaptive.enabled", str(spark_config['adaptive_enabled']).lower())
            .config("spark.sql.adaptive.coalescePartitions.enabled", str(spark_config['adaptive_coalesce_enabled']).lower())
            .config("spark.executor.memory", spark_config['executor_memory'])
            .config("spark.driver.memory", spark_config['driver_memory'])
            .config("spark.driver.maxResultSize", spark_config['max_result_size'])
            .getOrCreate())

def get_expenses_schema():
    """Define schema for expenses data"""
    return StructType([
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

def get_budgets_schema():
    """Define schema for budgets data"""
    return StructType([
        StructField("dept_id", IntegerType(), False),
        StructField("dept_name", StringType(), True),
        StructField("budget_month", StringType(), False),
        StructField("budget_amount", DecimalType(12, 2), False),
        StructField("currency", StringType(), True)
    ])

def get_campaign_spend_schema():
    """Define schema for campaign spend data"""
    return StructType([
        StructField("spend_id", StringType(), False),
        StructField("campaign_id", IntegerType(), False),
        StructField("campaign_name", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("objective", StringType(), True),
        StructField("date", DateType(), False),
        StructField("cost", DecimalType(10, 2), False),
        StructField("clicks", IntegerType(), True),
        StructField("impressions", IntegerType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("attributed_revenue", DecimalType(10, 2), True)
    ])

def get_holidays_schema():
    """Define schema for holidays data"""
    return StructType([
        StructField("date", DateType(), False),
        StructField("week", IntegerType(), True),
        StructField("month_key", StringType(), True),
        StructField("quarter", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("is_holiday", BooleanType(), True)
    ])

def transform_expenses(spark, bronze_path, silver_path, expenses_file):
    logger.info("Processing expenses data...")
    
    df = spark.read.csv(
        f"{bronze_path}/{expenses_file}",
        header=True,
        schema=get_expenses_schema()
    )
    
    # Data quality checks and transformations
    df_clean = (df
        .filter(col("invoice_id").isNotNull())
        .filter(col("amount") > 0)
        .filter(col("trx_date").isNotNull())
        .withColumn("trx_date", to_date(col("trx_date")))
        .withColumn("amount", col("amount").cast(DecimalType(10, 2)))
        .withColumn("vendor_id", col("vendor_id").cast(IntegerType()))
        .withColumn("employee_id", col("employee_id").cast(IntegerType()))
        .withColumn("dept_id", col("dept_id").cast(IntegerType()))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(expenses_file))
    )
    
    # Remove duplicates based on invoice_id
    df_deduped = df_clean.dropDuplicates(["invoice_id"])
    
    # Write to silver layer as Parquet
    df_deduped.write.mode("overwrite").parquet(f"{silver_path}/expenses")
    
    logger.info(f"Processed {df_deduped.count()} expense records")
    return df_deduped

def transform_budgets(spark, bronze_path, silver_path, budgets_file):
    logger.info("Processing budgets data...")
    
    df = spark.read.csv(
        f"{bronze_path}/{budgets_file}",
        header=True,
        schema=get_budgets_schema()
    )
    
    # Data quality checks and transformations
    df_clean = (df
        .filter(col("dept_id").isNotNull())
        .filter(col("budget_amount") > 0)
        .withColumn("budget_amount", col("budget_amount").cast(DecimalType(12, 2)))
        .withColumn("dept_id", col("dept_id").cast(IntegerType()))
        .withColumn("budget_month", col("budget_month"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(budgets_file))
    )
    
    # Remove duplicates based on dept_id and budget_month
    df_deduped = df_clean.dropDuplicates(["dept_id", "budget_month"])
    
    # Write to silver layer as Parquet
    df_deduped.write.mode("overwrite").parquet(f"{silver_path}/budgets")
    
    logger.info(f"Processed {df_deduped.count()} budget records")
    return df_deduped

def transform_campaign_spend(spark, bronze_path, silver_path, campaign_file):
    logger.info("Processing campaign spend data...")
    
    df = spark.read.csv(
        f"{bronze_path}/{campaign_file}",
        header=True,
        schema=get_campaign_spend_schema()
    )
    
    # Data quality checks and transformations
    df_clean = (df
        .filter(col("spend_id").isNotNull())
        .filter(col("cost") > 0)
        .filter(col("date").isNotNull())
        .withColumn("date", to_date(col("date")))
        .withColumn("cost", col("cost").cast(DecimalType(10, 2)))
        .withColumn("campaign_id", col("campaign_id").cast(IntegerType()))
        .withColumn("clicks", col("clicks").cast(IntegerType()))
        .withColumn("impressions", col("impressions").cast(IntegerType()))
        .withColumn("conversions", col("conversions").cast(IntegerType()))
        .withColumn("attributed_revenue", col("attributed_revenue").cast(DecimalType(10, 2)))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(campaign_file))
    )
    
    # Remove duplicates based on spend_id
    df_deduped = df_clean.dropDuplicates(["spend_id"])
    
    # Write to silver layer as Parquet
    df_deduped.write.mode("overwrite").parquet(f"{silver_path}/campaign_spend")
    
    logger.info(f"Processed {df_deduped.count()} campaign spend records")
    return df_deduped

def transform_holidays(spark, bronze_path, silver_path, holidays_file):
    logger.info("Processing holidays data...")
    
    df = spark.read.csv(
        f"{bronze_path}/{holidays_file}",
        header=True,
        schema=get_holidays_schema()
    )
    
    # Data quality checks and transformations
    df_clean = (df
        .filter(col("date").isNotNull())
        .withColumn("date", to_date(col("date")))
        .withColumn("week", col("week").cast(IntegerType()))
        .withColumn("year", col("year").cast(IntegerType()))
        .withColumn("is_weekend", col("is_weekend").cast(BooleanType()))
        .withColumn("is_holiday", col("is_holiday").cast(BooleanType()))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(holidays_file))
    )
    
    # Remove duplicates based on date
    df_deduped = df_clean.dropDuplicates(["date"])
    
    # Write to silver layer as Parquet
    df_deduped.write.mode("overwrite").parquet(f"{silver_path}/holidays")
    
    logger.info(f"Processed {df_deduped.count()} holiday records")
    return df_deduped

def main():
    logger.info("Starting Bronze to Silver transformation...")
    
    config = load_config()
    spark = create_spark_session(config)
    
    bronze_path = config['paths']['bronze']
    silver_path = config['paths']['silver']
    data_sources = config['data_sources']
    
    try:
        expenses_df = transform_expenses(spark, bronze_path, silver_path, data_sources['expenses_file'])
        budgets_df = transform_budgets(spark, bronze_path, silver_path, data_sources['budgets_file'])
        campaign_df = transform_campaign_spend(spark, bronze_path, silver_path, data_sources['campaign_spend_file'])
        holidays_df = transform_holidays(spark, bronze_path, silver_path, data_sources['holidays_file'])
        
        logger.info("Bronze to Silver transformation completed successfully!")
        
        # Show sample data for verification
        logger.info("Sample expenses data:")
        expenses_df.show(5)
        
        logger.info("Sample budgets data:")
        budgets_df.show(5)
        
    except Exception as e:
        logger.error(f"Error in transformation pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
