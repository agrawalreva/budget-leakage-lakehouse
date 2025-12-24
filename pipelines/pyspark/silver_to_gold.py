#!/usr/bin/env python3
"""
Silver to Gold Transformation Pipeline
Builds dimensions and facts, joins into star schema, writes partitioned Parquet to gold layer
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
            .appName(f"{spark_config['app_name']}-SilverToGold")
            .master("local[*]")
            .config("spark.sql.adaptive.enabled", str(spark_config['adaptive_enabled']).lower())
            .config("spark.sql.adaptive.coalescePartitions.enabled", str(spark_config['adaptive_coalesce_enabled']).lower())
            .config("spark.executor.memory", spark_config['executor_memory'])
            .config("spark.driver.memory", spark_config['driver_memory'])
            .config("spark.driver.maxResultSize", spark_config['max_result_size'])
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .getOrCreate())

def build_dimensions(spark, silver_path):
    """Build dimension tables from silver data"""
    logger.info("Building dimension tables...")
    
    # Read silver data
    expenses_df = spark.read.parquet(f"{silver_path}/expenses")
    campaign_df = spark.read.parquet(f"{silver_path}/campaign_spend")
    holidays_df = spark.read.parquet(f"{silver_path}/holidays")
    
    # Build dim_vendor
    dim_vendor = (expenses_df
        .select(
            col("vendor_id"),
            col("vendor_name"),
            lit("Technology").alias("category"),  # Simplified category
            lit("US").alias("country")  # Simplified country
        )
        .dropDuplicates(["vendor_id"])
        .withColumn("vendor_key", monotonically_increasing_id())
    )
    
    # Build dim_employee
    dim_employee = (expenses_df
        .select(
            col("employee_id"),
            col("employee_name"),
            col("dept_id"),
            lit("Employee").alias("title"),  # Simplified title
            lit("Remote").alias("location")  # Simplified location
        )
        .dropDuplicates(["employee_id"])
        .withColumn("employee_key", monotonically_increasing_id())
    )
    
    # Build dim_department
    dim_department = (expenses_df
        .select(
            col("dept_id"),
            col("dept_name"),
            concat(lit("Owner_"), col("dept_name")).alias("owner")
        )
        .dropDuplicates(["dept_id"])
        .withColumn("dept_key", monotonically_increasing_id())
    )
    
    # Build dim_campaign
    dim_campaign = (campaign_df
        .select(
            col("campaign_id"),
            col("campaign_name"),
            col("channel"),
            col("objective")
        )
        .dropDuplicates(["campaign_id"])
        .withColumn("campaign_key", monotonically_increasing_id())
    )
    
    # Build dim_calendar
    dim_calendar = (holidays_df
        .select(
            col("date"),
            col("week"),
            col("month_key"),
            col("quarter"),
            col("year"),
            col("is_weekend"),
            col("is_holiday")
        )
        .withColumn("calendar_key", monotonically_increasing_id())
    )
    
    logger.info(f"Built dimensions: vendor({dim_vendor.count()}), employee({dim_employee.count()}), "
                f"department({dim_department.count()}), campaign({dim_campaign.count()}), "
                f"calendar({dim_calendar.count()})")
    
    return dim_vendor, dim_employee, dim_department, dim_campaign, dim_calendar

def build_facts(spark, silver_path, dim_vendor, dim_employee, dim_department, dim_calendar):
    """Build fact tables with dimension keys"""
    logger.info("Building fact tables...")
    
    # Read silver data
    expenses_df = spark.read.parquet(f"{silver_path}/expenses")
    campaign_df = spark.read.parquet(f"{silver_path}/campaign_spend")
    budgets_df = spark.read.parquet(f"{silver_path}/budgets")
    
    # Build fact_expense
    fact_expense = (expenses_df
        .join(dim_vendor, expenses_df.vendor_id == dim_vendor.vendor_id, "left")
        .join(dim_employee, expenses_df.employee_id == dim_employee.employee_id, "left")
        .join(dim_department, expenses_df.dept_id == dim_department.dept_id, "left")
        .join(dim_calendar, expenses_df.trx_date == dim_calendar.date, "left")
        .select(
            monotonically_increasing_id().alias("expense_id"),
            expenses_df["invoice_id"],
            expenses_df["vendor_id"],
            expenses_df["employee_id"],
            expenses_df["dept_id"],
            expenses_df["trx_date"],
            expenses_df["amount"],
            expenses_df["currency"],
            expenses_df["payment_method"],
            dim_calendar["month_key"].alias("budget_month_key"),
            dim_vendor["vendor_key"],
            dim_employee["employee_key"],
            dim_department["dept_key"],
            dim_calendar["calendar_key"],
            current_timestamp().alias("created_timestamp")
        )
    )
    
    # Build fact_campaign_spend
    fact_campaign_spend = (campaign_df
        .join(dim_calendar, campaign_df.date == dim_calendar.date, "left")
        .select(
            campaign_df["spend_id"],
            campaign_df["campaign_id"],
            campaign_df["channel"],
            campaign_df["date"],
            campaign_df["cost"],
            campaign_df["clicks"],
            campaign_df["impressions"],
            campaign_df["conversions"],
            campaign_df["attributed_revenue"],
            dim_calendar["calendar_key"],
            current_timestamp().alias("created_timestamp")
        )
    )
    
    logger.info(f"Built facts: expense({fact_expense.count()}), campaign_spend({fact_campaign_spend.count()})")
    
    return fact_expense, fact_campaign_spend

def write_gold_layer(gold_path, dim_vendor, dim_employee, dim_department, dim_campaign, dim_calendar, 
                    fact_expense, fact_campaign_spend):
    """Write all tables to gold layer as partitioned Parquet"""
    logger.info("Writing to gold layer...")
    
    # Write dimensions
    dim_vendor.write.mode("overwrite").parquet(f"{gold_path}/dim_vendor")
    dim_employee.write.mode("overwrite").parquet(f"{gold_path}/dim_employee")
    dim_department.write.mode("overwrite").parquet(f"{gold_path}/dim_department")
    dim_campaign.write.mode("overwrite").parquet(f"{gold_path}/dim_campaign")
    dim_calendar.write.mode("overwrite").parquet(f"{gold_path}/dim_calendar")
    
    # Write facts with partitioning
    fact_expense.write.mode("overwrite").partitionBy("budget_month_key").parquet(f"{gold_path}/fact_expense")
    fact_campaign_spend.write.mode("overwrite").partitionBy("date").parquet(f"{gold_path}/fact_campaign_spend")
    
    logger.info("Gold layer written successfully!")

def create_summary_views(spark, gold_path):
    """Create summary views for quick analysis"""
    logger.info("Creating summary views...")
    
    # Read gold tables
    fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
    fact_campaign = spark.read.parquet(f"{gold_path}/fact_campaign_spend")
    dim_department = spark.read.parquet(f"{gold_path}/dim_department")
    dim_calendar = spark.read.parquet(f"{gold_path}/dim_calendar")
    
    # Monthly expense summary
    monthly_expenses = (fact_expense
        .groupBy("budget_month_key")
        .agg(
            sum("amount").alias("total_expenses"),
            count("*").alias("expense_count"),
            avg("amount").alias("avg_expense_amount")
        )
        .orderBy("budget_month_key")
    )
    
    # Department expense summary
    dept_expenses = (fact_expense
        .join(dim_department, fact_expense.dept_id == dim_department.dept_id, "left")
        .groupBy(fact_expense["dept_id"], dim_department["dept_name"])
        .agg(
            sum("amount").alias("total_expenses"),
            count("*").alias("expense_count")
        )
        .orderBy(col("total_expenses").desc())
    )
    
    # Campaign performance summary
    campaign_performance = (fact_campaign
        .groupBy("campaign_id")
        .agg(
            sum("cost").alias("total_cost"),
            sum("clicks").alias("total_clicks"),
            sum("conversions").alias("total_conversions"),
            sum("attributed_revenue").alias("total_revenue"),
            (sum("attributed_revenue") / sum("cost")).alias("roas")
        )
        .filter(col("total_cost") > 0)
        .orderBy(col("roas").desc())
    )
    
    # Write summary views
    monthly_expenses.write.mode("overwrite").parquet(f"{gold_path}/summary_monthly_expenses")
    dept_expenses.write.mode("overwrite").parquet(f"{gold_path}/summary_dept_expenses")
    campaign_performance.write.mode("overwrite").parquet(f"{gold_path}/summary_campaign_performance")
    
    logger.info("Summary views created successfully!")

def main():
    logger.info("Starting Silver to Gold transformation...")
    
    config = load_config()
    spark = create_spark_session(config)
    
    silver_path = config['paths']['silver']
    gold_path = config['paths']['gold']
    
    try:
        # Build dimensions
        dim_vendor, dim_employee, dim_department, dim_campaign, dim_calendar = build_dimensions(spark, silver_path)
        
        # Build facts
        fact_expense, fact_campaign_spend = build_facts(
            spark, silver_path, dim_vendor, dim_employee, dim_department, dim_calendar
        )
        
        # Write to gold layer
        write_gold_layer(gold_path, dim_vendor, dim_employee, dim_department, dim_campaign, dim_calendar,
                        fact_expense, fact_campaign_spend)
        
        # Create summary views
        create_summary_views(spark, gold_path)
        
        logger.info("Silver to Gold transformation completed successfully!")
        
        # Show sample data for verification
        logger.info("Sample fact_expense data:")
        fact_expense.show(5)
        
        logger.info("Sample fact_campaign_spend data:")
        fact_campaign_spend.show(5)
        
    except Exception as e:
        logger.error(f"Error in transformation pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
