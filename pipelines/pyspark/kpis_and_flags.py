#!/usr/bin/env python3
"""
KPIs and Leakage Detection Pipeline
Computes monthly aggregates, budget vs actual variance, vendor concentration, and leakage flags
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName("BudgetLeakage-KPIsAndFlags")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

def detect_duplicate_invoices(spark, gold_path):
    """Detect duplicate invoices within vendor in last 30 days"""
    logger.info("Detecting duplicate invoices...")
    
    fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
    dim_vendor = spark.read.parquet(f"{gold_path}/dim_vendor")
    
    # Join with vendor info
    expense_vendor = fact_expense.join(dim_vendor, "vendor_id", "left")
    
    # Window function to find duplicates within 30 days
    # Also check for similar amounts (±10% tolerance)
    window_spec = Window.partitionBy("vendor_id").orderBy("trx_date").rangeBetween(-30, 30)
    
    duplicate_flags = (expense_vendor
        .withColumn("similar_amounts", 
                   count("*").over(window_spec))
        .withColumn("amount_tolerance", 
                   col("amount") * 0.1)  # 10% tolerance
        .withColumn("duplicate_score", 
                   when(col("similar_amounts") > 1, 75.0)
                   .when(col("similar_amounts") > 2, 90.0)  # Higher score for multiple duplicates
                   .otherwise(0.0))
        .filter(col("duplicate_score") > 0)
        .select(
            lit("duplicate_invoice").alias("rule_name"),
            lit("vendor").alias("entity_type"),
            col("vendor_id").cast("string").alias("entity_id"),
            col("vendor_name").alias("entity_name"),
            col("trx_date").alias("signal_date"),
            col("duplicate_score").alias("score"),
            concat(lit("Duplicate invoice detected for vendor "), 
                   col("vendor_name"), 
                   lit(" within 30 days")).alias("details")
        )
    )
    
    return duplicate_flags

def detect_sub_threshold_repeats(spark, gold_path):
    """Detect ≥3 claims < $100 by same employee in 7 days"""
    logger.info("Detecting sub-threshold repeats...")
    
    fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
    dim_employee = spark.read.parquet(f"{gold_path}/dim_employee")
    
    # Join with employee info
    expense_employee = fact_expense.join(dim_employee, "employee_id", "left")
    
    # Filter for small amounts and create window
    small_expenses = expense_employee.filter(col("amount") < 100)
    window_spec = Window.partitionBy("employee_id").orderBy("trx_date").rangeBetween(-7, 7)
    
    repeat_flags = (small_expenses
        .withColumn("small_claims_count", 
                   count("*").over(window_spec))
        .withColumn("repeat_score", 
                   when(col("small_claims_count") >= 3, 85.0).otherwise(0.0))
        .filter(col("repeat_score") > 0)
        .select(
            lit("sub_threshold_repeats").alias("rule_name"),
            lit("employee").alias("entity_type"),
            col("employee_id").cast("string").alias("entity_id"),
            col("employee_name").alias("entity_name"),
            col("trx_date").alias("signal_date"),
            col("repeat_score").alias("score"),
            concat(lit("Employee "), 
                   col("employee_name"), 
                   lit(" made "), 
                   col("small_claims_count"),
                   lit(" small claims in 7 days")).alias("details")
        )
    )
    
    return repeat_flags

def detect_weekend_holiday_claims(spark, gold_path):
    """Detect expenses on weekends or holidays"""
    logger.info("Detecting weekend/holiday claims...")
    
    fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
    dim_calendar = spark.read.parquet(f"{gold_path}/dim_calendar")
    dim_employee = spark.read.parquet(f"{gold_path}/dim_employee")
    
    # Join with calendar and employee info
    expense_calendar = (fact_expense
        .join(dim_calendar, fact_expense.trx_date == dim_calendar.date, "left")
        .join(dim_employee, "employee_id", "left")
    )
    
    weekend_holiday_flags = (expense_calendar
        .filter(col("is_weekend") | col("is_holiday"))
        .withColumn("weekend_score", 
                   when(col("is_weekend") & col("is_holiday"), 90.0)
                   .when(col("is_weekend"), 70.0)
                   .when(col("is_holiday"), 60.0)
                   .otherwise(0.0))
        .select(
            lit("weekend_holiday_claim").alias("rule_name"),
            lit("employee").alias("entity_type"),
            col("employee_id").cast("string").alias("entity_id"),
            col("employee_name").alias("entity_name"),
            col("trx_date").alias("signal_date"),
            col("weekend_score").alias("score"),
            concat(lit("Expense on "), 
                   when(col("is_weekend") & col("is_holiday"), "weekend holiday")
                   .when(col("is_weekend"), "weekend")
                   .otherwise("holiday"),
                   lit(" by "), col("employee_name")).alias("details")
        )
    )
    
    return weekend_holiday_flags

def detect_round_number_spikes(spark, gold_path):
    """Detect proportion of amounts ending in '00' by dept/month"""
    logger.info("Detecting round number spikes...")
    
    fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
    dim_department = spark.read.parquet(f"{gold_path}/dim_department")
    
    # Join with department info
    expense_dept = fact_expense.join(dim_department, "dept_id", "left")
    
    # Calculate round number proportion
    round_number_flags = (expense_dept
        .withColumn("amount_ends_00", 
                   when(col("amount") % 100 == 0, 1).otherwise(0))
        .groupBy("dept_id", "dept_name", "budget_month_key")
        .agg(
            count("*").alias("total_expenses"),
            sum("amount_ends_00").alias("round_expenses"),
            (sum("amount_ends_00") / count("*") * 100).alias("round_percentage")
        )
        .withColumn("round_score", 
                   when(col("round_percentage") > 30, 65.0)
                   .when(col("round_percentage") > 20, 45.0)
                   .otherwise(0.0))
        .filter(col("round_score") > 0)
        .select(
            lit("round_number_spikes").alias("rule_name"),
            lit("department").alias("entity_type"),
            col("dept_id").cast("string").alias("entity_id"),
            col("dept_name").alias("entity_name"),
            current_date().alias("signal_date"),
            col("round_score").alias("score"),
            concat(lit("Department "), 
                   col("dept_name"), 
                   lit(" has "), 
                   round(col("round_percentage"), 1),
                   lit("% round number expenses in "), 
                   col("budget_month_key")).alias("details")
        )
    )
    
    return round_number_flags

def detect_campaign_spend_spikes(spark, gold_path):
    """Detect campaign spend spikes without conversion lift"""
    logger.info("Detecting campaign spend spikes...")
    
    fact_campaign = spark.read.parquet(f"{gold_path}/fact_campaign_spend")
    dim_campaign = spark.read.parquet(f"{gold_path}/dim_campaign")
    
    # Join with campaign info
    campaign_info = fact_campaign.join(dim_campaign, "campaign_id", "left")
    
    # Window function to compare day-over-day
    window_spec = Window.partitionBy("campaign_id").orderBy("date")
    
    spend_spike_flags = (campaign_info
        .withColumn("prev_cost", lag("cost", 1).over(window_spec))
        .withColumn("prev_conversions", lag("conversions", 1).over(window_spec))
        .withColumn("cost_increase_pct", 
                   when(col("prev_cost") > 0, 
                        (col("cost") - col("prev_cost")) / col("prev_cost") * 100)
                   .otherwise(0))
        .withColumn("conversion_change_pct", 
                   when(col("prev_conversions") > 0, 
                        (col("conversions") - col("prev_conversions")) / col("prev_conversions") * 100)
                   .otherwise(0))
        .withColumn("spike_score", 
                   when((col("cost_increase_pct") >= 30) & (col("conversion_change_pct") <= 10), 80.0)
                   .when((col("cost_increase_pct") >= 20) & (col("conversion_change_pct") <= 5), 60.0)
                   .otherwise(0.0))
        .filter(col("spike_score") > 0)
        .select(
            lit("campaign_spend_spike").alias("rule_name"),
            lit("campaign").alias("entity_type"),
            col("campaign_id").cast("string").alias("entity_id"),
            col("campaign_name").alias("entity_name"),
            col("date").alias("signal_date"),
            col("spike_score").alias("score"),
            concat(lit("Campaign "), 
                   col("campaign_name"), 
                   lit(" spend increased "), 
                   round(col("cost_increase_pct"), 1),
                   lit("% with only "), 
                   round(col("conversion_change_pct"), 1),
                   lit("% conversion lift")).alias("details")
        )
    )
    
    return spend_spike_flags

def load_budget_data(spark, silver_path):
    """Load budget data from silver layer"""
    logger.info("Loading budget data...")
    
    try:
        budget_data = spark.read.parquet(f"{silver_path}/budgets")
        logger.info(f"Loaded {budget_data.count()} budget records")
        return budget_data
    except Exception as e:
        logger.warning(f"Could not load budget data from silver: {e}")
        logger.info("Creating synthetic budget data...")
        
        # Fallback to synthetic data
        return spark.createDataFrame([
            (1, "Engineering", "2024-01", 150000.0),
            (2, "Marketing", "2024-01", 120000.0),
            (3, "Sales", "2024-01", 100000.0),
            (4, "Finance", "2024-01", 80000.0),
            (5, "HR", "2024-01", 60000.0),
            (6, "Operations", "2024-01", 90000.0),
            (7, "Legal", "2024-01", 70000.0)
        ], ["dept_id", "dept_name", "budget_month", "budget_amount"])

def calculate_budget_variance(spark, gold_path, silver_path):
    """Calculate budget vs actual variance by department and month"""
    logger.info("Calculating budget variance...")
    
    fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
    dim_department = spark.read.parquet(f"{gold_path}/dim_department")
    
    # Load budget data
    budget_data = load_budget_data(spark, silver_path)
    
    # Calculate actual expenses by dept and month
    actual_expenses = (fact_expense
        .join(dim_department, "dept_id", "left")
        .groupBy("dept_id", "dept_name", "budget_month_key")
        .agg(sum("amount").alias("actual_amount"))
    )
    
    # Join with budget and calculate variance
    budget_variance = (actual_expenses
        .join(budget_data, 
              (actual_expenses.dept_id == budget_data.dept_id) & 
              (actual_expenses.budget_month_key == budget_data.budget_month), "left")
        .withColumn("budget_amount", coalesce(col("budget_amount"), lit(0)))
        .withColumn("variance_amount", col("actual_amount") - col("budget_amount"))
        .withColumn("variance_percentage", 
                   when(col("budget_amount") > 0, 
                        col("variance_amount") / col("budget_amount") * 100)
                   .otherwise(0))
        .select(
            col("dept_id"),
            col("dept_name"),
            col("budget_month_key"),
            col("budget_amount"),
            col("actual_amount"),
            col("variance_amount"),
            col("variance_percentage")
        )
    )
    
    return budget_variance

def calculate_vendor_concentration(spark, gold_path):
    """Calculate vendor concentration and risk scores"""
    logger.info("Calculating vendor concentration...")
    
    fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
    dim_vendor = spark.read.parquet(f"{gold_path}/dim_vendor")
    
    # Join with vendor info
    expense_vendor = fact_expense.join(dim_vendor, "vendor_id", "left")
    
    # Calculate vendor spend totals
    vendor_totals = (expense_vendor
        .groupBy("vendor_id", "vendor_name")
        .agg(
            sum("amount").alias("total_spend"),
            count("*").alias("transaction_count"),
            avg("amount").alias("avg_transaction_amount")
        )
    )
    
    # Calculate total spend for percentage
    total_spend = vendor_totals.agg(sum("total_spend").alias("grand_total")).collect()[0]["grand_total"]
    
    # Calculate concentration metrics
    vendor_concentration = (vendor_totals
        .withColumn("spend_percentage", col("total_spend") / lit(total_spend) * 100)
        .withColumn("concentration_score", 
                   when(col("spend_percentage") > 10, 90.0)
                   .when(col("spend_percentage") > 5, 70.0)
                   .when(col("spend_percentage") > 2, 50.0)
                   .otherwise(20.0))
        .orderBy(col("total_spend").desc())
    )
    
    return vendor_concentration

def persist_leakage_signals(spark, gold_path, leakage_flags):
    """Persist all leakage signals to gold layer"""
    logger.info("Persisting leakage signals...")
    
    # Combine all leakage flags
    all_signals = leakage_flags[0]  # Start with first dataframe
    for signals in leakage_flags[1:]:
        all_signals = all_signals.union(signals)
    
    # Add metadata
    final_signals = (all_signals
        .withColumn("created_timestamp", current_timestamp())
        .withColumn("signal_id", monotonically_increasing_id())
    )
    
    # Write to gold layer
    final_signals.write.mode("overwrite").parquet(f"{gold_path}/fact_leakage_signal")
    
    logger.info(f"Persisted {final_signals.count()} leakage signals")
    return final_signals

def main():
    """Main KPIs and leakage detection pipeline"""
    logger.info("Starting KPIs and Leakage Detection pipeline...")
    
    # Initialize Spark session
    spark = create_spark_session()
    
    # Configure paths
    gold_path = "s3://your-bucket/gold"  # In production
    silver_path = "s3://your-bucket/silver"  # In production
    gold_path = "data/gold"  # For local testing
    silver_path = "data/silver"  # For local testing
    
    try:
        # Detect various leakage patterns
        duplicate_flags = detect_duplicate_invoices(spark, gold_path)
        repeat_flags = detect_sub_threshold_repeats(spark, gold_path)
        weekend_flags = detect_weekend_holiday_claims(spark, gold_path)
        round_flags = detect_round_number_spikes(spark, gold_path)
        campaign_flags = detect_campaign_spend_spikes(spark, gold_path)
        
        # Persist leakage signals
        all_flags = [duplicate_flags, repeat_flags, weekend_flags, round_flags, campaign_flags]
        leakage_signals = persist_leakage_signals(spark, gold_path, all_flags)
        
        # Calculate KPIs
        budget_variance = calculate_budget_variance(spark, gold_path, silver_path)
        vendor_concentration = calculate_vendor_concentration(spark, gold_path)
        
        # Write KPI summaries
        budget_variance.write.mode("overwrite").parquet(f"{gold_path}/kpi_budget_variance")
        vendor_concentration.write.mode("overwrite").parquet(f"{gold_path}/kpi_vendor_concentration")
        
        logger.info("KPIs and Leakage Detection completed successfully!")
        
        # Show sample results
        logger.info("Sample leakage signals:")
        leakage_signals.show(5)
        
        logger.info("Sample budget variance:")
        budget_variance.show(5)
        
        logger.info("Top vendor concentration:")
        vendor_concentration.show(5)
        
    except Exception as e:
        logger.error(f"Error in KPIs and leakage detection pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
