#!/usr/bin/env python3
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import logging

sys.path.append(str(Path(__file__).parent.parent))
from config.config_loader import load_config
from data_quality.quality_checks import DataQualityChecker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    config = load_config()
    
    spark_config = config['spark']
    spark = (SparkSession.builder
             .appName("DataQualityChecks")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    
    checker = DataQualityChecker(spark)
    
    silver_path = config['paths']['silver']
    gold_path = config['paths']['gold']
    
    try:
        logger.info("Running data quality checks on Silver layer...")
        
        expenses_df = spark.read.parquet(f"{silver_path}/expenses")
        checker.check_expenses_quality(expenses_df, "silver.expenses")
        
        budgets_df = spark.read.parquet(f"{silver_path}/budgets")
        checker.check_budgets_quality(budgets_df, "silver.budgets")
        
        campaign_df = spark.read.parquet(f"{silver_path}/campaign_spend")
        checker.check_campaign_quality(campaign_df, "silver.campaign_spend")
        
        logger.info("Running data quality checks on Gold layer...")
        
        fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
        checker.check_expenses_quality(fact_expense, "gold.fact_expense")
        
        summary = checker.get_summary()
        checker.print_summary()
        
        if summary['failed'] > 0:
            logger.warning(f"Data quality checks failed: {summary['failed']} failures detected")
            return 1
        else:
            logger.info("All data quality checks passed!")
            return 0
            
    except Exception as e:
        logger.error(f"Error running data quality checks: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())

