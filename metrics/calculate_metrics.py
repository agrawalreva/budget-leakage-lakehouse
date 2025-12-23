#!/usr/bin/env python3
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max, min
import json
import logging

sys.path.append(str(Path(__file__).parent.parent))
from config.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_pipeline_metrics(spark, config):
    gold_path = config['paths']['gold']
    
    metrics = {}
    
    try:
        fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
        leakage_signals = spark.read.parquet(f"{gold_path}/fact_leakage_signal")
        budget_variance = spark.read.parquet(f"{gold_path}/kpi_budget_variance")
        vendor_concentration = spark.read.parquet(f"{gold_path}/kpi_vendor_concentration")
        
        expense_stats = fact_expense.agg(
            count("*").alias("total_transactions"),
            sum("amount").alias("total_expenses"),
            avg("amount").alias("avg_transaction_amount"),
            max("amount").alias("max_transaction_amount"),
            min("amount").alias("min_transaction_amount")
        ).collect()[0]
        
        metrics['expenses'] = {
            'total_transactions': int(expense_stats['total_transactions']),
            'total_expenses': float(expense_stats['total_expenses']),
            'avg_transaction_amount': float(expense_stats['avg_transaction_amount']),
            'max_transaction_amount': float(expense_stats['max_transaction_amount']),
            'min_transaction_amount': float(expense_stats['min_transaction_amount'])
        }
        
        leakage_stats = leakage_signals.agg(
            count("*").alias("total_signals"),
            sum("score").alias("total_risk_score"),
            avg("score").alias("avg_risk_score")
        ).collect()[0]
        
        high_risk_count = leakage_signals.filter(col("score") >= 80).count()
        medium_risk_count = leakage_signals.filter((col("score") >= 60) & (col("score") < 80)).count()
        low_risk_count = leakage_signals.filter(col("score") < 60).count()
        
        metrics['leakage_detection'] = {
            'total_signals': int(leakage_stats['total_signals']),
            'high_risk_signals': int(high_risk_count),
            'medium_risk_signals': int(medium_risk_count),
            'low_risk_signals': int(low_risk_count),
            'avg_risk_score': float(leakage_stats['avg_risk_score']) if leakage_stats['avg_risk_score'] else 0.0
        }
        
        leakage_by_rule = leakage_signals.groupBy("rule_name").count().collect()
        metrics['leakage_by_rule'] = {row['rule_name']: int(row['count']) for row in leakage_by_rule}
        
        budget_stats = budget_variance.agg(
            sum("budget_amount").alias("total_budget"),
            sum("actual_amount").alias("total_actual"),
            sum("variance_amount").alias("total_variance"),
            avg("variance_percentage").alias("avg_variance_pct")
        ).collect()[0]
        
        over_budget_count = budget_variance.filter(col("variance_percentage") > 10).count()
        under_budget_count = budget_variance.filter(col("variance_percentage") < -10).count()
        
        metrics['budget_analysis'] = {
            'total_budget': float(budget_stats['total_budget']) if budget_stats['total_budget'] else 0.0,
            'total_actual': float(budget_stats['total_actual']) if budget_stats['total_actual'] else 0.0,
            'total_variance': float(budget_stats['total_variance']) if budget_stats['total_variance'] else 0.0,
            'avg_variance_pct': float(budget_stats['avg_variance_pct']) if budget_stats['avg_variance_pct'] else 0.0,
            'over_budget_departments': int(over_budget_count),
            'under_budget_departments': int(under_budget_count)
        }
        
        vendor_stats = vendor_concentration.agg(
            count("*").alias("total_vendors"),
            sum("total_spend").alias("total_vendor_spend"),
            avg("spend_percentage").alias("avg_spend_percentage"),
            max("spend_percentage").alias("max_spend_percentage")
        ).collect()[0]
        
        high_risk_vendors = vendor_concentration.filter(col("concentration_score") >= 70).count()
        
        metrics['vendor_analysis'] = {
            'total_vendors': int(vendor_stats['total_vendors']),
            'total_vendor_spend': float(vendor_stats['total_vendor_spend']) if vendor_stats['total_vendor_spend'] else 0.0,
            'avg_spend_percentage': float(vendor_stats['avg_spend_percentage']) if vendor_stats['avg_spend_percentage'] else 0.0,
            'max_spend_percentage': float(vendor_stats['max_spend_percentage']) if vendor_stats['max_spend_percentage'] else 0.0,
            'high_risk_vendors': int(high_risk_vendors)
        }
        
        top_vendors = vendor_concentration.orderBy(col("total_spend").desc()).limit(5).collect()
        metrics['top_vendors'] = [
            {
                'vendor_name': row['vendor_name'],
                'total_spend': float(row['total_spend']),
                'spend_percentage': float(row['spend_percentage']),
                'concentration_score': float(row['concentration_score'])
            }
            for row in top_vendors
        ]
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating metrics: {str(e)}")
        raise

def estimate_potential_leakage(metrics):
    total_expenses = metrics['expenses']['total_expenses']
    high_risk_signals = metrics['leakage_detection']['high_risk_signals']
    
    if high_risk_signals > 0:
        avg_transaction = metrics['expenses']['avg_transaction_amount']
        estimated_leakage = high_risk_signals * avg_transaction * 0.5
    else:
        estimated_leakage = 0.0
    
    return {
        'estimated_potential_leakage': estimated_leakage,
        'leakage_percentage': (estimated_leakage / total_expenses * 100) if total_expenses > 0 else 0.0
    }

def print_metrics_report(metrics, leakage_estimate):
    print("\n" + "="*60)
    print("BUDGET LEAKAGE DETECTION - METRICS REPORT")
    print("="*60)
    
    print("\nEXPENSE STATISTICS")
    print(f"  Total Transactions: {metrics['expenses']['total_transactions']:,}")
    print(f"  Total Expenses: ${metrics['expenses']['total_expenses']:,.2f}")
    print(f"  Average Transaction: ${metrics['expenses']['avg_transaction_amount']:,.2f}")
    print(f"  Max Transaction: ${metrics['expenses']['max_transaction_amount']:,.2f}")
    print(f"  Min Transaction: ${metrics['expenses']['min_transaction_amount']:,.2f}")
    
    print("\nLEAKAGE DETECTION")
    print(f"  Total Signals: {metrics['leakage_detection']['total_signals']}")
    print(f"  High Risk (80+): {metrics['leakage_detection']['high_risk_signals']}")
    print(f"  Medium Risk (60-79): {metrics['leakage_detection']['medium_risk_signals']}")
    print(f"  Low Risk (<60): {metrics['leakage_detection']['low_risk_signals']}")
    print(f"  Average Risk Score: {metrics['leakage_detection']['avg_risk_score']:.2f}")
    
    if metrics['leakage_by_rule']:
        print("\n  Signals by Rule:")
        for rule, count in metrics['leakage_by_rule'].items():
            print(f"    - {rule}: {count}")
    
    print("\nBUDGET ANALYSIS")
    print(f"  Total Budget: ${metrics['budget_analysis']['total_budget']:,.2f}")
    print(f"  Total Actual: ${metrics['budget_analysis']['total_actual']:,.2f}")
    print(f"  Total Variance: ${metrics['budget_analysis']['total_variance']:,.2f}")
    print(f"  Average Variance %: {metrics['budget_analysis']['avg_variance_pct']:.2f}%")
    print(f"  Over Budget Departments: {metrics['budget_analysis']['over_budget_departments']}")
    print(f"  Under Budget Departments: {metrics['budget_analysis']['under_budget_departments']}")
    
    print("\nVENDOR ANALYSIS")
    print(f"  Total Vendors: {metrics['vendor_analysis']['total_vendors']}")
    print(f"  Total Vendor Spend: ${metrics['vendor_analysis']['total_vendor_spend']:,.2f}")
    print(f"  Average Spend %: {metrics['vendor_analysis']['avg_spend_percentage']:.2f}%")
    print(f"  Max Spend %: {metrics['vendor_analysis']['max_spend_percentage']:.2f}%")
    print(f"  High Risk Vendors: {metrics['vendor_analysis']['high_risk_vendors']}")
    
    if metrics['top_vendors']:
        print("\n  Top 5 Vendors by Spend:")
        for i, vendor in enumerate(metrics['top_vendors'], 1):
            print(f"    {i}. {vendor['vendor_name']}: ${vendor['total_spend']:,.2f} ({vendor['spend_percentage']:.2f}%)")
    
    print("\nPOTENTIAL LEAKAGE ESTIMATE")
    print(f"  Estimated Potential Leakage: ${leakage_estimate['estimated_potential_leakage']:,.2f}")
    print(f"  Leakage as % of Total Expenses: {leakage_estimate['leakage_percentage']:.2f}%")
    
    print("\n" + "="*60)

def main():
    config = load_config()
    
    spark_config = config['spark']
    spark = (SparkSession.builder
             .appName("MetricsCalculation")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    
    try:
        logger.info("Calculating pipeline metrics...")
        metrics = calculate_pipeline_metrics(spark, config)
        
        leakage_estimate = estimate_potential_leakage(metrics)
        
        print_metrics_report(metrics, leakage_estimate)
        
        output_file = Path(__file__).parent / "metrics_report.json"
        report = {
            'metrics': metrics,
            'leakage_estimate': leakage_estimate
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Metrics report saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error calculating metrics: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

