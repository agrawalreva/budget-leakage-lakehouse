#!/usr/bin/env python3
"""
Generate all dashboard charts and save as images for analysis
"""

import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

sys.path.append(str(Path(__file__).parent.parent))
from config.config_loader import load_config

def load_data(config):
    from pyspark.sql import SparkSession
    
    spark_config = config['spark']
    spark = (SparkSession.builder
             .appName("ChartGeneration")
             .master("local[*]")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
             .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
             .getOrCreate())
    
    gold_path = config['paths']['gold']
    
    try:
        fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
        fact_campaign = spark.read.parquet(f"{gold_path}/fact_campaign_spend")
        leakage_signals = spark.read.parquet(f"{gold_path}/fact_leakage_signal")
        budget_variance = spark.read.parquet(f"{gold_path}/kpi_budget_variance")
        vendor_concentration = spark.read.parquet(f"{gold_path}/kpi_vendor_concentration")
        
        expenses_df = fact_expense.toPandas()
        campaigns_df = fact_campaign.toPandas()
        leakage_df = leakage_signals.toPandas()
        budget_df = budget_variance.toPandas()
        vendor_df = vendor_concentration.toPandas()
        
        if not expenses_df.empty:
            expenses_df['amount'] = pd.to_numeric(expenses_df['amount'], errors='coerce')
        
        if not leakage_df.empty:
            leakage_df['score'] = pd.to_numeric(leakage_df['score'], errors='coerce')
        
        if not budget_df.empty:
            budget_df['budget_amount'] = pd.to_numeric(budget_df['budget_amount'], errors='coerce')
            budget_df['actual_amount'] = pd.to_numeric(budget_df['actual_amount'], errors='coerce')
            budget_df['variance_amount'] = pd.to_numeric(budget_df['variance_amount'], errors='coerce')
            budget_df['variance_percentage'] = pd.to_numeric(budget_df['variance_percentage'], errors='coerce')
        
        if not vendor_df.empty:
            vendor_df['total_spend'] = pd.to_numeric(vendor_df['total_spend'], errors='coerce')
            vendor_df['transaction_count'] = pd.to_numeric(vendor_df['transaction_count'], errors='coerce')
            vendor_df['spend_percentage'] = pd.to_numeric(vendor_df['spend_percentage'], errors='coerce')
            vendor_df['concentration_score'] = pd.to_numeric(vendor_df['concentration_score'], errors='coerce')
        
        return {
            'expenses': expenses_df,
            'campaigns': campaigns_df,
            'leakage': leakage_df,
            'budget_variance': budget_df,
            'vendor_concentration': vendor_df
        }
    finally:
        spark.stop()

def generate_chart_1_monthly_expenses(data, output_dir):
    if data['expenses'].empty:
        return None, "No expense data available"
    
    expenses_by_month = data['expenses'].groupby('budget_month_key')['amount'].sum().reset_index()
    expenses_by_month.columns = ['Month', 'Total Expenses']
    expenses_by_month = expenses_by_month.sort_values('Month')
    
    fig = px.line(expenses_by_month, x='Month', y='Total Expenses', 
                 title='Monthly Expense Trends',
                 markers=True)
    fig.update_layout(width=1200, height=600)
    
    filepath = output_dir / "chart_1_monthly_expenses.html"
    fig.write_html(str(filepath))
    
    analysis = {
        'chart_name': 'Monthly Expense Trends',
        'type': 'Line Chart',
        'description': 'Shows total expenses by month across all departments',
        'insights': [
            f"Total months analyzed: {len(expenses_by_month)}",
            f"Average monthly expenses: ${expenses_by_month['Total Expenses'].mean():,.2f}",
            f"Highest month: {expenses_by_month.loc[expenses_by_month['Total Expenses'].idxmax(), 'Month']} (${expenses_by_month['Total Expenses'].max():,.2f})",
            f"Lowest month: {expenses_by_month.loc[expenses_by_month['Total Expenses'].idxmin(), 'Month']} (${expenses_by_month['Total Expenses'].min():,.2f})",
            f"Month-over-month volatility: {expenses_by_month['Total Expenses'].std():,.2f}"
        ]
    }
    
    return filepath, analysis

def generate_chart_2_leakage_by_type(data, output_dir):
    if data['leakage'].empty:
        return None, "No leakage signals available"
    
    leakage_by_type = data['leakage']['rule_name'].value_counts().reset_index()
    leakage_by_type.columns = ['Rule Name', 'Count']
    
    fig = px.bar(leakage_by_type, x='Rule Name', y='Count',
                title='Leakage Signals by Detection Rule',
                labels={'Count': 'Number of Signals', 'Rule Name': 'Detection Rule'})
    fig.update_layout(width=1200, height=600)
    
    filepath = output_dir / "chart_2_leakage_by_type.html"
    fig.write_html(str(filepath))
    
    total_signals = leakage_by_type['Count'].sum()
    analysis = {
        'chart_name': 'Leakage Signals by Detection Rule',
        'type': 'Bar Chart',
        'description': 'Distribution of leakage signals across different detection algorithms',
        'insights': [
            f"Total signals detected: {total_signals}",
            f"Most common rule: {leakage_by_type.iloc[0]['Rule Name']} ({leakage_by_type.iloc[0]['Count']} signals, {leakage_by_type.iloc[0]['Count']/total_signals*100:.1f}%)",
            f"Number of active detection rules: {len(leakage_by_type)}",
            f"Average signals per rule: {leakage_by_type['Count'].mean():.1f}"
        ],
        'rule_breakdown': leakage_by_type.to_dict('records')
    }
    
    return filepath, analysis

def generate_chart_3_risk_distribution(data, output_dir):
    if data['leakage'].empty:
        return None, "No leakage signals available"
    
    fig = px.histogram(data['leakage'], x='score', nbins=20,
                      title='Distribution of Risk Scores',
                      labels={'score': 'Risk Score', 'count': 'Number of Signals'})
    fig.update_layout(width=1200, height=600)
    
    filepath = output_dir / "chart_3_risk_distribution.html"
    fig.write_html(str(filepath))
    
    scores = data['leakage']['score']
    analysis = {
        'chart_name': 'Risk Score Distribution',
        'type': 'Histogram',
        'description': 'Distribution of risk scores across all leakage signals',
        'insights': [
            f"Mean risk score: {scores.mean():.2f}",
            f"Median risk score: {scores.median():.2f}",
            f"Standard deviation: {scores.std():.2f}",
            f"High risk signals (80+): {len(scores[scores >= 80])} ({len(scores[scores >= 80])/len(scores)*100:.1f}%)",
            f"Medium risk signals (60-79): {len(scores[(scores >= 60) & (scores < 80)])} ({len(scores[(scores >= 60) & (scores < 80)])/len(scores)*100:.1f}%)",
            f"Low risk signals (<60): {len(scores[scores < 60])} ({len(scores[scores < 60])/len(scores)*100:.1f}%)"
        ]
    }
    
    return filepath, analysis

def generate_chart_4_signals_over_time(data, output_dir):
    if data['leakage'].empty or 'signal_date' not in data['leakage'].columns:
        return None, "No time-series leakage data available"
    
    signals_by_date = data['leakage'].groupby('signal_date').size().reset_index()
    signals_by_date.columns = ['Date', 'Count']
    signals_by_date = signals_by_date.sort_values('Date')
    
    fig = px.line(signals_by_date, x='Date', y='Count',
                 title='Leakage Signals Over Time',
                 markers=True)
    fig.update_layout(width=1200, height=600)
    
    filepath = output_dir / "chart_4_signals_over_time.html"
    fig.write_html(str(filepath))
    
    analysis = {
        'chart_name': 'Leakage Signals Over Time',
        'type': 'Time Series Line Chart',
        'description': 'Trend of leakage signals detected over time',
        'insights': [
            f"Date range: {signals_by_date['Date'].min()} to {signals_by_date['Date'].max()}",
            f"Average signals per day: {signals_by_date['Count'].mean():.2f}",
            f"Peak day: {signals_by_date.loc[signals_by_date['Count'].idxmax(), 'Date']} ({signals_by_date['Count'].max()} signals)",
            f"Days with signals: {len(signals_by_date)}",
            f"Trend: {'Increasing' if signals_by_date['Count'].iloc[-1] > signals_by_date['Count'].iloc[0] else 'Decreasing' if signals_by_date['Count'].iloc[-1] < signals_by_date['Count'].iloc[0] else 'Stable'}"
        ]
    }
    
    return filepath, analysis

def generate_chart_5_budget_variance(data, output_dir):
    if data['budget_variance'].empty:
        return None, "No budget variance data available"
    
    budget_df = data['budget_variance'].copy()
    budget_df['variance_pct'] = budget_df['variance_percentage']
    
    fig = px.bar(budget_df, x='dept_name', y='variance_pct',
                title='Budget Variance Percentage by Department',
                labels={'variance_pct': 'Variance %', 'dept_name': 'Department'})
    fig.add_hline(y=10, line_dash="dash", line_color="red", annotation_text="Over Budget Threshold")
    fig.add_hline(y=-10, line_dash="dash", line_color="green", annotation_text="Under Budget Threshold")
    fig.update_layout(width=1200, height=600)
    
    filepath = output_dir / "chart_5_budget_variance.html"
    fig.write_html(str(filepath))
    
    over_budget = len(budget_df[budget_df['variance_percentage'] > 10])
    under_budget = len(budget_df[budget_df['variance_percentage'] < -10])
    on_target = len(budget_df[(budget_df['variance_percentage'] >= -10) & (budget_df['variance_percentage'] <= 10)])
    
    analysis = {
        'chart_name': 'Budget Variance by Department',
        'type': 'Bar Chart',
        'description': 'Budget vs actual variance percentage by department',
        'insights': [
            f"Total department-month combinations: {len(budget_df)}",
            f"Over budget (>10%): {over_budget} instances ({over_budget/len(budget_df)*100:.1f}%)",
            f"Under budget (<-10%): {under_budget} instances ({under_budget/len(budget_df)*100:.1f}%)",
            f"On target: {on_target} instances ({on_target/len(budget_df)*100:.1f}%)",
            f"Average variance: {budget_df['variance_percentage'].mean():.2f}%",
            f"Highest variance: {budget_df.loc[budget_df['variance_percentage'].idxmax(), 'dept_name']} ({budget_df['variance_percentage'].max():.2f}%)"
        ]
    }
    
    return filepath, analysis

def generate_chart_6_monthly_budget_performance(data, output_dir):
    if data['budget_variance'].empty:
        return None, "No budget variance data available"
    
    budget_df = data['budget_variance'].copy()
    monthly_budget = budget_df.groupby('budget_month_key').agg({
        'budget_amount': 'sum',
        'actual_amount': 'sum'
    }).reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=monthly_budget['budget_month_key'], 
                        y=monthly_budget['budget_amount'],
                        name='Budget', marker_color='blue'))
    fig.add_trace(go.Bar(x=monthly_budget['budget_month_key'], 
                        y=monthly_budget['actual_amount'],
                        name='Actual', marker_color='orange'))
    fig.update_layout(title='Monthly Budget vs Actual', barmode='group', width=1200, height=600)
    
    filepath = output_dir / "chart_6_monthly_budget_performance.html"
    fig.write_html(str(filepath))
    
    total_budget = monthly_budget['budget_amount'].sum()
    total_actual = monthly_budget['actual_amount'].sum()
    overall_variance = ((total_actual - total_budget) / total_budget * 100) if total_budget > 0 else 0
    
    analysis = {
        'chart_name': 'Monthly Budget vs Actual Performance',
        'type': 'Grouped Bar Chart',
        'description': 'Comparison of budgeted vs actual spending by month',
        'insights': [
            f"Total budget: ${total_budget:,.2f}",
            f"Total actual: ${total_actual:,.2f}",
            f"Overall variance: {overall_variance:.2f}%",
            f"Average monthly budget: ${monthly_budget['budget_amount'].mean():,.2f}",
            f"Average monthly actual: ${monthly_budget['actual_amount'].mean():,.2f}",
            f"Months analyzed: {len(monthly_budget)}"
        ]
    }
    
    return filepath, analysis

def generate_chart_7_top_vendors(data, output_dir):
    if data['vendor_concentration'].empty:
        return None, "No vendor concentration data available"
    
    vendor_df = data['vendor_concentration'].copy()
    top_vendors = vendor_df.nlargest(10, 'total_spend')
    
    fig = px.bar(top_vendors, x='vendor_name', y='total_spend',
                title='Top 10 Vendors by Total Spend',
                labels={'total_spend': 'Total Spend ($)', 'vendor_name': 'Vendor'})
    fig.update_layout(width=1200, height=600)
    
    filepath = output_dir / "chart_7_top_vendors.html"
    fig.write_html(str(filepath))
    
    top_vendor = top_vendors.iloc[0]
    total_spend_all = vendor_df['total_spend'].sum()
    top_10_percentage = top_vendors['total_spend'].sum() / total_spend_all * 100 if total_spend_all > 0 else 0
    
    analysis = {
        'chart_name': 'Top 10 Vendors by Spend',
        'type': 'Bar Chart',
        'description': 'Top 10 vendors ranked by total spending',
        'insights': [
            f"Top vendor: {top_vendor['vendor_name']} (${top_vendor['total_spend']:,.2f}, {top_vendor['spend_percentage']:.2f}%)",
            f"Top 10 vendors account for {top_10_percentage:.1f}% of total spend",
            f"Total vendors: {len(vendor_df)}",
            f"Total spend across all vendors: ${total_spend_all:,.2f}",
            f"Average vendor spend: ${vendor_df['total_spend'].mean():,.2f}"
        ],
        'top_vendors': top_vendors[['vendor_name', 'total_spend', 'spend_percentage']].to_dict('records')
    }
    
    return filepath, analysis

def generate_chart_8_vendor_risk_distribution(data, output_dir):
    if data['vendor_concentration'].empty:
        return None, "No vendor concentration data available"
    
    vendor_df = data['vendor_concentration'].copy()
    risk_counts = vendor_df['concentration_score'].apply(
        lambda x: 'High' if x >= 70 else 'Medium' if x >= 50 else 'Low'
    ).value_counts()
    
    fig = px.pie(values=risk_counts.values, names=risk_counts.index,
                title='Vendor Risk Distribution')
    fig.update_layout(width=1200, height=600)
    
    filepath = output_dir / "chart_8_vendor_risk_distribution.html"
    fig.write_html(str(filepath))
    
    analysis = {
        'chart_name': 'Vendor Risk Distribution',
        'type': 'Pie Chart',
        'description': 'Distribution of vendors by concentration risk level',
        'insights': [
            f"High risk vendors: {risk_counts.get('High', 0)} ({risk_counts.get('High', 0)/len(vendor_df)*100:.1f}%)",
            f"Medium risk vendors: {risk_counts.get('Medium', 0)} ({risk_counts.get('Medium', 0)/len(vendor_df)*100:.1f}%)",
            f"Low risk vendors: {risk_counts.get('Low', 0)} ({risk_counts.get('Low', 0)/len(vendor_df)*100:.1f}%)",
            f"Total vendors: {len(vendor_df)}"
        ]
    }
    
    return filepath, analysis

def main():
    config = load_config()
    output_dir = Path(__file__).parent / "charts"
    output_dir.mkdir(exist_ok=True)
    
    print("Loading data from gold layer...")
    data = load_data(config)
    
    print("Generating charts...")
    charts = []
    
    chart_funcs = [
        ("Monthly Expense Trends", generate_chart_1_monthly_expenses),
        ("Leakage Signals by Type", generate_chart_2_leakage_by_type),
        ("Risk Score Distribution", generate_chart_3_risk_distribution),
        ("Signals Over Time", generate_chart_4_signals_over_time),
        ("Budget Variance by Department", generate_chart_5_budget_variance),
        ("Monthly Budget Performance", generate_chart_6_monthly_budget_performance),
        ("Top Vendors by Spend", generate_chart_7_top_vendors),
        ("Vendor Risk Distribution", generate_chart_8_vendor_risk_distribution),
    ]
    
    for chart_name, chart_func in chart_funcs:
        try:
            filepath, analysis = chart_func(data, output_dir)
            if filepath:
                charts.append({
                    'name': chart_name,
                    'file': str(filepath.name),
                    'analysis': analysis
                })
                print(f"  ✓ Generated: {chart_name}")
            else:
                print(f"  ✗ Skipped: {chart_name} - {analysis}")
        except Exception as e:
            print(f"  ✗ Error generating {chart_name}: {str(e)}")
    
    analysis_doc = {
        'summary': {
            'total_charts': len(charts),
            'generation_date': pd.Timestamp.now().isoformat(),
            'data_sources': {
                'expenses_count': len(data['expenses']),
                'leakage_signals_count': len(data['leakage']),
                'budget_records_count': len(data['budget_variance']),
                'vendors_count': len(data['vendor_concentration'])
            }
        },
        'charts': charts
    }
    
    analysis_file = output_dir / "dashboard_analysis.json"
    with open(analysis_file, 'w') as f:
        json.dump(analysis_doc, f, indent=2)
    
    print(f"\nGenerated {len(charts)} charts in {output_dir}")
    print(f"Analysis saved to {analysis_file}")
    
    return charts, analysis_doc

if __name__ == "__main__":
    main()

