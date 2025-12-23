import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.append(str(Path(__file__).parent.parent))
from config.config_loader import load_config

st.set_page_config(page_title="Budget Leakage Dashboard", layout="wide")

@st.cache_data
def load_config_cached():
    return load_config()

@st.cache_data
def load_gold_data(config):
    from pyspark.sql import SparkSession
    
    spark_config = config['spark']
    spark = (SparkSession.builder
             .appName("Dashboard")
             .master("local[*]")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    
    gold_path = config['paths']['gold']
    
    try:
        fact_expense = spark.read.parquet(f"{gold_path}/fact_expense")
        fact_campaign = spark.read.parquet(f"{gold_path}/fact_campaign_spend")
        leakage_signals = spark.read.parquet(f"{gold_path}/fact_leakage_signal")
        budget_variance = spark.read.parquet(f"{gold_path}/kpi_budget_variance")
        vendor_concentration = spark.read.parquet(f"{gold_path}/kpi_vendor_concentration")
        
        return {
            'expenses': fact_expense.toPandas(),
            'campaigns': fact_campaign.toPandas(),
            'leakage': leakage_signals.toPandas(),
            'budget_variance': budget_variance.toPandas(),
            'vendor_concentration': vendor_concentration.toPandas()
        }
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None
    finally:
        spark.stop()

config = load_config_cached()

st.title("Budget Leakage Detection Dashboard")

if st.button("Refresh Data"):
    st.cache_data.clear()

data = load_gold_data(config)

if data is None:
    st.warning("No data available. Please run the pipeline first.")
    st.stop()

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Leakage Signals", "Budget Analysis", "Vendor Risk"])

with tab1:
    st.header("Executive Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_expenses = data['expenses']['amount'].sum()
    total_leakage_signals = len(data['leakage'])
    high_risk_signals = len(data['leakage'][data['leakage']['score'] >= 80])
    total_vendors = data['vendor_concentration'].shape[0]
    
    with col1:
        st.metric("Total Expenses", f"${total_expenses:,.2f}")
    with col2:
        st.metric("Leakage Signals", total_leakage_signals)
    with col3:
        st.metric("High Risk Signals", high_risk_signals)
    with col4:
        st.metric("Active Vendors", total_vendors)
    
    st.subheader("Expense Trends")
    if not data['expenses'].empty:
        expenses_by_month = data['expenses'].groupby('budget_month_key')['amount'].sum().reset_index()
        expenses_by_month.columns = ['Month', 'Total Expenses']
        expenses_by_month = expenses_by_month.sort_values('Month')
        
        fig = px.line(expenses_by_month, x='Month', y='Total Expenses', 
                     title='Monthly Expense Trends',
                     markers=True)
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Leakage Signals by Type")
    if not data['leakage'].empty:
        leakage_by_type = data['leakage']['rule_name'].value_counts().reset_index()
        leakage_by_type.columns = ['Rule Name', 'Count']
        
        fig = px.bar(leakage_by_type, x='Rule Name', y='Count',
                    title='Leakage Signals by Detection Rule')
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header("Leakage Detection Signals")
    
    if data['leakage'].empty:
        st.info("No leakage signals detected.")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            risk_filter = st.selectbox("Filter by Risk Level", 
                                     ["All", "Critical (80+)", "High (60-79)", "Medium (40-59)", "Low (<40)"])
        
        with col2:
            rule_filter = st.selectbox("Filter by Rule", 
                                     ["All"] + list(data['leakage']['rule_name'].unique()))
        
        filtered_leakage = data['leakage'].copy()
        
        if risk_filter != "All":
            if risk_filter == "Critical (80+)":
                filtered_leakage = filtered_leakage[filtered_leakage['score'] >= 80]
            elif risk_filter == "High (60-79)":
                filtered_leakage = filtered_leakage[(filtered_leakage['score'] >= 60) & (filtered_leakage['score'] < 80)]
            elif risk_filter == "Medium (40-59)":
                filtered_leakage = filtered_leakage[(filtered_leakage['score'] >= 40) & (filtered_leakage['score'] < 60)]
            else:
                filtered_leakage = filtered_leakage[filtered_leakage['score'] < 40]
        
        if rule_filter != "All":
            filtered_leakage = filtered_leakage[filtered_leakage['rule_name'] == rule_filter]
        
        st.dataframe(filtered_leakage[['rule_name', 'entity_type', 'entity_name', 'signal_date', 'score', 'details']], 
                    use_container_width=True)
        
        st.subheader("Risk Score Distribution")
        fig = px.histogram(filtered_leakage, x='score', nbins=20,
                          title='Distribution of Risk Scores')
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Signals Over Time")
        if 'signal_date' in filtered_leakage.columns:
            signals_by_date = filtered_leakage.groupby('signal_date').size().reset_index()
            signals_by_date.columns = ['Date', 'Count']
            signals_by_date = signals_by_date.sort_values('Date')
            
            fig = px.line(signals_by_date, x='Date', y='Count',
                         title='Leakage Signals Over Time')
            st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header("Budget vs Actual Analysis")
    
    if data['budget_variance'].empty:
        st.info("No budget variance data available.")
    else:
        st.subheader("Budget Variance by Department")
        
        budget_df = data['budget_variance'].copy()
        budget_df['variance_pct'] = budget_df['variance_percentage']
        
        fig = px.bar(budget_df, x='dept_name', y='variance_pct',
                    title='Budget Variance Percentage by Department',
                    labels={'variance_pct': 'Variance %', 'dept_name': 'Department'})
        fig.add_hline(y=10, line_dash="dash", line_color="red", annotation_text="Over Budget Threshold")
        fig.add_hline(y=-10, line_dash="dash", line_color="green", annotation_text="Under Budget Threshold")
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Budget Variance Details")
        st.dataframe(budget_df[['dept_name', 'budget_month_key', 'budget_amount', 
                                'actual_amount', 'variance_amount', 'variance_percentage']],
                    use_container_width=True)
        
        st.subheader("Monthly Budget Performance")
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
        fig.update_layout(title='Monthly Budget vs Actual', barmode='group')
        st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header("Vendor Concentration Risk")
    
    if data['vendor_concentration'].empty:
        st.info("No vendor concentration data available.")
    else:
        vendor_df = data['vendor_concentration'].copy()
        
        st.subheader("Top Vendors by Spend")
        top_vendors = vendor_df.nlargest(10, 'total_spend')
        
        fig = px.bar(top_vendors, x='vendor_name', y='total_spend',
                    title='Top 10 Vendors by Total Spend',
                    labels={'total_spend': 'Total Spend ($)', 'vendor_name': 'Vendor'})
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Vendor Concentration Risk")
        risk_counts = vendor_df['concentration_score'].apply(
            lambda x: 'High' if x >= 70 else 'Medium' if x >= 50 else 'Low'
        ).value_counts()
        
        fig = px.pie(values=risk_counts.values, names=risk_counts.index,
                    title='Vendor Risk Distribution')
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Vendor Concentration Details")
        st.dataframe(vendor_df[['vendor_name', 'total_spend', 'transaction_count', 
                               'spend_percentage', 'concentration_score']].sort_values('total_spend', ascending=False),
                    use_container_width=True)

