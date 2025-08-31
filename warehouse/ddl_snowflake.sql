-- Snowflake DDL for Budget Leakage Lakehouse
-- Creates tables, stages, and loads data from gold layer

-- Create database and schema
CREATE DATABASE IF NOT EXISTS budget_leakage_lakehouse;
USE DATABASE budget_leakage_lakehouse;

CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- Create file format for Parquet files
CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';

-- Create S3 stage (replace with your bucket)
CREATE OR REPLACE STAGE gold_stage
    URL = 's3://your-bucket/gold/'
    FILE_FORMAT = parquet_format
    CREDENTIALS = (AWS_KEY_ID = 'your-key-id' AWS_SECRET_KEY = 'your-secret-key');

-- Dimension Tables
CREATE OR REPLACE TABLE dim_vendor (
    vendor_key NUMBER AUTOINCREMENT,
    vendor_id NUMBER NOT NULL,
    vendor_name VARCHAR(255),
    category VARCHAR(100),
    country VARCHAR(50),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE dim_employee (
    employee_key NUMBER AUTOINCREMENT,
    employee_id NUMBER NOT NULL,
    employee_name VARCHAR(255),
    dept_id NUMBER,
    title VARCHAR(100),
    location VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE dim_department (
    dept_key NUMBER AUTOINCREMENT,
    dept_id NUMBER NOT NULL,
    dept_name VARCHAR(100),
    owner VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE dim_campaign (
    campaign_key NUMBER AUTOINCREMENT,
    campaign_id NUMBER NOT NULL,
    campaign_name VARCHAR(255),
    channel VARCHAR(100),
    objective VARCHAR(100),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE dim_calendar (
    calendar_key NUMBER AUTOINCREMENT,
    date DATE NOT NULL,
    week NUMBER,
    month_key VARCHAR(7),
    quarter VARCHAR(2),
    year NUMBER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Fact Tables
CREATE OR REPLACE TABLE fact_expense (
    expense_id NUMBER AUTOINCREMENT,
    invoice_id VARCHAR(50) NOT NULL,
    vendor_id NUMBER,
    employee_id NUMBER,
    dept_id NUMBER,
    trx_date DATE,
    amount DECIMAL(10,2),
    currency VARCHAR(3),
    payment_method VARCHAR(50),
    budget_month_key VARCHAR(7),
    vendor_key NUMBER,
    employee_key NUMBER,
    dept_key NUMBER,
    calendar_key NUMBER,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE fact_campaign_spend (
    spend_id VARCHAR(50) NOT NULL,
    campaign_id NUMBER,
    channel VARCHAR(100),
    date DATE,
    cost DECIMAL(10,2),
    clicks NUMBER,
    impressions NUMBER,
    conversions NUMBER,
    attributed_revenue DECIMAL(10,2),
    calendar_key NUMBER,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Budget table
CREATE OR REPLACE TABLE fact_budget (
    budget_id NUMBER AUTOINCREMENT,
    dept_id NUMBER NOT NULL,
    dept_name VARCHAR(100),
    budget_month VARCHAR(7) NOT NULL,
    budget_amount DECIMAL(12,2),
    currency VARCHAR(3),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Leakage signals table
CREATE OR REPLACE TABLE fact_leakage_signal (
    signal_id NUMBER AUTOINCREMENT,
    rule_name VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50), -- 'vendor', 'employee', 'department', 'campaign'
    entity_id VARCHAR(50),
    entity_name VARCHAR(255),
    signal_date DATE,
    score DECIMAL(5,2), -- 0-100 risk score
    details VARCHAR(1000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Summary tables
CREATE OR REPLACE TABLE summary_monthly_expenses (
    budget_month_key VARCHAR(7),
    total_expenses DECIMAL(12,2),
    expense_count NUMBER,
    avg_expense_amount DECIMAL(10,2),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE summary_dept_expenses (
    dept_id NUMBER,
    dept_name VARCHAR(100),
    total_expenses DECIMAL(12,2),
    expense_count NUMBER,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE summary_campaign_performance (
    campaign_id NUMBER,
    total_cost DECIMAL(12,2),
    total_clicks NUMBER,
    total_conversions NUMBER,
    total_revenue DECIMAL(12,2),
    roas DECIMAL(10,4),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load data from S3 (example commands - adjust paths as needed)
-- COPY INTO dim_vendor FROM @gold_stage/dim_vendor/ FILE_FORMAT = parquet_format;
-- COPY INTO dim_employee FROM @gold_stage/dim_employee/ FILE_FORMAT = parquet_format;
-- COPY INTO dim_department FROM @gold_stage/dim_department/ FILE_FORMAT = parquet_format;
-- COPY INTO dim_campaign FROM @gold_stage/dim_campaign/ FILE_FORMAT = parquet_format;
-- COPY INTO dim_calendar FROM @gold_stage/dim_calendar/ FILE_FORMAT = parquet_format;

-- COPY INTO fact_expense FROM @gold_stage/fact_expense/ FILE_FORMAT = parquet_format;
-- COPY INTO fact_campaign_spend FROM @gold_stage/fact_campaign_spend/ FILE_FORMAT = parquet_format;

-- COPY INTO summary_monthly_expenses FROM @gold_stage/summary_monthly_expenses/ FILE_FORMAT = parquet_format;
-- COPY INTO summary_dept_expenses FROM @gold_stage/summary_dept_expenses/ FILE_FORMAT = parquet_format;
-- COPY INTO summary_campaign_performance FROM @gold_stage/summary_campaign_performance/ FILE_FORMAT = parquet_format;

-- Create indexes for performance
CREATE OR REPLACE INDEX idx_fact_expense_date ON fact_expense(trx_date);
CREATE OR REPLACE INDEX idx_fact_expense_dept ON fact_expense(dept_id);
CREATE OR REPLACE INDEX idx_fact_expense_vendor ON fact_expense(vendor_id);
CREATE OR REPLACE INDEX idx_fact_campaign_date ON fact_campaign_spend(date);
CREATE OR REPLACE INDEX idx_fact_campaign_id ON fact_campaign_spend(campaign_id);
CREATE OR REPLACE INDEX idx_leakage_signal_date ON fact_leakage_signal(signal_date);
CREATE OR REPLACE INDEX idx_leakage_signal_entity ON fact_leakage_signal(entity_type, entity_id);

-- Grant permissions (adjust as needed)
GRANT USAGE ON DATABASE budget_leakage_lakehouse TO ROLE analyst;
GRANT USAGE ON SCHEMA gold TO ROLE analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO ROLE analyst;
