-- BI Views for Budget Leakage Lakehouse
-- Views for Tableau/Power BI consumption

USE DATABASE budget_leakage_lakehouse;
USE SCHEMA gold;

-- View 1: Budget vs Actual Variance
CREATE OR REPLACE VIEW vw_budget_vs_actual AS
SELECT 
    d.dept_name,
    d.dept_id,
    b.budget_month,
    b.budget_amount,
    COALESCE(SUM(e.amount), 0) as actual_amount,
    (COALESCE(SUM(e.amount), 0) - b.budget_amount) as variance_amount,
    CASE 
        WHEN b.budget_amount > 0 
        THEN ((COALESCE(SUM(e.amount), 0) - b.budget_amount) / b.budget_amount) * 100
        ELSE 0 
    END as variance_percentage,
    CASE 
        WHEN variance_percentage > 10 THEN 'Over Budget'
        WHEN variance_percentage < -10 THEN 'Under Budget'
        ELSE 'On Target'
    END as budget_status
FROM fact_budget b
LEFT JOIN fact_expense e ON b.dept_id = e.dept_id 
    AND b.budget_month = e.budget_month_key
LEFT JOIN dim_department d ON b.dept_id = d.dept_id
GROUP BY d.dept_name, d.dept_id, b.budget_month, b.budget_amount
ORDER BY d.dept_name, b.budget_month;

-- View 2: Vendor Concentration Analysis
CREATE OR REPLACE VIEW vw_vendor_concentration AS
SELECT 
    v.vendor_name,
    v.vendor_id,
    v.category,
    COUNT(e.invoice_id) as transaction_count,
    SUM(e.amount) as total_spend,
    AVG(e.amount) as avg_transaction_amount,
    (SUM(e.amount) / (SELECT SUM(amount) FROM fact_expense)) * 100 as spend_percentage,
    CASE 
        WHEN spend_percentage > 10 THEN 'High Risk'
        WHEN spend_percentage > 5 THEN 'Medium Risk'
        WHEN spend_percentage > 2 THEN 'Low Risk'
        ELSE 'Minimal Risk'
    END as concentration_risk,
    ROW_NUMBER() OVER (ORDER BY total_spend DESC) as vendor_rank
FROM fact_expense e
JOIN dim_vendor v ON e.vendor_id = v.vendor_id
GROUP BY v.vendor_name, v.vendor_id, v.category
ORDER BY total_spend DESC;

-- View 3: Leakage Flags Summary
CREATE OR REPLACE VIEW vw_leakage_flags AS
SELECT 
    rule_name,
    entity_type,
    entity_name,
    signal_date,
    score,
    details,
    CASE 
        WHEN score >= 80 THEN 'Critical'
        WHEN score >= 60 THEN 'High'
        WHEN score >= 40 THEN 'Medium'
        ELSE 'Low'
    END as risk_level,
    COUNT(*) OVER (PARTITION BY rule_name) as rule_count,
    COUNT(*) OVER (PARTITION BY entity_type) as entity_type_count
FROM fact_leakage_signal
WHERE signal_date >= DATEADD(month, -3, CURRENT_DATE())  -- Last 3 months
ORDER BY score DESC, signal_date DESC;

-- View 4: Campaign ROI Analysis
CREATE OR REPLACE VIEW vw_campaign_roi AS
SELECT 
    c.campaign_name,
    c.campaign_id,
    c.channel,
    c.objective,
    COUNT(s.spend_id) as spend_days,
    SUM(s.cost) as total_cost,
    SUM(s.clicks) as total_clicks,
    SUM(s.impressions) as total_impressions,
    SUM(s.conversions) as total_conversions,
    SUM(s.attributed_revenue) as total_revenue,
    CASE 
        WHEN SUM(s.cost) > 0 
        THEN SUM(s.attributed_revenue) / SUM(s.cost)
        ELSE 0 
    END as roas,
    CASE 
        WHEN SUM(s.impressions) > 0 
        THEN SUM(s.clicks) / SUM(s.impressions) * 100
        ELSE 0 
    END as ctr_percentage,
    CASE 
        WHEN SUM(s.clicks) > 0 
        THEN SUM(s.conversions) / SUM(s.clicks) * 100
        ELSE 0 
    END as conversion_rate,
    CASE 
        WHEN roas > 3 THEN 'Excellent'
        WHEN roas > 2 THEN 'Good'
        WHEN roas > 1 THEN 'Break-even'
        ELSE 'Poor'
    END as roi_category
FROM fact_campaign_spend s
JOIN dim_campaign c ON s.campaign_id = c.campaign_id
GROUP BY c.campaign_name, c.campaign_id, c.channel, c.objective
ORDER BY roas DESC;

-- View 5: Monthly Expense Trends
CREATE OR REPLACE VIEW vw_monthly_expense_trends AS
SELECT 
    budget_month_key,
    dept_name,
    SUM(amount) as monthly_expenses,
    COUNT(*) as transaction_count,
    AVG(amount) as avg_transaction_amount,
    LAG(SUM(amount)) OVER (PARTITION BY dept_name ORDER BY budget_month_key) as prev_month_expenses,
    CASE 
        WHEN LAG(SUM(amount)) OVER (PARTITION BY dept_name ORDER BY budget_month_key) > 0
        THEN ((SUM(amount) - LAG(SUM(amount)) OVER (PARTITION BY dept_name ORDER BY budget_month_key)) / 
               LAG(SUM(amount)) OVER (PARTITION BY dept_name ORDER BY budget_month_key)) * 100
        ELSE 0
    END as month_over_month_change
FROM fact_expense e
JOIN dim_department d ON e.dept_id = d.dept_id
GROUP BY budget_month_key, dept_name
ORDER BY dept_name, budget_month_key;

-- View 6: Employee Expense Analysis
CREATE OR REPLACE VIEW vw_employee_expense_analysis AS
SELECT 
    emp.employee_name,
    emp.employee_id,
    d.dept_name,
    COUNT(e.invoice_id) as expense_count,
    SUM(e.amount) as total_expenses,
    AVG(e.amount) as avg_expense_amount,
    MAX(e.amount) as max_expense_amount,
    MIN(e.trx_date) as first_expense_date,
    MAX(e.trx_date) as last_expense_date,
    COUNT(DISTINCT v.vendor_id) as unique_vendors,
    -- Weekend/Holiday expense count
    COUNT(CASE WHEN cal.is_weekend OR cal.is_holiday THEN 1 END) as weekend_holiday_expenses,
    -- Round number expense count
    COUNT(CASE WHEN e.amount % 100 = 0 THEN 1 END) as round_number_expenses
FROM fact_expense e
JOIN dim_employee emp ON e.employee_id = emp.employee_id
JOIN dim_department d ON e.dept_id = d.dept_id
JOIN dim_vendor v ON e.vendor_id = v.vendor_id
JOIN dim_calendar cal ON e.trx_date = cal.date
GROUP BY emp.employee_name, emp.employee_id, d.dept_name
ORDER BY total_expenses DESC;

-- View 7: Risk Dashboard Summary
CREATE OR REPLACE VIEW vw_risk_dashboard AS
SELECT 
    'Budget Variance' as metric_type,
    COUNT(CASE WHEN variance_percentage > 10 THEN 1 END) as high_risk_count,
    COUNT(CASE WHEN variance_percentage BETWEEN -10 AND 10 THEN 1 END) as normal_count,
    COUNT(CASE WHEN variance_percentage < -10 THEN 1 END) as low_risk_count
FROM vw_budget_vs_actual
UNION ALL
SELECT 
    'Vendor Concentration' as metric_type,
    COUNT(CASE WHEN concentration_risk = 'High Risk' THEN 1 END) as high_risk_count,
    COUNT(CASE WHEN concentration_risk = 'Medium Risk' THEN 1 END) as normal_count,
    COUNT(CASE WHEN concentration_risk IN ('Low Risk', 'Minimal Risk') THEN 1 END) as low_risk_count
FROM vw_vendor_concentration
UNION ALL
SELECT 
    'Leakage Signals' as metric_type,
    COUNT(CASE WHEN risk_level IN ('Critical', 'High') THEN 1 END) as high_risk_count,
    COUNT(CASE WHEN risk_level = 'Medium' THEN 1 END) as normal_count,
    COUNT(CASE WHEN risk_level = 'Low' THEN 1 END) as low_risk_count
FROM vw_leakage_flags;

-- Grant permissions to views
GRANT SELECT ON VIEW vw_budget_vs_actual TO ROLE analyst;
GRANT SELECT ON VIEW vw_vendor_concentration TO ROLE analyst;
GRANT SELECT ON VIEW vw_leakage_flags TO ROLE analyst;
GRANT SELECT ON VIEW vw_campaign_roi TO ROLE analyst;
GRANT SELECT ON VIEW vw_monthly_expense_trends TO ROLE analyst;
GRANT SELECT ON VIEW vw_employee_expense_analysis TO ROLE analyst;
GRANT SELECT ON VIEW vw_risk_dashboard TO ROLE analyst;
