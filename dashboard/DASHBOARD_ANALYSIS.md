# Dashboard Charts Analysis Report

**Generated:** December 23, 2025  
**Data Sources:** Gold layer Parquet files  
**Total Charts:** 8 interactive visualizations

## Executive Summary

The dashboard successfully visualizes budget leakage detection across 1,500 expense transactions, 2,263 leakage signals, 84 budget records, and 50 vendors. All charts are functional and provide actionable insights for financial analysis.

---

## Chart 1: Monthly Expense Trends

**Type:** Line Chart  
**File:** `chart_1_monthly_expenses.html`  
**Tab:** Overview

### Analysis

**Data Points:**
- Total months analyzed: 12
- Average monthly expenses: $557,453.08
- Highest month: January 2024 ($1,408,934.86)
- Lowest month: July 2024 ($395,865.65)
- Month-over-month volatility: $272,653.84

### Insights

1. **Seasonal Pattern:** January shows the highest spending ($1.4M), suggesting year-start budget allocation or catch-up spending
2. **Volatility:** High standard deviation ($272K) indicates inconsistent spending patterns
3. **Trend:** Significant variation between months requires investigation into spending drivers

### Business Value

- Identifies months requiring budget adjustments
- Highlights seasonal spending patterns
- Supports cash flow planning

---

## Chart 2: Leakage Signals by Detection Rule

**Type:** Bar Chart  
**File:** `chart_2_leakage_by_type.html`  
**Tab:** Overview

### Analysis

**Signal Distribution:**
- Total signals detected: 2,263
- Most common rule: duplicate_invoice (1,488 signals, 65.8%)
- Number of active detection rules: 4
- Average signals per rule: 565.8

**Rule Breakdown:**
1. duplicate_invoice: 1,488 signals (65.8%)
2. weekend_holiday_claim: 557 signals (24.6%)
3. campaign_spend_spike: 197 signals (8.7%)
4. round_number_spikes: 21 signals (0.9%)

### Insights

1. **Duplicate Invoice Dominance:** 65.8% of all signals are duplicate invoices, indicating potential vendor fraud or processing errors
2. **Weekend/Holiday Claims:** 24.6% of signals are weekend/holiday expenses, suggesting policy violations
3. **Campaign Spikes:** 8.7% are campaign spend anomalies requiring marketing review
4. **Round Numbers:** Minimal round number spikes (0.9%) suggests most expenses are legitimate

### Business Value

- Prioritizes investigation efforts (focus on duplicate invoices)
- Validates detection rule effectiveness
- Identifies areas needing process improvements

---

## Chart 3: Risk Score Distribution

**Type:** Histogram  
**File:** `chart_3_risk_distribution.html`  
**Tab:** Leakage Signals

### Analysis

**Risk Statistics:**
- Mean risk score: 73.81
- Median risk score: 75.00
- Standard deviation: 3.99
- High risk signals (80+): 192 (8.5%)
- Medium risk signals (60-79): 2,057 (90.9%)
- Low risk signals (<60): 14 (0.6%)

### Insights

1. **Risk Concentration:** 90.9% of signals are medium risk, indicating consistent detection threshold
2. **High-Risk Focus:** 192 high-risk signals (8.5%) require immediate investigation
3. **Low Variance:** Standard deviation of 3.99 shows consistent risk scoring
4. **Distribution:** Bell curve centered around 75 suggests well-calibrated scoring system

### Business Value

- Quantifies overall risk exposure
- Prioritizes high-risk signals for investigation
- Validates risk scoring methodology

---

## Chart 4: Leakage Signals Over Time

**Type:** Time Series Line Chart  
**File:** `chart_4_signals_over_time.html`  
**Tab:** Leakage Signals

### Analysis

**Temporal Patterns:**
- Date range: January 1, 2024 to December 23, 2025
- Average signals per day: 6.25
- Peak day: January 21, 2024 (70 signals)
- Days with signals: 362
- Trend: Increasing

### Insights

1. **Increasing Trend:** Signal detection is increasing over time, suggesting either:
   - Improved detection capabilities
   - Actual increase in leakage incidents
   - More data being processed

2. **Peak Detection:** January 21, 2024 had 70 signals, requiring investigation into that specific day's transactions

3. **Consistent Detection:** 362 days with signals out of ~730 days indicates regular monitoring

### Business Value

- Identifies temporal patterns in leakage
- Shows detection system effectiveness
- Supports trend-based forecasting

---

## Chart 5: Budget Variance by Department

**Type:** Bar Chart  
**File:** `chart_5_budget_variance.html`  
**Tab:** Budget Analysis

### Analysis

**Variance Statistics:**
- Total department-month combinations: 84
- Over budget (>10%): 8 instances (9.5%)
- Under budget (<-10%): 70 instances (83.3%)
- On target: 6 instances (7.1%)
- Average variance: -36.62%
- Highest variance: Engineering (95.77%)

### Insights

1. **Systematic Under-Spending:** 83.3% of instances are under budget, suggesting:
   - Overly conservative budgets
   - Budget allocation issues
   - Delayed spending

2. **Engineering Over-Budget:** Engineering shows 95.77% variance, indicating significant budget overrun

3. **Low On-Target Rate:** Only 7.1% of instances are on target, suggesting budget accuracy issues

### Business Value

- Identifies departments with budget management issues
- Highlights need for budget recalibration
- Supports budget allocation decisions

---

## Chart 6: Monthly Budget vs Actual Performance

**Type:** Grouped Bar Chart  
**File:** `chart_6_monthly_budget_performance.html`  
**Tab:** Budget Analysis

### Analysis

**Budget Performance:**
- Total budget: $11,543,713.50
- Total actual: $6,689,436.99
- Overall variance: -42.05%
- Average monthly budget: $961,976.13
- Average monthly actual: $557,453.08
- Months analyzed: 12

### Insights

1. **Significant Under-Spending:** 42% overall variance indicates systematic budget overestimation
2. **Consistent Pattern:** Average actual ($557K) is consistently below average budget ($962K)
3. **Budget Accuracy:** Large gap suggests budget planning process needs review

### Business Value

- Shows budget accuracy over time
- Identifies need for budget recalibration
- Supports financial planning improvements

---

## Chart 7: Top 10 Vendors by Spend

**Type:** Bar Chart  
**File:** `chart_7_top_vendors.html`  
**Tab:** Vendor Risk

### Analysis

**Vendor Statistics:**
- Top vendor: George Group ($188,906.77, 2.82%)
- Top 10 vendors account for 25.3% of total spend
- Total vendors: 50
- Total spend across all vendors: $6,689,436.99
- Average vendor spend: $133,788.74

**Top 10 Vendors:**
1. George Group: $188,906.77 (2.82%)
2. Mcclain, Miller and Henderson: $184,446.33 (2.76%)
3. Smith-Bowen: $177,910.82 (2.66%)
4. Watts, Robinson and Nguyen: $169,829.65 (2.54%)
5. Arroyo, Miller and Tucker: $169,423.35 (2.53%)
6. Hoffman, Baker and Richards: $164,335.54 (2.46%)
7. Blake and Sons: $161,179.15 (2.41%)
8. Abbott-Munoz: $159,480.55 (2.38%)
9. Guzman, Hoffman and Baldwin: $159,281.03 (2.38%)
10. Rodriguez-Graham: $158,031.88 (2.36%)

### Insights

1. **Low Concentration:** Top vendor only accounts for 2.82% of spend, indicating good vendor diversification
2. **Top 10 Concentration:** 25.3% concentration is moderate, suggesting healthy vendor distribution
3. **No Single Point of Failure:** No vendor exceeds 3% of total spend, reducing vendor risk

### Business Value

- Identifies key vendor relationships
- Highlights vendor concentration risk (low in this case)
- Supports vendor management decisions

---

## Chart 8: Vendor Risk Distribution

**Type:** Pie Chart  
**File:** `chart_8_vendor_risk_distribution.html`  
**Tab:** Vendor Risk

### Analysis

**Risk Distribution:**
- High risk vendors: 0 (0.0%)
- Medium risk vendors: 28 (56.0%)
- Low risk vendors: 22 (44.0%)
- Total vendors: 50

### Insights

1. **No High-Risk Vendors:** Zero vendors exceed 10% spend threshold, indicating excellent vendor diversification
2. **Balanced Distribution:** 56% medium risk, 44% low risk shows healthy vendor portfolio
3. **Low Concentration Risk:** No single vendor poses significant financial risk

### Business Value

- Quantifies vendor concentration risk (very low)
- Validates vendor diversification strategy
- Supports vendor management decisions

---

## Overall Dashboard Assessment

### Functionality Status: ✅ ALL CHARTS WORKING

All 8 charts generate successfully with real data:
- ✅ Monthly Expense Trends
- ✅ Leakage Signals by Type
- ✅ Risk Score Distribution
- ✅ Signals Over Time
- ✅ Budget Variance by Department
- ✅ Monthly Budget Performance
- ✅ Top Vendors by Spend
- ✅ Vendor Risk Distribution

### Key Findings

1. **Leakage Detection:** 2,263 signals detected, with duplicate invoices being the primary concern (65.8%)
2. **Budget Management:** Systematic under-spending (42% variance) suggests budget calibration needed
3. **Vendor Risk:** Excellent diversification with no high-risk vendors
4. **Risk Distribution:** 90.9% medium-risk signals indicate consistent detection thresholds

### Recommendations

1. **Immediate Action:** Investigate 192 high-risk leakage signals
2. **Process Improvement:** Review duplicate invoice detection (1,488 signals)
3. **Budget Review:** Recalibrate budgets given 42% under-spending
4. **Vendor Management:** Maintain current diversification strategy

---

## Technical Validation

- **Data Loading:** ✅ Successfully loads from gold layer Parquet files
- **Data Types:** ✅ All numeric columns properly converted
- **Chart Rendering:** ✅ All Plotly charts render correctly
- **Interactive Features:** ✅ Hover, zoom, and export functionality working
- **Performance:** ✅ Charts load quickly with 1,500+ transactions

---

## Chart Files Location

All chart HTML files are saved in: `dashboard/charts/`

- `chart_1_monthly_expenses.html`
- `chart_2_leakage_by_type.html`
- `chart_3_risk_distribution.html`
- `chart_4_signals_over_time.html`
- `chart_5_budget_variance.html`
- `chart_6_monthly_budget_performance.html`
- `chart_7_top_vendors.html`
- `chart_8_vendor_risk_distribution.html`

These files can be opened directly in a web browser for interactive viewing.

---

*Analysis generated from actual pipeline execution data*
