# Dashboard Charts Analysis Report

**Generated:** December 23, 2025  
**Data Sources:** Gold layer Parquet files (verified Apache Parquet format with Snappy compression)  
**Total Charts:** 8 interactive visualizations

## Data Format Verification

✅ **Confirmed:** All data is stored in **Apache Parquet format** with Snappy compression
- Silver layer: `data/silver/*/*.snappy.parquet`
- Gold layer: `data/gold/*/*.snappy.parquet`
- Verified using `file` command and PySpark read operations
- Format provides columnar storage, compression, and schema evolution capabilities

## Executive Summary

The dashboard successfully visualizes budget leakage detection across 1,500 expense transactions, 2,263 leakage signals, 84 budget records, and 50 vendors. All charts provide actionable insights for financial analysis.

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

1. **Duplicate Invoice Dominance:** 65.8% of signals are duplicate invoices - this high percentage suggests:
   - The 30-day detection window may be too broad (legitimate recurring expenses flagged)
   - Potential data quality issues in invoice processing
   - Need to review detection threshold and add amount similarity checks
   - **Action:** Investigate sample of duplicate signals to validate rule accuracy

2. **Weekend/Holiday Claims:** 24.6% are weekend/holiday expenses - this is significant and suggests:
   - Policy violations or legitimate business travel
   - Need to cross-reference with employee travel policies
   - **Action:** Review company expense policy for weekend/holiday allowances

3. **Campaign Spikes:** 8.7% are campaign spend anomalies - indicates:
   - Marketing campaigns with cost increases but low conversion lift
   - Potential inefficient ad spend
   - **Action:** Review campaign performance and adjust bidding strategies

4. **Round Numbers:** Only 0.9% round number spikes - this is actually good:
   - Suggests most expenses are legitimate (not fabricated)
   - Low percentage indicates healthy expense patterns

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

1. **Risk Concentration:** 90.9% medium risk signals indicates:
   - Most anomalies are moderate severity
   - Scoring system is working but may need more granularity
   - Consider adding sub-categories (medium-high, medium-low)

2. **High-Risk Focus:** 192 high-risk signals (8.5%) require immediate investigation:
   - These represent highest priority for finance team review
   - Should be triaged within 24-48 hours
   - **Action:** Create automated alert workflow for high-risk signals

3. **Low Variance:** Standard deviation of 3.99 indicates:
   - Risk scores are clustered around 75
   - May need to expand scoring range for better differentiation
   - Consider adding more nuanced scoring factors

4. **Distribution:** Concentration around 75 suggests:
   - Scoring algorithm may be too conservative
   - Consider recalibrating thresholds to spread scores more evenly

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

1. **Systematic Under-Spending:** 83.3% under budget is a RED FLAG:
   - Average -36.62% variance indicates budgets are significantly overestimated
   - This suggests either: budget planning process needs improvement, or spending is being under-reported
   - **Action:** Review budget planning methodology and verify all expenses are captured
   - **Business Impact:** Over-budgeting ties up capital unnecessarily

2. **Engineering Over-Budget:** 95.77% variance in Engineering requires investigation:
   - May indicate project cost overruns or scope creep
   - Could be legitimate (new initiatives) or problematic (poor cost control)
   - **Action:** Department head review of Engineering spending patterns

3. **Low On-Target Rate:** Only 7.1% on target indicates:
   - Budget accuracy is poor across the organization
   - Budgets are not aligned with actual operational needs
   - **Action:** Implement rolling forecast instead of annual budgets

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

1. **Critical Budget Issue:** -42% variance is EXTREME and indicates:
   - Budgets are nearly double actual spending on average
   - This is not normal - typical variance should be ±10-15%
   - **Root Cause Analysis Needed:** Is this budget planning error or missing expense data?

2. **Consistent Pattern:** $557K actual vs $962K budget shows:
   - Systematic overestimation, not random error
   - Suggests budget model or assumptions are fundamentally flawed
   - **Action:** Recalibrate budget model using historical actuals

3. **Business Impact:**
   - Over-budgeting reduces available capital for other initiatives
   - Creates false sense of spending constraints
   - May indicate poor financial planning processes

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

### Key Findings

1. **Leakage Detection:** 2,263 signals detected
   - **Primary Issue:** 65.8% duplicate invoices - rule may be too sensitive, needs tuning
   - **Action Required:** Review 30-day window and add amount similarity validation
   - **High Priority:** 192 high-risk signals need immediate investigation

2. **Budget Management:** Critical issue identified
   - **-42% variance is EXTREME** - not normal business variance (typical ±10-15%)
   - Indicates either: budget model failure or missing expense data
   - **Action Required:** Root cause analysis and budget model recalibration

3. **Vendor Risk:** Excellent diversification
   - Top vendor only 2.82% of spend (well below 10% risk threshold)
   - No high-risk vendors identified
   - Current vendor strategy is sound

4. **Risk Scoring:** Needs refinement
   - 90.9% signals clustered in medium risk (60-79)
   - Low standard deviation (3.99) suggests scoring lacks granularity
   - **Action:** Expand scoring range for better signal differentiation

### Real-World Applicability

**Strengths:**
- All metrics are calculated from real Parquet data
- Detection rules identify actual anomalies
- Vendor concentration analysis follows industry best practices
- Budget variance calculations are standard FP&A metrics

**Areas for Improvement:**
- Duplicate detection rule needs calibration (too many false positives likely)
- Budget variance is extreme and would trigger executive review in real business
- Risk scoring could be more nuanced to better prioritize signals

**Production Readiness:**
- Data pipeline is production-ready (Parquet format, proper schemas)
- Analysis provides actionable insights
- Charts accurately represent underlying data
- Metrics align with standard financial analysis practices

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

