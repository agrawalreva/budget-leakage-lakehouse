#!/usr/bin/env python3
"""
Generate sample datasets for Budget Leakage Lakehouse
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
Faker.seed(42)
np.random.seed(42)
random.seed(42)

fake = Faker()

def generate_expenses_data(n_rows=1500):
    """Generate expense-like transactions (PaySim inspired)"""
    
    # Generate base data
    data = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Pre-generate some entities for consistency
    vendors = [fake.company() for _ in range(50)]
    employees = [fake.name() for _ in range(100)]
    departments = ['Engineering', 'Marketing', 'Sales', 'Finance', 'HR', 'Operations', 'Legal']
    
    for i in range(n_rows):
        # Create some duplicate invoices for leakage detection
        if i < 50:  # 50 duplicate invoices
            vendor_id = random.choice(range(1, 11))  # Focus on first 10 vendors
            amount = round(random.uniform(100, 5000), 2)
            # Create duplicate with same vendor, similar amount, different date
            if i % 2 == 0:
                trx_date = start_date + timedelta(days=random.randint(0, 30))
            else:
                trx_date = start_date + timedelta(days=random.randint(0, 30) + 15)  # 15 days later
        else:
            vendor_id = random.randint(1, 50)
            amount = round(random.uniform(10, 10000), 2)
            trx_date = start_date + timedelta(days=random.randint(0, 365))
        
        # Create some weekend/holiday expenses for leakage detection
        if random.random() < 0.1:  # 10% weekend expenses
            trx_date = start_date + timedelta(days=random.choice([5, 6, 12, 13, 19, 20, 26, 27]))  # Weekends
        
        # Create some round number expenses for leakage detection
        if random.random() < 0.15:  # 15% round numbers
            amount = round(random.uniform(100, 5000), -2)  # Round to hundreds
        
        # Create some sub-threshold repeats
        if random.random() < 0.05:  # 5% small repeated expenses
            amount = round(random.uniform(50, 99), 2)
        
        data.append({
            'invoice_id': f"INV-{i+1:06d}",
            'vendor_id': vendor_id,
            'vendor_name': vendors[vendor_id-1],
            'employee_id': random.randint(1, 100),
            'employee_name': random.choice(employees),
            'dept_id': random.randint(1, 7),
            'dept_name': random.choice(departments),
            'trx_date': trx_date.strftime('%Y-%m-%d'),
            'amount': amount,
            'currency': 'USD',
            'payment_method': random.choice(['Credit Card', 'ACH', 'Wire', 'Check']),
            'description': fake.sentence(nb_words=6)
        })
    
    return pd.DataFrame(data)

def generate_budgets_data():
    """Generate monthly budgets by department"""
    
    departments = ['Engineering', 'Marketing', 'Sales', 'Finance', 'HR', 'Operations', 'Legal']
    months = ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06',
              '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12']
    
    data = []
    for dept_id, dept_name in enumerate(departments, 1):
        base_budget = random.uniform(50000, 200000)  # Different base budgets per dept
        for month in months:
            # Add some seasonal variation
            seasonal_factor = 1 + 0.2 * np.sin(2 * np.pi * (int(month.split('-')[1]) - 1) / 12)
            budget = round(base_budget * seasonal_factor, 2)
            
            data.append({
                'dept_id': dept_id,
                'dept_name': dept_name,
                'budget_month': month,
                'budget_amount': budget,
                'currency': 'USD'
            })
    
    return pd.DataFrame(data)

def generate_campaign_spend_data(n_rows=800):
    """Generate marketing campaign spend data"""
    
    campaigns = [f"Campaign_{i}" for i in range(1, 21)]
    channels = ['Facebook', 'Google Ads', 'LinkedIn', 'Twitter', 'Instagram', 'TikTok']
    objectives = ['Brand Awareness', 'Lead Generation', 'Sales', 'App Install']
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(n_rows):
        campaign_id = random.randint(1, 20)
        channel = random.choice(channels)
        date = start_date + timedelta(days=random.randint(0, 365))
        
        # Base metrics
        cost = round(random.uniform(100, 5000), 2)
        impressions = random.randint(1000, 100000)
        clicks = random.randint(10, 1000)
        
        # Calculate realistic conversion rate and revenue
        ctr = clicks / impressions if impressions > 0 else 0
        conversion_rate = random.uniform(0.01, 0.05)  # 1-5%
        conversions = int(clicks * conversion_rate)
        
        # Revenue with some correlation to conversions
        revenue = round(conversions * random.uniform(50, 200), 2)
        
        # Create some spend spikes without conversion lift for leakage detection
        if random.random() < 0.1:  # 10% spend spikes
            cost *= random.uniform(1.5, 3.0)  # 50-200% increase
            conversions = max(1, int(conversions * random.uniform(0.5, 1.2)))  # Flat or slight increase
        
        data.append({
            'spend_id': f"SPEND-{i+1:06d}",
            'campaign_id': campaign_id,
            'campaign_name': campaigns[campaign_id-1],
            'channel': channel,
            'objective': random.choice(objectives),
            'date': date.strftime('%Y-%m-%d'),
            'cost': cost,
            'clicks': clicks,
            'impressions': impressions,
            'conversions': conversions,
            'attributed_revenue': revenue
        })
    
    return pd.DataFrame(data)

def generate_holidays_data():
    """Generate US holidays and calendar data"""
    
    # US Federal Holidays 2024
    holidays_2024 = [
        '2024-01-01',  # New Year's Day
        '2024-01-15',  # Martin Luther King Jr. Day
        '2024-02-19',  # Presidents' Day
        '2024-05-27',  # Memorial Day
        '2024-07-04',  # Independence Day
        '2024-09-02',  # Labor Day
        '2024-10-14',  # Columbus Day
        '2024-11-11',  # Veterans Day
        '2024-11-28',  # Thanksgiving
        '2024-12-25',  # Christmas
    ]
    
    data = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weekday = current_date.weekday()
        is_weekend = weekday >= 5  # Saturday = 5, Sunday = 6
        is_holiday = date_str in holidays_2024
        
        data.append({
            'date': date_str,
            'week': current_date.isocalendar()[1],
            'month_key': current_date.strftime('%Y-%m'),
            'quarter': f"Q{(current_date.month-1)//3 + 1}",
            'year': current_date.year,
            'is_weekend': is_weekend,
            'is_holiday': is_holiday
        })
        
        current_date += timedelta(days=1)
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    print("Generating sample datasets...")
    
    # Generate all datasets
    expenses_df = generate_expenses_data(1500)
    budgets_df = generate_budgets_data()
    campaign_df = generate_campaign_spend_data(800)
    holidays_df = generate_holidays_data()
    
    # Save to CSV
    expenses_df.to_csv('expenses_small.csv', index=False)
    budgets_df.to_csv('budgets_small.csv', index=False)
    campaign_df.to_csv('campaign_spend_small.csv', index=False)
    holidays_df.to_csv('holidays.csv', index=False)
    
    print(f"Generated datasets:")
    print(f"- Expenses: {len(expenses_df)} rows")
    print(f"- Budgets: {len(budgets_df)} rows")
    print(f"- Campaign Spend: {len(campaign_df)} rows")
    print(f"- Holidays: {len(holidays_df)} rows")
    print("\nFiles saved in current directory.")
