from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results = []
    
    def check_not_null(self, df, column: str, table_name: str) -> Tuple[bool, str]:
        null_count = df.filter(col(column).isNull()).count()
        total_count = df.count()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 0
        
        passed = null_count == 0
        message = f"{table_name}.{column}: {null_count} nulls ({null_pct:.2f}%)"
        
        self.results.append({
            'table': table_name,
            'column': column,
            'check': 'not_null',
            'passed': passed,
            'null_count': null_count,
            'null_percentage': null_pct,
            'message': message
        })
        
        return passed, message
    
    def check_unique(self, df, column: str, table_name: str) -> Tuple[bool, str]:
        total_count = df.count()
        distinct_count = df.select(column).distinct().count()
        duplicate_count = total_count - distinct_count
        
        passed = duplicate_count == 0
        message = f"{table_name}.{column}: {duplicate_count} duplicates"
        
        self.results.append({
            'table': table_name,
            'column': column,
            'check': 'unique',
            'passed': passed,
            'duplicate_count': duplicate_count,
            'message': message
        })
        
        return passed, message
    
    def check_range(self, df, column: str, min_val: float, max_val: float, table_name: str) -> Tuple[bool, str]:
        out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
        total_count = df.count()
        
        passed = out_of_range == 0
        message = f"{table_name}.{column}: {out_of_range} values outside range [{min_val}, {max_val}]"
        
        self.results.append({
            'table': table_name,
            'column': column,
            'check': 'range',
            'passed': passed,
            'out_of_range_count': out_of_range,
            'min_value': min_val,
            'max_value': max_val,
            'message': message
        })
        
        return passed, message
    
    def check_positive(self, df, column: str, table_name: str) -> Tuple[bool, str]:
        negative_count = df.filter(col(column) <= 0).count()
        total_count = df.count()
        
        passed = negative_count == 0
        message = f"{table_name}.{column}: {negative_count} non-positive values"
        
        self.results.append({
            'table': table_name,
            'column': column,
            'check': 'positive',
            'passed': passed,
            'negative_count': negative_count,
            'message': message
        })
        
        return passed, message
    
    def check_row_count(self, df, min_rows: int, table_name: str) -> Tuple[bool, str]:
        row_count = df.count()
        passed = row_count >= min_rows
        message = f"{table_name}: {row_count} rows (minimum: {min_rows})"
        
        self.results.append({
            'table': table_name,
            'check': 'row_count',
            'passed': passed,
            'row_count': row_count,
            'min_rows': min_rows,
            'message': message
        })
        
        return passed, message
    
    def check_expenses_quality(self, df, table_name: str = "expenses") -> Dict[str, bool]:
        logger.info(f"Running data quality checks on {table_name}")
        
        checks = {}
        
        checks['invoice_id_not_null'], _ = self.check_not_null(df, 'invoice_id', table_name)
        checks['invoice_id_unique'], _ = self.check_unique(df, 'invoice_id', table_name)
        checks['amount_positive'], _ = self.check_positive(df, 'amount', table_name)
        checks['amount_range'], _ = self.check_range(df, 'amount', 0.01, 1000000.0, table_name)
        checks['trx_date_not_null'], _ = self.check_not_null(df, 'trx_date', table_name)
        checks['vendor_id_not_null'], _ = self.check_not_null(df, 'vendor_id', table_name)
        checks['employee_id_not_null'], _ = self.check_not_null(df, 'employee_id', table_name)
        checks['dept_id_not_null'], _ = self.check_not_null(df, 'dept_id', table_name)
        checks['min_rows'], _ = self.check_row_count(df, 1, table_name)
        
        return checks
    
    def check_budgets_quality(self, df, table_name: str = "budgets") -> Dict[str, bool]:
        logger.info(f"Running data quality checks on {table_name}")
        
        checks = {}
        
        checks['dept_id_not_null'], _ = self.check_not_null(df, 'dept_id', table_name)
        checks['budget_month_not_null'], _ = self.check_not_null(df, 'budget_month', table_name)
        checks['budget_amount_positive'], _ = self.check_positive(df, 'budget_amount', table_name)
        checks['dept_month_unique'], _ = self.check_unique(df.select('dept_id', 'budget_month'), 
                                                          'dept_id', table_name)
        checks['min_rows'], _ = self.check_row_count(df, 1, table_name)
        
        return checks
    
    def check_campaign_quality(self, df, table_name: str = "campaign_spend") -> Dict[str, bool]:
        logger.info(f"Running data quality checks on {table_name}")
        
        checks = {}
        
        checks['spend_id_not_null'], _ = self.check_not_null(df, 'spend_id', table_name)
        checks['spend_id_unique'], _ = self.check_unique(df, 'spend_id', table_name)
        checks['cost_positive'], _ = self.check_positive(df, 'cost', table_name)
        checks['date_not_null'], _ = self.check_not_null(df, 'date', table_name)
        checks['campaign_id_not_null'], _ = self.check_not_null(df, 'campaign_id', table_name)
        checks['min_rows'], _ = self.check_row_count(df, 1, table_name)
        
        return checks
    
    def get_summary(self) -> Dict:
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r['passed'])
        failed_checks = total_checks - passed_checks
        
        return {
            'total_checks': total_checks,
            'passed': passed_checks,
            'failed': failed_checks,
            'pass_rate': (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            'results': self.results
        }
    
    def print_summary(self):
        summary = self.get_summary()
        logger.info(f"Data Quality Summary: {summary['passed']}/{summary['total_checks']} checks passed ({summary['pass_rate']:.2f}%)")
        
        for result in self.results:
            status = "PASS" if result['passed'] else "FAIL"
            logger.info(f"  [{status}] {result['message']}")

