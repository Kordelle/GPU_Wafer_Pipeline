"""
Great Expectations Configuration for Manufacturing Telemetry
Defines validation rules for Silver layer data quality
"""

from great_expectations.core import ExpectationConfiguration
from typing import List


class ManufacturingExpectations:
    """CVD Process Validation Rules"""
    
    @staticmethod
    def get_silver_expectations() -> List[ExpectationConfiguration]:
        """
        Silver layer validation suite
        Based on Chemical Vapor Deposition process specs
        """
        return [
            # Schema Validation
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={
                    "column_list": [
                        "timestamp", "wafer_id", "equipment_id",
                        "temperature_c", "pressure_torr", "yield_rate",
                        "defect_count", "is_anomaly", "date",
                        "shift_number", "hour_of_day", "equipment_age_days",
                        "temperature_deviation_c", "pressure_deviation_torr",
                        "quality_flag", "silver_ingestion_time", "quality_score"
                    ]
                }
            ),
            
            # Uniqueness Constraints
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "wafer_id"}
            ),
            
            # Null Validation (NOT NULL columns)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "wafer_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "equipment_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "timestamp"}
            ),
            
            # Equipment ID Validation (4 CVD chambers)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "equipment_id",
                    "value_set": ["EQ001", "EQ002", "EQ003", "EQ004"]
                }
            ),
            
            # CVD Process Parameter Ranges
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "temperature_c",
                    "min_value": 330.0,
                    "max_value": 380.0,
                    "mostly": 0.98  # Allow 2% out-of-range (anomalies)
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "pressure_torr",
                    "min_value": 6.0,
                    "max_value": 14.0,
                    "mostly": 0.98
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "yield_rate",
                    "min_value": 0.50,  # Worst-case anomaly
                    "max_value": 1.00,
                    "mostly": 0.95  # 95% should be >85%
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "defect_count",
                    "min_value": 0,
                    "max_value": 30,
                    "mostly": 0.98
                }
            ),
            
            # Shift Validation (3 shifts per day)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "shift_number",
                    "value_set": [1, 2, 3]
                }
            ),
            
            # Quality Flag Validation
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "quality_flag",
                    "value_set": ["PASS", "FAIL"]
                }
            ),
            
            # Quality Score Range (0-100)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "quality_score",
                    "min_value": 0,
                    "max_value": 100
                }
            ),
            
            # Statistical Checks (Manufacturing KPIs)
            ExpectationConfiguration(
                expectation_type="expect_column_mean_to_be_between",
                kwargs={
                    "column": "temperature_c",
                    "min_value": 345.0,
                    "max_value": 355.0  # Process mean should be near 350°C
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_mean_to_be_between",
                kwargs={
                    "column": "yield_rate",
                    "min_value": 0.90,  # Target: 95% average yield
                    "max_value": 0.98
                }
            ),
            
            # Data Freshness (timestamp should be recent)
            ExpectationConfiguration(
                expectation_type="expect_column_max_to_be_between",
                kwargs={
                    "column": "timestamp",
                    "min_value": "2024-01-01",  # After equipment install
                    "max_value": "2026-12-31",
                    "parse_strings_as_datetimes": True
                }
            ),
        ]
    
    @staticmethod
    def get_critical_expectations() -> List[str]:
        """
        Critical expectations that MUST pass (block pipeline if failed)
        Non-critical expectations generate warnings but allow pipeline to continue
        """
        return [
            "expect_column_values_to_be_unique",  # Duplicate wafer IDs
            "expect_column_values_to_not_be_null",  # Schema violations
            "expect_column_values_to_be_in_set",  # Invalid equipment IDs
        ]