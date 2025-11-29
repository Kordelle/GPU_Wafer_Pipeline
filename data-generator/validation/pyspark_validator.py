"""
Native PySpark validation for Databricks Serverless
No external dependencies, compatible with all Spark environments
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, avg, stddev, 
    min as spark_min, max as spark_max
)
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class PySparkValidator:
    """Native PySpark validator for cloud environments"""
    
    # CVD Process Validation Thresholds (same as Great Expectations)
    TEMP_MIN = 330.0
    TEMP_MAX = 380.0
    TEMP_MEAN_MIN = 345.0
    TEMP_MEAN_MAX = 355.0
    
    PRESSURE_MIN = 6.0
    PRESSURE_MAX = 14.0
    
    YIELD_MIN = 0.50
    YIELD_MAX = 1.00
    YIELD_MEAN_MIN = 0.90
    YIELD_MEAN_MAX = 0.98
    
    DEFECT_MAX = 30
    TOLERANCE = 0.98  # 98% pass rate for range validations
    
    VALID_EQUIPMENT = ["EQ001", "EQ002", "EQ003", "EQ004"]
    VALID_SHIFTS = [1, 2, 3]
    VALID_QUALITY_FLAGS = ["PASS", "FAIL"]
    
    def __init__(self):
        self.validation_results = []
    
    def validate_dataframe(self, df: DataFrame, layer: str = "silver") -> Dict[str, Any]:
        """
        Validate Spark DataFrame using native PySpark queries
        
        Args:
            df: Spark DataFrame to validate
            layer: Data layer (bronze/silver/gold)
        
        Returns:
            Validation results dictionary (same structure as LocalValidator)
        """
        logger.info(f"Starting PySpark validation for {layer} layer")
        
        self.validation_results = []
        record_count = df.count()
        
        # Schema validation
        self._validate_schema(df, layer)
        
        # Uniqueness validation
        self._validate_uniqueness(df, "wafer_id")
        
        # Null validation
        critical_columns = ["wafer_id", "equipment_id", "timestamp", "temperature_c", "yield_rate"]
        for column in critical_columns:
            self._validate_not_null(df, column)
        
        # Categorical validation
        self._validate_categorical(df, "equipment_id", self.VALID_EQUIPMENT)
        
        if layer == "silver":
            # Silver-specific validations
            if "shift_number" in df.columns:
                self._validate_categorical(df, "shift_number", self.VALID_SHIFTS)
            if "quality_flag" in df.columns:
                self._validate_categorical(df, "quality_flag", self.VALID_QUALITY_FLAGS)
        
        # Range validation (CVD process parameters)
        self._validate_range(df, "temperature_c", self.TEMP_MIN, self.TEMP_MAX, record_count)
        self._validate_range(df, "pressure_torr", self.PRESSURE_MIN, self.PRESSURE_MAX, record_count)
        self._validate_range(df, "yield_rate", self.YIELD_MIN, self.YIELD_MAX, record_count)
        self._validate_range(df, "defect_count", 0, self.DEFECT_MAX, record_count)
        
        # Statistical validation
        self._validate_mean(df, "temperature_c", self.TEMP_MEAN_MIN, self.TEMP_MEAN_MAX)
        self._validate_mean(df, "yield_rate", self.YIELD_MEAN_MIN, self.YIELD_MEAN_MAX)
        
        # Calculate summary
        passed = sum(1 for r in self.validation_results if r["success"])
        failed = len(self.validation_results) - passed
        
        critical_failures = [
            r for r in self.validation_results 
            if not r["success"] and r.get("critical", False)
        ]
        
        summary = {
            "total_validations": len(self.validation_results),
            "passed": passed,
            "failed": failed,
            "critical_failures": len(critical_failures),
            "results": self.validation_results
        }
        
        logger.info(
            f"Validation complete: {passed}/{len(self.validation_results)} passed, "
            f"{len(critical_failures)} critical failures"
        )
        
        return summary
    
    def _validate_schema(self, df: DataFrame, layer: str):
        """Validate expected columns exist"""
        expected_bronze = [
            "timestamp", "wafer_id", "equipment_id",
            "temperature_c", "pressure_torr", "yield_rate",
            "defect_count", "is_anomaly", "date"
        ]
        
        expected_silver = expected_bronze + [
            "shift_number", "hour_of_day", "equipment_age_days",
            "temperature_deviation_c", "pressure_deviation_torr",
            "quality_flag", "silver_ingestion_time", "quality_score"
        ]
        
        expected_columns = expected_silver if layer == "silver" else expected_bronze
        actual_columns = df.columns
        missing_columns = set(expected_columns) - set(actual_columns)
        
        self.validation_results.append({
            "validation": "schema_columns",
            "success": len(missing_columns) == 0,
            "critical": True,
            "details": {"missing": list(missing_columns), "layer": layer}
        })
    
    def _validate_uniqueness(self, df: DataFrame, column: str):
        """Validate column uniqueness"""
        total = df.count()
        unique = df.select(countDistinct(column)).collect()[0][0]
        
        self.validation_results.append({
            "validation": f"{column}_uniqueness",
            "column": column,
            "success": total == unique,
            "critical": True,
            "details": {"duplicates": total - unique}
        })
    
    def _validate_not_null(self, df: DataFrame, column: str):
        """Validate NOT NULL constraint"""
        null_count = df.filter(col(column).isNull()).count()
        
        self.validation_results.append({
            "validation": f"{column}_not_null",
            "column": column,
            "success": null_count == 0,
            "critical": True,
            "details": {"null_count": null_count}
        })
    
    def _validate_categorical(self, df: DataFrame, column: str, valid_values: List):
        """Validate categorical column values"""
        invalid_count = df.filter(~col(column).isin(valid_values)).count()
        
        self.validation_results.append({
            "validation": f"{column}_values",
            "column": column,
            "success": invalid_count == 0,
            "critical": True,
            "details": {"invalid_count": invalid_count, "valid_values": valid_values}
        })
    
    def _validate_range(self, df: DataFrame, column: str, min_val: float, max_val: float, total: int):
        """Validate numeric range with tolerance"""
        out_of_range = df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        
        pass_rate = (total - out_of_range) / total if total > 0 else 0
        
        self.validation_results.append({
            "validation": f"{column}_range",
            "column": column,
            "success": pass_rate >= self.TOLERANCE,
            "critical": False,
            "details": {
                "min": min_val,
                "max": max_val,
                "out_of_range": out_of_range,
                "pass_rate": pass_rate
            }
        })
    
    def _validate_mean(self, df: DataFrame, column: str, min_mean: float, max_mean: float):
        """Validate statistical mean"""
        actual_mean = df.select(avg(column)).collect()[0][0]
        
        self.validation_results.append({
            "validation": f"{column}_mean",
            "column": column,
            "success": min_mean <= actual_mean <= max_mean,
            "critical": False,
            "details": {
                "expected_min": min_mean,
                "expected_max": max_mean,
                "actual": float(actual_mean)
            }
        })
    
    def is_valid(self, validation_summary: Dict[str, Any]) -> bool:
        """Check if validation passed (no critical failures)"""
        return validation_summary["critical_failures"] == 0