"""
Data Quality Validation Module
Supports both Great Expectations (local) and native PySpark (Databricks)
"""

from .local_validator import LocalValidator
from .pyspark_validator import PySparkValidator

__all__ = ["LocalValidator", "PySparkValidator"]