"""
Local validation using Great Expectations library
For Docker/local Spark clusters with PERSIST support
"""

from great_expectations.dataset import SparkDFDataset
from great_expectations_config import ManufacturingExpectations
from pyspark.sql import DataFrame
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class LocalValidator:
    """Great Expectations validator for local Spark environments"""
    
    def __init__(self):
        self.expectations = ManufacturingExpectations.get_silver_expectations()
        self.critical_expectations = ManufacturingExpectations.get_critical_expectations()
    
    def validate_dataframe(self, df: DataFrame, layer: str = "silver") -> Dict[str, Any]:
        """
        Validate Spark DataFrame using Great Expectations
        
        Args:
            df: Spark DataFrame to validate
            layer: Data layer (bronze/silver/gold)
        
        Returns:
            Validation results dictionary with structure:
            {
                "total_validations": int,
                "passed": int,
                "failed": int,
                "critical_failures": int,
                "results": List[Dict]
            }
        """
        logger.info(f"Starting Great Expectations validation for {layer} layer")
        
        # Convert to Great Expectations dataset
        ge_df = SparkDFDataset(df)
        
        validation_results = []
        
        for expectation in self.expectations:
            exp_type = expectation.expectation_type
            kwargs = expectation.kwargs
            
            try:
                result = ge_df.expect(**{"expectation_type": exp_type, **kwargs})
                
                is_critical = exp_type in self.critical_expectations
                
                validation_results.append({
                    "expectation": exp_type,
                    "column": kwargs.get("column", "N/A"),
                    "success": result.success,
                    "critical": is_critical,
                    "result": result.result
                })
                
                if not result.success:
                    logger.warning(
                        f"Validation failed: {exp_type} on {kwargs.get('column')} "
                        f"(critical={is_critical})"
                    )
                
            except Exception as e:
                logger.error(f"Error running expectation {exp_type}: {str(e)}")
                validation_results.append({
                    "expectation": exp_type,
                    "column": kwargs.get("column", "N/A"),
                    "success": False,
                    "critical": True,
                    "error": str(e)
                })
        
        # Check critical failures
        critical_failures = [
            r for r in validation_results 
            if not r["success"] and r.get("critical", False)
        ]
        
        summary = {
            "total_validations": len(validation_results),
            "passed": sum(1 for r in validation_results if r["success"]),
            "failed": sum(1 for r in validation_results if not r["success"]),
            "critical_failures": len(critical_failures),
            "results": validation_results
        }
        
        logger.info(
            f"Validation complete: {summary['passed']}/{summary['total_validations']} passed, "
            f"{summary['critical_failures']} critical failures"
        )
        
        return summary
    
    def is_valid(self, validation_summary: Dict[str, Any]) -> bool:
        """
        Check if validation passed (no critical failures)
        
        Args:
            validation_summary: Results from validate_dataframe()
        
        Returns:
            True if no critical failures, False otherwise
        """
        return validation_summary["critical_failures"] == 0