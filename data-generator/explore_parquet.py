"""
Parquet File Exploration Tool for Manufacturing Telemetry
Provides 16 analysis options for data quality validation and profiling
"""

import pandas as pd
import pyarrow.parquet as pq
import time
from pathlib import Path
from typing import Dict, Tuple, Callable

# ============================================================================
# ANALYSIS FUNCTIONS (Testable, Reusable)
# ============================================================================

def analyze_head(df: pd.DataFrame) -> str:
    """Return first 5 rows as formatted string"""
    return f"\nFirst 5 rows:\n{df.head()}"

def analyze_tail(df: pd.DataFrame) -> str:
    """Return last 5 rows as formatted string"""
    return f"\nLast 5 rows:\n{df.tail()}"

def analyze_sample(df: pd.DataFrame) -> str:
    """Return random sample of 5 rows"""
    return f"\nRandom sample (5 rows):\n{df.sample(min(5, len(df)))}"

def analyze_dtypes(df: pd.DataFrame) -> str:
    """Return data types for all columns"""
    return f"\nData Types:\n{df.dtypes}"

def analyze_info(df: pd.DataFrame) -> str:
    """Return DataFrame info with memory usage"""
    import io
    buffer = io.StringIO()
    df.info(buf=buffer)
    info_str = buffer.getvalue()
    memory_mb = df.memory_usage(deep=True).sum() / 1024**2
    return f"\nDataFrame Info:\n{info_str}\nMemory Usage: {memory_mb:.2f} MB"

def analyze_describe(df: pd.DataFrame) -> str:
    """Return descriptive statistics for numeric columns"""
    return f"\nDescriptive Statistics (Numeric Columns):\n{df.describe()}"

def analyze_describe_all(df: pd.DataFrame) -> str:
    """Return descriptive statistics for all columns"""
    return f"\nDescriptive Statistics (All Columns):\n{df.describe(include='all')}"

def analyze_shape(df: pd.DataFrame) -> str:
    """Return shape and column information"""
    rows, cols = df.shape
    return (f"\nShape: {rows:,} rows × {cols} columns\n"
            f"Total elements: {df.size:,}\n"
            f"Columns: {', '.join(df.columns)}")

def analyze_nulls(df: pd.DataFrame) -> str:
    """Analyze missing data per column"""
    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df) * 100).round(2)
    null_summary = pd.DataFrame({
        'Null_Count': null_counts,
        'Null_Percentage': null_pct
    })
    
    if null_counts.sum() == 0:
        return "\nMissing Data Analysis:\nNo missing values detected!"
    
    return f"\nMissing Data Analysis:\n{null_summary[null_summary['Null_Count'] > 0]}"

def analyze_duplicates(df: pd.DataFrame) -> str:
    """Check for duplicate rows and wafer IDs"""
    dup_count = df.duplicated().sum()
    result = f"\nDuplicate Rows: {dup_count:,} ({dup_count/len(df)*100:.2f}%)"
    
    if 'wafer_id' in df.columns:
        dup_wafers = df['wafer_id'].duplicated().sum()
        result += f"\nDuplicate Wafer IDs: {dup_wafers:,}"
    
    return result

def analyze_unique(df: pd.DataFrame) -> str:
    """Count unique values per column"""
    return f"\nUnique Values per Column:\n{df.nunique()}"

def analyze_value_counts(df: pd.DataFrame) -> str:
    """Show frequency distributions for categorical columns"""
    result = "\nFrequency Distribution:"
    
    if 'equipment_id' in df.columns:
        result += f"\n\nEquipment ID Distribution:\n{df['equipment_id'].value_counts()}"
    
    if 'is_anomaly' in df.columns:
        result += f"\n\nAnomaly Distribution:\n{df['is_anomaly'].value_counts()}"
    
    return result

def analyze_anomalies(df: pd.DataFrame) -> str:
    """Detailed anomaly analysis for manufacturing data"""
    if 'is_anomaly' not in df.columns:
        return "\nNo 'is_anomaly' column found"
    
    anomaly_count = df['is_anomaly'].sum()
    anomaly_rate = anomaly_count / len(df) * 100
    
    result = (f"\nAnomaly Analysis:\n"
              f"  Total anomalies: {anomaly_count:,} ({anomaly_rate:.2f}%)\n"
              f"  Normal records: {len(df) - anomaly_count:,}")
    
    if anomaly_count > 0:
        anomaly_df = df[df['is_anomaly'] == True]
        stats_cols = ['temperature_c', 'pressure_torr', 'yield_rate', 'defect_count']
        result += f"\n\nAnomaly Statistics:\n{anomaly_df[stats_cols].describe()}"
    
    return result

def analyze_correlation(df: pd.DataFrame) -> str:
    """Calculate correlation matrix for numeric columns"""
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    return f"\nCorrelation Matrix (Numeric Columns):\n{df[numeric_cols].corr().round(3)}"

def analyze_memory(df: pd.DataFrame) -> str:
    """Analyze memory usage by column"""
    mem_usage = df.memory_usage(deep=True)
    mem_mb = mem_usage / 1024**2
    mem_summary = pd.DataFrame({
        'Column': mem_usage.index,
        'Memory_MB': mem_mb.round(2)
    }).sort_values('Memory_MB', ascending=False)
    
    return f"\nMemory Usage by Column:\n{mem_summary}\n\nTotal Memory: {mem_mb.sum():.2f} MB"

# ============================================================================
# ANALYSIS REGISTRY (Maps menu choices to functions)
# ============================================================================

ANALYSIS_FUNCTIONS: Dict[str, Callable[[pd.DataFrame], str]] = {
    'head': analyze_head,
    'tail': analyze_tail,
    'sample': analyze_sample,
    'dtypes': analyze_dtypes,
    'info': analyze_info,
    'describe': analyze_describe,
    'describe_all': analyze_describe_all,
    'shape': analyze_shape,
    'nulls': analyze_nulls,
    'duplicates': analyze_duplicates,
    'unique': analyze_unique,
    'value_counts': analyze_value_counts,
    'anomalies': analyze_anomalies,
    'corr': analyze_correlation,
    'memory': analyze_memory
}

# ============================================================================
# BATCH PROCESSING LOGIC
# ============================================================================

def process_parquet_batches(batch_files: list, analysis_type: str) -> None:
    """
    Process multiple Parquet files with specified analysis
    
    Args:
        batch_files: List of Parquet file names
        analysis_type: Type of analysis to perform (key from ANALYSIS_FUNCTIONS)
    """
    if analysis_type == 'exit':
        print("Exiting tool.")
        return
    
    if analysis_type not in ANALYSIS_FUNCTIONS:
        print(f"Invalid analysis type: {analysis_type}")
        return
    
    analysis_func = ANALYSIS_FUNCTIONS[analysis_type]
    
    for idx, file in enumerate(batch_files, start=1):
        df = pd.read_parquet(f'./output/{file}')
        batch_num = file[21:27]  # Extract batch number from filename
        
        # Print batch header
        print("=" * 70)
        print(f"Batch {idx}/{len(batch_files)} | Batch: {batch_num} | {len(df):,} records")
        print("=" * 70)
        
        # Run analysis and print results
        result = analysis_func(df)
        print(result)
        print()
        
        # Pause between batches (not after last batch)
        if idx < len(batch_files):
            time.sleep(1)

# ============================================================================
# MENU INTERFACE
# ============================================================================

def display_menu_and_get_choice() -> Tuple[str, str]:
    """
    Display interactive menu and get user's analysis choice
    
    Returns:
        Tuple of (analysis_type, description)
    """
    menu_options = {
        1: ('head', 'First 5 rows'),
        2: ('tail', 'Last 5 rows'),
        3: ('sample', 'Random sample'),
        4: ('dtypes', 'Data types'),
        5: ('info', 'DataFrame info + memory'),
        6: ('describe', 'Statistics (numeric only)'),
        7: ('describe_all', 'Statistics (all columns)'),
        8: ('shape', 'Shape + column names'),
        9: ('nulls', 'Missing data analysis'),
        10: ('duplicates', 'Duplicate row check'),
        11: ('unique', 'Unique values per column'),
        12: ('value_counts', 'Frequency distributions'),
        13: ('anomalies', 'Anomaly analysis'),
        14: ('corr', 'Correlation matrix'),
        15: ('memory', 'Memory usage by column'),
        0: ('exit', 'Exit tool')
    }
    
    print("\n" + "=" * 70)
    print("PARQUET FILE EXPLORATION TOOL")
    print("=" * 70)
    
    # Display menu
    print("\nSelect analysis type:")
    for key, (_, description) in sorted(menu_options.items()):
        print(f"  {key:2d}. {description}")
    
    # Get validated user input
    while True:
        try:
            choice = int(input(f"\nEnter choice (0-{max(menu_options.keys())}): "))
            if choice in menu_options:
                return menu_options[choice]
            print(f"Invalid choice. Please enter 0-{max(menu_options.keys())}.")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\n\nExiting tool.")
            return ('exit', 'Exit')

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    # Find all Parquet batch files
    batch_files = [
        file.name for file in Path('./output/').glob('kafka_batch_*.parquet')
    ]
    
    if not batch_files:
        print("No Parquet batch files found in ./output/")
        exit(1)
    
    print(f"Found {len(batch_files)} Parquet files")
    
    # Get user's analysis choice
    analysis_type, description = display_menu_and_get_choice()
    
    if analysis_type == 'exit':
        exit(0)
    
    # Process all batches with selected analysis
    print(f"\nGenerating '{description}' summary...\n")
    process_parquet_batches(batch_files, analysis_type)
    
    print("=" * 70)
    print("SUMMARY COMPLETE")
    print("=" * 70)