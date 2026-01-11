import pandas as pd
import pyarrow.parquet as pq
import time
from pathlib import Path

kafka_batches = [file.name for file in list(Path('./output/').glob('kafka_batch_*.parquet'))]

def summarize_parquet_files(kafka_batches, summary_type='head') -> None:
    """
    Explore Parquet files with various summary options
    
    Args:
        kafka_batches: List of Parquet file names
        summary_type: Type of summary to generate
    """
    if summary_type == 'exit':
        print("Exiting summary tool.")
        return
    
    for file in kafka_batches: 
        df = pd.read_parquet(f'./output/{file}')
        batch_num = "Batch: " + file[21:27]
        file_count = kafka_batches.index(file) + 1
        
        print("=" * 70)
        print(f"Batch {file_count}/{len(kafka_batches)} | {batch_num} | {len(df):,} records")
        print("=" * 70)
        
        match summary_type:
            case 'head':
                print(f"\nFirst 5 rows:")
                print(df.head())
            
            case 'tail':
                print(f"\nLast 5 rows:")
                print(df.tail())
            
            case 'sample':
                print(f"\nRandom sample (5 rows):")
                print(df.sample(min(5, len(df))))
            
            case 'dtypes':
                print(f"\nData Types:")
                print(df.dtypes)
            
            case 'info':
                print(f"\nDataFrame Info:")
                print(df.info())
                print(f"\nMemory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            
            case 'describe':
                print(f"\nDescriptive Statistics (Numeric Columns):")
                print(df.describe())
            
            case 'describe_all':
                print(f"\nDescriptive Statistics (All Columns):")
                print(df.describe(include='all'))
            
            case 'shape':
                print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
                print(f"Total elements: {df.size:,}")
                print(f"Columns: {', '.join(df.columns)}")
            
            case 'nulls':
                print(f"\nMissing Data Analysis:")
                null_counts = df.isnull().sum()
                null_pct = (null_counts / len(df) * 100).round(2)
                null_summary = pd.DataFrame({
                    'Null_Count': null_counts,
                    'Null_Percentage': null_pct
                })
                print(null_summary[null_summary['Null_Count'] > 0])
                if null_counts.sum() == 0:
                    print("No missing values detected!")
            
            case 'duplicates':
                dup_count = df.duplicated().sum()
                print(f"\nDuplicate Rows: {dup_count:,} ({dup_count/len(df)*100:.2f}%)")
                if 'wafer_id' in df.columns:
                    dup_wafers = df['wafer_id'].duplicated().sum()
                    print(f"Duplicate Wafer IDs: {dup_wafers:,}")
            
            case 'unique':
                print(f"\nUnique Values per Column:")
                unique_counts = df.nunique()
                print(unique_counts)
            
            case 'value_counts':
                print(f"\nFrequency Distribution:")
                if 'equipment_id' in df.columns:
                    print("\nEquipment ID Distribution:")
                    print(df['equipment_id'].value_counts())
                if 'is_anomaly' in df.columns:
                    print("\nAnomaly Distribution:")
                    print(df['is_anomaly'].value_counts())
            
            case 'anomalies':
                if 'is_anomaly' in df.columns:
                    anomaly_count = df['is_anomaly'].sum()
                    anomaly_rate = anomaly_count / len(df) * 100
                    print(f"\nAnomaly Analysis:")
                    print(f"  Total anomalies: {anomaly_count:,} ({anomaly_rate:.2f}%)")
                    print(f"  Normal records: {len(df) - anomaly_count:,}")
                    
                    if anomaly_count > 0:
                        print("\nAnomaly Statistics:")
                        anomaly_df = df[df['is_anomaly'] == True]
                        print(anomaly_df[['temperature_c', 'pressure_torr', 'yield_rate', 'defect_count']].describe())
                else:
                    print("No 'is_anomaly' column found")
            
            case 'corr':
                print(f"\nCorrelation Matrix (Numeric Columns):")
                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                print(df[numeric_cols].corr().round(3))
            
            case 'memory':
                print(f"\nMemory Usage by Column:")
                mem_usage = df.memory_usage(deep=True)
                mem_mb = mem_usage / 1024**2
                mem_summary = pd.DataFrame({
                    'Column': mem_usage.index,
                    'Memory_MB': mem_mb.round(2)
                }).sort_values('Memory_MB', ascending=False)
                print(mem_summary)
                print(f"\nTotal Memory: {mem_mb.sum():.2f} MB")
            
            case _:
                print(f"Invalid summary type: {summary_type}")
                return
        
        print("\n")
        
        # Only pause between batches, not at the end
        if file_count < len(kafka_batches):
            time.sleep(1)

# Menu options
summary_options = {
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
print(f"Found {len(kafka_batches)} Parquet files\n")

# Display menu
print("Select summary type:")
for key, (_, description) in sorted(summary_options.items()):
    print(f"  {key:2d}. {description}")

# Get user input
while True:
    try:
        choice = int(input(f"\nEnter choice (0-{max(summary_options.keys())}): "))
        if choice in summary_options:
            break
        print(f"Invalid choice. Please enter a number between 0 and {max(summary_options.keys())}.")
    except ValueError:
        print("Invalid input. Please enter a number.")

summary_type, description = summary_options[choice]

if summary_type == 'exit':
    print("\nExiting tool.")
else:
    print(f"\nGenerating '{description}' summary...\n")
    summarize_parquet_files(kafka_batches, summary_type=summary_type)
    print("=" * 70)
    print("SUMMARY COMPLETE")
    print("=" * 70)