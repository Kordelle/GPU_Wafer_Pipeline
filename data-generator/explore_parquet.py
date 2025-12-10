import pandas as pd
import pyarrow.parquet as pq
import time
from pathlib import Path

kafka_batches = [file.name for file in list(Path('../data-generator/output/').glob('kafka_batch_*.parquet'))]

# Summarize parquet files
def summerize_parquet_files(kafka_batches, summary_type='head') -> None:
    for file in kafka_batches: 
        df = pd.read_parquet(f'../data-generator/output/{file}')
        batch_num = "Batch: " + file[21:27]
        file_count = kafka_batches.index(file) + 1
        print("="*50)
        print(f"Batch number {file_count} of {len(kafka_batches)}")
        # Generate summaries based on user choice
        match summary_type:
            case 'head':
                print(f"\nSummary of {batch_num}:")
                print(df.head())
            case 'sample':
                print(f"\nSample of {batch_num}:")
                print(df.sample())
            case 'columns':
                print(f"\nColumns of {batch_num}:")
                print(df.columns)
            case 'dtypes':
                print(f"\nData Types of {batch_num}:")
                print(df.dtypes)
            case 'info':
                print(f"\nInfo of {batch_num}:")
                print(df.info())
                print(f"Total Records: {len(df)}")
            case 'index':
                print(f"\nIndex of {batch_num}:")
                print(df.index)
            case 'describe':
                print(f"\nDescriptive Statistics of {batch_num}:")
                print(df.describe())
            case 'describe_all':
                print(f"\nDescriptive Statistics (All Columns):")
                print(df.describe(include='all'))
            case 'size':
                table = pq.read_table(f'../data-generator/output/{file}')
                print(f"\nSize of {batch_num}:")
                print(f"Number of rows: {table.num_rows}")
                print(f"Number of columns: {table.num_columns}")
                print(f"Total size (bytes): {table.nbytes}")
            case 'shape':
                print(f"\nShape of {batch_num}:")
                print(df.shape)
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
            case _:
                print("Exiting summary.")
        print("\n")
        time.sleep(2)  # Pause for readability
        
summary_options = {
    1: 'head',
    2: 'sample',
    3: 'columns',
    4: 'dtypes',
    5: 'info',
    6: 'index',
    7: 'describe',
    8: 'describe_all',
    9: 'size',
    10: 'shape',
    11: 'nulls',
    12: 'exit'
}
        
i = int(input(f"Select summary type:\n1. Head\n2. sample\n3. Columns\n4. Data Types\n5. Info\n6. Index\n7. Describe\n8. Describe All\n9. Size\n10. Shape\n11. Missing Data Analysis\n12. Exit\nEnter choice (1-12): \n"))
while i not in summary_options:
    i = int(input("Invalid choice. Please enter a number between 1 and 12: \n"))
        
summerize_parquet_files(kafka_batches, summary_type= summary_options[i])

