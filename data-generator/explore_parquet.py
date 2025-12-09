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
            case 'size':
                table = pq.read_table(f'../data-generator/output/{file}')
                print(f"\nSize of {batch_num}:")
                print(f"Number of rows: {table.num_rows}")
                print(f"Number of columns: {table.num_columns}")
                print(f"Total size (bytes): {table.nbytes}")
            case 'shape':
                print(f"\nShape of {batch_num}:")
                print(df.shape)
            case _:
                print("Exiting summary.")
        print("\n")
        time.sleep(2)  # Pause for readability
        
summary_options = {
    1: 'head',
    2: 'columns',
    3: 'dtypes',
    4: 'info',
    5: 'index',
    6: 'describe',
    7: 'size',
    8: 'shape',
    9: 'exit'
}
        
i = int(input(f"Select summary type:\n1. Head\n2. Columns\n3. Data Types\n4. Info\n5. Index\n6. Describe\n7. Size\n8. Shape\n9. Exit\nEnter choice (1-9): \n"))
while i not in summary_options:
    i = int(input("Invalid choice. Please enter a number between 1 and 9: \n"))
        
summerize_parquet_files(kafka_batches, summary_type= summary_options[i])

