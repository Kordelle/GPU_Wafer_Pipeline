import pandas as pd
import pyarrow.parquet as pq
import time

kafka_batches = ['kafka_batch_20251106_035141_100_records.parquet', 'kafka_batch_20251106_035151_100_records.parquet',
                 'kafka_batch_20251106_035201_100_records.parquet', 'kafka_batch_20251106_035211_100_records.parquet',
                 'kafka_batch_20251106_035221_100_records.parquet']



def summerize_parquet_files(kafka_batches, summary_type='head') -> None:
    for file in kafka_batches: 
        df = pd.read_parquet(f'../data-generator/output/{file}')
        batch_num = "Batch: " + file[21:27]
        file_count = kafka_batches.index(file) + 1
        print("="*50)
        print(f"Batch number {file_count} of {len(kafka_batches)}")
        if summary_type == 'head':
            print(f"\nSummary of {batch_num}:")
            print(df.head())
        elif summary_type == 'dtypes':
            print(f"\nData Types of {batch_num}:")
            print(df.dtypes)
        elif summary_type == 'info':
            print(f"\nInfo of {batch_num}:")
            print(df.info())
            print(f"Total Records: {len(df)}")
        elif summary_type == 'describe':
            print(f"\nDescriptive Statistics of {batch_num}:")
            print(df.describe())
        print("\n")
        time.sleep(2)  # Pause for readability
            
        
summary_options = {
    1: 'head',
    2: 'dtypes',
    3: 'info',
    4: 'describe'
}
        
i = int(input(f"Select summary type:\n1. Head\n2. Data Types\n3. Info\n4. Describe\nEnter choice (1-4): \n"))

while i not in summary_options:
    i = int(input("Invalid choice. Please enter a number between 1 and 4: \n"))
        
summerize_parquet_files(kafka_batches, summary_type= summary_options[i])

