# ===================================
# Basic Data Generation Script
# Simulating semiconductor manufacturing data
# ===================================


import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

def generate_wafer_data(num_records=1000):
    """Generate synthetic semiconductor manufacturing data"""

    np.random.seed(42)

    data = {
        'timestamp': [datetime.now() - timedelta(seconds=x) for x in range(num_records)],
        'wafer_id': [f'W{str(i).zfill(8)}' for i in range(num_records)],
        'equipment_id': np.random.choice(['EQ001', 'EQ002', 'EQ003', 'EQ004'], num_records), 
        'temperature_c': np.random.normal(350, 5, num_records), # Fabrication temp
        'pressure_torr': np.random.normal(10, 0.5, num_records), # Chamber pressure
        'yield_rate': np.random.uniform(0.85, 0.99, num_records), # Defect-free chips
        'defect_count': np.random.poisson(2, num_records) # Defects per wafer
    }

    df = pd.DataFrame(data)

    # Save to JSON (Kafka-like format)
    output_path = '/output/wafer_data.json'
    df.to_json(output_path, orient='records', lines=True)

    print(f"Generated {num_records} records")
    print(f"Sample data:\n{df.head()}")
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    generate_wafer_data(1000)
    
    
