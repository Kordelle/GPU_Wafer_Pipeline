# ===================================
# Enhanced Data Generation Script
# 
#  1. Anomaly Injection (simulate equipment failures)
#  2. Batch processing (handle 10M+ records)
#  3. Streaming mode (continuous generation)
#  4. CLI arguments (configurable parameters)
#  5. Logging (production observability)
# ===================================

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from kafka_producer import ManufacturingKafkaProducer

import numpy as np
import pandas as pd

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/output/generator.log')
    ]
)
logger = logging.getLogger(__name__)


class ManufacturingDataGenerator:
    """
    Generates synthetic semiconductor manufacturing telemetry data
    
    Features:
    - Realistic statistical distributions
    - Configurable anomaly injection
    - Batch processing for scale
    - Streaming mode for real-time simulation
    """
    
    def __init__(
        self,
        equipment_ids: List[str] = None,
        temp_mean: float = 350.0,
        temp_std: float = 5.0,
        pressure_mean: float = 10.0,
        pressure_std: float = 0.5,
        defect_lambda: float = 2.0,
        anomaly_rate: float = 0.01,
        random_seed: int = 42
    ):
        """
        Initialize generator with configurable parameters
        
        Args:
            equipment_ids: List of equipment IDs (default: EQ001-EQ004)
            temp_mean: Target temperature in Celsius
            temp_std: Temperature standard deviation
            pressure_mean: Target pressure in Torr
            pressure_std: Pressure standard deviation
            defect_lambda: Average defects per wafer (Poisson)
            anomaly_rate: Fraction of records that are anomalous
            random_seed: Random seed for reproducibility
        """
        self.equipment_ids = equipment_ids or ['EQ001', 'EQ002', 'EQ003', 'EQ004']
        self.temp_mean = temp_mean
        self.temp_std = temp_std
        self.pressure_mean = pressure_mean
        self.pressure_std = pressure_std
        self.defect_lambda = defect_lambda
        self.anomaly_rate = anomaly_rate
        
        np.random.seed(random_seed)
        
        logger.info(f"Initialized generator with {len(self.equipment_ids)} equipment IDs")
        logger.info(f"Temperature: μ={temp_mean}°C, σ={temp_std}°C")
        logger.info(f"Anomaly rate: {anomaly_rate*100:.1f}%")
    
    def generate_batch(
        self,
        num_records: int,
        start_id: int = 0,
        inject_anomalies: bool = True
    ) -> pd.DataFrame:
        """
        Generate a batch of manufacturing records
        
        Args:
            num_records: Number of records to generate
            start_id: Starting wafer ID
            inject_anomalies: Whether to inject anomalies
            
        Returns:
            DataFrame with manufacturing telemetry
        """
        logger.debug(f"Generating batch: {num_records} records starting at ID {start_id}")
        
        # Generate timestamps (descending - newest first)
        base_time = datetime.now()
        timestamps = [base_time - timedelta(seconds=x) for x in range(num_records)]
        
        # Generate wafer IDs
        wafer_ids = [f'W{str(i + start_id).zfill(8)}' for i in range(num_records)]
        
        # Randomly assign equipment
        equipment = np.random.choice(self.equipment_ids, num_records)
        
        # Generate process parameters (normal distributions)
        temperature = np.random.normal(self.temp_mean, self.temp_std, num_records)
        pressure = np.random.normal(self.pressure_mean, self.pressure_std, num_records)
        
        # Generate yield rate (beta distribution - more realistic than uniform)
        # Beta(α=20, β=2) gives high values with slight left skew
        yield_rate = np.random.beta(20, 2, num_records)
        yield_rate = 0.80 + (yield_rate * 0.19)  # Scale to [0.80, 0.99]
        
        # Generate defect count (Poisson distribution)
        defect_count = np.random.poisson(self.defect_lambda, num_records)
        
        # Create DataFrame
        df = pd.DataFrame({
            'timestamp': timestamps,
            'wafer_id': wafer_ids,
            'equipment_id': equipment,
            'temperature_c': temperature,
            'pressure_torr': pressure,
            'yield_rate': yield_rate,
            'defect_count': defect_count,
            'is_anomaly': False  # Mark anomalies
        })
        
        # Inject anomalies
        if inject_anomalies and self.anomaly_rate > 0:
            df = self._inject_anomalies(df)
        
        return df
    
    def _inject_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Inject realistic anomalies to simulate equipment failures
        
        Anomaly types:
        1. Temperature spikes (overheating)
        2. Temperature drops (cooling failure)
        3. Pressure fluctuations (vacuum leak)
        4. Defect bursts (contamination event)
        """
        num_anomalies = int(len(df) * self.anomaly_rate)
        
        if num_anomalies == 0:
            return df
        
        anomaly_indices = np.random.choice(len(df), num_anomalies, replace=False)
        
        for idx in anomaly_indices:
            anomaly_type = np.random.choice([
                'temp_spike', 'temp_drop', 'pressure_fluctuation', 'defect_burst'
            ])
            
            if anomaly_type == 'temp_spike':
                # Overheating: +15-25°C above normal
                df.loc[idx, 'temperature_c'] = self.temp_mean + np.random.uniform(15, 25)
                
            elif anomaly_type == 'temp_drop':
                # Cooling failure: -15-25°C below normal
                df.loc[idx, 'temperature_c'] = self.temp_mean - np.random.uniform(15, 25)
                
            elif anomaly_type == 'pressure_fluctuation':
                # Vacuum leak: ±3-5 Torr
                df.loc[idx, 'pressure_torr'] = self.pressure_mean + np.random.choice([-1, 1]) * np.random.uniform(3, 5)
                
            elif anomaly_type == 'defect_burst':
                # Contamination: 10-30 defects
                df.loc[idx, 'defect_count'] = np.random.randint(10, 31)
                df.loc[idx, 'yield_rate'] = np.random.uniform(0.50, 0.70)  # Low yield
            
            df.loc[idx, 'is_anomaly'] = True
        
        logger.info(f"Injected {num_anomalies} anomalies ({self.anomaly_rate*100:.1f}%)")
        return df
    
    def generate_large_dataset(
        self,
        total_records: int,
        batch_size: int = 1_000_000,
        output_path: str = '/output/wafer_data.json'
    ):
        """
        Generate large datasets using batching to manage memory
        
        Args:
            total_records: Total number of records to generate
            batch_size: Records per batch
            output_path: Where to save the data
        """
        logger.info(f"Starting large dataset generation: {total_records:,} records")
        logger.info(f"Batch size: {batch_size:,} | Batches: {total_records // batch_size}")
        
        start_time = time.time()
        
        for batch_num, start_id in enumerate(range(0, total_records, batch_size)):
            records_in_batch = min(batch_size, total_records - start_id)
            
            # Generate batch
            df = self.generate_batch(records_in_batch, start_id=start_id)
            
            # Write to file (append mode after first batch)
            mode = 'w' if batch_num == 0 else 'a'
            df.to_json(output_path, orient='records', lines=True, mode=mode)
            
            # Progress logging
            elapsed = time.time() - start_time
            progress = ((batch_num + 1) * batch_size) / total_records * 100
            rate = (start_id + records_in_batch) / elapsed
            
            logger.info(
                f"Batch {batch_num + 1}: {records_in_batch:,} records | "
                f"Progress: {progress:.1f}% | Rate: {rate:,.0f} rec/sec"
            )
            
            # Free memory
            del df
        
        total_time = time.time() - start_time
        logger.info(f"Generated {total_records:,} records in {total_time:.2f}s")
        logger.info(f"   Average rate: {total_records/total_time:,.0f} records/sec")
        
        # File size
        file_size = Path(output_path).stat().st_size
        logger.info(f"   File size: {file_size / 1024 / 1024:.2f} MB")
    
    def stream_data(
        self,
        interval_seconds: float = 1.0,
        output_path: str = '/output/wafer_stream.json'
    ):
        """
        Stream data continuously to simulate real-time telemetry
        
        Args:
            interval_seconds: Time between records
            output_path: Where to append streaming data
        """
        logger.info(f"Starting streaming mode: 1 record every {interval_seconds}s")
        logger.info("Press Ctrl+C to stop")
        
        wafer_id = 0
        
        try:
            while True:
                # Generate single record
                df = self.generate_batch(1, start_id=wafer_id)
                
                # Append to file
                df.to_json(output_path, orient='records', lines=True, mode='a')
                
                logger.info(f"📡 Streamed wafer {df.iloc[0]['wafer_id']} | "
                           f"Temp: {df.iloc[0]['temperature_c']:.1f}°C | "
                           f"Yield: {df.iloc[0]['yield_rate']:.2%} | "
                           f"Defects: {df.iloc[0]['defect_count']}")
                
                wafer_id += 1
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info(f"\nStreaming stopped after {wafer_id} wafers")
    
    def stream_to_kafka(
        self,
        interval_seconds: float = 1.0,
        topic: str = None
    ):
                """
                Stream data continuously to Kafka (real-time telemetry simulation)
                
                Args:
                    interval_seconds: Time between records
                    topic: Kafka topic (uses env var if not provided)
                """
                logger.info(f"Starting Kafka streaming mode: 1 record every {interval_seconds}s")
                logger.info("Press Ctrl+C to stop")
                
                # Initialize Kafka producer
                producer = ManufacturingKafkaProducer(topic=topic)
                
                wafer_id = 0
                
                try:
                    while True:
                        # Generate single record
                        df = self.generate_batch(1, start_id=wafer_id)
                        record = df.iloc[0].to_dict()
                        
                        # Convert timestamp to ISO format (JSON serializable)
                        record['timestamp'] = record['timestamp'].isoformat()
                        
                        # Send to Kafka
                        producer.send_wafer_telemetry(record)
                        
                        # Log to console (for monitoring)
                        logger.info(
                            f"📡 Streamed {record['wafer_id']} | "
                            f"Equipment: {record['equipment_id']} | "
                            f"Temp: {record['temperature_c']:.1f}°C | "
                            f"Yield: {record['yield_rate']:.2%} | "
                            f"Defects: {record['defect_count']} | "
                            f"Anomaly: {record['is_anomaly']}"
                        )
                        
                        wafer_id += 1
                        time.sleep(interval_seconds)
                        
                except KeyboardInterrupt:
                    logger.info(f"\nStreaming stopped after {wafer_id} wafers")
                    stats = producer.get_stats()
                    logger.info(f"📊 Final stats: {stats['messages_sent']} sent, {stats['messages_failed']} failed")
                finally:
                    producer.close()


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Generate synthetic semiconductor manufacturing data'
    )
    
    parser.add_argument(
        '--mode',
        choices=['batch', 'large', 'stream', 'kafka'],
        default='batch',
        help='Generation mode'
    )
    parser.add_argument(
        '--records',
        type=int,
        default=1000,
        help='Number of records (batch/large mode)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1_000_000,
        help='Batch size for large mode'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Seconds between records (stream/kafka mode)'
    )
    parser.add_argument(
        '--output',
        default='/output/wafer_data.json',
        help='Output file path (batch/large/stream mode)'
    )
    parser.add_argument(
        '--topic',
        default=None,
        help='Kafka topic (kafka mode, uses env var if not provided)'
    )
    parser.add_argument(
        '--anomaly-rate',
        type=float,
        default=0.01,
        help='Fraction of anomalous records (0.01 = 1%%)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility'
    )
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = ManufacturingDataGenerator(
        anomaly_rate=args.anomaly_rate,
        random_seed=args.seed
    )
    
    # Execute based on mode
    if args.mode == 'batch':
        logger.info(f"Batch mode: Generating {args.records:,} records")
        df = generator.generate_batch(args.records)
        df.to_json(args.output, orient='records', lines=True)
        logger.info(f"Saved to {args.output}")
        
        # Print sample
        print("\n📊 Sample data:")
        print(df.head())
        print(f"\nAnomalies: {df['is_anomaly'].sum()} ({df['is_anomaly'].mean()*100:.2f}%)")
        
    elif args.mode == 'large':
        generator.generate_large_dataset(
            total_records=args.records,
            batch_size=args.batch_size,
            output_path=args.output
        )
        
    elif args.mode == 'stream':
        generator.stream_data(
            interval_seconds=args.interval,
            output_path=args.output
        )
    
    elif args.mode == 'kafka':  
        generator.stream_to_kafka(
            interval_seconds=args.interval,
            topic=args.topic
        )


if __name__ == "__main__":
    main()