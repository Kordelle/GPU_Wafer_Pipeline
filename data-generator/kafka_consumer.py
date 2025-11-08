import json
import logging
from typing import Dict, List
from datetime import datetime
from io import BytesIO

from kafka import KafkaConsumer
from minio import Minio
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ManufacturingKafkaConsumer:
    """
    Kafka consumer that reads wafer telemetry and archives to MinIO
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "kafka:29092",
        topic: str = "wafer-telemetry",
        group_id: str = "manufacturing-consumer-group",
        minio_endpoint: str = "minio:9000",
        minio_access_key: str = "admin",
        minio_secret_key: str = "password123",
        batch_size: int = 100
    ):
        """
        Initialize Kafka consumer and MinIO client
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.buffer = []
        self.messages_written = 0
        self.running = True
        
        # Initialize Kafka consumer
        logger.info(f"🔌 Connecting to Kafka: {bootstrap_servers}")
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info(f"Connected to Kafka topic: {topic}")
        
        # Initialize MinIO client
        logger.info(f"Connecting to MinIO: {minio_endpoint}")
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        # Ensure bucket structure exists
        self._ensure_bucket_structure()
        
        logger.info(f"Consumer initialized (batch_size={batch_size})")
    
    def _ensure_bucket_structure(self):
        """
        Ensure MinIO bucket exists
        """
        bucket_name = "manufacturing-data"
        
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                logger.info(f"Creating bucket: {bucket_name}")
                self.minio_client.make_bucket(bucket_name)
            else:
                logger.info(f"Bucket exists: {bucket_name}")
            
            logger.info(f"Ready to write to: s3://{bucket_name}/bronze/wafer-telemetry/")
            
        except Exception as e:
            logger.error(f"Failed to setup bucket: {e}")
            raise
    
    def _write_batch_to_minio(self, batch: List[Dict]):
        """
        Write batch of messages to MinIO as Parquet file
        """
        if not batch:
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(batch)
            
            # Add timestamp processing
            df['timestamp'] = pd.to_datetime(df['timestamp'].dt.round('ms'))
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"kafka_batch_{timestamp}_{len(batch)}_records.parquet"
            
            # Write Parquet to memory buffer
            parquet_buffer = BytesIO()
            df.to_parquet(
                parquet_buffer,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            parquet_buffer.seek(0)
            
            # Upload to MinIO with date partitioning
            bucket_name = "manufacturing-data"
            object_name = f"bronze/wafer-telemetry/{date_str}/{filename}"
            
            self.minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            self.messages_written += 1
            
            logger.info(
                f"Wrote batch to MinIO: s3://{bucket_name}/{object_name} "
                f"({len(batch)} messages, {parquet_buffer.getbuffer().nbytes / 1024:.1f} KB)"
            )
            
        except Exception as e:
            logger.error(f"Failed to write batch to MinIO: {e}")
            logger.exception(e)  # Print full stack trace
            raise
    
    def consume_and_archive(self):
        """
        Consume messages from Kafka and archive to MinIO in batches
        """
        logger.info(f"Starting consumer loop...")
        logger.info(f"Batching {self.batch_size} messages before writing to MinIO")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                # Add message to buffer
                self.buffer.append(message.value)
                
                # Write batch when buffer is full
                if len(self.buffer) >= self.batch_size:
                    logger.info(f"Received batch of {len(self.buffer)} messages")
                    self._write_batch_to_minio(self.buffer)
                    
                    # Commit offset after successful write
                    self.consumer.commit()
                    
                    # Clear buffer
                    self.buffer.clear()
                    
                    logger.info(f"Total batches written: {self.messages_written}")
        
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            logger.exception(e)
        finally:
            self.shutdown()
    
    def shutdown(self):
        """
        Graceful shutdown - write remaining buffer and close connections
        """
        logger.info("Shutting down consumer...")
        
        # Write remaining buffered messages
        if self.buffer:
            logger.info(f"Writing final batch of {len(self.buffer)} messages")
            self._write_batch_to_minio(self.buffer)
            self.consumer.commit()
        
        # Close consumer
        self.consumer.close()
        logger.info(f"Consumer shutdown complete. Total batches written: {self.messages_written}")


if __name__ == "__main__":
    import os
    
    # Get config from environment variables
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "wafer-telemetry")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    
    logger.info("=" * 60)
    logger.info("KAFKA CONSUMER - Manufacturing Telemetry Archive")
    logger.info("=" * 60)
    logger.info(f"Kafka: {bootstrap_servers}")
    logger.info(f"Topic: {topic}")
    logger.info(f"MinIO: {minio_endpoint}")
    logger.info("=" * 60)
    
    # Create and start consumer
    consumer = ManufacturingKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_user,
        minio_secret_key=minio_password,
        batch_size=100
    )
    
    consumer.consume_and_archive()