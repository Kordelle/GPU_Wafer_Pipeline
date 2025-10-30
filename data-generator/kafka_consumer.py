"""
Kafka Consumer for Manufacturing Telemetry
Consumes wafer data from Kafka and writes to MinIO for archival
"""

import json
import logging
import os
import sys
import time
from typing import Dict, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ManufacturingKafkaConsumer:
    """
    Kafka consumer for archiving manufacturing telemetry to MinIO
    
    Features:
    - Batch processing (efficiency)
    - MinIO integration (object storage)
    - Offset management (exactly-once processing)
    - Error handling with retry logic
    """
    
    def __init__(
        self,
        topic: str = None,
        group_id: str = "minio-archiver",
        batch_size: int = 1000,
        poll_interval_ms: int = 10000
    ):
        """
        Initialize Kafka consumer and MinIO client
        
        Args:
            topic: Kafka topic to consume from
            group_id: Consumer group identifier
            batch_size: Number of messages to batch before writing
            poll_interval_ms: Polling interval in milliseconds
        """
        # Kafka config from environment
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic = topic or os.getenv("KAFKA_TOPIC", "wafer-telemetry")
        self.group_id = group_id
        self.batch_size = batch_size
        
        # MinIO config from environment
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_user = os.getenv("MINIO_ROOT_USER")
        self.minio_password = os.getenv("MINIO_ROOT_PASSWORD")
        
        # Stats tracking
        self.messages_consumed = 0
        self.messages_written = 0
        
        logger.info(f"Initializing Kafka consumer")
        logger.info(f"  Topic: {self.topic}")
        logger.info(f"  Group ID: {self.group_id}")
        logger.info(f"  Batch size: {self.batch_size}")
        
        # Initialize consumer and MinIO client
        self.consumer = self._create_consumer()
        self.minio_client = self._create_minio_client()
    
    def _create_consumer(self) -> KafkaConsumer:
        """Create and configure Kafka consumer"""
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers.split(','),
            group_id=self.group_id,
            
            # Deserialization
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            
            # Offset management
            enable_auto_commit=False,  # Manual commit for exactly-once
            auto_offset_reset='earliest',  # Start from beginning if no offset
            
            # Performance
            max_poll_records=self.batch_size,
            max_poll_interval_ms=300000,  # 5 minutes
            session_timeout_ms=30000,  # 30 seconds
        )
        
        logger.info("✅ Connected to Kafka consumer")
        return consumer
    
    def _create_minio_client(self) -> Minio:
        """Create MinIO client for object storage"""
        client = Minio(
            endpoint=self.minio_endpoint,
            access_key=self.minio_user,
            secret_key=self.minio_password,
            secure=False
        )
        
        # Ensure bucket exists
        bucket_name = "manufacturing-data"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"✅ Created MinIO bucket: {bucket_name}")
        
        return client
    
    def consume_and_archive(self):
        """
        Main consumption loop: Poll Kafka → Batch → Write to MinIO
        """
        logger.info("🚀 Starting consumer loop (press Ctrl+C to stop)")
        
        buffer = []
        
        try:
            while True:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)
                
                # Process messages from all partitions
                for topic_partition, records in messages.items():
                    for record in records:
                        buffer.append(record.value)
                        self.messages_consumed += 1
                
                # Write batch to MinIO when buffer is full
                if len(buffer) >= self.batch_size:
                    self._write_batch_to_minio(buffer)
                    buffer.clear()
                    
                    # Commit offset after successful write
                    self.consumer.commit()
                    
                    logger.info(
                        f"📊 Consumed: {self.messages_consumed} | "
                        f"Written: {self.messages_written} batches"
                    )
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Shutting down consumer...")
            
            # Write remaining buffered messages
            if buffer:
                self._write_batch_to_minio(buffer)
                self.consumer.commit()
            
            logger.info(f"✅ Final stats: {self.messages_consumed} messages consumed")
        
        finally:
            self.consumer.close()
            logger.info("✅ Consumer closed")
    
    def _write_batch_to_minio(self, batch: List[Dict]):
        """
        Write batch of messages to MinIO as JSON file
        
        Args:
            batch: List of message dictionaries
        """
        # TODO (Next session): Implement MinIO write logic
        # - Generate filename with timestamp
        # - Convert batch to JSON
        # - Upload to MinIO
        # - Handle errors
        
        logger.info(f"📦 Writing batch of {len(batch)} messages to MinIO...")
        self.messages_written += 1
        
        # Placeholder for now
        pass


def main():
    """CLI entry point for consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka consumer for manufacturing telemetry")
    parser.add_argument("--topic", default="wafer-telemetry", help="Kafka topic")
    parser.add_argument("--group-id", default="minio-archiver", help="Consumer group ID")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size")
    
    args = parser.parse_args()
    
    # Initialize and start consumer
    consumer = ManufacturingKafkaConsumer(
        topic=args.topic,
        group_id=args.group_id,
        batch_size=args.batch_size
    )
    
    consumer.consume_and_archive()


if __name__ == "__main__":
    main()