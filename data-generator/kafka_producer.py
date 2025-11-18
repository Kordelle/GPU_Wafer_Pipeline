"""
Kafka Producer for Manufacturing Telemetry Streaming
Produces wafer manufacturing data to Kafka topics with partitioning by equipment_id
Ensures ordered processing of messages per equipment while allowing parallelism across different equipment.
"""
import json
import logging
import os
import sys
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ManufacturingKafkaProducer:
    """
    Kafka producer for streaming manufacturing telemetry data
    
    Features:
    - Partitioning by equipment_id (maintains ordering per equipment)
    - JSON serialization with metadata
    - Retry logic with exponential backoff
    - Delivery confirmation callbacks
    """
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        max_retries: int = 3,
        retry_backoff_ms: int = 1000
    ):
        """
        Initialize Kafka producer with configuration
        
        Args:
            bootstrap_servers: Kafka broker addresses (comma-separated)
            topic: Default Kafka topic name
            max_retries: Number of retry attempts for failed sends
            retry_backoff_ms: Backoff time between retries (milliseconds)
        """
        # Read from environment variables (from docker-compose.yml)
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:29092"
        )
        self.topic = topic or os.getenv(
            "KAFKA_TOPIC",
            "wafer-telemetry"
        )
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        
        # Stats tracking
        self.messages_sent = 0
        self.messages_failed = 0
        
        # Initialize producer
        self.producer = self._create_producer()
        
        logger.info(f"Initialized Kafka producer")
        logger.info(f"  Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"  Default topic: {self.topic}")
    
    def _create_producer(self) -> KafkaProducer:
        """
        Create and configure Kafka producer with retry logic
        
        Returns:
            Configured KafkaProducer instance
            
        Raises:
            SystemExit: If cannot connect to Kafka after retries
        """
        for attempt in range(self.max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(','),
                    
                    # Serialization
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    
                    # Performance tuning
                    batch_size=16384,  # 16KB batches
                    linger_ms=10,      # Wait 10ms to batch messages
                    compression_type='snappy',  # Compress messages
                    
                    # Reliability
                    acks='all',  # Wait for all replicas (most reliable)
                    retries=3,   # Retry failed sends
                    max_in_flight_requests_per_connection=5,
                    
                    # Timeouts
                    request_timeout_ms=30000,  # 30 seconds
                    api_version=(2, 5, 0)
                )
                
                logger.info("Connected to Kafka broker")
                return producer
                
            except NoBrokersAvailable:
                wait_time = self.retry_backoff_ms * (2 ** attempt) / 1000
                logger.warning(
                    f"Kafka broker not available (attempt {attempt + 1}/{self.max_retries}). "
                    f"Retrying in {wait_time:.1f}s..."
                )
                time.sleep(wait_time)
        
        # Failed after all retries
        error_msg = (
            f"Failed to connect to Kafka after {self.max_retries} attempts. "
            f"Bootstrap servers: {self.bootstrap_servers}"
        )
        logger.error(error_msg)
        sys.exit(1)
    
    def send_wafer_telemetry(
        self,
        wafer_data: Dict[str, Any],
        topic: Optional[str] = None,
        partition_key: Optional[str] = None
    ) -> bool:
        """
        Send wafer telemetry message to Kafka
        
        Args:
            wafer_data: Wafer telemetry dictionary
            topic: Override default topic
            partition_key: Key for partitioning (defaults to equipment_id)
            
        Returns:
            True if send successful, False otherwise
        """
        topic = topic or self.topic
        
        # Use equipment_id as partition key (ensures ordering per equipment)
        if partition_key is None:
            partition_key = wafer_data.get('equipment_id', 'unknown')
        
        try:
            # Send message asynchronously with callback
            future = self.producer.send(
                topic=topic,
                key=partition_key,
                value=wafer_data
            )
            
            # Add callback for delivery confirmation
            future.add_callback(lambda metadata: self._on_send_success(metadata, wafer_data))
            future.add_errback(lambda exc: self._on_send_error(exc, wafer_data))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self.messages_failed += 1
            return False
    
    def _on_send_success(self, record_metadata, wafer_data: Dict):
        """Callback for successful message delivery"""
        self.messages_sent += 1
        
        # Log every 100th message to avoid spam
        if self.messages_sent % 100 == 0:
            logger.info(
                f"📡 Sent {self.messages_sent} messages | "
                f"Latest: {wafer_data['wafer_id']} → "
                f"topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )
    
    def _on_send_error(self, exception, wafer_data: Dict):
        """Callback for failed message delivery"""
        self.messages_failed += 1
        logger.error(
            f"Failed to send wafer {wafer_data.get('wafer_id', 'unknown')}: {exception}"
        )
    
    def flush(self, timeout: int = 30):
        """
        Wait for all buffered messages to be sent
        
        Args:
            timeout: Max seconds to wait
        """
        logger.info("Flushing producer buffer...")
        self.producer.flush(timeout=timeout)
        logger.info(f"Flush complete. Sent: {self.messages_sent}, Failed: {self.messages_failed}")
    
    def close(self):
        """Close producer and cleanup resources"""
        logger.info("Closing Kafka producer...")
        self.flush()
        self.producer.close()
        logger.info("Producer closed")
    
    def get_stats(self) -> Dict[str, int]:
        """Get producer statistics"""
        return {
            'messages_sent': self.messages_sent,
            'messages_failed': self.messages_failed,
            'success_rate': (
                self.messages_sent / (self.messages_sent + self.messages_failed) * 100
                if (self.messages_sent + self.messages_failed) > 0
                else 0
            )
        }


def main():
    """Test the Kafka producer with sample data"""
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description="Test Kafka producer")
    parser.add_argument("--messages", type=int, default=1000, help="Number of test messages")
    parser.add_argument("--topic", default="wafer-telemetry", help="Kafka topic")
    args = parser.parse_args()
    
    # Initialize producer
    producer = ManufacturingKafkaProducer(topic=args.topic)
    
    try:
        # Send test messages
        logger.info(f"Sending {args.messages} test messages to topic '{args.topic}'")
        
        for i in range(args.messages):
            wafer_data = {
                'timestamp': datetime.now().isoformat(),
                'wafer_id': f'W{str(i).zfill(8)}',
                'equipment_id': f'EQ00{(i % 4) + 1}',  # 4 equipment
                'temperature_c': 350.0 + (i % 10),
                'pressure_torr': 10.0 + (i % 5) * 0.1,
                'yield_rate': 0.95 - (i % 10) * 0.01,
                'defect_count': i % 5,
                'is_anomaly': False
            }
            
            producer.send_wafer_telemetry(wafer_data)
            time.sleep(0.1)  # Small delay between messages
        
        # Wait for all messages to be delivered
        producer.flush()
        
        # Print stats
        stats = producer.get_stats()
        logger.info(f"\n📊 Producer Statistics:")
        logger.info(f"  Messages sent: {stats['messages_sent']}")
        logger.info(f"  Messages failed: {stats['messages_failed']}")
        logger.info(f"  Success rate: {stats['success_rate']:.1f}%")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()