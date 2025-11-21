#!/usr/bin/env python3
"""
Kafka Consumer that FAILS ALL Messages
This consumer intentionally fails to process all messages to demonstrate DLQ behavior
"""

import time
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaError, Producer


class FailAllConsumer:
    def __init__(self, 
                 bootstrap_servers='localhost:9092', 
                 schema_registry_url='http://localhost:8081',
                 group_id='fail-all-consumer-group',
                 max_retries=3):
        """
        Initialize the Kafka consumer that fails all messages
        
        Args:
            bootstrap_servers: Kafka broker address
            schema_registry_url: Schema Registry URL
            group_id: Consumer group ID
            max_retries: Maximum number of retries for failed messages
        """
        # Configure consumer
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'schema.registry.url': schema_registry_url,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        self.consumer = AvroConsumer(self.consumer_config)
        
        # Configure DLQ producer
        self.dlq_producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })
        
        self.max_retries = max_retries
        self.retry_counts = {}
        
        # Statistics
        self.total_messages = 0
        self.failed_messages = 0
        self.dlq_messages = 0
        
    def delivery_report(self, err, msg):
        """Callback for DLQ message delivery"""
        if err is not None:
            print(f'     DLQ delivery failed: {err}')
        else:
            print(f'     Message sent to DLQ topic: {msg.topic()}')
    
    def send_to_dlq(self, message, error_reason):
        """
        Send failed message to Dead Letter Queue
        
        Args:
            message: The failed message
            error_reason: Reason for failure
        """
        dlq_topic = 'orders-dlq'
        
        dlq_message = {
            'original_message': str(message.value()),
            'original_topic': message.topic(),
            'partition': message.partition(),
            'offset': message.offset(),
            'error_reason': error_reason,
            'timestamp': time.time()
        }
        
        print(f"\n    Sending to DLQ: {error_reason}")
        
        self.dlq_producer.produce(
            topic=dlq_topic,
            key=message.key(),
            value=json.dumps(dlq_message),
            callback=self.delivery_report
        )
        self.dlq_producer.poll(0)
        self.dlq_messages += 1
    
    def process_message(self, message):
        """
        Process message - ALWAYS FAILS
        
        Args:
            message: Kafka message to process
            
        Returns:
            bool: Always False (simulating failure)
        """
        try:
            order = message.value()
            order_id = order.get('orderId', 'unknown')
            product = order.get('product', 'unknown')
            price = order.get('price', 0.0)
            
            self.total_messages += 1
            
            print(f"\n{'='*60}")
            print(f" Attempting to process order:")
            print(f"   Order ID: {order_id}")
            print(f"   Product:  {product}")
            print(f"   Price:    ${price:.2f}")
            
            # ALWAYS FAIL - simulating a broken processor
            raise Exception("INTENTIONAL FAILURE - This consumer fails all messages!")
            
        except Exception as e:
            print(f"\n  Processing error: {e}")
            self.failed_messages += 1
            return False
    
    def handle_message(self, message):
        """
        Handle message with retry logic and DLQ
        
        Args:
            message: Kafka message
        """
        message_key = message.key() if message.key() else b"unknown"
        if isinstance(message_key, bytes):
            message_key = message_key.decode('utf-8')
        
        # Initialize retry count for this message
        if message_key not in self.retry_counts:
            self.retry_counts[message_key] = 0
        
        # Try to process the message (will always fail)
        success = self.process_message(message)
        
        if not success:
            # Retry up to max_retries times
            while not success and self.retry_counts[message_key] < self.max_retries:
                self.retry_counts[message_key] += 1
                print(f"   Retry attempt {self.retry_counts[message_key]}/{self.max_retries}")
                time.sleep(1)
                
                # Retry processing (will fail again)
                success = self.process_message(message)
            
            if not success:
                # Max retries exceeded, send to DLQ
                error_reason = f"Max retries ({self.max_retries}) exceeded - Consumer fails all messages"
                self.send_to_dlq(message, error_reason)
            
            # Reset retry count for next message
            self.retry_counts[message_key] = 0
    
    def consume_orders(self, topic='orders'):
        """
        Consume order messages from Kafka topic
        
        Args:
            topic: Kafka topic to consume from
        """
        print("=" * 60)
        print(" FAIL-ALL CONSUMER - SENDS ALL MESSAGES TO DLQ")
        print("=" * 60)
        print(f" Starting consumer for topic '{topic}'")
        print(f" Consumer Group: {self.consumer_config['group.id']}")
        print(f" Max Retries: {self.max_retries}")
        print(f" DLQ Topic: orders-dlq")
        print("  WARNING: This consumer intentionally fails ALL messages!")
        print("-" * 60)
        
        # Subscribe to topic
        self.consumer.subscribe([topic])
        
        try:
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f'📭 Reached end of partition {msg.partition()}')
                    else:
                        print(f' Consumer error: {msg.error()}')
                    continue
                
                # Handle the message (will fail and go to DLQ)
                self.handle_message(msg)
                
                # Commit offset after processing
                self.consumer.commit(asynchronous=False)
                
        except KeyboardInterrupt:
            print("\n\n  Consumer interrupted by user")
        except Exception as e:
            print(f"\n Consumer error: {e}")
        finally:
            # Clean up
            print("\n🧹 Closing consumer and flushing DLQ producer...")
            self.dlq_producer.flush()
            self.consumer.close()
            
            # Print final statistics
            print("\n" + "=" * 60)
            print(" Final Statistics:")
            print(f"   Total Messages Received:  {self.total_messages}")
            print(f"   Total Messages Failed:    {self.failed_messages}")
            print(f"   Total Messages Sent to DLQ: {self.dlq_messages}")
            print("=" * 60)


def main():
    """
    Main function to run the fail-all consumer
    """
    print("=" * 60)
    print(" Kafka Fail-All Consumer - DLQ Demonstration")
    print("=" * 60)
    
    # Wait for Kafka to be ready
    print("\n Waiting 10 seconds for Kafka and Schema Registry to be ready...")
    time.sleep(10)
    
    # Create and run consumer
    consumer = FailAllConsumer(max_retries=3)
    consumer.consume_orders(topic='orders')


if __name__ == '__main__':
    main()
