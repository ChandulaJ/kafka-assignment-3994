#!/usr/bin/env python3
"""
Kafka Consumer for Order Messages
Consumes order messages with Avro deserialization from the 'orders' topic
Features:
- Real-time aggregation (running average of prices)
- Retry logic for temporary failures
- Dead Letter Queue (DLQ) for permanently failed messages
"""

import time
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaError, Producer


class OrderConsumer:
    def __init__(self, 
                 bootstrap_servers='localhost:9092', 
                 schema_registry_url='http://localhost:8081',
                 group_id='order-consumer-group',
                 max_retries=3):
        """
        Initialize the Kafka consumer with Avro deserialization
        
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
            'enable.auto.commit': False  # Manual commit for better control
        }
        
        self.consumer = AvroConsumer(self.consumer_config)
        
        # Configure DLQ producer
        self.dlq_producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })
        
        self.max_retries = max_retries
        self.retry_counts = {}  # Track retry attempts per message
        
        # Aggregation state
        self.total_price = 0.0
        self.message_count = 0
        self.running_average = 0.0
        
    def delivery_report(self, err, msg):
        """
        Callback for DLQ message delivery
        """
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
        
        print(f"\n   Sending to DLQ: {error_reason}")
        
        self.dlq_producer.produce(
            topic=dlq_topic,
            key=message.key(),
            value=json.dumps(dlq_message),
            callback=self.delivery_report
        )
        self.dlq_producer.poll(0)
    
    def process_message(self, message):
        """
        Process an order message with retry logic
        
        Args:
            message: Kafka message to process
            
        Returns:
            bool: True if processing succeeded, False otherwise
        """
        try:
            order = message.value()
            message_key = message.key()
            
            # Simulate potential processing failure (10% chance for demonstration)
            import random
            if random.random() < 0.1:
                raise Exception("Simulated temporary processing failure")
            
            # Extract order details
            order_id = order['orderId']
            product = order['product']
            price = order['price']
            
            # Update running average
            self.message_count += 1
            self.total_price += price
            self.running_average = self.total_price / self.message_count
            
            # Display processed order
            print(f"\n{'='*60}")
            print(f" Processed Order:")
            print(f"   Order ID: {order_id}")
            print(f"   Product:  {product}")
            print(f"   Price:    ${price:.2f}")
            print(f"\n Real-time Aggregation:")
            print(f"   Total Orders Processed: {self.message_count}")
            print(f"   Total Revenue:          ${self.total_price:.2f}")
            print(f"   Running Average Price:  ${self.running_average:.2f}")
            print(f"{'='*60}")
            
            return True
            
        except Exception as e:
            print(f"\n   Processing error: {e}")
            return False
    
    def handle_message(self, message):
        """
        Handle message with retry logic and DLQ
        
        Args:
            message: Kafka message
        """
        message_key = message.key() if message.key() else "unknown"
        
        # Initialize retry count for this message
        if message_key not in self.retry_counts:
            self.retry_counts[message_key] = 0
        
        # Try to process the message
        success = self.process_message(message)
        
        if not success:
            self.retry_counts[message_key] += 1
            
            if self.retry_counts[message_key] < self.max_retries:
                # Retry
                print(f"   Retry attempt {self.retry_counts[message_key]}/{self.max_retries}")
                time.sleep(1)  # Backoff before retry
                
                # Retry processing
                success = self.process_message(message)
                
                if success:
                    # Reset retry count on success
                    self.retry_counts[message_key] = 0
            
            if not success and self.retry_counts[message_key] >= self.max_retries:
                # Max retries exceeded, send to DLQ
                error_reason = f"Max retries ({self.max_retries}) exceeded"
                self.send_to_dlq(message, error_reason)
                self.retry_counts[message_key] = 0  # Reset for future messages
        else:
            # Reset retry count on success
            if message_key in self.retry_counts:
                self.retry_counts[message_key] = 0
    
    def consume_orders(self, topic='orders'):
        """
        Consume order messages from Kafka topic
        
        Args:
            topic: Kafka topic to consume from
        """
        print(f" Starting consumer for topic '{topic}'")
        print(f" Consumer Group: {self.consumer_config['group.id']}")
        print(f" Max Retries: {self.max_retries}")
        print(f" DLQ Topic: orders-dlq")
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
                        print(f' Reached end of partition {msg.partition()}')
                    else:
                        print(f' Consumer error: {msg.error()}')
                    continue
                
                # Handle the message with retry logic
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
            print(f"   Total Orders Processed: {self.message_count}")
            print(f"   Total Revenue:          ${self.total_price:.2f}")
            if self.message_count > 0:
                print(f"   Average Order Price:    ${self.running_average:.2f}")
            print("=" * 60)


def main():
    """
    Main function to run the consumer
    """
    print("=" * 60)
    print(" Kafka Order Consumer with Avro Deserialization")
    print("=" * 60)
    
    # Wait for Kafka to be ready
    print("\n Waiting 10 seconds for Kafka and Schema Registry to be ready...")
    time.sleep(10)
    
    # Create and run consumer
    consumer = OrderConsumer(max_retries=3)
    consumer.consume_orders(topic='orders')


if __name__ == '__main__':
    main()
