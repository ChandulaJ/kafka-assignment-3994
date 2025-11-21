#!/usr/bin/env python3
"""
Kafka Producer for Order Messages
Produces order messages with Avro serialization to the 'orders' topic
"""

import random
import time
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


class OrderProducer:
    def __init__(self, bootstrap_servers='localhost:9092', schema_registry_url='http://localhost:8081'):
        """
        Initialize the Kafka producer with Avro serialization
        
        Args:
            bootstrap_servers: Kafka broker address
            schema_registry_url: Schema Registry URL
        """
        # Load Avro schema
        with open('schemas/order.avsc', 'r') as f:
            schema_str = f.read()
        
        self.value_schema = avro.loads(schema_str)
        
        # Configure producer
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'schema.registry.url': schema_registry_url
        }
        
        self.producer = AvroProducer(
            self.producer_config,
            default_key_schema=avro.loads('{"type": "string"}'),
            default_value_schema=self.value_schema
        )
        
        # Sample products for random generation
        self.products = [
            "Item1", "Item2", "Item3", "Item4", "Item5",
            "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"
        ]
        
        self.order_counter = 1000
    
    def delivery_report(self, err, msg):
        """
        Callback for message delivery reports
        
        Args:
            err: Error if delivery failed
            msg: Message that was produced
        """
        if err is not None:
            print(f' Message delivery failed: {err}')
        else:
            print(f'✅ Message delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}')
    
    def generate_order(self):
        """
        Generate a random order message
        
        Returns:
            dict: Order message with orderId, product, and price
        """
        order = {
            'orderId': str(self.order_counter),
            'product': random.choice(self.products),
            'price': round(random.uniform(10.0, 500.0), 2)
        }
        self.order_counter += 1
        return order
    
    def produce_orders(self, topic='orders', num_messages=10, interval=1):
        """
        Produce multiple order messages to Kafka
        
        Args:
            topic: Kafka topic name
            num_messages: Number of messages to produce
            interval: Time interval (seconds) between messages
        """
        print(f" Starting to produce {num_messages} orders to topic '{topic}'")
        print(f" Message interval: {interval} second(s)")
        print("-" * 60)
        
        try:
            for i in range(num_messages):
                order = self.generate_order()
                
                print(f"\n📦 Producing order #{i+1}: {order}")
                
                # Produce message with key (orderId) and value (order)
                self.producer.produce(
                    topic=topic,
                    value=order,
                    key=order['orderId'],
                    callback=self.delivery_report
                )
                
                # Trigger delivery reports
                self.producer.poll(0)
                
                time.sleep(interval)
            
            # Wait for all messages to be delivered
            print("\n Flushing remaining messages...")
            self.producer.flush()
            print(f"\n✨ Successfully produced {num_messages} orders!")
            
        except KeyboardInterrupt:
            print("\n  Producer interrupted by user")
        except Exception as e:
            print(f"\n Error producing messages: {e}")
        finally:
            self.producer.flush()


def main():
    """
    Main function to run the producer
    """
    print("=" * 60)
    print(" Kafka Order Producer with Avro Serialization")
    print("=" * 60)
    
    # Wait for Kafka to be ready
    print("\n Waiting 5 seconds for Kafka to be ready...")
    time.sleep(5)
    
    # Create producer
    producer = OrderProducer()
    
    # Produce orders (continuous mode for demonstration)
    # Modify num_messages and interval as needed
    try:
        while True:
            producer.produce_orders(
                topic='orders',
                num_messages=5,
                interval=2
            )
            print("\n" + "=" * 60)
            print("  Batch complete. Waiting 5 seconds before next batch...")
            print("=" * 60)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n\n👋 Producer shutting down gracefully...")


if __name__ == '__main__':
    main()
