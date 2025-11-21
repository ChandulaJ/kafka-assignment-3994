#!/usr/bin/env python3
"""
Kafka Topic Monitor - View real-time messages from any topic
"""

from confluent_kafka import Consumer, KafkaError
import sys


def monitor_topic(topic='orders', bootstrap_servers='localhost:9092'):
    """
    Monitor messages from a Kafka topic in real-time
    
    Args:
        topic: Topic name to monitor
        bootstrap_servers: Kafka broker address
    """
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': f'{topic}-monitor-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    
    print("=" * 60)
    print(f"📡 Kafka Topic Monitor")
    print(f"📬 Topic: {topic}")
    print(f"🔌 Bootstrap Servers: {bootstrap_servers}")
    print("=" * 60)
    print("\nListening for messages... (Press Ctrl+C to stop)\n")
    
    message_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Error: {msg.error()}")
                continue
            
            message_count += 1
            
            print(f"\n{'─'*60}")
            print(f"📨 Message #{message_count}")
            print(f"{'─'*60}")
            print(f"🔑 Key:       {msg.key().decode('utf-8') if msg.key() else 'None'}")
            print(f"📦 Value:     {msg.value()}")
            print(f"🎯 Partition: {msg.partition()}")
            print(f"📊 Offset:    {msg.offset()}")
            print(f"🕐 Timestamp: {msg.timestamp()[1] if msg.timestamp()[0] != -1 else 'N/A'}")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Monitor interrupted by user")
    finally:
        consumer.close()
        print(f"\n{'='*60}")
        print(f"📊 Total Messages Received: {message_count}")
        print("=" * 60)


if __name__ == '__main__':
    topic = sys.argv[1] if len(sys.argv) > 1 else 'orders'
    print(f"\n🎯 Monitoring topic: {topic}\n")
    monitor_topic(topic=topic)
