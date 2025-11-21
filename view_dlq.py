#!/usr/bin/env python3
"""
DLQ Viewer - View messages in the Dead Letter Queue
"""

from confluent_kafka import Consumer, KafkaError
import json


def view_dlq(bootstrap_servers='localhost:9092', dlq_topic='orders-dlq'):
    """
    View all messages in the Dead Letter Queue
    
    Args:
        bootstrap_servers: Kafka broker address
        dlq_topic: DLQ topic name
    """
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'dlq-viewer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([dlq_topic])
    
    print("=" * 60)
    print(f"📭 Dead Letter Queue Viewer")
    print(f"📬 Topic: {dlq_topic}")
    print("=" * 60)
    print("\nFetching messages... (Press Ctrl+C to stop)\n")
    
    message_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            
            if msg is None:
                if message_count == 0:
                    print("📭 No messages in DLQ (yet)")
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"\n✅ Reached end of partition (Total messages: {message_count})")
                    break
                else:
                    print(f"❌ Error: {msg.error()}")
                continue
            
            message_count += 1
            
            try:
                dlq_data = json.loads(msg.value().decode('utf-8'))
                
                print(f"\n{'─'*60}")
                print(f"Message #{message_count}")
                print(f"{'─'*60}")
                print(f"🔑 Key:              {msg.key().decode('utf-8') if msg.key() else 'None'}")
                print(f"📦 Original Message: {dlq_data.get('original_message', 'N/A')}")
                print(f"📍 Original Topic:   {dlq_data.get('original_topic', 'N/A')}")
                print(f"🎯 Partition:        {dlq_data.get('partition', 'N/A')}")
                print(f"📊 Offset:           {dlq_data.get('offset', 'N/A')}")
                print(f"❌ Error Reason:     {dlq_data.get('error_reason', 'N/A')}")
                print(f"🕐 Timestamp:        {dlq_data.get('timestamp', 'N/A')}")
                
            except json.JSONDecodeError:
                print(f"\nMessage #{message_count}: {msg.value()}")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Viewer interrupted by user")
    finally:
        consumer.close()
        print(f"\n{'='*60}")
        print(f"📊 Total DLQ Messages: {message_count}")
        print("=" * 60)


if __name__ == '__main__':
    view_dlq()
