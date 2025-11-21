# System Architecture

## Component Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Kafka Ecosystem                               │
│                                                                      │
│  ┌──────────────┐         ┌──────────────┐      ┌──────────────┐  │
│  │  Zookeeper   │────────▶│    Kafka     │◀─────│   Schema     │  │
│  │   :2181      │         │   Broker     │      │  Registry    │  │
│  │              │         │   :9092      │      │   :8081      │  │
│  └──────────────┘         └──────────────┘      └──────────────┘  │
│                                   │                      │          │
└───────────────────────────────────┼──────────────────────┼──────────┘
                                    │                      │
                    ┌───────────────┴──────────────────────┘
                    │
        ┌───────────▼───────────┐
        │   Topic: 'orders'     │
        │  (Avro Serialized)    │
        └───────────┬───────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
┌───────▼────────┐      ┌───────▼────────┐
│   Producer     │      │   Consumer     │
│  producer.py   │      │  consumer.py   │
├────────────────┤      ├────────────────┤
│ - Generate     │      │ - Deserialize  │
│   orders       │      │ - Process      │
│ - Serialize    │      │ - Aggregate    │
│   with Avro    │      │ - Calculate    │
│ - Send to      │      │   avg price    │
│   Kafka        │      │ - Retry on     │
│                │      │   failure      │
└────────────────┘      └────────┬───────┘
                                 │
                     ┌───────────┴──────────┐
                     │                      │
              Success│                      │Max Retries
                     │                      │Exceeded
                     ▼                      ▼
            ┌────────────────┐    ┌─────────────────┐
            │  Aggregation   │    │  Dead Letter    │
            │   Statistics   │    │     Queue       │
            ├────────────────┤    │  'orders-dlq'   │
            │ - Count        │    ├─────────────────┤
            │ - Total $      │    │ - Failed msgs   │
            │ - Running Avg  │    │ - Error reason  │
            └────────────────┘    │ - Metadata      │
                                  └─────────────────┘
```

## Data Flow

### 1. Message Production
```
Producer → Avro Serialize → Schema Registry → Kafka Topic (orders)
```

### 2. Message Consumption (Success Path)
```
Kafka Topic → Consumer → Avro Deserialize → Process → Update Aggregation → Commit Offset
```

### 3. Message Consumption (Failure Path)
```
Kafka Topic → Consumer → Processing Fails
                ↓
            Retry (max 3x)
                ↓
        ┌───────┴────────┐
        │                │
    Success          Permanent
        │             Failure
        ▼                ▼
   Continue      Send to DLQ → Log Error
   Processing
```

## Message Schema (Avro)

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

## Key Features Implementation

### 1. Avro Serialization
- **Schema Definition**: `schemas/order.avsc`
- **Schema Registry**: Manages schema versions
- **Serialization**: Producer uses AvroProducer
- **Deserialization**: Consumer uses AvroConsumer

### 2. Real-time Aggregation
```python
# Running average calculation
total_price += current_price
message_count += 1
running_average = total_price / message_count
```

### 3. Retry Logic
```python
max_retries = 3
retry_count = 0

while retry_count < max_retries:
    try:
        process_message()
        break  # Success
    except Exception:
        retry_count += 1
        if retry_count >= max_retries:
            send_to_dlq()
```

### 4. Dead Letter Queue
- **Topic**: `orders-dlq`
- **Content**: Failed message + metadata
- **Metadata**: Error reason, timestamp, original topic/partition/offset

## Deployment Architecture

```
┌─────────────────────────────────────────┐
│          Docker Compose                 │
│                                         │
│  ┌──────────┐  ┌──────────┐  ┌───────┐│
│  │Zookeeper │  │  Kafka   │  │Schema ││
│  │Container │  │Container │  │Registry│
│  └──────────┘  └──────────┘  └───────┘│
└─────────────────────────────────────────┘
           │
           │ Exposed Ports
           │
    ┌──────┴──────────────────┐
    │                         │
┌───▼────┐            ┌───────▼──┐
│ :2181  │            │  :9092   │
│        │            │  :8081   │
└────────┘            └──────────┘
    │                      │
    │    Host Machine      │
    │                      │
┌───▼──────────────────────▼────┐
│   Python Applications         │
│   - producer.py               │
│   - consumer.py               │
│   - view_dlq.py               │
│   - monitor_topic.py          │
└───────────────────────────────┘
```

## Technology Stack

- **Message Broker**: Apache Kafka 7.5.0
- **Serialization**: Apache Avro
- **Schema Management**: Confluent Schema Registry
- **Programming Language**: Python 3.8+
- **Kafka Client**: confluent-kafka-python
- **Container Platform**: Docker & Docker Compose
- **Coordination**: Apache Zookeeper

## Performance Characteristics

- **Throughput**: Configurable batch size and interval
- **Latency**: Low latency with async processing
- **Reliability**: At-least-once delivery semantics
- **Scalability**: Horizontal scaling via partitions
- **Fault Tolerance**: Retry logic + DLQ for failures
