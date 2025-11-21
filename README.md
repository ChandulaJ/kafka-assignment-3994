# Kafka Order Processing System with Avro Serialization

A complete Kafka-based system demonstrating real-time order processing with Avro serialization, featuring real-time aggregation, retry logic, and Dead Letter Queue (DLQ) handling.

##  Features

- **Avro Serialization**: All messages use Apache Avro for efficient serialization/deserialization
- **Real-time Aggregation**: Running average calculation of order prices
- **Retry Logic**: Automatic retry mechanism for temporary processing failures
- **Dead Letter Queue (DLQ)**: Permanently failed messages are sent to a DLQ topic
- **Schema Registry**: Centralized schema management for Avro schemas
- **Docker Support**: Complete containerized setup with Docker Compose

##  Order Message Schema

Each order message follows this Avro schema (`schemas/order.avsc`):

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.kafka.assignment",
  "fields": [
    {
      "name": "orderId",
      "type": "string",
      "doc": "Unique identifier for the order"
    },
    {
      "name": "product",
      "type": "string",
      "doc": "Name of the purchased item"
    },
    {
      "name": "price",
      "type": "float",
      "doc": "Price of the product"
    }
  ]
}
```

##  Architecture

```
┌──────────────┐         ┌─────────────┐         ┌──────────────┐
│   Producer   │────────▶│   Kafka     │────────▶│   Consumer   │
│  (Avro)      │         │   Topic:    │         │  (Avro)      │
│              │         │   'orders'  │         │              │
└──────────────┘         └─────────────┘         └──────────────┘
                                                         │
                                                         │ Failed after retries
                                                         ▼
                                                  ┌─────────────┐
                                                  │     DLQ     │
                                                  │   Topic:    │
                                                  │'orders-dlq' │
                                                  └─────────────┘
```

### Components

1. **Zookeeper**: Manages Kafka cluster coordination
2. **Kafka Broker**: Message broker for order messages
3. **Schema Registry**: Stores and manages Avro schemas
4. **Producer**: Generates random order messages with Avro serialization
5. **Consumer**: Consumes orders with retry logic, aggregation, and DLQ handling

##  Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for running producer/consumer locally)
- Git (for version control)

### 1. Clone the Repository

```bash
git clone https://github.com/ChandulaJ/kafka-assignment-3994.git
cd kafka-assignment-3994
```

### 2. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This will start:
- Zookeeper (port 2181)
- Kafka Broker (port 9092)
- Schema Registry (port 8081)

Wait about 30 seconds for all services to be fully ready.

### 3. Verify Services are Running

```bash
docker-compose ps
```

All services should show "Up" status.

### 4. Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 5. Create Kafka Topics (Optional - auto-created on first use)

```bash
docker exec -it kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 6. Run the Consumer

In one terminal:

```bash
python consumer.py
```

The consumer will:
- Subscribe to the `orders` topic
- Process incoming messages
- Calculate running average of prices
- Retry failed messages up to 3 times
- Send permanently failed messages to DLQ

### 7. Run the Producer

In another terminal:

```bash
python producer.py
```

The producer will:
- Generate random order messages
- Serialize messages using Avro
- Send messages to the `orders` topic
- Display delivery confirmations

##  Real-time Aggregation

The consumer maintains a **running average** of order prices:

```
 Real-time Aggregation:
   Total Orders Processed: 15
   Total Revenue:          $3,245.67
   Running Average Price:  $216.38
```

This updates with each successfully processed message.

## Retry Logic

The consumer implements intelligent retry logic:

1. **First Attempt**: Process message normally
2. **Temporary Failure**: Retry up to 3 times with 1-second backoff
3. **Permanent Failure**: After max retries, send to DLQ

Example output:
```
 Processing error: Simulated temporary processing failure
Retry attempt 1/3
Processed Order: ...
```

##  Dead Letter Queue (DLQ)

Messages that fail after maximum retries are sent to `orders-dlq` topic with metadata:

```json
{
  "original_message": "...",
  "original_topic": "orders",
  "partition": 0,
  "offset": 42,
  "error_reason": "Max retries (3) exceeded",
  "timestamp": 1700000000.0
}
```

### Viewing DLQ Messages

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning
```

##  Testing the System

### Test 1: Normal Operation

1. Start consumer
2. Start producer
3. Observe messages being produced and consumed
4. Check running average calculations

### Test 2: Retry Logic

The consumer has a 10% simulated failure rate to demonstrate retry logic:

- Watch for retry messages in consumer output
- Successful retries will continue processing
- Failed retries will go to DLQ

### Test 3: DLQ Verification

```bash
# Monitor DLQ topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning
```

### Test 4: View All Topics

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

##  Project Structure

```
kafka-assignment-3994/
├── schemas/
│   └── order.avsc              # Avro schema definition
├── producer.py                 # Order message producer
├── consumer.py                 # Order message consumer with retry/DLQ
├── docker-compose.yml          # Docker infrastructure setup
├── requirements.txt            # Python dependencies
├── .gitignore                  # Git ignore rules
└── README.md                   # This file
```

## 🔧 Configuration

### Producer Configuration

- **Bootstrap Servers**: `localhost:9092`
- **Schema Registry**: `http://localhost:8081`
- **Topic**: `orders`
- **Message Interval**: 2 seconds
- **Batch Size**: 5 messages per batch

### Consumer Configuration

- **Bootstrap Servers**: `localhost:9092`
- **Schema Registry**: `http://localhost:8081`
- **Consumer Group**: `order-consumer-group`
- **Auto Offset Reset**: `earliest`
- **Max Retries**: 3
- **DLQ Topic**: `orders-dlq`

## 🛠️ Troubleshooting

### Issue: "Connection refused" error

**Solution**: Ensure Kafka services are running
```bash
docker-compose ps
docker-compose logs kafka
```

### Issue: "Schema Registry unavailable"

**Solution**: Wait for Schema Registry to be ready
```bash
docker-compose logs schema-registry
# Wait 30-60 seconds after starting services
```

### Issue: Import errors in Python

**Solution**: Ensure virtual environment is activated and dependencies installed
```bash
pip install -r requirements.txt
```

### Issue: Messages not being consumed

**Solution**: Check consumer group and reset offsets if needed
```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group order-consumer-group \
  --describe
```

## Cleanup

### Stop Services

```bash
docker-compose down
```

### Remove All Data (including volumes)

```bash
docker-compose down -v
```

### Deactivate Virtual Environment

```bash
deactivate
```

## Key Concepts Demonstrated

1. **Avro Serialization**: Efficient binary serialization with schema evolution support
2. **Schema Registry**: Centralized schema management
3. **Producer-Consumer Pattern**: Decoupled message processing
4. **Retry Logic**: Handling transient failures gracefully
5. **Dead Letter Queue**: Managing permanently failed messages
6. **Real-time Aggregation**: Stateful stream processing
7. **Offset Management**: Manual commit for better control
8. **Error Handling**: Comprehensive error handling and logging

##  Learning Outcomes

- Understanding Kafka architecture and components
- Implementing Avro serialization in Python
- Building resilient message processing systems
- Implementing retry mechanisms and DLQ patterns
- Real-time data aggregation and analytics
- Docker containerization for Kafka ecosystem

##  Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Apache Avro](https://avro.apache.org/docs/)
- [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)

##  Development

### Modifying the Producer

Edit `producer.py` to:
- Change message generation rate
- Add new product types
- Modify price ranges
- Customize message format

### Modifying the Consumer

Edit `consumer.py` to:
- Adjust retry count and backoff strategy
- Modify aggregation logic
- Add custom processing logic
- Change DLQ behavior

