#!/bin/bash
# Setup script for Kafka Order Processing System

echo "==========================================="
echo "Kafka Order Processing System - Setup"
echo "==========================================="
echo ""

# Check if Docker is running
echo "📦 Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi
echo "✅ Docker is running"
echo ""

# Start Docker Compose services
echo "🚀 Starting Kafka infrastructure..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to be ready (30 seconds)..."
sleep 30

# Check service health
echo ""
echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "📋 Creating Kafka topics..."
docker exec -it kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null
docker exec -it kafka kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null

echo ""
echo "📚 Listing all topics..."
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "🐍 Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

echo ""
echo "📦 Installing Python dependencies..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows
    ./venv/Scripts/pip install -r requirements.txt
else
    # Linux/Mac
    ./venv/bin/pip install -r requirements.txt
fi

echo ""
echo "==========================================="
echo "✨ Setup Complete!"
echo "==========================================="
echo ""
echo "Next steps:"
echo "1. Activate virtual environment:"
echo "   Windows: venv\\Scripts\\activate"
echo "   Linux/Mac: source venv/bin/activate"
echo ""
echo "2. Run the consumer:"
echo "   python consumer.py"
echo ""
echo "3. In another terminal, run the producer:"
echo "   python producer.py"
echo ""
echo "==========================================="
