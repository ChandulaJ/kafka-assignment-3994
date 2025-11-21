@echo off
REM Setup script for Kafka Order Processing System (Windows)

echo ===========================================
echo Kafka Order Processing System - Setup
echo ===========================================
echo.

REM Check if Docker is running
echo Checking Docker...
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running. Please start Docker and try again.
    exit /b 1
)
echo Docker is running
echo.

REM Start Docker Compose services
echo Starting Kafka infrastructure...
docker-compose up -d

echo.
echo Waiting for services to be ready (30 seconds)...
timeout /t 30 /nobreak >nul

REM Check service health
echo.
echo Checking service status...
docker-compose ps

echo.
echo Creating Kafka topics...
docker exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists 2>nul
docker exec kafka kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists 2>nul

echo.
echo Listing all topics...
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo.
echo Setting up Python virtual environment...
if not exist "venv" (
    python -m venv venv
    echo Virtual environment created
) else (
    echo Virtual environment already exists
)

echo.
echo Installing Python dependencies...
call venv\Scripts\pip install -r requirements.txt

echo.
echo ===========================================
echo Setup Complete!
echo ===========================================
echo.
echo Next steps:
echo 1. Activate virtual environment:
echo    venv\Scripts\activate
echo.
echo 2. Run the consumer:
echo    python consumer.py
echo.
echo 3. In another terminal, run the producer:
echo    python producer.py
echo.
echo ===========================================
pause
