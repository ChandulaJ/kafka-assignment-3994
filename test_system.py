#!/usr/bin/env python3
"""
System Test Script
Validates that the Kafka system is working correctly
"""

import time
import subprocess
import sys
import requests


def check_docker():
    """Check if Docker is running"""
    print("🐳 Checking Docker...")
    try:
        result = subprocess.run(['docker', 'info'], 
                              capture_output=True, 
                              text=True)
        if result.returncode == 0:
            print("  ✅ Docker is running")
            return True
        else:
            print("  ❌ Docker is not running")
            return False
    except FileNotFoundError:
        print("  ❌ Docker is not installed")
        return False


def check_docker_compose():
    """Check if docker-compose services are running"""
    print("\n📦 Checking Docker Compose services...")
    try:
        result = subprocess.run(['docker-compose', 'ps'], 
                              capture_output=True, 
                              text=True)
        if 'kafka' in result.stdout and 'Up' in result.stdout:
            print("  ✅ Kafka is running")
        else:
            print("  ❌ Kafka is not running")
            print("  💡 Run: docker-compose up -d")
            return False
            
        if 'zookeeper' in result.stdout and 'Up' in result.stdout:
            print("  ✅ Zookeeper is running")
        else:
            print("  ❌ Zookeeper is not running")
            return False
            
        if 'schema-registry' in result.stdout and 'Up' in result.stdout:
            print("  ✅ Schema Registry is running")
        else:
            print("  ❌ Schema Registry is not running")
            return False
            
        return True
    except FileNotFoundError:
        print("  ❌ docker-compose is not installed")
        return False


def check_kafka_connection():
    """Check if Kafka broker is accessible"""
    print("\n🔌 Checking Kafka broker connection...")
    try:
        result = subprocess.run(
            ['docker', 'exec', 'kafka', 'kafka-broker-api-versions', 
             '--bootstrap-server', 'localhost:9092'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print("  ✅ Kafka broker is accessible")
            return True
        else:
            print("  ❌ Cannot connect to Kafka broker")
            return False
    except subprocess.TimeoutExpired:
        print("  ❌ Kafka broker connection timeout")
        return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def check_schema_registry():
    """Check if Schema Registry is accessible"""
    print("\n📋 Checking Schema Registry...")
    try:
        response = requests.get('http://localhost:8081/subjects', timeout=5)
        if response.status_code == 200:
            print("  ✅ Schema Registry is accessible")
            subjects = response.json()
            if subjects:
                print(f"  📚 Registered schemas: {', '.join(subjects)}")
            else:
                print("  📚 No schemas registered yet (this is normal before first run)")
            return True
        else:
            print(f"  ❌ Schema Registry returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Cannot connect to Schema Registry: {e}")
        return False


def check_topics():
    """Check Kafka topics"""
    print("\n📬 Checking Kafka topics...")
    try:
        result = subprocess.run(
            ['docker', 'exec', 'kafka', 'kafka-topics', 
             '--list', '--bootstrap-server', 'localhost:9092'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            topics = [t for t in topics if t]  # Remove empty strings
            
            if topics:
                print(f"  ✅ Found {len(topics)} topic(s):")
                for topic in topics:
                    print(f"     - {topic}")
            else:
                print("  ⚠️  No topics found yet (will be auto-created)")
            return True
        else:
            print("  ❌ Cannot list topics")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def check_python_dependencies():
    """Check if Python dependencies are installed"""
    print("\n🐍 Checking Python dependencies...")
    try:
        import confluent_kafka
        print("  ✅ confluent-kafka is installed")
        
        import avro
        print("  ✅ avro-python3 is installed")
        
        import requests
        print("  ✅ requests is installed")
        
        return True
    except ImportError as e:
        print(f"  ❌ Missing dependency: {e}")
        print("  💡 Run: pip install -r requirements.txt")
        return False


def check_files():
    """Check if required files exist"""
    print("\n📄 Checking required files...")
    required_files = [
        'producer.py',
        'consumer.py',
        'schemas/order.avsc',
        'docker-compose.yml',
        'requirements.txt'
    ]
    
    all_exist = True
    for file in required_files:
        try:
            with open(file, 'r'):
                print(f"  ✅ {file}")
        except FileNotFoundError:
            print(f"  ❌ {file} not found")
            all_exist = False
    
    return all_exist


def run_all_checks():
    """Run all system checks"""
    print("=" * 60)
    print("🧪 Kafka Order Processing System - System Test")
    print("=" * 60)
    
    checks = [
        ("Docker Installation", check_docker),
        ("Docker Compose Services", check_docker_compose),
        ("Kafka Broker Connection", check_kafka_connection),
        ("Schema Registry", check_schema_registry),
        ("Kafka Topics", check_topics),
        ("Python Dependencies", check_python_dependencies),
        ("Required Files", check_files)
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"\n❌ Unexpected error in {check_name}: {e}")
            results[check_name] = False
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 Test Summary")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for check_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {check_name}")
    
    print("\n" + "=" * 60)
    print(f"Result: {passed}/{total} checks passed")
    
    if passed == total:
        print("✨ All systems ready! You can now run the producer and consumer.")
        print("\n📝 Next steps:")
        print("   Terminal 1: python consumer.py")
        print("   Terminal 2: python producer.py")
        print("=" * 60)
        return 0
    else:
        print("⚠️  Some checks failed. Please fix the issues above.")
        print("=" * 60)
        return 1


if __name__ == '__main__':
    exit_code = run_all_checks()
    sys.exit(exit_code)
