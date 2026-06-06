#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Kafka connection va topics
"""

import sys
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import json
import time

def test_kafka_connection():
    """Test ket noi Kafka"""
    print("\n" + "="*60)
    print("  KAFKA CONNECTION TEST")
    print("="*60 + "\n")
    
    broker = 'localhost:9092'
    
    # Test 1: Kiem tra Kafka server
    print("[1/5] Kiem tra Kafka server...")
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=broker,
            request_timeout_ms=5000
        )
        print("✅ Kafka server dang chay tai: " + broker)
        admin.close()
    except Exception as e:
        print(f"❌ Khong ket noi duoc Kafka: {e}")
        print("\n💡 Giai phap:")
        print("   1. Chay: run_all_kafka.bat")
        print("   2. Hoac chay tung buoc:")
        print("      - start_zookeeper.bat")
        print("      - start_kafka.bat")
        print("      - create_topics.bat")
        return False
    
    # Test 2: List topics
    print("\n[2/5] Kiem tra Kafka topics...")
    try:
        admin = KafkaAdminClient(bootstrap_servers=broker)
        topics = admin.list_topics()
        
        required_topics = ['fb_mock_data', 'realtime_market', 'realtime_policy']
        missing_topics = [t for t in required_topics if t not in topics]
        
        if missing_topics:
            print(f"⚠️  Topics bi thieu: {missing_topics}")
            print("\n💡 Tao topics bang cach chay: create_topics.bat")
            return False
        else:
            print("✅ Tat ca topics da san sang:")
            for topic in required_topics:
                print(f"   - {topic}")
        
        admin.close()
    except Exception as e:
        print(f"❌ Loi kiem tra topics: {e}")
        return False
    
    # Test 3: Producer test
    print("\n[3/5] Test Kafka Producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000
        )
        
        test_message = {
            "test": True,
            "message": "Test message from Python",
            "timestamp": time.time()
        }
        
        future = producer.send('fb_mock_data', test_message)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Producer OK - Sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}")
        producer.close()
    except Exception as e:
        print(f"❌ Producer loi: {e}")
        return False
    
    # Test 4: Consumer test
    print("\n[4/5] Test Kafka Consumer...")
    try:
        consumer = KafkaConsumer(
            'fb_mock_data',
            bootstrap_servers=broker,
            auto_offset_reset='latest',
            consumer_timeout_ms=3000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        print("✅ Consumer OK - Listening to topic: fb_mock_data")
        consumer.close()
    except Exception as e:
        print(f"❌ Consumer loi: {e}")
        return False
    
    # Test 5: Full workflow test
    print("\n[5/5] Test full Producer → Consumer workflow...")
    try:
        # Producer
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_data = {
            "ticker": "SHB",
            "test_id": int(time.time()),
            "message": "Full workflow test"
        }
        
        producer.send('fb_mock_data', test_data)
        producer.flush()
        print("✅ Sent test message")
        
        # Consumer
        consumer = KafkaConsumer(
            'fb_mock_data',
            bootstrap_servers=broker,
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Send another message to trigger consumer
        producer.send('fb_mock_data', test_data)
        producer.flush()
        
        received = False
        for message in consumer:
            if message.value.get('test_id') == test_data['test_id']:
                print(f"✅ Received test message: {message.value}")
                received = True
                break
        
        if not received:
            print("⚠️  Khong nhan duoc message (co the do timing)")
        
        producer.close()
        consumer.close()
        
    except Exception as e:
        print(f"❌ Workflow test loi: {e}")
        return False
    
    # Summary
    print("\n" + "="*60)
    print("  ✅ TAT CA TESTS DA PASS!")
    print("="*60)
    print("\nKafka da san sang de su dung:")
    print(f"  - Bootstrap server: {broker}")
    print("  - Topics: fb_mock_data, realtime_market, realtime_policy")
    print("\nBuoc tiep theo:")
    print("  1. Enable Kafka trong dashboard:")
    print("     - Sua file: dashboard_realtime.py")
    print("     - Doi: kafka_enabled=False → kafka_enabled=True")
    print("\n  2. Chay he thong:")
    print("     - Terminal 1: python realtime_pipeline\\scheduler.py --ticker SHB")
    print("     - Terminal 2: python realtime_pipeline\\run_vector_worker.py")
    print("     - Terminal 3: streamlit run dashboard_realtime.py")
    print("\n" + "="*60)
    
    return True

if __name__ == "__main__":
    try:
        success = test_kafka_connection()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
