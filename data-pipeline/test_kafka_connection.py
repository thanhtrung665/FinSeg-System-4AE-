#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_kafka_connection.py
Test Kafka connection, producer và consumer
"""

import json
import sys
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError

def print_header(title):
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def test_admin():
    """Test Kafka Admin connection and list topics"""
    print_header("TEST 1: Kafka Admin & Topics")
    
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='test-admin'
        )
        
        # List topics
        topics = admin.list_topics()
        print(f"✅ Connected to Kafka Admin")
        print(f"📋 Found {len(topics)} topics:")
        for topic in sorted(topics):
            print(f"   - {topic}")
        
        admin.close()
        return True
        
    except Exception as e:
        print(f"❌ Admin connection failed: {e}")
        return False

def test_producer():
    """Test Kafka Producer"""
    print_header("TEST 2: Kafka Producer")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Send test message
        test_msg = {
            'test': 'hello from test_kafka_connection.py',
            'timestamp': '2025-06-06T14:00:00',
            'data': 'This is a test message 中文'
        }
        
        future = producer.send('realtime_news', key='test', value=test_msg)
        result = future.get(timeout=10)
        
        print(f"✅ Producer connected")
        print(f"📤 Sent message to topic: {result.topic}")
        print(f"   Partition: {result.partition}")
        print(f"   Offset: {result.offset}")
        print(f"   Message: {test_msg}")
        
        producer.flush()
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Producer failed: {e}")
        return False

def test_consumer():
    """Test Kafka Consumer"""
    print_header("TEST 3: Kafka Consumer")
    
    try:
        consumer = KafkaConsumer(
            'realtime_news',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,  # 5 seconds timeout
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='test-consumer-group'
        )
        
        print(f"✅ Consumer connected")
        print(f"📥 Reading messages from 'realtime_news' topic...")
        
        msg_count = 0
        for message in consumer:
            msg_count += 1
            print(f"\n   Message {msg_count}:")
            print(f"   - Partition: {message.partition}")
            print(f"   - Offset: {message.offset}")
            print(f"   - Key: {message.key.decode('utf-8') if message.key else None}")
            print(f"   - Value: {message.value}")
            
            if msg_count >= 3:  # Read max 3 messages
                break
        
        if msg_count == 0:
            print(f"⚠️  No messages found (this is OK for a fresh topic)")
        else:
            print(f"\n✅ Read {msg_count} message(s)")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"❌ Consumer failed: {e}")
        return False

def test_all_topics():
    """Test sending messages to all 4 topics"""
    print_header("TEST 4: All Topics Producer Test")
    
    topics = ['realtime_news', 'realtime_social', 'realtime_market', 'realtime_policy']
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        
        for topic in topics:
            test_msg = {
                'topic': topic,
                'test': f'Test message for {topic}',
                'timestamp': '2025-06-06T14:00:00'
            }
            
            future = producer.send(topic, value=test_msg)
            result = future.get(timeout=10)
            print(f"✅ Sent to {topic} (partition {result.partition}, offset {result.offset})")
        
        producer.flush()
        producer.close()
        print(f"\n✅ All 4 topics tested successfully")
        return True
        
    except Exception as e:
        print(f"❌ All topics test failed: {e}")
        return False

def main():
    print("\n" + "🚀"*30)
    print("  KAFKA CONNECTION TEST")
    print("🚀"*30)
    
    results = []
    
    # Test 1: Admin
    results.append(("Admin Connection", test_admin()))
    
    # Test 2: Producer
    results.append(("Producer", test_producer()))
    
    # Test 3: Consumer
    results.append(("Consumer", test_consumer()))
    
    # Test 4: All topics
    results.append(("All Topics", test_all_topics()))
    
    # Summary
    print_header("TEST SUMMARY")
    
    all_passed = True
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}  {name}")
        if not passed:
            all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("🎉 ALL TESTS PASSED! Kafka is ready.")
        print("="*60)
        return 0
    else:
        print("⚠️  SOME TESTS FAILED. Check the errors above.")
        print("="*60)
        return 1

if __name__ == "__main__":
    sys.exit(main())
