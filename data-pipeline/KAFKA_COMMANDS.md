# 📋 Kafka Commands Quick Reference

## 🚀 Khởi Động Nhanh (Quick Start)

### Cách 1: Tự động (Recommended)
```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Chạy script tự động setup
bash setup_kafka.sh

# Test connection
python3 test_kafka_connection.py
```

### Cách 2: Manual

#### Nếu KHÔNG có Zookeeper (KRaft Mode - Recommended cho Kafka 4.3.0+)
```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Bước 1: Format storage (chỉ chạy 1 lần)
KAFKA_CLUSTER_ID=$(./bin/kafka-storage.sh random-uuid)
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Bước 2: Start Kafka
nohup ./bin/kafka-server-start.sh config/kraft/server.properties > /root/logs/kafka.log 2>&1 &

# Bước 3: Verify
ps aux | grep kafka
netstat -tuln | grep 9092
```

#### Nếu CÓ Zookeeper (Traditional Mode)
```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Bước 1: Start Zookeeper
nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties > /root/logs/zookeeper.log 2>&1 &

# Chờ 10 giây
sleep 10

# Bước 2: Start Kafka
nohup ./bin/kafka-server-start.sh config/server.properties > /root/logs/kafka.log 2>&1 &

# Chờ 15 giây
sleep 15

# Bước 3: Verify
ps aux | grep zookeeper
ps aux | grep kafka
netstat -tuln | grep 9092
```

---

## 🛑 Dừng Kafka

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Dừng Kafka
./bin/kafka-server-stop.sh

# Nếu dùng Zookeeper, dừng cả Zookeeper
./bin/zookeeper-server-stop.sh

# Force kill (nếu stop không work)
pkill -9 -f kafka.Kafka
pkill -9 -f QuorumPeerMain  # Zookeeper
```

---

## 📝 Tạo Topics

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Template
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic TOPIC_NAME

# Tạo 4 topics cho FinSeg
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic realtime_news
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic realtime_social
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic realtime_market
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic realtime_policy
```

---

## 🔍 Quản Lý Topics

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# List tất cả topics
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Xem chi tiết 1 topic
./bin/kafka-topics.sh --describe --topic realtime_news --bootstrap-server localhost:9092

# Xóa topic
./bin/kafka-topics.sh --delete --topic realtime_news --bootstrap-server localhost:9092

# Modify partitions (chỉ tăng, không giảm được)
./bin/kafka-topics.sh --alter --topic realtime_news --partitions 5 --bootstrap-server localhost:9092
```

---

## 📤 Test Producer (Gửi Message)

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Console producer (gõ messages, Ctrl+C để thoát)
./bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news

# Hoặc gửi từ file
cat test_messages.txt | ./bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news

# Echo 1 message
echo '{"test":"hello kafka"}' | ./bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news
```

---

## 📥 Test Consumer (Đọc Messages)

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Đọc từ đầu
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news \
  --from-beginning

# Đọc messages mới nhất (real-time)
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news

# Đọc với consumer group
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news \
  --group test-group \
  --from-beginning

# Đọc kèm key và timestamp
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news \
  --property print.key=true \
  --property print.timestamp=true \
  --from-beginning
```

---

## 📊 Monitoring

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Consumer groups
./bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Consumer group lag (độ trễ)
./bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group my-group \
  --describe

# Broker info
./bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Check log directories
./bin/kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe
```

---

## 🧪 Test Python Connection

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Activate venv
source .venv/bin/activate

# Run test script
python3 test_kafka_connection.py

# Quick test
python3 -c "
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('realtime_news', {'test': 'hello'})
producer.flush()
print('✅ Kafka OK')
"
```

---

## 📋 Check Status

```bash
# Kafka process
ps aux | grep kafka.Kafka

# Zookeeper process (nếu dùng)
ps aux | grep QuorumPeerMain

# Port 9092
netstat -tuln | grep 9092

# Logs
tail -f /root/logs/kafka.log
tail -f /root/logs/zookeeper.log  # nếu dùng
```

---

## ❗ Troubleshooting

### Lỗi: "Connection refused"
```bash
# Check Kafka có chạy không
ps aux | grep kafka

# Check port
netstat -tuln | grep 9092

# Check logs
tail -n 100 /root/logs/kafka.log
```

### Lỗi: "Topic does not exist"
```bash
# List topics
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Tạo topic nếu chưa có
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_news
```

### Lỗi: "Address already in use"
```bash
# Kill process đang chiếm port 9092
lsof -ti:9092 | xargs kill -9

# Hoặc stop Kafka đúng cách
./bin/kafka-server-stop.sh
```

### Reset Kafka (xóa toàn bộ data)
```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Stop Kafka
./bin/kafka-server-stop.sh
./bin/zookeeper-server-stop.sh  # nếu dùng

# Xóa data directories
rm -rf /tmp/kafka-logs
rm -rf /tmp/zookeeper
rm -rf /tmp/kraft-combined-logs  # KRaft mode

# Format lại và start lại
# (xem phần "Khởi Động Nhanh" ở trên)
```

---

## 🎯 Production Workflow

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# 1. Setup Kafka (1 lần duy nhất)
bash setup_kafka.sh

# 2. Test connection
python3 test_kafka_connection.py

# 3. Test 1 crawl cycle
python3 realtime_pipeline/scheduler.py --ticker SHB --once

# 4. Start production
bash realtime_pipeline/manage_processes.sh start

# 5. Check status
bash realtime_pipeline/manage_processes.sh status

# 6. View logs
bash realtime_pipeline/manage_processes.sh logs

# 7. Stop when needed
bash realtime_pipeline/manage_processes.sh stop
```

---

## 📚 Tài Liệu Tham Khảo

- [Kafka Quick Start](https://kafka.apache.org/quickstart)
- [KRaft Mode](https://kafka.apache.org/documentation/#kraft)
- [kafka-python Docs](https://kafka-python.readthedocs.io/)
