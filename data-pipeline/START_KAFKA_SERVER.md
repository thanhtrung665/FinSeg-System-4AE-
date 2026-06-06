# Hướng Dẫn Khởi Động Kafka trên GPU Server

## 📍 Thông Tin Hiện Tại
- **Server Path**: `/root/FinSeg-System-4AE-/data-pipeline`
- **Kafka Path**: `/root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0`
- **Kafka Version**: 4.3.0 (hỗ trợ KRaft mode nhưng có thể chạy traditional mode)
- **Kafka Process**: Đã running (PID 20564)

## ⚠️ LƯU Ý QUAN TRỌNG

Kafka 4.3.0+ **KHÔNG CẦN** Zookeeper riêng nữa nếu dùng **KRaft mode**.
Nếu bạn không có file `zookeeper.properties` → bạn đang dùng KRaft mode.

---

## 🚀 BƯỚC 1: Kiểm Tra Kafka Đã Chạy Chưa

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Kiểm tra process
ps aux | grep kafka

# Nếu có output chứa "kafka.Kafka" → Kafka đang chạy
# PID của bạn: 20564
```

---

## 🛑 BƯỚC 2: Dừng Kafka Cũ (nếu đang chạy sai config)

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Dừng Kafka
./bin/kafka-server-stop.sh

# Chờ 5 giây
sleep 5

# Verify đã dừng
ps aux | grep kafka
```

---

## ✅ BƯỚC 3: Khởi Động Kafka Đúng Cách

### Option A: Traditional Mode (dùng Zookeeper nội bộ)

**Nếu bạn có file `config/zookeeper.properties`:**

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Bước 1: Start Zookeeper (background)
nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties > /root/logs/zookeeper.log 2>&1 &

# Chờ 10 giây
sleep 10

# Bước 2: Start Kafka
nohup ./bin/kafka-server-start.sh config/server.properties > /root/logs/kafka.log 2>&1 &

# Verify
ps aux | grep zookeeper
ps aux | grep kafka
```

### Option B: KRaft Mode (KHÔNG CẦN Zookeeper)

**Nếu KHÔNG có file zookeeper.properties:**

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Bước 1: Format storage (chỉ chạy 1 lần đầu)
KAFKA_CLUSTER_ID=$(./bin/kafka-storage.sh random-uuid)
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Bước 2: Start Kafka KRaft
nohup ./bin/kafka-server-start.sh config/kraft/server.properties > /root/logs/kafka.log 2>&1 &

# Verify
ps aux | grep kafka
```

---

## 🔍 BƯỚC 4: Kiểm Tra Kafka Port

```bash
# Cài net-tools nếu chưa có
apt-get update && apt-get install -y net-tools

# Kiểm tra port 9092
netstat -tuln | grep 9092

# Kết quả mong đợi:
# tcp6       0      0 :::9092                 :::*                    LISTEN
```

---

## 📝 BƯỚC 5: Tạo Các Topic Cần Thiết

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Topic 1: realtime_news (tin tức từ CafeF, Vietstock, ChinhPhu)
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_news

# Topic 2: realtime_social (Facebook posts/comments)
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_social

# Topic 3: realtime_market (giá cổ phiếu vnstock)
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_market

# Topic 4: realtime_policy (văn bản NHNN)
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_policy
```

---

## ✅ BƯỚC 6: Verify Topics

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# List tất cả topics
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Kết quả mong đợi:
# realtime_news
# realtime_social
# realtime_market
# realtime_policy
```

---

## 🧪 BƯỚC 7: Test Kafka Connection từ Python

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Activate venv
source .venv/bin/activate

# Test connection
python3 -c "
from kafka import KafkaProducer, KafkaConsumer
import json

# Test Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
producer.send('realtime_news', {'test': 'hello kafka'})
producer.flush()
print('✅ Producer OK')

# Test Consumer
consumer = KafkaConsumer(
    'realtime_news',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000
)
for msg in consumer:
    print(f'✅ Consumer OK: {msg.value}')
    break
"
```

---

## 🎯 BƯỚC 8: Chạy Test Cycle

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Test 1 lần crawl cho SHB
python3 realtime_pipeline/scheduler.py --ticker SHB --once

# Kiểm tra logs
tail -f logs/scheduler_*.log
```

---

## 🚀 BƯỚC 9: Start Production System

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Start tất cả services
bash realtime_pipeline/manage_processes.sh start

# Check status
bash realtime_pipeline/manage_processes.sh status

# View logs
bash realtime_pipeline/manage_processes.sh logs
```

---

## 🛠️ Các Lệnh Quản Lý Kafka

```bash
# 1. Check Kafka logs
tail -f /root/logs/kafka.log

# 2. Check Zookeeper logs (nếu dùng traditional mode)
tail -f /root/logs/zookeeper.log

# 3. List topics
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# 4. Describe topic
./bin/kafka-topics.sh --describe --topic realtime_news --bootstrap-server localhost:9092

# 5. Delete topic
./bin/kafka-topics.sh --delete --topic realtime_news --bootstrap-server localhost:9092

# 6. Consumer test (đọc messages)
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news \
  --from-beginning

# 7. Producer test (gửi messages)
echo "test message" | ./bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news
```

---

## ❗ Troubleshooting

### Lỗi: "Address already in use"
```bash
# Port 9092 đã bị chiếm → kill process cũ
lsof -ti:9092 | xargs kill -9

# Hoặc dừng Kafka đúng cách
./bin/kafka-server-stop.sh
```

### Lỗi: "Connection refused"
```bash
# Check Kafka có chạy không
ps aux | grep kafka

# Check port
netstat -tuln | grep 9092

# Check firewall
ufw status
ufw allow 9092
```

### Lỗi: "Topic does not exist"
```bash
# Tạo lại topic
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_news
```

---

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   KAFKA CLUSTER                     │
│                  (localhost:9092)                   │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Topic: realtime_news     (3 partitions)           │
│  Topic: realtime_social   (3 partitions)           │
│  Topic: realtime_market   (3 partitions)           │
│  Topic: realtime_policy   (3 partitions)           │
│                                                     │
└─────────────────────────────────────────────────────┘
          ▲                          │
          │                          │
     PRODUCER                   CONSUMER
          │                          │
          │                          ▼
┌─────────────────┐        ┌──────────────────┐
│  Scheduler.py   │        │ Vector Worker    │
│  - Crawlers     │        │ - Chunking       │
│  - VMSI         │        │ - Embedding      │
│  - Normalizers  │        │ - ChromaDB       │
└─────────────────┘        └──────────────────┘
```

---

## ✅ Checklist

- [ ] Kafka đã start (KRaft hoặc Traditional mode)
- [ ] Port 9092 đã listening
- [ ] 4 topics đã được tạo
- [ ] Python Kafka connection test pass
- [ ] Test cycle chạy thành công (`--ticker SHB --once`)
- [ ] Production system start OK

---

## 📞 Hỗ Trợ

Nếu gặp lỗi, check các log files:
- Kafka: `/root/logs/kafka.log`
- Zookeeper: `/root/logs/zookeeper.log` (nếu dùng)
- Scheduler: `/root/FinSeg-System-4AE-/data-pipeline/logs/scheduler_*.log`
- Vector Worker: `/root/FinSeg-System-4AE-/data-pipeline/logs/vector_worker_*.log`
