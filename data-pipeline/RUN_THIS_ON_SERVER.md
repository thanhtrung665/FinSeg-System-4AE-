# 🚀 Chạy Các Lệnh Này Trên GPU Server

## 📍 Vị Trí: `/root/FinSeg-System-4AE-/data-pipeline`

---

## ✅ BƯỚC 1: Dừng Kafka Cũ (nếu đang chạy)

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Dừng Kafka
./bin/kafka-server-stop.sh

# Chờ 5 giây
sleep 5

# Verify đã dừng
ps aux | grep kafka
```

**Kết quả mong đợi:** Không còn process nào chứa "kafka.Kafka"

---

## ✅ BƯỚC 2: Xác Định Mode (KRaft hay Traditional)

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Kiểm tra file config
ls -la config/ | grep zookeeper
```

**Nếu KHÔNG có `zookeeper.properties`:**
➡️ Bạn sẽ dùng **KRaft Mode** (không cần Zookeeper riêng)

**Nếu CÓ `zookeeper.properties`:**
➡️ Bạn sẽ dùng **Traditional Mode** (cần start Zookeeper trước)

---

## ✅ BƯỚC 3a: Start Kafka - KRaft Mode (KHÔNG CÓ zookeeper.properties)

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Tạo thư mục logs
mkdir -p /root/logs

# Format storage (chỉ chạy 1 lần duy nhất)
KAFKA_CLUSTER_ID=$(./bin/kafka-storage.sh random-uuid)
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Start Kafka
nohup ./bin/kafka-server-start.sh config/kraft/server.properties > /root/logs/kafka.log 2>&1 &

# Chờ 15 giây
sleep 15

# Verify
ps aux | grep kafka.Kafka
```

**Kết quả mong đợi:** Có 1 process "kafka.Kafka" đang chạy

---

## ✅ BƯỚC 3b: Start Kafka - Traditional Mode (CÓ zookeeper.properties)

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Tạo thư mục logs
mkdir -p /root/logs

# Bước 1: Start Zookeeper
nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties > /root/logs/zookeeper.log 2>&1 &

# Chờ 10 giây
sleep 10

# Verify Zookeeper
ps aux | grep QuorumPeerMain

# Bước 2: Start Kafka
nohup ./bin/kafka-server-start.sh config/server.properties > /root/logs/kafka.log 2>&1 &

# Chờ 15 giây
sleep 15

# Verify Kafka
ps aux | grep kafka.Kafka
```

**Kết quả mong đợi:** 
- Có 1 process "QuorumPeerMain" (Zookeeper)
- Có 1 process "kafka.Kafka"

---

## ✅ BƯỚC 4: Cài net-tools và Verify Port

```bash
# Cài net-tools
apt-get update && apt-get install -y net-tools

# Kiểm tra port 9092
netstat -tuln | grep 9092
```

**Kết quả mong đợi:**
```
tcp6       0      0 :::9092                 :::*                    LISTEN
```

---

## ✅ BƯỚC 5: Tạo 4 Topics

```bash
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0

# Topic 1
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_news

# Topic 2
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_social

# Topic 3
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_market

# Topic 4
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

./bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Kết quả mong đợi:**
```
realtime_market
realtime_news
realtime_policy
realtime_social
```

---

## ✅ BƯỚC 7: Test Kafka Connection từ Python

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Activate venv
source .venv/bin/activate

# Test connection
python3 test_kafka_connection.py
```

**Kết quả mong đợi:**
```
🎉 ALL TESTS PASSED! Kafka is ready.
```

---

## ✅ BƯỚC 8: Test 1 Crawl Cycle

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# QUAN TRỌNG: Phải cd vào thư mục này trước!
# Nếu không sẽ bị lỗi "No module named 'realtime_pipeline'"

python3 realtime_pipeline/scheduler.py --ticker SHB --once
```

**Kết quả mong đợi:**
- Crawl news từ CafeF, Vietstock, ChinhPhu
- Crawl stock data từ vnstock
- Crawl NHNN documents
- Gửi data vào Kafka
- Vector Worker xử lý và đưa vào ChromaDB

---

## ✅ BƯỚC 9: Start Production

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

bash realtime_pipeline/manage_processes.sh start
```

---

## ✅ BƯỚC 10: Check Status

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Check status
bash realtime_pipeline/manage_processes.sh status

# View logs
bash realtime_pipeline/manage_processes.sh logs
```

---

## 🛑 Stop System

```bash
cd /root/FinSeg-System-4AE-/data-pipeline

# Stop all processes
bash realtime_pipeline/manage_processes.sh stop

# Stop Kafka
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0
./bin/kafka-server-stop.sh

# Nếu dùng Zookeeper
./bin/zookeeper-server-stop.sh
```

---

## 📊 Giám Sát

```bash
# Kafka logs
tail -f /root/logs/kafka.log

# Scheduler logs
tail -f /root/FinSeg-System-4AE-/data-pipeline/logs/scheduler_*.log

# Vector Worker logs
tail -f /root/FinSeg-System-4AE-/data-pipeline/logs/vector_worker_*.log

# Check Kafka topics
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic realtime_news \
  --from-beginning
```

---

## ❗ Nếu Gặp Lỗi

### Lỗi: "No module named 'realtime_pipeline'"
```bash
# Fix: Phải cd vào đúng thư mục
cd /root/FinSeg-System-4AE-/data-pipeline

# RỒI MỚI chạy
python3 realtime_pipeline/scheduler.py --ticker SHB --once
```

### Lỗi: "Connection refused" khi connect Kafka
```bash
# Kiểm tra Kafka có chạy không
ps aux | grep kafka

# Kiểm tra port
netstat -tuln | grep 9092

# Xem logs
tail -n 100 /root/logs/kafka.log
```

### Lỗi: "Topic does not exist"
```bash
# Tạo lại topics (xem BƯỚC 5)
cd /root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0
./bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic realtime_news
```

---

## 📝 Checklist

- [ ] Kafka đã start (KRaft hoặc Traditional mode)
- [ ] Port 9092 listening
- [ ] 4 topics đã tạo: realtime_news, realtime_social, realtime_market, realtime_policy
- [ ] Python Kafka connection test PASS
- [ ] Test cycle chạy OK (`--ticker SHB --once`)
- [ ] Production system start OK

---

## 🎯 TL;DR - Cách Nhanh Nhất

```bash
# 1. Cd vào thư mục
cd /root/FinSeg-System-4AE-/data-pipeline

# 2. Chạy script tự động
bash setup_kafka.sh

# 3. Test
python3 test_kafka_connection.py

# 4. Test crawl
python3 realtime_pipeline/scheduler.py --ticker SHB --once

# 5. Start production
bash realtime_pipeline/manage_processes.sh start

# DONE! 🎉
```
