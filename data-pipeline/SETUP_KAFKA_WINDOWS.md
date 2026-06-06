# Setup Kafka trên Windows - Hướng dẫn đầy đủ

## 🎯 Mục tiêu

Cài đặt Kafka trên Windows để dashboard có thể:
- ✅ Lấy dữ liệu realtime từ Kafka topics
- ✅ Tính toán VMSI với dữ liệu streaming
- ✅ Cập nhật tương tác trên frontend qua Kafka

---

## ⚡ Cách 1: Sử dụng Docker (KHUYẾN NGHỊ - DỄ NHẤT)

### Bước 1: Cài Docker Desktop

1. Download Docker Desktop cho Windows:
   - Link: https://www.docker.com/products/docker-desktop
   - Chọn phiên bản Windows

2. Cài đặt Docker Desktop:
   - Chạy file installer
   - Restart máy khi được yêu cầu

3. Verify Docker đã cài:
   ```cmd
   docker --version
   docker-compose --version
   ```

### Bước 2: Khởi động Kafka bằng Docker Compose

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline
docker-compose -f docker-compose.kafka.yml up -d
```

**Output mong đợi:**
```
Creating network "data-pipeline_default" with the default driver
Creating kafka ... done
```

### Bước 3: Verify Kafka đang chạy

```cmd
docker ps
```

**Output mong đợi:**
```
CONTAINER ID   IMAGE                    PORTS                    STATUS
xxxxxxxxxxxx   confluentinc/cp-kafka    0.0.0.0:9092->9092/tcp   Up 30 seconds
```

### Bước 4: Test kết nối Kafka

```cmd
python test_kafka_connection.py
```

**Output mong đợi:**
```
✅ Kafka connection OK!
✅ Topics: fb_mock_data, realtime_market, realtime_policy
```

---

## 🔧 Cách 2: Cài Kafka Native trên Windows (Advanced)

### Bước 1: Cài Java

Kafka cần Java 11 hoặc cao hơn.

**Download Java:**
- Link: https://adoptium.net/temurin/releases/
- Chọn: Windows x64, JDK 11 hoặc 17

**Cài đặt Java:**
1. Chạy installer
2. Thêm Java vào PATH:
   ```cmd
   setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-11.0.XX-hotspot"
   setx PATH "%PATH%;%JAVA_HOME%\bin"
   ```

**Verify Java:**
```cmd
java -version
```

### Bước 2: Download Kafka

1. Download Kafka binary:
   - Link: https://kafka.apache.org/downloads
   - Chọn: Binary downloads → kafka_2.13-3.6.0.tgz (hoặc mới hơn)

2. Giải nén vào `C:\kafka`:
   ```cmd
   # Dùng 7-Zip hoặc WinRAR
   # Giải nén vào: C:\kafka
   ```

### Bước 3: Cấu hình Kafka

**Tạo thư mục data:**
```cmd
mkdir C:\kafka\data\zookeeper
mkdir C:\kafka\data\kafka
```

**Sửa file `C:\kafka\config\zookeeper.properties`:**
```properties
dataDir=C:/kafka/data/zookeeper
clientPort=2181
```

**Sửa file `C:\kafka\config\server.properties`:**
```properties
log.dirs=C:/kafka/data/kafka
zookeeper.connect=localhost:2181
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
```

### Bước 4: Khởi động Zookeeper

**Mở Command Prompt (Terminal 1):**
```cmd
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

**Output mong đợi:**
```
[2024-01-01 10:00:00,000] INFO binding to port 0.0.0.0/0.0.0.0:2181
```

### Bước 5: Khởi động Kafka Server

**Mở Command Prompt mới (Terminal 2):**
```cmd
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Output mong đợi:**
```
[2024-01-01 10:00:05,000] INFO [KafkaServer id=0] started
```

### Bước 6: Tạo Kafka Topics

**Mở Command Prompt mới (Terminal 3):**
```cmd
cd C:\kafka

# Tạo topic cho social data
.\bin\windows\kafka-topics.bat --create --topic fb_mock_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Tạo topic cho market data
.\bin\windows\kafka-topics.bat --create --topic realtime_market --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Tạo topic cho policy data
.\bin\windows\kafka-topics.bat --create --topic realtime_policy --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Bước 7: Verify Topics

```cmd
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

**Output mong đợi:**
```
fb_mock_data
realtime_market
realtime_policy
```

---

## ✅ Bước tiếp theo: Enable Kafka trong Dashboard

### 1. Sửa `dashboard_realtime.py`

Tìm dòng:
```python
return RealtimeVMSIEngine(ticker=ticker, kafka_enabled=False)
```

Thay thành:
```python
return RealtimeVMSIEngine(ticker=ticker, kafka_enabled=True)
```

### 2. Restart Dashboard

```cmd
# Ctrl+C để dừng dashboard hiện tại
# Chạy lại:
streamlit run dashboard_realtime.py
```

---

## 🚀 Chạy hệ thống đầy đủ

### Terminal 1: Scheduler (Crawl + Push Kafka)

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline\realtime_pipeline
python scheduler.py --ticker SHB
```

**Output mong đợi:**
```
[INFO] Crawling Facebook posts...
[INFO] [Social] 10 Facebook posts → Kafka
[INFO] [Social] 15 news articles → Kafka
[INFO] [Market] 30 bars → Kafka
[INFO] VMSI = 65.3 / 100
```

### Terminal 2: Vector Worker (Embedding + ChromaDB)

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline\realtime_pipeline
python run_vector_worker.py
```

**Output mong đợi:**
```
[INFO] Vector Worker started
[INFO] Listening to Kafka topic: realtime_policy
[INFO] Processing document: NHNN_doc_001
[INFO] Embedded 5 chunks → ChromaDB
```

### Terminal 3: Dashboard Realtime

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline
streamlit run dashboard_realtime.py
```

**Mở browser:** http://localhost:8501

---

## 🔍 Test Kafka hoạt động

### Test 1: Gửi message test

```cmd
cd C:\kafka
echo "Test message" | .\bin\windows\kafka-console-producer.bat --topic fb_mock_data --bootstrap-server localhost:9092
```

### Test 2: Đọc message từ topic

```cmd
.\bin\windows\kafka-console-consumer.bat --topic fb_mock_data --from-beginning --bootstrap-server localhost:9092
```

### Test 3: Python test script

**Tạo file `test_kafka_full.py`:**
```python
from kafka import KafkaProducer, KafkaConsumer
import json
import time

# Test Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_data = {
    "ticker": "SHB",
    "content": "Test message from Python",
    "timestamp": time.time()
}

producer.send('fb_mock_data', test_data)
producer.flush()
print("✅ Sent message to Kafka")

# Test Consumer
consumer = KafkaConsumer(
    'fb_mock_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    consumer_timeout_ms=5000,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🔊 Listening for messages...")
for message in consumer:
    print(f"✅ Received: {message.value}")
    break

print("✅ Kafka test completed!")
```

**Chạy test:**
```cmd
python test_kafka_full.py
```

---

## 🐛 Troubleshooting

### ❌ Lỗi: Docker không khởi động được

**Giải pháp:**
1. Kiểm tra Hyper-V đã enable:
   ```cmd
   # Windows Home: Cài WSL 2
   wsl --install
   
   # Windows Pro: Enable Hyper-V
   # Settings → Apps → Optional Features → More Windows Features → Hyper-V
   ```

2. Restart Docker Desktop

3. Verify Docker status:
   ```cmd
   docker info
   ```

### ❌ Lỗi: Port 9092 đã được sử dụng

**Giải pháp:**
```cmd
# Kiểm tra process đang dùng port 9092
netstat -ano | findstr :9092

# Kill process (thay PID bằng số từ lệnh trên)
taskkill /PID <PID> /F
```

### ❌ Lỗi: Kafka timeout khi connect

**Giải pháp:**

1. Kiểm tra Kafka đang chạy:
   ```cmd
   # Docker:
   docker ps | findstr kafka
   
   # Native:
   # Kiểm tra Terminal 1 & 2 có đang chạy không
   ```

2. Kiểm tra firewall:
   ```cmd
   # Allow port 9092
   netsh advfirewall firewall add rule name="Kafka" dir=in action=allow protocol=TCP localport=9092
   ```

3. Test connection:
   ```cmd
   telnet localhost 9092
   # Nếu connected → Kafka OK
   ```

### ❌ Lỗi: Java not found

**Giải pháp:**
```cmd
# Kiểm tra JAVA_HOME
echo %JAVA_HOME%

# Set JAVA_HOME nếu chưa có
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.XX-hotspot"
setx PATH "%PATH%;%JAVA_HOME%\bin"

# Restart Command Prompt
```

---

## 📊 Kiểm tra hệ thống hoạt động

### 1. Kiểm tra Kafka topics có data

```cmd
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --topic fb_mock_data --from-beginning --bootstrap-server localhost:9092 --max-messages 5
```

### 2. Kiểm tra Dashboard nhận data

1. Mở dashboard: http://localhost:8501
2. Click nút "▶ PHÂN TÍCH"
3. Xem logs trong dashboard → phải thấy:
   ```
   ✅ Crawl Facebook + tin tức...
   ✅ 10 Facebook posts → Kafka
   ✅ 15 news articles → Kafka
   ✅ VMSI = 65.3 / 100
   ```

### 3. Kiểm tra Vector Worker xử lý

```cmd
# Xem logs của Vector Worker
# Phải thấy:
[INFO] Received message from Kafka
[INFO] Chunking document...
[INFO] Embedding 5 chunks...
[INFO] Ingested to ChromaDB
```

---

## 🎯 Workflow đầy đủ với Kafka

```
┌──────────────────────────────────────────────────────────────┐
│  1. SCHEDULER (Crawl + Push Kafka)                          │
│     ├─ Crawl Facebook → Kafka: fb_mock_data                 │
│     ├─ Crawl News     → Kafka: fb_mock_data                 │
│     ├─ Crawl NHNN     → Kafka: realtime_policy              │
│     ├─ Crawl Stock    → Kafka: realtime_market              │
│     └─ Calculate VMSI → live_vmsi.json                      │
└──────────────────────────────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  2. KAFKA CLUSTER (localhost:9092)                          │
│     ├─ Topic: fb_mock_data      (Social sentiment)         │
│     ├─ Topic: realtime_market   (Stock prices)             │
│     └─ Topic: realtime_policy   (NHNN documents)           │
└──────────────────────────────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  3. VECTOR WORKER (Kafka Consumer)                          │
│     ├─ Listen: realtime_policy                              │
│     ├─ Chunking: 1500 chars, overlap 200                    │
│     ├─ Embedding: vietnamese-sbert                          │
│     └─ Ingest: ChromaDB Cloud                               │
└──────────────────────────────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  4. DASHBOARD (Streamlit)                                    │
│     ├─ Read: live_vmsi.json                                 │
│     ├─ Click "CẬP NHẬT STREAM" → Crawl → Push Kafka        │
│     └─ Display: VMSI score + Charts + News                  │
└──────────────────────────────────────────────────────────────┘
```

---

## ✅ Checklist

### Cài đặt Kafka:
- [ ] Docker Desktop installed (Cách 1)
- [ ] hoặc Java + Kafka binary (Cách 2)
- [ ] Kafka running on port 9092
- [ ] 3 topics created: fb_mock_data, realtime_market, realtime_policy

### Enable Kafka trong code:
- [ ] Sửa `dashboard_realtime.py`: `kafka_enabled=True`
- [ ] Restart dashboard

### Chạy hệ thống:
- [ ] Terminal 1: `python scheduler.py --ticker SHB`
- [ ] Terminal 2: `python run_vector_worker.py`
- [ ] Terminal 3: `streamlit run dashboard_realtime.py`

### Verify:
- [ ] Dashboard hiển thị VMSI score
- [ ] Logs scheduler có "→ Kafka"
- [ ] Vector Worker có "Ingested to ChromaDB"
- [ ] Dashboard click "CẬP NHẬT STREAM" hoạt động

---

## 📖 Tài liệu liên quan

- **docker-compose.kafka.yml** - Cấu hình Kafka Docker
- **QUICKSTART_WINDOWS.md** - Hướng dẫn chạy trên Windows
- **README_INTEGRATION.md** - Chi tiết kiến trúc hệ thống

---

Made with ❤️ by FinSent-Agent Team
