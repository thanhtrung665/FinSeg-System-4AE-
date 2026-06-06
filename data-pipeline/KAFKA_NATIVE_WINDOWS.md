# Kafka Native trên Windows - Hướng dẫn đầy đủ

## 🎯 Mục tiêu

Cài đặt Kafka native trên Windows (không dùng Docker) để:
- ✅ Dashboard lấy dữ liệu realtime từ Kafka
- ✅ Tính toán VMSI với streaming data
- ✅ Cập nhật frontend qua Kafka topics

---

## ⚡ Quick Start (3 bước)

### Bước 1: Download và Setup Kafka tự động

**Chạy PowerShell script:**
```powershell
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline
powershell -ExecutionPolicy Bypass -File download_kafka.ps1
```

Script sẽ:
- ✅ Kiểm tra Java
- ✅ Download Kafka tự động
- ✅ Giải nén và cấu hình
- ✅ Tạo scripts khởi động

### Bước 2: Khởi động Kafka

**Option A: Chạy tất cả một lệnh (Khuyến nghị):**
```cmd
run_all_kafka.bat
```

**Option B: Chạy từng bước (để debug):**
```cmd
# Terminal 1
start_zookeeper.bat

# Terminal 2 (đợi 10 giây)
start_kafka.bat

# Terminal 3 (đợi 15 giây)
create_topics.bat
```

### Bước 3: Test Kafka

```cmd
python test_kafka_connection.py
```

**Output mong đợi:**
```
============================================================
  KAFKA CONNECTION TEST
============================================================

[1/5] Kiem tra Kafka server...
✅ Kafka server dang chay tai: localhost:9092

[2/5] Kiem tra Kafka topics...
✅ Tat ca topics da san sang:
   - fb_mock_data
   - realtime_market
   - realtime_policy

[3/5] Test Kafka Producer...
✅ Producer OK - Sent to topic: fb_mock_data, partition: 0

[4/5] Test Kafka Consumer...
✅ Consumer OK - Listening to topic: fb_mock_data

[5/5] Test full Producer → Consumer workflow...
✅ Sent test message
✅ Received test message

============================================================
  ✅ TAT CA TESTS DA PASS!
============================================================
```

---

## 🔧 Setup Chi tiết (Nếu cần làm thủ công)

### 1. Cài Java (Bắt buộc)

Kafka cần Java 11+.

**Download:**
- Link: https://adoptium.net/temurin/releases/
- Chọn: Windows x64, JDK 17 (Khuyến nghị)

**Cài đặt:**
1. Chạy installer
2. Set JAVA_HOME:
   ```cmd
   setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.10+7"
   setx PATH "%PATH%;%JAVA_HOME%\bin"
   ```

**Verify:**
```cmd
java -version
```

### 2. Download Kafka

**Manual download:**
- Link: https://kafka.apache.org/downloads
- Chọn: kafka_2.13-3.6.0.tgz (hoặc mới hơn)

**Giải nén:**
```cmd
# Sử dụng 7-Zip hoặc WinRAR
# Giải nén vào: C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline\kafka
```

### 3. Cấu hình Kafka

**Tạo thư mục data:**
```cmd
mkdir kafka\data\zookeeper
mkdir kafka\data\kafka-logs
```

**Sửa `kafka\config\zookeeper.properties`:**
```properties
dataDir=C:/Users/asus/Downloads/FinSeg-System-4AE-/data-pipeline/kafka/data/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
```

**Sửa `kafka\config\server.properties`:**
```properties
broker.id=0
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=C:/Users/asus/Downloads/FinSeg-System-4AE-/data-pipeline/kafka/data/kafka-logs
num.partitions=3
zookeeper.connect=localhost:2181
```

---

## 🚀 Chạy hệ thống đầy đủ

### Terminal 1: Kafka Infrastructure

```cmd
# Kafka da chay roi (tu run_all_kafka.bat)
# Hoac check status:
docker ps # (neu dung Docker)

# Hoac kiem tra processes:
tasklist | findstr java
```

### Terminal 2: Scheduler (Crawl + Push Kafka)

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
[INFO] [Policy] 5 NHNN docs → Kafka
[INFO] Calculating VMSI...
[INFO] VMSI = 65.3 / 100
[INFO] Saved to live_vmsi.json
```

### Terminal 3: Vector Worker (Embedding + ChromaDB)

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline\realtime_pipeline
python run_vector_worker.py
```

**Output mong đợi:**
```
[INFO] Vector Worker started
[INFO] Kafka Consumer connected: localhost:9092
[INFO] Listening to topic: realtime_policy
[INFO] Received message: NHNN_doc_001
[INFO] Chunking document (1500 chars, overlap 200)...
[INFO] Created 5 chunks
[INFO] Embedding chunks...
[INFO] Ingested to ChromaDB: 5 chunks
```

### Terminal 4: Dashboard

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline
streamlit run dashboard_realtime.py
```

**Mở browser:** http://localhost:8501

---

## ✅ Enable Kafka trong Dashboard

Dashboard hiện đang ở chế độ `kafka_enabled=False`. Để enable Kafka:

### Cách 1: Sửa code (Permanent)

**File: `dashboard_realtime.py`**

Tìm dòng:
```python
return RealtimeVMSIEngine(ticker=ticker, kafka_enabled=False)
```

Thay thành:
```python
return RealtimeVMSIEngine(ticker=ticker, kafka_enabled=True)
```

**Restart dashboard:**
```cmd
# Ctrl+C để dừng
streamlit run dashboard_realtime.py
```

### Cách 2: Environment variable (Temporary)

```cmd
set KAFKA_ENABLED=true
streamlit run dashboard_realtime.py
```

---

## 📊 Kiểm tra Kafka hoạt động

### 1. List topics

```cmd
cd kafka
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

**Output:**
```
fb_mock_data
realtime_market
realtime_policy
```

### 2. Consume messages từ topic

```cmd
.\bin\windows\kafka-console-consumer.bat --topic fb_mock_data --from-beginning --bootstrap-server localhost:9092 --max-messages 5
```

### 3. Produce test message

```cmd
echo {"test": "message"} | .\bin\windows\kafka-console-producer.bat --topic fb_mock_data --bootstrap-server localhost:9092
```

### 4. Check topic details

```cmd
.\bin\windows\kafka-topics.bat --describe --topic fb_mock_data --bootstrap-server localhost:9092
```

---

## 🎯 Workflow đầy đủ với Kafka

```
┌─────────────────────────────────────────────────────────┐
│  USER ACTION                                            │
│  - Click "▶ PHÂN TÍCH" trong dashboard                │
└─────────────────────────┬───────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────┐
│  SCHEDULER (scheduler.py)                               │
│  1. Crawl Facebook posts                                │
│  2. Crawl News articles                                 │
│  3. Crawl NHNN documents                                │
│  4. Get stock prices                                    │
│  5. Normalize data                                      │
│  6. Push to Kafka topics →→→                           │
└─────────────────────────┬───────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────┐
│  KAFKA CLUSTER (localhost:9092)                         │
│  • Topic: fb_mock_data      (Social sentiment)         │
│  • Topic: realtime_market   (Stock prices)             │
│  • Topic: realtime_policy   (NHNN documents)           │
└────────────┬────────────────────────────┬───────────────┘
             ▼                            ▼
┌────────────────────────┐  ┌────────────────────────────┐
│  VECTOR WORKER         │  │  MAC SYSTEM                │
│  (run_vector_worker)   │  │  (Multi-Agent)             │
│                        │  │                            │
│  • Consume policy data │  │  • Social Agent            │
│  • Chunking            │  │    (consume social data)   │
│  • Embedding           │  │  • Macro Agent             │
│  • Ingest ChromaDB     │  │    (RAG query ChromaDB)    │
└────────────────────────┘  │  • Risk Agent              │
                            │    (calculate VMSI)        │
                            └────────────┬───────────────┘
                                         ▼
                            ┌────────────────────────────┐
                            │  live_vmsi.json            │
                            │  {                         │
                            │    "vmsi": 65.3,           │
                            │    "status": "normal",     │
                            │    ...                     │
                            │  }                         │
                            └────────────┬───────────────┘
                                         ▼
┌─────────────────────────────────────────────────────────┐
│  DASHBOARD (streamlit)                                  │
│  • Read live_vmsi.json                                  │
│  • Display charts + VMSI score                          │
│  • User clicks "CẬP NHẬT STREAM" → Trigger scheduler   │
└─────────────────────────────────────────────────────────┘
```

---

## 🐛 Troubleshooting

### ❌ Lỗi: Java not found

```cmd
java -version
# Neu loi → Download Java tai: https://adoptium.net/temurin/releases/
```

### ❌ Lỗi: Port 9092 đã được sử dụng

```cmd
# Kiem tra process
netstat -ano | findstr :9092

# Kill process (thay PID)
taskkill /PID <PID> /F
```

### ❌ Lỗi: Zookeeper connection timeout

**Nguyên nhân:** Zookeeper chưa chạy hoặc đã stop.

**Giải pháp:**
```cmd
# Restart Zookeeper
start_zookeeper.bat

# Đợi 10 giây, rồi restart Kafka
start_kafka.bat
```

### ❌ Lỗi: Topic does not exist

```cmd
# Tao lai topics
create_topics.bat
```

### ❌ Dashboard vẫn timeout

**Kiểm tra:**
```cmd
# 1. Kafka dang chay?
tasklist | findstr java

# 2. Topics da co?
python test_kafka_connection.py

# 3. Dashboard da enable Kafka?
# Check file dashboard_realtime.py: kafka_enabled=True
```

---

## 🛑 Stop Kafka

```cmd
stop_kafka.bat
```

Hoặc thủ công:
```cmd
taskkill /FI "WindowTitle eq Kafka*" /T /F
taskkill /FI "WindowTitle eq Zookeeper*" /T /F
```

---

## ✅ Checklist đầy đủ

### Setup Kafka:
- [ ] Java 11+ đã cài
- [ ] Kafka binary đã download
- [ ] `run_all_kafka.bat` đã chạy
- [ ] 3 topics đã được tạo
- [ ] `test_kafka_connection.py` pass

### Enable Kafka trong code:
- [ ] `dashboard_realtime.py`: `kafka_enabled=True`
- [ ] Dashboard đã restart

### Chạy hệ thống:
- [ ] Terminal 1: Kafka (run_all_kafka.bat)
- [ ] Terminal 2: Scheduler (scheduler.py)
- [ ] Terminal 3: Vector Worker (run_vector_worker.py)
- [ ] Terminal 4: Dashboard (streamlit)

### Verify:
- [ ] Dashboard hiển thị VMSI
- [ ] Scheduler logs có "→ Kafka"
- [ ] Vector Worker logs có "Ingested to ChromaDB"
- [ ] Click "▶ PHÂN TÍCH" hoạt động
- [ ] Click "↻ CẬP NHẬT STREAM" hoạt động

---

## 📖 Files quan trọng

| File | Mô tả |
|------|-------|
| `download_kafka.ps1` | Script tự động download Kafka |
| `run_all_kafka.bat` | Chạy tất cả Kafka một lệnh |
| `start_zookeeper.bat` | Khởi động Zookeeper |
| `start_kafka.bat` | Khởi động Kafka Server |
| `create_topics.bat` | Tạo 3 topics cần thiết |
| `stop_kafka.bat` | Dừng Kafka và Zookeeper |
| `test_kafka_connection.py` | Test Kafka hoạt động |

---

Made with ❤️ by FinSent-Agent Team
