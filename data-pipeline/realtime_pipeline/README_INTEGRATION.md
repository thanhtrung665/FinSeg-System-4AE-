# Tích hợp Vector Worker vào Hệ thống Realtime

## Tổng quan

Vector Worker đã được tích hợp vào hệ thống realtime để xử lý **Chunking**, **Embedding**, và **ChromaDB Ingestion** một cách tối ưu và song song.

### Kiến trúc mới (Kafka-Native)

```
┌─────────────────────────────────────────────────────────────────────┐
│                       REALTIME PIPELINE                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────┐                                                    │
│  │  Scheduler  │ ← Chu kỳ 30 phút                                   │
│  └──────┬──────┘                                                    │
│         │                                                            │
│         v                                                            │
│  ┌──────────────────────────────────────────────────────┐          │
│  │          RealtimeVMSIEngine                           │          │
│  ├──────────────────────────────────────────────────────┤          │
│  │  1. Crawl Facebook + News                            │          │
│  │  2. Crawl NHNN policies                              │          │
│  │  3. Crawl Stock prices (vnstock)                     │          │
│  │  4. Normalize data                                   │          │
│  │  5. Push to Kafka ────────────────────┐             │          │
│  └──────────────────────────────────────│──────────────┘          │
│                                          │                           │
│                                          v                           │
│  ┌───────────────────────────────────────────────────────┐         │
│  │                  KAFKA CLUSTER                         │         │
│  ├───────────────────────────────────────────────────────┤         │
│  │  • fb_mock_data      (Social/News sentiment)          │         │
│  │  • realtime_market   (Stock prices)                   │         │
│  │  • realtime_policy   (NHNN docs for RAG) ─────┐      │         │
│  └───────────────────────────────────────────────│───────┘         │
│                                                   │                  │
│         ┌─────────────────────────────────────────┘                 │
│         │                                                            │
│         v                                                            │
│  ┌──────────────────────────────────────────────────────┐          │
│  │         Vector Worker (NEW!)                          │          │
│  ├──────────────────────────────────────────────────────┤          │
│  │  • Listen: realtime_policy, news_data                │          │
│  │  • Chunking: 1500 chars, overlap 200                 │          │
│  │  • Embedding: vietnamese-sbert                       │          │
│  │  • Ingest: ChromaDB Cloud                            │          │
│  └──────────────────────────────────────────────────────┘          │
│                                                                       │
│  ┌──────────────────────────────────────────────────────┐          │
│  │         MAC System (Social → Macro → Risk)            │          │
│  │  • Reads from Kafka                                   │          │
│  │  • Queries ChromaDB (updated by Vector Worker)       │          │
│  │  • Calculates VMSI                                    │          │
│  │  • Writes live_vmsi.json                             │          │
│  └──────────────────────────────────────────────────────┘          │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Các thay đổi chính

### 1. `vmsi_realtime.py`
- ✅ **Loại bỏ** logic gọi `push_policies_to_chroma()` trực tiếp
- ✅ **Thay thế** bằng `push_policies()` → đẩy vào Kafka topic `realtime_policy`
- ✅ Vector Worker sẽ tự động hứng và xử lý

**Trước:**
```python
ingested = self._get_producer().push_policies_to_chroma(normalized)
```

**Sau:**
```python
ingested = self._get_producer().push_policies(normalized)
# → Vector Worker sẽ tự động: chunk → embed → ingest ChromaDB
```

### 2. `realtime_producer.py`
- ✅ Đã có sẵn `PolicyRealtimeProducer` → push vào Kafka topic `realtime_policy`
- ✅ Không còn gọi ChromaDB trực tiếp (tách biệt concerns)

### 3. `vector_worker.py` (FILE MỚI)
- ✅ Kafka Consumer chuyên dụng
- ✅ Lắng nghe topics: `realtime_policy`, `news_data`
- ✅ Chunking văn bản: 1500 chars, overlap 200
- ✅ Embedding: `keepitreal/vietnamese-sbert`
- ✅ Ingest: ChromaDB Cloud với metadata đầy đủ

### 4. `run_vector_worker.py` (FILE MỚI)
- ✅ Entry point để chạy Vector Worker
- ✅ Graceful shutdown (SIGINT/SIGTERM)
- ✅ Logging đầy đủ

### 5. `manage_processes.sh` (FILE MỚI)
- ✅ Script quản lý tất cả processes
- ✅ Commands: `start`, `stop`, `restart`, `status`, `logs`
- ✅ Colorful terminal UI
- ✅ PID management

---

## Cách chạy hệ thống

### Option A: Dùng Script Quản Lý (KHUYẾN NGHỊ)

```bash
# Khởi động TẤT CẢ (Scheduler + Vector Worker + 2 Dashboards)
bash realtime_pipeline/manage_processes.sh start

# Kiểm tra trạng thái
bash realtime_pipeline/manage_processes.sh status

# Xem logs
bash realtime_pipeline/manage_processes.sh logs scheduler
bash realtime_pipeline/manage_processes.sh logs vector

# Dừng tất cả
bash realtime_pipeline/manage_processes.sh stop
```

### Option B: Chạy Thủ Công

```bash
# 1. Khởi động Kafka (BẮT BUỘC)
docker compose -f docker-compose.kafka.yml up -d

# 2. Scheduler (chu kỳ 30 phút)
nohup python3 realtime_pipeline/scheduler.py --ticker SHB \
  > logs/scheduler.log 2>&1 &

# 3. Vector Worker (lắng nghe Kafka)
nohup python3 realtime_pipeline/run_vector_worker.py \
  > logs/vector_worker.log 2>&1 &

# 4. Dashboard Realtime (port 8502)
nohup streamlit run dashboard_realtime.py \
  --server.port 8502 \
  --server.address 0.0.0.0 \
  --server.headless true \
  > logs/dashboard_rt.log 2>&1 &
```

---

## Lợi ích của kiến trúc mới

### 1. **Tách biệt Concerns (Separation of Concerns)**
- Scheduler: Thu thập dữ liệu + tính VMSI
- Vector Worker: Chunking + Embedding + Vector DB
- Mỗi component làm 1 việc và làm tốt

### 2. **Khả năng Scale**
- Có thể chạy nhiều Vector Worker instances
- Load balancing tự động qua Kafka Consumer Groups
- Không block Scheduler khi embedding chậm

### 3. **Fault Tolerance**
- Vector Worker crash → Scheduler vẫn chạy
- Scheduler crash → Vector Worker vẫn xử lý backlog
- Kafka giữ lại messages chưa xử lý

### 4. **Performance**
- Scheduler không phải đợi embedding (I/O bound)
- Chunking + Embedding chạy song song
- ChromaDB ingestion không block main flow

### 5. **Maintainability**
- Code rõ ràng, dễ debug
- Logs tách biệt cho từng component
- Dễ dàng thay đổi embedding model

---

## Monitoring

### Xem logs realtime

```bash
# Scheduler logs
tail -f logs/scheduler.log

# Vector Worker logs (quan trọng để theo dõi embedding)
tail -f logs/vector_worker.log

# Tất cả logs
tail -f logs/*.log
```

### Kiểm tra Kafka messages

```bash
# Install kafkacat (kcat)
# Ubuntu: sudo apt install kafkacat
# MacOS: brew install kafkacat

# Xem messages trong topic realtime_policy
kafkacat -b localhost:9092 -t realtime_policy -C -o end

# Xem tất cả topics
kafkacat -b localhost:9092 -L
```

### Kiểm tra ChromaDB

```python
import chromadb
import os

client = chromadb.HttpClient(
    host="api.trychroma.com",
    headers={"x-chroma-token": os.getenv("CHROMADB_API_KEY")},
    tenant=os.getenv("CHROMADB_TENANT"),
    database=os.getenv("CHROMADB_DATABASE")
)

collection = client.get_collection("realtime_policies")

# Đếm documents
count = collection.count()
print(f"Total documents: {count}")

# Query thử
results = collection.query(
    query_texts=["chính sách ngân hàng"],
    n_results=3
)
print(results)
```

---

## Troubleshooting

### Vector Worker không chạy

**Lỗi:** `ModuleNotFoundError: No module named 'sentence_transformers'`

```bash
pip install sentence-transformers
```

**Lỗi:** `kafka.errors.NoBrokersAvailable`

```bash
# Kiểm tra Kafka
docker compose -f docker-compose.kafka.yml ps
# Nếu không chạy:
docker compose -f docker-compose.kafka.yml up -d
```

### ChromaDB connection failed

**Lỗi:** `Authentication failed`

```bash
# Kiểm tra .env
cat .env | grep CHROMADB

# Đảm bảo có đầy đủ:
# CHROMADB_API_KEY=ck-xxx
# CHROMADB_TENANT=xxx
# CHROMADB_DATABASE=xxx
```

### Embedding quá chậm

**Giải pháp:** Chạy Vector Worker trên GPU

```bash
# Kiểm tra GPU
nvidia-smi

# Cài GPU packages
pip install bitsandbytes accelerate

# Chạy lại Vector Worker
bash realtime_pipeline/manage_processes.sh restart
```

### Kafka lag cao

**Nguyên nhân:** Vector Worker xử lý chậm hơn tốc độ produce

**Giải pháp:** Chạy nhiều Vector Worker instances

```bash
# Instance 1
python3 realtime_pipeline/run_vector_worker.py > logs/vector_worker_1.log 2>&1 &

# Instance 2
python3 realtime_pipeline/run_vector_worker.py > logs/vector_worker_2.log 2>&1 &
```

---

## Testing

### Test Vector Worker độc lập

```bash
# 1. Gửi test message vào Kafka
python3 -c "
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_msg = {
    'doc_id': 'test_001',
    'content_text': 'Ngân hàng Nhà nước ban hành chính sách mới về lãi suất. ' * 20,
    'title': 'Test NHNN Policy',
    'ticker_context': 'SHB',
    'source': 'test',
    'published_at': '2026-06-06'
}

producer.send('realtime_policy', test_msg)
producer.flush()
print('Sent test message to Kafka')
"

# 2. Xem Vector Worker logs
tail -f logs/vector_worker.log
# Mong đợi: "Đã nạp thành công 'Test NHNN Policy'..."
```

---

## Next Steps

1. ✅ **Hoàn thành**: Tích hợp Vector Worker
2. 📝 **Kế hoạch**: Monitoring dashboard cho Kafka lag
3. 📝 **Kế hoạch**: Auto-scaling Vector Worker dựa trên Kafka lag
4. 📝 **Kế hoạch**: A/B testing các embedding models khác nhau

---

## Liên hệ

Nếu gặp vấn đề, kiểm tra:
1. Logs: `logs/vector_worker.log`
2. Kafka: `docker compose -f docker-compose.kafka.yml logs`
3. Process status: `bash realtime_pipeline/manage_processes.sh status`
