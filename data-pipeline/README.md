# FinSent-Agent: Local Data Pipeline Infrastructure

Hệ thống Data Pipeline phân tích cảm xúc tài chính theo thời gian thực, sử dụng **Apache Kafka** chạy trực tiếp trên máy (không qua Docker) kết hợp với Vector Database được quản lý trên **ChromaDB Cloud**.

## Yêu cầu hệ thống (Prerequisites)

- **Python** ≥ 3.10
- **Apache Kafka** ≥ 3.x (cài đặt thủ công, xem hướng dẫn bên dưới)
- **Java** ≥ 11 (Kafka yêu cầu Java Runtime)
- Minimum RAM: **4GB**

---

## 1. Cài đặt Apache Kafka (thủ công)

### Windows

1. Tải Kafka tại: https://kafka.apache.org/downloads (chọn bản Binary, ví dụ `kafka_2.13-3.7.x.tgz`)
2. Giải nén vào thư mục, ví dụ `C:\kafka`
3. Khởi động **ZooKeeper** (cửa sổ terminal 1):
   ```cmd
   C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties
   ```
4. Khởi động **Kafka Broker** (cửa sổ terminal 2):
   ```cmd
   C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
   ```

### macOS / Linux

1. Tải và giải nén Kafka, ví dụ vào `~/kafka`
2. Khởi động ZooKeeper:
   ```bash
   ~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties
   ```
3. Khởi động Kafka Broker:
   ```bash
   ~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
   ```

> Kafka mặc định lắng nghe tại `localhost:9092` — khớp với cấu hình của project.

---

## 2. Tạo Kafka Topics

Sau khi Kafka đang chạy, tạo 4 topics cần thiết.

**Windows:**
```cmd
C:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic news_rss_data     --partitions 1 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic f319_data         --partitions 1 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic fb_mock_data      --partitions 1 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic market_stock_data --partitions 1 --replication-factor 1
```

**macOS / Linux:**
```bash
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic news_rss_data     --partitions 1 --replication-factor 1
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic f319_data         --partitions 1 --replication-factor 1
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic fb_mock_data      --partitions 1 --replication-factor 1
~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic market_stock_data --partitions 1 --replication-factor 1
```

---

## 3. Cài đặt Python Dependencies

```bash
cd data-pipeline
pip install -r requirements.txt
```

---

## 4. Cấu hình môi trường

```bash
cp .env.example .env
```

Sau đó mở `.env` và điền vào:
- `CHROMADB_API_KEY` — API key của ChromaDB Cloud
- `CHROMADB_TENANT` — Tenant ID của tài khoản ChromaDB
- `HUGGINGFACE_API_KEY` — Token từ HuggingFace

---

## 5. Khởi chạy nhanh (Quick Start)

### Ingest tài liệu NHNN lên ChromaDB Cloud (chạy 1 lần)
```bash
cd data_pipeline_ingestion
python nhnn_ingestor.py
```

### Stream dữ liệu mạng xã hội giả lập vào Kafka
```bash
cd data_pipeline_ingestion
python replay_service.py
```

### Khởi động hệ thống Multi-Agent
```bash
cd multi_agent_system
python agents/mac_orchestrator.py
```

### Chạy Dashboard Streamlit
```bash
streamlit run dashboard.py
```

---

## Kiến trúc hệ thống

```
Kafka (localhost:9092)
├── Topic: fb_mock_data        ← Producer: replay_service.py / social_injector.py
│                               ← Consumer: social_agent.py
├── Topic: market_stock_data   ← Producer: vnstock_producer.py / market_producer.py
├── Topic: news_rss_data       ← (planned)
└── Topic: f319_data           ← (planned)

ChromaDB Cloud
├── Ingestor: nhnn_ingestor.py (writes PDF embeddings)
└── Reader:   macro_agent.py  (semantic search)

Multi-Agent System
├── SocialAgent     → consume Kafka, score sentiment
├── MacroAgent      → query ChromaDB, analyze policy
├── RiskSynthesis   → calculate VMSI, write live_vmsi.json
└── MAC Orchestrator → coordinate all agents

Dashboard (Streamlit)
└── Reads live_vmsi.json → visualize VMSI
```

---

## Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| `fb_mock_data` | `replay_service.py`, `social_injector.py` | `social_agent.py` |
| `market_stock_data` | `vnstock_producer.py`, `market_producer.py` | — |
| `news_rss_data` | (planned) | — |
| `f319_data` | (planned) | — |
