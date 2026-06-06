# FinSent-Agent — Quick Start Guide

## 🚀 Khởi động nhanh (5 phút)

### Bước 1: Clone & Cài đặt

```bash
# Windows
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline

# Linux/Mac
cd ~/FinSeg-System-4AE-/data-pipeline

# Cài đặt dependencies
pip install -r requirements.txt
pip install -r realtime_pipeline/requirements_realtime.txt
```

### Bước 2: Cấu hình API Keys

```bash
# Copy file .env.example
cp .env.example .env

# Chỉnh sửa .env (BẮT BUỘC)
nano .env   # Linux/Mac
notepad .env  # Windows
```

**Điền các key sau:**
```env
# ChromaDB Cloud (lấy tại https://www.trychroma.com)
CHROMADB_API_KEY=ck-xxxxxxxxxxxx
CHROMADB_TENANT=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
CHROMADB_DATABASE=fin-sent-database

# HuggingFace (lấy tại https://huggingface.co/settings/tokens)
HUGGINGFACE_API_KEY=hf_xxxxxxxxxxxx
```

### Bước 3: Khởi động Kafka

**Windows:**
```bash
# Cài Docker Desktop: https://www.docker.com/products/docker-desktop
docker-compose -f docker-compose.kafka.yml up -d
```

**Linux:**
```bash
docker compose -f docker-compose.kafka.yml up -d
```

### Bước 4: Ingest dữ liệu NHNN (1 lần duy nhất)

```bash
cd data_pipeline_ingestion
python nhnn_ingestor.py
cd ..
```

### Bước 5: Test hệ thống

```bash
# Verify tất cả components
python realtime_pipeline/verify.py

# Test 1 chu kỳ VMSI
python realtime_pipeline/scheduler.py --ticker SHB --once
```

### Bước 6: Khởi động Production

**Linux/Mac:**
```bash
bash realtime_pipeline/manage_processes.sh start
```

**Windows:**
```bash
realtime_pipeline\manage_processes.bat start
```

### Bước 7: Truy cập Dashboard

Mở trình duyệt:
- **Realtime Dashboard**: http://localhost:8502
- **Demo Dashboard**: http://localhost:8501

---

## 📊 Kiểm tra trạng thái

**Linux/Mac:**
```bash
bash realtime_pipeline/manage_processes.sh status
```

**Windows:**
```bash
realtime_pipeline\manage_processes.bat status
```

**Output mong đợi:**
```
╔════════════════════════════════════════════════════════╗
║       FinSent-Agent Process Status                     ║
╚════════════════════════════════════════════════════════╝

[1/4] Scheduler:       ✓ Running
[2/4] Vector Worker:   ✓ Running
[3/4] Dashboard Demo:  ✓ Running (http://localhost:8501)
[4/4] Dashboard RT:    ✓ Running (http://localhost:8502)
```

---

## 📝 Xem logs

**Linux/Mac:**
```bash
# Xem tất cả logs
tail -f logs/*.log

# Xem log cụ thể
bash realtime_pipeline/manage_processes.sh logs scheduler
bash realtime_pipeline/manage_processes.sh logs vector
```

**Windows:**
```bash
# Xem log trong Notepad
notepad logs\scheduler.log
notepad logs\vector_worker.log

# Hoặc dùng PowerShell
Get-Content logs\scheduler.log -Wait
```

---

## 🛑 Dừng hệ thống

**Linux/Mac:**
```bash
bash realtime_pipeline/manage_processes.sh stop
```

**Windows:**
```bash
realtime_pipeline\manage_processes.bat stop
```

---

## 🔧 Troubleshooting

### ❌ Lỗi: Kafka không kết nối được

```bash
# Kiểm tra Kafka
docker ps | grep kafka

# Nếu không chạy, khởi động lại
docker-compose -f docker-compose.kafka.yml restart
```

### ❌ Lỗi: ChromaDB authentication failed

```bash
# Kiểm tra API keys trong .env
cat .env | grep CHROMADB

# Đảm bảo có đầy đủ 3 keys:
# CHROMADB_API_KEY
# CHROMADB_TENANT
# CHROMADB_DATABASE
```

### ❌ Lỗi: Module not found

```bash
# Cài lại dependencies
pip install -r requirements.txt
pip install -r realtime_pipeline/requirements_realtime.txt

# Nếu dùng GPU
pip install bitsandbytes accelerate
```

### ❌ Dashboard không mở được

```bash
# Kiểm tra port có bị chiếm không
netstat -an | grep 8502

# Windows
netstat -an | findstr 8502

# Nếu bị chiếm, đổi port
streamlit run dashboard_realtime.py --server.port 8503
```

---

## 📚 Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│                   REALTIME PIPELINE                     │
├────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────┐     Crawl Data (30 phút/chu kỳ)     │
│  │  Scheduler  │ ────────────────────────────────────┐ │
│  └─────────────┘                                     │ │
│                                                       ▼ │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Data Sources                                     │  │
│  │  • Facebook (Playwright)                          │  │
│  │  • News (CafeF, Vietstock, ChinhPhu RSS)         │  │
│  │  • NHNN (sbv.gov.vn API)                         │  │
│  │  • Stock Prices (vnstock API v4)                 │  │
│  └─────────────────────┬────────────────────────────┘  │
│                        │                                │
│                        ▼                                │
│  ┌──────────────────────────────────────────────────┐  │
│  │           Kafka Cluster                           │  │
│  │  • fb_mock_data (Social sentiment)               │  │
│  │  • realtime_market (Stock prices)                │  │
│  │  • realtime_policy (NHNN docs) ─────────────┐    │  │
│  └──────────────────────────────────────────────│────┘  │
│                                                  │       │
│  ┌───────────────────────────────────────────────┘      │
│  │                                                       │
│  ▼                                                       │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Vector Worker (Kafka Consumer)                  │  │
│  │  • Chunking: 1500 chars, overlap 200            │  │
│  │  • Embedding: vietnamese-sbert                   │  │
│  │  • Ingest: ChromaDB Cloud                        │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  MAC System (Multi-Agent Collaboration)          │  │
│  │  1. Social Agent: Sentiment từ Kafka             │  │
│  │  2. Macro Agent: RAG query ChromaDB              │  │
│  │  3. Risk Agent: Tính VMSI → live_vmsi.json      │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Dashboards (Streamlit)                          │  │
│  │  • Port 8501: Demo với dữ liệu CSV               │  │
│  │  • Port 8502: Realtime với dữ liệu thực          │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
└────────────────────────────────────────────────────────┘
```

---

## 🎯 Next Steps

1. ✅ **Hoàn thành**: Cài đặt & chạy hệ thống
2. 📖 **Đọc thêm**: [SETUP_GPU_SERVER.md](SETUP_GPU_SERVER.md) - Hướng dẫn deploy lên GPU server
3. 📖 **Đọc thêm**: [realtime_pipeline/README_INTEGRATION.md](realtime_pipeline/README_INTEGRATION.md) - Chi tiết tích hợp Vector Worker
4. 🔧 **Tùy chỉnh**: Chỉnh sửa `.env` để thay đổi chu kỳ crawl, ticker, v.v.
5. 🚀 **Production**: Deploy lên cloud server (AWS, GCP, Azure)

---

## 📞 Support

Nếu gặp vấn đề:
1. Kiểm tra logs: `logs/scheduler.log`, `logs/vector_worker.log`
2. Chạy verify: `python realtime_pipeline/verify.py`
3. Kiểm tra Kafka: `docker ps | grep kafka`
4. Kiểm tra process status: `manage_processes.sh status` (Linux) hoặc `manage_processes.bat status` (Windows)

---

**Made with ❤️ by FinSent-Agent Team**
