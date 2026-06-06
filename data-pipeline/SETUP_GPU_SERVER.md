# FinSent-Agent — Huong dan Setup GPU Server

## Yeu cau he thong

| Thanh phan     | Toi thieu          | Khuyen nghi        |
|----------------|--------------------|--------------------|
| OS             | Ubuntu 20.04+      | Ubuntu 22.04 LTS   |
| Python         | 3.10+              | 3.12               |
| RAM            | 8 GB               | 16 GB              |
| GPU (tuy chon) | NVIDIA 8 GB VRAM   | RTX 3090 / A100    |
| Disk           | 20 GB free         | 50 GB SSD          |
| Internet       | Bat buoc           | Bat buoc           |

---

## Buoc 1 — Upload code len server

```bash
# Option A: Git clone (neu co repo)
git clone <your-repo-url>
cd FinSeg-System-4AE-/data-pipeline

# Option B: SCP tu may Windows
scp -r "C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline" user@SERVER_IP:~/finsent/
ssh user@SERVER_IP
cd ~/finsent/data-pipeline
```

---

## Buoc 2 — Cai dat tren server (chay 1 lan)

```bash
bash setup_server.sh
```

**Hoac tu lam tung buoc:**

### 2.1 Python & pip

```bash
# Kiem tra Python
python3 --version   # Can >= 3.10

# Neu chua co Python 3.12
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.12 python3.12-pip python3.12-venv -y
```

### 2.2 Docker (cho Kafka)

```bash
curl -fsSL https://get.docker.com | bash
sudo usermod -aG docker $USER
newgrp docker

# Kiem tra
docker --version
docker compose version
```

### 2.3 Khoi dong Kafka

```bash
docker compose -f docker-compose.kafka.yml up -d

# Kiem tra Kafka da chay
docker compose -f docker-compose.kafka.yml ps
# Cho ~30s roi kiem tra port
nc -z localhost 9092 && echo "Kafka OK"
```

### 2.4 Python dependencies

```bash
# Core packages
pip3 install -r requirements.txt

# Realtime pipeline packages
pip3 install -r realtime_pipeline/requirements_realtime.txt

# Facebook browser automation
playwright install chromium --with-deps

# GPU packages (CHI khi co NVIDIA GPU + CUDA)
# Kiem tra GPU truoc:
nvidia-smi
# Neu co GPU:
pip3 install bitsandbytes accelerate
```

### 2.5 Cau hinh .env

```bash
cp .env.example .env
nano .env
```

**Cac key bat buoc phai dien:**

```bash
# ChromaDB Cloud (lay tai https://www.trychroma.com)
CHROMADB_API_KEY=ck-xxxxxxxxxxxx
CHROMADB_TENANT=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
CHROMADB_DATABASE=fin-sent-database
CHROMADB_COLLECTION=macro_policies

# HuggingFace (lay tai https://huggingface.co/settings/tokens)
HUGGINGFACE_API_KEY=hf_xxxxxxxxxxxx

# Facebook (de crawl that, ko thi chay stub mode)
FB_EMAIL=your_email@gmail.com
FB_PASSWORD=your_password

# Telegram (de gui bao cao tu dong)
TELEGRAM_BOT_TOKEN=123456789:AAxxxxx
TELEGRAM_CHAT_ID=-100xxxxxxxxx
```

**Cac key tuy chon:**

```bash
# Kafka (mac dinh localhost:9092, chi doi neu Kafka chay may khac)
KAFKA_BROKER_URL=localhost:9092

# Realtime settings
CRAWL_INTERVAL_SECONDS=1800   # 30 phut
HISTORY_START_DATE=2025-04-01
HISTORY_END_DATE=2026-06-30
CHROMA_REALTIME_COLLECTION=realtime_policies
```

---

## Buoc 3 — Ingest du lieu lan dau (1 lan duy nhat)

```bash
# Ingest tai lieu NHNN PDF vao ChromaDB
# (Da co san trong data/nhnn_docs_SCB/)
cd data_pipeline_ingestion
python3 nhnn_ingestor.py
cd ..
```

---

## Buoc 4 — Verify he thong

```bash
export VNSTOCK_DISABLE_NOTIFICATION=1
export VNSTOCK_SHOW_ADS=0
python3 realtime_pipeline/verify.py
```

**Ket qua mong doi:** 20/20 files OK, tat ca checks PASSED

---

## Buoc 5 — Chay he thong

### Test 1 chu ky (truoc khi chay daemon)

```bash
python3 realtime_pipeline/scheduler.py --ticker SHB --once
```

Ket qua mong doi:
```
VMSI = 50-70 / 100
Status = normal / risk_low
74s
```

### Option A: Chay tu dong bang script quan ly (KHUYEN NGHI)

```bash
# Khoi dong TAT CA processes (Scheduler + Vector Worker + 2 Dashboards)
bash realtime_pipeline/manage_processes.sh start

# Kiem tra trang thai
bash realtime_pipeline/manage_processes.sh status

# Xem logs
bash realtime_pipeline/manage_processes.sh logs scheduler    # Scheduler logs
bash realtime_pipeline/manage_processes.sh logs vector       # Vector Worker logs
bash realtime_pipeline/manage_processes.sh logs rt           # Realtime Dashboard logs

# Dung tat ca
bash realtime_pipeline/manage_processes.sh stop

# Khoi dong lai
bash realtime_pipeline/manage_processes.sh restart
```

**Output mong doi:**
```
╔════════════════════════════════════════════════════════╗
║       FinSent-Agent Realtime Process Manager          ║
╚════════════════════════════════════════════════════════╝

[1/4] Scheduler:       ✓ Running (PID: 12345)
[2/4] Vector Worker:   ✓ Running (PID: 12346)
[3/4] Dashboard Demo:  ✓ Running (PID: 12347)
[4/4] Dashboard RT:    ✓ Running (PID: 12348)
```

### Option B: Chay thu cong tung process

```bash
# Tao logs folder neu chua co
mkdir -p logs

# 1. Scheduler (30 phut / chu ky)
nohup python3 realtime_pipeline/scheduler.py --ticker SHB \
  > logs/scheduler.log 2>&1 &
echo "Scheduler PID: $!"

# 2. Vector Worker (Chunking + Embedding + ChromaDB)
nohup python3 realtime_pipeline/run_vector_worker.py \
  > logs/vector_worker.log 2>&1 &
echo "Vector Worker PID: $!"

# 3. Demo Dashboard (port 8501)
nohup streamlit run dashboard.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  --server.headless true \
  > logs/dashboard_demo.log 2>&1 &

# 4. Realtime Dashboard (port 8502)
nohup streamlit run dashboard_realtime.py \
  --server.port 8502 \
  --server.address 0.0.0.0 \
  --server.headless true \
  > logs/dashboard_rt.log 2>&1 &

echo "Dashboard Demo:     http://SERVER_IP:8501"
echo "Dashboard Realtime: http://SERVER_IP:8502"
```

---

## Quan ly processes

### Su dung script quan ly (KHUYEN NGHI)

```bash
# Xem trang thai tat ca processes
bash realtime_pipeline/manage_processes.sh status

# Dung tat ca processes
bash realtime_pipeline/manage_processes.sh stop

# Khoi dong lai
bash realtime_pipeline/manage_processes.sh restart

# Xem logs realtime
bash realtime_pipeline/manage_processes.sh logs scheduler
bash realtime_pipeline/manage_processes.sh logs vector
```

### Thu cong (neu can)

```bash
# Xem tat ca process dang chay
ps aux | grep -E "scheduler|vector_worker|streamlit"

# Xem log realtime
tail -f logs/scheduler.log
tail -f logs/vector_worker.log

# Dung processes
pkill -f "scheduler.py"
pkill -f "vector_worker.py"
pkill -f "streamlit"

# Tat Kafka
docker compose -f docker-compose.kafka.yml down
```

---

## Mo firewall (neu can truy cap tu ngoai)

```bash
sudo ufw allow 8501/tcp   # Demo dashboard
sudo ufw allow 8502/tcp   # Realtime dashboard
sudo ufw reload
```

---

## Ket noi tu trinh duyet

```
Demo:     http://SERVER_IP:8501
Realtime: http://SERVER_IP:8502
```

---

## Cau truc file quan trong

```
data-pipeline/
├── dashboard.py                      ← Demo dashboard (port 8501)
├── dashboard_realtime.py             ← Realtime dashboard (port 8502)
├── docker-compose.kafka.yml          ← Kafka infra (1 lenh khoi dong)
├── setup_server.sh                   ← Script setup tu dong
├── requirements.txt                  ← Python deps core
├── .env                              ← API keys (PHAI dien day du)
│
├── data_pipeline_ingestion/          ← Demo pipeline
│   ├── nhnn_ingestor.py              ← Ingest NHNN PDF lan dau
│   └── replay_service.py            ← Stream data demo
│
├── realtime_pipeline/                ← Realtime pipeline
│   ├── scheduler.py                  ← ENTRY POINT chinh (Scheduler)
│   ├── run_vector_worker.py          ← ENTRY POINT Vector Worker (moi)
│   ├── manage_processes.sh           ← Script quan ly processes (moi)
│   ├── vmsi_realtime.py              ← Engine tinh VMSI
│   ├── config.py                     ← Config URLs, settings
│   ├── crawlers/
│   │   ├── news_crawler.py           ← CafeF, Vietstock, ChinhPhu
│   │   ├── facebook_crawler.py       ← Facebook Playwright
│   │   ├── nhnn_crawler.py           ← NHNN sbv.gov.vn
│   │   ├── stock_crawler.py          ← vnstock API v4
│   │   └── vector_worker.py          ← Kafka Consumer → Chunking + Embedding + ChromaDB (moi)
│   ├── normalizers/
│   │   └── unified_normalizer.py     ← Standard JSON
│   └── producers/
│       └── realtime_producer.py      ← Kafka producers (Social, Market, Policy)
│
└── multi_agent_system/               ← AI Agents
    ├── agents/
    │   ├── mac_orchestrator.py       ← Dieu phoi (entry point MAC)
    │   ├── social_agent.py           ← Doc Kafka, tinh S_social
    │   ├── macro_agent.py            ← ChromaDB RAG, tinh S_nhnn
    │   ├── risk_agent.py             ← Tinh VMSI, ghi JSON
    │   └── chatbot_agent.py          ← Qwen2-7B (GPU)
    └── engines/
        └── vmsi_engine.py            ← Cong thuc toan VMSI
```

---

## Luu y quan trong

1. **Chromium headless** — Can co tren server de crawl Facebook that:
   ```bash
   playwright install chromium --with-deps
   ```

2. **GPU chatbot** — Chi hoat dong khi co NVIDIA GPU + CUDA + bitsandbytes.
   Tren CPU, chatbot chay STUB mode (van hoat dong, tra loi placeholder).

3. **vnstock banner** — Hien ra terminal nhung KHONG anh huong logic.
   Du lieu gia co phieu van lay dung 100%.

4. **ChromaDB quota** — Doc ID toi da 128 bytes (da xu ly trong code).

5. **Facebook** — Neu khong co credentials, he thong tu dong chay stub mode
   voi 10 posts mau, pipeline van tinh duoc VMSI.

6. **Vector Worker** — Chay SONG SONG voi Scheduler. Worker lang nghe Kafka,
   tu dong xu ly embedding + chunking + ingest ChromaDB khi co du lieu moi.
   Khong can restart, chi can dam bao Kafka dang chay.

7. **Kiem tra Kafka** — Truoc khi chay he thong, dam bao Kafka da khoi dong:
   ```bash
   docker compose -f docker-compose.kafka.yml ps
   nc -z localhost 9092 && echo "Kafka OK"
   ```
