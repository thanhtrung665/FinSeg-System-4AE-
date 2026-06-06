#!/bin/bash
# =============================================================
# setup_server.sh — FinSent-Agent GPU Server Setup
# Chay 1 lan duy nhat sau khi SSH vao server
# Usage: bash setup_server.sh
# =============================================================
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}   $1"; }
info() { echo -e "${YELLOW}[ > ]${NC}  $1"; }
err()  { echo -e "${RED}[ERR]${NC}  $1"; exit 1; }
hdr()  { echo -e "\n${CYAN}══ $1 ══${NC}"; }

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   FinSent-Agent — GPU Server Setup Script   ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── 1. Kiem tra OS ────────────────────────────────────────────
hdr "1. Kiem tra moi truong"
OS=$(uname -s)
info "OS: $OS ($(uname -r))"
command -v python3 &>/dev/null || err "Python3 chua co. Cai: sudo apt install python3 python3-pip -y"
PY_VER=$(python3 --version | awk '{print $2}')
ok "Python $PY_VER"

# ── 2. Docker ─────────────────────────────────────────────────
hdr "2. Docker"
if ! command -v docker &>/dev/null; then
    info "Dang cai Docker..."
    curl -fsSL https://get.docker.com | bash
    sudo usermod -aG docker "$USER"
    ok "Docker da cai (co the can logout/login lai)"
else
    ok "Docker: $(docker --version | cut -d' ' -f3 | tr -d ',')"
fi

if ! docker compose version &>/dev/null; then
    info "Cai docker-compose plugin..."
    sudo apt-get install -y docker-compose-plugin 2>/dev/null || true
fi
ok "Docker Compose: $(docker compose version --short 2>/dev/null || echo 'available')"

# ── 3. Kafka ──────────────────────────────────────────────────
hdr "3. Kafka (qua Docker)"
info "Khoi dong Kafka + ZooKeeper..."
docker compose -f docker-compose.kafka.yml up -d

info "Cho Kafka san sang (toi da 90s)..."
TRIES=0
while ! nc -z localhost 9092 2>/dev/null; do
    TRIES=$((TRIES+1))
    [ $TRIES -gt 18 ] && err "Kafka khong khoi dong duoc sau 90s. Kiem tra: docker compose -f docker-compose.kafka.yml logs"
    echo -n "."
    sleep 5
done
echo ""
ok "Kafka dang chay tai localhost:9092"

# ── 4. Python packages ────────────────────────────────────────
hdr "4. Python dependencies"
info "Cai core packages..."
pip3 install -r requirements.txt -q
ok "Core packages OK"

info "Cai realtime packages..."
pip3 install -r realtime_pipeline/requirements_realtime.txt -q
ok "Realtime packages OK"

# GPU check
info "Kiem tra GPU..."
if python3 -c "import torch; assert torch.cuda.is_available()" 2>/dev/null; then
    GPU=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1)
    ok "GPU phat hien: $GPU"
    info "Cai GPU packages (bitsandbytes + accelerate)..."
    pip3 install bitsandbytes accelerate -q
    ok "GPU packages OK — Chatbot se chay voi Qwen2-7B day du"
else
    info "Khong phat hien GPU CUDA — Chatbot se chay STUB mode (van hoat dong)"
fi

# Playwright cho Facebook crawler
info "Cai Playwright Chromium..."
if playwright install chromium --with-deps 2>/dev/null; then
    ok "Playwright Chromium OK"
else
    info "Playwright skip — Facebook se chay stub mode"
fi

# ── 5. Tao folders ────────────────────────────────────────────
hdr "5. Cau truc thu muc"
mkdir -p logs data/shb
ok "Tao logs/ va data/shb/"

# ── 6. .env ───────────────────────────────────────────────────
hdr "6. Cau hinh .env"
if [ ! -f ".env" ]; then
    cp .env.example .env
    info ".env da tao tu .env.example"
    echo ""
    echo "  ┌─────────────────────────────────────────────────┐"
    echo "  │  QUAN TRONG: Dien cac key vao .env truoc khi chay│"
    echo "  │                                                   │"
    echo "  │  nano .env                                        │"
    echo "  │                                                   │"
    echo "  │  Bat buoc:                                        │"
    echo "  │    CHROMADB_API_KEY=ck-xxx                        │"
    echo "  │    CHROMADB_TENANT=uuid                           │"
    echo "  │    HUGGINGFACE_API_KEY=hf_xxx                     │"
    echo "  │                                                   │"
    echo "  │  Tuy chon (crawl that):                           │"
    echo "  │    FB_EMAIL=your@gmail.com                        │"
    echo "  │    FB_PASSWORD=your_password                      │"
    echo "  │    TELEGRAM_BOT_TOKEN=xxx                         │"
    echo "  │    TELEGRAM_CHAT_ID=xxx                           │"
    echo "  └─────────────────────────────────────────────────┘"
    echo ""
else
    ok ".env da ton tai"
    # Kiem tra cac key quan trong
    source .env 2>/dev/null || true
    [ -n "${CHROMADB_API_KEY:-}" ] && ok "CHROMADB_API_KEY: set" || info "CHROMADB_API_KEY: CHUA SET (can dien)"
    [ -n "${HUGGINGFACE_API_KEY:-}" ] && ok "HUGGINGFACE_API_KEY: set" || info "HUGGINGFACE_API_KEY: CHUA SET"
    [ -n "${FB_EMAIL:-}" ] && ok "FB_EMAIL: set" || info "FB_EMAIL: chua set (Facebook se dung stub mode)"
    [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && ok "TELEGRAM_BOT_TOKEN: set" || info "TELEGRAM_BOT_TOKEN: chua set (se khong gui duoc Telegram)"
fi

# ── 7. Ingest NHNN docs ───────────────────────────────────────
hdr "7. Ingest NHNN docs vao ChromaDB"
if [ -d "data/nhnn_docs_SCB" ] && [ "$(ls data/nhnn_docs_SCB/*.pdf 2>/dev/null | wc -l)" -gt 0 ]; then
    info "Tim thay $(ls data/nhnn_docs_SCB/*.pdf | wc -l) PDF files"
    read -p "  Ingest NHNN docs vao ChromaDB? (y/n): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        export VNSTOCK_DISABLE_NOTIFICATION=1
        export VNSTOCK_SHOW_ADS=0
        cd data_pipeline_ingestion && python3 nhnn_ingestor.py && cd ..
        ok "NHNN ingest hoan tat"
    else
        info "Bo qua ingest (co the chay sau: python3 data_pipeline_ingestion/nhnn_ingestor.py)"
    fi
else
    info "Khong tim thay PDF files trong data/nhnn_docs_SCB/ — bo qua ingest"
fi

# ── 8. Verify ─────────────────────────────────────────────────
hdr "8. Verify he thong"
export VNSTOCK_DISABLE_NOTIFICATION=1
export VNSTOCK_SHOW_ADS=0
if python3 realtime_pipeline/verify.py 2>/dev/null; then
    ok "Verify PASSED"
else
    info "Verify co canh bao (xem chi tiet o tren) — he thong van co the chay"
fi

# ── DONE ──────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    SETUP HOAN TAT                           ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  TEST 1 chu ky:                                              ║"
echo "║    python3 realtime_pipeline/scheduler.py --ticker SHB --once║"
echo "║                                                              ║"
echo "║  Chay daemon (30p/chu ky):                                   ║"
echo "║    nohup python3 realtime_pipeline/scheduler.py \\            ║"
echo "║      --ticker SHB > logs/scheduler.log 2>&1 &               ║"
echo "║                                                              ║"
echo "║  Dashboard Demo    (port 8501):                              ║"
echo "║    streamlit run dashboard.py \\                              ║"
echo "║      --server.port 8501 --server.address 0.0.0.0             ║"
echo "║                                                              ║"
echo "║  Dashboard Realtime (port 8502):                             ║"
echo "║    streamlit run dashboard_realtime.py \\                     ║"
echo "║      --server.port 8502 --server.address 0.0.0.0             ║"
echo "║                                                              ║"
echo "║  Tat Kafka:                                                  ║"
echo "║    docker compose -f docker-compose.kafka.yml down           ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
