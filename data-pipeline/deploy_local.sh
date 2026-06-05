#!/bin/bash
# ============================================
# Local Data Pipeline — Deploy Script
# Docker Desktop only
# ============================================

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "========================================"
echo "  Local Data Pipeline — Docker Deploy"
echo "========================================"

# --- Bước 1: Kiểm tra Docker ---
echo -e "\n${YELLOW}[1/3] Kiểm tra Docker Engine...${NC}"
if ! command -v docker &>/dev/null; then
    echo -e "${RED}[ERROR] Docker không được tìm thấy trên máy này.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Docker Engine đang hoạt động.${NC}"

# --- Bước 2: Khởi tạo thư mục Volumes ---
echo -e "\n${YELLOW}[2/3] Khởi tạo thư mục dữ liệu cục bộ...${NC}"
mkdir -p ./data-pipeline-volumes/{kafka,zookeeper/data,zookeeper/logs}
chmod -R 755 ./data-pipeline-volumes/
echo -e "${GREEN}[OK] Thư mục Volumes đã sẵn sàng.${NC}"

# --- Bước 3: Khởi động services ---
echo -e "\n${YELLOW}[3/3] Khởi động toàn bộ cụm Kafka...${NC}"
docker compose up -d

echo "Đang chờ Zookeeper và Kafka khởi động (khoảng 15-30 giây)..."
sleep 15

# --- Kiểm tra kết nối ---
echo -e "\n${YELLOW}[Kiểm tra sức khỏe hệ thống]${NC}"
if nc -z localhost 9092 2>/dev/null; then
    echo -e "${GREEN}[OK] Kafka Broker : localhost:9092 — REACHABLE${NC}"
else
    echo -e "${RED}[WARN] Kafka Broker : localhost:9092 — Đang khởi động, vui lòng đợi thêm.${NC}"
fi

echo ""
echo "========================================"
echo "  DEPLOYMENT SUMMARY"
echo "========================================"
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
echo "========================================"
echo -e "${GREEN}Pipeline Kafka khởi động hoàn tất! Vector DB được quản lý qua ChromaDB Cloud.${NC}"