#!/bin/bash
# setup_kafka.sh
# Script tự động setup Kafka và tạo topics trên GPU server

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Paths
KAFKA_DIR="/root/FinSeg-System-4AE-/data-pipeline/data/kafka/kafka_2.13-4.3.0"
LOG_DIR="/root/logs"

echo "=========================================="
echo "  KAFKA SETUP FOR FINSEG REALTIME"
echo "=========================================="
echo ""

# Create log directory
mkdir -p "$LOG_DIR"

# Step 1: Check if Kafka is already running
echo -e "${YELLOW}[1] Checking if Kafka is already running...${NC}"
if ps aux | grep -v grep | grep "kafka.Kafka" > /dev/null; then
    echo -e "${GREEN}✅ Kafka is already running!${NC}"
    KAFKA_PID=$(ps aux | grep -v grep | grep "kafka.Kafka" | awk '{print $2}')
    echo "   PID: $KAFKA_PID"
else
    echo -e "${RED}❌ Kafka is NOT running${NC}"
    
    # Check which mode to use
    if [ -f "$KAFKA_DIR/config/zookeeper.properties" ]; then
        echo -e "${YELLOW}[2] Starting Kafka in TRADITIONAL mode (with Zookeeper)...${NC}"
        
        # Check if Zookeeper is running
        if ps aux | grep -v grep | grep "QuorumPeerMain" > /dev/null; then
            echo -e "${GREEN}✅ Zookeeper is already running${NC}"
        else
            echo "   Starting Zookeeper..."
            cd "$KAFKA_DIR"
            nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties > "$LOG_DIR/zookeeper.log" 2>&1 &
            echo "   Waiting 10 seconds for Zookeeper to start..."
            sleep 10
            echo -e "${GREEN}✅ Zookeeper started${NC}"
        fi
        
        # Start Kafka
        echo "   Starting Kafka..."
        cd "$KAFKA_DIR"
        nohup ./bin/kafka-server-start.sh config/server.properties > "$LOG_DIR/kafka.log" 2>&1 &
        echo "   Waiting 15 seconds for Kafka to start..."
        sleep 15
        echo -e "${GREEN}✅ Kafka started${NC}"
        
    else
        echo -e "${YELLOW}[2] Starting Kafka in KRAFT mode (no Zookeeper)...${NC}"
        
        # Check if already formatted
        if [ ! -d "/tmp/kraft-combined-logs" ]; then
            echo "   Formatting storage..."
            cd "$KAFKA_DIR"
            KAFKA_CLUSTER_ID=$(./bin/kafka-storage.sh random-uuid)
            ./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
        else
            echo "   Storage already formatted"
        fi
        
        # Start Kafka
        echo "   Starting Kafka..."
        cd "$KAFKA_DIR"
        nohup ./bin/kafka-server-start.sh config/kraft/server.properties > "$LOG_DIR/kafka.log" 2>&1 &
        echo "   Waiting 15 seconds for Kafka to start..."
        sleep 15
        echo -e "${GREEN}✅ Kafka started in KRaft mode${NC}"
    fi
fi

echo ""

# Step 2: Verify Kafka is listening
echo -e "${YELLOW}[3] Verifying Kafka port 9092...${NC}"
if command -v netstat &> /dev/null; then
    if netstat -tuln | grep 9092 > /dev/null; then
        echo -e "${GREEN}✅ Kafka is listening on port 9092${NC}"
    else
        echo -e "${RED}❌ Kafka is NOT listening on port 9092${NC}"
        echo "Check logs: tail -f $LOG_DIR/kafka.log"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠️  netstat not found. Installing net-tools...${NC}"
    apt-get update > /dev/null 2>&1
    apt-get install -y net-tools > /dev/null 2>&1
    echo -e "${GREEN}✅ net-tools installed${NC}"
    
    if netstat -tuln | grep 9092 > /dev/null; then
        echo -e "${GREEN}✅ Kafka is listening on port 9092${NC}"
    else
        echo -e "${RED}❌ Kafka is NOT listening on port 9092${NC}"
        exit 1
    fi
fi

echo ""

# Step 3: Create topics
echo -e "${YELLOW}[4] Creating Kafka topics...${NC}"

cd "$KAFKA_DIR"

TOPICS=("realtime_news" "realtime_social" "realtime_market" "realtime_policy")

for TOPIC in "${TOPICS[@]}"; do
    # Check if topic exists
    if ./bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "^${TOPIC}$"; then
        echo -e "${GREEN}✅ Topic '$TOPIC' already exists${NC}"
    else
        echo "   Creating topic: $TOPIC"
        ./bin/kafka-topics.sh --create \
            --bootstrap-server localhost:9092 \
            --replication-factor 1 \
            --partitions 3 \
            --topic "$TOPIC" > /dev/null 2>&1
        echo -e "${GREEN}✅ Topic '$TOPIC' created${NC}"
    fi
done

echo ""

# Step 4: List all topics
echo -e "${YELLOW}[5] All Kafka topics:${NC}"
cd "$KAFKA_DIR"
./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

echo ""
echo -e "${GREEN}=========================================="
echo "  ✅ KAFKA SETUP COMPLETED"
echo "==========================================${NC}"
echo ""
echo "📝 Next steps:"
echo "   1. Test Kafka connection: python3 -c 'from kafka import KafkaProducer; print(\"OK\")'"
echo "   2. Run test cycle: python3 realtime_pipeline/scheduler.py --ticker SHB --once"
echo "   3. Start production: bash realtime_pipeline/manage_processes.sh start"
echo ""
echo "📊 Logs:"
echo "   - Kafka: $LOG_DIR/kafka.log"
echo "   - Zookeeper: $LOG_DIR/zookeeper.log (if using traditional mode)"
echo ""
