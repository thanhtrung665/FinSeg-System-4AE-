#!/bin/bash
# -*- coding: utf-8 -*-
# realtime_pipeline/manage_processes.sh
#
# Script quan ly tat ca processes cua FinSent-Agent Realtime System
# Su dung: bash realtime_pipeline/manage_processes.sh [start|stop|restart|status]

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
LOGS_DIR="$ROOT_DIR/logs"

# Tao logs directory neu chua co
mkdir -p "$LOGS_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# PID files
SCHEDULER_PID="$LOGS_DIR/scheduler.pid"
VECTOR_WORKER_PID="$LOGS_DIR/vector_worker.pid"
DASHBOARD_DEMO_PID="$LOGS_DIR/dashboard_demo.pid"
DASHBOARD_RT_PID="$LOGS_DIR/dashboard_rt.pid"

# Log files
SCHEDULER_LOG="$LOGS_DIR/scheduler.log"
VECTOR_WORKER_LOG="$LOGS_DIR/vector_worker.log"
DASHBOARD_DEMO_LOG="$LOGS_DIR/dashboard_demo.log"
DASHBOARD_RT_LOG="$LOGS_DIR/dashboard_rt.log"

# Default settings
DEFAULT_TICKER="SHB"
DEFAULT_INTERVAL="1800"

# ── Functions ─────────────────────────────────────────────────────────────────

print_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║       FinSent-Agent Realtime Process Manager          ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

check_process() {
    local pid_file=$1
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Running${NC} (PID: $pid)"
            return 0
        else
            echo -e "${RED}✗ Dead${NC} (stale PID file)"
            rm -f "$pid_file"
            return 1
        fi
    else
        echo -e "${YELLOW}○ Not running${NC}"
        return 1
    fi
}

start_scheduler() {
    echo -e "${BLUE}[1/4]${NC} Starting Scheduler..."
    if [ -f "$SCHEDULER_PID" ] && ps -p $(cat "$SCHEDULER_PID") > /dev/null 2>&1; then
        echo -e "  ${YELLOW}Already running${NC} (PID: $(cat "$SCHEDULER_PID"))"
    else
        cd "$ROOT_DIR"
        nohup python3 realtime_pipeline/scheduler.py \
            --ticker "$DEFAULT_TICKER" \
            --interval "$DEFAULT_INTERVAL" \
            > "$SCHEDULER_LOG" 2>&1 &
        echo $! > "$SCHEDULER_PID"
        sleep 2
        if ps -p $(cat "$SCHEDULER_PID") > /dev/null 2>&1; then
            echo -e "  ${GREEN}✓ Started${NC} (PID: $(cat "$SCHEDULER_PID"))"
            echo -e "  ${BLUE}Log:${NC} $SCHEDULER_LOG"
        else
            echo -e "  ${RED}✗ Failed to start${NC}"
            rm -f "$SCHEDULER_PID"
        fi
    fi
}

start_vector_worker() {
    echo -e "${BLUE}[2/4]${NC} Starting Vector Worker..."
    if [ -f "$VECTOR_WORKER_PID" ] && ps -p $(cat "$VECTOR_WORKER_PID") > /dev/null 2>&1; then
        echo -e "  ${YELLOW}Already running${NC} (PID: $(cat "$VECTOR_WORKER_PID"))"
    else
        cd "$ROOT_DIR"
        nohup python3 realtime_pipeline/run_vector_worker.py \
            > "$VECTOR_WORKER_LOG" 2>&1 &
        echo $! > "$VECTOR_WORKER_PID"
        sleep 2
        if ps -p $(cat "$VECTOR_WORKER_PID") > /dev/null 2>&1; then
            echo -e "  ${GREEN}✓ Started${NC} (PID: $(cat "$VECTOR_WORKER_PID"))"
            echo -e "  ${BLUE}Log:${NC} $VECTOR_WORKER_LOG"
        else
            echo -e "  ${RED}✗ Failed to start${NC}"
            rm -f "$VECTOR_WORKER_PID"
        fi
    fi
}

start_dashboard_demo() {
    echo -e "${BLUE}[3/4]${NC} Starting Dashboard Demo (port 8501)..."
    if [ -f "$DASHBOARD_DEMO_PID" ] && ps -p $(cat "$DASHBOARD_DEMO_PID") > /dev/null 2>&1; then
        echo -e "  ${YELLOW}Already running${NC} (PID: $(cat "$DASHBOARD_DEMO_PID"))"
    else
        cd "$ROOT_DIR"
        nohup streamlit run dashboard.py \
            --server.port 8501 \
            --server.address 0.0.0.0 \
            --server.headless true \
            > "$DASHBOARD_DEMO_LOG" 2>&1 &
        echo $! > "$DASHBOARD_DEMO_PID"
        sleep 3
        if ps -p $(cat "$DASHBOARD_DEMO_PID") > /dev/null 2>&1; then
            echo -e "  ${GREEN}✓ Started${NC} (PID: $(cat "$DASHBOARD_DEMO_PID"))"
            echo -e "  ${BLUE}URL:${NC} http://localhost:8501"
        else
            echo -e "  ${RED}✗ Failed to start${NC}"
            rm -f "$DASHBOARD_DEMO_PID"
        fi
    fi
}

start_dashboard_rt() {
    echo -e "${BLUE}[4/4]${NC} Starting Dashboard Realtime (port 8502)..."
    if [ -f "$DASHBOARD_RT_PID" ] && ps -p $(cat "$DASHBOARD_RT_PID") > /dev/null 2>&1; then
        echo -e "  ${YELLOW}Already running${NC} (PID: $(cat "$DASHBOARD_RT_PID"))"
    else
        cd "$ROOT_DIR"
        nohup streamlit run dashboard_realtime.py \
            --server.port 8502 \
            --server.address 0.0.0.0 \
            --server.headless true \
            > "$DASHBOARD_RT_LOG" 2>&1 &
        echo $! > "$DASHBOARD_RT_PID"
        sleep 3
        if ps -p $(cat "$DASHBOARD_RT_PID") > /dev/null 2>&1; then
            echo -e "  ${GREEN}✓ Started${NC} (PID: $(cat "$DASHBOARD_RT_PID"))"
            echo -e "  ${BLUE}URL:${NC} http://localhost:8502"
        else
            echo -e "  ${RED}✗ Failed to start${NC}"
            rm -f "$DASHBOARD_RT_PID"
        fi
    fi
}

stop_process() {
    local name=$1
    local pid_file=$2
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "  Stopping $name (PID: $pid)..."
            kill "$pid"
            sleep 2
            if ps -p "$pid" > /dev/null 2>&1; then
                echo -e "  ${YELLOW}Force killing...${NC}"
                kill -9 "$pid"
            fi
            echo -e "  ${GREEN}✓ Stopped${NC}"
        fi
        rm -f "$pid_file"
    else
        echo -e "  ${YELLOW}Not running${NC}"
    fi
}

# ── Commands ──────────────────────────────────────────────────────────────────

cmd_start() {
    print_header
    echo -e "${GREEN}Starting all processes...${NC}\n"
    
    start_scheduler
    echo ""
    start_vector_worker
    echo ""
    start_dashboard_demo
    echo ""
    start_dashboard_rt
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}All processes started!${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
}

cmd_stop() {
    print_header
    echo -e "${RED}Stopping all processes...${NC}\n"
    
    echo -e "${BLUE}[1/4]${NC} Scheduler:"
    stop_process "Scheduler" "$SCHEDULER_PID"
    echo ""
    
    echo -e "${BLUE}[2/4]${NC} Vector Worker:"
    stop_process "Vector Worker" "$VECTOR_WORKER_PID"
    echo ""
    
    echo -e "${BLUE}[3/4]${NC} Dashboard Demo:"
    stop_process "Dashboard Demo" "$DASHBOARD_DEMO_PID"
    echo ""
    
    echo -e "${BLUE}[4/4]${NC} Dashboard Realtime:"
    stop_process "Dashboard Realtime" "$DASHBOARD_RT_PID"
    echo ""
    
    echo -e "${RED}═══════════════════════════════════════════════════════${NC}"
    echo -e "${RED}All processes stopped!${NC}"
    echo -e "${RED}═══════════════════════════════════════════════════════${NC}"
}

cmd_status() {
    print_header
    echo -e "${BLUE}Process Status:${NC}\n"
    
    echo -e "${BLUE}[1/4]${NC} Scheduler:         $(check_process "$SCHEDULER_PID")"
    echo -e "${BLUE}[2/4]${NC} Vector Worker:     $(check_process "$VECTOR_WORKER_PID")"
    echo -e "${BLUE}[3/4]${NC} Dashboard Demo:    $(check_process "$DASHBOARD_DEMO_PID")"
    echo -e "${BLUE}[4/4]${NC} Dashboard RT:      $(check_process "$DASHBOARD_RT_PID")"
    echo ""
    echo -e "${BLUE}Log files:${NC}"
    echo -e "  Scheduler:      $SCHEDULER_LOG"
    echo -e "  Vector Worker:  $VECTOR_WORKER_LOG"
    echo -e "  Dashboard Demo: $DASHBOARD_DEMO_LOG"
    echo -e "  Dashboard RT:   $DASHBOARD_RT_LOG"
}

cmd_restart() {
    cmd_stop
    echo ""
    sleep 2
    cmd_start
}

cmd_logs() {
    local component=${2:-all}
    
    case $component in
        scheduler)
            tail -f "$SCHEDULER_LOG"
            ;;
        vector)
            tail -f "$VECTOR_WORKER_LOG"
            ;;
        demo)
            tail -f "$DASHBOARD_DEMO_LOG"
            ;;
        rt)
            tail -f "$DASHBOARD_RT_LOG"
            ;;
        all|*)
            echo "Available logs:"
            echo "  scheduler  - Scheduler logs"
            echo "  vector     - Vector Worker logs"
            echo "  demo       - Dashboard Demo logs"
            echo "  rt         - Dashboard Realtime logs"
            echo ""
            echo "Usage: $0 logs [scheduler|vector|demo|rt]"
            ;;
    esac
}

# ── Main ──────────────────────────────────────────────────────────────────────

case "${1:-status}" in
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    restart)
        cmd_restart
        ;;
    status)
        cmd_status
        ;;
    logs)
        cmd_logs "$@"
        ;;
    *)
        print_header
        echo "Usage: $0 {start|stop|restart|status|logs [component]}"
        echo ""
        echo "Commands:"
        echo "  start    - Start all processes"
        echo "  stop     - Stop all processes"
        echo "  restart  - Restart all processes"
        echo "  status   - Show process status"
        echo "  logs     - View logs (specify component: scheduler|vector|demo|rt)"
        echo ""
        exit 1
        ;;
esac
