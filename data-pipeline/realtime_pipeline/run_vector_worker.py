# -*- coding: utf-8 -*-
"""
realtime_pipeline/run_vector_worker.py

Entry point cho Vector Worker - Kafka Consumer chuyen dung cho Vector Database.
Chay song song voi Scheduler chinh.

Chuc nang:
  1. Lang nghe Kafka topics: news_data, policy_data
  2. Thuc hien Chunking (1500 chars, overlap 200)
  3. Embedding bang keepitreal/vietnamese-sbert
  4. Ingest len ChromaDB Cloud

Su dung:
  python realtime_pipeline/run_vector_worker.py
  
  # Hoac chay background (GPU server):
  nohup python realtime_pipeline/run_vector_worker.py > logs/vector_worker.log 2>&1 &
"""

import sys
import logging
import signal
from pathlib import Path

# ── Setup path ────────────────────────────────────────────────────────────────
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PIPELINE_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("VectorWorkerMain")


def main():
    logger.info("=== KHOI DONG VECTOR WORKER ===")
    logger.info("Chuc nang: Chunking + Embedding + ChromaDB Ingestion")
    logger.info("Topics lang nghe: news_data, policy_data")
    logger.info("Bam Ctrl+C de dung...")
    
    try:
        from realtime_pipeline.crawlers.vector_worker import RealtimeVectorIngestor
        
        worker = RealtimeVectorIngestor()
        
        # Signal handler de shutdown gracefully
        def shutdown_handler(sig, frame):
            logger.info("Nhan tin hieu dung. Shutdown Vector Worker...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
        # Start consumer loop
        worker.run_consumer()
        
    except KeyboardInterrupt:
        logger.info("Vector Worker dung boi nguoi dung.")
    except Exception as e:
        logger.error(f"Vector Worker gap loi: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
