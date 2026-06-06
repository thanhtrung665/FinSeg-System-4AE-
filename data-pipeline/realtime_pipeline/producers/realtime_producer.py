# -*- coding: utf-8 -*-
"""
realtime_pipeline/producers/realtime_producer.py

Producer trung tam cho realtime pipeline:
  1. Day du lieu Social/News vao Kafka topic fb_mock_data
     (de SocialAgent hien tai co the doc duoc ngay)
  2. Day du lieu Market vao Kafka topic market_stock_data
  3. Ingest van ban chinh sach vao ChromaDB realtime collection
     (de MacroAgent co the query)

Design: Ke thua logic BaseKafkaProducer cu, mo rong them ChromaDB ingest.
"""

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional

from data_pipeline_ingestion.base_producer import BaseKafkaProducer
from data_pipeline_ingestion.config        import settings as base_settings
from realtime_pipeline.config import (
    KAFKA_TOPIC_NEWS,
    KAFKA_TOPIC_SOCIAL,
    KAFKA_TOPIC_MARKET,
    KAFKA_TOPIC_POLICY,
    CHROMA_REALTIME_COLLECTION,
    get_chroma_client,
)

logger = logging.getLogger(__name__)

# Embedding model (dung chung voi MacroAgent)
_EMBEDDING_MODEL = "keepitreal/vietnamese-sbert"


# ── ChromaDB ingestor ─────────────────────────────────────────────────────────

class RealtimeChromaIngestor:
    """
    Ingest van ban chinh sach va tin tuc vao ChromaDB realtime collection.
    Su dung cung embedding model (keepitreal/vietnamese-sbert, 768 dim)
    de MacroAgent co the query ngay.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.embedder   = self._load_embedder()
        self.collection = self._setup_collection()

    def _load_embedder(self):
        try:
            from sentence_transformers import SentenceTransformer
            self.logger.info(f"Dang tai embedding model: {_EMBEDDING_MODEL}")
            return SentenceTransformer(_EMBEDDING_MODEL)
        except Exception as e:
            self.logger.error(f"Loi tai embedder: {e}")
            return None

    def _setup_collection(self):
        try:
            client = get_chroma_client()
            col = client.get_or_create_collection(
                name=CHROMA_REALTIME_COLLECTION,
                metadata={"hnsw:space": "cosine"},
            )
            self.logger.info(f"ChromaDB collection '{CHROMA_REALTIME_COLLECTION}' san sang")
            return col
        except Exception as e:
            self.logger.error(f"Loi ket noi ChromaDB: {e}")
            return None

    def ingest_documents(self, docs: List[Dict[str, Any]]) -> int:
        """
        Ingest list normalized policy/news docs vao ChromaDB.
        Moi doc da co san 'content_text' va 'metadata'.
        Tra ve so luong ingest thanh cong.
        """
        if not self.embedder or not self.collection:
            self.logger.warning("ChromaDB/Embedder chua san sang, bo qua ingest")
            return 0

        success = 0
        chunk_size = 2000

        for doc in docs:
            content = doc.get("content_text", "").strip()
            if not content or len(content) < 50:
                continue

            metadata_base = doc.get("metadata", {
                "source_file":    doc.get("doc_id", "unknown"),
                "ticker_context": doc.get("ticker_context", ""),
                "publish_date":   doc.get("published_at", ""),
                "document_type":  doc.get("source_type", "news"),
                "chunk_index":    0,
            })

            chunks = [content[i:i + chunk_size]
                      for i in range(0, len(content), chunk_size)]

            for chunk_idx, chunk_text in enumerate(chunks):
                try:
                    vector = self.embedder.encode(
                        chunk_text, normalize_embeddings=True
                    ).tolist()

                    raw_id  = f"{doc.get('doc_id', 'doc')}_{chunk_idx}"
                    doc_id  = raw_id
                    if len(doc_id.encode("utf-8")) > 128:
                        h = hashlib.md5(raw_id.encode()).hexdigest()[:16]
                        doc_id = f"rt_{h}_{chunk_idx}"

                    meta = {**metadata_base, "chunk_index": chunk_idx}
                    # ChromaDB chi chap nhan str, int, float, bool
                    meta = {
                        k: (str(v) if not isinstance(v, (str, int, float, bool)) else v)
                        for k, v in meta.items()
                    }

                    self.collection.add(
                        documents  = [chunk_text],
                        embeddings = [vector],
                        metadatas  = [meta],
                        ids        = [doc_id],
                    )
                    success += 1
                    time.sleep(0.05)   # Rate limit ChromaDB Cloud

                except Exception as e:
                    if "already exists" in str(e).lower():
                        success += 1   # Da ingest roi, tinh la OK
                    else:
                        self.logger.debug(f"Loi ingest {doc_id}: {e}")

        self.logger.info(
            f"ChromaDB ingest: {success}/{len(docs)} docs vao '{CHROMA_REALTIME_COLLECTION}'"
        )
        return success


# ── Kafka producers ────────────────────────────────────────────────────────────

class SocialRealtimeProducer(BaseKafkaProducer):
    """
    Day du lieu social (Facebook posts + news articles) vao Kafka.
    Su dung TOPIC fb_mock_data (tuong thich nguoc voi SocialAgent hien tai)
    VA topic realtime_social moi.
    """

    def __init__(self):
        # Day vao fb_mock_data de SocialAgent hien tai co the doc ngay
        super().__init__(topic="fb_mock_data")
        self._rt_topic = KAFKA_TOPIC_SOCIAL

    def produce_batch(self, messages: List[Dict[str, Any]]) -> int:
        """Day batch messages vao Kafka, tra ve so luong thanh cong."""
        success = 0
        for msg in messages:
            if self.send_message(msg):
                success += 1
        if success:
            self.producer.flush()
        self.logger.info(f"Social producer: {success}/{len(messages)} messages → fb_mock_data")
        return success


class MarketRealtimeProducer(BaseKafkaProducer):
    """Day du lieu gia co phieu vao Kafka topic market_stock_data."""

    def __init__(self):
        super().__init__(topic="market_stock_data")

    def produce_batch(self, bars: List[Dict[str, Any]]) -> int:
        success = 0
        for bar in bars:
            if self.send_message(bar):
                success += 1
        if success:
            self.producer.flush()
        self.logger.info(f"Market producer: {success}/{len(bars)} bars → market_stock_data")
        return success


# ── Unified RealtimeProducer ───────────────────────────────────────────────────

class RealtimeProducer:
    """
    Producer tong hop quan ly tat ca luong du lieu:
      - Social/News → Kafka fb_mock_data
      - Market      → Kafka market_stock_data
      - Policy/Docs → ChromaDB realtime collection
    """

    def __init__(self):
        self.logger  = logging.getLogger(self.__class__.__name__)
        self._social = None
        self._market = None
        self._chroma = None
        self._init_components()

    def _init_components(self):
        # Social producer
        try:
            self._social = SocialRealtimeProducer()
            self.logger.info("Social Kafka producer: san sang")
        except Exception as e:
            self.logger.error(f"Loi khoi tao Social producer: {e}")

        # Market producer
        try:
            self._market = MarketRealtimeProducer()
            self.logger.info("Market Kafka producer: san sang")
        except Exception as e:
            self.logger.error(f"Loi khoi tao Market producer: {e}")

        # ChromaDB ingestor
        try:
            self._chroma = RealtimeChromaIngestor()
            self.logger.info("ChromaDB ingestor: san sang")
        except Exception as e:
            self.logger.error(f"Loi khoi tao ChromaDB ingestor: {e}")

    def push_social(self, messages: List[Dict[str, Any]]) -> int:
        """Day social/news messages vao Kafka."""
        if not self._social:
            self.logger.warning("Social producer chua san sang")
            return 0
        return self._social.produce_batch(messages)

    def push_market(self, bars: List[Dict[str, Any]]) -> int:
        """Day market bars vao Kafka."""
        if not self._market:
            self.logger.warning("Market producer chua san sang")
            return 0
        return self._market.produce_batch(bars)

    def push_policies_to_chroma(self, docs: List[Dict[str, Any]]) -> int:
        """Ingest policy/news docs vao ChromaDB de MacroAgent query."""
        if not self._chroma:
            self.logger.warning("ChromaDB ingestor chua san sang")
            return 0
        return self._chroma.ingest_documents(docs)

    def close(self):
        """Dong ket noi an toan."""
        if self._social:
            try:
                self._social.flush_and_close()
            except Exception:
                pass
        if self._market:
            try:
                self._market.flush_and_close()
            except Exception:
                pass
        self.logger.info("RealtimeProducer da dong tat ca ket noi.")
