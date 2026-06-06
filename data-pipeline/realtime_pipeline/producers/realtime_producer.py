# -*- coding: utf-8 -*-
"""
realtime_pipeline/producers/realtime_producer.py

Producer trung tâm cho realtime pipeline (Đã tối ưu cho kiến trúc Kafka-Native):
  1. Đẩy dữ liệu Social/News vào Kafka topic fb_mock_data (hoặc news_data)
  2. Đẩy dữ liệu Market vào Kafka topic market_stock_data
  3. Đẩy văn bản chính sách vào Kafka topic policy_data (để Vector Worker xử lý sau)

Design: Chỉ tập trung vào tốc độ I/O. Mọi tác vụ nặng (Embedding) được đẩy sang Consumer.
"""

import logging
from typing import Any, Dict, List

# Kế thừa BaseKafkaProducer từ code cũ của bạn
from data_pipeline_ingestion.base_producer import BaseKafkaProducer
from realtime_pipeline.config import (
    KAFKA_TOPIC_NEWS,
    KAFKA_TOPIC_SOCIAL,
    KAFKA_TOPIC_MARKET,
    KAFKA_TOPIC_POLICY,
)

logger = logging.getLogger(__name__)

# ── Kafka producers (Siêu nhẹ) ──────────────────────────────────────────────────

class SocialRealtimeProducer(BaseKafkaProducer):
    """Đẩy dữ liệu social/news vào Kafka."""
    def __init__(self):
        # Giữ fb_mock_data để tương thích ngược với code cũ của Agent
        super().__init__(topic="fb_mock_data")

    def produce_batch(self, messages: List[Dict[str, Any]]) -> int:
        success = 0
        for msg in messages:
            if self.send_message(msg):
                success += 1
        if success:
            self.producer.flush()
        self.logger.info(f"Social producer: {success}/{len(messages)} messages → fb_mock_data")
        return success


class MarketRealtimeProducer(BaseKafkaProducer):
    """Đẩy dữ liệu giá cổ phiếu vào Kafka."""
    def __init__(self):
        super().__init__(topic=KAFKA_TOPIC_MARKET)

    def produce_batch(self, bars: List[Dict[str, Any]]) -> int:
        success = 0
        for bar in bars:
            if self.send_message(bar):
                success += 1
        if success:
            self.producer.flush()
        self.logger.info(f"Market producer: {success}/{len(bars)} bars → {KAFKA_TOPIC_MARKET}")
        return success


class PolicyRealtimeProducer(BaseKafkaProducer):
    """
    Đẩy văn bản NHNN vào Kafka thay vì gọi ChromaDB trực tiếp.
    Vector Worker sẽ hứng dữ liệu từ topic này.
    """
    def __init__(self):
        super().__init__(topic=KAFKA_TOPIC_POLICY)

    def produce_batch(self, docs: List[Dict[str, Any]]) -> int:
        success = 0
        for doc in docs:
            if self.send_message(doc):
                success += 1
        if success:
            self.producer.flush()
        self.logger.info(f"Policy producer: {success}/{len(docs)} docs → {KAFKA_TOPIC_POLICY}")
        return success


# ── Unified RealtimeProducer ───────────────────────────────────────────────────

class RealtimeProducer:
    """
    Bộ điều phối tổng quản lý tất cả các vòi bơm Kafka.
    """
    def __init__(self):
        self.logger  = logging.getLogger(self.__class__.__name__)
        self._social = None
        self._market = None
        self._policy = None
        self._init_components()

    def _init_components(self):
        try:
            self._social = SocialRealtimeProducer()
            self.logger.info("✅ Social Kafka producer: Sẵn sàng")
        except Exception as e:
            self.logger.error(f"❌ Lỗi khởi tạo Social producer: {e}")

        try:
            self._market = MarketRealtimeProducer()
            self.logger.info("✅ Market Kafka producer: Sẵn sàng")
        except Exception as e:
            self.logger.error(f"❌ Lỗi khởi tạo Market producer: {e}")

        try:
            self._policy = PolicyRealtimeProducer()
            self.logger.info("✅ Policy Kafka producer: Sẵn sàng")
        except Exception as e:
            self.logger.error(f"❌ Lỗi khởi tạo Policy producer: {e}")

    def push_social(self, messages: List[Dict[str, Any]]) -> int:
        if not self._social: return 0
        return self._social.produce_batch(messages)

    def push_market(self, bars: List[Dict[str, Any]]) -> int:
        if not self._market: return 0
        return self._market.produce_batch(bars)

    def push_policies(self, docs: List[Dict[str, Any]]) -> int:
        if not self._policy: return 0
        return self._policy.produce_batch(docs)

    def close(self):
        """Đóng kết nối an toàn."""
        for producer in [self._social, self._market, self._policy]:
            if producer:
                try:
                    producer.flush_and_close()
                except Exception:
                    pass
        self.logger.info("RealtimeProducer đã đóng tất cả kết nối Kafka.")