import json
import time
import logging
from typing import Dict, Any, List, Tuple
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import sys
import os
# Trỏ về thư mục data-pipeline/ để import data_pipeline_ingestion
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from data_pipeline_ingestion.config import settings


class SocialAgent:
    """
    Agent tiêu thụ dữ liệu mạng xã hội từ Kafka topic fb_mock_data.
    Tích hợp Circuit Breaker và Dead Letter Queue (DLQ).
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.topic = 'fb_mock_data'

        # Circuit Breaker
        self.failure_count = 0
        self.circuit_open = False

        self.consumer = self._connect_kafka_with_retry()

    def _connect_kafka_with_retry(self) -> KafkaConsumer:
        """Kết nối Kafka Consumer với cơ chế Exponential Backoff."""
        max_retries = 5
        base_delay = 1.0

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(
                    f"Đang thử kết nối Consumer tới {settings.KAFKA_BROKER} "
                    f"(Lần {attempt}/{max_retries})..."
                )
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[settings.KAFKA_BROKER],
                    group_id='social_sentiment_group',
                    # 'earliest' để đọc lại toàn bộ dữ liệu khi consumer group mới
                    # (quan trọng khi deploy lên server mới)
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    # Timeout kết nối hợp lý cho môi trường server
                    request_timeout_ms=30000,
                    session_timeout_ms=10000,
                )
                self.logger.info("Social Agent kết nối Kafka thành công!")
                self.circuit_open = False
                self.failure_count = 0
                return consumer
            except KafkaError as e:
                delay = base_delay * (2 ** (attempt - 1))
                self.logger.warning(f"Lỗi kết nối Consumer: {e}. Thử lại sau {delay:.1f}s...")
                time.sleep(delay)

        self.circuit_open = True
        self.logger.error("CRITICAL: Circuit Breaker [OPEN]. Không thể kết nối Kafka Consumer.")
        raise ConnectionError("Social Agent không thể kết nối tới Kafka Broker.")

    def close(self):
        """Đóng consumer an toàn khi shutdown."""
        if hasattr(self, 'consumer') and self.consumer:
            try:
                self.consumer.close()
                self.logger.info("Đã đóng Kafka Consumer.")
            except Exception:
                pass

    def _route_to_dlq(self, message: Any, error_reason: str):
        """
        Dead Letter Queue: log gói tin lỗi, không crash luồng chính.
        TODO production: đẩy vào Kafka topic 'dlq_sentiment_topic'.
        """
        self.logger.error(
            f"[DLQ ROUTED] Lý do: {error_reason} | "
            f"Data: {str(message)[:200]}..."
        )

    def extract_phobert_scores(self, message: Dict) -> float:
        """
        Trích xuất điểm sentiment từ message.
        - Nếu pipeline đã chạy PhoBERT thực: dùng message['sentiment']['label'] + ['confidence']
        - Fallback: heuristic keyword trên content_text
        Trả về float trong dải [-1.0, 1.0].
        """
        sentiment_data = message.get("sentiment", {})
        label = sentiment_data.get("label", "Neutral")
        confidence = float(sentiment_data.get("confidence", 0.0))

        if label.lower() == "positive":
            return min(confidence, 1.0)
        elif label.lower() == "negative":
            return max(-confidence, -1.0)

        # Fallback heuristic keyword
        content = message.get("content_text", "").lower()
        if any(kw in content for kw in ["cứu", "sập", "bắt đáy", "hoảng", "rút tiền"]):
            return -0.8
        elif any(kw in content for kw in ["múc", "tím", "lên", "tăng mạnh", "bứt phá"]):
            return 0.8

        return 0.0  # Neutral

    def extract_interaction_metrics(self, message: Dict) -> Tuple[int, int, int]:
        """Trích xuất (likes, shares, comments) — chặn giá trị âm."""
        try:
            likes = max(0, int(message.get("likes", 0)))
            shares = max(0, int(message.get("shares", 0)))
            comments = max(0, int(message.get("comments", 0)))
            return likes, shares, comments
        except (ValueError, TypeError):
            return 0, 0, 0

    def extract_credibility_factors(self, message: Dict) -> float:
        """
        Tính độ uy tín nguồn tin, dải [0.1, 1.0].
        f319 < facebook < báo chính thống.
        """
        source = message.get("source_dataset", "").lower()
        if "f319" in source:
            return 0.4
        elif "facebook" in source:
            return 0.5
        return 0.5  # default

    def process_message_batch(self, batch_size: int = 100, timeout_ms: int = 3000) -> List[Dict]:
        """
        Poll một batch từ Kafka và trả về list payload sạch.
        Dùng poll() trực tiếp — không wrap trong thread để tránh socket fd leak.
        """
        if self.circuit_open:
            self.logger.warning("Circuit Breaker [OPEN] — bỏ qua poll.")
            return []

        processed_batch = []
        try:
            # poll() block tối đa timeout_ms ms rồi trả về (thread-safe trực tiếp)
            msg_pack = self.consumer.poll(timeout_ms=timeout_ms, max_records=batch_size)

            for _tp, messages in msg_pack.items():
                for msg in messages:
                    raw_data = msg.value
                    try:
                        phobert_score = self.extract_phobert_scores(raw_data)
                        likes, shares, comments = self.extract_interaction_metrics(raw_data)
                        credibility = self.extract_credibility_factors(raw_data)

                        clean_payload = {
                            "comment_id": raw_data.get("comment_id", "unknown"),
                            "ticker_context": raw_data.get("ticker_context", "SHB"),
                            "phobert_score": phobert_score,
                            "likes": likes,
                            "shares": shares,
                            "comments": comments,
                            "credibility": credibility,
                            "timestamp": raw_data.get("created_at", ""),
                        }
                        processed_batch.append(clean_payload)

                    except Exception as e:
                        self._route_to_dlq(raw_data, str(e))

        except Exception as e:
            self.failure_count += 1
            self.logger.error(f"Lỗi khi poll Kafka: {e}")
            if self.failure_count >= 3:
                self.circuit_open = True
                self.logger.error("Circuit Breaker kích hoạt sau 3 lỗi liên tiếp.")

        return processed_batch


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    agent = SocialAgent()
    logging.info("Social Agent đang lắng nghe dữ liệu... (Ctrl+C để dừng)")
    try:
        while True:
            batch = agent.process_message_batch(batch_size=50, timeout_ms=3000)
            if batch:
                logging.info(f"Đã xử lý {len(batch)} tin nhắn.")
                for item in batch[:2]:
                    logging.info(f"  -> {item}")
            time.sleep(2)
    except KeyboardInterrupt:
        logging.info("Tắt Social Agent.")
        agent.close()
