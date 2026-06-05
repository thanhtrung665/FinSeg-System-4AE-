import json
import time
import logging
from typing import Dict, Any, List, Tuple
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Import cấu hình tập trung (tận dụng lại từ Giai đoạn 1/2)
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from data_pipeline_ingestion.config import settings

class SocialAgent:
    """
    Agent tiêu thụ dữ liệu mạng xã hội từ Kafka.
    Tích hợp cơ chế chịu lỗi (Fault Tolerance) và Dead Letter Queue (DLQ) chuẩn mực.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.topic = 'fb_mock_data' # Hoặc 'sentiment_scored_data' theo thiết kế
        
        # Cấu hình Circuit Breaker cơ bản
        self.failure_count = 0
        self.circuit_open = False
        
        # Kết nối Kafka an toàn
        self.consumer = self._connect_kafka_with_retry()

    def _connect_kafka_with_retry(self) -> KafkaConsumer:
        """Kết nối Kafka với cơ chế Exponential Backoff [Req 2.3]"""
        max_retries = 5
        base_delay = 1.0
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Đang thử kết nối Consumer tới {settings.KAFKA_BROKER} (Lần {attempt}/{max_retries})...")
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[settings.KAFKA_BROKER],
                    group_id='social_sentiment_group',
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                self.logger.info("Social Agent đã kết nối Kafka thành công!")
                self.circuit_open = False
                self.failure_count = 0
                return consumer
            except KafkaError as e:
                delay = base_delay * (2 ** (attempt - 1)) # Exponential backoff
                self.logger.warning(f"Lỗi kết nối Consumer: {e}. Thử lại sau {delay}s...")
                time.sleep(delay)
                
        self.circuit_open = True
        self.logger.error("CRITICAL: Circuit Breaker [OPEN]. Không thể kết nối Kafka Consumer.")
        raise ConnectionError("Social Agent không thể kết nối tới Kafka Broker.")

    def _route_to_dlq(self, message: Any, error_reason: str):
        """
        Dead Letter Queue (DLQ) [Req 2.4]
        Cách ly các gói tin lỗi, không để chúng làm sập luồng xử lý chính.
        Ở MVP, ta log lại thành file/cảnh báo. Ở Production sẽ đẩy vào 1 topic Kafka riêng.
        """
        self.logger.error(f"[DLQ ROUTED] Lý do: {error_reason} | Data: {str(message)[:200]}...")
        # TODO: Implement push to 'dlq_sentiment_topic' if needed

    def extract_phobert_scores(self, message: Dict) -> float:
        """
        Giả lập trích xuất điểm PhoBERT từ message [Req 2.5]
        Biến đổi nhãn (Positive/Negative/Neutral) và độ tự tin (Confidence) thành dải [-1, 1].
        """
        # Nếu data pipeline chưa tích hợp PhoBERT thực, ta giả lập điểm dựa trên like/comment
        # Trong bản đầy đủ, gói tin sẽ có trường sentiment.label và sentiment.confidence
        sentiment_data = message.get("sentiment", {})
        label = sentiment_data.get("label", "Neutral")
        confidence = float(sentiment_data.get("confidence", 0.0))

        if label.lower() == "positive":
            return min(confidence, 1.0)
        elif label.lower() == "negative":
            return max(-confidence, -1.0)
        
        # Fallback tính điểm thô nếu chưa chạy qua PhoBERT model
        content = message.get("content_text", "").lower()
        if "cứu" in content or "sập" in content or "bắt đáy" in content:
            return -0.8
        elif "múc" in content or "tím" in content or "lên" in content:
            return 0.8
            
        return 0.0 # Neutral

    def extract_interaction_metrics(self, message: Dict) -> Tuple[int, int, int]:
        """Trích xuất lượt tương tác, chặn số âm [Req 2.6]"""
        try:
            likes = max(0, int(message.get("likes", 0)))
            shares = max(0, int(message.get("shares", 0)))
            comments = max(0, int(message.get("comments", 0)))
            return likes, shares, comments
        except (ValueError, TypeError):
            return 0, 0, 0

    def extract_credibility_factors(self, message: Dict) -> float:
        """Trích xuất độ uy tín của nguồn tin, dải [0.1, 1.0] [Req 2.7]"""
        # Giả lập: Các bài viết từ F319 có độ uy tín thấp hơn báo chính thống
        source = message.get("source_dataset", "")
        if "f319" in source.lower():
            return 0.4
        elif "facebook" in source.lower():
            return 0.5
        return 0.5 # Mặc định

    def process_message_batch(self, batch_size: int = 100, timeout_ms: int = 1000) -> List[Dict]:
        """
        Tiêu thụ dữ liệu theo lô (Batch Processing) để tối ưu hiệu năng [Req 2.8]
        Trả về danh sách các dữ liệu đã được bóc tách sạch sẽ.
        """
        if self.circuit_open:
            self.logger.warning("Circuit Breaker đang MỞ. Tạm ngưng consume dữ liệu.")
            return []

        processed_batch = []
        try:
            # Poll dữ liệu từ Kafka
            msg_pack = self.consumer.poll(timeout_ms=timeout_ms, max_records=batch_size)
            
            for tp, messages in msg_pack.items():
                for msg in messages:
                    raw_data = msg.value
                    
                    try:
                        # 1. Trích xuất PhoBERT Score
                        phobert_score = self.extract_phobert_scores(raw_data)
                        
                        # 2. Trích xuất Tương tác
                        likes, shares, comments = self.extract_interaction_metrics(raw_data)
                        
                        # 3. Trích xuất Uy tín
                        credibility = self.extract_credibility_factors(raw_data)
                        
                        # 4. Đóng gói payload sạch để gửi cho Risk Synthesis Agent
                        clean_payload = {
                            "comment_id": raw_data.get("comment_id", "unknown"),
                            "ticker_context": raw_data.get("ticker_context", "SHB"),
                            "phobert_score": phobert_score,
                            "likes": likes,
                            "shares": shares,
                            "comments": comments,
                            "credibility": credibility,
                            "timestamp": raw_data.get("created_at", "")
                        }
                        processed_batch.append(clean_payload)
                        
                    except Exception as e:
                        # Gửi gói tin lỗi vào Dead Letter Queue
                        self._route_to_dlq(raw_data, str(e))
                        continue
                        
        except Exception as e:
            self.failure_count += 1
            self.logger.error(f"Lỗi khi poll dữ liệu từ Kafka: {e}")
            if self.failure_count >= 3:
                self.circuit_open = True
                self.logger.error("Đã kích hoạt Circuit Breaker: Tạm ngưng Consumer do lỗi liên tục.")

        return processed_batch

if __name__ == "__main__":
    # Khởi tạo logger để test
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    agent = SocialAgent()
    logging.info("Đang lắng nghe dữ liệu mạng xã hội...")
    
    # Test chạy loop để consume dữ liệu
    try:
        while True:
            batch = agent.process_message_batch()
            if batch:
                logging.info(f"Đã xử lý thành công {len(batch)} tin nhắn.")
                for item in batch[:2]: # In thử 2 tin đầu tiên
                    logging.info(f"-> {item}")
            time.sleep(2)
    except KeyboardInterrupt:
        logging.info("Tắt Social Agent.")