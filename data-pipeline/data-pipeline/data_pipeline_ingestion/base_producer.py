import json
import time
import logging
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import cấu hình tập trung từ Giai đoạn 1
from config import settings

class BaseKafkaProducer:
    """
    Lớp nền tảng quản lý kết nối Kafka (Local).
    Đảm bảo tính chịu lỗi (Fault Tolerance) và chuẩn hóa Standard JSON.
    """
    def __init__(self, topic: str):
        self.topic = topic
        # Thiết lập logger tự động lấy tên của lớp con (MarketDataProducer, FacebookMockInjector,...)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.producer = self._connect_kafka_with_retry()

    def _connect_kafka_with_retry(self) -> KafkaProducer:
        """
        Tự động thử lại kết nối với cơ chế Exponential Backoff cơ bản.
        Giúp hệ thống không bị crash nếu chạy script khi Docker Kafka chưa kịp khởi động hoàn toàn.
        """
        max_retries = 5
        retry_delay = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Đang thử kết nối Kafka tại {settings.KAFKA_BROKER} (Lần {attempt}/{max_retries})...")
                producer = KafkaProducer(
                    bootstrap_servers=[settings.KAFKA_BROKER],
                    # BẮT BUỘC: Ép kiểu dữ liệu xuất ra thành Standard JSON (tuyệt đối không dùng JSONL)
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',  # Đảm bảo dữ liệu không bị mất (wait for all replicas)
                    retries=3    # Tự động retry ở mức độ Kafka Client
                )
                self.logger.info("Kết nối Kafka Local thành công!")
                return producer
            except KafkaError as e:
                self.logger.warning(f"Lỗi kết nối: {e}. Thử lại sau {retry_delay} giây...")
                time.sleep(retry_delay)
                
        self.logger.error("CRITICAL: Không thể kết nối tới Kafka Broker. Vui lòng kiểm tra lại lệnh docker-compose!")
        raise ConnectionError("Kafka Broker không phản hồi.")

    def send_message(self, message: Dict[str, Any]) -> bool:
        """
        Gửi một thông điệp vào Kafka Topic.
        Chỉ nhận kiểu dữ liệu Dictionary để dump ra Standard JSON an toàn.
        """
        if not isinstance(message, dict):
            self.logger.error("Dữ liệu đầu vào bắt buộc phải là Dictionary để ép thành Standard JSON.")
            return False
            
        try:
            # producer.send là hàm bất đồng bộ (asynchronous)
            self.producer.send(self.topic, value=message)
            return True
        except Exception as e:
            self.logger.error(f"Lỗi khi gửi dữ liệu vào topic {self.topic}: {e}")
            return False

    def flush_and_close(self):
        """Đảm bảo mọi dữ liệu còn đọng trong buffer được đẩy đi hết trước khi tắt app"""
        if hasattr(self, 'producer'):
            self.logger.info(f"Đang xả (flush) buffer cho topic {self.topic}...")
            self.producer.flush()
            self.producer.close()
            self.logger.info("Đã đóng kết nối Kafka an toàn.")