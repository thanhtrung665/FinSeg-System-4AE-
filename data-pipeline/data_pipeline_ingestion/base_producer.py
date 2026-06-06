import json
import time
import logging
import sys
import os
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Đảm bảo import config hoạt động dù gọi từ thư mục nào
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_pipeline_ingestion.config import settings


class BaseKafkaProducer:
    """
    Lớp nền tảng quản lý kết nối Kafka.
    Đảm bảo tính chịu lỗi (Fault Tolerance) và chuẩn hóa Standard JSON.
    """

    def __init__(self, topic: str):
        self.topic = topic
        self.logger = logging.getLogger(self.__class__.__name__)
        self.producer = self._connect_kafka_with_retry()

    def _connect_kafka_with_retry(self) -> KafkaProducer:
        """Kết nối Kafka với cơ chế Exponential Backoff."""
        max_retries = 5
        retry_delay = 3

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(
                    f"Đang thử kết nối Kafka tại {settings.KAFKA_BROKER} "
                    f"(Lần {attempt}/{max_retries})..."
                )
                producer = KafkaProducer(
                    bootstrap_servers=[settings.KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                    acks='all',
                    retries=3,
                )
                self.logger.info("Kết nối Kafka thành công!")
                return producer
            except KafkaError as e:
                self.logger.warning(f"Lỗi kết nối: {e}. Thử lại sau {retry_delay} giây...")
                time.sleep(retry_delay)

        self.logger.error(
            f"CRITICAL: Không thể kết nối Kafka tại {settings.KAFKA_BROKER}. "
            "Vui lòng kiểm tra Kafka đang chạy."
        )
        raise ConnectionError("Kafka Broker không phản hồi.")

    def send_message(self, message: Dict[str, Any]) -> bool:
        """Gửi một message Dictionary vào Kafka topic."""
        if not isinstance(message, dict):
            self.logger.error("Dữ liệu đầu vào phải là Dictionary.")
            return False

        try:
            self.producer.send(self.topic, value=message)
            return True
        except Exception as e:
            self.logger.error(f"Lỗi khi gửi dữ liệu vào topic {self.topic}: {e}")
            return False

    def flush_and_close(self):
        """Flush buffer và đóng kết nối Kafka an toàn."""
        if hasattr(self, 'producer') and self.producer:
            self.logger.info(f"Đang flush buffer cho topic {self.topic}...")
            self.producer.flush()
            self.producer.close()
            self.logger.info("Đã đóng kết nối Kafka an toàn.")
