import csv
import json
import time
import logging
from kafka import KafkaProducer
from config import settings
from normalizer import normalize_csv_row

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataReplayService:
    def __init__(self):
        logger.info(f"Đang kết nối tới Kafka Broker tại {settings.KAFKA_BROKER}...")
        self.producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BROKER],
            # Ép chuẩn hóa dữ liệu đầu ra thành byte string của Standard JSON
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'fb_mock_data'
        self.rate_limit = float(settings.REPLAY_RATE_LIMIT)

    def stream_csv_to_kafka(self, file_path: str):
        """Đọc file CSV và đẩy từng dòng vào Kafka"""
        dataset_name = file_path.split('/')[-1]
        logger.info(f"Bắt đầu stream dữ liệu từ: {dataset_name}")
        
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                
                count = 0
                for row in reader:
                    # Bỏ qua các dòng trống hoàn toàn
                    if not any(row.values()):
                        continue
                        
                    # Bước 1: Chuẩn hóa dữ liệu
                    try:
                        normalized_data = normalize_csv_row(row, dataset_name)
                    except Exception as e:
                        logger.warning(f"Bỏ qua dòng lỗi trong {dataset_name}: {e}")
                        continue
                        
                    # Bước 2: Bắn vào Kafka (Standard JSON format)
                    self.producer.send(self.topic, value=normalized_data)
                    count += 1
                    
                    logger.info(f"Đã đẩy: {normalized_data['ticker']} | Shares: {normalized_data['shares']} | Comments: {normalized_data['comments']}")
                    
                    # Bước 3: Nghỉ 1 nhịp để giả lập Real-time
                    time.sleep(self.rate_limit)
                    
            # Chờ Kafka gửi nốt các message còn đọng
            self.producer.flush()
            logger.info(f"Hoàn thành! Đã stream {count} bản ghi từ {dataset_name} vào Kafka.")
            
        except FileNotFoundError:
            logger.error(f"Không tìm thấy file: {file_path}. Vui lòng kiểm tra lại đường dẫn trong .env!")
        except Exception as e:
            logger.error(f"Lỗi không xác định khi stream dữ liệu: {e}")

if __name__ == "__main__":
    service = DataReplayService()
    
    # Bạn có thể chọn file cần test chạy giả lập ở đây
    # Chạy thử dataset của SCB
    service.stream_csv_to_kafka(settings.SCB_DATASET_PATH)
    
    # Chạy thử dataset của SHB (bỏ comment dòng dưới để test)
    # service.stream_csv_to_kafka(settings.SHB_DATASET_PATH)