import os
import csv
import time
from base_producer import BaseKafkaProducer
from normalizer import normalize_csv_row
from config import settings

class FacebookMockInjector(BaseKafkaProducer):
    def __init__(self, ticker_context: str = "SHB"):
        # Chỉ định đích đến là topic fb_mock_data
        super().__init__(topic='fb_mock_data')
        self.ticker_context = ticker_context
        self.rate_limit = float(settings.REPLAY_RATE_LIMIT)

    def stream_csv_data(self, file_path: str):
        dataset_name = os.path.basename(file_path)
        self.logger.info(f"Bắt đầu bơm dữ liệu mạng xã hội từ {dataset_name} (Context: {self.ticker_context})")
        
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                count = 0
                
                for row in reader:
                    # Bỏ qua dòng trống
                    if not any(row.values()):
                        continue
                    
                    try:
                        # 1. Tái sử dụng hàm chuẩn hóa dữ liệu từ Giai đoạn 1
                        normalized_data = normalize_csv_row(row, dataset_name)
                        
                        # 2. ĐÍNH KÈM NGỮ CẢNH (Yếu tố cốt lõi của Giai đoạn 2)
                        normalized_data['ticker_context'] = self.ticker_context
                        
                    except Exception as e:
                        self.logger.warning(f"Bỏ qua dòng lỗi do sai định dạng CSV: {e}")
                        continue
                        
                    # 3. Gửi dữ liệu qua lớp cha (Tự động ép về Standard JSON chuẩn mực)
                    if self.send_message(normalized_data):
                        count += 1
                        self.logger.info(f"[{self.ticker_context}] Đã đẩy tin: {normalized_data.get('ticker')} | ID: {normalized_data.get('comment_id')} | Likes: {normalized_data.get('likes')}")
                    
                    # 4. Giả lập tốc độ dòng thời gian (Real-time simulation)
                    time.sleep(self.rate_limit)
                    
            # Đẩy toàn bộ dữ liệu đọng trong buffer trước khi kết thúc
            self.flush_and_close()
            self.logger.info(f"Hoàn thành: Đã bơm {count} bình luận vào Kafka.")
            
        except FileNotFoundError:
            self.logger.error(f"CRITICAL: Không tìm thấy file {file_path}. Vui lòng kiểm tra lại đường dẫn.")