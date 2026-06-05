import json
import logging
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from vnstock import stock_historical_data
from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MarketDataProducer:
    def __init__(self):
        logger.info("Đang kết nối Kafka Broker cho Market Data...")
        self.producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'market_stock_data'

    def stream_historical_data(self, ticker: str, start_date: str, end_date: str):
        """Kéo dữ liệu giá lịch sử và đẩy vào Kafka"""
        logger.info(f"Đang tải dữ liệu {ticker} từ {start_date} đến {end_date}...")
        
        try:
            # Lấy dữ liệu từ thư viện vnstock
            df = stock_historical_data(symbol=ticker, 
                                       start_date=start_date, 
                                       end_date=end_date, 
                                       resolution='1D', 
                                       type='stock')
            
            if df.empty:
                logger.warning(f"Không có dữ liệu cho mã {ticker} trong khoảng thời gian này.")
                return

            count = 0
            # Duyệt qua từng dòng dữ liệu chứng khoán
            for index, row in df.iterrows():
                # Chuẩn hóa format (Standard JSON)
                market_msg = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ticker_context": ticker,
                    "source": "vnstock",
                    "content": {
                        "trading_date": str(row.get('time', '')),
                        "open": float(row.get('open', 0)),
                        "high": float(row.get('high', 0)),
                        "low": float(row.get('low', 0)),
                        "close": float(row.get('close', 0)),
                        "volume": int(row.get('volume', 0))
                    }
                }
                
                self.producer.send(self.topic, value=market_msg)
                count += 1
                
                # Cố tình delay một chút để không làm nghẽn Kafka local
                time.sleep(0.1)

            self.producer.flush()
            logger.info(f"Đã stream thành công {count} phiên giao dịch của {ticker} vào Kafka.")
            
        except Exception as e:
            logger.error(f"Lỗi khi kéo dữ liệu vnstock: {e}")

if __name__ == "__main__":
    producer = MarketDataProducer()
    
    # Kéo dữ liệu Baseline cho SHB (Giai đoạn đầu năm 2026)
    # Lưu ý: vnstock định dạng ngày là YYYY-MM-DD
    producer.stream_historical_data(
        ticker="SHB", 
        start_date="2026-01-01", 
        end_date="2026-06-01"
    )
    
    # Đối với SCB, do không niêm yết public nên chúng ta dùng VNINDEX làm proxy như thiết kế
    producer.stream_historical_data(
        ticker="VNINDEX", 
        start_date="2022-10-01", 
        end_date="2022-12-31"
    )