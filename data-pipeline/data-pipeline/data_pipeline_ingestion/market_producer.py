import time
from datetime import datetime, timezone
from vnstock import stock_historical_data
from base_producer import BaseKafkaProducer

class MarketDataProducer(BaseKafkaProducer):
    def __init__(self, ticker_context: str = "SHB"):
        # Gọi hàm khởi tạo của lớp cha, chỉ định đích đến là topic market_stock_data
        super().__init__(topic='market_stock_data')
        self.ticker_context = ticker_context

    def stream_historical_data(self, ticker: str, start_date: str, end_date: str):
        self.logger.info(f"Kéo dữ liệu {ticker} (Context: {self.ticker_context}) từ {start_date} đến {end_date}")
        try:
            df = stock_historical_data(symbol=ticker, 
                                       start_date=start_date, 
                                       end_date=end_date, 
                                       resolution='1D', 
                                       type='stock')
            if df.empty:
                self.logger.warning(f"Không có dữ liệu cho mã {ticker} trong giai đoạn này.")
                return

            count = 0
            for index, row in df.iterrows():
                # Đóng gói thành Dictionary, lớp cha sẽ tự động ép thành Standard JSON
                market_msg = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ticker_context": self.ticker_context,  # Gắn nhãn ngữ cảnh bắt buộc
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
                
                # Gọi hàm send_message từ lớp cha, an toàn và chuẩn xác
                if self.send_message(market_msg):
                    count += 1
                
                time.sleep(0.05) # Rate limit nhỏ để không dội tải hệ thống

            self.logger.info(f"Thành công: Đã stream {count} phiên giao dịch của {ticker} vào hệ thống.")
        except Exception as e:
            self.logger.error(f"Lỗi hệ thống khi kéo dữ liệu vnstock: {e}")