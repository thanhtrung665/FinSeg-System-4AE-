import os
import time
import warnings
from datetime import datetime, timezone

# Suppress vnstock banner TRUOC khi import
os.environ.setdefault("VNSTOCK_DISABLE_NOTIFICATION", "1")
os.environ.setdefault("VNSTOCK_SHOW_ADS", "0")
warnings.filterwarnings("ignore")

from vnstock.api.quote import Quote
from base_producer import BaseKafkaProducer


class MarketDataProducer(BaseKafkaProducer):
    def __init__(self, ticker_context: str = "SHB"):
        super().__init__(topic="market_stock_data")
        self.ticker_context = ticker_context

    def stream_historical_data(self, ticker: str, start_date: str, end_date: str):
        """Keo du lieu gia lich su va day vao Kafka (dung vnstock.api moi)."""
        self.logger.info(
            f"Keo du lieu {ticker} (Context: {self.ticker_context}) "
            f"tu {start_date} den {end_date}"
        )
        try:
            q  = Quote(symbol=ticker, source="VCI")
            df = q.history(start=start_date, end=end_date, interval="1D")

            if df is None or df.empty:
                self.logger.warning(f"Khong co du lieu cho ma {ticker}.")
                return

            count = 0
            for _, row in df.iterrows():
                market_msg = {
                    "timestamp":      datetime.now(timezone.utc).isoformat(),
                    "ticker_context": self.ticker_context,
                    "source":         "vnstock",
                    "content": {
                        "trading_date": str(row.get("time",   "")),
                        "open":         float(row.get("open",   0)),
                        "high":         float(row.get("high",   0)),
                        "low":          float(row.get("low",    0)),
                        "close":        float(row.get("close",  0)),
                        "volume":       int(row.get("volume",   0)),
                    },
                }
                if self.send_message(market_msg):
                    count += 1
                time.sleep(0.05)

            self.logger.info(
                f"Stream thanh cong {count} phien giao dich {ticker}."
            )
        except Exception as e:
            self.logger.error(f"Loi keo du lieu vnstock {ticker}: {e}")
