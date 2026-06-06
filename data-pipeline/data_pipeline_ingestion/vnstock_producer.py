import contextlib
import io
import json
import logging
import sys
import time
import warnings
from datetime import datetime, timezone
from kafka import KafkaProducer

# Suppress vnstock stdout/stderr banner truoc khi import
warnings.filterwarnings("ignore")
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    from vnstock.api.quote import Quote

sys.path.insert(0, ".")
from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MarketDataProducer:
    def __init__(self):
        logger.info("Ket noi Kafka Broker cho Market Data...")
        self.producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        self.topic = "market_stock_data"

    def stream_historical_data(self, ticker: str, start_date: str, end_date: str):
        """Keo du lieu gia lich su va day vao Kafka."""
        logger.info(f"Tai du lieu {ticker} tu {start_date} den {end_date}...")
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                q  = Quote(symbol=ticker, source="VCI")
                df = q.history(start=start_date, end=end_date, interval="1D")

            if df is None or df.empty:
                logger.warning(f"Khong co du lieu cho ma {ticker}.")
                return

            count = 0
            for _, row in df.iterrows():
                market_msg = {
                    "timestamp":      datetime.now(timezone.utc).isoformat(),
                    "ticker_context": ticker,
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
                self.producer.send(self.topic, value=market_msg)
                count += 1
                time.sleep(0.1)

            self.producer.flush()
            logger.info(f"Stream thanh cong {count} phien giao dich {ticker} vao Kafka.")

        except Exception as e:
            logger.error(f"Loi keo du lieu vnstock {ticker}: {e}")


if __name__ == "__main__":
    producer = MarketDataProducer()
    producer.stream_historical_data("SHB",     "2026-01-01", "2026-06-01")
    producer.stream_historical_data("VNINDEX", "2022-10-01", "2022-12-31")
