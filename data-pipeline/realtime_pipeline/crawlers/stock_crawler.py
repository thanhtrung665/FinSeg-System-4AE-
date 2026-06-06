import logging
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, List, Optional

# Suppress vnstock banner TRUOC khi import bat ky thu gi tu vnstock
os.environ.setdefault("VNSTOCK_DISABLE_NOTIFICATION", "1")
os.environ.setdefault("VNSTOCK_SHOW_ADS", "0")
warnings.filterwarnings("ignore")

from realtime_pipeline.config import (
    HISTORY_START_DATE, HISTORY_END_DATE, TICKER_MAP
)

logger = logging.getLogger(__name__)

# Import API moi (khong dung Vnstock().stock() da deprecated)
try:
    from vnstock.api.quote import Quote as _VnQuote
    _VNSTOCK_OK = True
except Exception:
    _VnQuote = None
    _VNSTOCK_OK = False


# ── Data classes ───────────────────────────────────────────────────────────────
@dataclass
class RawStockBar:
    bar_id:         str
    ticker:         str
    trading_date:   str
    open:           float
    high:           float
    low:            float
    close:          float
    volume:         int
    timestamp:      str
    ticker_context: str   = ""
    source:         str   = "vnstock"
    source_type:    str   = "market"
    price_change:   float = 0.0
    is_up:          bool  = False


@dataclass
class RawStockInfo:
    ticker:       str
    company_name: str   = ""
    exchange:     str   = ""
    industry:     str   = ""
    market_cap:   float = 0.0
    pe_ratio:     float = 0.0
    eps:          float = 0.0
    timestamp:    str   = ""


# ── Core fetchers ──────────────────────────────────────────────────────────────

def fetch_stock_history(
    ticker:     str,
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
) -> List[RawStockBar]:
    """
    Lay lich su OHLCV tu start_date → end_date (mac dinh thang 4-6 hien tai).
    Dung vnstock.api.quote.Quote — API moi, khong deprecation warning.
    """
    if not _VNSTOCK_OK:
        logger.error("vnstock khong kha dung — kiem tra cai dat")
        return []

    start  = start_date or HISTORY_START_DATE
    end    = end_date   or HISTORY_END_DATE
    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    bars: List[RawStockBar] = []

    try:
        logger.info(f"[Stock] {symbol}: {start} → {end}")
        q  = _VnQuote(symbol=symbol, source="VCI")
        df = q.history(start=start, end=end, interval="1D")

        if df is None or df.empty:
            logger.warning(f"[Stock] Khong co data cho {symbol}")
            return []

        prev_close: Optional[float] = None
        for _, row in df.iterrows():
            c   = float(row.get("close", 0))
            pct = 0.0
            if prev_close and prev_close > 0:
                pct = (c - prev_close) / prev_close * 100

            td     = str(row.get("time", ""))
            bar_id = f"{symbol}_{td.replace('-', '')}"

            bars.append(RawStockBar(
                bar_id         = bar_id,
                ticker         = symbol,
                trading_date   = td,
                open           = float(row.get("open",   0)),
                high           = float(row.get("high",   0)),
                low            = float(row.get("low",    0)),
                close          = c,
                volume         = int(row.get("volume",   0)),
                timestamp      = datetime.now(timezone.utc).isoformat(),
                ticker_context = ticker,
                price_change   = round(pct, 2),
                is_up          = c > (prev_close or c),
            ))
            prev_close = c

        logger.info(f"[Stock] {symbol}: {len(bars)} phien giao dich")
        return bars

    except Exception as e:
        logger.error(f"[Stock] Loi lay lich su {symbol}: {e}")
        return []


def fetch_stock_realtime(ticker: str) -> Optional[RawStockBar]:
    """Lay gia co phieu phien hien tai hoac phien cuoi cung."""
    if not _VNSTOCK_OK:
        return None

    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    try:
        q     = _VnQuote(symbol=symbol, source="VCI")
        today = date.today().strftime("%Y-%m-%d")
        df    = q.history(start=today, end=today, interval="1D")

        if df is None or df.empty:
            yesterday = (date.today() - timedelta(days=3)).strftime("%Y-%m-%d")
            df = q.history(start=yesterday, end=today, interval="1D")
            if df is None or df.empty:
                return None

        row = df.iloc[-1]
        c   = float(row.get("close", 0))
        return RawStockBar(
            bar_id         = f"{symbol}_{today}_rt",
            ticker         = symbol,
            trading_date   = today,
            open           = float(row.get("open",   0)),
            high           = float(row.get("high",   0)),
            low            = float(row.get("low",    0)),
            close          = c,
            volume         = int(row.get("volume",   0)),
            timestamp      = datetime.now(timezone.utc).isoformat(),
            ticker_context = ticker,
        )
    except Exception as e:
        logger.error(f"[Stock] Loi lay realtime {symbol}: {e}")
        return None


def fetch_stock_info(ticker: str) -> RawStockInfo:
    """Lay thong tin co ban (graceful fallback neu API khong ho tro)."""
    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    if not _VNSTOCK_OK:
        return RawStockInfo(ticker=symbol, timestamp=datetime.now(timezone.utc).isoformat())

    try:
        q = _VnQuote(symbol=symbol, source="VCI")
        if hasattr(q, "profile"):
            profile = q.profile()
            if profile is not None and not getattr(profile, "empty", True):
                row = profile.iloc[0]
                return RawStockInfo(
                    ticker       = symbol,
                    company_name = str(row.get("company_name", "")),
                    exchange     = str(row.get("exchange",     "")),
                    industry     = str(row.get("industry",     "")),
                    timestamp    = datetime.now(timezone.utc).isoformat(),
                )
    except Exception as e:
        logger.debug(f"[Stock] Khong lay duoc info {symbol}: {e}")

    return RawStockInfo(ticker=symbol, timestamp=datetime.now(timezone.utc).isoformat())


# ── Sentiment calculator ───────────────────────────────────────────────────────

def calculate_market_sentiment(bars: List[RawStockBar]) -> float:
    """
    Tinh diem cam xuc gia thi truong trong [-1.0, 1.0].
    >= +2% → +1.0 | > 0% → +0.5 | = 0% → 0 | < 0% → -0.5 | <= -2% → -1.0
    """
    if not bars:
        return 0.0
    scores = []
    for b in bars:
        if b.price_change >= 2.0:
            scores.append(1.0)
        elif b.price_change > 0:
            scores.append(0.5)
        elif b.price_change <= -2.0:
            scores.append(-1.0)
        elif b.price_change < 0:
            scores.append(-0.5)
        else:
            scores.append(0.0)
    return round(sum(scores) / len(scores), 4)


# ── Public API ─────────────────────────────────────────────────────────────────

def crawl_stocks_for_ticker(ticker: str) -> Dict[str, Any]:
    """
    Thu thap day du du lieu co phieu: lich su, realtime, info, sentiment.
    """
    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    logger.info(f"[Stock] Crawl {symbol} (ticker_context={ticker})")

    bars          = fetch_stock_history(ticker)
    rt            = fetch_stock_realtime(ticker)
    info          = fetch_stock_info(ticker)
    mkt_sentiment = calculate_market_sentiment(
        bars[-10:] if len(bars) > 10 else bars
    )

    return {
        "ticker":           symbol,
        "ticker_context":   ticker,
        "historical_bars":  bars,
        "realtime_bar":     rt,
        "info":             info,
        "market_sentiment": mkt_sentiment,
        "total_bars":       len(bars),
    }
